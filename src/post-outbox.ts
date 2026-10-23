import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { backoffDelay, type RetryPolicy } from "@fungi.computer/almanac";

const PageSize = 64;
/** Maximum receiver attempts retained from BirdDog. */
export const maxDeliveryAttempts = 20;

/** BirdDog's retained receiver retry policy, shared by every Post adapter. */
export const postRetryPolicy: RetryPolicy = {
  baseMs: 2_000,
  maxMs: 300_000,
};

const earlier = (a: number | undefined, b: number | undefined) =>
  b === undefined ? a : a === undefined ? b : Math.min(a, b);

const logDeliveryFailure = (cause: unknown): void => {
  console.error("Carrier Post outbox delivery failed", {
    failureKind: cause instanceof Error ? "Error" : "NonErrorThrown",
  });
};

/** Durable, string-backed rows. A delivered receipt must outlive source replay. */
export type PostOutboxStore<E> = Readonly<{
  /** Commit one pending row or completion receipt. */
  put: (key: string, value: string) => Effect.Effect<void, E>;
  /** Inspect one durable row by key. */
  read: (key: string) => Effect.Effect<string | undefined, E>;
  /** Read an ordered bounded page. */
  list: (
    limit: number,
    afterKey?: string,
  ) => Effect.Effect<readonly Readonly<{ key: string; value: string }>[], E>;
  /** Discard one malformed row. */
  delete: (key: string) => Effect.Effect<void, E>;
}>;

/** Bounded generic delivery owned by Post. */
export type PostOutbox<M, E> = Readonly<{
  /** Queue a message and return its wake hint so a fan-out can schedule once. */
  enqueue: (message: M, at?: number) => Effect.Effect<number | undefined, E>;
  /** Attempt one bounded page and arrange the next wake. */
  drain: () => Effect.Effect<void, E>;
  /** Earliest owed delivery, including a future retry; absent when none remain. */
  nextWakeAt: () => Effect.Effect<number | undefined, E>;
  /** Inspect exhausted messages in an ordered page. */
  deadLetters: (limit: number, afterKey?: string) => Effect.Effect<M[], E>;
}>;

/** One durable row: a message still owed delivery, or its delivered receipt. */
function postRowText<M>(message: Schema.Codec<M>) {
  return Schema.fromJsonString(
    Schema.Union([
      Schema.Struct({
        mail: message,
        attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
        nextAttemptAt: Schema.optional(
          Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
        ),
      }),
      Schema.Struct({ state: Schema.Literal("delivered") }),
    ]),
  );
}

/** The same bounded delivery and receipt machinery serves Post and BirdDog. */
export function makePostOutbox<M, E>(
  options: Readonly<{
    message: Schema.Codec<M>;
    key: (message: M) => string;
    outbox: PostOutboxStore<E>;
    deliver: (message: M) => Effect.Effect<boolean, E>;
    scheduleWake: (at: number) => Effect.Effect<void, E>;
    now: () => number;
    invalidRow: () => E;
    encodingFailed: () => E;
  }>,
): PostOutbox<M, E> {
  const Text = postRowText(options.message);
  const decode = (value: string) =>
    Schema.decodeUnknownEffect(Text)(value).pipe(
      Effect.mapError(options.invalidRow),
    );
  const encode = (row: Schema.Schema.Type<typeof Text>) =>
    Effect.try({
      try: () => Schema.encodeSync(Text)(row),
      catch: options.encodingFailed,
    });

  const enqueue = (message: M, at = options.now()) =>
    Effect.gen(function* () {
      const key = options.key(message);
      const existing = yield* options.outbox.read(key);
      if (existing !== undefined) {
        const row = yield* decode(existing);
        if ("state" in row || row.attempts >= maxDeliveryAttempts) return;
        return Math.max(at, row.nextAttemptAt ?? at);
      }
      const value = yield* encode({ mail: message, attempts: 0 });
      yield* options.outbox.put(key, value);
      return at;
    });

  const process = (
    stored: Readonly<{ key: string; value: string }>,
    now: number,
  ) =>
    Effect.gen(function* () {
      const decoded = yield* Effect.result(decode(stored.value));
      if (decoded._tag === "Failure") {
        yield* options.outbox.delete(stored.key);
        return undefined;
      }
      const row = decoded.success;
      if ("state" in row || row.attempts >= maxDeliveryAttempts)
        return undefined;
      if (row.nextAttemptAt !== undefined && row.nextAttemptAt > now)
        return row.nextAttemptAt;
      const delivered = yield* options.deliver(row.mail).pipe(
        Effect.catch((cause) =>
          Effect.sync(() => {
            logDeliveryFailure(cause);
          }).pipe(Effect.as(false)),
        ),
      );
      if (delivered) {
        yield* options.outbox.put(
          stored.key,
          yield* encode({ state: "delivered" }),
        );
        return undefined;
      }
      const attempts = row.attempts + 1;
      const nextAttemptAt = now + backoffDelay(attempts, postRetryPolicy);
      yield* options.outbox.put(
        stored.key,
        yield* encode({ mail: row.mail, attempts, nextAttemptAt }),
      );
      return attempts < maxDeliveryAttempts ? nextAttemptAt : undefined;
    });

  let cursor: string | undefined;
  let earliestFuture: number | undefined;
  const drain = () =>
    Effect.gen(function* () {
      const now = options.now();
      const rows = yield* options.outbox.list(PageSize, cursor);
      for (const row of rows)
        earliestFuture = earlier(earliestFuture, yield* process(row, now));
      if (rows.length === PageSize) {
        const lastKey = rows[rows.length - 1]?.key;
        if (
          lastKey !== undefined &&
          (yield* options.outbox.list(1, lastKey)).length > 0
        ) {
          cursor = lastKey;
          yield* options.scheduleWake(now);
          return;
        }
      }
      cursor = undefined;
      if (earliestFuture !== undefined)
        yield* options.scheduleWake(Math.max(now, earliestFuture));
      earliestFuture = undefined;
    });

  const deadLetters = (limit: number, afterKey?: string) =>
    Effect.gen(function* () {
      const rows = yield* options.outbox.list(limit, afterKey);
      const dead: M[] = [];
      for (const stored of rows) {
        const row = yield* decode(stored.value);
        if (!("state" in row) && row.attempts >= maxDeliveryAttempts)
          dead.push(row.mail);
      }
      return dead;
    });

  const nextWakeAt = () =>
    Effect.gen(function* () {
      let afterKey: string | undefined;
      let due: number | undefined;
      for (;;) {
        const rows = yield* options.outbox.list(PageSize, afterKey);
        for (const stored of rows) {
          // Malformed rows are discarded by drain, not deliverable work.
          const decoded = yield* Effect.result(decode(stored.value));
          if (decoded._tag === "Failure") continue;
          const row = decoded.success;
          if (!("state" in row) && row.attempts < maxDeliveryAttempts)
            due = earlier(due, row.nextAttemptAt ?? options.now());
        }
        const lastKey = rows.at(-1)?.key;
        if (rows.length < PageSize || lastKey === undefined) return due;
        afterKey = lastKey;
      }
    });

  return { enqueue, drain, deadLetters, nextWakeAt };
}

/**
 * Whether any message in an outbox still awaits a delivery attempt. Delivered
 * receipts and exhausted dead letters are retained but owe nothing more, so a
 * host may let their owner rest.
 */
export function postOutboxPending<M, E>(
  options: Readonly<{
    message: Schema.Codec<M>;
    outbox: Pick<PostOutboxStore<E>, "list">;
  }>,
): Effect.Effect<boolean, E> {
  const Text = postRowText(options.message);
  return Effect.gen(function* () {
    let afterKey: string | undefined;
    for (;;) {
      const rows = yield* options.outbox.list(PageSize, afterKey);
      for (const stored of rows) {
        // A malformed row is not deliverable; the next drain discards it.
        const decoded = yield* Effect.result(
          Schema.decodeUnknownEffect(Text)(stored.value),
        );
        if (decoded._tag === "Failure") continue;
        const row = decoded.success;
        if (!("state" in row) && row.attempts < maxDeliveryAttempts)
          return true;
      }
      const lastKey = rows.at(-1)?.key;
      if (rows.length < PageSize || lastKey === undefined) return false;
      afterKey = lastKey;
    }
  });
}
