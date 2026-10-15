import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const PageSize = 64;
/** Maximum receiver attempts retained from BirdDog. */
export const maxDeliveryAttempts = 20;

/** BirdDog's retained bounded retry schedule. */
export const retryDelay = (attempts: number): number =>
  Math.min(2_000 * 2 ** Math.max(0, attempts - 1), 300_000);

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
  /** Inspect exhausted messages in an ordered page. */
  deadLetters: (limit: number, afterKey?: string) => Effect.Effect<M[], E>;
}>;

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
  const Pending = Schema.Struct({
    mail: options.message,
    attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    nextAttemptAt: Schema.optional(
      Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
    ),
  });
  const Row = Schema.Union([
    Pending,
    Schema.Struct({ state: Schema.Literal("delivered") }),
  ]);
  const Text = Schema.fromJsonString(Row);
  const decode = (value: string) =>
    Schema.decodeUnknownEffect(Text)(value).pipe(
      Effect.mapError(options.invalidRow),
    );
  const encode = (row: Schema.Schema.Type<typeof Row>) =>
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
      const nextAttemptAt = now + retryDelay(attempts);
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

  return { enqueue, drain, deadLetters };
}
