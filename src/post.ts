import * as Schema from "effect/Schema";
import {
  PostError,
  type PostDeliveryResult,
} from "./internal/post-contract.js";
export {
  PostError,
  PostDeliveryResultSchema,
} from "./internal/post-contract.js";
export type { PostDeliveryResult } from "./internal/post-contract.js";
import {
  deliverPost,
  postDisposition,
  deliveryReceipt,
} from "./internal/post-execution.js";

import { defineIdFamily } from "@fungi.computer/cairn";
import type {
  WatchdogRunningJob,
  WatchdogSqliteOwner,
  WatchdogRuntime,
  WatchdogTransactionalProjection,
} from "@fungi.computer/watchdog";

import { maxDeliveryAttempts, retryDelay } from "./post-outbox.js";

export { makePostOutbox, retryDelay } from "./post-outbox.js";
export type { PostOutbox, PostOutboxStore } from "./post-outbox.js";

const Key = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const JobPayload = Schema.Struct({ postKey: Key });
const PageLimit = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(1),
  Schema.isLessThanOrEqualTo(64),
);
const ids = defineIdFamily({
  family: "post",
  version: 1,
  kinds: { work: { prefix: "post_work" } },
});
/** Wire envelope parsed at the durable row and receiver boundaries. */
export const PostEnvelopeSchema = Schema.Struct({
  key: Key,
  kind: Key,
  address: Schema.Unknown,
  event: Schema.Unknown,
});
/** Persisted Post row parser. */
export const PostRowSchema = Schema.fromJsonString(
  Schema.Union([
    Schema.Struct({
      state: Schema.Literal("pending"),
      envelope: PostEnvelopeSchema,
      attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      nextAttemptAt: Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
      jobId: Schema.optional(Key),
      delivery: Schema.optional(PostEnvelopeSchema),
      nextKey: Schema.optional(Key),
    }),
    Schema.Struct({
      state: Schema.Literal("delivered"),
      receipt: Schema.optional(Schema.Json),
      nextKey: Schema.optional(Key),
    }),
    Schema.Struct({
      state: Schema.Literal("dead"),
      envelope: PostEnvelopeSchema,
      attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      reason: Schema.Literals(["refused", "exhausted"]),
      receipt: Schema.optional(Schema.Json),
      delivery: Schema.optional(PostEnvelopeSchema),
      nextKey: Schema.optional(Key),
    }),
  ]),
);

/** One addressed event identified by the producer's stable key. */
export type PostEnvelope = Schema.Schema.Type<typeof PostEnvelopeSchema>;
/** Pending row, terminal receipt, or dead letter. */
export type PostRow = Schema.Schema.Type<typeof PostRowSchema>;

/** Schema-correlated ordinary events or prepared delivery facts. */
export type PostRoute<A, E, D = E, AI = A, EI = E, DI = EI> = Readonly<{
  /** Fixed registry label shared by routes to one recipient namespace. */
  addressKind: string;
  /** Address parser for this kind. */
  address: Schema.Codec<A, AI>;
  /** Staged event parser for this kind. */
  event: Schema.Codec<E, EI>;
  /** Stable Durable Object name for one address. */
  name: (address: A) => string;
  /** Durable Object namespace for this kind. */
  namespace: Readonly<{
    /** Obtain the addressed Durable Object. */
    getByName: (name: string) => Readonly<{
      /** Idempotently apply one addressed envelope. */
      deliver: (envelope: PostEnvelope) => Promise<PostDeliveryResult>;
    }>;
  }>;
}> &
  (
    | Readonly<{
        /** Prepare one fact before receiver I/O; its callbacks consume that fact. */
        prepare: Readonly<{
          /** Parser for the prepared delivery fact. */
          event: Schema.Codec<D, DI>;
          /** Resolve the staged intent into a delivery fact. */
          run: (address: A, intent: E, signal: AbortSignal) => Promise<D>;
        }>;
        /** Acknowledge delivery in the producer's receipt transaction. */
        onDelivered?: (address: A, event: D) => void;
        /** Commit the receiver cursor in the producer's receipt transaction. */
        onCursor?: (address: A, event: D, cursor: number) => void;
        /** Revoke a subscription after permanent refusal. */
        onRefused?: (address: A, event: D) => void;
      }>
    | Readonly<{
        /** An ordinary event is also its delivery fact. */
        prepare?: undefined;
        /** Acknowledge delivery in the producer's receipt transaction. */
        onDelivered?: (address: A, event: E) => void;
        /** Commit the receiver cursor in the producer's receipt transaction. */
        onCursor?: (address: A, event: E, cursor: number) => void;
        /** Revoke a subscription after permanent refusal. */
        onRefused?: (address: A, event: E) => void;
      }>
  );

type PostReceiptHooks<A, D> = Readonly<{
  /** Acknowledge domain delivery in the same transaction as the Post receipt. */
  onDelivered?: (address: A, event: D) => void;
  /** Commit a receiver's durable cursor with the producer's Post receipt. */
  onCursor?: (address: A, event: D, cursor: number) => void;
  /** Revoke a producer subscription after the receiver permanently refuses. */
  onRefused?: (address: A, event: D) => void;
}>;

/** Runtime operations that preserve the route's schema evidence. */
export type PostRegistryRoute = Readonly<{
  /** Recipient namespace identity used for per-address ordering. */
  addressKind: string;
  /** Parser for this address kind. */
  address: Schema.Codec<unknown, unknown>;
  /** Parser for this event kind. */
  event: Schema.Codec<unknown, unknown>;
  /** Encode a producer's domain values with this route's schemas. */
  encodeEnvelope: (envelope: PostEnvelope) => PostEnvelope;
  /** Decode a prepared wire fact, or the ordinary event when no preparation is needed. */
  parseDelivery: (envelope: PostEnvelope) => PostEnvelope;
  /** Prepare the wire fact before receiver I/O. */
  prepareEnvelope?:
    | ((envelope: PostEnvelope, signal: AbortSignal) => Promise<PostEnvelope>)
    | undefined;
  /** Acknowledge producer domain state in the receipt transaction. */
  recordDelivered?: ((envelope: PostEnvelope) => void) | undefined;
  /** Decode the address and return its stable Durable Object name. */
  nameOf: (envelope: Pick<PostEnvelope, "address">) => string;
  /** Durable Object namespace. */
  namespace: PostRoute<never, never>["namespace"];
  /** Apply a typed receiver cursor in the producer transaction. */
  recordCursor?: ((envelope: PostEnvelope, cursor: number) => void) | undefined;
  /** Apply a typed permanent refusal in the producer transaction. */
  recordRefused?: ((envelope: PostEnvelope) => void) | undefined;
}>;

function receiptOperations<A, E, AI, EI>(
  address: Schema.Codec<A, AI>,
  event: Schema.Codec<E, EI>,
  hooks: PostReceiptHooks<A, E>,
): Pick<
  PostRegistryRoute,
  "recordCursor" | "recordRefused" | "recordDelivered" | "parseDelivery"
> {
  const { onCursor, onRefused, onDelivered } = hooks;
  const decodeAddress = Schema.decodeUnknownSync(address);
  const decodeEvent = Schema.decodeUnknownSync(event);
  return {
    recordCursor:
      onCursor === undefined
        ? undefined
        : (envelope, cursor) => {
            onCursor(
              decodeAddress(envelope.address),
              decodeEvent(envelope.event),
              cursor,
            );
          },
    recordRefused:
      onRefused === undefined
        ? undefined
        : (envelope) => {
            onRefused(
              decodeAddress(envelope.address),
              decodeEvent(envelope.event),
            );
          },
    recordDelivered:
      onDelivered === undefined
        ? undefined
        : (envelope) => {
            onDelivered(
              decodeAddress(envelope.address),
              decodeEvent(envelope.event),
            );
          },
    parseDelivery: (envelope) => ({
      ...envelope,
      address: Schema.encodeSync(address)(decodeAddress(envelope.address)),
      event: Schema.encodeSync(event)(decodeEvent(envelope.event)),
    }),
  };
}

/** Preserve each route's inferred address and event types in a registry. */
export function definePostRoute<A, E, AI = A, EI = E>(
  route: Extract<PostRoute<A, E, E, AI, EI, EI>, { prepare?: undefined }>,
): PostRoute<A, E, E, AI, EI, EI> &
  Omit<PostRegistryRoute, "address" | "event">;
export function definePostRoute<A, E, D, AI = A, EI = E, DI = EI>(
  route: Extract<PostRoute<A, E, D, AI, EI, DI>, { prepare: unknown }>,
): PostRoute<A, E, D, AI, EI, DI> &
  Omit<PostRegistryRoute, "address" | "event">;
export function definePostRoute<A, E, D = E, AI = A, EI = E, DI = EI>(
  route: PostRoute<A, E, D, AI, EI, DI>,
): PostRoute<A, E, D, AI, EI, DI> &
  Omit<PostRegistryRoute, "address" | "event"> {
  const delivery =
    route.prepare === undefined
      ? receiptOperations(route.address, route.event, route)
      : receiptOperations(route.address, route.prepare.event, route);
  const prepare = route.prepare;
  return {
    ...route,
    ...delivery,
    addressKind: Schema.decodeUnknownSync(Key)(route.addressKind),
    prepareEnvelope:
      prepare === undefined
        ? undefined
        : async (envelope, signal) => ({
            ...envelope,
            event: Schema.encodeUnknownSync(prepare.event)(
              await prepare.run(
                Schema.decodeUnknownSync(route.address)(envelope.address),
                Schema.decodeUnknownSync(route.event)(envelope.event),
                signal,
              ),
            ),
          }),
    encodeEnvelope: (envelope) => ({
      ...envelope,
      address: Schema.encodeUnknownSync(route.address)(envelope.address),
      event: Schema.encodeUnknownSync(route.event)(envelope.event),
    }),
    nameOf: (envelope) =>
      route.name(Schema.decodeUnknownSync(route.address)(envelope.address)),
  };
}

const StoredRow = Schema.Struct({ key: Schema.String, value: Schema.String });
const DueRow = Schema.Struct({
  due: Schema.NullOr(Schema.Number.check(Schema.isGreaterThanOrEqualTo(0))),
});
const openPostStore = (owner: WatchdogSqliteOwner) => {
  const sql = owner.sql;
  owner.transactionSync(() => {
    sql.exec(
      "CREATE TABLE IF NOT EXISTS carrier_post_rows (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    );
  });
  return {
    commit: <A>(mutation: () => A) => owner.transactionSync(mutation),
    read: (key: string): string | undefined => {
      const row = sql
        .exec("SELECT key,value FROM carrier_post_rows WHERE key=?", key)
        .toArray()[0];
      return row === undefined
        ? undefined
        : Schema.decodeUnknownSync(StoredRow)(row).value;
    },
    put: (key: string, value: string): void => {
      sql.exec(
        "INSERT INTO carrier_post_rows VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        key,
        value,
      );
    },
    list: (limit: number, afterKey = "") =>
      Schema.decodeUnknownSync(Schema.Array(StoredRow))(
        sql
          .exec(
            "SELECT key,value FROM carrier_post_rows WHERE key LIKE 'post:%' AND key>? ORDER BY key LIMIT ?",
            afterKey,
            limit,
          )
          .toArray(),
      ),
    nextWake: () =>
      Schema.decodeUnknownSync(DueRow)(
        sql
          .exec(
            "SELECT MIN(json_extract(value,'$.nextAttemptAt')) AS due FROM carrier_post_rows WHERE key LIKE 'post:%' AND json_extract(value,'$.state')='pending' AND json_type(value,'$.jobId')='text'",
          )
          .toArray()[0],
      ).due,
  };
};

/** One producer's Post operations. */
export type PostRuntime<R extends Record<string, PostRegistryRoute>> =
  Readonly<{
    /** Write a row and Watchdog job in the producer's own transaction. */
    send: <K extends keyof R & string>(
      input: Readonly<{
        key: string;
        kind: K;
        address: Schema.Schema.Type<R[K]["address"]>;
        event: Schema.Schema.Type<R[K]["event"]>;
      }>,
    ) => void;
    /** Run as the Watchdog executor for a claimed Post job. */
    execute: (
      job: WatchdogRunningJob,
    ) => Promise<Readonly<{ kind: "completed" }>>;
    /** Recover settled delivery jobs and enqueue due retries after a wake. */
    enqueueDue: (
      limit?: number,
      afterKey?: string,
    ) => Promise<string | undefined>;
    /** Read an ordered page of refused or exhausted keys for operators. */
    deadLetters: (
      limit?: number,
      afterKey?: string,
    ) => Readonly<{
      letters: readonly Readonly<{
        key: string;
        reason: "refused" | "exhausted";
        envelope: PostEnvelope;
        attempts: number;
      }>[];
      nextCursor: string | undefined;
    }>;
    /** The latest staged event of one kind for an address, including its terminal receipt. */
    latest: <K extends keyof R & string>(
      kind: K,
      address: Schema.Schema.Type<R[K]["address"]>,
    ) => Readonly<{ key: string; row: PostRow }> | undefined;
    /** Requeue a failed event or expedite its settled retry, preserving its prepared fact. */
    retry: (key: string) => Promise<void>;
    /** Inspect one producer-local row or receipt. */
    read: (key: string) => PostRow | undefined;
    /** Earliest pending queue head, including a settled job waiting to retry. */
    nextWake: () => number | null;
  }>;

/** Decode at both persisted and DO boundaries with the route's Effect Schema. */
export function createPost<R extends Record<string, PostRegistryRoute>>(
  options: Readonly<{
    routes: R;
    owner: WatchdogSqliteOwner;
    transactional: WatchdogTransactionalProjection;
    readJob: WatchdogRuntime["read"];
    now: () => number;
    scheduleWake: (at: number) => Promise<void>;
  }>,
): PostRuntime<R> {
  const store = openPostStore(options.owner);
  const rowKey = (key: string) => `post:${key}`;
  const tailKey = (queue: string) => `post-tail:${queue}`;
  const headKey = (kind: string, queue: string) =>
    `post-head:${JSON.stringify([kind, queue])}`;
  const readRow = (key: string): PostRow | undefined => {
    const raw = store.read(rowKey(key));
    return raw === undefined
      ? undefined
      : Schema.decodeUnknownSync(PostRowSchema)(raw);
  };
  const nextWake = () => store.nextWake();
  const putRow = (key: string, row: PostRow) => {
    store.put(rowKey(key), Schema.encodeSync(PostRowSchema)(row));
  };
  const routeFor = (envelope: PostEnvelope) => {
    const route = options.routes[envelope.kind];
    if (route === undefined) throw new PostError("unknown address kind");
    return route;
  };
  const queueOf = (envelope: PostEnvelope) => {
    const route = routeFor(envelope);
    return JSON.stringify([route.addressKind, route.nameOf(envelope)]);
  };
  const enqueueJob = (row: Extract<PostRow, { state: "pending" }>) => {
    const jobId = ids.work.generate();
    putRow(row.envelope.key, { ...row, jobId });
    options.transactional.enqueueAcceptedJob({
      jobId,
      queue: queueOf(row.envelope),
      lane: "post",
      priority: 0,
      payload: { postKey: row.envelope.key },
      state: "queued",
    });
  };

  const send: PostRuntime<R>["send"] = (input) => {
    const incoming = Schema.decodeUnknownSync(PostEnvelopeSchema)(input);
    const route = routeFor(incoming);
    const envelope = route.encodeEnvelope(incoming);
    const existing = readRow(envelope.key);
    if (existing !== undefined) {
      if (
        existing.state !== "delivered" &&
        JSON.stringify(existing.envelope) !== JSON.stringify(envelope)
      )
        throw new PostError("conflicting post key");
      return;
    }
    const queue = queueOf(envelope);
    const previousKey = store.read(tailKey(queue));
    const previous =
      previousKey === undefined ? undefined : readRow(previousKey);
    const row = {
      state: "pending",
      envelope,
      attempts: 0,
      nextAttemptAt: options.now(),
    } as const;
    putRow(envelope.key, row);
    if (previousKey !== undefined && previous?.state === "pending") {
      putRow(previousKey, { ...previous, nextKey: envelope.key });
    } else {
      enqueueJob(row);
    }
    store.put(tailKey(queue), envelope.key);
    store.put(headKey(envelope.kind, queue), envelope.key);
  };

  const latest: PostRuntime<R>["latest"] = (kind, address) => {
    const route = options.routes[kind];
    if (route === undefined) throw new PostError("unknown address kind");
    const queue = JSON.stringify([
      route.addressKind,
      route.nameOf({
        address: Schema.encodeUnknownSync(route.address)(address),
      }),
    ]);
    const key = store.read(headKey(kind, queue));
    if (key === undefined) return undefined;
    const row = readRow(key);
    if (row === undefined)
      throw new PostError("latest delivery row is missing");
    return { key, row };
  };

  const releaseNext = (nextKey: string | undefined) => {
    if (nextKey === undefined) return;
    const next = readRow(nextKey);
    if (next?.state === "pending" && next.jobId === undefined) enqueueJob(next);
  };
  const finish = (
    key: string,
    row: Extract<PostRow, { state: "pending" }>,
    terminal:
      | Readonly<{
          state: "delivered";
        }>
      | Readonly<{ state: "dead"; reason: "refused" | "exhausted" }>,
    metadata?: Schema.Json,
  ) => {
    let receipt: PostRow;
    if (terminal.state === "dead") {
      const dead = {
        ...terminal,
        envelope: row.envelope,
        attempts: row.attempts,
      };
      receipt =
        row.delivery === undefined ? dead : { ...dead, delivery: row.delivery };
    } else receipt = terminal;
    if (metadata !== undefined) receipt = { ...receipt, receipt: metadata };
    putRow(
      key,
      row.nextKey === undefined
        ? receipt
        : { ...receipt, nextKey: row.nextKey },
    );
    releaseNext(row.nextKey);
  };

  const execute: PostRuntime<R>["execute"] = async (job) => {
    const { postKey } = Schema.decodeUnknownSync(JobPayload)(job.payload);
    const attempt = store.commit(() => {
      const row = readRow(postKey);
      if (row === undefined) throw new PostError("delivery row is missing");
      if (row.state !== "pending" || row.jobId !== job.jobId) return undefined;
      if (row.attempts >= maxDeliveryAttempts) {
        finish(postKey, row, { state: "dead", reason: "exhausted" });
        return undefined;
      }
      const attempts = row.attempts + 1;
      const nextAttemptAt = options.now() + retryDelay(attempts);
      const started = { ...row, attempts, nextAttemptAt };
      // Preserve recovery evidence before invoking the receiver. A failed receipt
      // write leaves this row available for retry after Watchdog settles the job.
      putRow(postKey, started);
      return started;
    });
    if (attempt === undefined) return { kind: "completed" };
    await options.scheduleWake(attempt.nextAttemptAt);
    const route = routeFor(attempt.envelope);
    const evidence = await deliverPost({
      attempt,
      route,
      savePrepared: (envelope) => {
        store.commit(() => {
          const current = readRow(postKey);
          if (current?.state !== "pending" || current.jobId !== job.jobId)
            throw new PostError("preparation superseded");
          putRow(postKey, { ...current, delivery: envelope });
        });
      },
    });
    let retryAt: number | undefined;
    store.commit(() => {
      // Another send may have linked a successor during the remote call.
      const current = readRow(postKey);
      if (current?.state !== "pending" || current.jobId !== job.jobId) return;
      const disposition = postDisposition(
        evidence,
        route,
        current.attempts,
        maxDeliveryAttempts,
      );
      if (disposition.state === "keep") return;
      if (disposition.state === "delivered") {
        if (evidence.envelope === undefined)
          throw new PostError("missing delivered fact");
        if (disposition.cursor !== undefined)
          route.recordCursor?.(evidence.envelope, disposition.cursor);
        route.recordDelivered?.(evidence.envelope);
        finish(
          postKey,
          current,
          { state: "delivered" },
          deliveryReceipt(evidence),
        );
      } else if (disposition.state === "dead") {
        if (disposition.reason === "refused" && evidence.envelope !== undefined)
          route.recordRefused?.(evidence.envelope);
        finish(postKey, current, disposition, deliveryReceipt(evidence));
      } else {
        const due = options.now() + retryDelay(current.attempts);
        if (due !== current.nextAttemptAt) {
          putRow(postKey, { ...current, nextAttemptAt: due });
          retryAt = due;
        }
      }
    });
    if (retryAt !== undefined) await options.scheduleWake(retryAt);
    return { kind: "completed" };
  };

  const enqueueDue: PostRuntime<R>["enqueueDue"] = async (
    limit = 64,
    afterKey,
  ) => {
    const pageSize = Schema.decodeUnknownSync(PageLimit)(limit);
    const rows = store.list(pageSize, afterKey);
    for (const stored of rows) {
      const row = Schema.decodeUnknownSync(PostRowSchema)(stored.value);
      if (row.state !== "pending" || row.jobId === undefined) continue;
      const work = await options.readJob(row.jobId);
      if (work.status === "missing")
        throw new PostError("delivery job is missing");
      if (work.job.state !== "settled") continue;
      if (row.nextAttemptAt > options.now()) {
        await options.scheduleWake(row.nextAttemptAt);
        continue;
      }
      store.commit(() => {
        const current = readRow(row.envelope.key);
        if (current?.state !== "pending" || current.jobId !== row.jobId) return;
        if (current.attempts >= maxDeliveryAttempts)
          finish(current.envelope.key, current, {
            state: "dead",
            reason: "exhausted",
          });
        else enqueueJob(current);
      });
    }
    return rows.length === pageSize ? rows.at(-1)?.key : undefined;
  };

  const retry: PostRuntime<R>["retry"] = async (input) => {
    const key = Schema.decodeUnknownSync(Key)(input);
    const row = readRow(key);
    if (row === undefined || row.state === "delivered") return;
    const work =
      row.state === "pending" && row.jobId !== undefined
        ? await options.readJob(row.jobId)
        : undefined;
    if (work?.status === "missing")
      throw new PostError("delivery job is missing");
    if (work?.status === "found" && work.job.state !== "settled") return;
    store.commit(() => {
      const current = readRow(key);
      if (current === undefined || current.state === "delivered") return;
      if (current.state === "pending") {
        if (row.state !== "pending" || current.jobId !== row.jobId) return;
        const pending = {
          ...current,
          attempts: 0,
          nextAttemptAt: options.now(),
        };
        putRow(key, pending);
        if (current.jobId !== undefined) enqueueJob(pending);
      } else {
        const pending = {
          state: "pending" as const,
          envelope: current.envelope,
          attempts: 0,
          nextAttemptAt: options.now(),
          delivery: current.delivery,
        };
        putRow(key, pending);
        const queue = queueOf(pending.envelope);
        const previousKey = store.read(tailKey(queue));
        const previous =
          previousKey === undefined || previousKey === key
            ? undefined
            : readRow(previousKey);
        if (previous?.state === "pending" && previousKey !== undefined)
          putRow(previousKey, { ...previous, nextKey: key });
        else enqueueJob(pending);
        store.put(tailKey(queue), key);
      }
    });
    await options.scheduleWake(options.now());
  };

  const deadLetters: PostRuntime<R>["deadLetters"] = (limit = 64, afterKey) => {
    const pageSize = Schema.decodeUnknownSync(PageLimit)(limit);
    const rows = store.list(pageSize, afterKey);
    const letters = rows.flatMap(({ key, value }) => {
      const row = Schema.decodeUnknownSync(PostRowSchema)(value);
      return row.state === "dead"
        ? [
            {
              key,
              reason: row.reason,
              envelope: row.envelope,
              attempts: row.attempts,
            },
          ]
        : [];
    });
    return {
      letters,
      nextCursor: rows.length === pageSize ? rows.at(-1)?.key : undefined,
    };
  };

  return Object.freeze({
    send,
    latest,
    retry,
    execute,
    enqueueDue,
    deadLetters,
    read: readRow,
    nextWake,
  });
}

/** Consumer admission runs inside the receiver's own SQLite transaction. */
export function admitPost<E, I>(
  options: Readonly<{
    key: string;
    event: unknown;
    schema: Schema.Codec<E, I>;
    hasReceipt: (key: string) => boolean;
    apply: (event: E) => PostDeliveryResult;
    putReceipt: (key: string) => void;
  }>,
): PostDeliveryResult {
  const key = Schema.decodeUnknownSync(Key)(options.key);
  const event = Schema.decodeUnknownSync(options.schema)(options.event);
  if (options.hasReceipt(key)) return "duplicate";
  const result = options.apply(event);
  if (result === "accepted") options.putReceipt(key);
  return result;
}
