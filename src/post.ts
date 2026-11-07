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
  WatchdogAttempt,
  WatchdogRetryPolicy,
  WatchdogExecutionDecision,
  WatchdogTransactionalProjection,
} from "@fungi.computer/watchdog";

import {
  maxDeliveryAttempts,
  postRetryPolicy,
} from "./internal/post-policy.js";

const Key = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const JobPayload = Schema.Struct({ postKey: Key });
const PageLimit = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(1),
  Schema.isLessThanOrEqualTo(64),
);
const maxRouteDeadlineMs = 7 * 24 * 60 * 60 * 1_000;
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
      deadlineAt: Schema.optional(
        Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
      ),
      jobId: Key,
      delivery: Schema.optional(PostEnvelopeSchema),
    }),
    Schema.Struct({
      state: Schema.Literal("delivered"),
      receipt: Schema.optional(Schema.Json),
    }),
    Schema.Struct({
      state: Schema.Literal("dead"),
      envelope: PostEnvelopeSchema,
      attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      reason: Schema.Literals(["refused", "exhausted", "expired"]),
      deadlineAt: Schema.optional(
        Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
      ),
      receipt: Schema.optional(Schema.Json),
      delivery: Schema.optional(PostEnvelopeSchema),
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
  /** Optional SQLite Post lifetime of 1 ms to 7 days from admission; relational adapters reject it. */
  deadlineMs?: number | undefined;
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
  /** Optional SQLite Post lifetime of 1 ms to 7 days from admission; relational adapters reject it. */
  deadlineMs?: number | undefined;
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
  if (route.deadlineMs !== undefined)
    Schema.decodeUnknownSync(
      Schema.Int.check(
        Schema.isBetween({ minimum: 1, maximum: maxRouteDeadlineMs }),
      ),
    )(route.deadlineMs);
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
const openPostStore = (owner: WatchdogSqliteOwner) => {
  const sql = owner.sql;
  owner.transactionSync(() => {
    sql.exec(
      "CREATE TABLE IF NOT EXISTS carrier_post_rows (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    );
    // Rows written before the Watchdog scheduler carry nextAttemptAt or nextKey;
    // Fungi 0.15.0 resets that store by refusing it, never loading or rewriting it.
    const preScheduler = sql
      .exec(
        "SELECT key FROM carrier_post_rows WHERE key LIKE 'post:%' AND (json_type(value,'$.nextAttemptAt') IS NOT NULL OR json_type(value,'$.nextKey') IS NOT NULL) LIMIT 1",
      )
      .toArray();
    if (preScheduler.length > 0)
      throw new PostError(
        "pre-scheduler Post store is not loaded; preserve it and authorize a reset through its owning host",
        "pre-scheduler-store",
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
    /** Resolve the retained route deadline or ordinary attempt ceiling. */
    retryPolicy: (job: WatchdogRunningJob) => Promise<WatchdogRetryPolicy>;
    /** Deliver using Watchdog's durably reserved attempt. */
    execute: (
      job: WatchdogRunningJob,
      attempt: WatchdogAttempt | undefined,
      signal?: AbortSignal,
    ) => Promise<WatchdogExecutionDecision>;
    /** Expedite queued deliveries without resetting their charged attempt count. */
    releaseNow: (
      limit?: number,
      afterKey?: string,
    ) => Promise<string | undefined>;
    /** Read an ordered page of refused, exhausted or expired keys for operators. */
    deadLetters: (
      limit?: number,
      afterKey?: string,
    ) => Readonly<{
      letters: readonly Readonly<{
        key: string;
        reason: "refused" | "exhausted" | "expired";
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
  }>;

/** Decode at both persisted and DO boundaries with the route's Effect Schema. */
export function createPost<R extends Record<string, PostRegistryRoute>>(
  options: Readonly<{
    routes: R;
    owner: WatchdogSqliteOwner;
    transactional: WatchdogTransactionalProjection;
    now: () => number;
    scheduleWake: (at: number) => Promise<void>;
  }>,
): PostRuntime<R> {
  const store = openPostStore(options.owner);
  const rowKey = (key: string) => `post:${key}`;
  const headKey = (kind: string, queue: string) =>
    `post-head:${JSON.stringify([kind, queue])}`;
  const readRow = (key: string): PostRow | undefined => {
    const raw = store.read(rowKey(key));
    return raw === undefined
      ? undefined
      : Schema.decodeUnknownSync(PostRowSchema)(raw);
  };
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
    putRow(row.envelope.key, row);
    options.transactional.enqueueAcceptedJob({
      jobId: row.jobId,
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
    const admittedAt = options.now();
    const baseRow = {
      state: "pending",
      envelope,
      jobId: ids.work.generate(),
    } as const;
    const row =
      route.deadlineMs === undefined
        ? baseRow
        : { ...baseRow, deadlineAt: admittedAt + route.deadlineMs };
    enqueueJob(row);
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

  const finish = (
    key: string,
    row: Extract<PostRow, { state: "pending" }>,
    terminal:
      | Readonly<{
          state: "delivered";
        }>
      | Readonly<{
          state: "dead";
          reason: "refused" | "exhausted" | "expired";
        }>,
    attempts: number,
    metadata?: Schema.Json,
  ) => {
    let receipt: PostRow;
    if (terminal.state === "dead") {
      const dead = {
        ...terminal,
        envelope: row.envelope,
        attempts,
      };
      const settled =
        row.deadlineAt === undefined
          ? dead
          : { ...dead, deadlineAt: row.deadlineAt };
      receipt =
        row.delivery === undefined
          ? settled
          : { ...settled, delivery: row.delivery };
    } else receipt = terminal;
    if (metadata !== undefined) receipt = { ...receipt, receipt: metadata };
    putRow(key, receipt);
  };

  const retryPolicy: PostRuntime<R>["retryPolicy"] = (job) => {
    const { postKey } = Schema.decodeUnknownSync(JobPayload)(job.payload);
    const row = readRow(postKey);
    if (row === undefined) throw new PostError("delivery row is missing");
    const limit =
      row.state !== "pending" || row.jobId !== job.jobId
        ? { maxAttempts: 0 }
        : row.deadlineAt === undefined
          ? { maxAttempts: maxDeliveryAttempts }
          : { deadlineAt: row.deadlineAt };
    return Promise.resolve({ ...postRetryPolicy, limit });
  };

  const execute: PostRuntime<R>["execute"] = async (
    job,
    reservation,
    signal,
  ) => {
    if (reservation === undefined)
      throw new PostError("delivery attempt is missing");
    const { postKey } = Schema.decodeUnknownSync(JobPayload)(job.payload);
    const row = store.commit(() => {
      const current = readRow(postKey);
      if (current === undefined) throw new PostError("delivery row is missing");
      if (current.state !== "pending" || current.jobId !== job.jobId)
        return undefined;
      if (reservation.status === "exhausted") {
        finish(
          postKey,
          current,
          {
            state: "dead",
            reason: current.deadlineAt === undefined ? "exhausted" : "expired",
          },
          reservation.count,
        );
        return undefined;
      }
      return current;
    });
    if (row === undefined) return { kind: "completed" };
    const route = routeFor(row.envelope);
    const evidence = await deliverPost({
      attempt: row,
      signal,
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
    return store.commit(() => {
      const current = readRow(postKey);
      if (current?.state !== "pending" || current.jobId !== job.jobId)
        return { kind: "completed" };
      const disposition = postDisposition(
        evidence,
        route,
        reservation.count,
        maxDeliveryAttempts,
        {
          deadlineExpired:
            current.deadlineAt !== undefined &&
            options.now() >= current.deadlineAt,
          retryUntilDeadline: current.deadlineAt !== undefined,
        },
      );
      if (disposition.state === "pending" || disposition.state === "keep")
        return { kind: "retry" };
      if (disposition.state === "delivered") {
        if (evidence.envelope === undefined)
          throw new PostError("missing delivered fact");
        if (disposition.cursor !== undefined)
          route.recordCursor?.(evidence.envelope, disposition.cursor);
        route.recordDelivered?.(evidence.envelope);
      } else if (
        disposition.reason === "refused" &&
        evidence.envelope !== undefined
      )
        route.recordRefused?.(evidence.envelope);
      finish(
        postKey,
        current,
        disposition,
        reservation.count,
        deliveryReceipt(evidence),
      );
      return { kind: "completed" };
    });
  };

  const retry: PostRuntime<R>["retry"] = async (input) => {
    const key = Schema.decodeUnknownSync(Key)(input);
    store.commit(() => {
      const row = readRow(key);
      if (row === undefined || row.state === "delivered") return;
      if (row.state === "pending") {
        options.transactional.expediteQueuedJob(row.jobId, options.now());
        return;
      }
      enqueueJob({
        state: "pending",
        envelope: row.envelope,
        jobId: ids.work.generate(),
        delivery: row.delivery,
        deadlineAt: row.deadlineAt,
      });
    });
    await options.scheduleWake(options.now());
  };

  const releaseNow: PostRuntime<R>["releaseNow"] = async (
    limit = 64,
    afterKey,
  ) => {
    const pageSize = Schema.decodeUnknownSync(PageLimit)(limit);
    const rows = store.list(pageSize, afterKey);
    const now = options.now();
    const released = store.commit(() => {
      let changed = false;
      for (const stored of rows) {
        const row = Schema.decodeUnknownSync(PostRowSchema)(stored.value);
        if (row.state === "pending")
          changed =
            options.transactional.expediteQueuedJob(row.jobId, now) || changed;
      }
      return changed;
    });
    if (released) await options.scheduleWake(now);
    return rows.length === pageSize ? rows.at(-1)?.key : undefined;
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
    retryPolicy,
    releaseNow,
    deadLetters,
    read: readRow,
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
