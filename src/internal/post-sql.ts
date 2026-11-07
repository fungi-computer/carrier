import * as Schema from "effect/Schema";
import { defineIdFamily } from "@fungi.computer/cairn";
import type {
  WatchdogRunningJob,
  WatchdogAttempt,
  WatchdogExecutionDecision,
  WatchdogRetryPolicy,
} from "@fungi.computer/watchdog";
import {
  PostEnvelopeSchema,
  PostError,
  type PostEnvelope,
  type PostRegistryRoute,
  type PostRow,
} from "../post.js";
import {
  deliverPost,
  postDisposition,
  deliveryReceipt,
} from "./post-execution.js";
import { maxDeliveryAttempts, postRetryPolicy } from "./post-policy.js";

export type Value = string | number | null;
export type Statement = Readonly<{ text: string; values: readonly Value[] }>;
export type Row = Record<string, Value>;
export type PostSql = Readonly<{
  query: (statement: Statement) => Promise<readonly Row[]>;
}>;
const ids = defineIdFamily({
  family: "post",
  version: 1,
  kinds: { work: { prefix: "post_work" } },
});
const EnvelopeJson = Schema.fromJsonString(PostEnvelopeSchema);
const ReceiptJson = Schema.fromJsonString(Schema.Json);
const Stored = Schema.Struct({
  post_key: Schema.String,
  envelope_json: Schema.String,
  queue_name: Schema.String,
  state: Schema.Literals(["pending", "delivered", "dead"]),
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  job_id: Schema.String,
  delivery_json: Schema.NullOr(Schema.String),
  receipt_json: Schema.NullOr(ReceiptJson),
  reason: Schema.NullOr(Schema.Literals(["refused", "exhausted"])),
});
const columns =
  "post_key,envelope_json,queue_name,state,attempts,job_id,delivery_json,reason,receipt_json";
/** SQL native Carrier runtime configuration; route decisions remain in Post. */
export type SqlPostOptions<R extends Record<string, PostRegistryRoute>> =
  Readonly<{
    /** The same registry used by SQLite Post; routes with deadlines are rejected. */
    routes: R;
    /** Optional bounded, redacted family diagnostic; never persist raw provider errors. */
    failureDetail?: (cause: unknown) => string;
    /** Family policy may tighten the twenty-attempt Post ceiling. */
    retry?: Readonly<{
      maxAttempts: number;
      baseDelayMs: number;
      maxDelayMs: number;
    }>;
    /** Clock shared with the database's Watchdog. */
    now: () => number;
  }>;
/** Async inspection and Watchdog executor shared by D1 and PostgreSQL. */
export type SqlPostRuntime = Readonly<{
  /** Resolve the delivery budget before Watchdog reserves an attempt. */
  retryPolicy: (job: WatchdogRunningJob) => Promise<WatchdogRetryPolicy>;
  /** Execute an already claimed Post job with its reserved delivery attempt. */
  execute: (
    job: WatchdogRunningJob,
    attempt: WatchdogAttempt | undefined,
    signal?: AbortSignal,
  ) => Promise<WatchdogExecutionDecision>;
  /** Read one producer receipt or pending envelope. */
  read: (key: string) => Promise<PostRow | undefined>;
}>;

export function postSchema(postgres: boolean): readonly string[] {
  return [
    `CREATE TABLE IF NOT EXISTS carrier_post_rows (
      sequence ${postgres ? "SERIAL PRIMARY KEY" : "INTEGER PRIMARY KEY AUTOINCREMENT"},
      post_key TEXT NOT NULL UNIQUE CHECK(length(post_key) BETWEEN 1 AND 256),
      envelope_json TEXT NOT NULL,queue_name TEXT NOT NULL,
      state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','delivered','dead')),
      attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
      job_id TEXT NOT NULL,delivery_json TEXT,
      reason TEXT CHECK(reason IN ('refused','exhausted')),
      receipt_json TEXT,
      settled_at ${postgres ? "DOUBLE PRECISION" : "INTEGER"},
      last_error TEXT CHECK(last_error IS NULL OR length(last_error)<=2048),
      attempt_until ${postgres ? "DOUBLE PRECISION" : "INTEGER"}
    )`,
    "CREATE INDEX IF NOT EXISTS idx_post_address ON carrier_post_rows(queue_name,state,sequence)",
  ];
}

export function admitPostSelect(source: Statement): Statement {
  return {
    text: `INSERT INTO carrier_post_rows(post_key,envelope_json,queue_name,job_id)
    SELECT post_key,envelope_json,queue_name,job_id FROM (${source.text}) AS admission WHERE true
    ON CONFLICT(post_key) DO UPDATE SET post_key=NULL
    WHERE carrier_post_rows.envelope_json<>excluded.envelope_json OR carrier_post_rows.queue_name<>excluded.queue_name`,
    values: source.values,
  };
}

export function jobSelect(
  key: string,
  postgres: boolean,
  expectedJobId?: string,
): Statement {
  const bind = postgres ? "$" : "?";
  const payload = postgres
    ? "json_build_object('postKey',post_key)::text"
    : "json_object('postKey',post_key)";
  return {
    text: `SELECT job_id,queue_name,'post' AS lane,0 AS priority,${payload} AS payload_json,
      CAST(NULL AS INTEGER) AS recovery_max,CAST(NULL AS INTEGER) AS recovery_window_ms,CAST(0 AS INTEGER) AS not_before FROM carrier_post_rows
      WHERE post_key=${bind}1 AND state='pending' ${expectedJobId === undefined ? "" : `AND job_id=${bind}2`}`,
    values: expectedJobId === undefined ? [key] : [key, expectedJobId],
  };
}

export function preparePost<R extends Record<string, PostRegistryRoute>>(
  options: SqlPostOptions<R>,
  input: PostEnvelope,
) {
  const incoming = Schema.decodeUnknownSync(PostEnvelopeSchema)(input);
  const route = options.routes[incoming.kind];
  if (route === undefined) throw new PostError("unknown address kind");
  const envelope = route.encodeEnvelope(incoming);
  const queue = JSON.stringify([route.addressKind, route.nameOf(envelope)]);
  const jobId = ids.work.generate();
  return {
    key: envelope.key,
    statement: admitPostSelect({
      text: `SELECT CAST($1 AS TEXT) AS post_key,CAST($2 AS TEXT) AS envelope_json,
      CAST($3 AS TEXT) AS queue_name,CAST($4 AS TEXT) AS job_id`,
      values: [
        envelope.key,
        Schema.encodeSync(EnvelopeJson)(envelope),
        queue,
        jobId,
      ],
    }),
  };
}

function parseStored(raw: Row) {
  const stored = Schema.decodeUnknownSync(Stored)(raw);
  const envelope = Schema.decodeUnknownSync(EnvelopeJson)(stored.envelope_json);
  const delivery =
    stored.delivery_json === null
      ? undefined
      : Schema.decodeUnknownSync(EnvelopeJson)(stored.delivery_json);
  return { ...stored, envelope, delivery };
}

function publicRow(stored: ReturnType<typeof parseStored>): PostRow {
  const receipt = stored.receipt_json;
  if (stored.state === "delivered")
    return receipt === null
      ? { state: "delivered" }
      : { state: "delivered", receipt };
  if (stored.state === "dead") {
    if (stored.reason === null)
      throw new PostError("dead letter has no reason");
    const dead = {
      state: "dead" as const,
      envelope: stored.envelope,
      attempts: stored.attempts,
      reason: stored.reason,
      delivery: stored.delivery,
    };
    return receipt === null ? dead : { ...dead, receipt };
  }
  return {
    state: "pending",
    envelope: stored.envelope,
    jobId: stored.job_id,
    delivery: stored.delivery,
  };
}

export function openSqlPost<R extends Record<string, PostRegistryRoute>>(
  database: PostSql,
  options: SqlPostOptions<R>,
): SqlPostRuntime {
  if (
    Object.values(options.routes).some(
      (route) => route.deadlineMs !== undefined,
    )
  )
    throw new PostError(
      "relational Post adapters do not support route deadlines",
      "relational-deadline-unsupported",
    );
  const policy = Schema.decodeUnknownSync(
    Schema.Struct({
      maxAttempts: Schema.Int.check(
        Schema.isBetween({ minimum: 1, maximum: maxDeliveryAttempts }),
      ),
      baseDelayMs: Schema.Int.check(Schema.isGreaterThan(0)),
      maxDelayMs: Schema.Int.check(Schema.isGreaterThan(0)),
    }),
  )(
    options.retry ?? {
      maxAttempts: maxDeliveryAttempts,
      baseDelayMs: postRetryPolicy.baseMs,
      maxDelayMs: postRetryPolicy.maxMs,
    },
  );
  for (const route of Object.values(options.routes)) {
    // Synchronous owner hooks cannot participate in an asynchronous transaction.
    if (
      route.recordDelivered !== undefined ||
      route.recordRefused !== undefined ||
      route.recordCursor !== undefined
    )
      throw new PostError(
        "SQL Post requires transaction-bound receipt projections",
      );
  }

  const query = (text: string, values: readonly Value[] = []) =>
    database.query({ text, values });
  const readStored = async (key: string) => {
    const raw = (
      await query(
        `SELECT ${columns} FROM carrier_post_rows WHERE post_key=$1`,
        [key],
      )
    )[0];
    return raw === undefined ? undefined : parseStored(raw);
  };
  const postKeyOf = (job: WatchdogRunningJob) =>
    Schema.decodeUnknownSync(Schema.Struct({ postKey: Schema.String }))(
      job.payload,
    ).postKey;
  // Watchdog owns the attempt generation and lease. The retained ten-second
  // receiver fence rejects delayed writes from a timed-out preparation/call.
  const claimed = `post_key=$1 AND job_id=$2 AND state='pending' AND EXISTS (
    SELECT 1 FROM watchdog_jobs w WHERE w.job_id=carrier_post_rows.job_id
    AND w.state='running' AND w.attempt_count=$3 AND w.lease_until>$4)`;
  return {
    read: async (key) => {
      const stored = await readStored(key);
      return stored === undefined ? undefined : publicRow(stored);
    },
    retryPolicy: async (job) => {
      const row = await readStored(postKeyOf(job));
      if (row === undefined) throw new PostError("delivery row is missing");
      return {
        baseMs: policy.baseDelayMs,
        maxMs: policy.maxDelayMs,
        limit: {
          maxAttempts:
            row.state === "pending" && row.job_id === job.jobId
              ? policy.maxAttempts
              : 0,
        },
      };
    },
    execute: async (job, reservation, signal) => {
      if (reservation === undefined)
        throw new PostError("delivery attempt is missing");
      const postKey = postKeyOf(job);
      const current = await readStored(postKey);
      if (current === undefined) throw new PostError("delivery row is missing");
      if (current.state !== "pending" || current.job_id !== job.jobId)
        return { kind: "completed" };
      const bindings = () => [
        postKey,
        job.jobId,
        reservation.count,
        options.now(),
      ];
      if (reservation.status === "exhausted") {
        const settled = await query(
          `UPDATE carrier_post_rows SET state='dead',reason='exhausted',attempts=$3,attempt_until=NULL,settled_at=$4 WHERE ${claimed} RETURNING post_key`,
          bindings(),
        );
        if (settled.length !== 1)
          throw new PostError("receipt was not committed");
        return { kind: "completed" };
      }
      const raw = (
        await query(
          `UPDATE carrier_post_rows SET attempt_until=$5 WHERE ${claimed} RETURNING ${columns}`,
          [...bindings(), options.now() + 10_000],
        )
      )[0];
      if (raw === undefined)
        throw new PostError("attempt claim is unavailable");
      const attempt = publicRow(parseStored(raw));
      if (attempt.state !== "pending")
        throw new PostError("attempt is not pending");
      const route = options.routes[attempt.envelope.kind];
      if (route === undefined) throw new PostError("unknown address kind");
      const evidence = await deliverPost({
        signal,
        attempt,
        route,
        savePrepared: async (envelope) => {
          const saved = await query(
            `UPDATE carrier_post_rows SET delivery_json=$5 WHERE ${claimed} AND attempt_until>$4 RETURNING post_key`,
            [...bindings(), Schema.encodeSync(EnvelopeJson)(envelope)],
          );
          if (saved.length === 0) throw new PostError("preparation superseded");
        },
      });
      const disposition = postDisposition(
        evidence,
        route,
        reservation.count,
        policy.maxAttempts,
      );
      if (disposition.state === "keep") return { kind: "retry" };
      const receipt = deliveryReceipt(evidence);
      const settled = await query(
        `UPDATE carrier_post_rows SET state=$5,reason=$6,attempts=CASE WHEN $5='pending' THEN 0 ELSE $3 END,attempt_until=NULL,last_error=$7,receipt_json=$8,settled_at=CASE WHEN $5='pending' THEN NULL ELSE CAST($4 AS DOUBLE PRECISION) END
        WHERE ${claimed} AND attempt_until>$4 RETURNING post_key`,
        [
          ...bindings(),
          disposition.state,
          disposition.state === "dead" ? disposition.reason : null,
          evidence.result === "unavailable" &&
          options.failureDetail !== undefined
            ? options.failureDetail(evidence.failure).slice(0, 2048)
            : null,
          receipt === undefined
            ? null
            : Schema.encodeSync(ReceiptJson)(receipt),
        ],
      );
      if (settled.length !== 1)
        throw new PostError("receipt was not committed");
      return { kind: disposition.state === "pending" ? "retry" : "completed" };
    },
  };
}
