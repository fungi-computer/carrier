import * as Schema from "effect/Schema";
import { backoffDelay } from "@fungi.computer/almanac";
import { defineIdFamily } from "@fungi.computer/cairn";
import type { WatchdogRunningJob } from "@fungi.computer/watchdog";
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
import { maxDeliveryAttempts, postRetryPolicy } from "../post-outbox.js";

export type Value = string | number | null;
export type Statement = Readonly<{ text: string; values: readonly Value[] }>;
export type Row = Record<string, Value>;
export type PostSql = Readonly<{
  query: (statement: Statement) => Promise<readonly Row[]>;
  commit: (
    statements: readonly Statement[],
    key: string,
    jobId: string,
  ) => Promise<void>;
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
  next_attempt_at: Schema.Number,
  job_id: Schema.String,
  delivery_json: Schema.NullOr(Schema.String),
  receipt_json: Schema.NullOr(ReceiptJson),
  reason: Schema.NullOr(Schema.Literals(["refused", "exhausted"])),
});
const columns =
  "post_key,envelope_json,queue_name,state,attempts,next_attempt_at,job_id,delivery_json,reason,receipt_json";
const JobReference = Schema.Struct({
  source_job: Schema.NullOr(Schema.String),
});
const pageSize = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(1),
  Schema.isLessThanOrEqualTo(64),
);

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
    /** Arm a durable wake after committed progress. */
    scheduleWake: (at: number) => Promise<void>;
  }>;
/** Async inspection and Watchdog executor shared by D1 and PostgreSQL. */
export type SqlPostRuntime = Readonly<{
  /** Execute an already claimed Post job through the registered receiver. */
  execute: (
    job: WatchdogRunningJob,
    signal?: AbortSignal,
  ) => Promise<Readonly<{ kind: "completed" }>>;
  /** Recover a bounded page of settled attempts into new Watchdog jobs. */
  enqueueDue: (
    limit?: number,
    afterKey?: string,
  ) => Promise<string | undefined>;
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
      next_attempt_at ${postgres ? "DOUBLE PRECISION" : "INTEGER"} NOT NULL,
      job_id TEXT NOT NULL,delivery_json TEXT,
      reason TEXT CHECK(reason IN ('refused','exhausted')),
      receipt_json TEXT,
      settled_at ${postgres ? "DOUBLE PRECISION" : "INTEGER"},
      last_error TEXT CHECK(last_error IS NULL OR length(last_error)<=2048),
      attempt_until ${postgres ? "DOUBLE PRECISION" : "INTEGER"}
    )`,
    "CREATE INDEX IF NOT EXISTS idx_post_due ON carrier_post_rows(state,next_attempt_at,post_key)",
    "CREATE INDEX IF NOT EXISTS idx_post_address ON carrier_post_rows(queue_name,state,sequence)",
  ];
}

export function admitPostSelect(source: Statement): Statement {
  return {
    text: `INSERT INTO carrier_post_rows(post_key,envelope_json,queue_name,next_attempt_at,job_id)
    SELECT post_key,envelope_json,queue_name,next_attempt_at,job_id FROM (${source.text}) AS admission WHERE true
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
      CAST(NULL AS INTEGER) AS recovery_max FROM carrier_post_rows
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
      CAST($3 AS TEXT) AS queue_name,CAST($4 AS DOUBLE PRECISION) AS next_attempt_at,CAST($5 AS TEXT) AS job_id`,
      values: [
        envelope.key,
        Schema.encodeSync(EnvelopeJson)(envelope),
        queue,
        options.now(),
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
    attempts: stored.attempts,
    nextAttemptAt: stored.next_attempt_at,
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
      baseDelayMs: backoffDelay(1, postRetryPolicy),
      maxDelayMs: backoffDelay(maxDeliveryAttempts, postRetryPolicy),
    },
  );
  const delay = (attempt: number) =>
    backoffDelay(attempt, {
      baseMs: policy.baseDelayMs,
      maxMs: policy.maxDelayMs,
    });
  const retrySql = Array.from(
    { length: policy.maxAttempts },
    (_, index) => `WHEN ${String(index)} THEN ${String(delay(index + 1))}`,
  ).join(" ");
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
  return {
    read: async (key) => {
      const raw = (
        await query(
          `SELECT ${columns} FROM carrier_post_rows WHERE post_key=$1`,
          [key],
        )
      )[0];
      return raw === undefined ? undefined : publicRow(parseStored(raw));
    },
    execute: async (job, signal) => {
      const { postKey } = Schema.decodeUnknownSync(
        Schema.Struct({ postKey: Schema.String }),
      )(job.payload);
      const raw = (
        await query(
          `UPDATE carrier_post_rows SET attempts=attempts+1,
        next_attempt_at=CAST($1 AS DOUBLE PRECISION)+CASE attempts ${retrySql} ELSE ${String(delay(policy.maxAttempts))} END,attempt_until=$2 WHERE post_key=$3 AND job_id=$4 AND state='pending'
        AND attempts<$5 AND NOT EXISTS (SELECT 1 FROM carrier_post_rows AS earlier
          WHERE earlier.queue_name=carrier_post_rows.queue_name AND earlier.state='pending'
          AND earlier.sequence<carrier_post_rows.sequence) RETURNING ${columns}`,
          [
            options.now(),
            options.now() + 10_000,
            postKey,
            job.jobId,
            policy.maxAttempts,
          ],
        )
      )[0];
      if (raw === undefined) return { kind: "completed" };
      const stored = parseStored(raw);
      const attempt = publicRow(stored);
      if (attempt.state !== "pending")
        throw new PostError("attempt is not pending");
      const route = options.routes[attempt.envelope.kind];
      if (route === undefined) throw new PostError("unknown address kind");
      await options.scheduleWake(attempt.nextAttemptAt);
      const evidence = await deliverPost({
        signal,
        attempt,
        route,
        savePrepared: async (envelope) => {
          const saved = await query(
            `UPDATE carrier_post_rows SET delivery_json=$1
          WHERE post_key=$2 AND job_id=$3 AND attempts=$4 AND state='pending' AND attempt_until>$5 RETURNING post_key`,
            [
              Schema.encodeSync(EnvelopeJson)(envelope),
              postKey,
              job.jobId,
              attempt.attempts,
              options.now(),
            ],
          );
          if (saved.length === 0) throw new PostError("preparation superseded");
        },
      });
      const disposition = postDisposition(
        evidence,
        route,
        attempt.attempts,
        policy.maxAttempts,
      );
      if (disposition.state === "keep") return { kind: "completed" };
      const next = options.now() + delay(attempt.attempts);
      const receipt = deliveryReceipt(evidence);
      await query(
        `UPDATE carrier_post_rows SET state=$1,reason=$2,next_attempt_at=$3,attempt_until=NULL,last_error=$8,receipt_json=$9,settled_at=CASE WHEN $1='pending' THEN NULL ELSE CAST($10 AS DOUBLE PRECISION) END
        WHERE post_key=$4 AND job_id=$5 AND attempts=$6 AND state='pending' AND attempt_until>$7`,
        [
          disposition.state,
          disposition.state === "dead" ? disposition.reason : null,
          next,
          postKey,
          job.jobId,
          attempt.attempts,
          options.now(),
          evidence.result === "unavailable" &&
          options.failureDetail !== undefined
            ? options.failureDetail(evidence.failure).slice(0, 2048)
            : null,
          receipt === undefined
            ? null
            : Schema.encodeSync(ReceiptJson)(receipt),
          options.now(),
        ],
      );
      await options.scheduleWake(
        disposition.state === "pending" ? next : options.now(),
      );
      return { kind: "completed" };
    },
    enqueueDue: async (limit = 64, afterKey = "") => {
      Schema.decodeUnknownSync(pageSize)(limit);
      const rows = await query(
        `SELECT ${columns
          .split(",")
          .map((column) => `p.${column}`)
          .join(",")},w.job_id AS source_job
        FROM carrier_post_rows p LEFT JOIN watchdog_jobs w ON p.job_id=w.job_id
        WHERE p.state='pending' AND (w.job_id IS NULL OR (w.state='settled' AND p.next_attempt_at<=$1)) AND p.post_key>$2
        ORDER BY p.post_key LIMIT $3`,
        [options.now(), afterKey, limit],
      );
      let cursor: string | undefined;
      for (const raw of rows) {
        if (Schema.decodeUnknownSync(JobReference)(raw).source_job === null)
          throw new PostError("delivery job is missing");
        const row = parseStored(raw);
        cursor = row.post_key;
        const nextJobId = ids.work.generate();
        await database.commit(
          [
            {
              text: `UPDATE carrier_post_rows SET job_id=$1,state=CASE WHEN attempts>=$2 THEN 'dead' ELSE 'pending' END,
            reason=CASE WHEN attempts>=$2 THEN 'exhausted' ELSE NULL END,
            settled_at=CASE WHEN attempts>=$2 THEN CAST($5 AS DOUBLE PRECISION) ELSE NULL END
            WHERE post_key=$3 AND job_id=$4 AND state='pending' AND next_attempt_at<=$5`,
              values: [
                nextJobId,
                policy.maxAttempts,
                row.post_key,
                row.job_id,
                options.now(),
              ],
            },
          ],
          row.post_key,
          nextJobId,
        );
      }
      return rows.length === limit ? cursor : undefined;
    },
  };
}
