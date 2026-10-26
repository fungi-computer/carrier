import type {
  D1Database,
  D1PreparedStatement,
} from "@cloudflare/workers-types";
import {
  purgeD1Queues,
  prepareD1Admission,
  type D1Watchdog,
} from "@fungi.computer/watchdog/d1";
import type { PostEnvelope, PostRegistryRoute } from "./post.js";
import {
  admitPostSelect,
  jobSelect,
  openSqlPost,
  postSchema,
  preparePost,
  type Row,
  type SqlPostOptions,
  type SqlPostRuntime,
  type Statement,
} from "./internal/post-sql.js";

export type { SqlPostOptions, SqlPostRuntime } from "./internal/post-sql.js";
/** Trusted SELECT yielding post_key, envelope_json, queue_name, next_attempt_at and job_id. */
export type D1PostSource = Readonly<{
  /** Trusted native SELECT text with SQLite ?NNN bindings. */
  text: string;
  /** Scalar values bound by the native driver. */
  values: readonly (string | number | null)[];
}>;

/** Post admission statements; callers commit them with the originating D1 fact. */
export type D1Post = SqlPostRuntime &
  Readonly<{
    /** Prepare envelope and Watchdog admission without performing I/O. */
    send: (envelope: PostEnvelope) => readonly D1PreparedStatement[];
    /** Admit conditionally generated source facts entirely inside the producer batch. */
    sendFromSelect: (source: D1PostSource) => readonly D1PreparedStatement[];
  }>;
/** Carrier-owned D1 schema, applied by the deployment migration owner. */
export const postD1Schema: readonly string[] = postSchema(false);

/** Bind the Post registry to D1; route deadlines are rejected with PostError. */
export function createD1Post<R extends Record<string, PostRegistryRoute>>(
  options: SqlPostOptions<R> &
    Readonly<{
      database: D1Database;
      watchdog: D1Watchdog;
    }>,
): D1Post {
  // Only package-owned templates use $N; caller SELECTs remain native SQL.
  const prepare = (statement: Statement) =>
    options.database
      .prepare(statement.text.replace(/\$(\d+)/gu, "?$1"))
      .bind(...statement.values);
  const runtime = openSqlPost(
    {
      query: async (statement) => (await prepare(statement).all<Row>()).results,
      commit: async (statements, key, jobId) => {
        await options.database.batch([
          ...statements.map(prepare),
          options.watchdog.enqueueFromSelect(jobSelect(key, false, jobId)),
        ]);
      },
    },
    options,
  );
  return {
    ...runtime,
    send: (envelope) => {
      const prepared = preparePost(options, envelope);
      return [
        prepare(prepared.statement),
        options.watchdog.enqueueFromSelect(jobSelect(prepared.key, false)),
      ];
    },
    sendFromSelect: (source) =>
      prepareD1PostAdmission(options.database, source),
  };
}

/** Prepare conditional envelopes and their jobs inside the producer's D1 batch. */
export function prepareD1PostAdmission(
  database: D1Database,
  source: D1PostSource,
): readonly D1PreparedStatement[] {
  return [
    database.prepare(admitPostSelect(source).text).bind(...source.values),
    prepareD1Admission(database).enqueueFromSelect({
      text: `SELECT p.job_id,p.queue_name,'post' AS lane,0 AS priority,
        json_object('postKey',p.post_key) AS payload_json,CAST(NULL AS INTEGER) AS recovery_max
        FROM carrier_post_rows p JOIN (${source.text}) AS source ON p.post_key=source.post_key
        WHERE p.state='pending'`,
      values: source.values,
    }),
  ];
}

/** Native erasure statement; its values remain bound by the caller's D1 driver. */
export type D1PostErasureStatement = Readonly<{
  text: string;
  values: readonly (string | number | null)[];
}>;
/** Remove selected recipient queues, including every envelope and attempt, atomically.
 * The trusted SELECT yields queue_name from producer/envelope facts, not job state. */
export function purgeD1Post(
  source: D1PostErasureStatement,
): readonly D1PostErasureStatement[] {
  return [
    ...purgeD1Queues(source),
    {
      text: `DELETE FROM carrier_post_rows WHERE queue_name IN (SELECT queue_name FROM (${source.text}) AS erasure)`,
      values: source.values,
    },
  ];
}
