import * as Schema from "effect/Schema";
import type { Pool, PoolClient } from "pg";
import {
  purgePostgresQueues,
  type PostgresWatchdog,
} from "@fungi.computer/watchdog/postgres";
import type { PostEnvelope, PostRegistryRoute } from "./post.js";
import {
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
/** Post on PostgreSQL; the producer retains ownership of BEGIN and COMMIT. */
export type PostgresPost = SqlPostRuntime &
  Readonly<{
    /** Persist the envelope and job using the already checked-out transaction client. */
    send: (
      client: Pick<PoolClient, "query">,
      envelope: PostEnvelope,
    ) => Promise<void>;
  }>;
/** Carrier-owned PostgreSQL schema, applied by the deployment migration owner. */
export const postPostgresSchema: readonly string[] = postSchema(true);
const Job = Schema.Struct({
  job_id: Schema.String,
  queue_name: Schema.String,
  lane: Schema.String,
  priority: Schema.Number,
  payload_json: Schema.fromJsonString(Schema.Unknown),
});

/** Bind the Post registry to a PostgreSQL owner and the shared delivery engine. */
export function createPostgresPost<R extends Record<string, PostRegistryRoute>>(
  options: SqlPostOptions<R> &
    Readonly<{
      database: Pick<Pool, "query" | "connect">;
      watchdog: PostgresWatchdog;
    }>,
): PostgresPost {
  const enqueue = async (
    client: Pick<PoolClient, "query">,
    source: Statement,
  ) => {
    const native = source;
    const rows = (await client.query<Row>(native.text, [...native.values]))
      .rows;
    for (const raw of rows) {
      const job = Schema.decodeUnknownSync(Job)(raw);
      await options.watchdog.enqueue(client, {
        jobId: job.job_id,
        queue: job.queue_name,
        lane: job.lane,
        priority: job.priority,
        payload: job.payload_json,
        state: "queued",
      });
    }
  };
  const runtime = openSqlPost(
    {
      query: async (statement) =>
        (
          await options.database.query<Row>(statement.text, [
            ...statement.values,
          ])
        ).rows,
      commit: async (statements, key, jobId) => {
        const client = await options.database.connect();
        try {
          await client.query("BEGIN");
          for (const statement of statements)
            await client.query(statement.text, [...statement.values]);
          await enqueue(client, jobSelect(key, true, jobId));
          await client.query("COMMIT");
        } catch (error) {
          await client.query("ROLLBACK");
          throw error;
        } finally {
          client.release();
        }
      },
    },
    options,
  );
  return {
    ...runtime,
    send: async (client, envelope) => {
      const prepared = preparePost(options, envelope);
      await client.query(prepared.statement.text, [
        ...prepared.statement.values,
      ]);
      await enqueue(client, jobSelect(prepared.key, true));
    },
  };
}

/** Parameterized native erasure; the producer controls BEGIN and COMMIT. */
export type PostgresPostErasureStatement = Readonly<{
  text: string;
  values: readonly (string | number | null)[];
}>;
/** Purge selected recipient queues, including all envelopes and attempts.
 * The trusted SELECT yields queue_name from producer/envelope facts, not job state. */
export function purgePostgresPost(
  source: PostgresPostErasureStatement,
): readonly PostgresPostErasureStatement[] {
  return [
    // Serialize erasure with send/retry before taking the jobs' deletion snapshot.
    {
      text: `UPDATE carrier_post_rows SET post_key=post_key WHERE queue_name IN (SELECT queue_name FROM (${source.text}) AS erasure)`,
      values: source.values,
    },
    ...purgePostgresQueues(source),
    {
      text: `DELETE FROM carrier_post_rows WHERE queue_name IN (SELECT queue_name FROM (${source.text}) AS erasure)`,
      values: source.values,
    },
  ];
}
