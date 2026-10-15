import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { Pool } from "pg";
import { beforeAll, afterAll, it, expect, vi } from "vitest";
import * as Schema from "effect/Schema";
import {
  createPostgresWatchdog,
  watchdogPostgresSchema,
} from "@fungi.computer/watchdog/postgres";
import {
  createPostgresPost,
  purgePostgresPost,
  postPostgresSchema,
} from "../src/post-postgres.js";
import { definePostRoute } from "../src/post.js";
import { sqlPostLaws, type Fixture } from "./sql-post-laws.fixture.js";

const pg = process.env.BOTANICAL_TEST_POSTGRES_ROOT ?? "/";
const bin = resolve(pg, "usr/lib/postgresql/16/bin");
let directory: string | undefined;
let socketDirectory: string | undefined;
let pool: Pool | undefined;

function isPostgresAdminShutdown(
  error: Error,
): error is Error & { code: "57P01" } {
  return "code" in error && error.code === "57P01";
}

beforeAll(() => {
  const scratch = resolve("../../.botanical/carrier-post");
  mkdirSync(scratch, { recursive: true });
  directory = mkdtempSync(`${scratch}/pg-`);
  execFileSync(
    `${bin}/initdb`,
    [
      "-D",
      `${directory}/data`,
      "-L",
      resolve(pg, "usr/share/postgresql/16"),
      "-U",
      "postgres",
      "--auth=trust",
    ],
    { stdio: "ignore", timeout: 20_000 },
  );
  socketDirectory = mkdtempSync(resolve(tmpdir(), "carrier-post-"));
  execFileSync(
    `${bin}/pg_ctl`,
    [
      "-D",
      `${directory}/data`,
      "-l",
      `${directory}/server.log`,
      "-o",
      `-h '' -k ${socketDirectory}`,
      "-w",
      "start",
    ],
    { stdio: "ignore", timeout: 20_000 },
  );
  pool = new Pool({
    host: socketDirectory,
    user: "postgres",
    database: "postgres",
  });
}, 30_000);
afterAll(async () => {
  if (directory === undefined) return;
  let stoppingPostgres = false;
  let unexpectedPoolError: Error | undefined;
  const onPoolError = (error: Error): void => {
    if (stoppingPostgres && isPostgresAdminShutdown(error)) return;
    unexpectedPoolError ??= error;
  };
  pool?.on("error", onPoolError);
  {
    try {
      try {
        // Drain checked-out and idle clients before stopping the server.
        await pool?.end();
      } finally {
        if (existsSync(`${directory}/data/postmaster.pid`)) {
          stoppingPostgres = true;
          try {
            execFileSync(
              `${bin}/pg_ctl`,
              ["-D", `${directory}/data`, "-m", "fast", "-w", "stop"],
              { stdio: "ignore", timeout: 20_000 },
            );
          } finally {
            stoppingPostgres = false;
          }
        }
      }
    } finally {
      pool?.off("error", onPoolError);
      rmSync(directory, { recursive: true, force: true });
      if (socketDirectory !== undefined)
        rmSync(socketDirectory, { recursive: true, force: true });
    }
  }
  if (unexpectedPoolError !== undefined) throw unexpectedPoolError;
});

const openFixture = async (): Promise<Fixture> => {
  const database = pool;
  if (database === undefined)
    throw new Error("Postgres fixture was not started");
  await database.query(
    "DROP TABLE IF EXISTS producer_facts,carrier_post_rows,watchdog_jobs CASCADE",
  );
  for (const sql of [...watchdogPostgresSchema, ...postPostgresSchema])
    await database.query(sql);
  await database.query("CREATE TABLE producer_facts (id SERIAL PRIMARY KEY)");
  const clock = { now: Date.UTC(2026, 9, 3) };
  const deliveries: string[] = [];
  const receiver = {
    run: (): Promise<"accepted"> => Promise.resolve("accepted"),
  };
  const routes = {
    account: definePostRoute({
      addressKind: "account",
      address: Schema.Struct({ id: Schema.String }),
      event: Schema.Struct({ text: Schema.String }),
      name: ({ id }) => id,
      namespace: {
        getByName: () => ({
          deliver: async (envelope) => {
            deliveries.push(envelope.key);
            return receiver.run();
          },
        }),
      },
    }),
  };
  const reopen = () => {
    const watchdog = createPostgresWatchdog({
      database,
      now: () => clock.now,
      execute: (job, signal) => post.execute(job, signal),
      wake: { recompute: () => Promise.resolve() },
    });
    const post = createPostgresPost({
      database,
      watchdog,
      routes,
      now: () => clock.now,
      scheduleWake: () => Promise.resolve(),
    });
    return { watchdog, post };
  };
  const { watchdog, post } = reopen();
  return {
    watchdog,
    worker: (execute, useDatabaseTime = false) => {
      const options = {
        database,
        execute,
        wake: { recompute: () => Promise.resolve() },
      };
      return createPostgresWatchdog(
        useDatabaseTime ? options : { ...options, now: () => clock.now },
      );
    },
    lease: async (jobId) =>
      Schema.decodeUnknownSync(Schema.Struct({ lease_until: Schema.Number }))(
        (
          await database.query(
            "SELECT lease_until FROM watchdog_jobs WHERE job_id=$1",
            [jobId],
          )
        ).rows[0],
      ).lease_until,
    commitJob: async (job) => {
      const client = await database.connect();
      try {
        await client.query("BEGIN");
        await client.query("INSERT INTO producer_facts DEFAULT VALUES");
        await watchdog.enqueue(client, job);
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    },
    post,
    reopen,
    clock,
    deliveries,
    receiver,
    close: () => Promise.resolve(),
    commit: async (envelope, fail = false) => {
      const client = await database.connect();
      try {
        await client.query("BEGIN");
        await client.query("INSERT INTO producer_facts DEFAULT VALUES");
        await post.send(client, envelope);
        if (fail) await client.query("SELECT 1/0");
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    },
    purge: async (key, fail = false) => {
      const client = await database.connect();
      try {
        await client.query("BEGIN");
        for (const { text, values } of purgePostgresPost({
          text: "SELECT queue_name FROM carrier_post_rows WHERE post_key=$1",
          values: [key],
        }))
          await client.query(text, [...values]);
        await client.query("DELETE FROM producer_facts WHERE id=1");
        if (fail)
          await client.query("INSERT INTO producer_facts(id) VALUES (2)");
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    },
    counts: async () => {
      const result: number[] = [];
      for (const table of [
        "producer_facts",
        "carrier_post_rows",
        "watchdog_jobs",
      ]) {
        const row = Schema.decodeUnknownSync(
          Schema.Struct({ count: Schema.Number }),
        )(
          (await database.query(`SELECT count(*)::int AS count FROM ${table}`))
            .rows[0],
        );
        result.push(row.count);
      }
      return result;
    },
  };
};
sqlPostLaws(openFixture);

it("serializes erasure with a retry that has updated its envelope but not inserted its job", async () => {
  const f = await openFixture();
  if (pool === undefined) throw new Error("Postgres fixture was not started");
  const database = pool;
  await f.commit({
    key: "racing",
    kind: "account",
    address: { id: "a" },
    event: { text: "hello" },
  });
  f.receiver.run = () => Promise.reject(new Error("retry"));
  await f.watchdog.tick();
  const pending = await f.post.read("racing");
  if (pending?.state !== "pending") throw new Error("Expected retry");
  f.clock.now = pending.nextAttemptAt + 1;
  const barrier = await database.connect();
  await barrier.query("SELECT pg_advisory_lock(918237)");
  await database.query(`CREATE OR REPLACE FUNCTION hold_post_retry() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN PERFORM pg_advisory_xact_lock(918237); RETURN NEW; END; $$;
    CREATE TRIGGER hold_post_retry BEFORE INSERT ON watchdog_jobs FOR EACH ROW EXECUTE FUNCTION hold_post_retry()`);
  const retry = f.post.enqueueDue();
  let erasure: Promise<void> | undefined;
  try {
    // The real INSERT waits after Post has locked and updated the envelope.
    await vi.waitFor(
      async () => {
        const waiting = await database.query(
          "SELECT pid FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE 'INSERT INTO watchdog_jobs%'",
        );
        expect(waiting.rowCount).toBe(1);
      },
      { timeout: 3000 },
    );
    erasure = f.purge("racing");
    await vi.waitFor(
      async () => {
        const waiting = await database.query(
          "SELECT pid FROM pg_stat_activity WHERE wait_event='transactionid' AND query LIKE 'UPDATE carrier_post_rows SET post_key=post_key%'",
        );
        expect(waiting.rowCount).toBe(1);
      },
      { timeout: 3000 },
    );
  } finally {
    await barrier.query("SELECT pg_advisory_unlock(918237)");
    barrier.release();
    await retry;
    await erasure;
    await database.query(
      "DROP TRIGGER hold_post_retry ON watchdog_jobs; DROP FUNCTION hold_post_retry()",
    );
  }
  expect(await f.counts()).toEqual([0, 0, 0]);
});
