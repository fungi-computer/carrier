import { env } from "cloudflare:test";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import {
  createD1Watchdog,
  watchdogD1Schema,
} from "@fungi.computer/watchdog/d1";
import { createD1Post, postD1Schema, purgeD1Post } from "../src/post-d1.js";
import { definePostRoute } from "../src/post.js";
import { sqlPostLaws } from "./sql-post-laws.fixture.js";

async function open(retry) {
  const database = env.POST_DB;
  for (const table of ["producer_facts", "carrier_post_rows", "watchdog_jobs"])
    await database.prepare(`DROP TABLE IF EXISTS ${table}`).run();
  for (const sql of [...watchdogD1Schema, ...postD1Schema])
    await database.prepare(sql).run();
  await database
    .prepare(
      "CREATE TABLE producer_facts (id INTEGER PRIMARY KEY AUTOINCREMENT)",
    )
    .run();
  const clock = { now: 1_000 };
  const deliveries = [];
  const receiver = { run: () => Promise.resolve("accepted") };
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
    const watchdog = createD1Watchdog({
      database,
      now: () => clock.now,
      execute: (job, signal) => post.execute(job, signal),
      wake: { recompute: () => Promise.resolve() },
    });
    const options = {
      database,
      watchdog,
      routes,
      now: () => clock.now,
      scheduleWake: () => Promise.resolve(),
    };
    const post = createD1Post(
      retry === undefined ? options : { ...options, retry },
    );
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
      return createD1Watchdog(
        useDatabaseTime ? options : { ...options, now: () => clock.now },
      );
    },
    lease: async (jobId) =>
      Schema.decodeUnknownSync(Schema.Number)(
        await database
          .prepare("SELECT lease_until FROM watchdog_jobs WHERE job_id=?")
          .bind(jobId)
          .first("lease_until"),
      ),
    commitJob: async (job) => {
      await database.batch([
        database.prepare("INSERT INTO producer_facts DEFAULT VALUES"),
        watchdog.enqueue(job),
      ]);
    },
    post,
    reopen,
    clock,
    deliveries,
    receiver,
    close: () => Promise.resolve(),
    commit: async (envelope, fail = false) => {
      await database.batch([
        database.prepare("INSERT INTO producer_facts DEFAULT VALUES"),
        ...post.send(envelope),
        ...(fail
          ? [database.prepare("INSERT INTO producer_facts(id) VALUES (1)")]
          : []),
      ]);
    },
    purge: async (key, fail = false) => {
      const statements = purgeD1Post({
        text: "SELECT queue_name FROM carrier_post_rows WHERE post_key=?1",
        values: [key],
      }).map(({ text, values }) => database.prepare(text).bind(...values));
      statements.push(
        database.prepare("DELETE FROM producer_facts WHERE id=1"),
      );
      if (fail)
        statements.push(
          database.prepare("INSERT INTO producer_facts(id) VALUES (2)"),
        );
      await database.batch(statements);
    },
    counts: async () => {
      const result = [];
      for (const table of [
        "producer_facts",
        "carrier_post_rows",
        "watchdog_jobs",
      ]) {
        result.push(
          await database
            .prepare(`SELECT count(*) AS count FROM ${table}`)
            .first("count"),
        );
      }
      return result;
    },
  };
}
sqlPostLaws(open);

it("admits a conditionally generated ledger event and its job in the producer's one D1 batch", async () => {
  const f = await open();
  const db = env.POST_DB;
  await db
    .prepare(
      "CREATE TABLE ledger (id TEXT PRIMARY KEY,eligible INTEGER NOT NULL)",
    )
    .run();
  const source = {
    text: `SELECT id AS post_key,json_object('key',id,'kind','account','address',json_object('id','a'),
    'event',json_object('text','hello')) AS envelope_json,'["account","a"]' AS queue_name,
    1000 AS next_attempt_at,'ledger:'||id AS job_id FROM ledger WHERE id=?1 AND eligible=1`,
    values: ["conditional"],
  };
  const statements = () => [
    db.prepare("INSERT INTO ledger VALUES (?,1)").bind("conditional"),
    ...f.post.sendFromSelect(source),
  ];
  await expect(
    db.batch([
      ...statements(),
      db.prepare("INSERT INTO ledger VALUES (?,1)").bind("conditional"),
    ]),
  ).rejects.toThrow();
  expect(
    await db.prepare("SELECT count(*) AS count FROM ledger").first("count"),
  ).toBe(0);
  expect(await f.counts()).toEqual([0, 0, 0]);
  await db.batch(statements());
  expect(await f.counts()).toEqual([0, 1, 1]);
  await f.watchdog.tick();
  expect(f.deliveries).toEqual(["conditional"]);
});
