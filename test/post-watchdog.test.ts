import type { WatchdogRuntime } from "@fungi.computer/watchdog";
import * as Schema from "effect/Schema";
import { afterEach, expect, it, vi } from "vitest";
import { Watchdog, type WatchdogExecutor } from "@fungi.computer/watchdog";

import { admitPost, createPost, definePostRoute } from "../src/post.js";
import { createSqliteOwnerFixture } from "../../watchdog/test/sqlite-owner.fixture.js";

afterEach(() => vi.restoreAllMocks());

const Event = Schema.Struct({ text: Schema.String });

it("joins producer SQLite atomically and recovers a failed receipt without applying twice", async () => {
  const producer = createSqliteOwnerFixture();
  const receiver = createSqliteOwnerFixture();
  try {
    const sql = producer.owner.sql;
    sql.exec("CREATE TABLE producer_facts (id TEXT PRIMARY KEY)");
    receiver.owner.sql.exec(
      "CREATE TABLE admitted_events (key TEXT PRIMARY KEY, text TEXT NOT NULL)",
    );
    receiver.owner.sql.exec(
      "CREATE TABLE admission_receipts (key TEXT PRIMARY KEY)",
    );
    let failCompletion = false;
    let loseFirstReceipt = true;
    let calls = 0;
    let now = 100;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const wakes: number[] = [];
    const postOwner = {
      ...producer.owner,
      transactionSync: <A>(mutation: () => A): A => {
        if (failCompletion) {
          failCompletion = false;
          throw new Error("receipt write unavailable");
        }
        return producer.owner.transactionSync(mutation);
      },
    };
    const routes = {
      team: definePostRoute({
        addressKind: "team",
        address: Schema.Struct({ teamId: Schema.String }),
        event: Event,
        name: (address) => address.teamId,
        namespace: {
          getByName: (name) => ({
            deliver: (envelope) => {
              expect(name).toBe("t1");
              calls += 1;
              const result = receiver.owner.transactionSync(() =>
                admitPost({
                  key: envelope.key,
                  event: envelope.event,
                  schema: Event,
                  hasReceipt: (key) =>
                    receiver.owner.sql
                      .exec(
                        "SELECT key FROM admission_receipts WHERE key=?",
                        key,
                      )
                      .toArray().length > 0,
                  apply: (event) => {
                    receiver.owner.sql.exec(
                      "INSERT INTO admitted_events VALUES (?,?)",
                      envelope.key,
                      event.text,
                    );
                    return "accepted";
                  },
                  putReceipt: (key) => {
                    receiver.owner.sql.exec(
                      "INSERT INTO admission_receipts VALUES (?)",
                      key,
                    );
                  },
                }),
              );
              failCompletion = loseFirstReceipt;
              loseFirstReceipt = false;
              return Promise.resolve(result);
            },
          }),
        },
      }),
    };
    let execute: WatchdogExecutor = () =>
      Promise.reject(new Error("Post is not open"));
    const watchdog: WatchdogRuntime = await Watchdog.make({
      owner: producer.owner,
      retryPolicy: (job) => post.retryPolicy(job),
      execute: (job, signal, attempt) => execute(job, signal, attempt),
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
    const options = {
      routes,
      owner: postOwner,
      transactional: watchdog.transactional,
      now: () => now,
      scheduleWake: (at: number) => {
        wakes.push(at);
        return Promise.resolve();
      },
    };
    const post = createPost(options);
    execute = (job, signal, attempt) => post.execute(job, attempt, signal);
    const envelope = {
      key: "fact:one",
      kind: "team",
      address: { teamId: "t1" },
      event: { text: "hello" },
    } as const;

    expect(() =>
      producer.owner.transactionSync(() => {
        sql.exec("INSERT INTO producer_facts VALUES ('one')");
        post.send(envelope);
        throw new Error("producer rollback");
      }),
    ).toThrow("producer rollback");
    expect(sql.exec("SELECT id FROM producer_facts").toArray()).toEqual([]);
    expect(post.read(envelope.key)).toBeUndefined();
    expect(await watchdog.readQueueHead('["team","t1"]')).toEqual({
      state: "idle",
    });

    producer.owner.transactionSync(() => {
      sql.exec("INSERT INTO producer_facts VALUES ('one')");
      post.send(envelope);
    });
    expect((await watchdog.tick()).status).toBe("requeued");
    expect(post.read(envelope.key)).toMatchObject({
      state: "pending",
    });
    expect(calls).toBe(1);
    expect(wakes).toEqual([]);
    expect(await watchdog.wakeState(now)).toEqual({
      pending: false,
      nextWakeAt: 2100,
    });

    now = 2_100;
    await watchdog.tick();
    expect(post.read(envelope.key)).toEqual({ state: "delivered" });
    expect(
      receiver.owner.sql.exec("SELECT key,text FROM admitted_events").toArray(),
    ).toEqual([{ key: "fact:one", text: "hello" }]);
    expect(calls).toBe(2);

    const reopened = createPost(options);
    execute = (job, signal, attempt) => reopened.execute(job, attempt, signal);
    producer.owner.transactionSync(() => {
      reopened.send(envelope);
    });
    await watchdog.tick();
    expect(calls).toBe(2);
  } finally {
    producer.close();
    receiver.close();
  }
});

it("releases a queued Post retry now without resetting its attempt count", async () => {
  const producer = createSqliteOwnerFixture();
  try {
    const now = 1_000;
    vi.spyOn(Date, "now").mockImplementation(() => now);
    let available = false;
    let execute: WatchdogExecutor = () =>
      Promise.reject(new Error("Post is not open"));
    const watchdog: WatchdogRuntime = await Watchdog.make({
      owner: producer.owner,
      retryPolicy: (job) => post.retryPolicy(job),
      execute: (job, signal, attempt) => execute(job, signal, attempt),
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
    const route = definePostRoute({
      addressKind: "team",
      address: Schema.Struct({ teamId: Schema.String }),
      event: Event,
      name: ({ teamId }) => teamId,
      namespace: {
        getByName: () => ({
          deliver: () =>
            available
              ? Promise.resolve("accepted")
              : Promise.reject(new Error("recipient offline")),
        }),
      },
    });
    const post = createPost({
      routes: { team: route },
      owner: producer.owner,
      transactional: watchdog.transactional,
      now: () => now,
      scheduleWake: () => Promise.resolve(),
    });
    execute = (job, signal, attempt) => post.execute(job, attempt, signal);
    producer.owner.transactionSync(() => {
      post.send({
        key: "held",
        kind: "team",
        address: { teamId: "team-one" },
        event: { text: "keep the original event" },
      });
    });

    await watchdog.tick();
    expect(post.read("held")).toMatchObject({
      state: "pending",
    });
    const jobId = post.read("held");
    expect(await watchdog.wakeState(now)).toEqual({
      pending: false,
      nextWakeAt: 3000,
    });
    available = true;
    expect(await post.releaseNow()).toBeUndefined();
    expect(post.read("held")).toMatchObject({
      state: "pending",
    });

    expect(post.read("held")).toEqual(jobId);
    expect(
      producer.owner.sql
        .exec("SELECT attempt_count,not_before FROM watchdog_jobs")
        .toArray(),
    ).toEqual([{ attempt_count: 1, not_before: now }]);
    await watchdog.tick();
    expect(post.read("held")).toMatchObject({ state: "delivered" });
  } finally {
    producer.close();
  }
});

it("retries a dead letter after queued successors and expedites a queued failure without duplicating live jobs", async () => {
  const producer = createSqliteOwnerFixture();
  try {
    vi.spyOn(Date, "now").mockReturnValue(100);
    const received: string[] = [];
    let rejectFirst = true;
    let unavailable = true;
    const route = definePostRoute({
      addressKind: "team",
      address: Schema.Struct({ teamId: Schema.String }),
      event: Event,
      name: ({ teamId }) => teamId,
      namespace: {
        getByName: () => ({
          deliver: (envelope) => {
            received.push(envelope.key);
            if (envelope.key === "one" && rejectFirst)
              return Promise.resolve("refused" as const);
            if (envelope.key === "four" && unavailable)
              return Promise.reject(new Error("Receiver unavailable"));
            return Promise.resolve("accepted" as const);
          },
        }),
      },
    });
    let execute: WatchdogExecutor = () =>
      Promise.reject(new Error("Post is not open"));
    const watchdog: WatchdogRuntime = await Watchdog.make({
      owner: producer.owner,
      retryPolicy: (job) => post.retryPolicy(job),
      execute: (job, signal, attempt) => execute(job, signal, attempt),
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
    const options = {
      routes: { team: route },
      owner: producer.owner,
      transactional: watchdog.transactional,
      now: () => 100,
      scheduleWake: () => Promise.resolve(),
    };
    const post = createPost(options);
    execute = (job, signal, attempt) => post.execute(job, attempt, signal);
    const send = (key: string) => {
      producer.owner.transactionSync(() => {
        post.send({
          key,
          kind: "team",
          address: { teamId: "t1" },
          event: { text: key },
        });
      });
    };
    send("one");
    send("two");
    await watchdog.tick();
    expect(post.read("one")).toMatchObject({
      state: "dead",
      reason: "refused",
    });
    rejectFirst = false;
    const reopened = createPost(options);
    execute = (job, signal, attempt) => reopened.execute(job, attempt, signal);
    await reopened.retry("one");
    send("three");
    await reopened.retry("two");
    for (let count = 0; count < 3; count += 1) await watchdog.tick();
    expect(received).toEqual(["one", "two", "one", "three"]);
    expect(reopened.latest("team", { teamId: "t1" })).toEqual({
      key: "three",
      row: { state: "delivered" },
    });
    send("four");
    await watchdog.tick();
    expect(reopened.read("four")).toMatchObject({
      state: "pending",
    });
    unavailable = false;
    await reopened.retry("four");
    await watchdog.tick();
    expect(reopened.read("four")).toEqual({ state: "delivered" });
    await reopened.retry("four");
    await reopened.retry("missing");
    expect((await watchdog.tick()).status).toBe("no_ready");
    expect(received).toEqual(["one", "two", "one", "three", "four", "four"]);
  } finally {
    producer.close();
  }
});
