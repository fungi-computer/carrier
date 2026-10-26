import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import { Watchdog, type WatchdogExecutor } from "@fungi.computer/watchdog";

import { admitPost, createPost, definePostRoute } from "../src/post.js";
import { createSqliteOwnerFixture } from "../../watchdog/test/sqlite-owner.fixture.js";

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
    const watchdog = await Watchdog.make({
      owner: producer.owner,
      execute: (job, signal) => execute(job, signal),
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
    const options = {
      routes,
      owner: postOwner,
      transactional: watchdog.transactional,
      readJob: watchdog.read,
      now: () => now,
      scheduleWake: (at: number) => {
        wakes.push(at);
        return Promise.resolve();
      },
    };
    const post = createPost(options);
    execute = post.execute;
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
    expect((await watchdog.tick()).status).toBe("settled");
    expect(post.read(envelope.key)).toMatchObject({
      state: "pending",
      attempts: 1,
    });
    expect(calls).toBe(1);
    expect(wakes).toEqual([2_100]);

    now = 2_100;
    await post.enqueueDue();
    await watchdog.tick();
    expect(post.read(envelope.key)).toEqual({ state: "delivered" });
    expect(
      receiver.owner.sql.exec("SELECT key,text FROM admitted_events").toArray(),
    ).toEqual([{ key: "fact:one", text: "hello" }]);
    expect(calls).toBe(2);

    const reopened = createPost(options);
    execute = reopened.execute;
    producer.owner.transactionSync(() => {
      reopened.send(envelope);
    });
    await reopened.enqueueDue();
    await watchdog.tick();
    expect(calls).toBe(2);
  } finally {
    producer.close();
    receiver.close();
  }
});

it("releases a settled Post retry now without resetting its attempt count", async () => {
  const producer = createSqliteOwnerFixture();
  try {
    const now = 1_000;
    let available = false;
    let execute: WatchdogExecutor = () =>
      Promise.reject(new Error("Post is not open"));
    const watchdog = await Watchdog.make({
      owner: producer.owner,
      execute: (job, signal) => execute(job, signal),
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
      readJob: watchdog.read,
      now: () => now,
      scheduleWake: () => Promise.resolve(),
    });
    execute = post.execute;
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
      attempts: 1,
      nextAttemptAt: 3_000,
    });
    available = true;
    expect(await post.releaseNow()).toBeUndefined();
    expect(post.read("held")).toMatchObject({
      state: "pending",
      attempts: 1,
      nextAttemptAt: now,
    });

    await watchdog.tick();
    expect(post.read("held")).toMatchObject({ state: "delivered" });
  } finally {
    producer.close();
  }
});

it("retries a dead letter after queued successors and expedites a settled failure without duplicating live jobs", async () => {
  const producer = createSqliteOwnerFixture();
  try {
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
    const watchdog = await Watchdog.make({
      owner: producer.owner,
      execute: (job, signal) => execute(job, signal),
      isAlive: () => Promise.resolve(false),
      wake: { recompute: () => Promise.resolve() },
    });
    const options = {
      routes: { team: route },
      owner: producer.owner,
      transactional: watchdog.transactional,
      readJob: watchdog.read,
      now: () => 100,
      scheduleWake: () => Promise.resolve(),
    };
    const post = createPost(options);
    execute = post.execute;
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
    execute = reopened.execute;
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
      attempts: 1,
      nextAttemptAt: 2_100,
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
