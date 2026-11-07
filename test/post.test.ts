import type { WatchdogRuntime } from "@fungi.computer/watchdog";
import { afterEach, expect, it, vi } from "vitest";
import * as Schema from "effect/Schema";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  PostError,
  admitPost,
  createPost,
  definePostRoute,
} from "../src/post.js";
import type { PostEnvelope, PostRegistryRoute } from "../src/post.js";
import { openSqlPost } from "../src/internal/post-sql.js";
import { deliverPost } from "../src/internal/post-execution.js";
import { Watchdog, type WatchdogSqliteOwner } from "@fungi.computer/watchdog";

import { createSqliteOwnerFixture } from "../../watchdog/test/sqlite-owner.fixture.js";

const fixtures: ReturnType<typeof createSqliteOwnerFixture>[] = [];
const temporaryDirectories: string[] = [];
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const fixture of fixtures.splice(0)) fixture.close();
  for (const directory of temporaryDirectories.splice(0))
    rmSync(directory, { recursive: true, force: true });
});
const sqliteFixture = (filename?: string) => {
  const fixture = createSqliteOwnerFixture(filename);
  let open = true;
  const tracked = Object.freeze({
    owner: fixture.owner,
    close: () => {
      if (!open) return;
      open = false;
      fixture.close();
    },
  });
  fixtures.push(tracked);
  return tracked;
};
const sqliteOwner = () => {
  const fixture = sqliteFixture();
  return fixture.owner;
};

async function openPost<R extends Record<string, PostRegistryRoute>>(
  options: Omit<Parameters<typeof createPost<R>>[0], "transactional">,
) {
  if (options.now !== Date.now)
    vi.spyOn(Date, "now").mockImplementation(options.now);
  const watchdog: WatchdogRuntime = await Watchdog.make({
    owner: options.owner,
    isAlive: () => Promise.resolve(false),
    retryPolicy: (job) => post.retryPolicy(job),
    execute: (job, signal, attempt) => post.execute(job, attempt, signal),
    wake: {
      recompute: async () => {
        const state = await watchdog.wakeState(options.now());
        const due = state.pending ? options.now() : state.nextWakeAt;
        if (due !== null) await options.scheduleWake(due);
      },
    },
  });
  const post = createPost({
    ...options,
    transactional: watchdog.transactional,
  });
  return {
    ...post,
    tick: watchdog.tick,
    wakeState: () => watchdog.wakeState(options.now()),
    jobs: () =>
      options.owner.sql
        .exec(
          "SELECT job_id,queue_name,attempt_count FROM watchdog_jobs ORDER BY sequence",
        )
        .toArray(),
  };
}

const address = Schema.Struct({ teamId: Schema.String });
const event = Schema.Struct({ text: Schema.String });

it("turns an undecodable receiver reply into unavailable evidence with its cause", async () => {
  const route = definePostRoute({
    addressKind: "team",
    address,
    event,
    name: ({ teamId }) => teamId,
    namespace: {
      getByName: () => ({
        deliver: () => Promise.resolve(JSON.parse('{"unexpected":true}')),
      }),
    },
  });
  const attempt = {
    state: "pending" as const,
    envelope: {
      key: "malformed-reply",
      kind: "team",
      address: { teamId: "t1" },
      event: { text: "hello" },
    },
    attempts: 1,
    nextAttemptAt: 100,
    jobId: "post_work_one",
  };

  const evidence = await deliverPost({
    attempt,
    route,
    savePrepared: () => undefined,
  });

  expect(evidence).toMatchObject({ result: "unavailable" });
  expect(evidence.failure).toBeDefined();
});

it("encodes domain values for delivery and decodes receipt callbacks with the maintained schema", async () => {
  const owner = sqliteOwner();
  const wire: PostEnvelope[] = [];
  const applied: number[] = [];
  const acknowledged: number[] = [];
  const receipts = new Set<string>();
  const address = Schema.Struct({ teamId: Schema.NumberFromString });
  const event = Schema.Struct({ sequence: Schema.NumberFromString });
  const post = await openPost({
    owner,
    routes: {
      fact: definePostRoute({
        addressKind: "team",
        address,
        event,
        name: ({ teamId }) => String(teamId),
        namespace: {
          getByName: (name) => ({
            deliver: (envelope) => {
              expect(name).toBe("42");
              wire.push(envelope);
              return Promise.resolve(
                admitPost({
                  key: envelope.key,
                  event: envelope.event,
                  schema: event,
                  hasReceipt: (key) => receipts.has(key),
                  putReceipt: (key) => {
                    receipts.add(key);
                  },
                  apply: ({ sequence }) => {
                    applied.push(sequence);
                    return "accepted";
                  },
                }),
              );
            },
          }),
        },
        onDelivered: ({ teamId }, { sequence }) => {
          expect(teamId).toBe(42);
          acknowledged.push(sequence);
        },
      }),
    },

    now: () => 1_000,
    scheduleWake: () => Promise.resolve(),
  });
  owner.transactionSync(() => {
    post.send({
      key: "encoded-fact",
      kind: "fact",
      address: { teamId: 42 },
      event: { sequence: 7 },
    });
  });
  await post.tick();
  expect(wire).toMatchObject([
    { address: { teamId: "42" }, event: { sequence: "7" } },
  ]);
  expect(applied).toEqual([7]);
  expect(acknowledged).toEqual([7]);
  expect(post.read("encoded-fact")).toMatchObject({ state: "delivered" });
  expect(post.latest("fact", { teamId: 42 })?.key).toBe("encoded-fact");
});

it("commits an addressed Watchdog job and keeps its receipt through replay", async () => {
  const owner = sqliteOwner();
  const received: string[] = [];
  const receipts = new Set<string>();
  const wakes: number[] = [];
  let releaseFirst: () => void = () => {
    throw new Error("delivery did not start");
  };
  const firstDelivery = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const route = definePostRoute({
    addressKind: "team",
    address,
    event,
    name: (to) => to.teamId,
    namespace: {
      getByName: (name) => ({
        deliver: async (envelope: PostEnvelope) => {
          if (envelope.key === "one") await firstDelivery;
          return admitPost({
            key: envelope.key,
            event: envelope.event,
            schema: event,
            hasReceipt: (key) => receipts.has(key),
            apply: (body) => {
              received.push(`${name}:${body.text}`);
              return "accepted";
            },
            putReceipt: (key) => {
              receipts.add(key);
            },
          });
        },
      }),
    },
  });
  const options = {
    routes: { team: route },
    owner,

    now: () => 100,
    scheduleWake: (at: number) => {
      wakes.push(at);
      return Promise.resolve();
    },
  };
  const post = await openPost(options);
  post.send({
    key: "one",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "hello" },
  });
  expect((await post.wakeState()).pending).toBe(true);
  expect(post.jobs()).toHaveLength(1);
  expect(post.jobs()[0]?.queue_name).toBe('["team","t1"]');
  const firstExecution = post.tick();
  post.send({
    key: "two",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "later" },
  });
  expect(post.jobs()).toHaveLength(2);
  expect(post.latest("team", { teamId: "t1" })).toMatchObject({
    key: "two",
    row: { state: "pending" },
  });
  expect(post.latest("team", { teamId: "other" })).toBeUndefined();
  releaseFirst();
  await firstExecution;
  expect(post.jobs()).toHaveLength(2);
  await post.tick();
  post.send({
    key: "one",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "hello" },
  });
  await post.tick();
  expect(post.jobs()).toHaveLength(2);
  expect(received).toEqual(["t1:hello", "t1:later"]);
  expect(post.read("one")).toEqual({ state: "delivered" });
  expect((await openPost(options)).latest("team", { teamId: "t1" })).toEqual({
    key: "two",
    row: { state: "delivered" },
  });
  expect((await post.wakeState()).nextWakeAt).toBeNull();
  expect((await post.wakeState()).pending).toBe(false);
});

it("commits a typed receiver cursor with the producer receipt", async () => {
  const owner = sqliteOwner();
  const cursors: number[] = [];
  const post = await openPost({
    routes: {
      team: definePostRoute({
        addressKind: "team",
        address,
        event,
        name: (to) => to.teamId,
        namespace: {
          getByName: () => ({
            deliver: () =>
              Promise.resolve({ status: "accepted" as const, cursor: 7 }),
          }),
        },
        onCursor: (_to, _event, cursor) => {
          cursors.push(cursor);
        },
      }),
    },
    owner,

    now: () => 100,
    scheduleWake: () => Promise.resolve(),
  });
  post.send({
    key: "cursor",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "hello" },
  });
  await post.tick();
  expect(post.read("cursor")).toEqual({ state: "delivered" });
  expect(cursors).toEqual([7]);
});

it("bounds unavailable delivery at twenty attempts and retains inspectable dead letters", async () => {
  const owner = sqliteOwner();
  const wakes: number[] = [];
  let now = 100;
  let refuse = false;
  const post = await openPost({
    routes: {
      team: definePostRoute({
        addressKind: "team",
        address,
        event,
        name: (to) => to.teamId,
        namespace: {
          getByName: () => ({
            deliver: () =>
              refuse
                ? Promise.resolve("refused" as const)
                : Promise.reject(new Error("offline")),
          }),
        },
      }),
    },
    owner,
    now: () => now,

    scheduleWake: (at) => {
      wakes.push(at);
      return Promise.resolve();
    },
  });
  post.send({
    key: "retry",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "x" },
  });
  await post.tick();
  expect(wakes).toEqual([2_100]);
  expect((await post.wakeState()).nextWakeAt).toBe(2_100);
  for (let attempt = 1; attempt < 20; attempt += 1) {
    now += Math.min(2_000 * 2 ** (attempt - 1), 300_000);
    expect(wakes.at(-1)).toBe(now);

    await post.tick();
  }

  expect(post.jobs()).toMatchObject([{ attempt_count: 20 }]);
  expect(post.deadLetters().letters).toMatchObject([
    {
      key: "post:retry",
      reason: "exhausted",
      attempts: 20,
      envelope: { key: "retry", event: { text: "x" } },
    },
  ]);
  refuse = true;
  post.send({
    key: "refused",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "refuse" },
  });
  await post.tick();
  expect(post.deadLetters().letters[0]).toMatchObject({
    key: "post:refused",
    reason: "refused",
    attempts: 1,
    envelope: { key: "refused", event: { text: "refuse" } },
  });
});

it("retries a seven-day route past the attempt ceiling and expires after physical SQLite reopens", async () => {
  const directory = mkdtempSync(join(tmpdir(), "carrier-post-deadline-"));
  temporaryDirectories.push(directory);
  const filename = join(directory, "post.sqlite");
  let fixture = sqliteFixture(filename);
  let now = 100;
  let deliveries = 0;
  const startAt = now;
  const deadlineMs = 604_800_000;
  const deadlineAt = startAt + deadlineMs;
  const routes = {
    team: definePostRoute({
      addressKind: "team",
      address,
      event,
      deadlineMs,
      name: (to) => to.teamId,
      namespace: {
        getByName: () => ({
          deliver: () => {
            deliveries += 1;
            return Promise.reject(new Error("offline"));
          },
        }),
      },
    }),
  };
  const open = (owner: WatchdogSqliteOwner) =>
    openPost({
      routes,
      owner,

      now: () => now,
      scheduleWake: () => Promise.resolve(),
    });
  let post = await open(fixture.owner);
  post.send({
    key: "seven-day-deadline",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "x" },
  });

  for (let attempts = 1; attempts <= 20; attempts += 1) {
    await post.tick();
    const row = post.read("seven-day-deadline");
    expect(row).toMatchObject({
      state: "pending",
      deadlineAt,
    });
    expect(post.deadLetters().letters).toEqual([]);
    if (attempts < 20) {
      if (row?.state !== "pending")
        throw new Error("Expected a pending seven-day route");
      const due = (await post.wakeState()).nextWakeAt;
      if (due === null) throw new Error("Missing retained retry deadline");
      now = due;
    }
  }
  expect(now - startAt).toBe(3_810_000);
  expect(deliveries).toBe(20);

  fixture.close();
  fixture = sqliteFixture(filename);
  post = await open(fixture.owner);
  now = startAt + 6 * 24 * 60 * 60 * 1_000;

  await post.tick();
  expect(post.read("seven-day-deadline")).toMatchObject({
    state: "pending",
    deadlineAt,
  });
  expect(post.jobs()).toMatchObject([{ attempt_count: 21 }]);
  expect(post.deadLetters().letters).toEqual([]);
  expect(deliveries).toBe(21);

  fixture.close();
  fixture = sqliteFixture(filename);
  post = await open(fixture.owner);
  now = deadlineAt + 1;
  const settledJobs = post.jobs().length;
  await post.tick();

  expect(post.read("seven-day-deadline")).toMatchObject({
    state: "dead",
    reason: "expired",
    attempts: 21,
    deadlineAt,
  });
  const letters = post.deadLetters().letters;
  expect(letters).toHaveLength(1);
  expect(letters).toMatchObject([
    {
      key: "post:seven-day-deadline",
      reason: "expired",
      attempts: 21,
    },
  ]);

  expect(post.jobs()).toHaveLength(settledJobs);
  expect(deliveries).toBe(21);
});

it("expires a route whose active delivery fails after its deadline", async () => {
  const owner = sqliteOwner();
  let now = 100;
  const post = await openPost({
    routes: {
      team: definePostRoute({
        addressKind: "team",
        address,
        event,
        deadlineMs: 1_000,
        name: (to) => to.teamId,
        namespace: {
          getByName: () => ({
            deliver: () => {
              now = 1_101;
              return Promise.reject(new Error("offline"));
            },
          }),
        },
      }),
    },
    owner,

    now: () => now,
    scheduleWake: () => Promise.resolve(),
  });
  post.send({
    key: "crossed-deadline",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "x" },
  });

  await post.tick();

  expect(post.read("crossed-deadline")).toMatchObject({
    state: "dead",
    reason: "expired",
    attempts: 1,
    deadlineAt: 1_100,
  });
});

it("refuses deadline routes on relational Post adapters with a typed reason", () => {
  const route = definePostRoute({
    addressKind: "team",
    address,
    event,
    deadlineMs: 1_000,
    name: (to) => to.teamId,
    namespace: {
      getByName: () => ({
        deliver: () => Promise.resolve("accepted"),
      }),
    },
  });

  expect(() =>
    openSqlPost(
      {
        query: () => Promise.resolve([]),
      },
      {
        routes: { team: route },
        now: () => 100,
      },
    ),
  ).toThrow(
    expect.objectContaining({
      _tag: PostError.name,
      reason: "relational-deadline-unsupported",
    }),
  );
});

it("times out a hung receiver, preserves its retry, and lets the next recipient run", async () => {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  vi.setSystemTime(100);
  const owner = sqliteOwner();
  let release: (result: "accepted") => void = () => undefined;
  let begin: () => void = () => {
    throw new Error("Missing start observer");
  };
  const started = new Promise<void>((resolve) => {
    begin = resolve;
  });
  const hung = new Promise<"accepted">((resolve) => {
    release = resolve;
  });
  const post = await openPost({
    routes: {
      team: definePostRoute({
        addressKind: "team",
        address,
        event,
        name: (to) => to.teamId,
        namespace: {
          getByName: (name) => ({
            deliver: () => {
              begin();
              return name === "hung"
                ? hung
                : Promise.resolve("accepted" as const);
            },
          }),
        },
      }),
    },
    owner,

    now: Date.now,
    scheduleWake: () => Promise.resolve(),
  });
  post.send({
    key: "hung",
    kind: "team",
    address: { teamId: "hung" },
    event: { text: "first" },
  });
  post.send({
    key: "healthy",
    kind: "team",
    address: { teamId: "healthy" },
    event: { text: "second" },
  });
  const attempt = post.tick();
  await started;
  await vi.advanceTimersByTimeAsync(10_000);
  await attempt;
  expect(post.read("hung")).toMatchObject({ state: "pending" });
  await post.tick();
  expect((await post.wakeState()).nextWakeAt).toBe(Date.now() + 2_000);
  expect(post.read("healthy")).toEqual({ state: "delivered" });
  release("accepted");
  await Promise.resolve();
  expect(post.read("hung")).toMatchObject({ state: "pending" });
  await vi.advanceTimersByTimeAsync(2_000);

  await post.tick();
  expect(post.read("hung")).toEqual({ state: "delivered" });
});

it("orders different event kinds for one recipient through lost receipts without delaying another namespace", async () => {
  const owner = sqliteOwner();
  const applied: string[] = [];
  const receipts = new Set<string>();
  let loseReceipt = true;
  let now = 100;
  const route = (addressKind: string) => {
    const definition = {
      addressKind,
      address,
      event,
      name: (to: Schema.Schema.Type<typeof address>) => to.teamId,
      namespace: {
        getByName: () => ({
          deliver: (envelope: PostEnvelope) => {
            const duplicate = receipts.has(envelope.key);
            if (!duplicate) {
              receipts.add(envelope.key);
              applied.push(envelope.key);
            }
            if (envelope.key === "home" && loseReceipt) {
              loseReceipt = false;
              return Promise.reject(new Error("accepted receipt lost"));
            }
            return Promise.resolve(
              duplicate ? ("duplicate" as const) : ("accepted" as const),
            );
          },
        }),
      },
    };
    return definePostRoute(definition);
  };
  const options = {
    routes: {
      home: route("agent"),
      control: route("agent"),
      member: route("memberInbox"),
    },
    owner,

    now: () => now,
    scheduleWake: () => Promise.resolve(),
  };
  let post = await openPost(options);
  for (const kind of ["home", "control", "member"] as const)
    post.send({
      key: kind,
      kind,
      address: { teamId: "same-name" },
      event: { text: kind },
    });
  expect(post.latest("home", { teamId: "same-name" })?.key).toBe("home");
  expect(post.latest("control", { teamId: "same-name" })?.key).toBe("control");
  expect(post.latest("member", { teamId: "same-name" })?.key).toBe("member");
  expect(post.jobs()).toHaveLength(3);
  await post.tick();
  await post.tick();
  expect(applied).toEqual(["home", "member"]);
  expect(post.read("control")).toMatchObject({ state: "pending" });
  expect((await post.wakeState()).nextWakeAt).toBe(2_100);
  post = await openPost(options);
  now = 2_100;

  expect(post.jobs()).toHaveLength(3);
  await post.tick();
  expect(post.jobs()).toHaveLength(3);
  await post.tick();
  expect(applied).toEqual(["home", "member", "control"]);
  expect(post.read("home")).toEqual({ state: "delivered" });
  expect((await post.wakeState()).nextWakeAt).toBeNull();
});

it("persists a prepared fact before delivery and commits its producer acknowledgment with the receipt", async () => {
  const owner = sqliteOwner();
  let now = 100;
  let prepareCount = 0;
  let loseResponse = true;
  let rejectAck = true;
  const received: unknown[] = [];
  owner.sql.exec(
    "CREATE TABLE grant_expiry (grant TEXT PRIMARY KEY, expires_at INTEGER NOT NULL)",
  );
  const intent = Schema.Struct({ revision: Schema.Int });
  const fact = Schema.Struct({
    revision: Schema.Int,
    grant: Schema.String,
    expiresAt: Schema.NumberFromString,
  });
  const route = definePostRoute({
    addressKind: "team",
    address,
    event: intent,
    name: (to) => to.teamId,
    prepare: {
      event: fact,
      run: (_to, value) => {
        prepareCount += 1;
        return Promise.resolve({
          ...value,
          grant: `sealed-${String(prepareCount)}`,
          expiresAt: now + 60_000,
        });
      },
    },
    onDelivered: (_to, value) => {
      owner.sql.exec(
        "INSERT INTO grant_expiry VALUES (?,?)",
        value.grant,
        value.expiresAt,
      );
      if (rejectAck) throw new Error("Producer acknowledgment failed");
    },
    namespace: {
      getByName: () => ({
        deliver: (envelope: PostEnvelope) => {
          received.push(Schema.decodeUnknownSync(fact)(envelope.event));
          if (loseResponse) {
            loseResponse = false;
            return Promise.reject(new Error("Accepted response lost"));
          }
          return Promise.resolve("duplicate" as const);
        },
      }),
    },
  });
  const options = {
    routes: { team: route },
    owner,

    now: () => now,
    scheduleWake: () => Promise.resolve(),
  };
  const input = {
    key: "prepared",
    kind: "team",
    address: { teamId: "t1" },
    event: { revision: 1 },
  } as const;
  const post = await openPost(options);
  post.send(input);
  await post.tick();
  expect(post.read(input.key)).toMatchObject({
    state: "pending",
    delivery: { event: { grant: "sealed-1", expiresAt: "60100" } },
  });
  const reopened = await openPost(options);
  reopened.send(input);
  expect(() => {
    reopened.send({ ...input, event: { revision: 2 } });
  }).toThrow(
    expect.objectContaining({
      _tag: "PostError",
      message: "conflicting post key",
    }),
  );
  now = 2_100;

  await expect(reopened.tick()).resolves.toMatchObject({ status: "requeued" });
  expect(reopened.read(input.key)).toMatchObject({ state: "pending" });
  expect(owner.sql.exec("SELECT * FROM grant_expiry").toArray()).toEqual([]);
  rejectAck = false;
  now = 10_100;

  await reopened.tick();
  expect(prepareCount).toBe(1);
  expect(received).toEqual(
    Array.from({ length: 3 }, () => ({
      revision: 1,
      grant: "sealed-1",
      expiresAt: 60_100,
    })),
  );
  expect(owner.sql.exec("SELECT * FROM grant_expiry").toArray()).toEqual([
    { grant: "sealed-1", expires_at: 60_100 },
  ]);
  expect(reopened.read(input.key)).toEqual({ state: "delivered" });
});

it("bounds preparation and ignores a fact prepared after its attempt timed out", async () => {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  vi.setSystemTime(100);
  const owner = sqliteOwner();
  const received: unknown[] = [];
  let release: (value: { text: string }) => void = () => {
    throw new Error("Preparation did not start");
  };
  let begin: () => void = () => {
    throw new Error("Missing start observer");
  };
  const started = new Promise<void>((resolve) => {
    begin = resolve;
  });
  const hung = new Promise<{ text: string }>((resolve) => {
    release = resolve;
  });
  let first = true;
  const post = await openPost({
    routes: {
      team: definePostRoute({
        addressKind: "team",
        address,
        event,
        name: (to) => to.teamId,
        prepare: {
          event,
          run: (_to, intent) => {
            if (first) {
              first = false;
              begin();
              return hung;
            }
            return Promise.resolve(intent);
          },
        },
        namespace: {
          getByName: () => ({
            deliver: (envelope: PostEnvelope) => {
              received.push(envelope.event);
              return Promise.resolve("accepted");
            },
          }),
        },
      }),
    },
    owner,

    now: Date.now,
    scheduleWake: () => Promise.resolve(),
  });
  post.send({
    key: "hung-preparation",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "current" },
  });
  const attempt = post.tick();
  await started;
  await vi.advanceTimersByTimeAsync(10_000);
  await attempt;
  release({ text: "late" });
  await vi.advanceTimersByTimeAsync(0);
  expect(received).toEqual([]);
  expect(post.read("hung-preparation")).toMatchObject({
    state: "pending",
  });
  await vi.advanceTimersByTimeAsync(2_000);

  await post.tick();
  expect(received).toEqual([{ text: "current" }]);
  expect(post.read("hung-preparation")).toEqual({ state: "delivered" });
});

it("refuses a pre-scheduler SQLite Post store without loading, admitting or rewriting it", async () => {
  const owner = sqliteOwner();
  const received: string[] = [];
  owner.sql.exec(
    "CREATE TABLE carrier_post_rows (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
  );
  // A pending row as Post wrote it before the Watchdog scheduler (Fungi 0.14).
  owner.sql.exec(
    "INSERT INTO carrier_post_rows VALUES (?,?)",
    "post:retained",
    JSON.stringify({
      state: "pending",
      envelope: {
        key: "retained",
        kind: "team",
        address: { teamId: "team-a" },
        event: { text: "kept" },
      },
      attempts: 0,
      nextAttemptAt: 100,
      jobId: "retained-job",
    }),
  );
  const rows = () =>
    owner.sql.exec("SELECT key,value FROM carrier_post_rows").toArray();
  const before = rows();
  const route = definePostRoute({
    addressKind: "team",
    address,
    event,
    name: (to) => to.teamId,
    namespace: {
      getByName: () => ({
        deliver: (envelope: PostEnvelope) => {
          received.push(envelope.key);
          return Promise.resolve("accepted");
        },
      }),
    },
  });
  await expect(
    openPost({
      routes: { team: route },
      owner,
      now: () => 100,
      scheduleWake: () => Promise.resolve(),
    }),
  ).rejects.toMatchObject({
    _tag: "PostError",
    reason: "pre-scheduler-store",
  });
  expect(rows()).toEqual(before);
  expect(
    owner.sql.exec("SELECT COUNT(*) AS count FROM watchdog_jobs").one(),
  ).toEqual({ count: 0 });
  expect(received).toEqual([]);
});
