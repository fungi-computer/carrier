import { afterEach, expect, it, vi } from "vitest";
import * as Schema from "effect/Schema";

import { admitPost, createPost, definePostRoute } from "../src/post.js";
import type { PostEnvelope } from "../src/post.js";
import { deliverPost } from "../src/internal/post-execution.js";
import type {
  WatchdogQueuedJob,
  WatchdogRunningJob,
  WatchdogReadResult,
  WatchdogTransactionalProjection,
} from "@fungi.computer/watchdog";

import { createSqliteOwnerFixture } from "../../watchdog/test/sqlite-owner.fixture.js";

const fixtures: ReturnType<typeof createSqliteOwnerFixture>[] = [];
afterEach(() => {
  vi.useRealTimers();
  for (const fixture of fixtures.splice(0)) fixture.close();
});
const sqliteOwner = () => {
  const fixture = createSqliteOwnerFixture();
  fixtures.push(fixture);
  return fixture.owner;
};

const running = (job: WatchdogQueuedJob | undefined): WatchdogRunningJob => {
  if (job === undefined) throw new Error("missing queued job");
  return { ...job, state: "running" };
};
const settledJob = (
  jobs: readonly WatchdogQueuedJob[],
  id: string,
): Promise<WatchdogReadResult> => {
  const job = jobs.find((candidate) => candidate.jobId === id);
  return Promise.resolve(
    job === undefined
      ? { status: "missing" }
      : {
          status: "found",
          job: { ...job, state: "settled", outcome: "completed" },
          cancellation: { status: "absent" },
        },
  );
};

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
  const jobs: WatchdogQueuedJob[] = [];
  const wire: PostEnvelope[] = [];
  const applied: number[] = [];
  const acknowledged: number[] = [];
  const receipts = new Set<string>();
  const address = Schema.Struct({ teamId: Schema.NumberFromString });
  const event = Schema.Struct({ sequence: Schema.NumberFromString });
  const post = createPost({
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
    transactional: {
      enqueueAcceptedJob: (job) => {
        jobs.push(job);
      },
      readQueueHead: () => ({ state: "idle" }),
      readLatestSettledJob: () => undefined,
      recordCancellationIntent: () => ({ status: "missing" }),
    },
    readJob: (id) => settledJob(jobs, id),
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
  await post.execute(running(jobs[0]));
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
  const jobs: WatchdogQueuedJob[] = [];
  const received: string[] = [];
  const receipts = new Set<string>();
  const wakes: number[] = [];
  let releaseFirst: () => void = () => {
    throw new Error("delivery did not start");
  };
  const firstDelivery = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const transactional: WatchdogTransactionalProjection = {
    enqueueAcceptedJob: (job) => {
      jobs.push(job);
    },
    readQueueHead: () => ({ state: "idle" }),
    readLatestSettledJob: () => undefined,
    recordCancellationIntent: () => ({ status: "missing" }),
  };
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
    transactional,
    readJob: (id: string) => settledJob(jobs, id),
    now: () => 100,
    scheduleWake: (at: number) => {
      wakes.push(at);
      return Promise.resolve();
    },
  };
  const post = createPost(options);
  post.send({
    key: "one",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "hello" },
  });
  expect(post.nextWake()).toBe(100);
  expect(jobs).toHaveLength(1);
  expect(jobs[0]?.queue).toBe('["team","t1"]');
  const firstExecution = post.execute(running(jobs[0]));
  post.send({
    key: "two",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "later" },
  });
  expect(jobs).toHaveLength(1);
  expect(post.latest("team", { teamId: "t1" })).toMatchObject({
    key: "two",
    row: { state: "pending" },
  });
  expect(post.latest("team", { teamId: "other" })).toBeUndefined();
  releaseFirst();
  await firstExecution;
  expect(jobs).toHaveLength(2);
  await post.execute(running(jobs[1]));
  post.send({
    key: "one",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "hello" },
  });
  await post.execute(running(jobs[0]));
  expect(jobs).toHaveLength(2);
  expect(received).toEqual(["t1:hello", "t1:later"]);
  expect(post.read("one")).toEqual({ state: "delivered", nextKey: "two" });
  expect(createPost(options).latest("team", { teamId: "t1" })).toEqual({
    key: "two",
    row: { state: "delivered" },
  });
  expect(post.nextWake()).toBeNull();
  expect(wakes).toEqual([2_100, 2_100]);
});

it("commits a typed receiver cursor with the producer receipt", async () => {
  const owner = sqliteOwner();
  const jobs: WatchdogQueuedJob[] = [];
  const cursors: number[] = [];
  const post = createPost({
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
    transactional: {
      enqueueAcceptedJob: (job) => {
        jobs.push(job);
      },
      readQueueHead: () => ({ state: "idle" }),
      readLatestSettledJob: () => undefined,
      recordCancellationIntent: () => ({ status: "missing" }),
    },
    readJob: (id) => settledJob(jobs, id),
    now: () => 100,
    scheduleWake: () => Promise.resolve(),
  });
  post.send({
    key: "cursor",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "hello" },
  });
  await post.execute(running(jobs[0]));
  expect(post.read("cursor")).toEqual({ state: "delivered" });
  expect(cursors).toEqual([7]);
});

it("bounds unavailable delivery at twenty attempts and retains inspectable dead letters", async () => {
  const owner = sqliteOwner();
  const jobs: WatchdogQueuedJob[] = [];
  const wakes: number[] = [];
  let now = 100;
  let refuse = false;
  const post = createPost({
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
    transactional: {
      enqueueAcceptedJob: (job) => {
        jobs.push(job);
      },
      readQueueHead: () => ({ state: "idle" }),
      readLatestSettledJob: () => undefined,
      recordCancellationIntent: () => ({ status: "missing" }),
    },
    now: () => now,
    readJob: (id) => settledJob(jobs, id),
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
  await post.execute(running(jobs[0]));
  expect(wakes).toEqual([2_100]);
  expect(post.nextWake()).toBe(2_100);
  for (let attempt = 1; attempt < 20; attempt += 1) {
    now += Math.min(2_000 * 2 ** (attempt - 1), 300_000);
    expect(wakes.at(-1)).toBe(now);
    await post.enqueueDue();
    await post.execute(running(jobs.at(-1)));
  }
  await post.enqueueDue();
  expect(jobs).toHaveLength(20);
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
  await post.execute(running(jobs.at(-1)));
  expect(post.deadLetters().letters[0]).toMatchObject({
    key: "post:refused",
    reason: "refused",
    attempts: 1,
    envelope: { key: "refused", event: { text: "refuse" } },
  });
});

it("times out a hung receiver, preserves its retry, and lets the next recipient run", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(100);
  const owner = sqliteOwner();
  const jobs: WatchdogQueuedJob[] = [];
  let release: (result: "accepted") => void = () => undefined;
  const hung = new Promise<"accepted">((resolve) => {
    release = resolve;
  });
  const post = createPost({
    routes: {
      team: definePostRoute({
        addressKind: "team",
        address,
        event,
        name: (to) => to.teamId,
        namespace: {
          getByName: (name) => ({
            deliver: () =>
              name === "hung" ? hung : Promise.resolve("accepted" as const),
          }),
        },
      }),
    },
    owner,
    transactional: {
      enqueueAcceptedJob: (job) => {
        jobs.push(job);
      },
      readQueueHead: () => ({ state: "idle" }),
      readLatestSettledJob: () => undefined,
      recordCancellationIntent: () => ({ status: "missing" }),
    },
    readJob: (id) => settledJob(jobs, id),
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
  const attempt = post.execute(running(jobs[0]));
  await vi.advanceTimersByTimeAsync(10_000);
  await attempt;
  expect(post.read("hung")).toMatchObject({ state: "pending", attempts: 1 });
  await post.execute(running(jobs[1]));
  expect(post.nextWake()).toBe(Date.now() + 2_000);
  expect(post.read("healthy")).toEqual({ state: "delivered" });
  release("accepted");
  await Promise.resolve();
  expect(post.read("hung")).toMatchObject({ state: "pending", attempts: 1 });
  await vi.advanceTimersByTimeAsync(2_000);
  await post.enqueueDue();
  await post.execute(running(jobs.at(-1)));
  expect(post.read("hung")).toEqual({ state: "delivered" });
});

it("orders different event kinds for one recipient through lost receipts without delaying another namespace", async () => {
  const owner = sqliteOwner();
  const jobs: WatchdogQueuedJob[] = [];
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
    transactional: {
      enqueueAcceptedJob: (job: WatchdogQueuedJob) => {
        jobs.push(job);
      },
      readQueueHead: () => ({ state: "idle" as const }),
      readLatestSettledJob: () => undefined,
      recordCancellationIntent: () => ({ status: "missing" as const }),
    },
    readJob: (id: string) => settledJob(jobs, id),
    now: () => now,
    scheduleWake: () => Promise.resolve(),
  };
  let post = createPost(options);
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
  expect(jobs).toHaveLength(2);
  await post.execute(running(jobs[0]));
  await post.execute(running(jobs[1]));
  expect(applied).toEqual(["home", "member"]);
  expect(post.read("control")).toMatchObject({ state: "pending", attempts: 0 });
  expect(post.nextWake()).toBe(2_100);
  post = createPost(options);
  now = 2_100;
  await post.enqueueDue();
  expect(jobs).toHaveLength(3);
  await post.execute(running(jobs[2]));
  expect(jobs).toHaveLength(4);
  await post.execute(running(jobs[3]));
  expect(applied).toEqual(["home", "member", "control"]);
  expect(post.read("home")).toEqual({ state: "delivered", nextKey: "control" });
  expect(post.nextWake()).toBeNull();
});

it("persists a prepared fact before delivery and commits its producer acknowledgment with the receipt", async () => {
  const owner = sqliteOwner();
  const jobs: WatchdogQueuedJob[] = [];
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
    transactional: {
      enqueueAcceptedJob: (job: WatchdogQueuedJob) => {
        jobs.push(job);
      },
      readQueueHead: () => ({ state: "idle" as const }),
      readLatestSettledJob: () => undefined,
      recordCancellationIntent: () => ({ status: "missing" as const }),
    },
    readJob: (id: string) => settledJob(jobs, id),
    now: () => now,
    scheduleWake: () => Promise.resolve(),
  };
  const input = {
    key: "prepared",
    kind: "team",
    address: { teamId: "t1" },
    event: { revision: 1 },
  } as const;
  const post = createPost(options);
  post.send(input);
  await post.execute(running(jobs[0]));
  expect(post.read(input.key)).toMatchObject({
    state: "pending",
    attempts: 1,
    delivery: { event: { grant: "sealed-1", expiresAt: "60100" } },
  });
  const reopened = createPost(options);
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
  await reopened.enqueueDue();
  await expect(reopened.execute(running(jobs.at(-1)))).rejects.toThrow(
    "Producer acknowledgment failed",
  );
  expect(reopened.read(input.key)).toMatchObject({ state: "pending" });
  expect(owner.sql.exec("SELECT * FROM grant_expiry").toArray()).toEqual([]);
  rejectAck = false;
  now = 10_100;
  await reopened.enqueueDue();
  await reopened.execute(running(jobs.at(-1)));
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
  vi.useFakeTimers();
  vi.setSystemTime(100);
  const owner = sqliteOwner();
  const jobs: WatchdogQueuedJob[] = [];
  const received: unknown[] = [];
  let release: (value: { text: string }) => void = () => {
    throw new Error("Preparation did not start");
  };
  const hung = new Promise<{ text: string }>((resolve) => {
    release = resolve;
  });
  let first = true;
  const post = createPost({
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
              return hung;
            }
            return Promise.resolve(intent);
          },
        },
        namespace: {
          getByName: () => ({
            deliver: (envelope: PostEnvelope) => {
              received.push(envelope.event);
              return Promise.resolve("accepted" as const);
            },
          }),
        },
      }),
    },
    owner,
    transactional: {
      enqueueAcceptedJob: (job) => {
        jobs.push(job);
      },
      readQueueHead: () => ({ state: "idle" }),
      readLatestSettledJob: () => undefined,
      recordCancellationIntent: () => ({ status: "missing" }),
    },
    readJob: (id) => settledJob(jobs, id),
    now: Date.now,
    scheduleWake: () => Promise.resolve(),
  });
  post.send({
    key: "hung-preparation",
    kind: "team",
    address: { teamId: "t1" },
    event: { text: "current" },
  });
  const attempt = post.execute(running(jobs[0]));
  await vi.advanceTimersByTimeAsync(10_000);
  await attempt;
  release({ text: "late" });
  await vi.advanceTimersByTimeAsync(0);
  expect(received).toEqual([]);
  expect(post.read("hung-preparation")).toMatchObject({
    state: "pending",
    attempts: 1,
  });
  await vi.advanceTimersByTimeAsync(2_000);
  await post.enqueueDue();
  await post.execute(running(jobs.at(-1)));
  expect(received).toEqual([{ text: "current" }]);
  expect(post.read("hung-preparation")).toEqual({ state: "delivered" });
});
