import { afterEach, expect, it } from "vitest";
import * as Schema from "effect/Schema";
import { createPost, definePostRoute } from "../src/post.js";
import {
  createDeliveryInspection,
  DeliveryInspectionError,
} from "../src/inspection.js";
import type { WatchdogQueuedJob } from "@fungi.computer/watchdog";
import { createCarrier } from "../src/index.js";
import { createSqliteOwnerFixture } from "../../watchdog/test/sqlite-owner.fixture.js";

const fixtures: ReturnType<typeof createSqliteOwnerFixture>[] = [];
afterEach(() => {
  for (const fixture of fixtures.splice(0)) fixture.close();
});

it("reads retained Post evidence as bounded metadata without dispatching", async () => {
  const fixture = createSqliteOwnerFixture();
  fixtures.push(fixture);
  const jobs: WatchdogQueuedJob[] = [];
  let deliveries = 0;
  let wakes = 0;
  let refused = false;
  const post = createPost({
    owner: fixture.owner,
    routes: {
      destination: definePostRoute({
        addressKind: "destination",
        address: Schema.Struct({ name: Schema.String }),
        event: Schema.Struct({ secret: Schema.String }),
        name: (address) => address.name,
        namespace: {
          getByName: () => ({
            deliver: () => {
              deliveries += 1;
              return Promise.resolve(
                refused ? ("refused" as const) : ("accepted" as const),
              );
            },
          }),
        },
      }),
    },
    transactional: {
      enqueueAcceptedJob: (job) => jobs.push(job),
      readQueueHead: () => ({ state: "idle" }),
      readLatestSettledJob: () => undefined,
      recordCancellationIntent: () => ({ status: "missing" }),
    },
    readJob: () => Promise.resolve({ status: "missing" }),
    now: () => 100,
    scheduleWake: () => {
      wakes += 1;
      return Promise.resolve();
    },
  });
  post.send({
    key: "a",
    kind: "destination",
    address: { name: "one" },
    event: { secret: "private event" },
  });
  post.send({
    key: "b",
    kind: "destination",
    address: { name: "two" },
    event: { secret: "private event" },
  });
  const job = jobs[0];
  if (job === undefined) throw new Error("Post did not queue the delivery");
  await post.execute({ ...job, state: "running" });

  // A fresh reader has no sender, Watchdog runtime or wake capability.
  const inspection = createDeliveryInspection(fixture.owner.sql);
  expect(inspection.post({ view: "receipts", limit: 1 })).toEqual({
    status: "available",
    records: [{ key: "a", state: "delivered" }],
    nextCursor: "a",
  });
  expect(inspection.post({ view: "receipts", limit: 1, after: "a" })).toEqual({
    status: "available",
    records: [
      {
        key: "b",
        state: "pending",
        kind: "destination",
        attempts: 0,
        nextAttemptAt: 100,
      },
    ],
    nextCursor: "b",
  });
  expect(inspection.post({ view: "receipts", after: "b" })).toEqual({
    status: "available",
    records: [],
    nextCursor: null,
  });
  expect(deliveries).toBe(1);
  expect(wakes).toBe(1);
  expect(jobs).toHaveLength(2);

  refused = true;
  post.send({
    key: "c",
    kind: "destination",
    address: { name: "three" },
    event: { secret: "private event" },
  });
  const refusalJob = jobs.at(-1);
  if (refusalJob === undefined)
    throw new Error("Post did not queue the refusal");
  await post.execute({ ...refusalJob, state: "running" });
  expect(inspection.post({ view: "deadLetters", limit: 1 })).toEqual({
    status: "available",
    records: [],
    nextCursor: "a",
  });
  expect(
    inspection.post({ view: "deadLetters", limit: 1, after: "a" }),
  ).toEqual({ status: "available", records: [], nextCursor: "b" });
  expect(
    createDeliveryInspection(fixture.owner.sql).post({
      view: "deadLetters",
      after: "b",
    }),
  ).toEqual({
    status: "available",
    records: [
      {
        key: "c",
        state: "dead",
        kind: "destination",
        attempts: 1,
        reason: "refused",
      },
    ],
    nextCursor: null,
  });
});

it("distinguishes uninitialized storage and rejects scans outside the bound", () => {
  const fixture = createSqliteOwnerFixture();
  fixtures.push(fixture);
  const inspection = createDeliveryInspection(fixture.owner.sql);
  expect(inspection.post({ view: "receipts" })).toEqual({
    status: "uninitialized",
  });
  for (const limit of [0, -1, 65, 1.5, Infinity]) {
    expect(() => inspection.post({ view: "receipts", limit })).toThrow(
      new DeliveryInspectionError("invalid_query"),
    );
  }
  expect(() =>
    inspection.post({ view: "receipts", after: "x".repeat(257) }),
  ).toThrow(new DeliveryInspectionError("invalid_query"));
});

it("reads Carrier receipt metadata without reopening the transport or disclosing email content", async () => {
  const fixture = createSqliteOwnerFixture();
  fixtures.push(fixture);
  let calls = 0;
  let wakes = 0;
  const carrier = await createCarrier({
    storage: fixture.owner,
    now: () => 100,
    host: {
      commit: (mutation) => Promise.resolve(mutation()),
      requestWake: () => {
        wakes += 1;
        return Promise.resolve();
      },
    },
    transport: () => {
      calls += 1;
      return Promise.resolve({
        kind: "provider_accepted",
        providerMessageId: "provider-one",
      });
    },
  });
  await carrier.send({
    id: "a",
    from: "sender@example.test",
    to: ["recipient@example.test"],
    subject: "Private subject",
    text: "Private token",
  });
  await carrier.tick();
  await carrier.send({
    id: "b",
    from: "sender@example.test",
    to: ["recipient@example.test"],
    subject: "Private subject",
    text: "Private token",
  });
  wakes = 0;
  const inspection = createDeliveryInspection(fixture.owner.sql);
  expect(inspection.email({ limit: 1 })).toEqual({
    status: "available",
    records: [
      {
        id: "a",
        status: "provider_accepted",
        attempts: 0,
        providerMessageId: "provider-one",
        retryAt: null,
      },
    ],
    nextCursor: "a",
  });
  expect(inspection.email({ after: "a" })).toEqual({
    status: "available",
    records: [{ id: "b", status: "queued", attempts: 0, retryAt: null }],
    nextCursor: null,
  });
  expect(calls).toBe(1);
  expect(wakes).toBe(0);
});
