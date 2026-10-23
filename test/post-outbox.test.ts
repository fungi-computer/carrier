import { createMemoryOutbox } from "../src/testing.js";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { afterEach, expect, it, vi } from "vitest";

import {
  makePostOutbox,
  maxDeliveryAttempts,
  postOutboxPending,
} from "../src/post-outbox.js";

afterEach(() => vi.restoreAllMocks());

it("reports delivery failures and keeps them on the bounded retry path", async () => {
  const rows = createMemoryOutbox();
  const wakes: number[] = [];
  const logged = vi
    .spyOn(globalThis.console, "error")
    .mockImplementation(() => undefined);
  const outbox = makePostOutbox({
    message: Schema.Struct({ key: Schema.String }),
    key: ({ key }) => key,
    outbox: {
      read: (key) => Effect.promise(() => rows.read(key)),
      put: (key, value) => Effect.promise(() => rows.put(key, value)),
      list: (limit, afterKey) =>
        Effect.promise(() => rows.list(limit, afterKey)),
      delete: (key) => Effect.promise(() => rows.delete(key)),
    },
    deliver: () => Effect.fail(new Error("private provider detail")),
    scheduleWake: (at) => Effect.sync(() => void wakes.push(at)),
    now: () => 100,
    invalidRow: () => new Error("invalid row"),
    encodingFailed: () => new Error("encoding failed"),
  });

  await Effect.runPromise(outbox.enqueue({ key: "event:one" }));
  await Effect.runPromise(outbox.drain());

  expect(JSON.parse((await rows.read("event:one")) ?? "null")).toMatchObject({
    attempts: 1,
    nextAttemptAt: 2_100,
  });
  expect(wakes).toEqual([2_100]);
  expect(logged).toHaveBeenCalledExactlyOnceWith(
    "Carrier Post outbox delivery failed",
    { failureKind: "Error" },
  );
});

it("stores the same doubled delay and caps later receiver retries", async () => {
  const rows = createMemoryOutbox();
  const outbox = makePostOutbox({
    message: Schema.Struct({ key: Schema.String }),
    key: ({ key }) => key,
    outbox: {
      read: (key) => Effect.promise(() => rows.read(key)),
      put: (key, value) => Effect.promise(() => rows.put(key, value)),
      list: (limit, afterKey) =>
        Effect.promise(() => rows.list(limit, afterKey)),
      delete: (key) => Effect.promise(() => rows.delete(key)),
    },
    deliver: () => Effect.succeed(false),
    scheduleWake: () => Effect.void,
    now: () => 100,
    invalidRow: () => new Error("invalid row"),
    encodingFailed: () => new Error("encoding failed"),
  });
  await rows.put(
    "event:two",
    JSON.stringify({ mail: { key: "event:two" }, attempts: 1 }),
  );
  await Effect.runPromise(outbox.drain());
  expect(JSON.parse((await rows.read("event:two")) ?? "null")).toMatchObject({
    attempts: 2,
    nextAttemptAt: 4_100,
  });
  await rows.put(
    "event:cap",
    JSON.stringify({ mail: { key: "event:cap" }, attempts: 8 }),
  );
  await Effect.runPromise(outbox.drain());
  expect(JSON.parse((await rows.read("event:cap")) ?? "null")).toMatchObject({
    attempts: 9,
    nextAttemptAt: 300_100,
  });
});

it("reads the earliest owed wake across pages, excluding receipts and dead letters", async () => {
  const rows = createMemoryOutbox();
  const outbox = makePostOutbox({
    message: Schema.Struct({ key: Schema.String }),
    key: ({ key }) => key,
    outbox: {
      read: (key) => Effect.promise(() => rows.read(key)),
      put: (key, value) => Effect.promise(() => rows.put(key, value)),
      list: (limit, afterKey) =>
        Effect.promise(() => rows.list(limit, afterKey)),
      delete: (key) => Effect.promise(() => rows.delete(key)),
    },
    deliver: () => Effect.succeed(true),
    scheduleWake: () => Effect.void,
    now: () => 100,
    invalidRow: () => new Error("invalid row"),
    encodingFailed: () => new Error("encoding failed"),
  });
  expect(await Effect.runPromise(outbox.nextWakeAt())).toBeUndefined();
  for (let index = 0; index < 70; index += 1)
    await rows.put(
      `a:${String(index).padStart(3, "0")}`,
      JSON.stringify({ state: "delivered" }),
    );
  await rows.put("b:invalid", "{");
  await rows.put(
    "b:dead",
    JSON.stringify({
      mail: { key: "b:dead" },
      attempts: maxDeliveryAttempts,
      nextAttemptAt: 0,
    }),
  );
  expect(await Effect.runPromise(outbox.nextWakeAt())).toBeUndefined();
  await rows.put(
    "c:later",
    JSON.stringify({
      mail: { key: "c:later" },
      attempts: 1,
      nextAttemptAt: 2_100,
    }),
  );
  await rows.put(
    "z:earlier",
    JSON.stringify({
      mail: { key: "z:earlier" },
      attempts: 2,
      nextAttemptAt: 1_100,
    }),
  );
  expect(await Effect.runPromise(outbox.nextWakeAt())).toBe(1_100);
  await rows.put(
    "z:now",
    JSON.stringify({ mail: { key: "z:now" }, attempts: 0 }),
  );
  expect(await Effect.runPromise(outbox.nextWakeAt())).toBe(100);
  await Effect.runPromise(outbox.drain());
  await Effect.runPromise(outbox.drain());
  expect(await Effect.runPromise(outbox.nextWakeAt())).toBe(1_100);
});

it("owes delivery only for undelivered, unexhausted rows, across pages", async () => {
  const rows = createMemoryOutbox();
  const message = Schema.Struct({ key: Schema.String });
  const outbox = {
    list: (limit: number, afterKey?: string) =>
      Effect.promise(() => rows.list(limit, afterKey)),
  };
  const pending = () =>
    Effect.runPromise(postOutboxPending({ message, outbox }));
  expect(await pending()).toBe(false);
  // Receipts, exhausted dead letters and malformed rows owe nothing.
  for (let index = 0; index < 70; index += 1)
    await rows.put(
      `a:${String(index).padStart(3, "0")}`,
      JSON.stringify({ state: "delivered" }),
    );
  await rows.put(
    "b:dead",
    JSON.stringify({ mail: { key: "b:dead" }, attempts: maxDeliveryAttempts }),
  );
  await rows.put("b:malformed", "{");
  expect(await pending()).toBe(false);
  // A retrying row past the first page is still owed.
  await rows.put(
    "c:retry",
    JSON.stringify({ mail: { key: "c:retry" }, attempts: 3 }),
  );
  expect(await pending()).toBe(true);
});
