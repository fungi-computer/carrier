import { createMemoryOutbox } from "../src/testing.js";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { afterEach, expect, it, vi } from "vitest";

import { makePostOutbox } from "../src/post-outbox.js";

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
