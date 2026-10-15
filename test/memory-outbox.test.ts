import { expect, it } from "vitest";
import { createMemoryOutbox } from "../src/testing.js";

it("pages retained string rows by key and replaces and deletes them", async () => {
  const outbox = createMemoryOutbox();
  await outbox.put("c", "third");
  await outbox.put("a", "first");
  await outbox.put("b", "second");
  expect(await outbox.list(2)).toEqual([
    { key: "a", value: "first" },
    { key: "b", value: "second" },
  ]);
  expect(await outbox.list(2, "b")).toEqual([{ key: "c", value: "third" }]);
  await outbox.put("b", "replacement");
  expect(await outbox.read("b")).toBe("replacement");
  await outbox.delete("b");
  expect(await outbox.read("b")).toBeUndefined();
  expect(await outbox.list(2, "a")).toEqual([{ key: "c", value: "third" }]);
});

it("keeps each memory outbox independent", async () => {
  const first = createMemoryOutbox();
  const second = createMemoryOutbox();
  await first.put("receipt", "delivered");
  expect(await second.read("receipt")).toBeUndefined();
  expect(await second.list(10)).toEqual([]);
});
