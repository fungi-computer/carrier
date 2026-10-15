import { fromNodeSqlite } from "@fungi.computer/watchdog/node-sqlite";
import { createD1Post } from "@fungi.computer/carrier/post-d1";
import { createPostgresPost } from "@fungi.computer/carrier/post-postgres";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";

import { createCarrier } from "@fungi.computer/carrier";
import * as productionCarrier from "@fungi.computer/carrier";
import { createMemoryOutbox } from "@fungi.computer/carrier/testing";
import { createCloudflareCarrier } from "@fungi.computer/carrier/cloudflare";
import { createCarrier as createEmailCarrier } from "@fungi.computer/carrier/email";
import { createPost } from "@fungi.computer/carrier/post";
import { createDeliveryInspection } from "@fungi.computer/carrier/inspection";

assert.equal(createEmailCarrier, createCarrier);
assert.ok(createPost);

// Real SQLite for the packed consumer. Native DO durability is qualified by
// the separate Cloudflare fixture; this adapter proves the artifact's interface.
function sqliteFixture() {
  const database = new DatabaseSync(":memory:");
  database.exec(
    "CREATE TABLE fixture_alarm (id INTEGER PRIMARY KEY, due INTEGER)",
  );
  let transaction = 0;
  const begin = () => {
    const name = `fixture_${transaction++}`;
    database.exec(`SAVEPOINT ${name}`);
    return name;
  };
  const commit = (name) => database.exec(`RELEASE ${name}`);
  const rollback = (name) => {
    database.exec(`ROLLBACK TO ${name}`);
    database.exec(`RELEASE ${name}`);
  };
  const storage = {
    ...fromNodeSqlite(database),
    async transaction(operation) {
      const name = begin();
      try {
        const result = await operation();
        commit(name);
        return result;
      } catch (error) {
        rollback(name);
        throw error;
      }
    },
    async getAlarm() {
      return (
        database.prepare("SELECT due FROM fixture_alarm WHERE id=1").get()
          ?.due ?? null
      );
    },
    async setAlarm(when) {
      database
        .prepare(
          "INSERT INTO fixture_alarm VALUES (1,?) ON CONFLICT(id) DO UPDATE SET due=excluded.due",
        )
        .run(Number(when));
    },
    async deleteAlarm() {
      database.exec("DELETE FROM fixture_alarm");
    },
  };
  return { storage, close: () => database.close() };
}

const email = {
  id: "packed-email-1",
  from: "sender@example.com",
  bcc: ["private@example.net"],
  subject: "Packed consumer",
  text: "No source imports or real provider I/O.",
};
const portableStore = sqliteFixture();
const nativeStore = sqliteFixture();
let portableCalls = 0;
let nativeCalls = 0;
try {
  const storage = portableStore.storage;
  const portable = await createCarrier({
    storage,
    now: Date.now,
    host: {
      commit: (mutation) =>
        storage.transaction(async () => {
          const result = mutation();
          await storage.setAlarm(Date.now());
          return result;
        }),
      requestWake: () => storage.setAlarm(Date.now()),
    },
    transport: async () => {
      portableCalls++;
      return {
        kind: "provider_accepted",
        providerMessageId: "portable-receipt",
      };
    },
  });
  assert.equal((await portable.send(email)).status, "queued");
  await portable.tick();
  assert.equal(
    (await portable.get(email.id)).providerMessageId,
    "portable-receipt",
  );
  assert.equal(await portable.hasPendingWork(), false);
  assert.equal(portableCalls, 1);
  const inspection = createDeliveryInspection(storage.sql);
  assert.deepEqual(inspection.email({}), {
    status: "available",
    records: [
      {
        id: email.id,
        status: "provider_accepted",
        attempts: 0,
        retryAt: null,
        providerMessageId: "portable-receipt",
      },
    ],
    nextCursor: null,
  });
  assert.deepEqual(inspection.post({ view: "receipts" }), {
    status: "uninitialized",
  });
  assert.equal(portableCalls, 1);

  const native = await createCloudflareCarrier({
    storage: nativeStore.storage,
    email: {
      send: async (envelope) => {
        nativeCalls++;
        assert.deepEqual(envelope.bcc, email.bcc);
        return { messageId: "native-receipt" };
      },
    },
  });
  const accepted = await native.send(email);
  await native.alarm();
  const sent = await native.get(email.id);
  assert.equal(sent.status, "provider_accepted");
  assert.deepEqual(sent.messages, accepted.messages);
  assert.deepEqual(await native.send(email), sent);
  await native.alarm();
  assert.equal(nativeCalls, 1);
  assert.equal(await nativeStore.storage.getAlarm(), null);
} finally {
  portableStore.close();
  nativeStore.close();
}

export const nativeConstructors = { createD1Post, createPostgresPost };

assert.equal(Object.hasOwn(productionCarrier, "createMemoryOutbox"), false);
const memoryOutbox = createMemoryOutbox();
await memoryOutbox.put("packed:one", "receipt");
assert.deepEqual(await memoryOutbox.list(1), [
  { key: "packed:one", value: "receipt" },
]);
await memoryOutbox.delete("packed:one");
assert.equal(await memoryOutbox.read("packed:one"), undefined);
