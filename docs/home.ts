import { DatabaseSync } from "node:sqlite";
import { fromNodeSqlite } from "@fungi.computer/watchdog/node-sqlite";
import { createCarrier } from "@fungi.computer/carrier";

const carrier = await createCarrier({
  storage: fromNodeSqlite(new DatabaseSync(":memory:")),
  now: Date.now,
  host: { commit: async (mutation) => mutation(), requestWake: async () => {} },
  transport: async () => ({
    kind: "provider_accepted",
    providerMessageId: "demo-1",
  }),
});

const receipt = await carrier.send({
  id: "welcome:1",
  from: "hello@example.com",
  to: ["ada@example.net"],
  subject: "Welcome",
  text: "Ready.",
});
await carrier.tick();
console.log((await carrier.get(receipt.id))?.status); // "provider_accepted"
