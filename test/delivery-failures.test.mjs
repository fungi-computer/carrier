import { fromNodeSqlite } from "@fungi.computer/watchdog/node-sqlite";
import { DatabaseSync } from "node:sqlite";

import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { createCloudflareCarrier } from "../src/cloudflare.js";
import { carrierPolicy } from "../src/internal/policy.js";

// Real in-memory SQLite and a stored alarm stand in for one Durable Object;
// the native fixture qualifies the runtime itself.
function durableObjectStorage() {
  const database = new DatabaseSync(":memory:");
  let alarm = null;
  let depth = 0;
  const atomic = (operation) => {
    const name = `fixture_${depth++}`;
    database.exec(`SAVEPOINT ${name}`);
    const settle = (commit) => {
      depth--;
      if (!commit) database.exec(`ROLLBACK TO ${name}`);
      database.exec(`RELEASE ${name}`);
    };
    try {
      const result = operation();
      if (result instanceof Promise)
        return result.then(
          (value) => (settle(true), value),
          (error) => {
            settle(false);
            throw error;
          },
        );
      settle(true);
      return result;
    } catch (error) {
      settle(false);
      throw error;
    }
  };
  return {
    ...fromNodeSqlite(database),
    transaction: (operation) => atomic(operation),
    getAlarm: async () => alarm,
    setAlarm: async (when) => {
      alarm = Number(when);
    },
    deleteAlarm: async () => {
      alarm = null;
    },
    close: () => database.close(),
  };
}

const providerError = (code) =>
  Object.assign(new Error(`Controlled provider refusal ${code}`), { code });

let storage;
let calls;
let replies;
let logged;
let carrier;

beforeEach(async () => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(Date.UTC(2026, 8, 25));
  logged = vi
    .spyOn(globalThis.console, "error")
    .mockImplementation(() => undefined);
  storage = durableObjectStorage();
  calls = 0;
  replies = [];
  carrier = await createCloudflareCarrier({
    storage,
    email: {
      send: async () => {
        calls++;
        const reply = replies.shift();
        if (reply instanceof Error) throw reply;
        return { messageId: `provider-${calls}` };
      },
    },
  });
});

afterEach(() => {
  storage.close();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

const submission = (id, to = "person@example.net") => ({
  id,
  from: "sender@example.com",
  to: [to],
  subject: "Controlled",
  text: "No live provider is contacted.",
});
// One alarm settles the current job; the next re-enqueues due work and runs it.
const drain = async () => {
  await carrier.alarm();
  await carrier.alarm();
};

it("does not dispatch a queued Email erased while source admission waits", async () => {
  storage.close();
  storage = durableObjectStorage();
  let entered;
  let release;
  const waiting = new Promise((resolve) => {
    entered = resolve;
  });
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  carrier = await createCloudflareCarrier({
    storage,
    canSend: async () => {
      entered();
      await gate;
      return true;
    },
    email: {
      send: async () => {
        calls++;
        return { messageId: "should-not-send" };
      },
    },
  });
  await carrier.send(submission("erased-during-check"));
  const alarm = carrier.alarm();
  await waiting;
  storage.transactionSync(() => {
    storage.sql.exec(
      "DELETE FROM carrier_messages WHERE email_id=?",
      "erased-during-check",
    );
    storage.sql.exec(
      "DELETE FROM carrier_emails WHERE id=?",
      "erased-during-check",
    );
  });
  release();
  await alarm;
  expect(calls).toBe(0);
  expect(await carrier.get("erased-during-check")).toBeUndefined();
});

it("retries a temporary provider rejection after backoff under the same owner", async () => {
  replies.push(providerError("E_RATE_LIMIT_EXCEEDED"));
  await carrier.send(submission("temporary-1"));
  await drain();

  expect(await carrier.get("temporary-1")).toMatchObject({
    status: "queued",
    providerRejection: { code: "E_RATE_LIMIT_EXCEEDED", class: "temporary" },
  });
  expect(calls).toBe(1);
  expect(await storage.getAlarm()).not.toBeNull();
  expect(logged).toHaveBeenCalledExactlyOnceWith("Carrier delivery failed", {
    id: "temporary-1",
    class: "temporary",
    cause: "E_RATE_LIMIT_EXCEEDED",
    status: "queued",
    attempt: 1,
  });

  vi.advanceTimersByTime(carrierPolicy.retry.baseDelayMs - 1);
  await drain();
  expect(calls).toBe(1);

  vi.advanceTimersByTime(1);
  await drain();
  expect(calls).toBe(2);
  const sent = await carrier.get("temporary-1");
  expect(sent.status).toBe("provider_accepted");
  expect(sent.providerMessageId).toBe("provider-2");
  expect(await storage.getAlarm()).toBeNull();
});

it("finishes a permanent provider rejection without another invocation and logs it", async () => {
  replies.push(providerError("E_SENDER_NOT_VERIFIED"));
  await carrier.send(submission("permanent-1"));
  await drain();
  vi.advanceTimersByTime(carrierPolicy.retry.maxDelayMs);
  await drain();

  expect(await carrier.get("permanent-1")).toMatchObject({
    status: "rejected",
    providerRejection: { code: "E_SENDER_NOT_VERIFIED", class: "permanent" },
  });
  expect(calls).toBe(1);
  expect(await storage.getAlarm()).toBeNull();
  expect(logged).toHaveBeenCalledExactlyOnceWith("Carrier delivery failed", {
    id: "permanent-1",
    class: "permanent",
    cause: "E_SENDER_NOT_VERIFIED",
    status: "rejected",
    attempt: 1,
  });
  expect(JSON.stringify(logged.mock.calls)).not.toContain("person@example.net");
});

it("rejects after the retry budget of temporary rejections is spent", async () => {
  const { maxAttempts, maxDelayMs } = carrierPolicy.retry;
  for (let attempt = 0; attempt < maxAttempts; attempt++)
    replies.push(providerError("E_DAILY_LIMIT_EXCEEDED"));
  await carrier.send(submission("exhausted-1"));
  for (let attempt = 0; attempt < maxAttempts + 1; attempt++) {
    await drain();
    vi.advanceTimersByTime(maxDelayMs);
  }

  expect(calls).toBe(maxAttempts);
  expect((await carrier.get("exhausted-1")).status).toBe("rejected");
  expect(logged).toHaveBeenLastCalledWith("Carrier delivery failed", {
    id: "exhausted-1",
    class: "temporary",
    cause: "E_DAILY_LIMIT_EXCEEDED",
    status: "rejected",
    attempt: maxAttempts,
  });
});

it("keeps an unclassified provider failure unknown and never resends it", async () => {
  replies.push(providerError("E_DELIVERY_FAILED"));
  await carrier.send(submission("unknown-1"));
  await drain();
  vi.advanceTimersByTime(carrierPolicy.retry.maxDelayMs);
  await drain();

  const receipt = await carrier.get("unknown-1");
  expect(receipt.status).toBe("outcome_unknown");
  expect(receipt).not.toHaveProperty("providerRejection");
  expect(calls).toBe(1);
  expect(logged).toHaveBeenCalledExactlyOnceWith("Carrier delivery failed", {
    id: "unknown-1",
    class: "unknown",
    cause: "E_DELIVERY_FAILED",
    status: "outcome_unknown",
  });
});

it("refuses a submission over the per-recipient cap without a provider attempt", async () => {
  const { messages, windowMs } = carrierPolicy.recipientCap;
  for (let index = 0; index < messages; index++)
    expect((await carrier.send(submission(`cap-${index}`))).status).toBe(
      "queued",
    );

  // Address spelling does not escape the cap.
  const refused = await carrier.send(
    submission("cap-over", " Person@Example.NET"),
  );
  expect(refused.status).toBe("refused");
  expect(
    await carrier.send(submission("cap-over", " Person@Example.NET")),
  ).toEqual(refused);
  expect(logged).toHaveBeenCalledExactlyOnceWith("Carrier delivery failed", {
    id: "cap-over",
    class: "recipient_cap",
    cause: "recipient_cap_reached",
    status: "refused",
  });
  for (let index = 0; index <= messages; index++) await carrier.alarm();
  expect(calls).toBe(messages);
  expect((await carrier.get("cap-over")).status).toBe("refused");

  // Another recipient is unaffected, and the window rolls forward.
  expect(
    (await carrier.send(submission("other-1", "other@example.net"))).status,
  ).toBe("queued");
  vi.advanceTimersByTime(windowMs);
  expect((await carrier.send(submission("cap-later"))).status).toBe("queued");
});
