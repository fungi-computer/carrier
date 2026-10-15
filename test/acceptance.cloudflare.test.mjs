import {
  env,
  evictDurableObject,
  runDurableObjectAlarm,
} from "cloudflare:test";
import { expect, it } from "vitest";

// RPC promises retain a remote call lifetime even after rejection. Release it
// before asking the native runtime to evict the object.
async function expectRpcRejection(operation, message) {
  try {
    await expect(Promise.resolve(operation)).rejects.toThrow(message);
  } finally {
    operation[Symbol.dispose]();
  }
}

const ordinary = () => ({
  id: globalThis.crypto.randomUUID(),
  from: "sender@example.com",
  bcc: ["private@example.net"],
  subject: "Ordinary email",
  text: "No fictional invitation expiry or validity required.",
  headers: { "List-Unsubscribe-Post": "List-Unsubscribe=One-Click" },
});

it("inspects native storage without initializing mail, changing alarms or exposing cargo", async () => {
  const owner = env.CARRIER.getByName(globalThis.crypto.randomUUID());
  expect(await owner.receipts({ limit: 1 })).toEqual({
    status: "uninitialized",
  });
  expect(await owner.receipts({ limit: 1 })).toEqual({
    status: "uninitialized",
  });
  expect(await owner.inspect()).toEqual({
    calls: [],
    validityChecks: [],
    alarm: null,
  });
  const email = ordinary();
  await owner.send(email);
  const before = await owner.inspect();
  await evictDurableObject(owner);
  expect(await owner.receipts({ limit: 1 })).toEqual({
    status: "available",
    records: [{ id: email.id, status: "queued", attempts: 0, retryAt: null }],
    nextCursor: email.id,
  });
  expect(await owner.inspect()).toEqual(before);
  await runDurableObjectAlarm(owner);
  await evictDurableObject(owner);
  expect(await owner.receipts({})).toEqual({
    status: "available",
    records: [
      {
        id: email.id,
        status: "provider_accepted",
        attempts: 0,
        retryAt: null,
        providerMessageId: "controlled-provider-receipt",
      },
    ],
    nextCursor: null,
  });
});

it("uses a supplied transport with native persistence and alarms without an email binding", async () => {
  const owner = env.CARRIER_TRANSPORT.getByName(globalThis.crypto.randomUUID());
  const email = ordinary();
  const accepted = await owner.send(email);
  await evictDurableObject(owner);
  expect(await owner.send(email)).toEqual(accepted);
  expect(await runDurableObjectAlarm(owner)).toBe(true);
  const receipt = await owner.get(email.id);
  expect(receipt.providerMessageId).toBe("alternate-provider-receipt");
  expect(receipt.messages).toEqual(accepted.messages);
  expect((await owner.inspect()).calls).toEqual([
    {
      from: email.from,
      bcc: email.bcc,
      subject: email.subject,
      text: email.text,
      headers: email.headers,
    },
  ]);
  expect((await owner.inspect()).alarm).toBeNull();
});

it("accepts once, keeps recipient identities across instance eviction, and drains through its native alarm", async () => {
  const name = globalThis.crypto.randomUUID();
  let owner = env.CARRIER.getByName(name);
  const submission = {
    id: "invitation-outbox-1",
    sendBefore: Date.now() + 60_000,
    from: { email: "sender@example.com", name: "Example" },
    to: ["first@example.net", "second@example.net"],
    cc: ["copy@example.net"],
    bcc: ["private@example.net"],
    subject: "Invitation",
    text: "Controlled invitation content",
  };

  const accepted = await owner.send(submission);
  expect(accepted.status).toBe("queued");
  expect(accepted.messages).toHaveLength(4);
  expect(new Set(accepted.messages.map((message) => message.id)).size).toBe(4);
  expect((await owner.inspect()).alarm).not.toBeNull();

  await evictDurableObject(owner);
  expect(await owner.send(submission)).toEqual(accepted);
  await expectRpcRejection(
    owner.send({ ...submission, subject: "Conflicting content" }),
    "conflict",
  );
  owner = env.CARRIER.getByName(name);
  expect(await runDurableObjectAlarm(owner)).toBe(true);

  const receipt = await owner.get(submission.id);
  expect(receipt.status).toBe("provider_accepted");
  expect(receipt.providerMessageId).toBe("controlled-provider-receipt");
  expect(receipt.messages).toEqual(accepted.messages);
  expect(
    receipt.messages.every((message) => message.status === "unconfirmed"),
  ).toBe(true);
  const observed = await owner.inspect();
  expect(observed.calls).toEqual([
    {
      from: submission.from,
      to: submission.to,
      cc: submission.cc,
      bcc: submission.bcc,
      subject: submission.subject,
      text: submission.text,
    },
  ]);
  expect(observed.alarm).toBeNull();

  await evictDurableObject(owner);
  await owner.drain();
  expect((await owner.inspect()).calls).toHaveLength(1);
});

it("can recover an interrupted dispatch that never reached its marker", async () => {
  const owner = env.CARRIER.getByName(globalThis.crypto.randomUUID());
  const email = ordinary();
  await owner.configure("before_dispatch");
  await owner.send(email);
  await owner.drain();
  expect((await owner.inspect()).calls).toHaveLength(0);
  await evictDurableObject(owner);
  await owner.drain();
  expect((await owner.inspect()).calls).toHaveLength(1);
  expect((await owner.get(email.id)).status).toBe("provider_accepted");
});

it("does not resend after a possible provider acceptance with a lost response", async () => {
  const owner = env.CARRIER.getByName(globalThis.crypto.randomUUID());
  const email = ordinary();
  await owner.configure("response_lost");
  await owner.send(email);
  await owner.drain();
  expect((await owner.get(email.id)).status).toBe("outcome_unknown");
  await evictDurableObject(owner);
  await owner.drain();
  expect((await owner.inspect()).calls).toHaveLength(1);
});

it("retains a provider receipt when Watchdog settlement fails afterward", async () => {
  const name = globalThis.crypto.randomUUID();
  let owner = env.CARRIER.getByName(name);
  const email = ordinary();
  await owner.configure("receipt_saved");
  await owner.send(email);
  await expectRpcRejection(owner.drain());
  owner = env.CARRIER.getByName(name);
  expect((await owner.get(email.id)).status).toBe("provider_accepted");
  await evictDurableObject(owner);
  await owner.drain();
  expect((await owner.get(email.id)).providerMessageId).toBe(
    "controlled-provider-receipt",
  );
  expect((await owner.inspect()).calls).toHaveLength(1);
});

it("does not dispatch an expired submission", async () => {
  const owner = env.CARRIER.getByName(globalThis.crypto.randomUUID());
  const email = { ...ordinary(), sendBefore: Date.now() - 1 };
  await owner.send(email);
  await owner.drain();
  expect((await owner.get(email.id)).status).toBe("expired");
  expect((await owner.inspect()).calls).toHaveLength(0);
});

it("recovers an instance terminated during provider I/O without a second invocation", async () => {
  const name = globalThis.crypto.randomUUID();
  let owner = env.CARRIER.getByName(name);
  const email = ordinary();
  await owner.configure("interrupted_send");
  await owner.send(email);
  await expectRpcRejection(owner.drain());
  owner = env.CARRIER.getByName(name);
  expect((await owner.get(email.id)).status).toBe("dispatching");
  await owner.drain();
  expect((await owner.get(email.id)).status).toBe("outcome_unknown");
  expect((await owner.inspect()).calls).toHaveLength(1);
  expect((await owner.inspect()).alarm).toBeNull();
});

it("does not acknowledge or retain a submission when its durable wake cannot commit", async () => {
  const name = globalThis.crypto.randomUUID();
  let owner = env.CARRIER.getByName(name);
  const email = ordinary();
  await owner.configure("acceptance_wake_failed");
  await expectRpcRejection(owner.send(email), "unavailable");
  owner = env.CARRIER.getByName(name);
  expect(await owner.get(email.id)).toBeUndefined();
  expect((await owner.inspect()).alarm).toBeNull();
  expect((await owner.inspect()).calls).toEqual([]);
  await evictDurableObject(owner);
  expect((await owner.send(email)).status).toBe("queued");
  await runDurableObjectAlarm(owner);
  expect((await owner.get(email.id)).status).toBe("provider_accepted");
  expect((await owner.inspect()).calls).toHaveLength(1);
});

it("recovers a lost durable acceptance response under the same submission identity", async () => {
  const name = globalThis.crypto.randomUUID();
  let owner = env.CARRIER.getByName(name);
  const email = ordinary();
  await owner.configure("acceptance_response_lost");
  await expectRpcRejection(owner.send(email));
  owner = env.CARRIER.getByName(name);
  const committed = await owner.get(email.id);
  expect(committed.status).toBe("queued");
  expect(await owner.send(email)).toEqual(committed);
  await runDurableObjectAlarm(owner);
  expect((await owner.inspect()).calls).toHaveLength(1);
  const sent = await owner.get(email.id);
  expect(sent.messages).toEqual(committed.messages);
  expect(await owner.send(email)).toEqual(sent);
  await owner.drain();
  expect((await owner.inspect()).calls).toHaveLength(1);
});

it("keeps source-unavailable work queued and rechecks validity after recovery", async () => {
  const owner = env.CARRIER.getByName(globalThis.crypto.randomUUID());
  const email = ordinary();
  await owner.configure("validity_unavailable");
  const accepted = await owner.send(email);
  await runDurableObjectAlarm(owner);
  expect((await owner.get(email.id)).status).toBe("queued");
  expect((await owner.inspect()).calls).toEqual([]);
  expect((await owner.inspect()).alarm).not.toBeNull();
  await evictDurableObject(owner);
  await runDurableObjectAlarm(owner);
  expect((await owner.get(email.id)).status).toBe("provider_accepted");
  expect((await owner.get(email.id)).messages).toEqual(accepted.messages);
  expect((await owner.inspect()).validityChecks).toEqual([email.id, email.id]);
  expect((await owner.inspect()).calls).toHaveLength(1);
});

it("does not dispatch an invitation the source has superseded after acceptance", async () => {
  const owner = env.CARRIER.getByName(globalThis.crypto.randomUUID());
  const email = ordinary();
  await owner.send(email);
  await owner.configure("superseded");
  await runDurableObjectAlarm(owner);
  expect((await owner.get(email.id)).status).toBe("superseded");
  expect((await owner.inspect()).validityChecks).toEqual([email.id]);
  expect((await owner.inspect()).calls).toEqual([]);
  await evictDurableObject(owner);
  expect((await owner.send(email)).status).toBe("superseded");
  await owner.drain();
  expect((await owner.inspect()).calls).toEqual([]);
});

it("retains one recipient Message identity across repeated visible and blind roles", async () => {
  const owner = env.CARRIER.getByName(globalThis.crypto.randomUUID());
  const email = {
    ...ordinary(),
    to: [
      { email: "shared@example.net", name: "Visible Name" },
      "shared@example.net",
    ],
    cc: ["shared@example.net"],
    bcc: ["private@example.net", "shared@example.net"],
  };
  const accepted = await owner.send(email);
  expect(accepted.messages).toHaveLength(2);
  expect(
    accepted.messages.map(({ recipient, roles, status }) => ({
      recipient,
      roles,
      status,
    })),
  ).toEqual([
    {
      recipient: "shared@example.net",
      roles: ["to", "cc", "bcc"],
      status: "unconfirmed",
    },
    { recipient: "private@example.net", roles: ["bcc"], status: "unconfirmed" },
  ]);
  await runDurableObjectAlarm(owner);
  const sent = await owner.get(email.id);
  expect(sent.messages).toEqual(accepted.messages);
  expect((await owner.inspect()).calls).toEqual([
    {
      from: email.from,
      to: email.to,
      cc: email.cc,
      bcc: email.bcc,
      subject: email.subject,
      text: email.text,
      headers: email.headers,
    },
  ]);
});
