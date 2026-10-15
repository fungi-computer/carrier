import type {
  DurableObjectStorage,
  EmailMessageBuilder,
  SendEmail,
} from "@cloudflare/workers-types";
import type { WatchdogSqliteOwner } from "@fungi.computer/watchdog";

import { createCarrier, type Carrier, type Options } from "./index.js";
import { sendCloudflareEmail } from "./internal/cloudflare-send.js";

/** Native SQLite and alarms; the Cloudflare adapter owns their atomic composition. */
export type CloudflareCarrierStorage = WatchdogSqliteOwner &
  Pick<
    DurableObjectStorage,
    "transaction" | "getAlarm" | "setAlarm" | "deleteAlarm"
  >;

/** Native storage/wake composition with exactly one provider capability. */
export type CloudflareCarrierOptions = Readonly<{
  storage: CloudflareCarrierStorage;
  /** Return false only for a definitely obsolete source; throw when validity is unavailable. */
  canSend?: (emailId: string) => Promise<boolean>;
}> &
  (
    | Readonly<{ email: SendEmail; transport?: never }>
    | Readonly<{ transport: Options["transport"]; email?: never }>
  );

/** Ordinary submission operations and the native Durable Object alarm door. */
export type CloudflareCarrier = Pick<Carrier, "send" | "get"> &
  Readonly<{
    /** Forward the Durable Object's native alarm to dispatch and rearm pending work. */
    alarm: () => Promise<void>;
  }>;

/**
 * Construct one owner per Durable Object. Forward its native alarm to `alarm`.
 * Acceptance commits the Email, recipient Messages, Watchdog job and wake together.
 */
export async function createCloudflareCarrier(
  options: CloudflareCarrierOptions,
): Promise<CloudflareCarrier> {
  const storage = options.storage;
  const arm = async () => {
    const current = await storage.getAlarm();
    const recoveryAt = Date.now() + 5_000;
    await storage.setAlarm(
      current === null ? recoveryAt : Math.min(current, recoveryAt),
    );
  };
  const configuration: Options = {
    storage,
    now: Date.now,
    transport:
      options.email === undefined
        ? options.transport
        : cloudflareEmailTransport(options.email),
    host: {
      commit: (mutation) =>
        storage.transaction(async () => {
          const result = mutation();
          await arm();
          return result;
        }),
      requestWake: arm,
    },
  };
  const carrier = await createCarrier(
    options.canSend === undefined
      ? configuration
      : { ...configuration, canSend: options.canSend },
  );
  if (await carrier.hasPendingWork()) await arm();
  let active: Promise<void> | undefined;
  const alarm = () => {
    if (active !== undefined) return active;
    const operation = (async () => {
      // Re-arm before external I/O so recovery survives an interrupted alarm.
      await arm();
      await carrier.tick();
      await storage.transaction(async () => {
        if (await carrier.hasPendingWork()) await arm();
        else await storage.deleteAlarm();
      });
    })();
    active = operation;
    return operation.finally(() => {
      active = undefined;
    });
  };
  return { send: carrier.send, get: carrier.get, alarm };
}

/**
 * The native binding as a Carrier transport. Exported so a host that holds the
 * binding elsewhere (the Hub's Cloudflare edge) classifies sends identically.
 */
export function cloudflareEmailTransport(
  binding: SendEmail,
): Options["transport"] {
  return ({ to, cc, bcc, ...content }) => {
    const envelope: Partial<EmailMessageBuilder> & typeof content = {
      ...content,
    };
    if (to !== undefined) envelope.to = [...to];
    if (cc !== undefined) envelope.cc = [...cc];
    if (bcc !== undefined) envelope.bcc = [...bcc];
    if (to !== undefined)
      return sendCloudflareEmail(binding, { ...envelope, to: [...to] });
    if (cc !== undefined)
      return sendCloudflareEmail(binding, { ...envelope, cc: [...cc] });
    if (bcc !== undefined)
      return sendCloudflareEmail(binding, { ...envelope, bcc: [...bcc] });
    // Submission decoding rejects an absent recipient envelope before persistence.
    return Promise.resolve({ kind: "outcome_unknown" as const });
  };
}
