import type { WatchdogSqliteOwner } from "@fungi.computer/watchdog";
import * as Data from "effect/Data";
import type * as Schema from "effect/Schema";
import type { CarrierStatusSchema } from "./receipt-schema.js";

/** One recipient address; display names remain part of the requested envelope. */
export type Address = string | Readonly<{ email: string; name: string }>;

/** Common content and envelope for one Email. */
export type Email = Readonly<{
  from: Address;
  to?: readonly Address[];
  cc?: readonly Address[];
  bcc?: readonly Address[];
  subject: string;
  html?: string;
  text?: string;
  headers?: Readonly<Record<string, string>>;
}>;

/** Source identity and expiry belong to the notification's producer. */
export type Submission = Email & Readonly<{ id: string; sendBefore?: number }>;

/** A recipient Message is distinct from its Email and provider request. */
export type Message = Readonly<{
  id: string;
  recipient: string;
  roles: readonly ("to" | "cc" | "bcc")[];
  status: "unconfirmed";
}>;

/** A provider's definite refusal of a whole request: retryable or final. */
export type ProviderRejection = Readonly<{
  code: string;
  class: "permanent" | "temporary";
}>;

/**
 * Durable submission and aggregate dispatch evidence, without credential content.
 * `refused` means Carrier's per-recipient cap declined the submission before
 * any provider attempt; `rejected` means the provider definitely refused it
 * (a permanent rejection, or temporary rejections until retries ran out).
 * `providerRejection` is the last provider refusal, also while a temporary
 * rejection waits `queued` for its retry.
 */
export type Receipt = Readonly<{
  id: string;
  status: Schema.Schema.Type<typeof CarrierStatusSchema>;
  providerMessageId?: string;
  providerRejection?: ProviderRejection;
  messages: readonly Message[];
}>;

/**
 * One transport invocation reports only the evidence it obtained. Report
 * `provider_rejected` only for a documented refusal of the whole request before
 * acceptance; `cause` is an optional sanitized provider code for logs.
 */
export type SendEvidence =
  | Readonly<{ kind: "provider_accepted"; providerMessageId: string }>
  | (Readonly<{ kind: "provider_rejected" }> & ProviderRejection)
  | Readonly<{ kind: "outcome_unknown"; cause?: string }>;

/** Mechanical host capabilities; commit must durably arm a wake with its mutation. */
export type Host = Readonly<{
  /** Commit the synchronous mutation and a durable recovery wake atomically. */
  commit: <A>(mutation: () => A) => Promise<A>;
  /** Durably schedule another dispatch/recovery step. */
  requestWake: () => Promise<void>;
}>;

/** Configure a durable owner; business validity remains with the source. */
export type Options = Readonly<{
  storage: WatchdogSqliteOwner;
  host: Host;
  /** Return the current Unix timestamp in milliseconds for source expiry checks. */
  now: () => number;
  /** Return false only for a definitely obsolete source; throw when validity is unavailable. */
  canSend?: (emailId: string) => Promise<boolean>;
  /** Make one provider attempt and report only observed evidence; Carrier owns retries. */
  transport: (email: Email) => Promise<SendEvidence>;
}>;

/** Stable failures omit email bodies, tokens and raw provider errors. */
export class CarrierError extends Data.TaggedError("CarrierError")<{
  readonly code: "invalid_input" | "conflict" | "unavailable";
  readonly message: string;
}> {
  constructor(code: "invalid_input" | "conflict" | "unavailable") {
    super({ code, message: `Carrier ${code}` });
  }
}
