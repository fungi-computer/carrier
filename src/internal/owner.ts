import { defineIdFamily } from "@fungi.computer/cairn";
import {
  makeRuntime,
  type WatchdogRuntimeEffect,
  type WatchdogRunningJob,
} from "@fungi.computer/watchdog/effect";
import { WatchdogError } from "@fungi.computer/watchdog";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

import {
  CarrierError,
  type Options,
  type Receipt,
  type SendEvidence,
  type Submission as CarrierSubmission,
} from "./contracts.js";
import { carrierPolicy } from "./policy.js";
import { CarrierStatusSchema } from "./receipt-schema.js";

const ids = defineIdFamily({
  family: "carrier",
  version: 1,
  kinds: {
    message: { prefix: "carrier_message" },
    work: { prefix: "carrier_work" },
  },
});
const Address = Schema.Union([
  Schema.NonEmptyString,
  Schema.Struct({ email: Schema.NonEmptyString, name: Schema.String }),
]);
const Submission = Schema.Struct({
  id: Schema.NonEmptyString,
  sendBefore: Schema.optionalKey(Schema.Number.check(Schema.isFinite())),
  from: Address,
  to: Schema.optionalKey(Schema.Array(Address)),
  cc: Schema.optionalKey(Schema.Array(Address)),
  bcc: Schema.optionalKey(Schema.Array(Address)),
  subject: Schema.String,
  html: Schema.optionalKey(Schema.String),
  text: Schema.optionalKey(Schema.String),
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
});
const decodeSubmission = Schema.decodeUnknownSync(Submission, {
  onExcessProperty: "error",
});
const decodeStoredSubmission = Schema.decodeUnknownSync(
  Schema.fromJsonString(Submission),
);
const RejectionClass = Schema.Literals(["permanent", "temporary"]);
const Row = Schema.Struct({
  id: Schema.String,
  content: Schema.NullOr(Schema.String),
  fingerprint: Schema.String,
  status: CarrierStatusSchema,
  job_id: Schema.String,
  provider_id: Schema.NullOr(Schema.String),
  attempts: Schema.Number,
  retry_at: Schema.NullOr(Schema.Number),
  error_code: Schema.NullOr(Schema.String),
  error_class: Schema.NullOr(RejectionClass),
});
const rowColumns =
  "id,content,fingerprint,status,job_id,provider_id,attempts,retry_at,error_code,error_class";
const decodeRow = Schema.decodeUnknownSync(Row);
const decodeRepairRow = Schema.decodeUnknownSync(
  Row.mapFields(Struct.pick(["id", "job_id", "status", "retry_at"])),
);
const decodeCount = Schema.decodeUnknownSync(
  Schema.Struct({ count: Schema.Number }),
);
const MessageRow = Schema.Struct({
  id: Schema.String,
  recipient: Schema.String,
  roles: Schema.fromJsonString(
    Schema.Array(Schema.Literals(["to", "cc", "bcc"])),
  ),
});
const decodeMessage = Schema.decodeUnknownSync(MessageRow);
const Evidence = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("provider_accepted"),
    providerMessageId: Schema.NonEmptyString,
  }),
  Schema.Struct({
    kind: Schema.Literal("provider_rejected"),
    code: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
    class: RejectionClass,
  }),
  Schema.Struct({
    kind: Schema.Literal("outcome_unknown"),
    cause: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(64))),
  }),
]);
const decodeEvidence = Schema.decodeUnknownSync(Evidence);
/** Recipient identity for the per-recipient cap; the envelope keeps its spelling. */
const recipientKey = (recipient: string) => recipient.trim().toLowerCase();
/** One structured line per failed or refused delivery; never content or addresses. */
const logFailure = (
  fields: Readonly<{
    id: string;
    class: "permanent" | "temporary" | "unknown" | "recipient_cap";
    cause: string;
    status: Receipt["status"];
    attempt?: number;
  }>,
) => {
  console.error("Carrier delivery failed", fields);
};
const unavailable = () => new CarrierError("unavailable");
const sync = <A>(operation: () => A) =>
  Effect.try({ try: operation, catch: unavailable });
const promise = <A>(operation: () => Promise<A>) =>
  Effect.tryPromise({ try: operation, catch: unavailable });

/** One private lifecycle; the public Promise entry only runs these effects. */
export function openOwner(options: Options) {
  const sql = options.storage.sql;
  let watchdog: WatchdogRuntimeEffect;
  let ticking = false;
  let repairCursor = "";
  const readRow = (id: string) => {
    const row = sql
      .exec(`SELECT ${rowColumns} FROM carrier_emails WHERE id=?`, id)
      .toArray()[0];
    return row === undefined ? undefined : decodeRow(row);
  };
  const receipt = (id: string): Receipt | undefined => {
    const row = readRow(id);
    if (row === undefined) return undefined;
    const messages = sql
      .exec(
        "SELECT id,recipient,roles FROM carrier_messages WHERE email_id=? ORDER BY position",
        id,
      )
      .toArray()
      .map((raw) => ({
        ...decodeMessage(raw),
        status: "unconfirmed" as const,
      }));
    const result: Receipt = { id, status: row.status, messages };
    if (row.provider_id !== null)
      return { ...result, providerMessageId: row.provider_id };
    if (row.error_code !== null && row.error_class !== null)
      return {
        ...result,
        providerRejection: { code: row.error_code, class: row.error_class },
      };
    return result;
  };
  const enqueue = (id: string, jobId: string) => {
    watchdog.transactional.enqueueAcceptedJob({
      jobId,
      queue: "carrier",
      lane: "email",
      priority: 0,
      payload: { emailId: id },
      state: "queued",
    });
  };
  const finish = (
    id: string,
    status: Receipt["status"],
    providerId: string | null = null,
  ) => {
    sql.exec(
      "UPDATE carrier_emails SET status=?,provider_id=?,content=NULL,retry_at=NULL WHERE id=?",
      status,
      providerId,
      id,
    );
  };
  const retryDelay = (attempt: number) =>
    Math.min(
      carrierPolicy.retry.baseDelayMs * 2 ** (attempt - 1),
      carrierPolicy.retry.maxDelayMs,
    );
  const execute = (job: WatchdogRunningJob) =>
    Effect.gen(function* () {
      const row = yield* sync(() => {
        const raw = sql
          .exec(
            `SELECT ${rowColumns} FROM carrier_emails WHERE job_id=?`,
            job.jobId,
          )
          .toArray()[0];
        if (raw === undefined) throw unavailable();
        return decodeRow(raw);
      });
      if (row.status !== "queued") return { kind: "completed" as const };
      const content = row.content;
      if (content === null) return yield* Effect.fail(unavailable());
      const submission = yield* sync(() => decodeStoredSubmission(content));
      if (
        submission.sendBefore !== undefined &&
        submission.sendBefore <= options.now()
      ) {
        yield* sync(() => {
          options.storage.transactionSync(() => {
            finish(row.id, "expired");
          });
        });
        return { kind: "completed" as const };
      }
      const canSend = options.canSend;
      const current =
        canSend === undefined ? true : yield* promise(() => canSend(row.id));
      const eligible = yield* sync(() =>
        options.storage.transactionSync(() => {
          // A shared owner may erase a queued Email while canSend awaits its
          // source. The earlier snapshot cannot authorize a later dispatch.
          if (readRow(row.id)?.status !== "queued") return false;
          if (
            submission.sendBefore !== undefined &&
            submission.sendBefore <= options.now()
          ) {
            finish(row.id, "expired");
            return false;
          }
          if (!current) {
            finish(row.id, "superseded");
            return false;
          }
          sql.exec(
            "UPDATE carrier_emails SET status='dispatching' WHERE id=? AND status='queued'",
            row.id,
          );
          return true;
        }),
      );
      if (!eligible) return { kind: "completed" as const };
      // Dispatch intent is committed before the one provider invocation.
      // Any interruption after the marker is ambiguous and cannot authorize a resend.
      const email = Struct.omit(submission, ["id", "sendBefore"]);
      const evidence = yield* promise(async (): Promise<SendEvidence> => {
        try {
          return decodeEvidence(await options.transport(email));
        } catch (cause) {
          // Exception messages may contain email content; retain only the
          // transport failure class as safe evidence for the outcome log.
          return {
            kind: "outcome_unknown",
            cause:
              cause instanceof Error
                ? "transport_exception"
                : "non_error_thrown",
          };
        }
      });
      if (evidence.kind === "provider_accepted") {
        yield* sync(() => {
          options.storage.transactionSync(() => {
            finish(row.id, "provider_accepted", evidence.providerMessageId);
          });
        });
        return { kind: "completed" as const };
      }
      if (evidence.kind === "outcome_unknown") {
        yield* sync(() => {
          options.storage.transactionSync(() => {
            finish(row.id, "outcome_unknown");
          });
        });
        logFailure({
          id: row.id,
          class: "unknown",
          cause: evidence.cause ?? "no_provider_answer",
          status: "outcome_unknown",
        });
        return { kind: "outcome_unknown" as const };
      }
      // A definite provider refusal: nothing was accepted, so a temporary one
      // may be dispatched again after backoff under this same owner.
      const attempt = row.attempts + 1;
      const retry =
        evidence.class === "temporary" &&
        attempt < carrierPolicy.retry.maxAttempts;
      yield* sync(() => {
        options.storage.transactionSync(() => {
          sql.exec(
            "UPDATE carrier_emails SET attempts=?,error_code=?,error_class=? WHERE id=?",
            attempt,
            evidence.code,
            evidence.class,
            row.id,
          );
          if (retry)
            sql.exec(
              "UPDATE carrier_emails SET status='queued',retry_at=? WHERE id=?",
              options.now() + retryDelay(attempt),
              row.id,
            );
          else finish(row.id, "rejected");
        });
      });
      logFailure({
        id: row.id,
        class: evidence.class,
        cause: evidence.code,
        status: retry ? "queued" : "rejected",
        attempt,
      });
      // Settling this job lets repair enqueue fresh work once retry_at passes.
      return { kind: retry ? ("failed" as const) : ("completed" as const) };
    }).pipe(Effect.mapError(() => new WatchdogError("execution_failed")));

  const initialize = Effect.gen(function* () {
    yield* sync(() => {
      options.storage.transactionSync(() => {
        sql.exec(
          "CREATE TABLE IF NOT EXISTS carrier_emails (id TEXT PRIMARY KEY,content TEXT,fingerprint TEXT NOT NULL,status TEXT NOT NULL,job_id TEXT NOT NULL UNIQUE,provider_id TEXT,accepted_at INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,retry_at INTEGER,error_code TEXT,error_class TEXT) STRICT",
        );
        sql.exec(
          "CREATE TABLE IF NOT EXISTS carrier_messages (id TEXT PRIMARY KEY,email_id TEXT NOT NULL REFERENCES carrier_emails(id),position INTEGER NOT NULL,recipient TEXT NOT NULL,recipient_key TEXT NOT NULL,roles TEXT NOT NULL,UNIQUE(email_id,recipient),UNIQUE(email_id,position)) STRICT",
        );
        sql.exec(
          "CREATE INDEX IF NOT EXISTS carrier_messages_recipient_key ON carrier_messages(recipient_key)",
        );
      });
    });
    watchdog = yield* makeRuntime({
      // One Carrier owns this store for its runtime lifetime. Its tick guard
      // excludes overlapping reconciliation; running claims observed by a new
      // runtime therefore belong to the terminated previous lifetime.
      owner: options.storage,
      isAlive: () => Effect.succeed(false),
      executor: { execute },
      wake: {
        recompute: () =>
          Effect.tryPromise({
            try: options.host.requestWake,
            catch: () => new WatchdogError("wake_unavailable"),
          }),
      },
    }).pipe(Effect.mapError(unavailable));
  });

  const send = (input: CarrierSubmission) =>
    Effect.gen(function* () {
      const submission = yield* Effect.try({
        try: () => {
          const parsed = decodeSubmission(input);
          if (parsed.html === undefined && parsed.text === undefined)
            throw new CarrierError("invalid_input");
          if (
            (parsed.to?.length ?? 0) +
              (parsed.cc?.length ?? 0) +
              (parsed.bcc?.length ?? 0) ===
            0
          )
            throw new CarrierError("invalid_input");
          return parsed;
        },
        catch: () => new CarrierError("invalid_input"),
      });
      const address = (value: Schema.Schema.Type<typeof Address>) =>
        Predicate.isString(value)
          ? value
          : { email: value.email, name: value.name };
      // Stable serialization preserves address spelling and recipient ordering.
      const content = JSON.stringify({
        id: submission.id,
        sendBefore: submission.sendBefore,
        from: address(submission.from),
        to: submission.to?.map(address),
        cc: submission.cc?.map(address),
        bcc: submission.bcc?.map(address),
        subject: submission.subject,
        html: submission.html,
        text: submission.text,
        headers: submission.headers,
      });
      const digest = yield* promise(() =>
        crypto.subtle.digest("SHA-256", new TextEncoder().encode(content)),
      );
      const fingerprint = Array.from(new Uint8Array(digest), (byte) =>
        byte.toString(16).padStart(2, "0"),
      ).join("");
      const { accepted, refused } = yield* Effect.tryPromise({
        try: () =>
          options.host.commit(() =>
            options.storage.transactionSync(() => {
              const prior = readRow(submission.id);
              let refused = false;
              if (prior !== undefined) {
                if (prior.fingerprint !== fingerprint)
                  throw new CarrierError("conflict");
              } else {
                const recipients = new Map<string, ("to" | "cc" | "bcc")[]>();
                for (const role of ["to", "cc", "bcc"] as const) {
                  for (const address of submission[role] ?? []) {
                    const recipient = Predicate.isString(address)
                      ? address
                      : address.email;
                    const roles = recipients.get(recipient) ?? [];
                    if (!roles.includes(role)) roles.push(role);
                    recipients.set(recipient, roles);
                  }
                }
                const acceptedAt = options.now();
                const cap = carrierPolicy.recipientCap;
                // Emails that never reached the provider do not use the cap.
                const capped = [...recipients.keys()].some(
                  (recipient) =>
                    decodeCount(
                      sql
                        .exec(
                          "SELECT COUNT(DISTINCT e.id) AS count FROM carrier_messages m JOIN carrier_emails e ON e.id=m.email_id WHERE m.recipient_key=? AND e.accepted_at>? AND e.status IN ('queued','dispatching','provider_accepted','outcome_unknown')",
                          recipientKey(recipient),
                          acceptedAt - cap.windowMs,
                        )
                        .toArray()[0],
                    ).count >= cap.messages,
                );
                // A refusal is durable: replaying this ID cannot bypass the cap.
                // Its unused job identity only satisfies the row's key.
                const jobId = ids.work.generate();
                sql.exec(
                  "INSERT INTO carrier_emails (id,content,fingerprint,status,job_id,accepted_at) VALUES (?,?,?,?,?,?)",
                  submission.id,
                  capped ? null : content,
                  fingerprint,
                  capped ? "refused" : "queued",
                  jobId,
                  acceptedAt,
                );
                let position = 0;
                for (const [recipient, roles] of recipients) {
                  sql.exec(
                    "INSERT INTO carrier_messages (id,email_id,position,recipient,recipient_key,roles) VALUES (?,?,?,?,?,?)",
                    ids.message.generate(),
                    submission.id,
                    position++,
                    recipient,
                    recipientKey(recipient),
                    JSON.stringify(roles),
                  );
                }
                if (capped) refused = true;
                else enqueue(submission.id, jobId);
              }
              const accepted = receipt(submission.id);
              if (accepted === undefined) throw unavailable();
              // Only the first refusal logs; a replay returns the same receipt.
              return { accepted, refused };
            }),
          ),
        catch: (error) =>
          error instanceof CarrierError ? error : unavailable(),
      });
      if (refused)
        logFailure({
          id: submission.id,
          class: "recipient_cap",
          cause: "recipient_cap_reached",
          status: "refused",
        });
      return accepted;
    });

  const repair = Effect.gen(function* () {
    const rows = yield* sync(() =>
      sql
        .exec(
          "SELECT id,job_id,status,retry_at FROM carrier_emails WHERE status IN ('queued','dispatching') AND id>? ORDER BY id LIMIT 64",
          repairCursor,
        )
        .toArray()
        .map((row) => decodeRepairRow(row)),
    );
    for (const row of rows) {
      const work = yield* watchdog
        .read(row.job_id)
        .pipe(Effect.mapError(unavailable));
      if (work.status !== "found") return yield* Effect.fail(unavailable());
      if (work.job.state !== "settled") continue;
      // A temporary provider rejection waits for its backoff on the same wake.
      if (row.retry_at !== null && row.retry_at > options.now()) continue;
      const interrupted = yield* promise(() =>
        options.host.commit(() =>
          options.storage.transactionSync(() => {
            const current = readRow(row.id);
            if (current === undefined) return false;
            if (current.job_id !== row.job_id || current.status !== row.status)
              return false;
            if (row.status === "dispatching") {
              finish(row.id, "outcome_unknown");
              return true;
            }
            const jobId = ids.work.generate();
            sql.exec(
              "UPDATE carrier_emails SET job_id=? WHERE id=? AND job_id=? AND status='queued'",
              jobId,
              row.id,
              row.job_id,
            );
            enqueue(row.id, jobId);
            return false;
          }),
        ),
      );
      if (interrupted)
        logFailure({
          id: row.id,
          class: "unknown",
          cause: "interrupted_dispatch",
          status: "outcome_unknown",
        });
    }
    const last = rows.at(-1);
    repairCursor = rows.length === 64 && last !== undefined ? last.id : "";
  });
  const tick = Effect.suspend(() => {
    if (ticking) return Effect.void;
    ticking = true;
    return watchdog.tick().pipe(
      Effect.mapError(unavailable),
      Effect.andThen(repair),
      Effect.asVoid,
      Effect.ensuring(
        Effect.sync(() => {
          ticking = false;
        }),
      ),
    );
  });
  return {
    initialize,
    send,
    tick,
    get: (id: string) => sync(() => receipt(id)),
    hasPendingWork: Effect.gen(function* () {
      const domainPending = yield* sync(
        () =>
          sql
            .exec(
              "SELECT 1 FROM carrier_emails WHERE status IN ('queued','dispatching') LIMIT 1",
            )
            .toArray().length !== 0,
      );
      return (
        domainPending ||
        (yield* watchdog.hasPendingWork.pipe(Effect.mapError(unavailable)))
      );
    }),
  };
}
