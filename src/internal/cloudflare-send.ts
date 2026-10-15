import type { EmailMessageBuilder, SendEmail } from "@cloudflare/workers-types";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { SendEvidence } from "./contracts.js";

// Cloudflare Email Service documents that the Workers binding throws a standard
// Error carrying a string `code` (Email Service, Workers API, error codes). The
// installed @cloudflare/workers-types declares no error type, so the documented
// codes are decoded here. Only codes naming a refusal of the whole request are
// classified; everything else remains an unknown outcome.
const Temporary = Schema.Literals([
  // Quota or temporary service unavailability; documented as retryable.
  "E_RATE_LIMIT_EXCEEDED",
  "E_DAILY_LIMIT_EXCEEDED",
  "E_INTERNAL_SERVER_ERROR",
]);
const isTemporary = Schema.is(Temporary);
const Rejection = Schema.Struct({
  code: Schema.Union([
    Temporary,
    // Request, sender, recipient or header validity; retrying cannot help.
    Schema.Literals([
      "E_VALIDATION_ERROR",
      "E_FIELD_MISSING",
      "E_TOO_MANY_RECIPIENTS",
      "E_TOO_MANY_ATTACHMENTS",
      "E_SENDER_NOT_VERIFIED",
      "E_RECIPIENT_NOT_ALLOWED",
      "E_RECIPIENT_SUPPRESSED",
      "E_SENDER_DOMAIN_NOT_AVAILABLE",
      "E_CONTENT_TOO_LARGE",
      "E_HEADER_NOT_ALLOWED",
      "E_HEADER_USE_API_FIELD",
      "E_HEADER_VALUE_INVALID",
      "E_HEADER_VALUE_TOO_LONG",
      "E_HEADER_NAME_INVALID",
      "E_HEADERS_TOO_LARGE",
      "E_HEADERS_TOO_MANY",
    ]),
  ]),
});
const decodeRejection = Schema.decodeUnknownOption(Rejection);
// E_DELIVERY_FAILED and undocumented codes stay unknown: an SMTP-stage failure
// may follow acceptance of some recipients, so it cannot authorize a resend.
// Their code is kept only as a sanitized log cause; messages are never kept
// because they may contain mail data.
const decodeCode = Schema.decodeUnknownOption(
  Schema.Struct({
    code: Schema.String.check(Schema.isPattern(/^E_[A-Z_]{1,62}$/u)),
  }),
);

/**
 * Invoke the native structured binding once, preserving its recipient envelope.
 * Cloudflare supplies no application idempotency key here. Only a documented
 * whole-request rejection is reported as such; any other exception cannot
 * authorize another send.
 */
export async function sendCloudflareEmail(
  binding: SendEmail,
  email: EmailMessageBuilder,
): Promise<SendEvidence> {
  try {
    const receipt = await binding.send(email);
    return {
      kind: "provider_accepted",
      providerMessageId: receipt.messageId,
    };
  } catch (error) {
    const rejection = decodeRejection(error);
    if (Option.isSome(rejection)) {
      const { code } = rejection.value;
      return {
        kind: "provider_rejected",
        code,
        class: isTemporary(code) ? "temporary" : "permanent",
      };
    }
    return Option.match(decodeCode(error), {
      onNone: () =>
        ({
          kind: "outcome_unknown",
          cause:
            error instanceof Error ? "provider_exception" : "non_error_thrown",
        }) as const,
      onSome: ({ code }) => ({ kind: "outcome_unknown", cause: code }) as const,
    });
  }
}
