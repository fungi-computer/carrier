import type {
  EmailMessage,
  EmailMessageBuilder,
  SendEmail,
} from "@cloudflare/workers-types";
import { describe, expect, it } from "vitest";

import { sendCloudflareEmail } from "../src/internal/cloudflare-send.js";

const email: EmailMessageBuilder = {
  from: { name: "Example", email: "sender@example.com" },
  to: ["first@example.net", "second@example.net"],
  cc: "copy@example.net",
  bcc: "private@example.net",
  subject: "Invitation",
  text: "A controlled qualification message.",
};

describe("native Cloudflare send boundary", () => {
  it("preserves visible recipient roles and BCC in one provider invocation", async () => {
    const received: (EmailMessage | EmailMessageBuilder)[] = [];
    const binding: SendEmail = {
      send(message) {
        received.push(message);
        return Promise.resolve({ messageId: "provider-request-1" });
      },
    };

    const result = await sendCloudflareEmail(binding, email);

    expect(received).toEqual([email]);
    // A request receipt does not establish acceptance of each recipient:
    // the service can drop suppressed recipients before dispatching the rest.
    expect(result).toEqual({
      kind: "provider_accepted",
      providerMessageId: "provider-request-1",
    });
    expect(result).not.toHaveProperty("messages");
    expect(result).not.toHaveProperty("accepted");
    expect(result).not.toHaveProperty("bcc");
  });

  it("does not resend when acceptance may precede a lost response", async () => {
    let invocations = 0;
    const binding: SendEmail = {
      send() {
        invocations += 1;
        return Promise.reject(
          new Error("Transport response lost after possible acceptance"),
        );
      },
    };

    expect(await sendCloudflareEmail(binding, email)).toEqual({
      kind: "outcome_unknown",
      cause: "provider_exception",
    });
    expect(invocations).toBe(1);
  });
});
