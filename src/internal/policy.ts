/** Carrier's tunable delivery numbers. Every value here is policy, not provider law. */
/**
 * Launch defaults; decision pending. See
 * wiki/3-resources/decisions/adr-fungi-launch-live-email.md.
 *
 * `recipientCap`: at most `messages` accepted Emails per normalized recipient
 * address within a rolling `windowMs`, counted per Carrier store (one Durable
 * Object). Emails that never reached the provider (refused, rejected, expired,
 * superseded) do not count.
 *
 * `retry`: a temporary provider rejection is dispatched again after
 * `baseDelayMs * 2^(attempt-1)`, capped at `maxDelayMs`, until `maxAttempts`
 * provider invocations have been rejected; then the Email is `rejected`.
 */
export const carrierPolicy = {
  recipientCap: { messages: 5, windowMs: 60 * 60 * 1000 },
  retry: { maxAttempts: 6, baseDelayMs: 30_000, maxDelayMs: 15 * 60 * 1000 },
} as const satisfies Readonly<{
  recipientCap: Readonly<{ messages: number; windowMs: number }>;
  retry: Readonly<{
    maxAttempts: number;
    baseDelayMs: number;
    maxDelayMs: number;
  }>;
}>;
