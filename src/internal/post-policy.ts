import type { RetryPolicy } from "@fungi.computer/almanac";

/** Ordinary receiver allowance retained from BirdDog; deadline routes own their lifetime. */
export const maxDeliveryAttempts = 20;
/** Retained receiver backoff, now applied by Watchdog. */
export const postRetryPolicy: RetryPolicy = { baseMs: 2_000, maxMs: 300_000 };
