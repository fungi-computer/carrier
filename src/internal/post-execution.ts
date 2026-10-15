import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { PostEnvelope, PostRegistryRoute, PostRow } from "../post.js";
import {
  PostDeliveryResultSchema,
  PostError,
  type PostDeliveryResult,
} from "./post-contract.js";

export type DeliveryEvidence = Readonly<{
  result: PostDeliveryResult | "unavailable";
  envelope: PostEnvelope | undefined;
  failure?: unknown;
}>;

/** Shared preparation, bounded receiver I/O and response decoding for every Post backend. */
export async function deliverPost(
  input: Readonly<{
    attempt: Extract<PostRow, { state: "pending" }>;
    signal?: AbortSignal | undefined;
    route: PostRegistryRoute;
    savePrepared: (envelope: PostEnvelope) => void | Promise<void>;
  }>,
): Promise<DeliveryEvidence> {
  const { attempt, route } = input;
  let envelope = attempt.delivery;
  let rawResult: unknown;
  try {
    rawResult = await Effect.runPromise(
      Effect.tryPromise({
        try: async (signal) => {
          signal.throwIfAborted();
          if (envelope === undefined) {
            envelope =
              route.prepareEnvelope === undefined
                ? route.parseDelivery(attempt.envelope)
                : await route.prepareEnvelope(attempt.envelope, signal);
            signal.throwIfAborted();
            if (route.prepareEnvelope !== undefined)
              await input.savePrepared(envelope);
          } else envelope = route.parseDelivery(envelope);
          signal.throwIfAborted();
          return route.namespace
            .getByName(route.nameOf(envelope))
            .deliver(envelope);
        },
        catch: (cause) => cause,
      }).pipe(Effect.timeout("10 seconds")),
      { signal: input.signal },
    );
  } catch (failure) {
    return { result: "unavailable", envelope, failure };
  }
  const decoded = Schema.decodeUnknownResult(PostDeliveryResultSchema)(
    rawResult,
  );
  if (decoded._tag === "Failure")
    return { result: "unavailable", envelope, failure: decoded.failure };
  return { result: decoded.success, envelope };
}

/** One settlement decision, shared by native storage adapters. */
export function postDisposition(
  evidence: DeliveryEvidence,
  route: PostRegistryRoute,
  attempts: number,
  maxAttempts: number,
) {
  const { result, envelope } = evidence;
  if (result === "unavailable")
    return attempts >= maxAttempts
      ? ({ state: "dead", reason: "exhausted" } as const)
      : ({ state: "pending" } as const);
  if (envelope === undefined) throw new PostError("missing delivered fact");
  if (result === "refused")
    return { state: "dead", reason: "refused" } as const;
  if (result === "accepted" || result === "duplicate") {
    return route.recordCursor !== undefined
      ? ({ state: "keep" } as const)
      : ({ state: "delivered" } as const);
  }
  if (result.status === "refused")
    return { state: "dead", reason: "refused" } as const;
  if ("cursor" in result)
    return { state: "delivered", cursor: result.cursor } as const;
  return route.recordCursor !== undefined
    ? ({ state: "keep" } as const)
    : ({ state: "delivered" } as const);
}

/** Durable provider/domain receipt data, separate from the staged event. */
export function deliveryReceipt(
  evidence: DeliveryEvidence,
): Schema.Json | undefined {
  const result = evidence.result;
  if (
    result === "accepted" ||
    result === "duplicate" ||
    result === "refused" ||
    result === "unavailable"
  )
    return undefined;
  return "receipt" in result ? result.receipt : undefined;
}
