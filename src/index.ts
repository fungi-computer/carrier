import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";

import { openOwner } from "./internal/owner.js";
import type { Options, Receipt, Submission } from "./internal/contracts.js";
import { CarrierError } from "./internal/contracts.js";

export { CarrierError } from "./internal/contracts.js";
export { CarrierStatusSchema } from "./internal/receipt-schema.js";
export type {
  Address,
  Email,
  Host,
  Message,
  Options,
  ProviderRejection,
  Receipt,
  SendEvidence,
  Submission,
} from "./internal/contracts.js";

/** Durable email submission and its host-driven recovery door. */
export type Carrier = Readonly<{
  /** Commit an Email and its recipient Messages durably; identical IDs replay the receipt. */
  send: (submission: Submission) => Promise<Receipt>;
  /** Read dispatch evidence, or undefined when this owner has no such submission. */
  get: (id: string) => Promise<Receipt | undefined>;
  /** Run one bounded dispatch/recovery step from the runtime's durable wake. */
  tick: () => Promise<void>;
  /** Report whether the runtime must retain a recovery wake. */
  hasPendingWork: () => Promise<boolean>;
}>;

const run = <A>(operation: Effect.Effect<A, CarrierError>): Promise<A> =>
  Effect.runPromiseExit(operation).then((exit) => {
    if (Exit.isSuccess(exit)) return exit.value;
    throw Option.getOrElse(
      Cause.findErrorOption(exit.cause),
      () => new CarrierError("unavailable"),
    );
  });

/**
 * Construct exactly one owner per store/runtime lifetime. A replacement may be
 * opened only after its predecessor has stopped; simultaneous owners of the
 * same store are unsupported. Native adapters supply their runtime ownership.
 */
export async function createCarrier(options: Options): Promise<Carrier> {
  const owner = openOwner(options);
  await run(owner.initialize);
  return {
    send: (submission) => run(owner.send(submission)),
    get: (id) => run(owner.get(id)),
    tick: () => run(owner.tick),
    hasPendingWork: () => run(owner.hasPendingWork),
  };
}
