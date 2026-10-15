import * as Schema from "effect/Schema";

/** Carrier's aggregate dispatch states at both storage and read boundaries. */
export const CarrierStatusSchema = Schema.Literals([
  "queued",
  "dispatching",
  "provider_accepted",
  "outcome_unknown",
  "rejected",
  "refused",
  "expired",
  "superseded",
]);
