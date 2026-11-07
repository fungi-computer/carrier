import * as Schema from "effect/Schema";
import * as Data from "effect/Data";

/** Receiver's idempotent admission decision. */
export type PostDeliveryResult =
  | "accepted"
  | "duplicate"
  | "refused"
  | Readonly<{ status: "accepted" | "duplicate"; cursor: number }>
  | Readonly<{
      status: "accepted" | "duplicate" | "refused";
      receipt: Schema.Json;
    }>;
/** Decode the receiver's RPC response before settling delivery. */
const CursorResultSchema = Schema.Struct({
  status: Schema.Literals(["accepted", "duplicate"]),
  cursor: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
/** Accepted receipt, revision cursor, or permanent refusal returned by a receiver. */
export const PostDeliveryResultSchema = Schema.Union([
  Schema.Literals(["accepted", "duplicate", "refused"]),
  CursorResultSchema,
  Schema.Struct({
    status: Schema.Literals(["accepted", "duplicate", "refused"]),
    receipt: Schema.Json,
  }),
]);

/** Post rejected an unknown route, conflicting key, unsupported policy or store. */
export class PostError extends Data.TaggedError("PostError")<{
  readonly message: string;
  readonly reason?: "relational-deadline-unsupported" | "pre-scheduler-store";
}> {
  constructor(
    message: string,
    reason?: "relational-deadline-unsupported" | "pre-scheduler-store",
  ) {
    super(reason === undefined ? { message } : { message, reason });
  }
}
