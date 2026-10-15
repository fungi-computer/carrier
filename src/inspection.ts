import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import type { WatchdogSqliteOwner } from "@fungi.computer/watchdog";
import { PostRowSchema } from "./post.js";
import { CarrierStatusSchema } from "./internal/receipt-schema.js";

const Key = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
/** One bounded scan; an empty filtered page can still have a continuation. */
export const DeliveryInspectionPageQuerySchema = Schema.Struct({
  limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 })).pipe(
    Schema.withDecodingDefaultType(Effect.succeed(32)),
  ),
  after: Schema.optional(Key),
});
/** Post can filter dead letters while scanning at most one bounded page. */
export const DeliveryInspectionQuerySchema = Schema.Struct({
  ...DeliveryInspectionPageQuerySchema.fields,
  view: Schema.Literals(["receipts", "deadLetters"]),
});
/** An email receipt page uses the same producer-key pagination. */
export type DeliveryInspectionPageQuery = Schema.Codec.Encoded<
  typeof DeliveryInspectionPageQuerySchema
>;
/** Input to one producer-local inspection. Cursors are producer keys, without storage prefixes. */
export type DeliveryInspectionQuery = Schema.Codec.Encoded<
  typeof DeliveryInspectionQuerySchema
>;

/** Dispatch evidence deliberately omits the address and event. */
export const PostReceiptSummarySchema = Schema.Union([
  Schema.Struct({ key: Key, state: Schema.Literal("delivered") }),
  Schema.Struct({
    key: Key,
    state: Schema.Literal("pending"),
    kind: Key,
    attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    nextAttemptAt: Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
  }),
  Schema.Struct({
    key: Key,
    state: Schema.Literal("dead"),
    kind: Key,
    attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    reason: Schema.Literals(["refused", "exhausted"]),
  }),
]);
/** Metadata for one retained Post row. */
export type PostReceiptSummary = Schema.Schema.Type<
  typeof PostReceiptSummarySchema
>;
/** Bounded evidence with an explicit distinction between empty and uninitialized storage. */
export const PostInspectionPageSchema = Schema.Union([
  Schema.Struct({ status: Schema.Literal("uninitialized") }),
  Schema.Struct({
    status: Schema.Literal("available"),
    records: Schema.Array(PostReceiptSummarySchema).check(
      Schema.isMaxLength(64),
    ),
    nextCursor: Schema.NullOr(Key),
  }),
]);
/** One read of producer-local Post dispatch evidence. */
export type PostInspectionPage = Schema.Schema.Type<
  typeof PostInspectionPageSchema
>;

/** Aggregate email evidence without bodies, headers or recipient addresses. */
export const CarrierReceiptSummarySchema = Schema.Struct({
  id: Key,
  status: CarrierStatusSchema,
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  retryAt: Schema.NullOr(Schema.Number.check(Schema.isGreaterThanOrEqualTo(0))),
  providerMessageId: Schema.optional(
    Schema.NonEmptyString.check(Schema.isMaxLength(1024)),
  ),
  providerRejection: Schema.optional(
    Schema.Struct({
      code: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
      class: Schema.Literals(["permanent", "temporary"]),
    }),
  ),
});
/** Carrier's bounded metadata page; unavailable storage is never reported as empty. */
export const CarrierInspectionPageSchema = Schema.Union([
  Schema.Struct({ status: Schema.Literal("uninitialized") }),
  Schema.Struct({
    status: Schema.Literal("available"),
    records: Schema.Array(CarrierReceiptSummarySchema).check(
      Schema.isMaxLength(64),
    ),
    nextCursor: Schema.NullOr(Key),
  }),
]);
/** One read of aggregate email dispatch evidence. */
export type CarrierInspectionPage = Schema.Schema.Type<
  typeof CarrierInspectionPageSchema
>;

/** Stable inspection errors never include stored cargo or raw SQL failures. */
export class DeliveryInspectionError extends Error {
  constructor(readonly code: "invalid_query" | "unavailable") {
    super(`Delivery inspection ${code}`);
  }
}

const StoredPost = Schema.Struct({ key: Schema.String, value: PostRowSchema });
const StoredEmail = Schema.Struct({
  id: Key,
  status: CarrierStatusSchema,
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  retry_at: Schema.NullOr(
    Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
  ),
  provider_id: Schema.NullOr(
    Schema.NonEmptyString.check(Schema.isMaxLength(1024)),
  ),
  error_code: Schema.NullOr(
    Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  ),
  error_class: Schema.NullOr(Schema.Literals(["permanent", "temporary"])),
});
const hasTable = (sql: WatchdogSqliteOwner["sql"], name: string) =>
  sql
    .exec(
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
      name,
    )
    .toArray().length === 1;

/**
 * Read existing storage without opening a sender, initializing tables, repairing
 * jobs or arming a wake. The trusted host must admit an operator before calling.
 */
export function createDeliveryInspection(sql: WatchdogSqliteOwner["sql"]) {
  return {
    /** Read aggregate dispatch evidence without email cargo or recipient addresses. */
    email(input: DeliveryInspectionPageQuery): CarrierInspectionPage {
      const parsed = Schema.decodeUnknownExit(
        DeliveryInspectionPageQuerySchema,
        { onExcessProperty: "error" },
      )(input);
      if (Exit.isFailure(parsed))
        throw new DeliveryInspectionError("invalid_query");
      const query = parsed.value;
      try {
        if (!hasTable(sql, "carrier_emails"))
          return { status: "uninitialized" };
        const rows = Schema.decodeUnknownSync(Schema.Array(StoredEmail))(
          sql
            .exec(
              "SELECT id,status,attempts,retry_at,provider_id,error_code,error_class FROM carrier_emails WHERE id>? ORDER BY id LIMIT ?",
              query.after ?? "",
              query.limit,
            )
            .toArray(),
        );
        return Schema.decodeUnknownSync(CarrierInspectionPageSchema)({
          status: "available",
          records: rows.map((row) => {
            const receipt: Schema.Schema.Type<
              typeof CarrierReceiptSummarySchema
            > = {
              id: row.id,
              status: row.status,
              attempts: row.attempts,
              retryAt: row.retry_at,
            };
            if (row.provider_id !== null)
              return { ...receipt, providerMessageId: row.provider_id };
            if (row.error_code !== null && row.error_class !== null)
              return {
                ...receipt,
                providerRejection: {
                  code: row.error_code,
                  class: row.error_class,
                },
              };
            return receipt;
          }),
          nextCursor: rows.length === query.limit ? rows.at(-1)?.id : null,
        });
      } catch {
        throw new DeliveryInspectionError("unavailable");
      }
    },
    /** Read Post metadata; filtered pages retain their continuation through other states. */
    post(input: DeliveryInspectionQuery): PostInspectionPage {
      const parsed = Schema.decodeUnknownExit(DeliveryInspectionQuerySchema, {
        onExcessProperty: "error",
      })(input);
      if (Exit.isFailure(parsed))
        throw new DeliveryInspectionError("invalid_query");
      const query = parsed.value;
      try {
        if (!hasTable(sql, "carrier_post_rows"))
          return { status: "uninitialized" };
        const rows = Schema.decodeUnknownSync(Schema.Array(StoredPost))(
          sql
            .exec(
              "SELECT key,value FROM carrier_post_rows WHERE key LIKE 'post:%' AND key>? ORDER BY key LIMIT ?",
              `post:${query.after ?? ""}`,
              query.limit,
            )
            .toArray(),
        );
        const records = rows.flatMap<PostReceiptSummary>(({ key, value }) => {
          if (query.view === "deadLetters" && value.state !== "dead") return [];
          const receiptKey = key.slice("post:".length);
          switch (value.state) {
            case "delivered":
              return [{ key: receiptKey, state: value.state }];
            case "pending":
              return [
                {
                  key: receiptKey,
                  state: value.state,
                  kind: value.envelope.kind,
                  attempts: value.attempts,
                  nextAttemptAt: value.nextAttemptAt,
                },
              ];
            case "dead":
              return [
                {
                  key: receiptKey,
                  state: value.state,
                  kind: value.envelope.kind,
                  attempts: value.attempts,
                  reason: value.reason,
                },
              ];
          }
        });
        return Schema.decodeUnknownSync(PostInspectionPageSchema)({
          status: "available",
          records,
          nextCursor:
            rows.length === query.limit
              ? rows.at(-1)?.key.slice("post:".length)
              : null,
        });
      } catch {
        throw new DeliveryInspectionError("unavailable");
      }
    },
  };
}
