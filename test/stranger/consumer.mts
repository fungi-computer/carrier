import {
  createMemoryOutbox,
  type MemoryOutbox,
} from "@fungi.computer/carrier/testing";
import { createD1Post } from "@fungi.computer/carrier/post-d1";
import { createPostgresPost } from "@fungi.computer/carrier/post-postgres";
import {
  createCarrier,
  type Options,
  type Receipt,
  type Submission,
} from "@fungi.computer/carrier";
import {
  createCloudflareCarrier,
  type CloudflareCarrierOptions,
} from "@fungi.computer/carrier/cloudflare";
import { createCarrier as createEmailCarrier } from "@fungi.computer/carrier/email";
import { createPost, definePostRoute } from "@fungi.computer/carrier/post";
import * as Schema from "effect/Schema";
import {
  createDeliveryInspection,
  type CarrierInspectionPage,
  type PostInspectionPage,
} from "@fungi.computer/carrier/inspection";

const teamRoute = definePostRoute({
  addressKind: "team",
  address: Schema.Struct({ teamId: Schema.String }),
  event: Schema.Struct({ text: Schema.String }),
  name: (address) => address.teamId,
  namespace: {
    getByName: () => ({
      deliver: () => Promise.resolve("accepted" as const),
    }),
  },
});
declare const postOptions: Omit<Parameters<typeof createPost>[0], "routes">;
const post = createPost({ ...postOptions, routes: { team: teamRoute } });
type TeamInput = Parameters<typeof post.send<"team">>[0];
// @ts-expect-error This address kind requires a string Team identity.
const invalidAddress: TeamInput["address"] = { teamId: 123 };
// @ts-expect-error This address kind admits only its typed event schema.
const invalidEvent: TeamInput["event"] = { text: 123 };
void invalidAddress;
void invalidEvent;
post.send({
  key: "team-event-1",
  kind: "team",
  address: { teamId: "team-1" },
  event: { text: "hello" },
});
void createEmailCarrier;

declare const portableOptions: Options;
const deliveryInspection = createDeliveryInspection(
  portableOptions.storage.sql,
);
const emailPage: CarrierInspectionPage = deliveryInspection.email({ limit: 1 });
const postPage: PostInspectionPage = deliveryInspection.post({
  view: "deadLetters",
  limit: 1,
});
void emailPage;
void postPage;
declare const alternateStorage: CloudflareCarrierOptions["storage"];
declare const nativeOptions: Extract<
  CloudflareCarrierOptions,
  { email: unknown }
>;

const submission = {
  id: "ordinary-email-1",
  from: "sender@example.com",
  bcc: ["recipient@example.net"],
  subject: "A normal email",
  text: "No expiry or source validity is necessary.",
} satisfies Submission;

const portable = await createCarrier(portableOptions);
const accepted: Receipt = await portable.send(submission);
await portable.tick();
const inspected: Receipt | undefined = await portable.get(accepted.id);
const pending: boolean = await portable.hasPendingWork();

const native = await createCloudflareCarrier({
  storage: nativeOptions.storage,
  email: nativeOptions.email,
});
await native.send(submission);
await native.alarm();
const nativeReceipt: Receipt | undefined = await native.get(submission.id);

void inspected;
void pending;
void nativeReceipt;

const alternate = await createCloudflareCarrier({
  storage: alternateStorage,
  transport: portableOptions.transport,
});
await alternate.send(submission);
await alternate.alarm();

// @ts-expect-error Choose one provider capability, never both.
await createCloudflareCarrier({
  storage: nativeOptions.storage,
  email: nativeOptions.email,
  transport: portableOptions.transport,
});
// @ts-expect-error A provider capability is required.
await createCloudflareCarrier({ storage: nativeOptions.storage });

export const nativeConstructors = { createD1Post, createPostgresPost };

const memoryOutbox: MemoryOutbox = createMemoryOutbox();
await memoryOutbox.put("typed:one", "receipt");
const memoryRows: readonly Readonly<{ key: string; value: string }>[] =
  await memoryOutbox.list(1);
void memoryRows;
