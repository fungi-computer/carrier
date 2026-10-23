# Carrier (`@fungi.computer/carrier`)

Carrier delivers things durably and tells you the truth about what happened. It
has two parts.

**Email.** You call `send()` with an email and a stable ID. Carrier commits it
to SQLite, dispatches it through your provider, retries temporary refusals, and
records what the provider actually said. A receipt never claims more than the
evidence shows. If a response is lost, the email is `outcome_unknown`, and
Carrier does not guess and send it twice.

**Post.** Typed, ordered, point-to-point envelopes between named Cloudflare
Durable Objects. You call `send` inside the same transaction as your own state
change, and Post delivers the event to the addressed object with retries, dead
letters and idempotent receipts.

Both run on [Watchdog](../watchdog/README.md) for durable work. The Cloudflare
adapter wires Email to Durable Object SQLite, alarms and the Email Service
binding, so your application keeps no delivery state machine of its own.

## Install or set up

```sh
npm install @fungi.computer/carrier @fungi.computer/watchdog
```

The Node SQLite quick start imports Watchdog’s adapter directly.

Carrier is ESM-only. It depends on `@fungi.computer/watchdog`, `effect`, `pg`
and `@cloudflare/workers-types`, which npm installs for you. The portable Email
owner and SQLite Post run on any synchronous SQLite handle, such as Durable
Object storage or `node:sqlite` in Node 24. The D1 and PostgreSQL Post adapters
need a migrated database.

Entrypoints:

- `@fungi.computer/carrier` and `@fungi.computer/carrier/email`: the Email API,
  `createCarrier` and its types.
- `@fungi.computer/carrier/cloudflare`: `createCloudflareCarrier` for a Durable
  Object, and `cloudflareEmailTransport`.
- `@fungi.computer/carrier/inspection`: read-only operator inspection.
- `@fungi.computer/carrier/post`: Post over Durable Object SQLite.
- `@fungi.computer/carrier/post-d1` and `@fungi.computer/carrier/post-postgres`:
  Post over shared D1 or PostgreSQL.

## Quick start

This sends one email through an in-memory `node:sqlite` store and a fake
transport. Swap the transport for a real provider and the store for your Durable
Object storage when you deploy.

```ts
import { DatabaseSync } from "node:sqlite";
import { fromNodeSqlite } from "@fungi.computer/watchdog/node-sqlite";
import { createCarrier } from "@fungi.computer/carrier";

const database = new DatabaseSync(":memory:");
const owner = fromNodeSqlite(database);

const carrier = await createCarrier({
  storage: owner,
  now: Date.now,
  host: {
    // Commit the mutation together with a durable wake. A demo needs no wake.
    commit: async (mutation) => mutation(),
    requestWake: async () => {},
  },
  transport: async (email) => {
    console.log(`provider got "${email.subject}"`);
    return { kind: "provider_accepted", providerMessageId: "demo-1" };
  },
});

const receipt = await carrier.send({
  id: "welcome:user-123:v1",
  from: { email: "hello@example.com", name: "Example" },
  to: ["ada@example.net"],
  subject: "Welcome",
  text: "Your account is ready.",
});
console.log(receipt.status); // "queued"

await carrier.tick(); // your wake calls this
console.log((await carrier.get(receipt.id))?.status); // "provider_accepted"

database.close();
```

`send()` only accepts the email. The provider call happens on `tick()`, which a
real host runs from its durable wake. The Cloudflare adapter does this for you
from the Durable Object alarm.

In a Durable Object, `ctx.storage` is already the SQLite owner.

### Memory outbox for tests

`createMemoryOutbox()` stores encoded string rows in process memory for tests
and quick starts. It provides `put`, `read`, key-ordered `list(limit, afterKey)`
and `delete`. Each instance is independent; it is not durable storage. BirdDog’s
[quick start](https://docs.fungi.computer/bird-dog/quickstart/) uses this
helper.

```ts
import { createMemoryOutbox } from "@fungi.computer/carrier/testing";

const outbox = createMemoryOutbox();
await outbox.put("mail:one", "encoded mail");
console.log(await outbox.list(1));
// [{ key: "mail:one", value: "encoded mail" }]
```

## Core concepts

**Emails and Messages.** One Email has shared content and an envelope: `from`,
`to`, `cc`, `bcc`, `subject`, `text`, `html` or both, and optional `headers`.
Each recipient becomes a separate Message with its own identity and roles. The
ordinary interface is `send()` and `get()`.

**Acceptance.** `send()` resolves after the Email, its recipient Messages, the
queued work and the wake commit together. A `queued` receipt means durable local
acceptance. It does not mean the provider accepted the email or delivered it.

**Replay.** Use a stable source ID across handoff retries. A repeated ID with
identical content returns the existing receipt. Changing its content, envelope,
headers or expiry produces `CarrierError` with `code: "conflict"`. `get()`
returns `undefined` when the ID is absent.

**Dispatch evidence.** Carrier records dispatch intent before it invokes the
provider. A successful response records `provider_accepted` and its
`providerMessageId`. That is an aggregate acknowledgment. It does not prove
every recipient was accepted or delivered, so recipient Messages stay
`unconfirmed`. A lost response or interrupted dispatch becomes
`outcome_unknown`. Carrier never sends that email again automatically, and an
unchanged-ID replay does not resend it. Sending under a new ID can duplicate
delivery. Work interrupted before dispatch intent recovers safely.

**Statuses.** A receipt's `status` is one of `queued`, `dispatching`,
`provider_accepted`, `outcome_unknown`, `rejected`, `refused`, `expired` or
`superseded`. Terminal receipts keep identities and dispatch evidence. Carrier
removes their stored content.

**Provider rejections and retries.** The Cloudflare transport decodes the
binding's documented error `code`. A whole-request refusal before acceptance is
`provider_rejected` evidence with a class:

- `temporary` codes (`E_RATE_LIMIT_EXCEEDED`, `E_DAILY_LIMIT_EXCEEDED`,
  `E_INTERNAL_SERVER_ERROR`) return the Email to `queued`. The same owner and
  wake dispatch it again after exponential backoff, until the retry budget is
  spent.
- `permanent` codes (sender, recipient, validation, size and header errors) end
  as `rejected`.

The receipt's `providerRejection` carries the last code and class.
`E_DELIVERY_FAILED`, undocumented codes and exceptions without a code stay
`outcome_unknown`, because some recipients may already have been accepted.

**Per-recipient cap.** Each Carrier store accepts a bounded number of Emails per
normalized recipient address in a rolling window. Emails that never reached the
provider (refused, rejected, expired or superseded) do not count. Over the cap,
`send()` durably records the submission as `refused` and never dispatches it. A
replay of that ID returns the same refused receipt.

**Expiring or superseded email.** Ordinary email needs no expiry or validity
callback. A source that owns expiring messages can supply `sendBefore`, an
absolute Unix timestamp in milliseconds. Carrier checks it before dispatch and
records `expired` without calling the provider once the deadline has passed. For
invitations or other replaceable notifications, construction can also take
`canSend: async (emailId) => boolean`. It must consult the source of truth:
return `false` for a definitely obsolete message, and throw when that truth is
unavailable. `false` records `superseded`. A throw leaves the email unsent for
later recovery. This is a pre-dispatch check, not an atomic fence against a
concurrent source change. Carrier does not create or validate authentication
tokens.

**Transports.** Provider and storage choices are independent. A transport takes
Carrier's `Email` and makes one provider attempt. It returns
`{ kind: "provider_accepted", providerMessageId }` only on an actual aggregate
acknowledgment, `{ kind: "provider_rejected", code, class }` only for a
documented refusal of the whole request, or `{ kind: "outcome_unknown" }` when
acceptance is uncertain. Credentials and provider configuration belong in the
transport, outside `send()`. Carrier's local submission ID does not give you
provider idempotency. Each transport handles its own SDK retry behaviour and
event correlation.

**Hosts.** `createCarrier({ storage, host, transport, now })` returns `send`,
`get`, `tick` and `hasPendingWork`. Its `storage` is Watchdog's structural
SQLite owner. `host.commit(mutation)` must commit the mutation and a durable
wake atomically. `host.requestWake()` must durably schedule recovery. Call
`tick()` from that wake, and keep the wake armed while `hasPendingWork()` is
true. Construct exactly one owner per dedicated store and runtime lifetime. A
replacement can start after its predecessor has stopped.
`createCloudflareCarrier` does all of this for a Durable Object.

## Operator inspection

`@fungi.computer/carrier/inspection` supplies `createDeliveryInspection(sql)`
for hosts that have already admitted an operator. It reads existing storage
without opening a delivery owner, configuring a provider, creating tables,
repairing jobs or changing alarms. Carrier does not authenticate the host's
caller. Do not grant this capability to models or guests.

Each call scans at most 64 rows (32 by default). Continue with
`after: page.nextCursor` until the cursor is `null`. Cursors and Post receipt
keys use the producer's key, without the internal `post:` prefix. A filtered
dead-letter page can be empty and still have a continuation. These are ordered
scans, not snapshots across concurrent writes.

An initialized store returns `{ status: "available", records, nextCursor }`. A
store without that delivery table returns `{ status: "uninitialized" }`. Results
include dispatch state, attempts, retry time and available provider evidence or
dead-letter reason. They omit Post addresses and events, email bodies, headers
and recipient addresses. Delivered Post rows keep only their key and state.
Email `attempts` counts definite provider rejections, so an accepted or
ambiguous first dispatch has zero. Post `attempts` counts receiver invocations.

## Post

Post uses Almanac's shared pure backoff calculation with its retained policy: 2
seconds initially, doubling to a 5-minute cap, with at most 20 receiver
attempts. The synchronous and SQL adapters share that default policy; explicit
SQL retry policies retain their own delays. Almanac supplies arithmetic only,
not a second scheduler or retry owner.

`@fungi.computer/carrier/post` delivers typed, point-to-point envelopes between
named Durable Objects. `definePostRoute` pairs each address schema and event
schema with its Durable Object namespace, name function and fixed `addressKind`
label. Every event route to the same namespace shares that label, so different
event kinds for one recipient keep producer order. The receiver implements
`deliver(envelope)` and calls `admitPost` inside its own SQLite transaction, so
the event mutation and admission receipt commit together. Keys must identify the
producing event across all senders to that receiver.

Construct `createPost` with the producer's structural SQLite `owner`, typed
`routes`, Watchdog's `transactional` projection and `readJob` operation, a
clock, and a durable `scheduleWake`. Post owns its tables. Call `send` inside
the same transaction as the producer's state change and durable initial wake. It
commits the envelope and Watchdog delivery work together. Each producer's events
reach a named address in order. A pending predecessor holds its successor until
it is delivered or dead. Replaying the same stable key keeps the existing work
or its completion receipt. Keep those receipts while the source can replay.

Use `execute` as the Watchdog executor for Post jobs. It records an attempt and
arms recovery before calling the addressed Durable Object. After the owner ticks
Watchdog, call `enqueueDue` at a wake to inspect settled jobs and enqueue due
retries, then tick Watchdog for the newly queued work. Continue its bounded
pages with the returned cursor. Queued or running jobs stay with Watchdog's
lifecycle. A missing job or row is an error. After a lost response or failed
receipt write, a retry carries the same envelope key and the receiver reports
`duplicate`.

`latest(kind, address)` reads the newest staged event or its receipt for that
recipient without scanning other recipients. A member or operator command can
call `retry(key)` inside the producer's transaction with its awaited wake. It
expedites a settled failed attempt or puts a dead letter after current queued
work, keeping its prepared fact. Queued and running jobs stay with Watchdog, and
a delivered receipt is never reopened.

A route can prepare a wire fact from its staged intent with
`prepare: { event: wireSchema, run(address, intent, signal) }`. Preparation and
receiver I/O share the ten-second attempt deadline. Post persists the parsed
prepared envelope before sending it. Restarts and lost responses reuse that
exact fact while the original intent still identifies a repeated `send`. A late
preparation result cannot send after its attempt expires. Use `onDelivered` to
acknowledge a domain ledger in the same producer transaction as the Post
receipt. An acknowledgment failure leaves both pending for retry. Prepared facts
should carry sealed grants, never raw credentials.

A definite refusal or an exhausted retry budget keeps a dead-letter envelope,
its reason and its attempt count. `deadLetters` returns `letters` and
`nextCursor`, including a continuation through pages that contain only receipts.

The same retry and receipt machinery is exported as `makePostOutbox` for a
string-backed outbox. [BirdDog](../bird-dog/README.md) uses it for agent reply
delivery. `postOutboxPending` reports whether such an outbox still owes any
message a delivery attempt; receipts and exhausted dead letters owe nothing. Its
`nextWakeAt()` reads the earliest owed delivery across the retained rows,
including future retries. Delivered receipts, exhausted dead letters and
malformed rows do not request a wake.

## Post transactions on D1 and PostgreSQL

`@fungi.computer/carrier/post-d1` and `/post-postgres` bind the ordinary Post
address and event registry to Watchdog's database adapters. Apply `postD1Schema`
or `postPostgresSchema` alongside the matching Watchdog schema through your
migration tool.

On D1, `post.send(envelope)` returns the envelope and job statements. Include
both in the same `database.batch` as the producer fact. `sendFromSelect` accepts
a trusted native SQLite `?NNN`-bound SELECT yielding `post_key`,
`envelope_json`, `queue_name`, `next_attempt_at` and `job_id`, for conditional
source facts created inside that batch. PostgreSQL `post.send(client, envelope)`
writes both rows on the producer's existing transaction client. Neither send API
commits for the producer. A conflicting replay rolls the whole transaction back.

Wire Watchdog's executor as `(job, signal) => post.execute(job, signal)`.
`enqueueDue` recovers a bounded page of settled delivery attempts, and the host
keeps its durable cron or wake. Preparation, bounded receiver calls, response
parsing and settlement decisions are shared with SQLite Post. Receipts suppress
source replay, failed attempts retry with the same Post policy, permanent
refusal or exhaustion becomes a dead letter, and earlier pending mail blocks
later mail to the same address. A receiver can return `{ status, receipt }` with
JSON receipt metadata. Post persists it with terminal settlement and exposes it
on the delivered receipt or dead letter. That is how an email producer can
record provider acceptance while minting credentials only during delivery. These
async adapters reject synchronous SQLite receipt hooks rather than run them
outside a transaction.

`purgeD1Post` and `purgePostgresPost` return parameterized statements selected
by trusted producer SQL yielding `queue_name`. Compose all of them in the same
native batch or transaction as your source erasure. They remove envelopes and
every Watchdog attempt, settled attempts included. PostgreSQL locks the selected
envelopes first, so an overlapping retry cannot leave an orphaned job.

## API reference

The generated reference is in [docs/api](docs/api/README.md). It has one page
per entrypoint: the Email root and `email`, `cloudflare`, `inspection`, `post`,
`post-d1` and `post-postgres`.

## Examples and checks

### Cloudflare Durable Object

Construct one Carrier per Durable Object instance, using that object's own
storage and an Email Service binding. Forward the object's alarm:

```ts
import { DurableObject } from "cloudflare:workers";
import type { SendEmail } from "@cloudflare/workers-types";
import type { Submission } from "@fungi.computer/carrier";
import { createCloudflareCarrier } from "@fungi.computer/carrier/cloudflare";

type Env = { EMAIL: SendEmail };

export class Mailbox extends DurableObject<Env> {
  private readonly carrier;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.carrier = createCloudflareCarrier({
      storage: ctx.storage,
      email: env.EMAIL,
    });
  }

  async send(email: Submission) {
    return (await this.carrier).send(email);
  }

  async get(id: string) {
    return (await this.carrier).get(id);
  }

  async alarm() {
    await (await this.carrier).alarm();
  }
}
```

Configure a SQLite Durable Object namespace and an Email Service binding in the
host application. An application with a bound `MAILBOX` namespace can then
submit and inspect mail:

```ts
const mailbox = env.MAILBOX.getByName("team-example");
const receipt = await mailbox.send({
  id: "welcome:user-123:v1",
  from: { email: "hello@example.com", name: "Example" },
  to: ["alice@example.net", "bob@example.net"],
  cc: ["host@example.net"],
  bcc: ["archive@example.net"],
  subject: "Welcome",
  text: "Your account is ready.",
  headers: { "List-Unsubscribe": "<https://example.com/unsubscribe/token>" },
});
const current = await mailbox.get(receipt.id);
```

`send()` needs at least one recipient across To, CC and BCC. To is optional. At
least one of `text` or `html` is required. Display names and visible To and CC
roles pass to the provider unchanged. BCC uses the provider's native BCC field.
Receipts expose all recipient addresses, BCC included, to the trusted caller, so
do not forward whole receipts to recipients or other viewers. Optional provider
headers are part of the immutable submission. A replay with different headers
conflicts just like a replay with different content.

### Another provider

To use a different provider with Durable Object SQLite and alarms, pass
`transport` instead of `email`. Exactly one of the two is required:

```ts
const carrier = await createCloudflareCarrier({
  storage: ctx.storage,
  transport: sendWithConfiguredProvider,
});
```

### Operator inspection

```ts
const inspection = createDeliveryInspection(ctx.storage.sql);
const emails = inspection.email({ limit: 32 });
const receipts = inspection.post({ view: "receipts", limit: 32 });
const dead = inspection.post({ view: "deadLetters", limit: 32 });
```

### Checks

Unit tests exercise the native transport with controlled I/O, Post ordering,
rollback, duplicate admission and recovery over local SQLite, and operator
inspection. Cloudflare runtime tests exercise SQLite, alarms, eviction,
interrupted dispatch and the D1 Post adapter without a live email binding. The
PostgreSQL proof starts an isolated PostgreSQL 16 instance over a private Unix
socket and always stops it. Install PostgreSQL 16, or set
`BOTANICAL_TEST_POSTGRES_ROOT` to its extracted root. The D1 and PostgreSQL
proofs cover producer, envelope and job rollback and commit, replay conflicts,
restart, overlapping workers and fenced lease expiry. From the package
directory:

```sh
pnpm test
```

No email is sent by these fixtures.

## Limits and errors

**Errors.**

- `CarrierError` has a `code` of `invalid_input`, `conflict` or `unavailable`.
  Its message never includes email bodies, tokens or raw provider errors.
- Post throws `PostError` for an unknown route or a conflicting key.
- Inspection raises `DeliveryInspectionError("invalid_query")` for bad bounds
  and the sanitized `DeliveryInspectionError("unavailable")` for invalid stored
  evidence or a storage failure. Inspection keys are limited to 256 characters
  and provider receipt IDs to 1,024.

**Logs.** Every failed, unknown or refused email delivery emits one
`console.error` line, `"Carrier delivery failed"`, with the submission `id`,
`class`, sanitized `cause`, resulting `status` and, for provider rejections, the
`attempt`. No content, address or provider message is logged.

**Policy numbers.**

- Recipient cap: 5 accepted Emails per normalized recipient address per rolling
  hour, counted per Carrier store.
- Email retries: a temporary rejection retries after 30 seconds, doubling,
  capped at 15 minutes, for up to 6 provider attempts. Then the Email is
  `rejected`.
- Post retries: 2 seconds, doubling, capped at 5 minutes, over 20 receiver
  attempts.
- Post attempts: preparation and receiver I/O share a ten-second deadline.

**Guarantees and gaps.**

- There is no exactly-once delivery promise. `outcome_unknown` is final, and a
  new ID for the same email can duplicate it.
- Recipient Messages stay `unconfirmed`. Carrier does not ingest provider
  delivery events or correlate them with receipts, and it does not expire
  terminal receipts.
- Run exactly one owner per store at a time. Simultaneous owners of the same
  store are unsupported.
- The test suite does not exercise real mail delivery, provider MIME generation
  or actual BCC secrecy.

## Contribute and license

Carrier is MIT licensed. See [LICENSE](./LICENSE).
