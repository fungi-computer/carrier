# Native transport and local recovery checkpoint

This checkpoint qualifies local dispatch and recovery through one native
transport, a SQLite/Watchdog owner and a native transaction/alarm adapter. No
live mail is sent by its controlled test fixture. The fixture checks local
invocation and evidence handling; it does not prove provider MIME generation,
actual BCC secrecy, repeated-address handling or recipient delivery.

The current Hub calls the structured `SendEmail.send(EmailMessageBuilder)`
binding. The existing `@cloudflare/workers-types` 5.20260828.1 declares
To/CC/BCC and returns only `EmailSendResult.messageId`. The boundary preserves
that native envelope and returns aggregate evidence. It does not manufacture
recipient acceptance, retry exceptions or expose raw provider errors containing
mail data. Cloudflare's Email Service Workers API now documents that the binding
throws an Error with a string `code`. The installed types still declare no error
shape, so the boundary decodes the documented codes with Effect Schema. Quota
and temporary-unavailability codes are retried by Carrier's owner after backoff;
request, sender, recipient and header refusals are final `rejected`.
`E_DELIVERY_FAILED`, undocumented codes and code-less exceptions remain unknown,
because a partial recipient acceptance cannot be excluded.

Email SDK revision `b9ee0371fb815e21ed2f340d10f62ae6d2aa4407`,
`packages/email-sdk/src/cloudflare.ts`, derives an accepted list from every
requested recipient after binding success. That is stronger than the native
receipt when suppression dropping is enabled. Its core also imports Node-based
telemetry; Worker runtime compatibility has not been proved. Direct native
transport therefore preserves more accurate evidence with less machinery for the
existing Hub consumer. This does not reject SDK adapters for future callers.

Cloudflare's
[headers contract](https://developers.cloudflare.com/email-service/reference/headers/)
allows X-headers but controls Message-ID. Its
[delivery event contract](https://developers.cloudflare.com/email-service/platform/event-subscriptions/)
provides eventId, provider messageId and recipient, without documented custom
header correlation. A lost acknowledgment cannot safely be reconciled by
matching address, subject or time. Event ingestion and durable correlation are
not implemented here.

The retained 00-pro source at `40c1f46d30025aa2e41fdf7e627ac02e5ba11174`
separates local Email submission from recipient Messages.
`lib/phoenix_00/messages/services/send_email.ex` saves the Email and enqueues an
Oban worker; `lib/workers/send_email.ex` submits through Swoosh/SES and passes
successful provider metadata to EventQueue. The worker can retry failures, and
the local submission ignores the enqueue result. Those are observed behaviors,
not guarantees retained by this candidate. Carrier instead uses the existing
Watchdog owner for queued execution, commits acceptance with a durable alarm,
and does not retry an ambiguous provider attempt. Its alternative transport seam
allows a future SES adapter; this package implements only the Cloudflare Email
Service transport.

The current Hub invitation sender still receives only the recipient and URL. It
does not pass the Global outbox identity or expiry to durable delivery. The
separate Team recovery candidate exposes `checkInvitationDelivery` after handoff
acknowledgment and bounded pending-outbox discovery. Carrier already accepts the
stable outbox identity as `id` and expiry as `sendBefore`; its optional
`canSend` callback can map a dispatchable source fact to true and an obsolete
fact to false. Source unavailability must throw, preserving recovery instead of
marking the message superseded. Joining these actual callers and scheduling
discovery remain consumer work. No Hub or Team implementation was changed in
this package checkpoint.

The native fixture uses the maintained Durable Object eviction helper and
explicit instance aborts, with invocation evidence retained in native fixture
storage. Source coverage now includes acceptance/alarm rollback, a lost durable
acceptance response, unavailable and superseded source validity, and stable
Message identities when an address appears in several envelope roles. Existing
cases cover expiry, failures before dispatch, lost provider acknowledgment,
provider evidence surviving failed Watchdog settlement, and interruption during
provider I/O. All twelve native cases passed on September 22, 2026, with the
pinned runtime and Hub's existing `2026-08-30` compatibility date. The fixture
proves no provider treatment of repeated recipients or BCC headers.

The native fixture explicitly disposes rejected RPC promises before eviction and
reacquires stubs after exceptions, following the platform's
[RPC lifecycle](https://developers.cloudflare.com/workers/runtime-apis/rpc/lifecycle/)
and
[stub contract](https://developers.cloudflare.com/durable-objects/api/stub/).
Without disposal, the eviction helper correctly reports outstanding references;
extending the test timeout does not fix that lifetime error. Restart assertions
remain intact. Controlled throws and aborts appear in workerd stderr; the final
run has no Vitest unhandled errors.

Passed from this package, using this checkout's own dependencies and serial
Cairn/Watchdog prerequisite builds:

- `pnpm lint`, `pnpm typecheck`, and `pnpm build`.
- `pnpm exec vitest run --config vitest.config.mjs --maxWorkers=1`: two tests.
- `pnpm exec vitest run --config vitest.cloudflare.config.mjs --maxWorkers=1`:
  twelve tests, including real SQLite, alarms, eviction and instance aborts.
- `pnpm docs:write` and `pnpm docs`: generated public API documentation checked
  with zero warnings.

Each execution scope used a ten-minute deadline, 3 GiB memory, 256 MiB swap,
150% CPU and a 1536 MiB Node heap. No pack, stranger, provider send, publication
or deployment command ran. This is not a completed Hub invitation consumer.

Scoped Fallow audit against `c0b3a5449399516845b0a8b394e9e5a2afdd993d`
identified the native Worker entry and public error tag as unused; both have
explicit source annotations naming their real external consumers. Its seven
health findings cover submission validation, atomic recipient insertion,
dispatch, transport translation, recovery and fixture validity. The largest
cyclomatic complexity is 11; recipient insertion has cognitive complexity 22.
These functions retain their bounded-step or atomic responsibilities. The audit
has no runtime coverage input, so its CRAP scores are estimates, not evidence
that the executed cases are absent. No duplicate groups, dependency issues or
cycles were found. Health findings remain advisory review observations.
