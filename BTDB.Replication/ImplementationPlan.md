# BTDB.Replication implementation plan

Date: 2026-09-14. Status: M0 implemented; M1 authority/TRL model and Azure experiment implemented; M2 core position tracking/cancellation implemented; M3 native streamed KVI export, PVL receipt/order helper and capture-backed canonical TRL lane implemented; full M3–M7 runtime integration pending.
See [Testing.md](Testing.md) for current evidence and limitations. No distributed replication runtime exists yet.

## Scope and source of truth

Implement the Azure-first design in [Architecture.md](Architecture.md), following the local
[working agreement](AGENTS.md). Storage semantics and provider qualification belong in
[ObjectStorages.md](ObjectStorages.md). This plan orders engineering work; it does not duplicate or supersede
the normative protocol rules. Record resolved mechanisms in their architectural owner sections, and link their tests
from the B1/B3/B5/B6 blocker register before declaring those blockers closed.

The application retains ownership of ordered input, handler execution, retries, failure classification and external
effects. Local commits and default reads retain their selected semantics. S3, local database import, stronger read APIs
and a business acknowledgement/outbox protocol are outside this implementation plan.

## Admission rule for implementation work

The milestones below organize dependencies; their mechanism/API lists are not an instruction to implement every
item. For each addition, first try the smaller design using existing TRL, storage and session information. Proceed
only when a concrete failure case establishes that it cannot satisfy the agreed behavior, or a relevant measurement
shows a substantial performance cost. Keep the supporting test or measurement and a short necessity argument with
the change. Otherwise defer the addition. Hypothetical flexibility or an older architectural sketch is not evidence.

Apply this rule to wire fields, persisted metadata, state machines, timers, extra round trips, hashes, abstractions and
optimizations. Preserve safety and user-selected semantics. Start M1 with the smallest executable authority/publication
model and add mechanisms only as counterexamples require them; do not first implement every sketched identity, token,
message or configuration option. Byte piggybacking and peer durability classification are not baseline requirements.

## Current implementation baseline

At planning time the replication directory contained design documents only. M0 now adds the project, internal
scheduling/entropy ports and a separate test project. The current checkout already
contains core preparation described in [ReplicationCore.md](../Doc/ReplicationCore.md):

- `StartWritingTransaction(eventId, inBatch)` on KeyValueDB/ObjectDB assigns the cursor after writer admission.
- Explicit transaction mode rejects synchronous `StartTransaction()`.
- Opt-in odd TRL/even non-TRL allocation preserves legacy files and rotates an even append target.
- ObjectDB initializes the complete relation list read-only first, then persists new schemas and index upgrades in at most one startup writer.
- An injected ID-based TRL size strategy provides soft and hard limits, including cross-file transactions.

These are existing core changes, not work to recreate or claim as replication implementation. Validate them as
the starting baseline. In particular, they do not yet provide leader admission, distributed canonical allocation,
transaction capture, canonical publication or follower comparison. Virtual batching already preserves native event
transactions, but distributed rollback capture and ObjectDB integration still require coverage.

Concrete integration points:

| Existing code | Planned use or required extension |
| --- | --- |
| `BTreeKeyValueDB.StartWritingTransaction`, `BTreeKeyValueDBTransaction.MakeWritable` | Startup-only secondary-index reconciliation before application processing; no generic core authority gate. |
| `CommitWritingTransaction`, rollback and TRL rotation paths | Capture closed ordered ranges, retain rollback evidence and signal background publication without external waits. |
| `LoadTransactionLogCore`, `FileTransactionLog` | Keep native decoding internal to replay; compare replication ranges byte for byte. |
| `FileCollectionWithFileInfos`, file collections and allocator | Capture retention boundary and validated restore preserving physical file IDs. |
| `CreateKeyIndexFile`, `IKeyValueDBInternal` | Capture native KVI recovery dependencies and fixed publication cut; serialize remote file references through an explicit address map. |
| `Compactor.RunCore`, `ReplaceBTreeValues`, `CommitFromCompactor` | Independent local compaction without KVI; separate remote inventory planning and remapped KVI export. |
| ObjectDB relation initialization and `CompactorLeakDetector` | Leader-admitted schema work and parent-event leak removal. |
| `BTDB.AzureStorage` | Inspect reusable transfer helpers only. Its current backend contract lacks conditional tokens, lease authority and ambiguous-write outcomes. |

## Implementation structure

Start with `BTDB.Replication` and `BTDB.Replication.Test`, targeting the repository's .NET version. Keep protocol state
machines, immutable identities, codecs and injected ports in the core project. Add separate Azure and ASP.NET Core
adapter projects when their stages begin; final package names can follow repository packaging conventions.

Use node-scoped dependencies and a serialized transition engine, plus one serialized publisher lane per database.
No Azure SDK, HTTP, sockets, ambient time, randomness or process-global state enters the protocol core. Ports cover
storage/leases, peers, input progress/recovery, generation/compatibility, local files, scheduling/time, IDs/randomness,
host lifecycle and observability. Expose remote deletion only through a leader-authorized maintenance port.

Use the production progress/control codec and authentication path in the in-process transport; transfer native TRL
bytes through the same bounded pull semantics as HTTP. Keep test doubles and the
deterministic scheduler in test support. Introduce the smallest core hooks required by each milestone; avoid one large
up-front public API. Disabled replication must preserve standalone behavior and hot-path costs.

## Ordered milestones

### M0 — Establish the baseline and test oracle

Implemented as test infrastructure; [coverage register](Testing.md#scenario-family-register) distinguishes synthetic
model checks, native BTDB evidence and unimplemented protocol requirements. No safety blocker is closed by M0.

1. Run SourceGenerator tests first, then the core preparation, TRL sizing and virtual batching tests. Preserve all
   existing working-tree edits and work on the current branch.
2. Map I1–I12 and the architecture's scenario families to named test groups. Track each as unimplemented, passing in
   the deterministic model, passing against real BTDB, or qualified against the production adapter.
3. Scaffold isolated node scopes, virtual monotonic time, explicit scheduling, seeded IDs/backoff, storage and peer
   fault controls. A shared event fixture supplies input to application test drivers; replication does not execute handlers.
4. Build an independent history/recovery oracle that checks selected authority, reachable complete transactions,
   per-database cursors and file dependencies after every scheduled step. Persist seeds and schedules on failure.

Exit: multiple isolated nodes can be constructed and advanced without sleeps, ports or global state; the current core
preparation tests pass. Harness assertions must inspect observable storage/history, not merely echo implementation flags.

### M1 — Resolve authority and storage representation before relying on them

Dependencies: M0. Owners: B1 and the representation portion of B5.

Current evidence: [M1Evidence.md](M1Evidence.md). Authority/grant helpers, candidate TRL metadata/CAS intents, native
multifile replay tests and the live Azure experiment are implemented. KVI follows the existing dependency-first
publication and restore-retry rules; no additional KVI selection proof blocks M2. Production allocation, publication
and restore are implemented in their following milestones. The list below remains subject to the
admission rule; no operation ID or general version/identity framework was added without a demonstrated need.

1. Specify exact serialized identities and compatibility versions: cluster/stream/database instance, term/session,
   operation, canonical sequence, native file mapping and continuation ancestry. Keep canonical sequence distinct from
   BTDB TransactionId and application CommitUlong.
2. Specify how native TRL bytes and atomically bound blob metadata encode continuation, adoption, operation identity,
   genesis discovery and the allocation watermark. Demonstrate compatibility with existing native TRL/KVI files.
   Listing and numeric maximum alone cannot select history or allocate a new canonical branch safely.
3. Model `Applied`, `Rejected` and `Ambiguous`, including a write still pending after a read returns the old version.
   Specify reconciliation and abandonment rules for every mutation and prepared successor.
4. Derive conservative authority/grant deadlines, clock drift and pause assumptions, renewal margins, challenge
   invalidation and disconnected-grant drain bounds. Serialize role changes with confirmation and publication dispatch.
5. Exercise predecessor append versus adoption, competing genesis and cross-file linkage. In M3, implement key
   non-reuse, dependency-first KVI publication and restart of restore when concurrent cleanup removes a needed file.
6. Run a focused Azure capability/conformance experiment for the exact chosen atomic content/metadata operation,
   conditional append and lease outcomes. Verify current official provider contracts at that time and record evidence
   in ObjectStorages.md. This experiment precedes the complete production adapter.

Exit: concrete codecs and transition tables, reproducible race schedules and an explicit supported clock model.
Unproved mechanisms remain blockers; a mock's stronger semantics cannot serve as provider evidence. If native format
compatibility and the selected representation conflict, document that exact conflict before changing agreed behavior.

### M2 — Add transaction capture, admission and local file lifetime hooks

Dependencies: M0 and M1's identities/representation. Owner: B3.

Implemented core slice: constant-memory completed/acknowledged TRL positions and compactor retention.
The publisher coalesces native bytes through a fixed complete position; no per-transaction side index is needed.
Local execution continues independently when publication stops. Coordinator integration remains pending.

1. Track the latest completed commit/rollback position and the acknowledged prefix. Retain required TRLs using
   the existing compactor used-file mechanism. Do not store a transaction queue or duplicate file-range metadata.
2. Compare corresponding native TRL ranges directly in bounded chunks without decoding commands.

3. Reconcile secondary indexes in at most the first ObjectDB writing transaction after open. The startup coordinator
   waits for leadership before it; no generic per-writer admission gate is needed. Preserve writer-queue cancellation.
4. Signal the publication lane after committed local work. Non-application signals bypass lazy delay but cannot
   overtake an unresolved earlier operation; no bounded remote queue may block an application commit.
5. Stop remote publication independently of local writes and compaction. Reuse ordinary startup cache validation
   against Blob history; do not add a scratch collection, allocation mode or extension-based cleanup.

Exit: real BTDB tests cover multi-file commits/rollbacks, no-byte rollbacks, large values, batching, ObjectDB metadata
rollback/retry, writer cancellation and continued local execution after publication stops. No additional persistent kind marker, TRL command or
replication-owned historical root is introduced. Standalone suites still pass.

### M3 — Implement the canonical publisher and restore as one vertical slice

Dependencies: M1, M2. Owners: B3/B5.

Implemented slice: pinned native KVI snapshots with direct `IMemWriter` output, whole-PVL ID substitution, and an
internal publication-order helper backed by a `ReplicationFileSet` with separate local and asynchronous remote
inventories through `IFileReplicatedCollection` passed to the `BTreeKeyValueDB.OpenAsync` replication path.
The collection owner awaits `InitializeAsync` before opening; neither open nor prefetch performs implicit initialization.
Its inherited count/lookup/enumeration remain local-only; the explicit remote counterparts drive recovery discovery.
Existing constructors keep the original synchronous opening and eager metadata loading. In the async path, metadata is lazy, and final
prefetch requests all accepted KVI references (or all TRLs without a valid KVI) in parallel. Shared, bounded downloads
and checksum-verified cache reuse hide exact-ID imports inside the collection. Tests restore actual native KVI (plain/Brotli) through this remote boundary,
reuse confirmed downloaded/uploaded PVLs, retry uncertain PVL uploads at the same ID, and reject premature KVI upload.
Local compaction and remote export accept independent tokens. The internal `CanonicalTrlPublisher` now consumes real
capture records, conditionally publishes native suffixes, prepares successors before selecting the predecessor,
and CAS-adopts a restored tail. Ambiguous outcomes preserve the exact intent and pins; later records cannot overtake
it. Lease authority is checked before each dispatch; read-only reconciliation may finish after fencing. Native restore
and old/new-term race tests cover this lane. Production Azure transport, coordinator authority/ID allocation, checkpoint
TRL-ensure integration, cold discovery/validation, no-local-KVI compactor mode and GC remain integration work.

1. Implement the per-database mutation lane and operation lifecycle using conditional storage semantics.
2. Publish same-file suffixes, prepare complete successor chains before predecessor CAS, and handle genesis and
   term adoption through the same publication rules. Pin fixed cuts while later local transactions continue.
3. Capture native KVI and its full dependency closure with whole-PVL file-ID placements; never remap TRL. Stream KVI without local disk staging. Enforce the prerequisite barrier before the first KVI upload
   request, including block staging. Reconcile ambiguous publication before enabling cleanup.
4. Implement canonical discovery and cold restore, both with KVI and with genesis/TRL only. Validate ancestry and
   transaction closure before exposing state; replay selected TRLs in numeric order, not download-completion order.
5. Add sealed-file reuse based on selected identity/version, length and freshly computed checksum. Always download
   the active tail. Use bounded parallel lookahead and retain files referenced by replayed values.
6. Validate local files against selected Blob history before opening the DB. Missing/corrupt mixed cache and interrupted downloads
   rebuild or remain unavailable; input recovery reports a missing retained range instead of inventing skips.

Exit: a single publishing node can die at every storage boundary and a fresh node restores exactly the selected
complete history. Old prefixes never change, partial transactions never advance cursors, and ambiguous retries never
duplicate events. A failed KVI publication never permits deletion. Schedule actual deletion with a configurable operational delay,
provisionally about one day after files become obsolete; keep restore retry independent of that delay.

### M4 — Implement multi-node following, election and takeover

Dependencies: M3 and M1 authority rules. Owners: B1/B3/B6.

1. Implement follower-first restoration of every required database, then the shared transition engine for selection,
   adoption and activation. Reconcile progress since preparation before admitting canonical work.
2. Implement leader-centered sessions, short-lived grants, three-field TRL progress notifications, bounded range
   pulls, status and resume. Coalesce notifications and compare pulled TRL byte ranges directly.
   Cover unchanged-eventId schema progress, cross-file reads, vanished ranges and stale-session responses. Defer
   sending bytes with notifications to a later optimization; restore KVI/PVL directly from Blob.
3. Compare equal event coverage and rollback history. Matching advances metadata only; lag waits; binary
   divergence fences and requests restart/rebuild. Track local committed, reader-visible, confirmed and published
   positions separately.
4. Implement optimistic-tail adoption after the actual adopted boundary. Validate overlap, skip decisions and file
   lineage; reuse complete eligible transactions, then reopen/replay canonical files before fresh handlers.
5. Add isolated lease renewal and pending-work watchdogs. Distinguish idle input and progressing restore from stalled
   activation/publication. Model fatal host termination separately from graceful restart requests.

Exit: a three-node real-BTDB in-process cluster handles partitions, competing candidates, paused nodes and lost
responses. Canonical 102 plus local 101–105 adopts only 103–105, without rerunning their handlers or duplicating them
after interruption. Loss of direct confirmation neither blocks local commits nor prevents eligible takeover.

This is the first complete functional milestone. It is not production qualification or completion of version one.

### M5 — Complete database lifecycle and application integration

Dependencies: M4 and independent publication cancellation.

1. Wire monotonic application generation and the inline database set into the existing transition engine; implement
   prepared newer-generation handoff preference and lower-generation ineligibility.
2. Add leader-only genesis with current-input-end capture, fixed published initialization and restartable unpublished
   initialization. Do not allow provisional follower writes or election blocked on pending migrations.
3. Wire explicit index/schema writes and immediate asynchronous publication. Live schema reception detaches the
   database before application/confirmation and permanently disqualifies that node session from leadership.
4. Implement removed-database volatile continuation, graceful leader drain and lease transfer. Bound compaction
   cancellation points; application transactions retain their selected shutdown semantics.
5. Add the detached-session 15-minute monotonic no-valid-leader graceful restart rule, input/progress ports and
   application-requested exact skips. Preserve skip history across ordinary failover; test the separate backup
   restore procedure with a new stream and leader-record reset.

Exit: partial multi-database activation, add/remove upgrades, schema changes, delayed callbacks and handoff failures
all recover through the same engine. Detached local work cannot be published or promoted; reconnection cannot clear detachment.

### M6 — Add compaction, leak events and safe cleanup

Dependencies: M4/M5 and M3 publication/recovery. Owner: remaining B5 and Q6.

1. Add a node-local mode with full physical compaction on both leaders and followers, using each node's own inventory
   and allocator. Suppress all KVI creation paths and protect reader/current-tree, virtual-batch replay, comparison
   and export pins. Local compaction remains independent of peer progress and emits no TRL or peer messages.
2. Let leader checkpoint publication share the local physical pass and its sealed PVLs. Reuse verified downloaded
   files and successful upload placements; send only missing whole PVLs under fresh remote IDs. Preserve offsets and
   all TRL identities. Use independent local/remote cancellation tokens; leadership loss cancels only remote work.
3. Stream native KVI directly to Blob chunks, with placed PVL references, unchanged TRL cursor and dependency fileIds. Publish required PVLs/TRL first, then KVI; verify restore with every local source absent. Implement
   leader-only remote GC and abandoned staging reconciliation with permanent key non-reuse.
4. Remove the former compaction control/result transport requirements from implementation scope: no compaction
   operations, PVLs, rewrites or completion results are sent to running peers. Remote outputs serve normal Blob restore.
5. Separate leak detection from mutation. Publish bounded exact-key candidates through the application's event port,
   then use ordinary idempotent erase operations on every replica consuming that input.

Exit: all nodes compact independently without creating KVI or sending compaction messages; long-lived readers remain
valid; local and remote file inventories/IDs may differ; a remapped remote KVI restores the same logical state using
only Blob files. Interrupted remote export preserves the previous closure, and deletion during follower restore causes
rediscovery. No distributed local-delete command, compaction TRL transaction or follower remote-delete capability exists.

### M7 — Production adapters and operational qualification

Dependencies: M4 for adapter development; M5/M6 for complete version-one acceptance.

1. Implement the Azure adapter against the qualified M1 semantics, with opaque tokens, explicit ambiguity and no
   hidden conditional-write retries. Isolate authority traffic from data transfers. Run reusable storage suites
   against real Azure, including response loss and stale in-flight operations.
2. Implement ASP.NET Core/Kestrel hosting through `AddBTDBReplication`, `MapBTDBReplication` and the selected endpoint.
   Run the same codec/authentication/resume/backpressure suites as the in-process transport, then multi-process tests.
3. Add bounded/redacted diagnostics, readiness, progress and recovery metrics, configuration validation and examples
   for application-owned event execution. Document clock assumptions, input retention and host restart requirements.
4. Measure baseline versus replication allocations, throughput, commit latency, comparison RAM/disk growth,
   warm/cold recovery, handoff and large database restore. Target complete startup within 15 minutes for approximately
   100 GB, including cold download, local writes, validation, KVI open and TRL replay. Benchmark real Azure throughput
   and optimize bounded file/range concurrency and pipeline overlap. Set operational defaults from measurements,
   not guesses; report hardware/network conditions and end-to-end readiness time.

Exit: all architectural scenario families have evidence at the applicable model, real-BTDB and adapter levels;
remaining limitations are explicit. Only then update the README from architecture status and prepare release packaging.

## Validation and delivery discipline

- Deliver each milestone as reviewable changes on the current branch. Update CHANGELOG.md for implementation,
  behavior or API changes and run relevant checks after final edits. A plan-only addition needs no changelog entry.
- Run SourceGenerator tests before the main suite. During core work run focused regression tests and the affected
  suites; run `dotnet test BTDB.sln` before a core-behavior PR and at release qualification.
- Fault tests interrupt before dispatch, after storage effect and before response delivery. Cover delayed operations,
  cancellation, queue pressure and crashes; check invariants at intermediate steps as well as convergence.
- Property/schedule exploration complements named regression cases. Keep the smallest failing schedule as a test.
- Protocol safety and measured performance are separate exit criteria. Benchmark disabled replication as well as
  enabled paths, and do not infer production latency or GC improvements from allocation measurements alone.

The next milestone is M3 canonical publication and restore, using the implemented M2 position tracking, retention,
writer cancellation. Distributed allocation and role integration remain future work.
[M1Evidence.md](M1Evidence.md) records the tested mechanisms and their integration preconditions. KVI publication
and restore follow the existing M3 ordering; there is no separate KVI ancestry/selection prerequisite for M2. Do not begin with
HTTP controllers or reuse unconditional Azure uploads as canonical publication.
