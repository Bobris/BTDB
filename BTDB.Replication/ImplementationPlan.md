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

The following mechanisms are implemented. Integrate them rather than introducing parallel core paths.
See [ReplicationCore.md](../Doc/ReplicationCore.md) and [Testing.md](Testing.md) for contracts and evidence.

| Existing mechanism | Remaining replication work |
| --- | --- |
| Event-ID writer admission, explicit transactions, virtual batching and native commit/rollback | Coordinator scheduling and distributed comparison. Preserve ordinary per-event TRL and rollback semantics. |
| `TransactionLogCapture.Completed` / `Acknowledged` and compactor retention | Schedule publication of a complete native prefix. No transaction queue, side index, extra root or capture record. |
| `ObjectDB.InitializeRelations` read-only schema check and at most one startup writer | Wait for leadership before initialization; publish afterward. No second initializer or generic writer gate. |
| Odd/even `FileIdAllocator`, soft/hard TRL limits and cross-file transactions | Remote PVL/KVI IDs come from refreshed inventory with conditional creation; TRLs retain native IDs. No durable reservation counter. |
| `ReplicationFileSet.InitializeAsync` and native `BTreeKeyValueDB.OpenAsync` | Orchestrate retries when remote history changes. Existing cache validation, downloads, KVI selection and replay implement ordinary restart. |
| `CanonicalTrlPublisher.PublishNextAsync` / `PublishThroughAsync` | Wire authority, allocation and application/startup scheduling; qualify provider failures. |
| `ReplicationCompactor` via ordinary `Compact` | Schedule existing local maintenance. Reader/tree/capture/snapshot retention and no-local-KVI behavior already exist. |
| `KeyIndexSnapshot`, file-set PVL receipts and `CheckpointPublisher` | Wire remote export and GC. Existing pinned closure, streaming serializer, whole-PVL remapping and prerequisite barrier need no manifest or staging serializer. |
| Ordinary local writes after publication stops | Keep standard `.trl` files; irreversibly stop remote publication and mark the node session ineligible for leadership. No scratch collection, temporary extension, write barrier or special cleanup. |
| `CompactorLeakDetector` bounded exact-key collection and idempotent erase logic | Expose candidates through a small seam and integrate the application event. Public `RunLeakDetection` exposes only a summary; do not run local leak removal independently on replicas. |

## Simplifications to preserve in remaining work

| Concern | Baseline implementation / next step | Do not introduce without a failing scenario |
| --- | --- | --- |
| PVL/KVI publication retry | Conditional create with atomically bound SHA metadata; matching SHA confirms intended content. Keep pending ID/snapshot/map only in the live publisher. | Durable reservation counter, operation ID, persisted upload receipt, or restart-time reservation cleanup. |
| Remote discovery versus local refresh | Use fresh remote enumeration to choose upload IDs. `RefreshRemoteInventoryAsync` separately reconciles local mappings and invalidates placement confirmations. | Full local refresh or cache cleanup before every upload; it would repeat work and invalidate receipts during a checkpoint. |
| Object naming | Numeric native PVL/KVI identities, type and existing database scope; preserve the selected TRL key/metadata contract. SHA belongs in metadata. | Mandatory attempt directories, content-addressed keys, separate format object or uploaded GC ledger. |
| Confirmation and handoff drain | Wire `LeaseAuthority`, `ConfirmationWindow` and `ConfirmationGrants`; the latter already retains the maximum grant expiry. | A second deadline calculator, persisted grants, or per-follower revocation/acknowledgement protocol. |
| Restart | Fresh initialization/open and remote metadata reconstruct state; failed attempts dispose and repeat. | Persisting session receipts, pending snapshots, local-to-remote maps or confirmation state just to survive restart. |
| Provider qualification | Run conformance/fault tests against supported endpoints; use normal lease/CAS failures for runtime errors. | A mandatory extra four-write capability probe on every process startup. |
| Cleanup | Derive candidates from native checkpoint dependencies and current publication state; delete observed versions under authority. | Reservation cleanup, follower restore leases, a second checkpoint manifest, or a persistent cleanup state machine by default. |

SHA matching applies to immutable PVL/KVI creation, not to a growing TRL. Keep the canonical lane's exact conditional
intent and version-bound reconciliation. Likewise, an elapsed deletion delay alone does not prove a file is unused;
remote GC must still exclude the current closure and pending publication dependencies. These simplifications do not
close the production-provider or cross-session deletion proof work.

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

1. Reuse database/stream identity, term/session, native file/offset positions and opaque CAS tokens. Do not add
   operation IDs, canonical counters, frame hashes or a version framework without a demonstrated failure case.
2. Specify how native TRL bytes and atomically bound blob metadata encode continuation, adoption
   and genesis discovery. Demonstrate compatibility with existing native TRL/KVI files. Listing maxima choose
   PVL/KVI candidates, but do not select canonical TRL history; no allocation watermark is required.
3. Model `Applied`, `Rejected` and `Ambiguous`, including a write still pending after a read returns the old version.
   Specify reconciliation and abandonment rules for every mutation and prepared successor.
4. Derive conservative authority/grant deadlines, clock drift and pause assumptions, renewal margins, challenge
   invalidation and disconnected-grant drain bounds. Serialize role changes with confirmation and publication dispatch.
5. Exercise predecessor append versus adoption, competing genesis and cross-file linkage. In M3, qualify conditional
   creation, version-bound deletion, dependency-first KVI publication and restart of restore when concurrent cleanup removes a needed file.
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

1. Wire the existing completed/acknowledged positions into coordinator publication and comparison (M4).
2. Wait for leadership before calling `ObjectDB.InitializeRelations`; schedule publication after it returns.
   Preserve existing read-only schema checks, single-writer upgrades and writer-queue cancellation.
3. Let application/startup orchestration call `PublishNextAsync` or `PublishThroughAsync` after owned transactions.
   Immediate non-application publication bypasses lazy delay. Add no core callback or wakeup queue unless an
   uncovered producer demonstrates a need; remote work never blocks local commit.
4. Stop remote publication independently of ordinary local transactions and compaction. Reuse cache validation on
   restart. Existing capture, rollback, batching, schema and stopped-publication tests are baseline evidence.

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
completed positions, conditionally publishes native suffixes, prepares successors before selecting the predecessor,
and CAS-adopts a restored tail. Ambiguous outcomes preserve the exact intent and pins; later prefixes cannot overtake
it. Lease authority is checked before each dispatch; read-only reconciliation may finish after fencing. Native restore
and old/new-term race tests cover this lane. `PublishThroughAsync` can select a retained complete checkpoint cut
without including later local commits. It resolves earlier ambiguous plans before proceeding, preserves an already
dispatched later plan unchanged, and performs required term adoption in the same serialized lane. The caller must
supply a known complete position from this database, retain it until completion, and treat pending/conflict/authority
loss as an unsatisfied barrier. Native restore tests cover same-file and cross-file cuts, late responses, cancellation,
and authority loss. `CheckpointPublisher` now requires that canonical lane and establishes the snapshot cut before
publishing PVLs or starting any KVI chunk. Pending/conflict/authority loss returns without starting KVI; retries
preserve canonical intents and successful PVL placements. It rechecks the same session authority between uploads;
adapters must also check before each actual request. A restored verified tail satisfies the barrier even before the
first new local commit. Integrated tests publish real canonical TRLs and native KVI, then restore through an empty
cache. The separate replication compactor already suppresses local KVI creation. Production Azure transport,
coordinator authority/ID allocation, concurrent publication/GC recovery qualification and GC remain integration work.
`CanonicalTrlInventory` supplies selected genesis/TRL-only links and version-bound reads to the remote-backed
collection. The owner initializes that collection and calls ordinary `OpenAsync` directly, with no separate header
validator or restore wrapper.
Native transaction recovery uses ordinary `OpenAsync`; no extra core replay option or strict decoder mode is added.
Tests resume a new-term publisher from the restored tail, ignore orphan prepared objects, reject broken selected metadata links,
and retry after version changes or interrupted downloads. The caller still supplies the database-scoped genesis
identity; native opening reads database identity from native headers. This seam provides no checksum metadata, so cached TRLs are redownloaded.
Ordinary KVI-based restart already uses `ReplicationFileSet.InitializeAsync` and `BTreeKeyValueDB.OpenAsync`.
`RestartRecoveryTest.RestartFromCheckpointAfterHistoryCleanupResumesPublication` verifies this with genesis and
obsolete TRLs deleted, all original process state discarded, empty/corrupt cache, plain/Brotli KVI, post-checkpoint
commit/rollback replay, new-term publication and a second fresh restart. Tail metadata is read from the remote
object through the existing storage interface; no previous publisher state or genesis-only helper is needed.
This is verified restart coverage, not a missing KVI discovery mechanism. Concurrent publication/GC races,
automatic recovery orchestration and production allocation/adapters remain pending.
Deterministic `RestartRecoveryTest` schedules now also cover replacement-checkpoint cleanup and active-tail append
after discovery. Stale opens fail and ordinary rediscovery/open restores the new history, reusing valid cache files.
These are native-code/in-memory-storage checks, not production-provider or exhaustive race qualification.

Remote PVL/KVI allocation now refreshes inventory and chooses the next even ID without reservation state.
Conditional creation confirms matching SHA metadata on retry; a mismatch fences the session. Checkpoint publication
retains its KVI ID/snapshot/map across uncertain responses. Restart reconciles normal files. TRLs retain native IDs.
Provider adapters and coordinator integration remain pending.

Remaining work:

1. Wire the existing serialized mutation lane to coordinator authority and inventory-based conditional creation. Preserve exact
   unresolved intents and existing fixed-cut publication; do not implement a second publisher.
2. Orchestrate existing file-set initialization and native open for KVI restart; use TRL-only discovery where applicable.
   Retry discovery/open when concurrent publication or cleanup invalidates the selected inventory. No new core replay mode.
3. Qualify concurrent cleanup/publication, ambiguous checkpoint finalization and process/disk failures against actual
   adapters. Existing cache verification, shared bounded downloads, receipt reuse and checkpoint ordering remain baseline.
4. Maintain input-retention error reporting and version-bound remote deletion. Missing retained input is not a skip.

Exit: a single publishing node can die at every storage boundary and a fresh node restores exactly the selected
complete history. Old prefixes never change, partial transactions never advance cursors, and ambiguous retries never
duplicate events. A failed KVI publication never permits deletion. Schedule actual deletion with a configurable operational delay,
provisionally about one day after files become obsolete; keep restore retry independent of that delay.

### M4 — Implement multi-node following, election and takeover

Dependencies: M3 and M1 authority rules. Owners: B1/B3/B6.

Implemented component: `TrlPrefixComparer` pulls native bytes through `ILeaderTrlReader`, bound to the current
leader/database session. It uses two bounded buffers and existing capture acknowledgement, waits for local coverage,
and preserves acknowledgement on failure or divergence without changing the BTree. Native TRL IDs advance by two
(or one from a legacy even tail); no Blob listing, sorting or predecessor-header decoding occurs in follower comparison.
Tests read the live leader's retained local files, including bytes never uploaded to Blob.
The caller must validate the advertised complete cut and session authority and cancel on session replacement.
A match supplies neither a confirmation grant nor durability. Blob validation of candidate history belongs to
becoming leader before adoption/publication; bootstrap/recovery also retains its existing Blob path. The takeover
coordinator, peer transport authentication, schema detachment, grant coordination and restart orchestration remain pending.

1. Implement follower-first restoration of every required database, then the shared transition engine for selection,
   adoption and activation. Reconcile progress since preparation before admitting canonical work.
2. Wire the existing lease/challenge/grant helpers into leader-centered sessions, three-field TRL progress notifications, bounded range
   pulls, status and resume. Coalesce notifications and compare pulled TRL byte ranges directly.
   Cover unchanged-eventId schema progress, cross-file reads, vanished ranges and stale-session responses. Defer
   sending bytes with notifications to a later optimization; restore KVI/PVL directly from Blob.
3. Compare equal event coverage and rollback history. Matching advances metadata only; lag waits; binary
   divergence fences and requests restart/rebuild. Track local committed, reader-visible, confirmed and published
   positions separately.
4. On becoming leader, validate candidate history against selected Blob history before adoption/publication; never
   treat follower comparison acknowledgement as Blob durability. Implement optimistic-tail adoption after the actual
   adopted boundary. Validate overlap, skip decisions and file
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
4. Wire removed-database local continuation, graceful leader drain and lease transfer. Reuse the existing grant
   drain deadline; no per-follower revoke/ack protocol. Stop remote work independently of ordinary local execution;
   only host shutdown needs to cancel local maintenance.
5. Add the detached-session 15-minute monotonic no-valid-leader graceful restart rule, input/progress ports and
   application-requested exact skips. Preserve skip history across ordinary failover; test the separate backup
   restore procedure with a new stream and leader-record reset.

Exit: partial multi-database activation, add/remove upgrades, schema changes, delayed callbacks and handoff failures
all recover through the same engine. Detached local work cannot be published or promoted; reconnection cannot clear detachment.

### M6 — Add compaction, leak events and safe cleanup

Dependencies: M4/M5 and M3 publication/recovery. Owner: remaining B5 and Q6.

1. Schedule the existing replication `Compact` path on all nodes. Reuse its native reader/tree/capture/export
   lifetimes and allocator; add no pin registry, compaction transaction or peer compaction protocol.
2. Wire the existing snapshot/receipt/checkpoint pipeline to leader maintenance, keeping local and remote tokens
   independent. Reuse sealed whole PVLs and direct KVI streaming; no second serializer or manifest.
3. Implement leader-only remote GC from native dependencies and pending publication state, version-bound deletion and restore retry
   when selected files disappear. A failed or ambiguous checkpoint must not authorize deletion.
4. Add a small candidate-access seam to the existing bounded `CompactorLeakDetector`, then publish exact keys as an
   application-owned event. Reuse idempotent exact-key erase behavior when consuming it on every replica. The public
   detection summary is insufficient; independently invoking `RunLeakRemovalAsync` on each node is not replication.

Exit: all nodes compact independently without creating KVI or sending compaction messages; long-lived readers remain
valid; local and remote file inventories/IDs may differ; a remapped remote KVI restores the same logical state using
only Blob files. Interrupted remote export preserves the previous closure, and deletion during follower restore causes
rediscovery. No distributed local-delete command, compaction TRL transaction or follower remote-delete capability exists.

### M7 — Production adapters and operational qualification

Dependencies: M4 for adapter development; M5/M6 for complete version-one acceptance.

1. Implement the Azure adapter against the qualified M1 semantics, with conditional immutable create/SHA reconciliation,
   opaque tokens, explicit ambiguity and no
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

The next work is M3 recovery-race qualification, inventory-based conditional creation and coordinator wiring of the existing
publisher/restore paths, followed by M4 peer comparison and takeover. Core capture, ordinary restart, local compaction
and streamed checkpoint export are implemented baselines.
[M1Evidence.md](M1Evidence.md) records the tested mechanisms and their integration preconditions. KVI publication
and restore follow the existing M3 ordering; there is no separate KVI ancestry/selection prerequisite for M2. Do not begin with
HTTP controllers or reuse unconditional Azure uploads as canonical publication.
