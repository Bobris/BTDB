# BTDB.Replication Working Agreement

## Working rules

- Write documentation, architectural notes, source code, identifiers, and other project artifacts in English.
- The user may communicate in Czech or Czenglish; respond naturally, but keep the project content in English.
- Implement the smallest mechanism that satisfies the agreed behavior. Before adding a protocol field, persistent
  record, state, timer, handshake, hash, abstraction or optimization, establish why the simpler implementation is
  insufficient: a concrete correctness/failure case, preferably reproduced by a deterministic test, or a measured
  substantial performance cost on a relevant workload. Record the evidence briefly beside the decision or change.
  Speculative future usefulness, generic robustness, or appearance in an older design is not sufficient justification.
  Defer unproven additions and derive information from existing native TRL/storage/session context where possible.
  Preserve agreed behavior and safety invariants; simplify the mechanism rather than silently weakening its guarantees.
- The library is implemented but not production-qualified. Review and design requests report findings; implement
  changes only when the user asks for them.
- `Architecture.md` is the living design document: behavior, invariants, selected mechanisms, the blocker register
  (stable IDs B1, B3, B5, B6) and backlog, and a short design history. Keep each rule in its owning section; link from
  other sections instead of copying it. Keep superseded alternatives only as one-line history and never imply that an
  open blocker is closed without linked evidence.
- Keep the provider-neutral storage contract, `denoland/celld` research and Azure/S3 behavior in `ObjectStorages.md`;
  test coverage and measurements in `Testing.md`; implementation status in `ImplementationPlan.md`; hosting in
  `../Doc/ReplicationHosting.md`; core APIs in `../Doc/ReplicationCore.md`. Update these whenever code changes what
  they describe, and remove statements that no longer hold.
- Put every nondeterministic or external boundary behind an explicit injected abstraction. The complete protocol must
  run multiple isolated nodes in one process through deterministic in-memory and fault-injecting adapters; HTTP,
  Kestrel, Azure SDK types, sockets, wall-clock time, randomness, and process-global state must not leak into the core.

## Topology and authority

- Version one is Azure-first: native Blob leases on `leader.json` and a semantic conditional atomic tail append for
  canonical TRL. Keep Azure Block Blob staging/commit mechanics in `ObjectStorages.md`; Amazon S3 remains later
  research.
- Use a leader-centered topology with no peer-to-peer membership mesh. Followers communicate only with the selected
  leader through the injected peer transport; losing that session may trigger lease contention but never grants
  authority by itself.
- Use short-lived confirmation grants (variant A). Direct confirmation is an optimization and a consistency/split-brain
  diagnostic, never a vote or a prerequisite for local commits or eligible takeover. After selecting its own new term,
  a node ignores predecessor live messages and determines subsequent history after activation, preserving published
  durable history. Planned lease transfer waits out every issued grant; production timing qualification is B1.
- Every node starts as a follower and verifies/restores all required published databases against Blob Storage,
  reusing verified local files, before attempting leadership. Unpublished additions and an empty cluster have no files
  to download.

## Application contract

- Use one ordered event stream for the entire cluster; per-database cursors refer to it. The application ensures
  identical ordered transactions on all nodes hosting the same database, including rollback, and supplies
  recoverable input through injected interfaces. Kafka is only an example; replication owns no consumer or handler.
- `BTDB.Replication.EventLog` is an optional sibling component that can serve as that application event log: named
  topics of opaque records in Blob Storage with per-topic ETag ownership. It never changes database replication:
  Commit still never awaits Blob Storage, and execution, retry and skip policy stay in the application. Its design and
  status live in `EventLogImplementationPlan.md`.
- The application event log supplies durability and replay. Blob KVI/TRL accelerates recovery from a valid base; an
  unpublished BTDB tail may be regenerated. Do not add client durability acknowledgements, Blob waits, an outbox or a
  business command acknowledgement. Published-boundary tracking is internal restore bookkeeping.
- Ordinary application commits are sufficient for local success, and default reads use local snapshots. The
  application owns memory-batch visibility, error handling and all external effects.
- Application event timeouts, skip decisions, execution/commit arbitration, retries, termination and marker
  interpretation belong entirely to the application. Do not implement them in replication or list them as missing
  replication work. Activation/publication/maintenance watchdogs monitor only replication work.
- Provide only generic read/conditional-write access to `applicationData` in `leader.json`: any node reads, only the
  active leader writes with lease plus ETag, and updates preserve authority fields and unrelated data. Replication
  never interprets this JSON.
- Application `StartWritingTransaction(eventId)` sets `CommitUlong` automatically. In replication mode prohibit
  synchronous `StartTransaction`; use `StartReadOnlyTransaction` for reads and the asynchronous writer for writes.
- Backup recovery is operational: scale to zero, copy the backup into primary Blob storage, then scale up. Do not add a
  backup-import API, a new application stream or a leader-record reset procedure. Assume the initial database is
  already in Blob Storage; there is no local-database import workflow.

## Transactions and non-application writes

- A committed transaction with unchanged `CommitUlong` is non-application; application commits change it. Genesis is
  non-application while setting the predecessor cursor. Rollbacks are decoded separately, kept in ordinary TRL and
  compared in order with later commits; no attempt protocol or synthetic rollback record other than the native
  rollback that terminates an unfinished published transaction on open. No reserved Ulong slots,
  kind sidecars or new `KVCommandType`.
- Non-application writes (genesis, schema) run only under leader authority, waited for in startup orchestration, not
  a generic core writer gate. ObjectDB checks the complete relation list read-only and persists new schemas, index
  upgrades and creation callbacks in at most one startup writer; never defer schema creation to application upserts
  or ID allocation. Each non-application commit is published immediately and asynchronously through canonical TRL CAS
  without blocking Commit or dependent local work. Follower startup restores canonical bases without running
  migrations, so pending schema work cannot deadlock election.
- Use native virtual batching (`StartWritingTransaction(inBatch: true)`) as a memory optimization only: every event
  keeps its own TRL transaction, so no extra markers, normalization or reframing. A failed transaction preserves
  earlier committed events in the batch. Reuse optimistic TRL after the adopted end during takeover without
  re-executing or duplicating events.
- TRL sizing uses an immutable strategy whose only input is the TRL numeric ID, identical across nodes, restarts and
  versions. Soft limits rotate between transactions; hard limits below 4 GiB may split between commands.
- New replication TRLs use odd file IDs and all other new files even IDs; valid legacy files keep their IDs. Writable
  startup converts a legacy tail by conditionally publishing an odd successor containing only its native header, selected
  from remote inventory before application work. Concurrent starts verify the same header; no opt-in flag or leader
  lease is needed for this create-only bootstrap. All later TRLs use exactly +2. IDs, offsets and bytes are never remapped.

## Publication, following and database sets

- CAS directly on canonical TRL establishes durability for complete published transactions; no second state CAS,
  per-batch `state.json` or per-transaction capture record. Track only the latest complete local position and the
  acknowledged prefix. All terms share `{id}.trl`. Publish native files in order with conditional create/CAS;
  compare existing bytes on create collisions and request restart on divergence. A crash may leave an unfinished
  final transaction: replay exposes only complete transactions, and writable open ends it with a native rollback at
  the start of the successor of the last published TRL, so no candidate must reproduce published bytes of an
  abandoned transaction. Reconcile ambiguous writes before later mutations.
- Followers poll the leader once per step: per database the progress `(eventId, trlFileId, trlPosition)`, the
  published cut, the latest non-application commit position and inline native TRL bytes within a budget, plus one
  grant; remaining bytes come by range. Compare native bytes directly in bounded chunks; do not decode commands or
  normalize payloads. No per-transaction envelope, range list, kind or hash. A match advances confirmation metadata
  only; a mismatch restarts the follower for canonical rebuild, never repairing the live suffix or retaining old roots.
  Never gate follower commits on leader progress or external I/O. Bootstrap KVI/PVL comes from Blob, not peers.
- Schema detachment: when the leader announces a non-application commit beyond a follower's canonical base, the
  follower detaches that database before comparison, continues locally without publication, and is permanently
  disqualified from leadership and handoff for the session, even after reconnection or database-set changes. After 15
  continuous monotonic minutes without valid leader evidence it requests a graceful restart; do not fail fast. A fresh
  compatible session restores normally, replaying or restoring the schema. Ordinary disconnects remain eligible.
- Rolling upgrades use a monotonic application generation and an inline `databaseNames` array selected atomically in
  `leader.json`; no catalog or transition documents. The selected generation is an election floor: older generations
  can follow compatible databases but never lead again. A prepared newer-generation follower has handoff priority.
  Added databases wait for leader-only genesis without provisional writes; genesis sets `CommitUlong` to the event ID
  preceding the first input and publishes immediately. Unpublished initialization may be recreated; never replace
  published history. A removed database is outside cluster coordination; names are never reused; old nodes continue
  it locally until shutdown and delayed old writes to it are harmless.
- Graceful shutdown, removal and detachment stop remote publication irreversibly, not local execution: ordinary
  writes, rollback, readers and compaction continue in ordinary files. No scratch collection, special extension,
  writer barrier or cleanup path; startup validates the cache and discards unselected files. A detached session never
  resumes publication.

## Checkpoints, cleanup and restore

- A checkpoint is a native KVI plus its required files; no checkpoint object, manifest, pointer or selection CAS.
  Upload every required PVL and publish canonical TRL through the KVI cursor before staging any KVI block; stream the
  KVI directly without a local staging file. Reuse verified PVL placements, upload others whole under fresh even
  remote IDs, and revalidate remembered placements after promotion. A running follower never receives a later KVI.
  Skip re-exporting an unchanged checkpoint but keep collecting garbage.
- Every node may run full local physical compaction independently without creating a KVI; it preserves logical state,
  ordinary TRL, readers, virtual-batch replay, the capture boundary and export roots. Only the leader publishes
  checkpoints and deletes remote files; no compaction operations, results or deletion commands go to peers. Local
  compaction and remote publication use separate cancellation tokens; losing leadership cancels only remote work.
- Only the leader deletes remote files, and only after the replacement KVI is published. Cleanup uses per-object
  deletion deadlines with a positive delay (production at least a day), ETag-conditional deletion that rechecks the
  deadline, and version protection that clears a mark before reuse. Never rewrite an unmarked file just to change its
  version. Do not track follower acknowledgements or add restore leases; a restore that loses a file starts over from
  the newest KVI. Never reuse retired keys. Preserve the canonical TRL chain from the oldest checkpoint dependency; the
  native KVI references define its recovery closure after pruning genesis. Blob metadata is limited to
  `btdb_sha256` and `btdb_delete_after`; no term, successor or recovery-root metadata.
- Treat every local file as a disposable, untrusted cache. Reuse only sealed files verified against the selected
  remote identity, length and a freshly calculated whole-file SHA-256; ETag and length alone do not prove equality.
  PVL/KVI and newly sealed canonical TRLs carry SHA-256 metadata. Restore downloads the growing tail and older
  sealed files without a checksum again. Unverified local data is never canonical input.
- Download in parallel with bounded memory; without a KVI replay all canonical TRLs in ascending lineage order while
  later files download; with a KVI fetch only its required files. All required databases finish restore before
  election. Prioritize local disk and startup time over aggressive Blob-space savings.
- Local disk exhaustion is a fatal node error with normal fencing, unavailability and restart/rebuild; no special
  quota or low-disk mode. Terminate old readers before removing obsolete cache.
- Target complete startup within 15 minutes for about 100 GB, including cold restore and replay. Qualify end-to-end
  readiness with real measurements. Recorded Azure benchmarks are in `Measurements.md`; they do not qualify the
  production application or every deployment.
