# BTDB.Replication Working Agreement

- Write documentation, architectural notes, source code, identifiers, and other project artifacts in English.
- The user may communicate in Czech or Czenglish; respond naturally, but keep the project content in English.
- Implement the smallest mechanism that satisfies the agreed behavior. Before adding a protocol field, persistent
  record, state, timer, handshake, hash, abstraction or optimization, establish why the simpler implementation is
  insufficient: a concrete correctness/failure case, preferably reproduced by a deterministic test, or a measured
  substantial performance cost on a relevant workload. Record the evidence briefly beside the decision or change.
  Speculative future usefulness, generic robustness, or appearance in an older design is not sufficient justification.
  Defer unproven additions and derive information from existing native TRL/storage/session context where possible.
  Existing architecture/backlog mechanism descriptions are candidates subject to this rule, not a checklist to build.
  Preserve agreed behavior and safety invariants; simplify the mechanism rather than silently weakening its guarantees.
- The library is currently in the architecture-brainstorming phase. Do not add an implementation unless the user explicitly asks for one.
- Use `Architecture.md` as the shared living document for protocol brainstorming, alternatives, open questions, and
  decisions.
- Keep each protocol rule in its normative owner section of `Architecture.md`: shared identities/invariants, the canonical
  TRL publisher, the cluster transition engine, or the follower acceptance/recovery path. Link from explanatory
  sections instead of copying another algorithm. Keep alternatives in design history and unresolved mechanisms in the
  blocker register; do not imply that a desired invariant already has an implementation or proof.
- Keep the provider-neutral storage contract, `denoland/celld` interface research, and provider-specific Azure/S3
  behavior in `ObjectStorages.md`; link to it from `Architecture.md` instead of duplicating that research.
- Treat version one as Azure-first, including native Blob leases and a semantic conditional atomic tail append. Keep
  Azure Block Blob staging/commit mechanics inside `ObjectStorages.md`, not the failover protocol, and keep Amazon S3
  as later provider research unless the user revisits that priority.
- Put every nondeterministic or external boundary behind an explicit injected abstraction. The complete protocol must
  support multiple isolated nodes in one process through deterministic in-memory and fault-injecting adapters; HTTP,
  Kestrel, Azure SDK types, sockets, wall-clock time, randomness, and process-global state must not leak into the core.
- Use a leader-centered topology with no peer-to-peer membership mesh. Followers communicate only with the selected
  leader through the injected peer transport; loss of that session may trigger Azure lease/CAS contention but never
  grants authority by itself.
- Use short-lived confirmation grants (variant A). Direct confirmation is an optimization and consistency/split-brain
  diagnostic, never a vote or a prerequisite for local commits or eligible takeover. After selecting its own new term,
  a node ignores predecessor live messages and determines subsequent history after activation, preserving published
  durable history. Planned lease transfer drains or revokes old grants; exact timing and race proofs remain B1.
- Support rolling application upgrades through a monotonic application generation and an inline `databaseNames` array
  selected atomically in the leader JSON. Keep instance identities, published initialization cursors in durable
  published per-database metadata; do not add separate catalog or transition documents. A prepared newer-generation follower has handoff priority. Databases added by that
  generation wait for leader-only initialization without provisional writes. Genesis sets CommitUlong to the event ID
  preceding the first applied input and publishes immediately to Blob Storage; followers restore published state. If initialization was not published, a successor may initialize
  from scratch; never replace existing published history or require a preselected provisional seed. A database
  removed by the selected database set is outside cluster coordination. Names are never reused. Old nodes continue
  independently from their local views in ordinary local files without publication until shutdown; no exact frozen boundary, retirement CAS,
  or durable retirement metadata is required. Previously dispatched writes to the abandoned namespace may still land. Once a newer database set is selected, older generations can follow compatible
  databases but can never lead the cluster again.
- Allow newer applications to author explicit non-application schema transactions (for example secondary-key changes)
  through the current leader's ordinary canonical TRL. They advance canonical sequence without consuming application
  input. A live follower receiving one detaches the affected database before application/confirmation, continues from
  its own view in ordinary local files without publication until replacement, and accepts no later canonical work for it. This is not a mismatch
  restart. Compatible replacement startup replays schema TRL or restores a checkpoint containing it. Detached suffixes
  never become canonical or takeover input; native ObjectDB schema and application compatibility govern checkpoint opens.
- Secondary-index reconciliation is at most the first ObjectDB writing transaction after open, before application
  processing. Wait for leader authority in startup orchestration, not a generic KV writer gate. Immediately after local commit, publish the complete recovery
  closure through asynchronous CAS directly on canonical TRL, bypassing lazy flush delays without waiting in Commit
  or blocking dependent local work. Ordinary application
  commits remain local-only. Follower startup restores canonical bases without running migrations, so pending schema
  work cannot deadlock election; genesis and required activation schema writes run under selected activating authority.
- Use one ordered event stream for the entire cluster and all replicas. Per-database cursors refer to that shared
  stream. Backup restore creates a new stream and deletes `leader.json` before startup, intentionally clearing old
  timeout skip entries. Ordinary failover preserves both the stream and skip list.
- The application owns failure classification: ordinary rollback, retry, fail-fast (for example out-of-memory), and
  optional exact-input skip requests. Never automatically convert a rollback or exception into a skip. Keep identical
  rollback attempts/outcomes across nodes as part of transaction comparison. Only the lease owner persists requested
  skip markers; preserve them across terms, never rewrite published history, and never delay required termination
  indefinitely for a marker. Watchdogs distinguish pending stalled work from idle input or progressing restore.
- Every node starts as a follower and verifies/restores all required published databases against Blob Storage, reusing verified local files
  into disposable temporary local storage before attempting leadership. Unpublished additions and initial empty-cluster
  bootstrap have no files to download. New genesis captures the current input end and sets CommitUlong to the predecessor
  event ID of the first input to apply. A published cursor is fixed; an unpublished retry may capture a later end.
- Ordinary application transaction commit is sufficient for local success. Default reads use local snapshots without waiting
  for other nodes or Blob publication. The application owns memory-batch visibility, error handling and all external
  effects; replication implements neither a business command acknowledgement nor an outbox/side-effect dispatcher.
- Never gate follower commits on leader progress or external I/O. Keep one current follower BTree and native TRL files with one acknowledged
  (fileId, offset) comparison position, with no replication-owned confirmed or intermediate roots. Compare corresponding TRL byte ranges directly in bounded chunks over equal
  event coverage, with file identity and range boundaries bound to the replication session. Do not decode commands
  or normalize payloads for comparison. A match
  advances confirmation metadata only. A structural mismatch fences and restarts the follower for canonical rebuild;
  never repair the live suffix or retain old roots for it. Ordinary user read transactions retain normal BTDB pins.
  Confirmed-only reads cannot silently expose a newer speculative head. Compaction applies only to an eligible current
  head; otherwise skip that optional operation without rollback or suffix replay.
- Use the implemented per-writer virtual batching (`StartWritingTransaction(inBatch: true)`) as a memory optimization.
  Each event retains its ordinary log transaction/commit; batching does not change TRL payloads or require extra event
  markers, different-batch normalization, or tail reframing. Compare actual event history; real divergence still
  restarts the follower. A failed transaction preserves earlier committed events in the virtual batch. Verify external
  application/ObjectDB rollback integration separately from core batching. Keep optimistic TRL separate and reuse only
  complete validated transactions after the adopted end during live takeover; retry must not duplicate events.
- Treat local disk exhaustion as a fatal node error with normal fencing, unavailability and restart/rebuild. Terminate
  the old process/readers before removing obsolete/invalid cache. Reuse files matching the selected remote identity,
  length and freshly calculated whole-file checksum; download missing files and replay. Do not require simultaneous
  old/new full copies, a special quota or
  low-disk mode. A compacted restore may be smaller but is not guaranteed; insufficient space keeps the node unavailable.
- The application ensures identical ordered transactions on all nodes hosting the same database, including rollback,
  and supplies recoverable input/progress through injected interfaces. Kafka is only an example; replication owns no
  consumer or handler execution. Only the leader publishes shared canonical Blob history. Missing unpublished work is
  recovered through the application, never by inventing skip outcomes.
- Every node, including followers, may run full node-local physical compaction independently. It creates no KVI,
  including small-waste and cleanup-only paths, and preserves logical state and ordinary TRL. Local file deletion
  protects the current tree, readers, pending virtual-batch rollback/replay, the capture TRL boundary and referenced export roots; a
  self-contained local restart cut is not required because startup validates/restores Blob history.
- Local and remote work may share one physical compaction pass. Only the current leader publishes the checkpoint.
  Remember fully downloaded, sealed files verified present in Blob Storage, including verified peer downloads.
  Reuse those PVLs without upload or remapping. Upload other whole sealed local PVLs under free remote IDs and retain
  successful placements for later compactions. These placements change only remote KVI references, never local IDs.
  TRL IDs, offsets and bytes are never remapped. Ensure all prerequisite PVLs/TRL before the first KVI chunk upload;
  serialize native KVI directly to the uploader without requiring a local staging file.
- Give local compaction and remote publication separate cancellation tokens. Loss of leadership cancels only remote
  uploads, KVI export/finalization and remote deletion; local compaction may finish. Host shutdown can independently cancel local maintenance. A remote failure must not propagate through a linked local token.
- Neither local nor remote compaction sends operations, PVLs, pointer-rewrite plans, completion results or deletion
  commands to running peers. Remote outputs are published only to Blob Storage and consumed during ordinary
  open/rebuild. There is no compaction message family or compaction-result endpoint. Transfer a canonical KVI only
  as a database's bootstrap artifact on open/rebuild; never push later KVIs into a running follower.
- Only the leader may delete remote files after replacement PVLs/canonical TRL and then the remapped KVI are
  successfully published and the selected recovery closure no longer needs them. Plan a configurable operational
  deletion delay, provisionally about one day after files become obsolete. Do not track follower acknowledgements
  or add restore leases. The delay does not guarantee old-KVI recoverability. A restore losing files starts over against the latest
  published KVI. Never reuse retired keys; delayed old deletes must remain harmless. Local reader pins are separate.
- CAS directly on canonical TRL establishes durability for complete published transactions. Do not add a second
  database-state CAS or per-batch state.json. Transactions may span several TRL files; the earlier unsplittable-transaction
  rule is withdrawn. Capture ordered ranges through commit/rollback, preserve supported file-offset limits, and never
  accept a partial transaction. Prepare and verify successor files first, then publish the whole transaction with the predecessor TRL CAS.
  Native KVI-last discovery is selected; TRL continuation and per-object CAS ordering still require proof. Checkpoint metadata never gates each append.
- Assume the initial existing database is already in Blob Storage. Do not design a local-database import workflow.
- Newly written replication TRLs use odd numeric file IDs; all new non-TRL files use even IDs, including KVI/PVL
  and sub-database files. Accept valid legacy databases with arbitrary file-ID
  parity unchanged. Before starting a write that would append to an even TRL, close that file and allocate a fresh odd
  TRL immediately, regardless of the size target. Preserve old IDs/value references and normal allocator non-reuse;
  rotate before transaction bytes, without changing CommitUlong or canonical sequence. Remote changes remain leader-only.
- Treat every local file as a disposable, untrusted cache. Arbitrary missing, truncated, stale, or mixed-generation local
  content must cause full validation and rebuild or fail-closed unavailability, never inferred canonical state.
- Graceful leader shutdown stops remote publication, not local transaction execution. Keep ordinary local files,
  writes, rollback, readers and compaction. Do not add a scratch collection, special extension, allocation mode,
  writer barrier or separate cleanup path. Startup validates local cache against selected Blob history and discards
  unselected files or replaces divergent tails through normal restore. Previously dispatched writes may still land;
  preserve authority fencing and ambiguous-write reconciliation. A detached session never resumes publication.

- A committed transaction with unchanged CommitUlong is non-application; application commits change it. Genesis alone
  is non-application while setting the predecessor cursor. Decode rollback separately. Do not add reserved Ulong slots
  or persistent kind sidecars. Preserve ordinary rollback TRL and compare it in order with subsequent commits; no
  separate attempt protocol or synthetic record for rollbacks that emit no bytes.
- In replication mode prohibit synchronous StartTransaction; use StartReadOnlyTransaction for reads and the existing
  asynchronous StartWritingTransaction for writes. ObjectDB initialization checks the complete relation list read-only,
  then persists new empty schemas and index upgrades in at most one startup writer under leader authority.
  Follower application writes remain
  allowed. Post-commit flush is immediate but asynchronous; local completion does not await Blob.

- Start uploading native KVI only after every required PVL and canonical TRL through its fixed cursor has finished
  publication. Do not even stage KVI blocks earlier; merely completing KVI last is insufficient. Later tail bytes
  beyond that cursor need not finish first. Only successful KVI publication permits
  deletion of old unused files; reconcile ambiguous KVI writes first. Do not add a separate checkpoint object, manifest,
  current-checkpoint pointer or selection CAS. Discover native KVIs from database files and validate their references
  and canonical ancestry. The word checkpoint is only shorthand for KVI plus its required files.

- Prioritize local disk space and startup time over aggressive Blob-space savings. Do not blanket-delete valid cache.
  Reuse only sealed files verified against selected remote identity/version, length and whole-file checksum; ETag and length
  alone do not prove equality. SHA-256 blob metadata is the proposed checksum for newly published files; no extra object.
  Do not hash a growing TRL; always download the last active TRL again on restore. Compute its final checksum after
  sealing, when it becomes eligible for reuse. Missing trustworthy checksum for a sealed file falls back to download. Unverified local data is never reusable canonical input.
- Internally download in parallel with bounded byte lookahead and prioritize the next replay file. Without KVI, obtain
  all canonical TRLs in ascending numeric ID order and replay in that order while later files download. Out-of-order
  completion never changes replay order. Do not delete a replayed TRL while roots still reference its values. With KVI,
  fetch only its required recovery files. All required databases must finish restore before election.

- Application StartWritingTransaction receives eventId and automatically sets CommitUlong; do not require separate
  application assignment/validation. Non-application commit triggers immediate asynchronous flush, not CommitAsync
  or a blocking Blob wait. Followers may ask the current leader for recent tail bytes with a short timeout, falling
  back to replaying unavailable unpublished input. Preserve normal authority/lineage checks and CAS ambiguity handling.
- Persist relation schemas during startup registration, including new empty relations; never defer schema creation
  to application upserts or ID allocation. Initialization changes use one leader-only non-application transaction
  with unchanged CommitUlong and live-follower detachment.

- Schema detachment permanently disqualifies that node session from leadership contention, lease acquisition and
  planned handoff, even after reconnection or database-set changes. After 15 continuous monotonic minutes without
  valid current-leader evidence, request graceful restart through the host; reset only on fresh valid leader evidence.
  Do not fail-fast merely because this timer expires. Detached local work never becomes canonical. A fresh compatible session must
  restore all required databases before eligibility. Ordinary network disconnect remains eligible for normal failover.

- The application event log supplies application durability and replay. Blob KVI/TRL accelerates recovery from a
  valid base; unpublished BTDB tail may be regenerated. Do not add client durability acknowledgements or Blob waits.
  Published-boundary tracking is internal restore bookkeeping. Preserve complete-file/transaction, ancestry and CAS
  fencing checks so event replay begins at a valid base/cursor. Distinguish engineering proof tasks from user choices.

- TRL sizing uses an immutable strategy whose only input is the created TRL numeric ID. Soft limits rotate only
  between transactions; hard limits below 4 GiB permit splitting between commands and include headers/terminators.
  Keep the production mapping stable across nodes, restarts and application versions; tiny test policies are injectable.

- Baseline peer data flow is notification plus pull: within the established database/authority session, announce only
  `(eventId, trlFileId, trlPosition)` at a complete committed leader-local boundary. Followers fetch native TRL bytes
  from their last compared leader position through that end, then compare those bytes directly against the corresponding local ranges.
  Coalesce notifications; notify when the position changes even if eventId does not, including schema commits.
  Bind range requests to exact term/session/file lineage and validate complete transactions before confirmation.
  Notification alone implies neither equality nor Blob durability. Defer byte piggyback/push optimizations that avoid
  the extra request; they must eventually reuse the same acceptance path. No per-transaction HTTP envelope, range
  list, kind or hash is required in the notification. Bootstrap KVI/PVL comes directly from Blob, not peer HTTP.

- Target complete startup within 15 minutes for approximately 100 GB, including cold Blob-to-local restore and replay.
  Qualify end-to-end readiness, not download time alone. Optimize transfer throughput using measured bounded file/range
  concurrency and overlap with local writes, validation and replay; do not claim this target is already achieved.

- Track only the latest complete local TRL position and the acknowledged (fileId, offset) prefix. Do not create
  per-transaction capture records, index files, sequences or duplicate range lists. Publisher work snapshots a fixed
  complete prefix, coalesces transactions and advances acknowledgement only after selection; compaction retains
  unacknowledged TRLs. Startup schema publication can explicitly request publishing the current completed position.
