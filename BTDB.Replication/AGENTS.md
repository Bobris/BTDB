# BTDB.Replication Working Agreement

- Write documentation, architectural notes, source code, identifiers, and other project artifacts in English.
- The user may communicate in Czech or Czenglish; respond naturally, but keep the project content in English.
- The library is currently in the architecture-brainstorming phase. Do not add an implementation unless the user explicitly asks for one.
- Use `Architecture.md` as the shared living document for protocol brainstorming, alternatives, open questions, and
  decisions.
- Keep each protocol rule in its normative owner section of `Architecture.md`: shared identities/invariants, the database
  state publisher, the cluster transition engine, or the follower acceptance/recovery path. Link from explanatory
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
- Support rolling application upgrades through a monotonic application generation and an inline `databaseNames` array
  selected atomically in the leader JSON. Keep instance identities, published initialization cursors in durable
  per-database state; do not add separate catalog or transition documents. A prepared newer-generation follower has handoff priority. Databases added by that
  generation run provisionally and locally until the leader publishes an initial empty transaction with the starting event cursor. All nodes
  discard provisional copies and reopen canonical state. If initialization was not published, a successor may initialize
  from scratch; never replace existing published history or require a preselected provisional seed. A database
  removed by the selected database set is outside cluster coordination. Names are never reused. Old nodes continue
  independently from their local views in disposable `.temptrl` until shutdown; no exact frozen boundary, retirement CAS,
  or durable retirement metadata is required. Previously dispatched writes to the abandoned namespace may still land. Once a newer database set is selected, older generations can follow compatible
  databases but can never lead the cluster again.
- Use one ordered event stream for the entire cluster and all replicas. Per-database cursors refer to that shared
  stream. Backup restore creates a new stream and deletes `leader.json` before startup, intentionally clearing old
  timeout skip entries. Ordinary failover preserves both the stream and skip list.
- For a stuck event handler, use the proposed persisted exact-event skip and bounded host-restart policy in
  `Architecture.md`. Keep rare skip entries in `leader.json`, preserving them across authority changes. Only the lease
  owner writes this list. Timeout decisions cannot undo published canonical history. Proposed one-day cleanup also
  requires durable event progress; idle input or storage stalls must not be mislabeled as handler failures.
- Never gate follower commits on leader progress or external I/O. Keep one current follower BTree and disk-backed TRL
  comparison data, with no replication-owned confirmed or intermediate roots. Compare decoded TRL structure over equal
  event coverage while ignoring different batching, framing, and permitted physical encoding differences. A match
  advances confirmation metadata only. A structural mismatch fences and restarts the follower for canonical rebuild;
  never repair the live suffix or retain old roots for it. Ordinary user read transactions retain normal BTDB pins.
  Confirmed-only reads cannot silently expose a newer speculative head. Compaction applies only to an eligible current
  head; otherwise skip that optional operation without rollback or suffix replay.
- Keep canonical TRL free of new replication transaction kinds. After the initial empty cursor transaction, commits
  consume an event range or encode an ordinary metadata-only singleton skip. All replicas may batch independently;
  emit a reserved event-end Ulong update into TRL after every event, without committing. Compare event groups while
  ignoring transaction start/end framing; only committed groups qualify. Failed batches roll back and retry individually; failed singleton handlers roll back
  before the cursor-only commit. Keep optimistic TRL beside canonical files. On takeover append only validated committed event groups strictly
  beyond the reconciled canonical end, reframing a partial local batch and rebasing deltas as needed. Reconcile retries
  to avoid duplicate events; reopen canonical bytes without rerunning copied handlers. Structural mismatch, provisional
  state, and disposable `.temptrl` never qualify for promotion. Leak removal remains a parent-published ordinary ordered event, never a special TRL command.
- Run full compaction only on the leader. Distribute sealed PVL artifacts and bounded physical pointer-rewrite operations
  out of band over the existing leader-to-follower session, never through TRL and never as canonical transaction sequence.
  Missing an out-of-band compaction operation leaves a follower on its valid older physical layout and requires no BTDB
  replay skip support. Never distribute local-file deletion: each node alone knows which files remain pinned by its open
  read-only transactions, retained roots, and local recovery cut, and reclaims only its own proven-unused cache files.
  A follower compactor stops before PVL creation or pointer replacement. Any KVI needed solely for safe follower-local
  cleanup is non-canonical cache metadata. Transfer a canonical KVI to a follower only as the initial bootstrap artifact
  when that database is opened or rebuilt; never push later leader-created KVIs to an already running follower. Only the
  current leader may delete remote files as soon as a complete replacement KVI/manifest/tail is published and the
  current recovery closure no longer needs them. Do not delay deletion for follower restore, acknowledgements, grace
  periods, or old-manifest fallback. A follower losing files during startup restarts and reads the newest published KVI.
  Never reuse retired object keys; delayed old deletes must remain harmless. Local reader pins remain independent.
- Treat transaction-aligned durability as a hard invariant. A database-state CAS may select only a cursor immediately
  after a complete BTDB transaction; if one transaction spans multiple TRL files, prepare and verify every segment and
  publish them together through one immutable boundary descriptor and one state CAS.
- Treat every local file as a disposable, untrusted cache. Arbitrary missing, truncated, stale, or mixed-generation local
  content must cause full validation and rebuild or fail-closed unavailability, never inferred canonical state.
- Graceful leader shutdown must not stop application transaction execution. At the next safe transaction boundary,
  irreversibly switch later writes to disposable local `.temptrl` scratch files so every newly written value remains
  readable. Never hard-flush, upload, checkpoint, replay, or promote that generation as canonical. Delete it on clean exit
  and unconditionally delete leftovers before constructing the canonical file collection on restart; cleanup failure
  keeps the node unavailable. Permit only the Azure lease authority operations needed to preserve or transfer
  fencing. Cancel compaction cooperatively at its next bounded safe point; never wait for the whole compaction to finish.
