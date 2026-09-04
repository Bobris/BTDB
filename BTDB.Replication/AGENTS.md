# BTDB.Replication Working Agreement

- Write documentation, architectural notes, source code, identifiers, and other project artifacts in English.
- The user may communicate in Czech or Czenglish; respond naturally, but keep the project content in English.
- The library is currently in the architecture-brainstorming phase. Do not add an implementation unless the user explicitly asks for one.
- Use `Architecture.md` as the shared living document for protocol brainstorming, alternatives, open questions, and
  decisions.
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
- Support rolling application upgrades through a monotonic application generation and an immutable database catalog
  selected by the leader record. A prepared newer-generation follower has handoff priority. Databases added by that
  generation run provisionally and locally until the selected target seeds their canonical genesis; other followers of
  that generation must discard their provisional copies and reopen from that exact leader-selected seed. A database
  removed by the selected catalog is frozen durably, and an older follower that still hosts it may continue only in a
  disposable local `.temptrl` generation. Once a newer catalog is selected, older generations can follow compatible
  databases but can never lead the cluster again.
- Never gate follower application commits on leader progress or external I/O. Followers synchronously append an
  unrestricted speculative suffix to local cache, retain a pinned leader-confirmed root, advance confirmation with no
  BTree work for exact transaction matches, and restore/replay/re-execute the suffix from that root on the first mismatch.
- Keep canonical TRL free of replication-specific transaction kinds. Every canonical BTDB transaction is the ordinary
  result of consuming one application event. Leak detection asks an injected parent-system port to publish a bounded
  exact-key removal event; no replica erases anything until that event returns through the normal ordered application
  stream, where the ordinary application handler applies it idempotently. Never add `LeakRemoval` or another maintenance
  command to BTDB TRL for replication.
- Run full compaction only on the leader. Distribute sealed PVL artifacts and bounded physical pointer-rewrite operations
  out of band over the existing leader-to-follower session, never through TRL and never as canonical transaction sequence.
  Missing an out-of-band compaction operation leaves a follower on its valid older physical layout and requires no BTDB
  replay skip support. Never distribute local-file deletion: each node alone knows which files remain pinned by its open
  read-only transactions, retained roots, and local recovery cut, and reclaims only its own proven-unused cache files.
  A follower compactor stops before PVL creation or pointer replacement. Any KVI needed solely for safe follower-local
  cleanup is non-canonical cache metadata. Transfer a canonical KVI to a follower only as the initial bootstrap artifact
  when that database is opened or rebuilt; never push later leader-created KVIs to an already running follower. Only the
  current leader may delete files from object storage, and only after they are permanently unreachable from every
  retained recovery root so a delayed old-term delete remains harmless.
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
