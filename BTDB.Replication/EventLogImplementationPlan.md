# BTDB.Replication Event Log — implementation proposal

Status: implemented as a preview on 2026-09-29 (E0 harness and E1–E5, section 11) in `BTDB.Replication/EventLog/`, `BTDB.Replication.Azure/AzureEventLogStorage.cs` and `BTDB.Replication.Http/`. Application integration (E6), migration and multi-VM qualification (E7) remain open. The live Azure GZRS storage screening is complete (section 16). It selects bounded single-request Put Blob splits of up to 256 KiB, merged into larger objects in the background (sections 5 and 6). The 10 ms goal is aspirational, not an acceptance threshold.

Confirmed requirements:

* Measure real Azure GZRS storage performance first.
* Minimize both steady-state and owner-change latency.
* Support 1–4 nodes.
* Publish exactly once for each single Publish API invocation, with no caller-facing deduplication contract.
* Records are opaque bytes. An application keeps its serializer metadata in a topic of its own.

Segment and split mean the same storage unit in this document.

## 1. Goal and scope

Add an embedded, Kafka-like ordered log of opaque records to the BTDB.Replication package family. Nodes communicate over HTTP; Azure Block Blob Storage is the durable source of truth. Every topic is a chain of Block Blobs. Multiple records share one storage commit but keep individual offsets.

A record is a `ReadOnlyMemory<byte>` payload and nothing else: no key, header, type, timestamp or schema, and the log never interprets it. Topics are generic named streams. The log assigns them no roles and does not limit how many an application uses; for example, an application using `BTDB.EventStore2Layer` keeps event data and serializer metadata in separate topics, exactly as it would with Kafka. Topics are independent. Each has its own offsets, owner and scheduling, and there is no cross-topic ordering or atomicity.

The API resembles a Kafka single-partition producer and an explicitly assigned consumer: durable publication returning the exact offset, bounded replay and live following. Out of scope for v1:

* Kafka wire compatibility.
* Partitions within a topic and consumer-group balancing.
* Cross-topic transactions.
* Record keys and headers.
* Kafka administration compatibility.

All nodes independently consume every topic they need. This is broadcast replay, not distributing records among workers.

The existing replication agreement says the application supplies a durable event log. This feature supplies that capability as an optional sibling component. It does not make ordinary BTDB transaction Commit await Blob Storage, and it does not move application execution, retry or skip policy into database replication. During implementation, update the working agreement and owning design documents to describe this explicit extension.

## 2. Source findings

Inspected: BTDB `bca71ce41734abcd0c090423e5d57a4ea756abb2`, plus an existing Kafka-based application that serializes events with `BTDB.EventStore2Layer`. That application is not described here beyond the requirements it implies.

| Source | Observed behavior | Consequence |
| --- | --- | --- |
| Existing Kafka producer adapter | One `ProduceAsync(byte[])` call per record on a single-partition topic returns its offset. The producer is idempotent, waits for acknowledgement from all replicas and uses a 10 ms linger. One publishing thread issues calls in queue order without awaiting each one. | Publish one opaque record per call and return its exact offset. Preserve the order of publications a process starts on one topic. Do not inherit the linger. |
| Existing Kafka producer edge case | A successful delivery report without an offset is completed with a substituted, previously observed offset after a delay. | A receipt always carries the exact committed offset and is never substituted. |
| Existing Kafka consumer adapter | An assignment has a topic, an inclusive start and an optional inclusive end. The virtual "current last" offset means the last existing record as an end and the next new record as a start. Behavior for missing start/end offsets is configurable, and consecutive offsets are validated. | Offer next-offset reads with an optional exclusive end, plus durable bounds. Adapters map inclusive and virtual offsets. A read below the first retained offset fails explicitly. |
| Event data and serializer metadata | Two topics. A producer whose serialization needs new metadata publishes it, waits until its own node has applied the metadata topic through that offset, serializes again and then publishes the event. A reader that meets an unknown type waits for more metadata. The whole metadata topic is replayed at startup. | Keep the split; the log stays payload-agnostic. A record must be readable by followers once its receipt completes. The metadata topic needs full retention. |
| `BTDB/EventStore2Layer/EventSerializer.cs` (`ProcessMetadataLog`) | A metadata log carries provisional negative type IDs. Applying it assigns final IDs in apply order and deduplicates equal descriptors. | Nodes that apply the same ordered metadata topic derive the same IDs. Concurrent producers therefore need no codec namespaces or split-local metadata in the log. |
| Event IDs | Event ID = topic offset + fixed shift. | Log offsets start at 0 and are contiguous. The shift stays in the application. |
| Coordination topics | Short-lived node/version messages. Subscribers start at the next new record. Kafka retention is 7 days or 1 GiB. | Use separate topics and start-at-end subscriptions through bounds. Independent retention can come later. |
| `BTDB/EventStoreLayer/AppendingEventStore.cs` | Stores multiple records together with block framing, checksum, compression and rotation. | Reuse the framing and checksum concepts, not its synchronous sector-overwrite file API as a remote commit protocol. |
| `BTDB.Replication/CanonicalTrlPublisher.cs` and `ObjectStorages.md` | Conditional canonical append, explicit ambiguous result, adoption and reconciliation before advancing. | Reuse the proven patterns; event input durability is separate from native TRL publication. |
| `BTDB.Replication.Azure/AzureReplicationStorage.cs` | Stages blocks, then commits a block list with ETag conditions; caches the committed block list. | That two-request append measured slower than one bounded Put Blob (section 16). |
| `BTDB.Replication/Measurements.md` | Strong throughput/restore results and loopback HTTP measurements, but no durable publish-to-all-consumers percentile. | Run a dedicated benchmark before claiming the target. |

## 3. Contracts and API

1. A successful publish receipt identifies a record already committed in canonical Blob history. Staging blocks, writing a local file, forwarding to the topic owner or receiving peer acknowledgements is insufficient.
2. Readers observe only complete durable commits. No speculative delivery in v1.
3. A committed prefix is never replaced, reordered or truncated. Background merging (section 6) adds immutable merged objects and later deletes the objects they cover, but never changes a record's bytes, offset or order. Losing every node and its disk does not lose acknowledged records.
4. Every record has a stable offset. Commit boundaries, rotations, merges and owner changes do not consume offsets.
5. A storage commit is atomic for visibility and receipts. It is not an atomic transaction over the application handlers for all its records.
6. Replay is at least once across consumer crashes. The application persists its applied event ID with its database transaction. This does not provide exactly-once external side effects.
7. Exactly-once publication is mandatory for each single API invocation: internal transport and storage retries must not append its record twice. Repeated independent invocations are separate publications, even with identical payloads.
8. Publications that one process starts on one topic, each after the previous `PublishAsync` call has returned its task, commit in that call order. Publications started concurrently by different threads or processes have no mutual order.
9. Cancellation after dispatch and lost responses can mean an unknown outcome. They must never be reported as proof that the record was not stored.
10. Slow or disconnected consumers do not prevent durability acknowledgements or delay other consumers. They catch up from durable storage.
11. All external boundaries are injected: storage, transport, clock/scheduler and identity generation. The full protocol must run in deterministic simulations.

Proposed API, to finalize with contract tests:

```csharp
public interface IEventLog
{
    IEventTopic GetTopic(string name);
}

public interface IEventTopic
{
    ValueTask<ulong> PublishAsync(ReadOnlyMemory<byte> record, CancellationToken cancellation = default);

    ValueTask<EventLogBounds> GetBoundsAsync(CancellationToken cancellation = default);

    IAsyncEnumerable<EventLogRecord> ReadAsync(
        ulong start, ulong? end = null, CancellationToken cancellation = default);
}

public readonly record struct EventLogBounds(ulong First, ulong Next);

public readonly record struct EventLogRecord(ulong Offset, ReadOnlyMemory<byte> Payload);
```

`PublishAsync` returns the record's offset once it is durable. The record is copied before the call returns, so the caller may reuse its buffer at once. A record above the configured maximum size is rejected before acceptance. There is no batch overload: concurrent calls are coalesced internally (section 7). Add one only if measurements show that per-call overhead matters.

`ReadAsync` yields records with consecutive offsets from `start`. `end` is exclusive, and `end: null` follows live records until cancellation. A `start` below `First`, or above the durable next offset at call time, fails explicitly; the log never silently skips data. A yielded payload stays valid until the enumerator advances; copy it to keep it.

`GetBoundsAsync` returns the first retained offset and the durable next offset, including for an empty topic. The returned next offset must cover every receipt completed before the call started. It must not come solely from the memory of an owner that may already be fenced without knowing it (section 8).

The existing application uses the current end in only two ways. The first is as the end of a startup replay: of the data topic before live consumption, and of the whole metadata topic before its subscription starts. The second is as the start of subscriptions that want only new records, such as coordination messages. A stale end would never lose data, because following continues from it. It would, however, let a node declare its startup replay complete before it has seen records already acknowledged to other nodes, which is why the end must be fresh. Readers waiting for serializer metadata do not query bounds; they wait for their live metadata subscription.

Adapters map other offset conventions. A Kafka inclusive end `e` becomes `end: e + 1`. "Current last" becomes `bounds.Next`, both as an end for bounded replay and as a start for new-records-only subscriptions. Replay to a captured end followed by live consumption is `ReadAsync(start, bounds.Next)` followed by `ReadAsync(bounds.Next)`, or a single `ReadAsync(start)`.

Any valid name addresses a topic; a topic without splits is empty, and its first publish creates the first split. There is no topic administration API. Per-topic cost (owner session, subscriptions, background merging) grows linearly with the number of active topics. No public option permits weaker durability. Publish completion and application replay completion remain separate signals.

## 4. Per-topic ownership through the active blob ETag

Each topic has its own writer owner, fenced by conditional operations on its actual writable tail. Do not add a cluster-wide event-log leader or a separate event-log `leader.json` lease. Database replication keeps its existing authority protocol; the event log has a narrower job: publish one immutable ordered history through storage CAS.

The active blob's metadata records the current owner session and its transport endpoint. These fields are needed for routing and for distinguishing a restarted writer at the same endpoint; they are not a separate authority object. Every append and every metadata update uses the exact ETag from the current session's preceding successful operation. ETags are opaque version tokens, not ordered terms.

To take over a topic, read and validate its writable tail and prepare a fresh owner session. Prefer combining owner selection and the first batch in the same conditional append: preserve the exact existing prefix, append the batch and set owner metadata atomically with If-Match. The successful write is both takeover and durable publication; it changes the ETag and fences predecessor requests. If no batch is ready, use a content-preserving conditional owner-metadata update. Neither path waits for lease expiry.

Crucial rules:

* A current owner that receives a conflicting write response stops that session. It must not silently fetch the new ETag and retry its write under somebody else's authority. The one exception is reconciling its own ambiguous write: a 412 on a retry can mean that the session's earlier attempt landed. The owner reads the tail once. If the current version is exactly its own intended write, including its own owner session, it adopts that version and continues. Otherwise it resolves which of its intended frames are present in canonical history and stops.
* An explicit takeover contender may rediscover and contend again with bounded backoff. A timeout or missing heartbeat triggers contention; it is not proof that the old owner stopped. CAS, not failure-detection timing, establishes safety. Under an asymmetric partition in which nodes cannot reach each other but all reach storage, ownership can alternate on every commit. That stays safe but costs a tail reread per takeover, so bound it with backoff and measure it.
* A fenced owner learns about the takeover only when its next conditional write fails. An idle deposed owner, and every subscriber still following it, must discover the change without waiting for a new write. Section 8 defines this.
* If a predecessor append wins the race, preserve it and retry takeover from the new canonical version. If takeover wins, delayed predecessor requests using its old version fail.
* A lost takeover response is reconciled by reading the selected owner session. Do not blindly repeat ownership selection under a fresh session identity.
* Put Blob and Put Block List can replace blob metadata, so every such write must explicitly preserve the current ownership.
* All staged blocks use attempt-unique block IDs. A stale writer must not be able to change an uncommitted block selected by another writer's commit.
* HTTP session checks reject obsolete owners, but only data-blob conditional writes provide storage fencing. This protocol is intended for the log's durable append path; it is not a replacement proof for all existing database-replication authority behavior.

Any node can publish by forwarding to the current topic owner, and topics may have different owners. Topic ownership and serving must be available before database restoration and application startup barriers complete, avoiding a dependency cycle. Opaque records can be served without loading application types or applying them locally.

Keep a candidate's current committed tail, ETag and recent bytes warm to minimize takeover work. A cached ETag can become stale; handle that as an ordinary failed CAS, not as permission to discard new history. Following requires no vote or consumer quorum. Heartbeats, connection loss and backoff determine liveness and contention; measure their intervals separately from the storage takeover time. The per-topic CAS protocol still needs deterministic rotation and lost-response tests before it is considered implemented or proved.

## 5. Segment layout and canonical append

Namespace example:

```text
eventlog/topics/{topic}/00000000000000000001.elog      level-0 split, named by split ID
eventlog/topics/{topic}/00000000000000000002.elog
eventlog/topics/{topic}/l1/00000000000000000000.elog   merged object, named by first record offset
eventlog/topics/{topic}/l2/00000000000000000000.elog
```

Topic prefixes are separate from database replication cleanup prefixes. Segment IDs come from canonical topic history, never local file inventory or wall-clock time.

Each segment has a versioned header with topic identity, segment number and first record offset. A commit frame contains:

* a bounded length and the record count;
* the publisher session and consecutive sequence numbers of its records, as runs (section 7);
* each record's length and payload;
* a checksum.

Offsets inside the frame derive from its first offset and record index. A terminal seal names the successor segment. Ordinary commits need neither a manifest nor a second `state.json` write.

The storage candidate selected by the GZRS screening (section 16) is a bounded active segment, rewritten with one ETag-conditional `Put Blob` per commit. It is still a Block Blob and logically append-only, because every replacement preserves the exact committed prefix. It needs one dependent storage request per commit, at the cost of retransmitting the prefix and rotating often.

Start with an active-split cap of 256 KiB and tune it in E0 with real record sizes. The 64 KiB cap measured p50 8.71 ms / p99 19.29 ms, 256 KiB measured 9.45/26.98 ms, and 1 MiB measured 22.45/55.44 ms. Records larger than the cap are handled as described below; measure them separately.

Rotation is size-based and frequent. At 1 MiB/s of records, 256 KiB splits rotate four times per second, about 345,000 objects per day. That makes the rotation protocol part of the steady-state latency path. Background merging (section 6) bounds the number of live objects.

A seal is the final frame of a split. It declares the split complete and names the successor split and its first offset. It is always written by a conditional write to the split itself. That write changes the split's ETag, which fences every later write to it: ETag fencing works only within one blob, so creating the successor cannot fence the predecessor. Without a seal, a contender could read `N` and see no `N+1`, and the owner could then create and fill `N+1`. The contender's CAS on the unchanged `N` would still succeed, forking the history. Readers that reach a seal move to the successor instead of waiting for more data in the split.

A segment rotation must be a specified protocol, not two unrelated object writes:

* **A batch never crosses a split boundary.** One storage commit writes one prepared batch into one split, and all its receipts complete together.
* **Proactive seal.** When the free space left after a batch drops below the seal threshold, the same write also carries the seal. Start with a threshold of 25% of the cap and tune it in E0. The next batch then goes directly into the successor create, so the rotation costs no extra request.
* **Seal-only write.** If a batch does not fit into an unsealed split, the owner seals the split with a content-preserving conditional write and puts the whole batch into the successor. Proactive sealing makes this rare.
* **Waiting for the seal.** The successor create must wait until the seal is committed; reconcile an ambiguous seal first. A sealed split without an existing successor is a valid idle tail; the writable tail is then the successor that does not exist yet.
* **Who may create the successor with content.** The successor inherits the owner session selected by the committed predecessor seal. Only that owner may create it with content, using a create-if-absent (`If-None-Match: *`) that carries the canonical header, the first batch and the inherited owner metadata.
* **Everyone else.** Any other node, whether a helper or a contender, may create only the identical header-only successor after observing the seal. Creating it gives no writer authority; a contender then CAS-selects its own owner on the successor.
* **Races.** Create-if-absent decides every race. If the owner's create loses to a header-only create, it rereads the successor. If the successor still holds the inherited owner session, the owner appends its batch with If-Match on the returned ETag. If a contender already took ownership, the owner stops and its pending transfers are reconciled as in section 7. A writer rereading after an ambiguous create must verify its own content or owner session before continuing.
* **Recovery.** Recovery follows the committed seal. A missing successor can be recreated header-only; an existing successor whose header differs from the canonical header is an error.

Exact storage operations around a proactive seal of split `N`. The owner holds `ETag(N)`, batch `A` brings `N` above the threshold, and `B` and `C` are the next batches:

| Step | Request | Condition | Durable after success | Waits for |
| --- | --- | --- | --- | --- |
| 1 | Put Blob `N` = committed prefix + `A` + seal naming `N+1`, owner metadata preserved | `If-Match: ETag(N)` | `A`; `N` is sealed | the previous commit |
| 2 | Put Blob `N+1` = header + `B`, inherited owner metadata | `If-None-Match: *` | `B`; returns `ETag(N+1)` | step 1 |
| 3 | Put Blob `N+1` = header + `B` + `C` | `If-Match: ETag(N+1)` | `C` | step 2 |

Every step carries records, so each batch is durable after one request, as with an ordinary append. Without the proactive seal, a batch `B` that does not fit costs one additional seal-only request on `N` before step 2.

A record whose frame does not fit an empty split within the cap is written as its own split, sealed immediately:

1. If the current split is not sealed yet, the owner seals it with a seal-only write. If the current split holds no records, the large record is written into it together with the seal instead, so a split is never sealed empty.
2. The owner creates the successor in one write, following the rules above. The write contains the header, the single large record and a seal naming the next split. A single conditional Put Blob handles records up to the configured single-request size. Larger records use attempt-unique staged blocks and a Put Block List conditional on `If-None-Match: *`.
3. The receipt completes after that write. Records published after the large one go to the next split, which the owner creates together with their batch.

The large split must not be created before the predecessor's seal is committed. Otherwise, a fenced owner could leave content in a split that a contender later names as its successor. A large-record split is an ordinary sealed split, including for background merging. The configured maximum record size bounds it; larger records are rejected before acceptance.

Takeover follows section 4 on the actual writable tail. If the predecessor wins an append/seal race, rediscover, preserve that committed history and adopt the resulting tail. If adoption wins, delayed writes using the old version fail. A new owner must not start assigning offsets from an earlier LIST result alone. Test rotation, missing-successor and delayed-create schedules explicitly before treating this mechanism as proved.

There is one authority transition, on the actual writable tail, not an owner election on one blob followed by data fencing on another. Never adopt a sealed predecessor as if it were writable; follow its committed successor. If sealing races adoption, the ETag decides which happened first. After an adoption conflict, no old session may issue a fresh successor write. A known successful seal remains part of canonical history, and takeover follows it even if successor creation was interrupted.

Storage results are Applied, Rejected or Ambiguous. Disable transparent retries on conditional mutations and reconcile the exact intended bytes and request identities. An old read after a timeout does not prove that a still-in-flight write will never land. Do not issue the next commit until the unresolved mutation has been resolved or fenced by a changed version.

### Measured alternatives

**Stage + commit.** A growing Block Blob with `Put Block` followed by an ETag-conditional `Put Block List`; only the second request commits. Measured results:

* p50 14.93 ms / p99 28.43 ms rotating at 64 KiB;
* 15.20/29.59 ms rewriting only a bounded tail block;
* 49.29/71.91 ms with about 4,100–6,100 committed blocks.

Keep it only as the append fallback. Do not ship both production append modes without a demonstrated need. Background merging uses staged blocks anyway, because it writes large objects off the latency path.

**One immutable blob per record or commit.** About 8.00/18.72 ms for 1 KiB, but that lower bound omits ownership and chain publication. It would also create excessive object counts and complicate fencing and retention, so do not use it as the default.

## 6. Background merging of sealed splits

Small splits keep append latency low but create many objects (section 5). A background process merges sealed splits into larger objects on two levels with a fan-out of 16. Merging never changes a record's bytes, offset or order, and it never runs on the append path.

* **Who merges.** The topic owner runs merging as low-priority background work. It has its own cancellation, separate from publication, and losing ownership cancels it. Appends keep priority for storage connections and bandwidth.
* **Fixed groups.** Groups are aligned to split IDs:
  * A level-1 group covers splits `16k+1 .. 16k+16`.
  * A level-2 group covers splits `256m+1 .. 256m+256`.

  A group is eligible once a later split exists, so neither the tail nor the latest split, where tail discovery starts, is ever merged. A level-2 merge may read the group from level-1 objects or from original splits, whichever still exist. Large-record splits take part like any other split. If a group's total size exceeds the configured maximum object size, it is not merged at that level.
* **Naming.** A merged object is a new, immutable blob named by its level and its first record offset, zero-padded to 20 digits: `l1/{firstOffset}.elog` or `l2/{firstOffset}.elog`. The application's event ID is that offset plus its shift. Splits are never sealed empty (section 5), so first offsets are unique within a level. Because the names are zero-padded, LIST returns merged objects in offset order.
* **How.** Create the object with create-if-absent (`If-None-Match: *`). Large content uses attempt-unique staged blocks and a Put Block List with the same condition. The object contains, in order:
  1. the merged header, including the covered split ID range;
  2. the frames of the group byte-for-byte, with records, checksums and transfer identities unchanged;
  3. the offset that follows the group.

  Record the whole-object SHA-256 and verify the committed length and hash before anything is marked for deletion. Existing splits are never rewritten.
* **Deterministic content.** The content is a pure function of the canonical records of the group, so every merger produces identical bytes under the same name. A merger whose create finds an existing object verifies that its content is identical, and then the merge is done. A different existing object is an error.
* **Merged header index.** The header contains the first offset, the record count, a table with the byte position of every record start and a smaller table of frame start positions. Entries are fixed-width, and the header records the entry width. Everything in the index is derived from the frames and covered by the object's SHA-256.
  * To read from offset `x`: take the record position `index[x - first]` and read from the start of the frame containing it, so frame checksums are still verified.
  * The index adds 4–8 bytes per record. For 1 KiB records that is under 1% of the object.
  * Ordinary level-0 splits have no index; they are at most 256 KiB and are scanned.
* **Canonical order.** The level-0 seal chain still defines canonical history and remains the only mechanism for appending and finding the tail. Merged objects are verified, immutable alternative representations of complete groups of that history.
* **Reading.** A reader resolves the object for its next offset through a cached listing and prefers the highest level that covers it. When it reaches an object's end, it resolves the following offset the same way instead of following successor names. After a miss or a 404, it refreshes the listing. A missing object is never a gap.
* **Garbage rule.** A level-0 split is garbage once a verified level-1 or level-2 object covers it. A level-1 object is garbage once a verified level-2 object covers it. A covering object is deleted only if a higher-level object covers it in turn, so this status is monotonic.
* **Deletion.** Reuse the delayed conditional deletion from `ObjectStorages.md`: mark with a deletion deadline, then use an ETag-conditional delete that rechecks both the deadline and the garbage status. The delay must exceed the longest reader pass over an object.
* **Crash recovery.** A crash at any point leaves at most an incomplete create (uncommitted blocks, which are ignored) or a complete merged object whose covered objects are not yet deleted. The next pass verifies the object by name and continues.
* **Caches.** Merged objects and sealed splits are immutable. Local caches are keyed by name and SHA-256 and never need invalidation.

**Object count.** A topic holds at most 15 unmerged splits, at most 15 level-1 objects, and one level-2 object per 256 splits. With full 256 KiB splits, a level-2 object is about 64 MiB. That is about 1,600 objects for 100 GB, or about 1,350 new objects per day at 1 MiB/s. A third level (`l3/`) with the same fan-out can be added later if object counts require it; the group alignment already allows it.

**Finding offset `x`.** Take the greatest `l2/` name whose first offset is at most `x`. Get its end from the next `l2/` name or from its header. If `x` lies beyond it, repeat with `l1/`, and then with the few remaining level-0 splits. The LIST needs no header reads because the offsets are in the names, and one page (5,000 names) covers about 300 GB of level-2 objects.

The evidence for this mechanism is the object-count cost computed in section 5. Do not add it before the append path works.

## 7. Publication, retries and batching

Normal path:

1. The caller enters Publish; latency measurement starts before queueing.
2. The local publisher lane sends records to the topic owner over a reused HTTP connection, or uses an in-process call when local.
3. The owner accepts the in-flight internal transfer, queues it, assigns tentative contiguous offsets and forms a commit.
4. The durable storage append commits the whole batch.
5. The owner advances its durable end, publishes the batch to every live subscription and resolves all covered receipts. These notifications proceed independently; there is no await-all-consumers barrier.
6. Each node executes records in order and persists the applicable event ID with each successful database transaction.

Publish can return before or after a fast consumer applies the record. Its promise is durability, not application success. A crash after the Blob commit but before the response or fanout is recovered by replay.

Batching policy:

* Begin with zero intentional linger, and drain already queued work into a bounded batch.
* While one storage commit is outstanding, accumulate the next batch and dispatch it as soon as the previous commit resolves.
* Allow one unresolved canonical mutation per topic. Ingress may be concurrent, but commit order is serialized.
* Benchmark an optional 0.25–0.5 ms maximum coalescing window only if it reduces measured storage cost without violating latency. Never apply a separate waiting window at every layer.
* Bound queued bytes, pending requests and subscriber buffers. Report overload before acceptance when possible. Include queue residence in measured end-to-end latency.

A node forwards its publications for one topic over one ordered stream, and the owner commits them in stream order. A node's committed publications on a topic are therefore always a prefix of its call sequence (contract 8). After an owner change, the node resolves its outstanding transfers in order and resubmits only the uncommitted suffix, in the original order.

### Exactly-once completion of a single Publish call

The caller invokes the interface once per publication. There is no application-level duplicate submission contract, public idempotency key, durable producer sequence table, business-command deduplication or historical receipt-lookup API. Two separate calls with identical data are two valid records.

Exactly-once is the responsibility of the internal delivery protocol:

* One accepted invocation creates one immutable internal pending operation. Queueing, reconnecting and storage retry do not create a second operation.
* Before storage dispatch, assign a candidate canonical segment, version and position, and keep the exact prepared bytes. If the response is lost, reconcile that candidate against canonical bytes. Never allocate another offset and append again merely because the first response timed out.
* Forwarding from a non-owner needs a private transfer identity, generated inside the library, because a lost HTTP response cannot distinguish “owner never received the request” from “owner durably committed it.” This is a transport detail, not an interface requirement. Include the transfer identity with its data in the durable frame only to resolve this concrete ambiguity. Identical payload bytes alone cannot identify an invocation.
* Implemented identity: every node's publisher for a topic has a random 64-bit session and numbers its calls with consecutive sequences, and frames store them as runs (session, first sequence, count). Because a session's records commit in call order (contract 8), its committed sequences are always a prefix. A resubmission therefore carries the oldest sequence without a receipt, whether any record was dispatched before, and the durable offset known at its first dispatch. An owner that does not know the session installs itself first and then scans committed frames from that offset.
* A reconnect to the same live owner session joins the same pending operation or returns its completed result. Keep only outstanding transfer state and session-scoped recent completions in memory; do not build a general historical deduplication service.
* On an owner change, first fence or adopt the canonical tail so that a predecessor cannot later land an unseen write. Then resolve the bounded set of outstanding transfers against committed frames, including preceding splits if rotation occurred. If a transfer is found, resume the still-live Publish call with its original result. Only after proving absence behind that fence may the new owner submit it, once.
* Cold recovery can serve previously committed records without producer state. If an outstanding call survives on another node and no exact candidate position was received, scan the relevant committed history for its private transfer identity. Correctness must not depend on a volatile old-owner table. Initial retention is disabled, so this evidence is available; future cleanup must preserve unresolved-transfer evidence.
* A storage commit can group many invocations, and each receives its own offset. Commit retries keep the same prepared membership and bytes until resolved.

A publisher process crash can destroy its in-memory awaiting task, so the library cannot deliver a response to that vanished invocation. Any committed record remains present exactly once and is replayable. Reissuing a new call after a crash is outside the single-call contract and is not automatically deduplicated. Do not introduce a caller outbox or durable business-command ID requirement for this feature.

## 8. HTTP and replay

Add a separate versioned event-log endpoint to `BTDB.Replication.Http`. Use binary bounded frames and persistent HTTP/2 response streams, one independently flow-controlled subscription per topic and node. Reuse the existing authentication and session-binding practices, but do not carry live records on the periodic database comparison poll.

Operations: publish, durable bounds, bounded read and live subscription. The exact URL names are an implementation detail. Every frame binds topic and owner session. Validate lengths, offsets and protocol versions before allocating large buffers, and reject stale-session live messages after switching owners.

Flush each committed batch promptly and disable intermediary response buffering for this endpoint. Test the actual ingress/proxy/TLS path, connection reuse and idle behavior. A transport fallback can use notification-driven long polling, but benchmark it separately rather than inheriting a fixed poll interval.

Followers normally never read Blob Storage. They receive every new committed batch from the topic owner over HTTP, and they keep their takeover candidate state warm from those frames. Blob reads by a follower are reserved for:

* cold startup replay;
* catch-up beyond the owner's recent-byte cache;
* owner discovery when the owner is unreachable.

Keep a bounded recent committed-byte cache on the topic owner, so that a follower reconnecting after a short interruption catches up over HTTP. A subscriber that falls behind that cache catches up from version-bound Blob reads and reconnects from its next offset; it cannot retain an unbounded owner queue. Coordinate subscription registration with reading the durable end, so that no notification is lost between catch-up and follow. Duplicate frames are harmless when validated against the expected offset. Gaps trigger catch-up, not skipping.

A fenced owner that has nothing to write never gets a 412, so it can keep serving live subscriptions and bounds while another node commits. Only the owner checks storage for that:

* Every live batch frame carries the tail blob version it produced.
* When its topic has been idle for a bounded interval, the owner checks its own tail with Get Blob Properties, one request per topic per interval. It then sends a heartbeat frame over every subscription carrying the tail version it has just validated.
* A changed version or owner session means that the owner is fenced. It stops, and it closes its subscriptions with a redirect to the owner recorded in the tail metadata.
* A follower that receives neither a batch nor a validated heartbeat within the bounded interval treats the owner as unreachable. Only then does it read the tail from Blob, to find the current owner and any committed suffix it missed.

`GetBoundsAsync` on a follower asks the owner over HTTP. The owner answers only after a successful conditional write or tail check that started after the request arrived, which gives the freshness required in section 3. A follower falls back to reading the tail from Blob only when the owner is unreachable.

Measure the heartbeat interval as part of takeover latency. A best-effort takeover notification from the new owner to the previous owner's endpoint may shorten it, but correctness must not depend on it.

For bounded replay, capture durable end H once, replay [next, H) and then continue at H. Merged-object names and header indexes (section 6) and any local sparse indexes are derived from Blob frames and can be rebuilt from them. They accelerate exact record lookup and startup but cannot define canonical history. Physical Blob offsets never escape as record offsets.

Report applied progress only after the application handler and transaction complete. Dequeueing or reading is not replay completion. Keep these progress reports outside the producer acknowledgement path.

## 9. Recovery, retention and application integration

After loss of every process, discover canonical segments, validate headers, seals and frames, find the committed tail and rebuild offsets. Ignore uncommitted Azure blocks. Corrupt committed data is an error; do not reinterpret it as an uncommitted suffix and silently truncate it. To find the tail, start at the first level-0 split after the newest merged group, whose ID follows from that object's covered split range, and follow seals. Merging keeps LIST small (section 6). Cold replay of many small unmerged splits needs bounded parallel reads; include that replay rate in the startup measurements.

Local storage is a disposable cache. Cached sealed segments require content validation before trusted reuse, and growing tails are reread. No local fsync is required to acknowledge publication, because Blob Storage is authoritative.

Application integration stays in the application, on top of the public API:

* **Topic choice.** The application decides which topics it needs, for example event data, serializer metadata and coordination messages.
* **Metadata before data.** A producer that needs new metadata publishes it and awaits the receipt, applies the metadata topic through that offset, and only then serializes and publishes the data record.
* **Reader dependency.** A reader that meets a data record needing newer metadata waits until its live metadata subscription has applied more records. The producer awaited the metadata receipt before publishing the data record, so the required metadata is already durable and will arrive. Section 8 bounds how long a subscription can stay on a stale owner.
* **Event IDs.** Event IDs remain offset plus the application's shift. The application continues using `StartWritingTransaction(eventId)` and native BTDB virtual batching where useful. A storage batch of 100 records does not become one database transaction or one CommitUlong.
* **Application semantics.** Failed handlers, deterministic rollback, nested events, application timeouts and external side effects keep their application-owned semantics.

Keep all splits by default in the first release; merging reduces object count without deleting records. The metadata topic always needs full retention, because any data record can depend on any earlier metadata record.

Add explicit retention for data topics only after proving that every retained database recovery base can replay its required suffix. A maximum applied cursor from one node is not sufficient. The retention contract must cover database sets, backups, offline nodes and added databases. Coordination topics are the natural first candidates for independent retention, but unresolved internal transfers must still be reconcilable.

## 10. Latency target and qualification

10 ms from publish invocation to replay on all nodes is an aspirational steady-state goal, not an acceptance threshold. The confirmed topology is 1–4 nodes, and no mandatory latency percentile or deadline is currently specified.

Measure the per-record maximum completion time across a fixed set of healthy ready nodes and report its distribution. Optimize steady-state latency and takeover interruption while preserving durable acknowledgement and correctness. The measured storage p99 above 10 ms does not block the selected per-topic ETag design.

Define the node set at run start. A disconnected or slow node is visible as a failed or unfinished sample, not silently removed to improve percentiles. Report failures and recovery separately; an unavailable node cannot meet a finite all-node deadline. Bound record sizes, offered load and application handler cost. If a handler itself takes more than 10 ms, the whole path cannot meet 10 ms.

Illustrative engineering budget, not measured evidence:

| Stage | Budget |
| --- | ---: |
| Publisher ingress and owner queue | 1.0 ms |
| Durable Blob append, including queue behind previous commit | 5.0 ms |
| Fanout and consumer scheduling | 1.0 ms |
| Application handler and local transaction | 2.0 ms |
| Margin | 1.0 ms |

The storage row is not supported by the measurements. On the measured Standard_GZRS configuration, the fastest single-request commit alone measured p50 8.71 ms and p99 19.29 ms (section 16), so even the median path exceeds this budget. The table only shows how the aspirational goal would have to be split.

Do not sum independent stage p99s as proof of an end-to-end p99; measure the full path. Also record invocation-to-durable-ack and durable-to-apply separately to locate costs.

Microsoft documents lower and more consistent latency for Premium Block Blobs, but that is not an end-to-end <=10 ms guarantee. The required qualification target is Standard_GZRS in the same region as the benchmark compute, with real authentication, HTTPS and the intended network path. Premium BlockBlobStorage does not provide GZRS and is not a substitute for the required durability configuration. Region-local durability does not mean synchronous survival of a whole-region loss under asynchronous geo-replication.

E0 end-to-end measurement matrix. The storage-primitive screening in section 16 is complete; it does not replace these rows:

* 1, 2, 3 and 4 processes across actual VMs, with owner-local and forwarded publications where applicable.
* 256 B, 1 KiB, 4 KiB and 16 KiB records, plus representative real records and separately reported large records.
* 100, 1,000 and 10,000 records/s initially; sweep to saturation and add the actual required load. Include an idle-to-one-record path and bursts.
* Bounded Put Blob splits on Standard_GZRS with the complete seal/successor rotation protocol, the chosen cap, concurrent topics and background merging running at the same time.
* A small deterministic handler first, then actual application replay with deserialization, database writes and reads.
* p50/p95/p99/p99.9/max, errors, offered/accepted/applied rates, allocations, GC, bytes retransmitted, commit rate and storage operations per record.
* Open-loop offered load, to expose queueing and avoid coordinated omission. Warm connections before steady-state runs and report cold start separately.
* One monotonic benchmark clock, with application-completion acknowledgements sent back to the driver as a conservative latency bound. If reporting exact distributed completion timestamps, also quantify clock synchronization error.
* Repeated sustained runs, including rotations, merges and GC. Averages, loopback HTTP and Azurite cannot qualify Azure tail latency.

Optimization rule: select the measured lower-latency Block Blob strategy within the required GZRS configuration and report the achieved end-to-end distributions for each workload. Exceeding the aspirational 10 ms goal is not a rejection criterion. Do not improve reported latency by acknowledging before durability, replaying uncommitted data, excluding queues or measuring only delivery.

## 11. Implementation milestones

Milestones use an E prefix so that they do not collide with the database-replication milestones M0–M7 in `ImplementationPlan.md`.

| Milestone | Work and likely files | Completion evidence |
| --- | --- | --- |
| E0: latency feasibility | `DBBenchmark/EventLog/`: `eventlog-storage` reruns the storage-primitive screening (section 16), `eventlog-e2e` measures open-loop publish-to-all-nodes latency over loopback HTTP. Implemented; multi-VM runs on the target workload remain. | End-to-end distributions and confirmed storage strategy, with no unsupported 10 ms claim |
| E1: API and model | `BTDB.Replication/EventLog/`: contracts, offsets, framing, deterministic storage and transport fixtures | Model tests establish the immutable prefix, exact offsets, per-process order, batching and independent consumers |
| E2: durable topic engine | Publisher lane, segment discovery/rotation/adoption, internal transfer reconciliation and recovery | The fault schedules below pass; a restart with all local state deleted recovers every acknowledged record |
| E3: Azure adapter | `BTDB.Replication.Azure/AzureEventLogStorage.cs`: conditional operations and reconciliation | Azurite contract suite plus live provider tests, including lost-response and stale-writer cases |
| E4: HTTP hosting | `BTDB.Replication.Http/`: event-log hosting, binary streams, bounded queues, reconnect | Multi-process/TLS tests; no replay/follow gaps; no global stall from a slow subscriber |
| E5: background merge | Merge, garbage detection and delayed deletion | Merge-race and reader tests pass; object count stays bounded under sustained load |
| E6: application integration | Producer/consumer adapters in the consuming application over the public API | Outside this repository; the application's existing event and upgrade tests pass |
| E7: migration and qualification | Import/cutover tooling, operational docs, benchmarks and observability | Event IDs and application state verified across migration; measured latency distributions for the target workload; no mandatory 10 ms gate |

Implementation status on 2026-09-29:

* **E1–E5 implemented** with the tests listed in `Testing.md`:
  * format, owner lane, service and merger with in-memory and fault-injecting storage;
  * the Azure adapter on Azurite, including a lost upload response;
  * HTTP hosting between Kestrel nodes.
* **E4 not yet covered:** TLS and proxy paths are not tested.
* **E6 is outside this repository.**
* **E7 open:** migration tooling (the plan uses the normal publish path), observability counters and multi-VM qualification.

E0 happens first because it can still change the storage strategy. E1 and E2 establish correctness before performance tuning. Avoid refactoring native database replication into a generic log framework merely to share names; reuse small mechanisms when their contracts actually match.

During implementation, update `Architecture.md`, `ObjectStorages.md`, `Testing.md`, `ImplementationPlan.md`, the hosting/core docs and CHANGELOG within their owning scopes. Run targeted suites after each change, then the full BTDB solution after the final integrated changes. This plan alone does not require running unrelated test suites.

## 12. Required failure tests

* Crash before staging, after staging, after durable commit, before the publish response, during fanout, and after local apply but before consumer restart.
* Lost successful response, delayed successful request, cancellation after dispatch, and a retry receiving 412 after its earlier attempt actually committed.
* Suspended old owner; concurrent per-topic takeovers; old writes arriving before and after adoption; conflicting writers fencing themselves instead of refreshing ETags; no acknowledged prefix lost.
* Every interleaving of seal, the owner's create-with-content, a helper's header-only create, first successor append and takeover, including an empty topic and an idle sealed tail without a successor. A batch never spans two splits.
* A large record between small records: ambiguous seal, ambiguous or lost create of the large split, a contender taking over between the two writes, and staged-block commits for records above the single-request size. Offsets and call order are preserved and the record appears exactly once.
* One Publish invocation with duplicated internal HTTP delivery, response loss, owner crash and split rotation. Verify exactly one durable occurrence and the original receipt for the surviving call.
* Two independent Publish calls with identical payloads create two records.
* A caller node survives owner loss and resolves a transfer whose position response was also lost.
* Pipelined publications from one process keep their call order across forwarding, reconnects, owner changes and rotation.
* Subscriber registration racing a commit; reconnect between every pair of frames; a slow consumer exceeding its memory budget. No data gap and no unbounded memory.
* An idle fenced owner with live subscribers while another node commits. The owner and its subscribers discover the takeover within the bounded heartbeat interval, and `GetBoundsAsync` never returns an end older than a completed receipt.
* A healthy steady state with idle and busy topics, in which followers issue no Blob requests.
* An asymmetric partition in which nodes cannot reach each other but reach storage. Ownership may alternate, but no acknowledged record is lost or duplicated.
* Merge cases:
  * a merge racing appends, rotation, a deposed owner's merge and cleanup;
  * readers inside a split while it is merged or deleted;
  * a crash between the merge write, verification and deletion;
  * level-1 and level-2 merges of overlapping groups running concurrently, including by a deposed owner and with identical creates racing;
  * a read starting at every offset of a merged object through its header index, which must return the same records as a scan.

  Every reader sees each record exactly once at its original offset.
* Truncated or corrupt wire frames, incompatible protocol version, stale session, and unauthorized or oversized requests.
* Storage throttling, prolonged ambiguous writes, overload and real cancellation. A receipt never precedes durable publication.
* Topics needed during startup keep working while database restore or application startup barriers wait, and a data backlog does not starve other topics.
* Database rollback or restart within a multi-record storage batch; committed predecessor records remain independently recoverable.
* Reference adapter test: two producers on different nodes concurrently need new serializer metadata, and every node decodes every data record.
* When retention is added: deleting a required record or unresolved-transfer dependency is prevented, and expired cursors fail explicitly.

## 13. Migrating an existing Kafka deployment

Topics stay separate and records are opaque, so migration copies each needed topic's records byte-for-byte, in offset order. Use a bounded cutover rather than unmanaged dual writing:

1. Quiesce publishers, drain Kafka publication and capture the final offset of each topic. Include publishers in other deployments that write into this one in the barrier.
2. Import each retained topic through the normal publish path; measure its rate, and add a bulk path only if it is too slow. The log assigns offsets from 0. If Kafka's first retained offset is nonzero, adjust the application's event ID shift by that amount so that event IDs are unchanged. The metadata topic must be imported completely. Validate counts, per-record hashes and the offset mapping against Kafka.
3. Coordination topics are not imported; they start empty.
4. Prepare database recovery bases at or before the cutover boundary, and ensure every node can replay the required suffix.
5. Switch all producers, consumers and coordination participants together. Verify reads and application cursors, then resume publication.
6. Keep Kafka read-only for a defined validation window. Once the new log accepts new records, simply switching back to Kafka is unsafe; rollback needs an explicit reverse copy of that suffix, or forward recovery.

## 14. External provider references

* [Put Block List](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list): staged blocks become the blob through block-list publication; conditional update support and block limits.
* [Put Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob): single-operation Block Blob creation and replacement for bounded segments.
* [Latency in Blob Storage](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-latency): the Standard/Premium latency distinction, operation-size, network and client effects, and the need for workload-specific measurements.
* [Lease Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob) and [Azure Storage redundancy](https://learn.microsoft.com/en-us/azure/storage/common/storage-redundancy). GZRS acknowledges synchronous primary-region zone replication; secondary-region replication is asynchronous.

These sources support provider behavior and benchmark design, not a measured latency result for this proposed event log.

## 15. Owner-change latency qualification

Minimize interruption during owner changes as well as steady-state latency. Keep a prepared contender with current committed offsets, the tail version and recent tail bytes. Do not rescan the whole log or restore databases before the embedded log can resume publication. Correctly reconcile in-flight writes before admitting a replacement append.

The lease comparison is complete and serves only as a historical baseline; section 4 replaced it with per-topic ETag ownership. Measured baseline:

* Prepared Change Lease + Renew + leader-record selection + adoption + first append: p50 38.45 ms, p99 62.20 ms.
* Release + Acquire: 42.19/67.87 ms.
* Unrenewed lease expiry: 15.001–15.041 s in five trials.

Do not use Break Lease after a single missed heartbeat as an unproved shortcut.

Still to measure for the ETag design:

* Unplanned owner loss end to end: failure detection, contention, an uncached tail read, the first durable record, publisher rerouting and the heartbeat-based stale-owner detection from section 8.
* Lost storage responses and lost forwarding responses around a handoff, both before and after split rotation. Count all acknowledged records and verify that there are no duplicate internal retries.
* One through four nodes. A single node cannot provide service while its only process is down; report restart recovery rather than describing it as peer failover.

The complete rotation and reconciliation protocol still requires proof and deterministic tests (section 12). Minimize latency without losing durable input or admitting two histories.

## 16. Live GZRS storage screening

The screening ran on one Standard_D4s_v5 Ubuntu VM and a dedicated Standard_GZRS Hot StorageV2 account, both in Sweden Central; the storage secondary was Sweden South. It used managed-identity authentication, HTTPS, raw REST with a reused HttpClient, no application retry policy and .NET 10.0.12. The temporary resources were deleted after the evidence was downloaded.

This was a closed-loop measurement of storage primitives and prepared ownership transitions: 39 cases and 73,605 timed samples, all without errors. It is not the open-loop application-to-all-nodes benchmark. It excludes:

* serialization and handlers;
* HTTP fanout and queueing;
* failure detection and cold tail discovery;
* concurrent writer races and lost-response reconciliation;
* the complete rotation protocol.

The full report, CSV and raw evidence archive are not in this repository yet. Add them to `Measurements.md`, or its evidence location, before these numbers are used as qualification evidence.

| Operation | n | p50 ms | p99 ms |
| --- | ---: | ---: | ---: |
| Conditional Put Blob, 1 KiB increments, 64 KiB split (repeat run) | 5000 | 8.71 | 19.29 |
| Conditional Put Blob, 1 KiB increments, 256 KiB split | 2000 | 9.45 | 26.98 |
| Conditional Put Blob, 1 KiB increments, 1 MiB split | 2000 | 22.45 | 55.44 |
| Stage + commit, 1 KiB, rotate at 64 KiB | 2000 | 14.93 | 28.43 |
| Stage + commit, rewrite <=64 KiB tail block | 2000 | 15.20 | 29.59 |
| Stage + commit, about 4,100–6,100 committed blocks | 2000 | 49.29 | 71.91 |
| Fresh immutable 1 KiB blob (lower bound only) | 2000 | 8.00 | 18.72 |
| ETag owner CAS, then first append, 64 KiB cap | 2000 | 17.08 | 33.42 |
| ETag owner CAS, then first append, 256 KiB cap | 2000 | 17.58 | 36.08 |
| Combined owner selection and first append, 64 KiB cap | 2000 | 8.33 | 19.49 |
| Combined owner selection and first append, 256 KiB cap | 2000 | 10.24 | 22.21 |

Consequences:

* Growing full-blob rewrites and long block lists are poor choices.
* A small single-request split is the latency leader. After review, splits of up to 256 KiB were judged acceptable, with background merging (section 6) controlling the object count.
* Even the lower-bound immutable write has a p99 of about 19 ms, so storage alone does not establish the aspirational 10 ms target.
* A rare sample above 600 ms in bounded-tail rewriting was not explained.

### Takeover experiments

**Separate ownership CAS.** Two dependent operations: a conditional owner-metadata update on the current tail, then the first conditional append, with owner metadata preserved.

**Combined takeover.** One conditional Put Blob both preserves the old prefix, appends the first batch and installs the new owner session atomically. It measured about half the latency of the separate path, making it the preferred takeover path. It assumes that a validated current tail is already available.

In both experiments, a write using the pre-takeover ETag was rejected with HTTP 412, and the final content was read back and verified once per cap. Concurrent contenders were not tested.

If an old append wins the race, the combined CAS must fail, and the contender must reconcile the new canonical history before retrying. Pending internal publications are resolved against that history; never append a potentially already committed invocation under a new identity. The 15-second lease floor disappears because ownership uses CAS rather than lease expiry. Failure detection, an uncached tail read, rerouting, contention and split rotation remain separate costs and tests. Do not present the primitive timings as a complete failover guarantee.
