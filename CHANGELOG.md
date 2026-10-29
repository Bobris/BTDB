# Changelog

## [unreleased]

### Added

- `LsmKeyValueDB` WAL uploads adapt to writers that wait for durability. Before an upload, the uploader waits briefly
  for commits that usually follow the previous upload closely, so concurrent writers awaiting
  `WaitForDurabilityAsync` (or using `DurableTransactions`) share one upload instead of splitting into a small and a
  large one. It chooses the wait from when commits arrive, up to the new `LsmKeyValueDBOptions.AdaptiveWalLingerMax`
  (3 ms by default), and keeps it at zero for one writer awaiting each commit and for independent commits, so their
  latency does not grow. The wait ends once pending commits fill a batch. `AdaptiveWalLingerMax = TimeSpan.Zero`
  uploads at once. On Azure, 2–16 concurrent durable writers went from about 8 to 4–4.5 ms at p50 with 1.7–1.9 times
  the throughput, and one writer was unchanged (`DBBenchmark lsm-durable`, BTDB.LSM/Measurements.md section 9).
- Lean 4 proofs (`lake build`):
  - `BTDB.Replication/Proofs/`, the event-log commit protocol:
    - the split chain never forks, committed history only grows, and a successful write lands exactly at its writer's
      next offset;
    - each Publish call commits once, in per-session call order, with exact receipts;
    - bounds from the owner cover every completed receipt;
    - background merging never leaves a split unreadable;
    - lease authority, confirmation grants and the handoff drain stay within the provider's lease for any clocks
      within the drift bound;
    - walking TRL headers back from an advertised end returns the files after the current one in order;
    - a split created where no later split exists reuses no name that merging deleted;
    - a stopped owner with no write in flight has given every accepted record its outcome;
    - a topic whose disposal detached its owner lane holds no lane afterwards, whatever takeovers attach later;
    - one writer awaiting each commit never makes the adaptive commit window of the owner or WAL uploader nonzero;
    - the poison-event attempt journal skips an event that crashes every run reaching it within `Attempts + 1` runs,
      skips nothing before it, and skips an event only after `Attempts` starts of it.
  - `BTDB.LSM/Proofs/`, `LsmKeyValueDB`:
    - a slice the decoder takes is refused exactly when it runs past the data, whatever its length;
    - across writer takeovers and flushes every acknowledged commit stays in every replay, and every replay is a
      prefix of one writer's history;
    - opening loads the manifest with the highest number among those listed, whatever stray objects lie among them,
      and replays the WAL chunks from the next one up to the first missing, no further;
    - a WAL upload takes the oldest pending records, at least one, and as many as fit the batch limit, and the parts
      a record is cut into for its chunks put together are the record, with only the first flagged first and only
      the final one flagged last, so replaying them rebuilds exactly the record;
    - counted entries add up across components, and the compaction merge preserves each key's state and count;
    - manifest fencing gives every writer session a fresh epoch, and a writer's writes started before its lease
      deadline end before another writer can hold the lease;
    - key ranks and counts computed from point deltas and range-tombstone coverage are exact over any stack of
      components;
    - cursors keep their position when another cursor changes a key suffix;
    - seeking in a data block through its restarts finds the first entry at or after the key, and decoding its
      prefix-compressed keys from any restart gives back the stored keys;
    - a merging iterator seeking older components past a range tombstone of a newer one skips no live key;
    - finding the range tombstone of a key returns the fragment containing it and covers exactly the fragments ending
      at or before it;
    - a data block's delta prefix from its restarts is the sum of the deltas of its first entries, and the prefix
      bounds stored for it cover every prefix sum;
    - a table's delta before a key, from the index and one block, a level's, from its tables, and a memtable's, from
      one path of its tree, is the sum of the deltas of its entries below the key;
    - splitting, merging and updating a memtable's tree keep it a search tree with exact subtree sums;
    - cutting range tombstones at the table boundaries of a compaction keeps their coverage, and merging an erased
      range into a memtable's fragments keeps them disjoint and covering exactly the old fragments and the range;
    - a scan from a key prefix up to its successor visits exactly the keys starting with the prefix, and no key lies
      between a key and its successor, the key with a zero byte appended;
    - the sorted keys with a prefix are one run, as finding a key by its index among them assumes, and finding a key
      with a prefix length needs to check the prefix of its nearest neighbours only;
    - variable-length integers, signed ones too, read back as written, and every one the decoder accepts fits in 64
      bits;
    - a table's Bloom filter passes every key added to it;
    - evicting from the block cache ends, and stops over capacity only when every block left is pinned;
    - level size targets are the exact products capped at `long.MaxValue`, never smaller for a deeper level;
    - looking up a key in a level reads a table exactly when its span holds the key, a compaction takes exactly the
      tables overlapping its range, and replacing them keeps the level sorted and disjoint;
    - the table a level compacts next goes round its tables, so each is picked within as many picks as the level has
      tables while it does not change.
  - `BTDB/Proofs/`, relation queries: the key range test accepts exactly the keys whose value satisfies the bounds,
    for every prefix-free, order-preserving key encoding, and the unsigned and signed variable-length integer
    encodings are such encodings; `Find` with a prefix length answers its contract, and a range scan starts at the
    first key passing the lower bound or, descending, at the last key within the upper bound; an ordinal
    `StartsWith` scans exactly the keys of the strings with the prefix, and string keys, null included, are prefix
    free, so an ordinal equality can be part of a key prefix, and string keys sort in code-point order, which is not
    ordinal UTF-16 order; `CountBy` and `AnyBy` with an advanced enumerator
    parameter count exactly the keys in the range from key indices, for every pair of propositions; the branchless
    `PackUnpack` length functions match the modelled encodings, readers take each encoding's length from its first
    byte correctly, and unpacking returns every packed 64-bit value.
  - `BTDB/Proofs/`, the read-only key value database: comparing a stored key, kept as a node prefix and a suffix and
    cut to a maximum length, with a search target gives the comparison of the whole cut key, and the node search
    finds the boundary of the keys below the target, also through branch nodes, and seeking a key index finds the
    child holding it; the creator's branch nodes fit their format and every level is smaller than the one below.
  - `BTDB/Proofs/`, ObjectDB dictionary freeing: it ends and registers each dictionary once, whatever damage makes the
    dictionaries hold;
  - `BTDB/Proofs/`, event metadata: the checks on type ids reject no metadata log the serializer writes, and reject
    an id the log cannot hold before the type table grows to it;
  - `BTDB/Proofs/`, Bon reading: containers that readers accept nest at most as deep as the data is long, even when
    damage makes a container's items run past its pointer;
  - `BTDB/Proofs/`, Bon serializer creation: once no creation of a type runs, the placeholder cached for recursive types
    is gone, also when a creation failed;
  - `BTDB/Proofs/`, saving a `HashSet`: skipping entries whose `Next` is below `-1` skips exactly the removed ones, so
    Bon, the event store and ObjectDB write the number of items they announce;
  - `BTDB/Proofs/`, writing inline ObjectDB objects: refusing an object already being written, writing ends whatever
    the objects hold, refuses a cycle and still writes an object held in two places;
  - `BTDB/Proofs/`, converting loaders and savers: the staging chosen for the type held always holds a value of it;
  - `BTDB/Proofs/`, emitted list key saving: without a try block it is valid under any pushed call arguments;
  - `BTDB/Proofs/`, Bon deserializer creation: with placeholders for registered classes it ends for any types whose
    other types hold no cycle of their own;
  - `BTDB/Proofs/`, Bon member deserializers: each consuming one value keeps every member on its own value;
  - `BTDB/Proofs/`, lazy ObjectDB collections: counting on each call stays right through several instances;
  - `BTDB/Proofs/`, merging re-serialized event descriptors: recording every descriptor met, it ends;
  - `BTDB/Proofs/`, converting savers: converting to the type a descriptor saves, creating a saver ends;
  - `BTDB/Proofs/`, merged event descriptors: registering those nested in them, they can be stored;
  - `BTDB/Proofs/`, LRU caches: no sequence of operations makes them hold more than their maximum capacity;
  - `BTDB/Proofs/`, `RemoveByIdPartial`: once it removes fewer rows than its limit, only rows `OnBeforeRemove` keeps are
    left;
  - `BTDB/Proofs/`, generated IOC dispatchers: each message type a handler handles is registered once, with a method
    the generated code can call;
  - `BTDB/Proofs/`, generated IOC dispatchers: each call uses handlers built for the root container it is called with,
    and the same ones each time;
  - `BTDB/Proofs/`, Bon integers: every `long` reads back as itself, and a value read or loaded into a member is the
    stored integer, within the member's range;
  - `BTDB/Proofs/`, `TreeNodeUtils.FindFirstDifference`: comparing keys given in parts finds the common prefix of the
    joined keys, and `CalcCommonPrefix` finds the common prefix of a node's keys;
  - `BTDB/Proofs/`, `PtrLenList.TryExclude`: once the excluded range starts in a gap, the rest of it removes exactly the
    positions below its end and keeps the ranges sorted and disjoint;
  - `BTDB/Proofs/`, `PtrLenList.TryInclude`: once the included range starts in a gap, merging the ranges it reaches
    adds exactly its positions and keeps the ranges sorted and disjoint;
  - `BTDB/Proofs/`, `PtrLenList.FindFreeSizeAfter`: the position it returns is the first one at or after the start
    from which the requested positions are free, and `TryFindLenAndRemove` removes exactly the positions it returns;
  - `BTDB/Proofs/`, `XxHash64`: updating with any chunks gives the state of hashing all of them at once;
  - `BTDB/Proofs/`, `KeyValueDBExportImporter`: importing an export gives back its key value pairs, commit ulong and
    ulongs;
  - `BTDB/Proofs/`, Fletcher-32 checksums: blocks of 360 words, one more than the textbook bound, never overflow the
    32-bit sums, and the result keeps both sums in 16 bits;
  - `BTDB/Proofs/`, IOC disposal: in every interleaving of two threads creating instances and one disposing their
    container, each instance is disposed exactly once;
  - `BTDB/Proofs/`, IOC registrations: every registration of a type is listed in order, and the default is the last
    one not preserving existing defaults;
  - `BTDB/Proofs/`, `MemoryPositionLessStream`: after any writes and size changes the bytes beyond the size are zero,
    so a stream grown again reads zeros where it was cut off;
  - `BTDB/Proofs/`, `UseBtdbIoc` disposal: an instance the container owns or the service provider tracks is disposed
    exactly once;
  - `BTDB/Proofs/`, `DiskChunkCache.Put`: content it caches fits after at most one new value file;
  - `BTDB/Proofs/`, Snappy decompression: valid data decompresses to at most 22 times its length;
  - `BTDB/Proofs/`, list keys: written as their count and items, they are prefix free when the item encoding is;
  - `BTDB/Proofs/`, keys of several fields: with prefix free, order keeping encodings of all fields but the last, a
    key sorts by its fields in order;
  - `BTDB/Proofs/`, roaring bitmap counts: through page commands without a count, sets and flushes, `Count` stays the
    number of values;
  - `BTDB/Proofs/`, `RoaringBitmap.Source`: for page ascending values it reads exactly those up to the maximum index;
  - `BTDB/Proofs/`, Roaring bitmap containers: the encoding chosen is the shortest, and its length tells its kind;
  - `BTDB/Proofs/`, sorted relation queries with paging: keeping only `skip + take` rows while sorting keeps exactly
    the first rows of the full sort;
  - `BTDB/Proofs/`, relation query paging: folding `Skip` and `Take` calls into one skip and take gives the rows of
    the calls applied in order, for any rows;
  - `BTDB/Proofs/`, string key prefixes: `Constraint.String.StartsWith` with a value ending with a high surrogate
    matches exactly the strings starting with it;
  - `BTDB/Proofs/`, injected `[Dependency]` members: walking a type's members and then its base types' injects each
    name once and every dependency no derived member hides;
  - `BTDB/Proofs/`, generated file names of the source generator: the search for a free name ends, keeps a free name
    unchanged, and every name returned differs from the others ignoring case.
  - `BTDB/Proofs/`, range enumerators: stopping at the key bound lists exactly the range while the caller removes
    listed keys, and the index comparison used before leaves the range.
  - `BTDB/Proofs/`, transaction log replay: a log ended by damage or inside a transaction replays to exactly the
    transactions committed before it, the transactions a later session appends after a transaction a crash left
    unfinished are kept, and splitting the log into files between any two commands changes nothing.
  - `BTDB/Proofs/`, reading a length-prefixed byte array or string: the reader returns a byte array exactly when the
    input holds the declared length and allocates at most four times the larger of the input and 4096 bytes, and a
    string at most six times the larger of the input bytes and 256 characters, whatever the prefix declares.
  - `BTDB/Proofs/`, database files: the Guid more than half of the files carry is the database's in every listing
    order, so a stray file cannot make the database's own files foreign.
  - `BTDB/Proofs/`, preserved history: writing a key index keeps the one preserved history opens from.
  - `BTDB/Proofs/`, key index durability: a durable key index never points past the durable transaction log.
  - `BTDB/Proofs/`, the transaction log chain: walking predecessor IDs back from the newest log always ends.
  - `BTDB/Proofs/`, file IDs: an allocated file ID is never 0 or one already in use, and has the parity asked for;
    a standalone file collection's next ID is above every file found on open, in any listing order.
  - `BTDB/Proofs/`, file reads: the file collections' end-of-file test refuses exactly the reads that do not fit,
    though `ulong` arithmetic can wrap.
  - `BTDB/Proofs/`, collections read from data: their capacity is never above the bytes left, and is the stored count
    for valid data.
  - `BTDB/Proofs/`, inline objects: the bound on inline object numbers refuses no valid record.
  - `BTDB/Proofs/`, event store: an event the store accepts into a new file ends within its maximum size, block
    headers included.
  - `BTDB/Proofs/`, ObjectDB transactions: what `Get` returns for an oid is what the commit writes, across stores,
    deletions and recreated singletons.
  - `BTDB/Proofs/`, ObjectDB enumeration: every stored object and every new one is enumerated once.
  - `BTDB/Proofs/`, type names: no two storable types are registered under one name.
  - `BTDB/Proofs/`, relation registration: initializing relations keeps every relation registered meanwhile.
  - `BTDB/Proofs/`, compaction scheduling: a batch of compactions stopped early, cancelled or failed, schedules again
    every database it took but those compacted with no work left.
  - `BTDB/Proofs/`, hash collections: bucket counts are rounded up to the smallest power of two not below the
    capacity asked for.
  - `BTDB/Proofs/`, ObjectDB singletons: a transaction taking a singleton from the shared content cache gets the one
    of its snapshot and the writer the committed one, whatever readers cache concurrently and for the writer numbering
    of both key value databases, and an allocated singleton oid is stored by a later writer
    unless already durable.
  - `BTDB/Proofs/`, Bon: a container holds at most as many items as its buffer has bytes, so the check refuses only
    damage.
  - `BTDB/Proofs/`, IOC: a service resolving itself while it is created fails instead of waiting for itself forever.
  - `BTDB/Proofs/`, `EquatableArray<T>`: equal arrays, the default one and an empty one included, hash alike.
  - `BTDB/Proofs/`, chunk storage: loading its hash index files gives each chunk key its latest entry across reopens.
  - `BTDB/Proofs/`, `MemReader` and the export importer: reading a block of any declared length allocates at most a
    constant plus four times the bytes the input holds.
  - `BTDB/Proofs/`, `SeqLock`: a read accepted by an unchanged even counter is consistent when its data reads come
    between the two counter reads, and can see a half-done write when they come after the second.
  - `BTDB/Proofs/`, the in-memory B-tree: erasing one key and splitting nodes on insert keep every branch separator the
    first key of its child.

### Fixed

- `ODBIterator` reported an encrypted string that failed to decrypt, as with another key, and then read its
  undecrypted buffer as a string anyway, which threw or reported garbage. It reports only the failure now, also for
  ordered encrypted strings whose decryption is no string. `AesGcmSymmetricCipher.OrderedDecrypt` of data that is no
  whole number of blocks returns false instead of throwing `CryptographicException`.
- ObjectDB loaded an object stored under a name as the interface registered under it, failing with
  `MemberAccessException`, when the interface took the name before its implementation: a relation field of interface
  type `IShape` registers it as `Shape` too. The implementation takes the name from the interface now, and a read
  before the implementation is registered reports the type as not registered instead of binding the table to the
  interface for the rest of the session.
- `ReadOnlyKeyValueDB` descending through a damaged child pointer that pointed back up its tree never ended: seeking a
  key index grew its node stack until `OutOfMemoryException`, and `CalcBTreeStats` crashed the process with a stack
  overflow. A child must lie before its parent, as the creator writes them, and within 64 levels; otherwise the image
  is reported as corrupted.
- ObjectDB reading a damaged inline object number grew its list of read objects to that number, a null at a time,
  up to two billion entries for one value. A number beyond the bytes of the record is reported as damage now.
- `SlowGetValue` and `SlowGetKey` returned a buffer sized by the stored size of the current key, which for a value
  written compressed by older versions of `BTreeKeyValueDB` is the compressed size, so they returned its beginning only.
  They return what the cursor read now.
- An object stored again after `IObjectDBTransaction.Delete` in the same transaction was missing from the
  transaction's cache, so `Get` returned null for it until the commit, and `StoreAndFlush` did not store it at all. It
  is cached and stored again now. Storing a deleted singleton after `Singleton` created a new one under its oid threw
  `ArgumentException`; the stored one takes the oid back now.
- `IObjectDBTransaction.Enumerate` missed new objects not flushed yet when an object stored after them was already
  written, as by `StoreAndFlush`: only those newer than the last stored object were added. All of them are now.
- ObjectDB accepted two storable types under one name, as registering names a type without its namespace or enclosing
  type, so `A.Person` and `B.Person` both became `Person`. A stored `B.Person` was then loaded as an `A.Person` after
  reopening when `A.Person` was registered first. Registering a second storable type under a taken name now throws
  `BTDBException`, and the refused type does not become a candidate for polymorphic fields; register one of them with
  its own name. An interface or abstract class may still share the name of its implementation.
- `IObjectDB.InitializeRelations` could drop relations registered meanwhile: each call published the registry and
  factories it had copied before waiting for the writer, so of two concurrent calls the one finishing last dropped the
  relations of the other, and relations auto-registered while it waited were dropped too. Calls now run one at a time
  and publish only when nothing changed since they copied, starting again otherwise; the relation registry is guarded
  by a lock.
- `CompactorScheduler`: removing one database, as disposing it does, cancelled the compaction running for another,
  and neither that one nor the databases due after it were scheduled again until their transaction log moved to a new
  file. They stay due now; a compaction that fails waits the usual time before it is tried again.
- `AppendingEventStore` stored events that fit a new file's maximum size only without the headers of their later
  blocks, writing past that size; reading stops there, so the events were lost. Such events are refused with
  `ArgumentOutOfRangeException` now, like larger ones.
- ObjectDB could lose a singleton on reopen: a commit that had stored its table info before another transaction
  first asked for the singleton cleared the need to store the singleton oid that transaction allocated, so the oid was
  never stored, and the singleton later saved under it was not found after reopening. A commit clears it now only when
  it stored the oid.
- ObjectDB could serve a stale singleton from its shared content cache after a commit changed it: storing the
  singleton got by enumeration or by its oid before the database had loaded its oid, deleting it by its oid, and
  `DeleteAllData` did not invalidate the cached content, which a reader could cache meanwhile or had cached before.
  They invalidate it now.
- `ODBIterator` decrypted and reported an encrypted string in a field the visitor skipped, and reported a null
  nullable in a skipped field as "null", so they showed up inside whatever the visitor was in. Both are skipped now.
- LSM decoding: a length or other 32-bit value out of range in damaged data threw `OverflowException`, and a 64-bit
  value with a tenth byte above 1 lost its high bits. Both are reported as a corrupted object now. So is a length
  whose end, near `int.MaxValue`, did not fit an `int`, which threw `ArgumentOutOfRangeException`.
- LSM WAL replay applied an insert of a key its memtable already held, which left the key in the memtable twice. It
  is reported as damage now, like an update of a missing key.
- `LsmKeyValueDB` failed to open when a stray object lay among its WAL chunks or manifests, such as the `.DS_Store`
  that macOS Finder leaves in a folder of `FileSystemLsmStorage`: every name under `wal/` was parsed as a chunk number,
  throwing `FormatException`, and the last name listed under `manifest/` was read as the newest manifest, so a name
  sorting after the digits failed decoding. Only names made of digits count as chunks and manifests now, and the
  newest manifest is the one with the highest number.
- `LsmKeyValueDB` native memtable: allocating a key or value within 7 bytes of `int.MaxValue`, as a span over native
  memory can be, rounded its size up to a negative one, so the arena handed out memory it did not have and the copy
  overwrote native memory. Such a size is rejected with `ArgumentOutOfRangeException` now.
- `LsmKeyValueDB` accepted `WalFlushInterval = TimeSpan.Zero`, which kept the WAL uploader of an idle writer spinning
  on a full core. Opening rejects it with `ArgumentOutOfRangeException` now, like a zero `RefreshRetryInterval`;
  commits wake the uploader at once whatever the interval.
- `LsmKeyValueDB` compaction: a level's size target past `long.MaxValue`, as `LevelMultiplier = 1000` gives level 5
  of seven, wrapped around to a negative or zero target, so the level never compacted down or always did. The target
  is capped at `long.MaxValue` now.
- `LsmBlockCache`: evicting index and filter partitions could stop over capacity with an evictable partition left,
  as with the data blocks below, once partitions of different sizes filled the cache: removals used up the fixed
  step budget too. Only moves of the eviction hand count against it now.
- `LsmBlockCache`: evicting blocks could stop with a shard still over capacity and an unpinned block left, because
  passing pinned blocks used up the eviction's fixed step budget. Eviction now stops only after passing every block of
  the shard pinned in a row.
- `LsmBlockCache`: a block loaded while the cache was being disposed was added to it anyway, so its native memory was
  never freed. Such a block now serves only the pin that loaded it and is freed when it is released.
- ObjectDB: a nullable, tuple, list or dictionary of a type without a field handler, like `char?`, failed with
  `NullReferenceException` while building its handler. It is now reported like the type itself: the property is named
  when the relation is created, or storing the list or dictionary fails with `InvalidOperationException`.
- Source generator: generated code of a partial class declared the members `Creator` and `Constr` in it, so a member
  of the class with either name, like a `Creator` property, made it fail to compile. They are named `Creator4BTDB`
  and `Constr4BTDB` in partial classes now.
- Source generator: the registration class generated for a type `Foo`, a file class `FooRegistration` in its namespace,
  hid a type `FooRegistration` of that namespace in the generated file, so generated code using that type did not
  compile. Such a registration class gets another name now.
- Source generator: a stored member, constructor parameter or `[Dependency]` member whose type generated code cannot
  access, like a private nested class of a class that is not partial, made the generated code fail to compile. It is
  now reported as BTDB0053, naming the member; making the class partial lets generated code use such types.
- IOC: with `UseBtdbIoc`, a disposable singleton, scoped or registered instance resolved through the service provider
  was disposed twice, by the provider, which disposes what factories return, and by the BTDB container owning it. The
  container now leaves such instances to the provider.
- IOC: a disposed scope returned the scoped instances it had disposed. Resolving one now throws
  `ObjectDisposedException`, like creating a new owned instance does; instances the scope does not own still resolve.
- IOC: a scoped or singleton instance created while its container was being disposed could stay undisposed: it was
  added after disposal took the owned instances, or adding it threw `ObjectDisposedException` without disposing it.
  Such an instance is now disposed before the exception.
- `ByteBuffer.Expand` restarted a slice at the start of its array, so `[4, 5]` expanded to 3 bytes read `0, 1, 2`,
  checked the size against the whole array instead of the space after the slice, and marked a buffer that is valid only
  synchronously as async safe. It now keeps the start and the async safety. `ResizingAppend` to or of a buffer without
  an array, like `default`, no longer throws.
- `MemoryPositionLessStream.SetSize` to a smaller size kept the bytes beyond it, so growing the stream again, by
  `SetSize` or a write past the end, read them back. Like a file, it now reads zeros there, and frees whole buffers
  beyond the new size.
- `PtrLenList.TryExclude` of a range ending in a gap moved the start of the next range back to the end of the excluded
  one, so excluding `[5, 8)` turned `[20, 24)` into `[8, 24)`; `UnmergeInPlace` did the same. Such a range now leaves
  the next one as it is.
- IOC: `ContainerVerification.SingletonsUsingOnlySingletons` verified only the default registration of each type, so a
  singleton registered before another registration of the same type, which `IEnumerable<T>` still resolves, could use
  a transient or scoped dependency unreported. Every registration is verified now.
- Source generator: a generic class, generated through `[GenerateFor]`, that inherits members from a generic base class
  made the module initializer throw `MissingFieldException`, failing every type of its assembly. The accessors of those
  members, and of hook methods of the base, were nested in the generic `Activator` and so had its type parameters too,
  which `UnsafeAccessor` rejects. They are declared beside it now.
- IOC: generated factories read only string keys of `[FromKeyedServices]`, as the text of their literal, so the key
  `1` became `"1"` and an enum or type key became the parameter name, and when a member also had `[Dependency]` the
  attribute declared first won. They now pass keys of any constant type and prefer the `[Dependency]` name, like the
  reflection fallback.
- Bon: `TryGetLong` read an integer above `long.MaxValue` as a wrapped negative long, and a crafted negative one below
  `long.MinValue` as a positive one. It now returns false for them and leaves the item unread, like an item of another
  type; untyped deserialization reads such a negative one as a double.
- Bon serialization: an integer outside of the member's type was truncated on load, for example 300 loaded into a
  `byte` as 44 and into a byte enum alike. It now leaves the member default, as other values that do not fit do.
- IOC: a dispatcher handler whose method came from a base class, such as a generic handler base, was not registered,
  so dispatching its messages threw "No handler for message". Public and same-assembly internal methods of base
  classes are now found.
- IOC: a dispatcher used the first one-parameter method with its name, so a static or private overload of the handler
  method broke compilation of the generated code, and a class handling several message types, such as one implementing
  `IHandler<A>` and `IHandler<B>`, handled only one of them. Every accessible instance overload is registered now.
- IOC: a dispatcher handler method taking a struct message made the generated code fail to compile, because it cast the
  message with `Unsafe.As`. Value type messages are unboxed now, and a `Nullable<T>` message is registered under `T`,
  the type of the boxed value.
- IOC: generated dispatchers kept the handler factories once per process, so creating a dispatcher for a second
  container made the dispatcher of the first one resolve its handlers from the second. `DispatcherItem` now keeps them
  for each root container, which scopes share, and the generated code calls the new `DispatcherItem.Prepare` and
  `Invoke`.
- IOC: a public `readonly` `[Dependency]` field was assigned directly by the generated factory, which did not compile.
  It is written through an `UnsafeAccessor` like non-public fields.
- ObjectDB: non-public `OnBeforeRemove` and `OnSerialize` methods of a base class of a relation item threw
  `MissingMethodException` in generated relations, whose accessors targeted the derived class, and private ones were
  not called at all in relations emitted as IL. They are now called in both, also from generic base classes.
- ObjectDB: `RemoveByIdPartial` counted the items an `OnBeforeRemove` method kept towards its `maxCount`, so it removed
  fewer items than it could, and calling it until it removed nothing left removable items after kept ones. Only
  removed items count now, as documented.
- IOC: a type without a generated factory, built by reflection, failed to resolve when a nullable enum constructor
  parameter had a default value: the default is reported as its underlying number, which was passed as it was. It is
  now converted to the enum.
- Source generator: members marked with an attribute derived from `DependencyAttribute` were not injected, because
  only `DependencyAttribute` itself was recognized; reflection takes derived attributes too.
- `AesGcmSymmetricCipher` took the nonces of `EncryptedString` values from `System.Random`, which is not a
  cryptographic generator. They now come from `RandomNumberGenerator`; the stored format is unchanged.
- `DiskChunkCache` loaded the entries of a damaged hash index as they were: a damaged length made `Get` allocate up
  to 4 GB, and an entry pointing past its value file or into an unknown file was kept. Such entries are now skipped.
- `SnappyDecompress.Decompress` accepted a copy with offset 0, which copied a byte onto itself, so damaged data
  decompressed instead of throwing `InvalidDataException`, and it allocated the declared size before checking the
  data, so a few damaged bytes could allocate up to 2 GB. It now refuses offset 0, and refuses a declared size above
  22 times the input, which no valid data reaches.
- `DiskChunkCache.Put` of content larger than one value file, an eighth of the capacity, never returned: it started
  new value files forever. Such content is now not cached, as `Get` of it returns nothing.
- IOC: a type without a generated factory, built by reflection, injected only public `[Dependency]` properties. Like
  generated factories, it now injects `[Dependency]` properties of any accessibility and `[Dependency]` fields, in
  the type and its base classes; a member hidden or overridden in a derived class is injected once.
- IOC: a type without a generated factory, built by reflection, resolved its constructor parameters by their names,
  ignoring the names `[Dependency("name")]` and `[FromKeyedServices("name")]` give them, as generated factories
  use.
- IOC: a type without a generated factory, built by reflection, did not inject a `[Dependency]` property whose
  private setter is declared in a base class: the inherited property looked read-only. Its setter is now found
  through the base class, as generated factories inject it.
- IOC: a type without a generated factory, built by reflection, passed `DBNull.Value` for a constructor parameter that
  could not be resolved and had no default value, so resolving it failed with a confusing conversion error, even
  for a nullable parameter. Like generated factories, it now passes null for a nullable parameter and otherwise
  throws `ArgumentException` naming the parameter.
- Source generator: `UpdateById` with `double` or `float` values threw `NotSupportedException`, because the
  generated code did not write them.
- ObjectDB: in relations emitted as IL, updating a row stored before its secondary key field became nullable failed a
  debug assertion in Debug builds while copying the stored value into the key. The value was converted correctly
  otherwise, and now is without the assertion.
- ObjectDB: `FindById` and `FindBy{SecondaryKey}` returning `ICollection<T>` or `IReadOnlyCollection<T>` threw
  `InvalidCastException` in relations without generated code, which are emitted as IL: their enumerator is no
  collection. They now return the enumerators the generated code returns.
- Source generator: lookups by a tuple key with enum elements, like `FindById((int Id, Color Color) key)`, threw
  `NotSupportedException`, because the generator knew tuple elements only by their type names, and so did
  `UpdateById` of tuple values and `AdvancedEnumeratorParam` bounds with enum elements. Relation method parameters now carry the underlying types of the
  enums anywhere in their types.
- Source generator: lookups by a key that is a list or set of items other than `string` and `ulong`, like
  `ListByTags(List<int> tags)`, threw `NotSupportedException`, although such keys are stored. The generated code now
  writes them like the key handler does: the count, then the items, enums and nullable items included.
- ObjectDB: saving an empty `HashSet` as a key failed with `NullReferenceException`, because an empty `HashSet` has no
  entries until its first item.
- `StructList.Pop`, `Clear` and a shrinking `SetCount` kept removed items reachable from the backing array, so for
  example the cursor stack of `InMemoryKeyValueDB` kept tree nodes of old versions alive. They now clear the removed
  slots when the items hold references, as `RemoveAt` does.
- `StructList.InsertRange` resized its array on every call, even when it had room, so it shrank the capacity
  `Reserve` made and copied the items again each time. It now grows the array only when needed.
  `StructList.ReplaceItem(item, newItem)` of a missing item throws `ArgumentOutOfRangeException`, as the other
  item-based methods do, instead of `IndexOutOfRangeException`.
- `LruCache.IncreaseCapacity` and `SpanByteLruCache.IncreaseCapacity` grew the cache beyond its maximum capacity, so it
  then held more items than the maximum before evicting the least recently used one. They now grow it at most to the
  maximum.
- ObjectDB: `float?` and `double?` key fields are refused like `float` and `double`; only the latter were checked.
  Their key encoding does not keep the order of negative values. Breaking: relations, `IDictionary` and `IOrderedSet`
  fields with such keys no longer open.
- ObjectDB: `decimal` and `decimal?` key fields are refused, like `float` and `double`: the source generator reports
  BTDB0035 and the runtime throws `NotSupportedException`. A decimal key sorted by its scale first and kept equal
  values with different scales apart, so 2 sorted before 1.5, negative values after positive ones, and looking up
  1.00 missed a row stored as 1.0. Breaking: relations with decimal key fields no longer open, and neither do
  `IDictionary` and `IOrderedSet` fields with decimal keys, which the same check covers.
- `RoaringBitmap.Source` of values: the first value above the maximum index ended the values, so later valid values
  of the same page were lost, and a page coming again after a later one was built out of the order merging expects.
  Values above the maximum are now skipped within its page, and pages out of ascending order throw
  `ArgumentException`; values of one page may still come in any order.
- `IRoaringBitmap.Count` of a bitmap whose pages were applied without their count yet, as `RoaringBitmap.BuildAsync`
  does across transactions, returned 0, and setting a bit then stored a count of 1. It now counts the pages, and
  applying a page forgets a count read before.
- Source generator: a `[Dependency]` property with a private setter in a base class from another assembly was not
  injected, because the generator does not see private members of other assemblies. The generated factory now finds
  the setter through reflection when it is created. A private `[Dependency]` field there is still not injected.
- ObjectDB: a nullable `DateTime` key accepted `Local` and `Unspecified` kinds, which a `DateTime` key refuses, because
  the value of a nullable key was written like a value outside keys. A local one sorted after every UTC one and missed
  ranges. It now refuses them the same way, and stores `DateTime.MinValue` and `MaxValue` as UTC, as its constraints
  already looked them up. Breaking: rows stored before with such a key fail to be written again, and those keyed by an
  unspecified `MinValue` or `MaxValue` are no longer found by it. The stored format of other nullable keys is
  unchanged.
- `Constraint.String.StartsWith` with a value ending with a high surrogate missed keys continuing it with a low
  surrogate, such as `StartsWith("a\uD83D")` for `"a😀"`, because a key stores a surrogate pair as one code point. It
  now matches every key that ordinal `StartsWith` accepts.
- Source generator: members marked with an attribute derived from `NotStoredAttribute` were stored, because only
  `NotStoredAttribute` itself was recognized. Bon serialized them and the generated metadata listed them, unlike
  reflection, which skips them. They are now left out of the generated metadata.
- Source generator: `[Dependency]` properties and fields declared in base classes were not injected, so a type
  resolved through its generated factory left them null. They are now injected like those of the type itself,
  including private setters and fields of a base class, members of generic base classes, and protected members of a
  non-partial type. A member hidden or overridden in a derived class is injected once, with the `[Dependency]` name
  of the nearest declaration that has the attribute. Breaking for types whose base dependencies are not registered:
  resolving them now fails unless the member is nullable.
- `BTreeKeyValueDB`: opening a database whose oldest transaction logs are missing and that has no usable key index
  deleted all remaining transaction logs and opened an empty database, unless `LenientOpen` was set. It now fails
  with `BTDBException` and keeps the files, so opening with `LenientOpen` can recover what they hold. Breaking for
  code that relied on the empty database.
- Bon: a string with a lone surrogate, which UTF-8 cannot hold, is written with the replacement character U+FFFD in
  its place, and strings and object keys are compared after that replacement. Before, two object keys differing only
  in a lone surrogate were both accepted and written as the same key. A `Dictionary` whose string keys differ only so
  fails to load, as its keys repeat.
- ObjectDB: a `DateTime` key with `Local` kind was stored with the kind in its highest bits, so it sorted after every
  UTC key, `DateTime` range constraints missed it, and its UTC equivalent did not find it. Keys and `DateTime`
  constraints now refuse `Local` kind with `ArgumentOutOfRangeException`, as they already refused `Unspecified`;
  convert such values to UTC. Breaking for code storing local keys; keys stored before still load.
- Event store: a tuple loaded without its type, a `DynamicTuple`, failed to store again; with IL emitted its saver
  read the items from generic arguments a `DynamicTuple` has none of and failed with `IndexOutOfRangeException`. It
  now saves the items it read, and values that know their descriptor use the savers that are not emitted. A tuple
  holding a collection, like `(string, List<int>)`, also failed to load as the root of an event, as its preferred
  tuple of collection interfaces is not registered; it loads as a `DynamicTuple` now. A list of objects also takes the
  items of a list of value types, like the `ListWithDescriptor<int>` such a tuple holds.
- Event store: an enum loaded without its type, a `DynamicEnum`, failed to store again, alone or in a list loaded
  without its type, as the enum descriptor converted it to an integer it is not. It now saves the value it read. With
  IL emitted (`IFieldHandler.UseNoEmitForDescriptors = false`) the enum saver instead converted such an object
  reference with `conv.i8` and saved its address as the enum's value, silently storing garbage.
- Event store: a nested collection in an `object` member, like a `List<List<int>>`, failed to load with "Cannot find
  collection metadata", as no member declares its preferred `ICollection<ICollection<int>>`. Such an `object` member
  now loads it as the collections of objects it holds, as it does a collection of a type the reader does not know.
- Event store: storing again a collection loaded without its type that holds nullables or other collections, like
  `List<int?>` or `List<List<int>>`, failed. The serializer did not register the descriptors nested in a merged one,
  and list and dictionary descriptors could not save a collection as `object`, the type of the items of a collection
  loaded without its type. Both are fixed, so such collections, documented as not supported, now round-trip.
- Event store: a dictionary held by an `object` member failed to load with "Cannot find collection metadata for
  IDictionary", the type preferred for it, as only that exact type was looked up. The dictionary types the generator
  registers for declared members, which all create a `Dictionary`, are now tried too, as lists already did.
- Event store: storing again a list of byte arrays loaded without its type, whose items are objects, overflowed the
  stack and ended the process. The byte array descriptor, and the ObjectDB byte array field handlers, converted a
  value of another type with the two types of the conversion swapped, so the conversion asked to save that type
  again. They now convert the value to the byte array they save.
- Event store: re-serializing an object loaded without its type, whose type held itself below the root, overflowed
  the stack and ended the process. Merging its descriptor recorded only the root as visited, so a cycle below it
  mapped itself without end. Every descriptor is recorded now, and a placeholder for a cycle resolves to the merged
  descriptor rather than the one read. Re-serializing such dynamic objects stays unsupported and fails with an
  exception (`BTDB/Proofs/BtdbProofs/MergeCycle.lean`).
- Bon serialization: a member holding a class of another name was skipped twice, as reading the class had already
  consumed it, so the deserializer lost the member after it and read the rest out of step. A class derived from the
  member's type, which members of a class's own type store as they are written with the runtime type, now loads as
  that class.
- Bon serialization: a thread deserializing a recursive class while another thread was creating its deserializer got
  the placeholder cached meanwhile, which loaded the same value again by name and got the placeholder again, until
  `InsufficientExecutionStackException`. The placeholder now waits for the deserializer and uses it, as serialization
  already did.
- Bon serialization: a member typed as a registered subclass of `List`, `HashSet` or `Dictionary` was deserialized
  into the base collection, so the member held an object of a type it cannot hold, which calling the subclass's
  members on reads past. It is now created as the subclass and filled as the base collection; members the subclass
  adds are still not stored.
- Bon serialization: a null class or `Tuple` was reported as not loaded, so a member kept the value its constructor
  gave it and a tuple holding a null class item, like `(long, Item?)?`, loaded as null. They now load as null. Found
  by a new serializer-level fuzz test.
- Bon serialization: deserializing a class that holds itself, like a tree node, overflowed the stack and ended the
  process when the class was first reached as a member type, such as the values of a dictionary, rather than by its
  name. Only the lookup by name put a placeholder in the cache while a class's deserializer was created; the lookup by
  type now does too.
- ObjectDB: each instance of a lazy `IDictionary` or `IOrderedSet` cached its count, so after one instance changed
  the collection, another instance of the same collection, as loading its owner twice in a transaction gives, reported
  the old count. `Count` is now counted on each call, which takes two seeks.
- Event store: a dictionary member holding a dictionary other than `Dictionary`, like a `SortedDictionary`,
  `ReadOnlyDictionary` or `ImmutableDictionary`, failed to store with "Cannot save ... as Dictionary". Any other
  `IDictionary` is now enumerated; the stored format is the same.
- Event store: a list of objects in a member declared only as `IReadOnlyList<T>` or `IReadOnlySet<T>`, with no other
  member of the item type declared as a list or set, failed to load with "Cannot find collection metadata". The
  collection types tried for it now include the read-only interfaces, whose generated metadata creates a `List` or
  `HashSet`, and a collection that loads as another one of the same items, like a `List` for an `IReadOnlySet<T>`
  member, is copied into the collection that member's metadata creates. With IL emitted
  (`IFieldHandler.UseNoEmitForDescriptors = false`) such a member failed with `InvalidCastException` instead;
  `IntelligentCast` now converts too.
- ObjectDB and the event store: a list member holding a collection other than an array, `List`, `HashSet` or
  `Collection`, like the `ReadOnlyCollection<T>` that `AsReadOnly()` returns or an `ImmutableList<T>`, failed to save
  with "Cannot save type". Any other `IEnumerable<T>` of the item type is now enumerated; the stored format is the
  same.
- ObjectDB: with relations emitted as IL (`IFieldHandler.UseNoEmitForRelations = false`), a `ListBy`, `CountBy` or
  `AnyBy` method whose key prefix ends with a list, like a secondary key on an `IList<ulong>`, failed with
  `InvalidProgramException`. The list saver opened a try block, which must start with an empty evaluation stack, while
  the method's call arguments were on it; it now disposes its enumerator after the loop instead.
- Event store: an `object` member holding `IPAddress.Loopback`, `IPAddress.Any` or another address .NET keeps as an
  instance of its internal read-only `IPAddress` subclass failed to store, as events describe such a member by its
  runtime type. That type now maps to the `IPAddress` descriptor in both event store versions.
- ObjectDB and the event store: changing a member from a value type larger than 16 bytes, like `DateTimeOffset?`,
  `Guid?` or `decimal?`, to another type, like `DateTimeOffset`, lost its value. ObjectDB reported the field and loaded
  the default, and the event store failed with `NotImplementedException`. The conversion had staging only for values up
  to 16 bytes without references; larger ones are now staged in a one-item array. Converting savers also chose their
  staging by the source type instead of the target type they write, which could hide a reference from the GC or
  overrun an 8-byte slot.
- Event store: an event whose objects nest deeper than the stack holds, like a long linked list, overflowed the stack
  and ended the process when stored in either event store version, and when read on a thread with a smaller stack than
  the one that wrote it or from damaged data. Both now fail with `BTDBException`.
- ObjectDB: inline objects nested deeper than the stack holds, like a long linked list of inline nodes, overflowed the
  stack and ended the process when written, and when read on a thread with a smaller stack than the one that wrote
  them or from damaged data. Both now fail with `BTDBException`.
- ObjectDB: storing an object that contains itself through inline objects, like a node whose `Next` is the node,
  overflowed the stack and ended the process, as each inline object is written with a writer context of its own. It
  now fails with `BTDBException` naming the type, and the transaction stays usable. Make one of the objects a stored
  object or reference it through `IIndirect<T>` (`BTDB/Proofs/BtdbProofs/InlineCycle.lean`).
- Bon serialization: a reference cycle, which Bon cannot store as it keeps values and not references, overflowed the
  stack and ended the process. Serialization now fails with `InsufficientExecutionStackException` saying so, as
  deserialization of too deep data already did.
- Bon serialization: a `char` member was serialized, as its UTF-16 code unit, but deserializing it failed with
  `NotSupportedException`. It now loads.
- Bon: a damaged `DateTime` whose ticks are out of range failed with `ArgumentException` from `DateTime.FromBinary`
  when read or dumped to JSON. It is now reported as `InvalidDataException`, like other damage.
- Bon: `DumpToJson` failed with `ArgumentException` on a NaN value, which JSON numbers cannot hold. It now writes NaN
  as the string `"NaN"`, as it writes infinities as `"+∞"` and `"-∞"`.
- ObjectDB and the event store failed with "Cannot save type <>z__ReadOnlySingleElementList" for the list a one-item
  collection expression makes for `IEnumerable<T>`, `IReadOnlyCollection<T>` or `IReadOnlyList<T>`. They already saved
  the array such expressions make for more items and now save the one-item list too.
- Bon serialization: members typed as a collection interface, such as `IList<T>`, `IReadOnlyList<T>`, `ISet<T>`,
  `IEnumerable<T>`, `IDictionary<TKey,TValue>` or `IReadOnlyDictionary<TKey,TValue>`, deserialized as null. They now
  load as the `List`, `HashSet` or `Dictionary` the generated collection metadata creates, and a registered class such
  a member holds still loads as that class. Serializing such a member also failed with `NotSupportedException` for
  implementations other than arrays, `List`, `HashSet` and `Dictionary`, like the collections that collection
  expressions make for `IReadOnlyList<T>` and `IEnumerable<T>`; those are now enumerated.
- Bon serialization: deserializing a `List`, array or `Dictionary` gave a null string or byte array item the value of
  the item before it, as items reuse one storage slot that a null item left unwritten. A null `byte[]` was written as
  an empty array, and a null string or byte array member kept the value the constructor gave it. Null items and
  members now load as null (`BTDB/Proofs/BtdbProofs/BonReusedSlot.lean`).
- ObjectDB: removing an object of a recursive type that keeps its children in an `IDictionary`, such as a tree node,
  left the dictionaries below the first one in the database. Detecting what needs freeing reports a type whose
  detection is in progress as needing nothing, and the dictionary kept that answer for its values. It now asks again
  once detection is over. Freeing also stops at a dictionary it already frees, which damage can make a dictionary hold,
  instead of recursing until the stack overflows (`BTDB/Proofs/BtdbProofs/DictionaryFree.lean`).
- Event store: `FullNameTypeMapper.ToType` failed with `ArgumentNullException` for a generic type name whose argument
  or definition maps to no type, as after that type was renamed, and asserted on unbalanced names that damage leaves;
  such names now map to no type. Both event store versions, and `EventSerializer.ProcessMetadataLog`, report an unknown
  type category in metadata as `BTDBException` ("Metadata corrupted") instead of `ArgumentOutOfRangeException`, and the version 1 store rejects a
  type id its block cannot hold instead of growing its type table to it.
- Event store: an event stored with a type name that maps to no type, as after the type was renamed or removed without
  a mapping, failed with `NullReferenceException` when loaded into a member of a known type such as `List<Item>`,
  without emitted code, which is the default. Its objects now load as dynamic objects and an unassignable one is
  reported with `BTDBException`.
- Event store: a damaged type id in a metadata log could make `EventDeserializer.ProcessMetadataLog` grow its type
  table to that id, so a large one exhausted memory, and other damaged ids failed with `ArgumentOutOfRangeException` or
  `NullReferenceException`; the types a failed log had read stayed behind for the next one. A type id outside what the
  log can hold, a missing type id and a reference to an unknown type now report `BTDBException` ("Metadata
  corrupted"), and a failed log leaves nothing behind (`BTDB/Proofs/BtdbProofs/EventMetadataIds.lean`).
- Event store: damaged event data whose back reference pointed to no object read before failed with
  `ArgumentOutOfRangeException`, and one naming a type id without a type failed with `NullReferenceException`. The
  first now reports `InvalidDataException`, and the second is treated as missing metadata, like an unknown type id.
- ObjectDB: a relation row whose stored value was damaged to name a version the relation never had failed to load with
  `IndexOutOfRangeException` or `NullReferenceException`; it now reports the damage with `BTDBException`.
- Bon serialization: a damaged dictionary whose key came out repeated or null failed to deserialize into a typed
  dictionary with `ArgumentException`; it now reports `InvalidDataException`, as other damage does.
- Bon: damaged data could crash the process with a stack overflow. A container pointer damaged to point at its own
  container, or past it, made deserialization recurse through the same values forever. Readers now reject, with
  `InvalidDataException`, a container that does not lie before the pointer to the container holding it, which valid
  Bon always does, so nesting ends within the data (`BTDB/Proofs/BtdbProofs/BonNesting.lean`). Deserialization also
  fails with `InsufficientExecutionStackException` instead of overflowing the stack on nesting deeper than it can take.
  Found by a fuzz test of damaged Bon.
- Bon serialization: when creating the serializer or deserializer of a class failed, for example on a member type it
  does not support, the placeholder cached for recursive types stayed behind. Serializing the class again then waited
  for it forever, and deserializing it again made the placeholder deserialize the same value again until the stack
  overflowed and the process crashed. The placeholder is now removed, so every attempt fails with the same exception
  (`BTDB/Proofs/BtdbProofs/SerializerPlaceholder.lean`).
- Bon serialization, and ObjectDB and the event store through the generated metadata they use by default (without
  emitted code), lost or misread `ValueTuple` items next to struct-typed ones; values written that way before the fix
  are wrong. `ValueTuple` has
  automatic layout, where the runtime places struct items such as `DateTime`, `Guid`, nullables and nested tuples
  after primitive ones, but the serializer computed offsets in another order, and the source generator measured tuple
  layouts on stand-in structs with sequential layout. So `(DateTime, byte)` came back with a default `DateTime`,
  `(Guid, byte)?` lost the end of its `Guid` in relations, and `((int, string), MyEnum)` made the serializer read a
  string from the enum's bytes and throw. The generator now measures the tuple itself, and the serializer uses its
  layout, or measures the tuple at runtime when no generated code registered it. `Tuple<>` items, laid out by the
  runtime the same way, were read by the serializer at the same wrong offsets.
- Source generator: an indexer of a `[Generate]` class was stored as a field, named `this[]`, through accessors
  without its index, which do not exist, so serializing the class failed with `MissingMethodException`. Indexers are
  no longer stored, as reflection skips them too.
- Source generator: a public field of a generic base class, such as `public List<T>? Items;` in a base of a
  `[Generate]` class, got an accessor typed with the type argument instead of the type parameter, which the runtime
  does not find. Loading the assembly then failed with `MissingFieldException` from its module initializer.
- Source generator: a `[Generate]` class implementing an interface property explicitly, such as
  `string IHasName.Name { get; set; }`, listed the property twice, once through a backing field named after the
  interface property, which does not exist. Loading the assembly then failed with `MissingFieldException` from its
  module initializer. The property is now stored once, under its own name, as reflection names it.
- Source generator: a dispatcher interface or a containing partial type named with a C# keyword, such as `@event`,
  was declared in the generated part without its `@`, and so were calls of public `[OnSerialize]` and
  `[OnBeforeRemove]` methods and of dispatcher handler methods named that way; the generated code did not compile.
- Source generator: the generated part of a `[Generate]` dispatcher interface nested in other types was written
  outside them and did not compile. It is now nested in partial parts of its containing types, and nesting in a type
  that is not partial is reported as BTDB0052.
- Source generator: generated types whose names differ only by case, such as `Item` and `ITEM`, or a delegate or
  dispatcher interface nested in a class with the same name as one outside it, got the same generated file name. The
  generator then failed with `ArgumentException` and generated nothing for the whole project. Nested delegates and
  interfaces are now named after their containing types, and a file name taken ignoring case gets a numeric suffix
  (`BTDB/Proofs/BtdbProofs/HintNames.lean`).
- Source generator: a type named `Unsafe`, `RuntimeHelpers`, `UnsafeAccessorAttribute`, `UnsafeAccessorKind`,
  `InvalidOperationException` or a generic `Func` in the namespace of a generated type broke the generated code, which
  lives in that namespace and named those `System` types without qualification. It now names them with `global::`.
- Source generator: relation interface methods whose parameters or parameter types are named with C# keywords, such
  as `FindById(ulong @event)`, `ListById(ulong id, AdvancedEnumeratorParam<long> @params)` or `FindById(@enum kind)`,
  generated code that used the names without their `@` and did not compile.
- Source generator: a `[Dependency]` property or field named with a C# keyword, such as `@base` or `@event`, was
  assigned without its `@`, so the generated code did not compile. A class in a namespace written with `@`, such as
  `@event`, also got that `@` in `ClassMetadata.Namespace` and so in its default persisted name, which the runtime
  name of the type does not have. Found by a test of keyword identifiers.
- Event log: a disposed `EventLogService` could keep owning a topic. A submit from a peer that was taking over the
  topic when the service was disposed attached its new owner lane after disposal had stopped the old one, so nothing
  stopped it and its heartbeats kept writing; publishers on other nodes went to the disposed node until its owner
  timed out. The topic now stops such a lane. Reading the service's disposal token after `DisposeAsync` no longer
  throws `ObjectDisposedException` in its background work either (`Proofs/EventLogProofs/DisposeAttach.lean`).
- Event log: publications could wait forever after their owner stopped. A stop from outside, such as a found
  takeover or the service shutting down, came while a write was carrying a batch of records, which were then in
  neither the queue nor a session, so the stop did not fail them. When that write then failed, its own stop found the
  lane already stopped and returned, so the batch never got an outcome and its publishers never resubmitted it. The
  lane now fails such a batch too, so publishers resubmit it to the next owner. Found as rare hangs of the cluster
  chaos test under load (`Proofs/EventLogProofs/StopSettles.lean`).
- Event log: an owner could acknowledge records that were then in no reader's history. It sealed its split and was
  slow to create the successor with its batch; meanwhile another owner took over, filled that successor and later
  splits, and merging deleted it. That needs a `DeletionDelay` shorter than the stall, as tests use. The late
  create-if-absent then landed on the free name, so the first owner acknowledged a batch hidden behind the merged
  objects. An owner now checks after creating a split that no later split exists, which merging needs before it
  deletes a split, and is fenced otherwise, so the batch is published again. Found by a reader test failing under
  machine load.
- `ReadOnlyKeyValueDB` threw `EndOfStreamException` or `OverflowException` for a damaged header whose counts ran past
  the data, where truncated data is documented to throw `InvalidDataException`. Found by fuzzing damaged images.
- Replaying a damaged `BTreeKeyValueDB` transaction log allocated whatever key length a command declared before
  reading the key, so a damaged length could allocate up to 2 GiB, and one beyond the maximum array size failed the
  open with `OutOfMemoryException` instead of ending replay. A length beyond the rest of the file now counts as
  damage, like a negative one.
- `KeyValueDBExportImporter.Import` and `ImportAsync` allocated the key or value size an export declared before
  reading it, so a few bytes of a damaged export could allocate gigabytes or fail with `OutOfMemoryException`
  instead of `EndOfStreamException`. The buffer now grows with the bytes that arrive, as `MemReader` does since its
  fix.
- `BTreeKeyValueDB.CreateKvi` ignored `PreserveHistoryUpToCommitUlong`: it dropped the key index preserved history
  opens from, so a later `OpenUpToCommitUlong` at the preserved commit silently opened a later commit. It now keeps
  that key index, as compaction does. Found by opening randomized histories at every preserved commit.
- A crash after `BTreeKeyValueDB` wrote a key index could open with wrong or unreadable values. The index, written by
  `CreateKvi` and compaction and flushed to disk, points at transaction log commands of its last commit, which were
  not necessarily flushed, so a crash could keep the index and lose those commands. The log is now flushed before
  the index, and the index's root is taken under the same exclusion of writing transactions; compaction waits for
  a running writer. `CreateKvi` called while a writing transaction holds the log writes an index only when the last
  commit is already durable (always with `DurableTransactions`), and otherwise skips it with a logged warning. Found
  by simulating crashes at every file operation, also beside a concurrent writer.
- Reading past the end of a file, as a damaged value pointer in a key index or transaction log asks for, read memory
  outside the mapping in `OnDiskMemoryMappedFileCollection`, which could crash the process or return foreign bytes,
  and failed with `ArgumentOutOfRangeException` in `OnDiskFileCollection` and `IndexOutOfRangeException` in
  `InMemoryFileCollection` and `InMemoryReplicationFileStorage`. All of them throw `EndOfStreamException` now, as
  `OnDiskReplicationFileStorage` already did.
- ObjectDB: stored relation or table metadata with a damaged count of key fields, secondary keys or fields, and a stored
  enum configuration with a damaged count of values, sized arrays and collections before reading them, so opening
  failed with `ArgumentOutOfRangeException` or `OverflowException`, or allocated gigabytes. Counts only hint capacities
  now, and a count past what the metadata holds is reported as `BTDBException`.
- Event stores: a damaged count of a list, dictionary or array in an event, or of an enum's names in its descriptor,
  sized what it was read into, so past `int.MaxValue` deserializing failed with `ArgumentOutOfRangeException` on a
  negative capacity or length. Counts only hint capacities now, and an array declaring more items than its data
  holds is `InvalidDataException` before it is allocated.
- ObjectDB: a damaged count of a stored list, set or dictionary sized the collection it was read into, so past
  `int.MaxValue` reading the object failed with `ArgumentOutOfRangeException` on a negative capacity, in both the
  emitted and the interpreted loaders. The count only hints the capacity now, at most the bytes left, and a damaged
  one fails at the end of the value with `EndOfStreamException`.
- Bon: a damaged array or tuple item count above `int.MaxValue` became a negative capacity when deserialized into a
  list, failing with `ArgumentOutOfRangeException`, and a large one sized collections far beyond the data; a dictionary's
  count was doubled in a `uint`, so 2^31 + 1 pairs wrapped to one and the damaged dictionary read as valid. An array,
  tuple, object, class or dictionary declaring more items than its buffer has bytes, as no valid one can, is
  `InvalidDataException` now.
- Source generator: the constructor chosen for a class could take a pointer or a ref struct, which no container or
  metadata can pass, so `[GenerateFor(typeof(string))]` generated a call of `string(sbyte*, int, int, Encoding)` that
  did not compile. Such constructors are skipped now.
- Source generator: `[GenerateFor(typeof(SomeStruct))]`, or of a delegate, generated a creator and metadata written for
  a class, so the generated code failed to compile. Such a type is now reported as BTDB0054 and nothing is generated
  for it; a delegate gets its factory from `[Generate]` on its own declaration.
- `StructList<T>.Reserve` given less than the capacity, but more than the count, shrank the array to it, dropping
  capacity reserved before. It only ever grows the array now.
- `BonSerializerFactory` refused a multi-dimensional array, to serialize or to deserialize into, with
  `InvalidOperationException`; it throws the `NotSupportedException` other unsupported types throw.
- `BonSerializerFactory.Serialize` of a plain `new object()`, also as the value of an `object` member, hung its thread
  in a busy loop: the serializer of object waited for a serializer replacing it, which never came. It throws the
  `NotSupportedException` other types without serialization support throw.
- IOC: a singleton or scoped service whose factory resolved the same service while it was being created, for example
  through the injected `IContainer`, hung its thread in a busy loop: it waited for its own instance under a lock it
  already held. It throws `InvalidOperationException` now, and so does every later attempt.
- Opening a `BTreeKeyValueDB` whose key index header had a damaged count of ulongs allocated that many at once,
  512 MB for a count of 2^26, before failing on the header. The values are read as they come now.
- Opening a `BTreeKeyValueDB` whose key index had a damaged key length allocated one and a half times that length,
  384 MB for a 240-byte index, before rejecting the index and replaying the logs. An uncompressed key index now
  rejects a key longer than the rest of the file before allocating it, as replay already did for logs.
- `MemReader.ReadStringInUtf8`, `ReadByteArrayAsSpan`, `ReadBlockAsSpan` and `ReadByteArrayAsMemory` over a stream
  allocated the whole declared length before reading, so a damaged length made a 100-byte input allocate 256 MB
  before failing. They grow with the bytes that arrive now, as `ReadString` and `ReadByteArray` already did, and a
  length past `int.MaxValue` is `InvalidDataException`.
- `EquatableArray<T>`, in the library and in the source generator, treated the default array as equal to an empty
  one but hashed them differently (0 against the hash of no items), so a dictionary or set keyed by such arrays could
  miss an equal key. Both now hash to 0.
- `Utf16Utf8Dictionary` given more keys than its count failed with `IndexOutOfRangeException`; it throws the
  `ArgumentException` it already threw for fewer keys.
- `SeqLock.RetryRead` read the counter again with only acquire semantics, so on arm64, Apple silicon included, the
  data reads before it could complete after it and a read seeing a half-done write passed as consistent (a stress
  test saw 831 such reads in 15 million). It now issues a read barrier first. `RefDictionary`,
  `SpanByteNoRemoveDictionary`, the IOC container and the serialization metadata registry read under it.
- The chunk storage sub-database of `BTreeKeyValueDB` (`GetSubDB<IChunkStorage>`) lost every chunk stored before the
  previous reopen: each open writes a new hash index file with only what it stores, but opening loaded only the
  newest one. Opening now loads them all, oldest first, and a later entry for a key replaces an earlier one, so a
  chunk that stopped being a leaf stays so. The packed content length also no longer clears the compressed flag.
- `OnDiskFileCollection`, `OnDiskMemoryMappedFileCollection` and `InMemoryFileCollection` kept the largest file ID in
  an `int`. A stray file named with the largest `uint`, like `4294967295.bin`, made the next file get ID 0, which every
  later open skips as no file, so the commits of a transaction log created there were lost; an ID above `int.MaxValue`
  listed before a smaller one also lowered the largest ID. The largest ID is a `uint` now, and allocating past it
  throws `InvalidOperationException` instead of wrapping around to 0.
- `OnDiskFileCollection` and `OnDiskMemoryMappedFileCollection` read a file's ID from its name allowing a sign and
  spaces, so a stray file such as `+5.bin` beside `00000005.trl` took the same ID and opening failed with
  `ArgumentException`. Only plain digits count as an ID now, as the replication storages already required.
- Opening a `BTreeKeyValueDB` whose transaction log header had a damaged predecessor ID pointing at a later log
  hung forever following the cycle. The chain walk now requires every predecessor to have a lower file ID, as
  `OpenAsync` already did, and fails the open with `BTDBException` otherwise. Found by flipping header bits.
- `BTreeKeyValueDB` could delete its own files on open when one foreign file, or one whose header Guid was damaged,
  came first in the file listing, as `OnDiskFileCollection` lists a directory in no particular order. The first
  listed file decided the database Guid, every file with another Guid counted as foreign, and opening deleted the
  foreign files, so the database opened as the stray file's database or crashed with `InvalidCastException`. The
  Guid most files carry now decides, and among equally common ones the Guid of the earliest file.
- `BTreeKeyValueDB` lost every commit made after reopening from a crash that cut the transaction log inside a
  transaction between two of its commands. Replay dropped the unfinished transaction, and the next session wrote its
  commits to a new log file, but the following open stopped replaying at that file's first transaction start because
  the unfinished one was still pending, and opened without those commits. Replay now drops a transaction left open
  when the next one starts, so the commits are kept, also in databases already affected. Found by fuzzing damaged
  transaction logs.
- `MemReader.ReadString`, `ReadByteArray` and `ReadStringValues` allocated the length or count a prefix declared before
  reading any of it. A few bytes of truncated or hostile input, such as a malformed replication peer message, could
  therefore allocate gigabytes, or fail with `OutOfMemoryException` instead of `InvalidDataException`. A declared
  length beyond the buffered bytes now grows the result with the data that arrives, so a too-long prefix fails at
  the end of the input. Values that fit the buffer are read as before.
- Event log: while other owners appended to a topic, a lane resolving a publisher session, or any reader, could fail
  with `EventLogOffsetNotAvailableException` for a committed offset. Locating an offset reads split headers at the
  version a listing gave, and every append changes the tail split's version, so all retries could meet a changed
  version. A split's header never changes, so the reader now reads it at the split's current version. Found by
  publishing from concurrent sessions through competing owners under random storage faults.
- Descending enumeration of a relation (`ListBy…` with an `AdvancedEnumeratorParam`, including the ordered dictionary
  enumerators), an `IOrderedSet` or an `IOrderedDictionary` returned one key below the range when the caller removed
  the range's first key while enumerating, as code deleting what it lists does. The enumerators stopped by comparing
  key indexes, and a removed bound key takes the index of the key before it. They now compare keys with the range
  bounds.
- `BTreeKeyValueDB` and `InMemoryKeyValueDB`: a cursor that `FastIterate` or `FastIterateNoCursor` moved past the
  last key kept an index past the end. After another cursor's change, iterating again visited nothing, and the
  in-memory cursor still reported itself valid, so reading or iterating from it threw `InvalidOperationException`.
  Such a cursor is now invalid, so a later iteration starts at the first key, as in `LsmKeyValueDB`. Found by
  comparing cursor operations of the three databases.
- `LsmKeyValueDB`: `FastIterate` from a cursor whose erased key was the last key threw `InvalidOperationException`;
  like `BTreeKeyValueDB`, it now visits nothing. `LsmFastIterateOracleTest` compares `FastIterate` and `KeyHasPrefix`
  with `BTreeKeyValueDB` from current and fuzzy cursors.
- Relation queries: `Last` and `LastOrDefault` after `Select`, over an ordered or paged query, returned the
  projection of the last row scanned instead of the last row of the window, for example of a row the filter rejected
  or the row after `Take`. The projection is now computed from the last row of the window. Found by running the
  relation query differential test with more seeds.
- `InMemoryKeyValueDB`: a cursor that called `SetValue` after another cursor inserted a key read the next key once a
  key before it was erased, because the erase kept the tree path `SetValue` had refilled. Found by running the key
  value differential test with more seeds.
- `LsmKeyValueDB`: a cursor whose key was erased moved to the wrong side of a key that another cursor renamed with
  `UpdateKeySuffix` past it, so its next and previous keys and its key index differed from `BTreeKeyValueDB`. A key
  keeps its index when its suffix changes, so such a cursor now stays right before the renamed key, or right after it
  when the key grew. A cursor whose erased key was created again by another cursor now follows that key's suffix
  update like a current cursor. Found by running the LSM oracle test with a thousand more seeds; the failing seeds are
  kept, and `BTDB.LSM/Proofs/LsmProofs/FuzzyCursor.lean` proves that every cursor keeps its position.
- `SetUlong` with an index of 2^31 or more failed with `IndexOutOfRangeException` or a misleading
  `ArgumentOutOfRangeException` from inside the ulong array growth, because `idx + 1` wrapped around. The transactions of
  `BTreeKeyValueDB`, `InMemoryKeyValueDB` and `LsmKeyValueDB` now reject such an index up front, and transaction log
  replay ends at a damaged ulong index instead of failing to open.
- `BTreeKeyValueDB` failed to open when its transaction log held a key or value length that was negative or did not fit
  an `int`, or a key suffix update whose prefix was longer than its key, throwing `ArgumentOutOfRangeException`,
  `OverflowException` or `InvalidDataException`. Replay now ends at such a command, as it does at other content it
  cannot apply after a crash, and keeps the transactions before it.
- Event log: a merged object whose header claimed a record count near `ulong.MaxValue` passed validation, because the
  frame and record counts were added before the bound check and the sum wrapped around. The reader decides from that
  part of the header, before reading the header checksum, whether an offset lies beyond the object, so it skipped the
  object and later failed with `EventLogOffsetNotAvailableException`. Such a header is now reported as corrupted, and
  so is a merged header whose level or first offset does not match its name, as the merger already reported it. Parsing
  a whole merged object whose frame count did not fit an `int` threw `OverflowException`; it reports corruption too.
- Upgrading a field between numeric types in IL-emitting mode (`DefaultTypeConvertorGenerator`) now gives what a C#
  cast gives. A `uint` widened to `long` was sign-extended, so `uint.MaxValue` became -1; negative `sbyte`, `short` and
  `int` values widened to `ulong` were zero-extended from 32 bits; and `uint` and `ulong` values converted to `float` or
  `double` were read as signed. The no-emit converters already did this right.
- Event log: some options were not validated. `TakeoverAttempts` of zero never let a node own a topic, so every
  publication retried forever; a zero `AmbiguousRetryDelay`, `ContentionBackoff` or `MergeInterval` retried or merged
  in a tight loop; and a negative `DeletionDelay` failed every merge pass before cleanup. Such values now throw
  `ArgumentOutOfRangeException` when a topic is opened.
- `LsmKeyValueDB.OpenAsync` accepted options the engine cannot run with. A `WalUploadParallelism` of zero spun forever
  in the WAL upload, a `LeaseSafetyMargin` covering the whole `LeaseDuration` kept every writer takeover retrying, a
  zero `RefreshRetryInterval` spun while waiting for a commit, a `LevelCount` of one crashed the first compaction, and a
  `Level0StopWritesTrigger` below `Level0CompactionTrigger` stopped writes before level 0 could compact, for good.
  Open now validates the options and throws `ArgumentOutOfRangeException` for such values.
- Event log over HTTP: with `SplitCap` above `MaxRecordSize` plus 8 MiB, a committed batch of small records could
  exceed the peer message limit, which was derived from `MaxRecordSize` alone. A subscriber then rejected the live
  message, took the owner for unreachable and could take a healthy topic over. The limit now covers the largest
  batch the options allow and its per-record framing.
- Event log: merged objects could exceed `MaxMergedObjectLength`. The merger compared the limit with the bytes of the
  splits it merges, but a merged header indexes every frame and record, so with small records a merged object is up
  to twice its splits' frames. With more merge levels or larger splits, a group of small records could therefore
  reach the format's maximum; creating its merged object then threw on every pass, before cleanup, so a topic's old
  objects were never deleted. The merger now checks the merged object's length, and the option is validated.
- `IKeyValueDBCursor.FindNextKey` and `FindPreviousKey` documented that a failed call leaves the position unchanged.
  Every implementation invalidates the cursor instead, so a further call starts again from the first or last key
  with the prefix; the documentation now says so, and a test covers it.
- Event log: one Publish call could be committed twice. This happened when its first dispatch reached a new topic
  owner after its retry had already committed through the previous owner, because a lane trusted
  `previouslyDispatched = false` for an unknown session. An unknown session's state now always comes from committed
  history from the round's resume offset. A first submission at the owner's next offset still needs no storage read and
  keeps the combined takeover write.
- Event log: `GetBoundsAsync` could return an end below a receipt another node had already completed. The owner
  trusted a write still in flight when the call started, or the end of its commit loop. That write could have landed
  before another owner took over and committed. The owner now trusts only a write dispatched after the call started,
  or a tail check.
- Event log: a storage read could stop early, as if the history had ended. A reader whose cached listing ended with
  a sealed split walked on to the next split, and when merging had deleted that split since, it reported the end of
  history. Catch-up from storage then stalled, and the owner's scan for an unknown publisher session could miss
  committed records. The end is now concluded only from a fresh listing.
- Event log: a storage read could also stop early when its listing ran during a merge. A listing returns splits before
  merged objects, so a listing with several pages can return a merged object that was created after it returned the
  splits. That object covered every listed split, and the reader reported the end of history after it. The reader now
  continues with the split after a merged object, because history always continues there.
- Event log: an idle owner could stop as if it had been taken over. Its tail was sealed, a helper had created the
  header-only successor, and its merger then marked the sealed split for deletion, changing that split's version.
  Validation of a sealed tail now checks only the successor.
- `LsmKeyValueDB` writer takeover could lose acknowledged durable commits. Parallel WAL uploads complete out of order,
  so a crashed writer could leave several missing chunk names, and the new writer fenced only the first. Replay then
  stopped at the next hole, and a later takeover fenced it and discarded the new writer's commits. A takeover now
  fences every missing name below the highest one.
- `LsmKeyValueDB` replay could apply a writer's commits after missing ones. This happened when a predecessor's delayed
  WAL chunk took one of the new writer's own names. Epoch markers now name the writer's first own chunk, and replay ends
  that writer's history at the first own name holding a predecessor's chunk. Manifests record the epoch of their WAL
  start chunk for the same check.
  - The WAL chunk format version is now 2 and the manifest version 7. Databases written by earlier versions cannot be
    opened.
- `LsmKeyValueDB` takeovers that overlap could both become the writer with one epoch. Two takeovers that read the same
  state wrote equal fencing manifests, so the second took the first one's manifest for its own lost response. Their
  parallel WAL uploads could then interleave in one history. Every manifest now records a random number of its writer
  session, so the second takeover is fenced and takes over again with a higher epoch. The manifest version is now 8.
- `LsmKeyValueDB` with a `DeletionGracePeriod` below a minute deleted replaced manifests at once. That freed their
  names, so a predecessor's or a concurrent takeover's delayed manifest create could take one again, and readers
  probing manifest names could load it. Replaced manifests now always stay for a minute.
- `LsmKeyValueDB` could report a spilled commit as not committed although it committed. When the writer lost its lease
  while the worker was publishing the commit's manifest, the commit failed with "lost before the commit", but the
  manifest could still land. A retry then applied the transaction twice. When background work kept failing, the
  commit also withdrew a request whose manifest might land and deleted its tables. A commit the worker started is now
  never withdrawn, and if the writer stops it fails saying it may have committed.
- `LsmKeyValueDB` could keep showing a commit it lost with the writer lease. A commit that had just found the
  instance the writer could publish its state after losing the lease reloaded the state from storage, so local
  transactions saw commits storage did not have until the next refresh. The check and the publish now share the
  state lock with ending the writer role. `ReleaseWriterLeaseAsync` whose WAL upload failed kept such commits visible
  too; it now reloads the state from storage, and its error says the commits may be lost.
- `LsmKeyValueDB` readers could publish a state that never existed after a failed refresh. A refresh that found a
  newer manifest dropped the memtables it flushed and then failed reading the WAL. When the next refresh no longer
  found that manifest, because it was replaced and deleted and listing was throttled, it published the older tables
  without those commits under the newer commit metadata, and reads could fail on tables deleted since. Such a failure
  now reloads the reader from storage on the next refresh.

## 35.13.0

### Added

- `LsmKeyValueDB` writing transactions are no longer limited by memory.
  - Above `MaxTransactionMemory` (now 64 MiB by default) a transaction moves its changes into tables that only it reads.
  - Its commit publishes them as the newest level 0 tables by one manifest edit, without a WAL record. The commit is
    durable when it returns.
  - Every four spilled parts of equal size merge into one run of non-overlapping tables, and the commit merges all of
    them into one. Level 0 now holds runs, and its triggers count runs, so the next writer does not stall behind a large
    commit. Databases written by earlier versions cannot be opened; the storage format of the preview changed.
  - Memory stays bounded whatever the transaction size. `LsmKeyValueDBOptions.MaxTransactionSize` (unlimited by
    default) can still cap the total.
- `ILsmKeyValueDB.ReleaseWriterLeaseAsync` hands the writer role over. It makes every commit durable, releases the
  lease without flushing memtables and continues as a reader, so another instance can take over at once.

### Changed

- `LsmKeyValueDB` takes over as the writer faster:
  - it continues the reader's WAL replay instead of reloading the whole unflushed WAL;
  - it searches for orphans of a crashed writer in the background instead of listing every table and value file first;
  - the first lease retries follow within milliseconds (`LeaseRetryInterval`, now 250 ms by default, is the longest
    pause).
- Each `LsmKeyValueDB` writer session numbers new files from its own range (epoch << 32).
- `LsmKeyValueDB.Dispose` of the writer uploads the WAL and releases the lease at once. Flushes and compactions are
  left to the next writer.
- `LsmKeyValueDBOptions.DeletionGracePeriod` defaults to 1 day. It is also the longest a transaction in another process
  may keep its snapshot.
  - The manifest records pending deletions, so a writer takeover no longer restarts the period.
  - Replaced manifests are kept for at most 1 minute.

### Removed

- The `LsmKeyValueDB` reader registry: `LsmKeyValueDBOptions.RegisterReader`, `ReaderHeartbeatInterval`, the
  `readers/` objects and `ILsmStorage.PutAsync`.

## 35.12.1

### Added

- `LsmKeyValueDBOptions.MaxTransactionMemory` (256 MiB by default) bounds the memtable bytes one `LsmKeyValueDB`
  writing transaction adds. A transaction above it throws `BTDBException` on its next modification or commit and can
  only be rolled back, so memtable memory stays bounded.
- The low-level interpreter has frame opcodes (`OpCode` 152–162): reading key or value fields straight into frame or
  object slots, repeated field skips, typed compare-and-jump against constants (`QueryValueType`, `QueryCompare`),
  string compare-and-jump, and slot conversion (`QuerySlotKind`). They use fixed-width operands; `InterpreterCtx` gains
  `Frame`, `Objects`, `KeyReader` and `ValueReader` refs for them.

- `IRelation.Query<T>()` `Where` predicates now support:
  - comparisons, `!`, bool members and comparisons between properties;
  - arithmetic, numeric and enum casts;
  - nullable values with lifted C# semantics;
  - `Guid`, `decimal`, `TimeSpan`, `DateTimeOffset` and enum fields;
  - string `StartsWith`, `EndsWith`, `Contains`, `Equals`, `Length`, `string.IsNullOrEmpty` and
    `string.CompareOrdinal`;
  - `Contains` on constant collections.

  String values are tested without allocating. Predicates keep C# short-circuit exception semantics. See
  [Doc/Relations.md](Doc/Relations.md#query).
- `IRelation.Query<T>()` supports terminal operators:
  - `Count` and `LongCount`, which never materialize rows and use key indexes in O(log n) without a residual
    predicate;
  - `Any` and `All`;
  - `First`, `FirstOrDefault`, `Single`, `SingleOrDefault`, `Last` and `LastOrDefault`, which materialize only the
    returned row;
  - `Min`, `Max`, `Sum` and `Average` with a selector, which read only the needed fields.

  Results and exceptions match LINQ-to-Objects.
- `IRelation.Query<T>()` plans access paths. These become key range seeks instead of row tests:
  - equalities and `Contains` / `||` lists on leading key fields, as several seeks merged in key order;
  - `<`, `<=`, `>`, `>=` on integer, `bool` and enum key fields;
  - ordinal `StartsWith` on string key fields.

  Secondary indexes are chosen when they allow a longer prefix or range than the primary key, still returning rows in
  primary key order. `Count`, `Any`, `All`, `Min` and `Max` read only a covering index.
- `IRelation.Query<T>()` supports `OrderBy`, `OrderByDescending`, `ThenBy`, `ThenByDescending`, `Skip` and `Take`
  after `Where`, with LINQ-to-Objects results and exceptions:
  - orderings by key fields stream in key order;
  - other orderings sort keys read from stored bytes, keeping only `Skip + Take` rows;
  - unfiltered `Skip` uses key indexes;
  - only returned rows are materialized.
- `IRelation.Query<T>()` supports `Select` after `Where` and ordering, followed by `Skip`, `Take`, enumeration or
  terminal operators:
  - single values, anonymous types, constructors, `ValueTuple` / `Tuple`, records and member initializers;
    `[Generate]` classes are filled through generated setters or by writing auto-property backing fields in place,
    and value arguments are not boxed per row;
  - only the selected fields are read, and only for returned rows;
  - `Min`, `Max`, `Sum` and `Average` without a selector aggregate projected values.

  `Query<T>()` now returns `IOrderedQueryable<T>`. Selector calls and exceptions match LINQ-to-Objects.
- BTDB.SourceGenerator intercepts `IRelation.Query<T>()` query chains written inline (`Where`, `Select`, ordering,
  `Skip`, `Take` and terminal operators) when a project adds `BTDB.Interceptors` to `InterceptorsNamespaces`, with a
  .NET 9 SDK compiler or newer. Intercepted operators skip building their expressions, and repeated executions read
  captured values directly instead of walking the expression: a point query takes 0.96 µs instead of 2.1 µs. See
  [Doc/SourceGenerator.md](Doc/SourceGenerator.md#relation-query-interceptors).
- The low-level interpreter has expression opcodes (`OpCode` 163–185):
  - frame-to-frame compare, arithmetic (`QueryArithmetic`, `QueryOperandKind`) and numeric conversion;
  - capturing raw field bytes, with encoded equality and `StartsWith`;
  - string decoding into reused buffers and string operations (`QueryStringEncoding`, `QueryStringOp`,
    `QueryStringSet`);
  - set membership, query exceptions (`QueryException`) and skipping fields by field handler;
  - copying selector results (`CopyToFrame`, 183);
  - decoding captured strings into object slots (`StringFromCaptured`, 184) and clearing object slots
    (`SetObjectNull`, 185).

  `QueryValueType` gains `Guid`, `Decimal`, `TimeSpan` and `DateTimeOffset`.

### Changed

- `ILsmKeyValueDB.WaitForDurabilityAsync` and `LsmKeyValueDB.WaitForDurabilityAsync` accept an optional transaction
  number. Omitting it or passing `null` waits for the latest transaction committed at the time of the call, without
  including later commits or uncommitted changes.
- `IRelation.Query<T>()` predicates evaluate 2–4× faster per scanned row:
  - one interpreter program per stored value version;
  - fields are read only up to the last one a conjunct needs, and each conjunct is tested right after its last field
    is read;
  - equalities already guaranteed by the primary-key cursor prefix are not re-checked;
  - rows are tested with `FastIterate`;
  - row loaders are cached per type.

  Selective queries are now faster than enumerating the relation and filtering with LINQ.
- The interpreter runs programs without try blocks in an instruction loop without exception bookkeeping, and no longer
  enters a try block per instruction for programs that have them.
- `IRelation.Query<T>()` throws `NotSupportedException` for `Where` with an element index.
- `IRelation.Query<T>()` caches compiled queries per relation by expression shape, with captured values and
  `Skip`/`Take` counts as parameters bound for each execution. The cache is bounded to 256 shapes per relation.
  Repeated queries skip expression conversion and compilation: a point query takes 2.1 µs instead of 3.3 µs and
  allocates 3.7 KB instead of 10 KB, and with `Select` 4.1 µs instead of 7.0 µs. Scanners rent their value buffer
  from `ArrayPool`, and captured arithmetic such as `id + 1` is evaluated without the expression interpreter.
- `IRelation.Query<T>()` scans are 5–15 % faster:
  - selected properties are read straight into their result slots;
  - common integer fields are decoded, and common fields skipped, without the generic `RunCodeOp` dispatch;
  - whole-prefix scans no longer re-check their keys.

### Fixed

- Fix `LsmKeyValueDB.CompactorReadBytesPerSecondLimit` being ignored; it now paces the reads of compaction inputs.
- `LsmKeyValueDB` compactor read and write limits counted time since the database opened (or up to 10 seconds of
  idle time) as unused budget, so a compaction after idle time ran unpaced until it used up that credit.
- `DiskChunkCache.Dispose` threw `NullReferenceException` when a hash index file it knew about had already been removed
  from its file collection.
- Source generator:
  - incremental generation compared only kind and name of a type, so edits (new properties, relation methods, moved
    errors) kept stale generated code in IDEs or crashed the generator; `EquatableArray.Equals(object)` overflowed
    the stack;
  - `decimal` primary keys and `UpdateById` values, and `IList<string>` / `IList<ulong>` key parameters threw
    `NotSupportedException` at runtime;
  - `FindBy` returning `ICollection<T>` or `IReadOnlyCollection<T>` threw `InvalidCastException`;
  - relation methods re-declared with `new` (e.g. `RemoveAll`, `UpsertRange`) threw `NotImplementedException`;
  - a read-only property whose getter returns a backing field was treated as stored, so the relation failed to open;
  - `IOrderedDictionaryEnumerator` type arguments lost `global::` and failed to compile in namespaces like
    `Collections.Model`;
  - non-public `[OnSerialize]` / `[OnBeforeRemove]` methods in generic classes generated code that did not compile.
- Relations:
  - `RemoveById(prefix, AdvancedEnumeratorParam)` in descending order removed rows outside the requested range;
  - `CountBy` / `AnyBy` with an excluded start skipped only one key when several rows shared the start prefix;
  - `Constraint<byte>` / `Constraint<sbyte>` (`Any`, generic `Exact`, also nullable) used variable length encoding
    although these keys are stored in one byte, so values ≥ 128 or negative were never found;
  - `IOrderedDictionaryEnumerator.NextKey` restarted from the beginning after reaching the end, and `Count` /
    `Position` reset to 0 there (setting `Position` back threw `NullReferenceException`);
  - loading a value type bigger than 16 bytes through a property with a custom getter or setter threw
    `NullReferenceException`;
  - IL emit relations (`UseNoEmitForRelations = false`): `FirstById` / `LastById` with constraints threw
    `InvalidProgramException` and `[OnBeforeRemove]` with container resolved parameters threw `ArgumentException`.
- ObjectDB collections and field handlers:
  - `IOrderedSet` and `IRoaringBitmap` enumerators restarted from the first item when `MoveNext` was called again
    after the end;
  - `IOrderedSet` with a tuple key such as `(int, byte[])` failed on reopening the database;
  - IL emit: upgrading `byte[]` items to `ReadOnlyMemory<byte>` / `ByteBuffer` and `IOrderedDictionary` with
    `ReadOnlyMemory<byte>` keys or values threw.
- Key value DB:
  - `InMemoryKeyValueDB` cursors stayed valid after a failed `FindFirstKey`, `FindLastKey` or `Find`, `FindKeyIndex`
    with a prefix could return a key outside of the prefix, an invalid cursor did not start from the last key on
    `FindPreviousKey` after another cursor wrote, and `FastIterate` threw after its last key was erased;
  - `BTreeKeyValueDB` cursors restarted `FastIterate` from the first key, threw from `SetValue` and returned a wrong
    `KeyHasPrefix` after another cursor of the same transaction wrote;
  - `BTreeKeyValueDB.IsCorruptedValue` reported values written in the current transaction as corrupted;
  - `KeyValueDBExportImporter.Import` / `ImportAsync` failed on streams returning fewer bytes than requested (e.g.
    `GZipStream`);
  - one `DiskChunkCache` file with a truncated header made the whole cache unloadable.
- Event store:
  - a cyclic object graph overflowed the stack in `TypeSerializers` and event store v1;
  - struct properties bigger than 16 bytes (e.g. `decimal?`, `Guid?`) with a custom getter or setter threw
    `NullReferenceException`;
  - event store v2 could not store `System.Tuple<>`, tuples with enum items, or re-serialize dynamically loaded
    objects, and in IL emit mode corrupted memory for value tuples with class items;
  - reading tuples with unknown or unregistered item types and `decimal?` / `Guid?` / tuple nullables as dynamic
    objects threw; setting a member of a dynamic object or tuple threw `InvalidCastException`.
- Streams, collections and compression:
  - `ReadOnlySequenceMemReader` read values split across segments wrongly and `ReadOnlyMemoryMemReader` did not
    advance after `ReadByteArrayAsMemory`;
  - `BrotliDecompressMemReader` reported wrong positions, overwrote reads bigger than 64 KiB and threw at the end of
    a source without trailing data; `BrotliCompressMemWriter` reported wrong positions after 64 KiB;
  - `MemWriter.WriteStringInUtf8` threw when a multi-byte character crossed a buffer boundary of a stream backed
    writer; on corrupted data `ReadString` / `SkipString` could read past the string instead of throwing
    `InvalidDataException`; `CheckMagic` consumed data on end of stream;
  - `LruCache` and `SpanByteLruCache` lost entries after `IncreaseCapacity` or `Clear` following a `Remove`, and
    `SpanByteLruCache` lost keys after compaction;
  - `EquatableArray.Equals(object)` overflowed the stack, `StructList.AddUnique(null)` threw, an empty
    `Utf16Utf8Dictionary` threw on lookup and `ByteStructs.Key32` read its last field from a wrong offset;
  - `FastBehaviourSubject.Subscribe` could deliver the current value twice when the observer re-entered the subject;
  - Snappy decompression of literals with a 4-byte length and compression of empty input were broken.
- `IRelation.Query<T>()` ignored the `StringComparison` of `string.Contains(char, StringComparison)`, so for example
  `OrdinalIgnoreCase` did not match a differently cased character.
- `IRelation.Query<T>()` threw `InvalidCastException` for `Contains` on a collection of `bool`.
- `IRelation.Query<T>()` equality conditions on `decimal`, `DateTime` and `DateTimeOffset` primary or secondary key
  fields searched for the exact stored key bytes, so values equal in C# but differing in decimal scale, `DateTime.Kind`
  or offset (for example `0.250m` and `0.25m`) were not found; such conditions are now tested on every row.
- `RawData.GetSizeAndAlign` guessed the alignment of structs from their size, so `DateTimeOffset?` fields of relations,
  objects and events (and nullables, value tuples and key value pairs of other structs whose size is not their
  alignment) were saved and loaded at a wrong offset: they could not be read back, typically failing with "Offset must
  be within plus or minus 14 hours". Values of such fields saved by earlier versions are not readable.
- On x86/x64, `MemReader.ReadStringOrdered`, `ReadStringOrderedAsSpan` and `PackUnpack.ReadAndExpandSimpleCharacters`
  wrote past the end of a character buffer shorter than 16 characters and lost characters (read as `\0`) when it grew;
  their vector stores also wrote through an unpinned pointer into heap buffers.
- IL: the caching IL builder treated `0.0` and `-0.0` constants as equal, and `Newobj` with a non-public
  constructor in a dynamic type threw `MethodAccessException`.

## 35.12.0

### Added

- `ReadOnlyKeyValueDB` is finished: an immutable `IKeyValueDB` in one block of memory, opened from a memory mapped file
  (`ReadOnlyKeyValueDB.OpenFile`), pinned memory (`ReadOnlyKeyValueDB.Create`) or an address. `ReadOnlyCreator` writes
  any transaction into it (`WriteToFile`, `WriteToArray`, `Run`). All cursor operations, including O(log n) key index
  access, `FastIterate` and `ObjectDB` reading, are supported. Its unfinished previous version threw
  `NotImplementedException`; `ReadOnlyKeyValueDB` is now sealed, its transaction type is no longer public and the
  format version 1 changed. See [Doc/ReadOnlyKeyValueDB.md](Doc/ReadOnlyKeyValueDB.md).

### Fixed

- Fix `UnmanagedMemoryManager.Pin` failing a debug assertion for every valid element index, which terminated Debug
  builds that pinned memory created from it.
- Fix `AesGcmSymmetricCipher.OrderedDecrypt` returning a garbled first 16 bytes for every value after the first one,
  because the CBC chain was not reset between values. Reading `OrderedEncryptedString` values now returns the stored
  text; the stored data was always written correctly. Ordered encryption and decryption also no longer allocate.
- Fix `BTreeKeyValueDB` cursors returning an empty key or value from `GetKeyMemory`, `GetKeySpan`,
  `GetValueMemory`, `GetStorageSizeOfCurrentKey` and `IsValid` after another cursor of the same transaction wrote a
  key; they now find their current key again, as `GetValueSpan` already did. A cursor whose key was erased still has
  no current key.
- Fix `UpdateKeySuffix` in `BTreeKeyValueDB` doing nothing (while reporting `Updated` and logging the change) or
  throwing when the key was the only key in its tree leaf, for example the only key in the database.
- Fix `UpdateKeySuffix` in `InMemoryKeyValueDB` corrupting the tree when it changed the first key of a leaf: later
  finds missed that key and `CreateOrUpdateKeyValue` could insert it a second time. The cursor also kept pointing to
  the old leaf, so it returned the old key.
- Fix relation `LastBy…`/reverse scans skipping a matching row when an `Exact` constraint value was smaller than
  another value stored for the same preceding key fields.
- Fix reusing one `Constraint` instance in several relation queries at once, which made the queries match against
  the wrong bytes. Constraints now keep their encoded value themselves.
- Fix `RoaringBitmap.BuildAsync` looping forever on nested `And`/`Or`/`Not` operations whose inputs have different
  pages, for example `And(And(a, b), c)`.
- Fix `BonSerializer` deserialization skipping two or three values when one field could not be deserialized (for
  example a `null` string or list), so the following fields got wrong values; `null` values inside `object` fields
  and dictionaries were also not consumed.
- Fix reading an event store that contains an empty event batch (`Store(metadata, [])`) throwing
  `IndexOutOfRangeException`, which made the store unreadable and impossible to append to after reopening.
- Fix `BTreeKeyValueDB` transactions disposed with a forgotten cursor: `Dispose` threw before releasing the
  transaction, so a writing transaction blocked all later writers forever, and the finalizer of a leaked transaction
  with an open cursor terminated the process. The transaction is now released first and its cursors are disposed;
  `Dispose` still reports the forgotten cursor and the finalizer reports the leak through the logger.
- Fix a failing commit in `BTreeKeyValueDB` leaving the writing transaction registered, which made every later writer
  wait forever. A commit failing before its commit command is written is rolled back and other writers continue. A
  commit failing later (for example `HardFlush` throwing an `IOException`) may already be in the transaction log, so
  writing is disabled with a `BTDBException` until the database is reopened, while reads keep working.
- Fix `ReadOnly` `BTreeKeyValueDB` accepting writes through `StartTransaction`, and running `Compact` and `CreateKvi`,
  which could append to or create database files.
- Fix IOC resolving hanging forever after a singleton or scoped registration failed to build once (for example because
  of a circular dependency); every resolve now throws again. A concurrent resolve could also cache a `null` factory and
  make the type unresolvable.
- Fix the relation query interpreter ignoring exceptions thrown inside its stack allocation blocks, so a failing
  predicate evaluation kept or dropped rows based on a stale result.
- Fix event store readers (`EventStoreManager` and `EventDeserializer`) numbering back references differently from
  writers when a stored value is a string, boxed value, list or dictionary in an `object` field or the event itself;
  reading such data threw or returned wrong shared instances. Already written data is now read correctly.
- Fix upgrading a relation whose primary key contains a `Tuple` or `ValueTuple`, which threw `ArgumentException`.
- Fix `BonSerializer` writing the values of properties with explicit setters into unrelated memory instead of the
  deserialized object.
- Fix saving an `IList<T>` backed by `Collection<T>` over an array in ObjectDB and event stores, which read the array
  length from the wrapper and could crash the process.

## 35.11.3

### Added

- `BTDB.LSM` and `BTDB.LSM.Azure` preview packages: `LsmKeyValueDB`, an `IKeyValueDB` implementation that stores a
  log-structured merge tree directly in object storage (Azure Blob Storage through `AzureLsmStorage`). Memory is
  bounded by the block cache and memtables and does not grow with the database; there is no local disk copy. One
  writer holds a blob lease with epoch fencing and failover; readers in other processes see consistent snapshots and
  catch up with `RefreshAsync` or `WaitForCommitUlongAsync`. Commits are non-durable by default and
  `WaitForDurabilityAsync` waits for the WAL upload. Bloom filters default to 16 bits per key, because on object
  storage a false positive costs a ranged GET; this makes random-key bulk loads about 5 times faster than with 10.
  `FileSystemLsmStorage` stores a database in a local directory shared by processes on one machine (development and
  tests). `LsmBlockCache` can be shared by several databases; `WorkerFailureTimeout` bounds how long `CreateKvi` and
  `Compact` wait for failing background work.
  See [Doc/LsmKeyValueDB.md](Doc/LsmKeyValueDB.md).

### Fixed

- Fix lost `SetValue` in `BTreeKeyValueDB`: when copy-on-write copied tree nodes for one cursor's `SetValue`, other
  cursors kept pointers to the old nodes, so a later `EraseCurrent`/`EraseUpTo` through such a cursor rewrote the leaf
  from its pre-`SetValue` copy, and reads through it returned the old value. Other cursors now re-read their node
  path after such a write. `ICursor.WriteValue` returns whether nodes were copied and `ICursor.RefreshNodes` was added.
- Fix `InMemoryKeyValueDB.SetValue` modifying tree nodes shared with older read-only snapshots, which made
  uncommitted values visible there. Cursors also no longer throw or crash after another cursor's first write
  made the transaction writable.
- Fix cursor key index tracking in `BTreeKeyValueDB` and `InMemoryKeyValueDB`: `CreateOrUpdateKeyValue` after
  `EraseCurrent` no longer reports a key index one too low; `UpdateKeySuffix` returning `NotFound` or
  `NotUniquePrefix` now invalidates the cursor instead of leaving a stale key index; and a cursor whose key was
  erased by another cursor stays between the same neighbouring keys when other keys are inserted next to it.

## 35.11.2

### Fixed

- Fix advanced relation enumeration with an excluded lower bound: when the boundary row was deleted or never
  existed, keep the first following row instead of skipping it. Excluding a partial key now skips every row with
  that prefix. This applies to primary and secondary indexes in both enumeration directions.

## 35.11.1

### Added

- Add `ReplicationPresence`, registered by `AddBTDBReplication`: each node announces one value to the selected
  replication leader, which aggregates the view in memory and returns it to followers with `LeaderNodeId` and a
  `Settling` flag. Followers exchange presence once per `ReplicationNodeOptions.PresenceInterval` (default 1 s) as a
  separate peer request, so a leader without presence support never disturbs replication. Presence makes no
  shared-storage requests and needs no synchronized clocks.

- Add `ReplicationLeadership.RunWhileLeaderAsync` for application schedulers: run once per activated local leader
  session, cancel on fencing, conservative lease expiry, handoff drain or shutdown, and expose callback failures
  through the subscription task. HTTP hosting registers the node-scoped service automatically; callback execution
  and cleanup do not block replication, and successive callbacks in one subscription never overlap.

- Add the `BTDB.Replication.EventSourcing` preview package: typed event dispatch with synchronous validation,
  separate ordered Layer2 data/metadata topics, ambient ObjectDB access, local result waits, per-event native virtual
  batching, replay from the restored database cursor and replication-progress integration. Release automation
  includes the new package. Failed handlers roll back their event and stop consumption without skipping it.
  `Ambient.RegisterPostCommit` runs actions after the entire batch becomes visible, suppresses replay and discards
  rolled-back registrations. Optional shared-storage consumer heartbeats expose self, selected leader and live node
  IDs through `Ambient.Cluster`, exclude startup replay and expire crashed sessions. `EventPublisher.Completion` and
  pending publications report the first (root-cause) fault, leader records are validated like replication discovery,
  and `StartQuery` returns `Ambient.Scope`.

- Add the `EventLogBench` multi-node benchmark: a node host (EventLog over HTTPS peers and Azure Blob Storage plus a
  control API) and a driver that measures open-loop latency and closed-loop throughput of raw EventLog topics and of
  event-sourcing consumption into ObjectDB, joining publications and deliveries of all nodes on clock-offset-corrected
  time. Kubernetes manifests and a container recipe are in `EventLogBench/deploy`.

- Add `Releaser prepare [major|minor|patch]` (patch by default) to build the complete NuGet package family and symbols
  for the next release without publishing, reading credentials or changing tracked versions, the changelog or Git.

### Improved

- `EventCluster` membership runs over `ReplicationPresence` instead of shared-storage heartbeats: the constructor
  is now `EventCluster(nodeId, replicationPresence)`, `EventClusterSnapshot` reports `Settling` instead of
  `SampledAt`, and `EventClusterOptions` keeps only `RequestTimeout` and `NewSessionId`. An idle three-node cluster
  no longer writes and lists membership blobs every 5 seconds per node (about $40 a month on GZRS Hot).

- An idle EventLog owner skips its background merge pass, and so the topic listing, while it committed nothing since
  the last pass and no deletion deadline is due. An idle topic now costs only the owner's heartbeat validation.

- `EventPublisher` no longer stops on a deterministic `Consume` exception: the event's writes roll back, only its
  cursor commits, its local publication fails with the exception and `OnConsumeFailed` reports it. Exceptions that
  `EventPublisherOptions.IsFatalConsumeException` classifies as fatal (by default out of memory, insufficient stack,
  cancellation, I/O, timeouts) still stop the publisher for a restart.

- Add `EventPublisherOptions.Poison` (`EventPoisonOptions`) to skip events whose consumption never completes, such as a
  process crash on stack overflow or a repeating fatal exception: attempts of the first events after each start are
  recorded in shared storage, and an event exhausting `Attempts` (default 2) is skipped on every node through a
  decision the active leader records in leader.json (`applicationData.btdbEventSkips`), read once at start. After the
  attempt window consumption waits until the window is canonical, so restarts keep progressing when the restored
  cursor lags; decisions it covers are pruned. `EventPublisher.SkipEventAsync` records an operator decision.

- Add `ReplicationDatabaseStatus.Canonical`, this node's canonical TRL cut on every role, and
  `ReplicationStatus.WaitForCanonicalAsync` to wait until a completed local cut would survive a restart.

- `EventLogOptions.MaxRecordSize` defaults to 51 MiB (was 16 MiB), admitting 50 MiB payloads.
  `HttpEventLogPeerTransport` without an explicit limit sizes its messages for that default.

- Add `EventPublisherOptions.MaximumEventSize` (default: the EventLog default `MaxRecordSize`): a larger serialized
  event fails with the new `EventTooLargeException` before its data publication.

- `EventPublisher` deserializes events in its data reader before the `BufferedEvents` queue, overlapping
  deserialization with consumption of earlier events, and no longer copies record payloads. The new
  `EventPublisherOptions.BufferedBytes` (default 64 MiB) also bounds that queue and the batch being consumed by
  serialized event bytes; one event always fits.

- The EventLog owner bounds each live subscriber's queue by `EventLogOptions.SubscriberBufferBytes` (default 64 MiB,
  one message always fits) besides its 1024 messages; a subscriber falling further behind catches up from storage.

- The EventLog owner now waits briefly before each storage write when publishers answer commits, so records sent
  right after the previous commit, often from other nodes, share the write instead of waiting for the next one. By
  default it chooses the window itself, up to `EventLogOptions.AdaptiveCommitLingerMax` (3 ms), from when records
  arrive after the moment a write would start: it opens only while that pays off and stays closed for independent
  traffic. `EventLogOptions.CommitLinger` sets a fixed window instead, and `AdaptiveCommitLingerMax = TimeSpan.Zero`
  with no fixed window restores immediate writes. A window ends early once the queue fills the next batch; windows
  below 2 ms yield instead of using a timer. `EventLogService.CommitLinger(topic)` reports the window in use.

- EventLog publishers pipeline publication rounds: a node keeps up to `EventLogOptions.MaxPublishRoundsInFlight`
  (default 64) rounds per topic in flight instead of waiting for each round's commit, so records published during the
  owner's storage write join its next write. Rounds carry the true oldest unresolved sequence and are marked previously
  dispatched when an older unresolved record was, so the owner keeps call order and exactly-once reconciliation.

- When queued records exceed the remaining room of the EventLog owner's writable split, the batch now fills that room
  and seals the split in the same write. Previously the whole batch went to a successor after a seal-only write that
  rewrote up to 256 KiB without carrying a record, which pushed closed-loop load into small batches and extra
  latency. Together with pipelining this cut p50 publish latency at 1,000 records/s on AKS with GZRS
  storage from 27–29 ms to 13–15 ms and raised throughput at 128 publish loops per pod 1.7–2.4×
  (`BTDB.Replication/Measurements.md`).

### Fixed

- Preserve nested dictionary values when upgrading stored `IDictionary` fields and their nested layers to `Dictionary`.
  Select the conversion path once when creating the converter, without per-entry conversion checks.
  Support value conversions including numeric, nullable, boxed and custom struct values, and handle empty dictionaries.

- An EventLog topic whose owner restarted at the same endpoint or vanished while idle no longer leaves every follower
  polling storage (List and Get Blob up to five times per second per node and topic). A node following the topic live,
  or the restarted node when a peer contacts it, takes the topic over, so an idle topic again costs only the owner's
  heartbeat validation. Publications from other nodes to such a restarted, idle owner no longer retry forever.
  `EventLogOptions` now requires `OwnerTimeout` to exceed `HeartbeatInterval`.

- EventLog publication rounds sent to a remote owner carry at most `MaxRecordSize` record bytes. Several large
  concurrent records from a follower no longer exceed the peer message limit and retry forever as a failing peer.

- The HTTP EventLog transport raises the server's request body limit (Kestrel: about 30 MB) to its peer message
  limit, so submissions of records above 30 MB from followers are accepted.

- Document installation and the included EventLog across all three replication preview packages; fix package README
  links and stale statements that HTTP hosting is non-packable or release packaging remains unimplemented.

## 35.11.0

### Added

- Release automation versions and publishes all three replication preview packages together with BTDB, checks
  child-process exit codes, and pushes the release tag explicitly.

- Start the `BTDB.Replication.EventLog` event log (preview, in progress; see
  `BTDB.Replication/EventLogImplementationPlan.md`): public `IEventLog`/`IEventTopic` contracts for named topics of
  opaque records, the `IEventLogStorage` object-storage contract, and the validated binary split, frame, seal and
  merged-object format. The per-topic owner lane appends batches with one conditional write per commit, rotates
  splits with proactive seals, writes large records as their own sealed split, repeats ambiguous writes until
  resolved, takes over a tail by its ETag and deduplicates publisher sessions so a record commits exactly once.
  `EventLogService` runs one node: publications are forwarded to the topic owner through `IEventLogPeerTransport`
  in call order, an unreachable owner is taken over after `OwnerTimeout`, followers receive live batches and
  validated heartbeats from the owner instead of reading storage, and `GetBoundsAsync` is fresh.
  `AzureEventLogStorage` stores topics as Block Blobs with one conditional Put Blob per commit (staged blocks above
  the single-request limit), owner metadata installed atomically, and delayed conditional deletion.
  `AddBTDBEventLog`/`MapBTDBEventLog` host a node over HTTP (`/_btdb/eventlog/v1`, bearer API key, streamed
  subscriptions with buffering disabled) through `HttpEventLogPeerTransport`.
  The owner merges sealed splits in the background into immutable level-1 and level-2 objects (fan-out 16, named
  `l{level}/{firstOffset}.elog`) whose headers index every record, and deletes covered objects after
  `DeletionDelay`; readers locate offsets from the listing and read merged objects from the containing frame.
  `DBBenchmark eventlog-storage` reruns the Azure storage-primitive screening, and `DBBenchmark eventlog-e2e`
  measures open-loop publish-to-all-nodes latency over loopback HTTP with in-memory or Azure storage.

- Qualify a rolling schema upgrade under continuous input in the subprocess tests and document the procedure: the
  lagging upgraded leader's activation deadline restarts it onto the frozen published history, and the selected
  generation keeps old builds from leading meanwhile.

- Prepare `BTDB.Replication`, `BTDB.Replication.Azure` and `BTDB.Replication.Http` as NuGet packages, versioned
  `{BTDB version}-preview` with their READMEs, and mention replication in the README.

- Qualify replication with a sample ObjectDB application in the subprocess tests: rollbacks inside virtual batches
  replicate byte for byte and survive failover and cold restore, and an upgraded leader's schema commit detaches old
  nodes while upgraded replacements restore it. Record the clock qualification of `SystemReplicationScheduler`.

- Add replication recovery counters: `ReplicationStatus.RestoreAttempts`, `RestoreFailures`, `RestoreDuration`,
  `LeaderSessions`, `LostLeaderSessions` and `FailedSteps`, exported by the `BTDB.Replication` meter as
  `btdb.replication.restore.*`, `btdb.replication.leader.sessions*` and `btdb.replication.step.failures`.
- Validate lease-dependent replication settings when the first lease is acquired: a drift bound and safety margin that
  consume the whole lease, or a `RequestTimeout` or `ConfirmationDuration` of at least half the usable lease, stop lease
  maintenance with `InvalidOperationException` instead of silently never holding authority or never granting.

- Add `SystemReplicationScheduler`, the production `IReplicationScheduler`: lease deadlines use a clock that time
  synchronization never slews and that keeps running while the process is stopped (`CLOCK_MONOTONIC_RAW` on Linux and
  macOS, interrupt time on Windows), with timer callbacks serialized on the thread pool. `AddBTDBReplication` registers
  it unless the application registers its own scheduler. `DBBenchmark replication clock` compares the Linux clocks.
- Qualify more subprocess scenarios on Azurite and live Azure: planned upgrade handoff, storage and peer partitions of
  a leader or follower, a kill during restore and partial multi-database activation.

- Qualify the Azure adapter against live Azure: `BTDB.Replication.Azure.Test` and `BTDB.Replication.Process.Test`
  target the account named by `BTDB_AZURE_BLOB_ENDPOINT` through `DefaultAzureCredential` (Azurite stays the default),
  and `AzureQualificationTest` covers lease expiry against the local deadline, stale in-flight requests after takeover
  and restores concurrent with publication and cleanup. Results are recorded in `BTDB.Replication/ObjectStorages.md`.

- Add `OnDiskReplicationFileStorage`, durable node-local replication storage with one `{id:D8}.{hint}` file per ID in a
  directory, and the `IReplicationFileStorage` contract it shares with `InMemoryReplicationFileStorage`.
  `ReplicationFileSet` now accepts any `IReplicationFileStorage`. A file being appended keeps its unmapped tail in
  pinned 1 MiB blocks, reused through a small pool, and its persisted prefix in a read-only mapping that grows in
  8–64 MiB steps; sealed and never-appended files are mapped once. Readers copy from an immutable snapshot without
  locks: a mapping stays alive until a concurrent read that started before its replacement or `Remove` finishes, and a
  reused block's generation makes a reader copy again. Sealing does not fsync, so TRL rotation never waits for the
  device. On Azure E-series VMs with four concurrent readers it sustains 3–14x the commits and 2.1–4.5x the reads of
  `OnDiskFileCollection`. Restore discards local tails lost by a crash through remote cache validation.

- Seal canonical TRLs with a whole-file SHA-256: the publication write that completes a TRL with a native successor
  records `btdb_sha256` (`TrlWrite.Sha256`, `TrlObjectState.Sha256`), `CanonicalTrlInventory` reports it for sealed
  TRLs, and restore reuses cached sealed TRLs instead of downloading them again; only the active tail is fetched.

- Make `TransactionLogPosition` comparable (`IComparable<TransactionLogPosition>` and `<`, `>`, `<=`, `>=`),
  ordered by file ID and then offset.

- Add `TransactionLogCapture.NonApplicationCommitted`, the end of the latest committed transaction that left
  `CommitUlong` unchanged, recorded while replaying the opened TRL and on local commits (rollbacks excluded).

- Add `ReplicationNodeOptions.DetachedLeaderTimeout` for the time a schema-detached node waits without leader
  evidence before requesting a restart (default 15 minutes, previously hard-coded).

### Changed

- Replicated `BTreeKeyValueDB.OpenAsync` no longer rewinds a transaction that a previous leader published only
  partly across TRL files and regenerates it in place. It keeps the published bytes and ends the transaction with a
  native rollback at the start of the successor of the last published TRL. Every node restored from that history
  writes the same terminator, and the application input then executes the transaction again after it. Takeover no
  longer depends on reproducing the abandoned bytes, so an unfinished transaction that no candidate could regenerate
  identically (for example after a changed or nondeterministic execution) no longer blocks leadership forever.
  `ReplicationRestoredPosition` is then the published end of the last TRL. Unfinished legacy tails are still rewound.
- Merge the replication leader-storage contracts `ILeaderRecordStorage`, `IReplicationLeaseStorage` and
  `IReplicationLeaseTransferStorage` into `IReplicationLeaderStorage`; lease transfer is now required instead of a
  runtime capability. Merge `IReplicationFatalRecovery` into `IReplicationNodeHost` as the required
  `RequestFatalRestart`. Hosts register one leader-storage service. Breaking for replication preview adapters and hosts.

- Simplify replication internals: keep each follower's database, comparison and reader in one map, derive cache
  readiness from its shared verification/download task, share file SHA-256 calculation, and remove redundant TRL
  publication state.

- Replicated `OpenAsync` loads the KVI (typically a fifth to a third of the database) while it downloads, or while a
  parallel task computes its SHA-256 for a cached copy, and accepts the load only after that verification; a cached
  copy that fails is discarded and the downloaded KVI loads instead. The TRLs it replays prefetch as soon as its header
  is read and each referenced file as soon as the load reaches it. `ReplicationFileSet.InitializeAsync` no longer
  hashes the cache: it removes only candidates ruled out by metadata, the first prefetch verifies the rest, and
  `GetFile` hides unverified cached copies. Measured on a KVI-heavy 10 GiB database: cold restore 28–31 % and warm
  23–25 % faster. Add `IStreamingFileRead`, `IFileReplicatedCollection.StartStreamingRead` and
  `IFileReplicatedCollection.DiscardLocalFile` (removes an unused file's local copy even when unverified).

- `AzureReplicationStorage` stages up to four TRL blocks at once; staging one block at a time capped canonical TRL
  publication at about 110 MiB/s on Azure.

- Followers request up to 4 MiB of inline TRL bytes per poll (the leader maximum, previously 1 MiB) and retain up to
  8 MiB per database, so a follower trailing by less than one poll still gets the next poll's bytes inline. Measured:
  1 MiB cost several range round trips per poll above about 20 MiB/s of TRL.

- Replication activation validates every database before adopting any canonical tail, so a candidate lagging on one
  database no longer repeats adoption writes (and rediscovery) of the databases before it on every retry.
  `CanonicalTrlPublisher` no longer sends a zero-length CAS of a fully published tail when publication continues into
  its successor TRL, saving a Blob commit on every TRL rotation.

- Store TRL, PVL and KVI directly under the database prefix as `{fileId}.{extension}`, without a `files/` subdirectory.
  Share fixed `{fileId}.trl` Blob keys across every term. Remove term, successor and recovery-root metadata;
  only `btdb_sha256` and `btdb_delete_after` remain. Conditional create collisions compare native bytes, reuse identical
  content or extend matching prefixes with CAS, and request restart on divergence. Tail adoption changes only ETag.
  Publish native files in order; after a crash, replay complete transactions and regenerate an unfinished local suffix
  with identical IDs and bytes. Hosts obtain the complete startup boundary from `ReplicationRestoredPosition`.
  Earlier term-scoped names and protocol metadata are not supported.

- Create replication TRL successors at exactly the previous TRL ID +2, independent of retained files and allocation
  counters. Validate size policies before allocating and reject exact-ID collisions instead of skipping IDs.
  `IFileReplicatedCollection` implementations must implement `CreateTransactionLogFile(fileId)`. Legacy conversion
  now runs automatically during writable open: conditionally publish a header-only successor selected from remote
  inventory, compare concurrent creates, and preserve the transition across restart without an application commit.
  Remove `AllowLegacyReplicationTransition`; custom collections publish via `PublishTransactionLogHeaderAsync`.
  Native generation zero preserves the +2 rule after restart.
  Retry stale inventories that omit an already published successor instead of creating a second transition ID.

- Read replication file headers from verified local cache entries, including completed prefetches, instead of issuing
  redundant remote range reads. Reading 100 cached PVL headers now needs zero remote reads instead of 100; evicted or
  replaced local files still fall back to the selected remote version.

- Simplify follower comparison to one `CompareAsync(progress, cancellation)` call per polled database. Remove the
  separate progress notification state and redundant comparison semaphore; the coordinator already serializes calls.
  Closing a session still cancels outstanding reads and prevents late comparison results from being accepted.

- A steady-state replication checkpoint no longer needs a request per retained remote file. The checkpoint's ID scan
  uses the maintenance listing, so reused PVLs listed without a deletion mark skip `ProtectPureValuesAsync`, and the
  Azure adapter prepares a KVI from one namespace listing (recovery-root hint, selected links and deletion marks)
  instead of a second listing and a HEAD per canonical TRL. On Azurite with 20 PVLs and 153 retained TRLs this cut a
  checkpoint from 4 listings and 173 HEAD requests to 3 listings and 1 HEAD.

- Detect schema transactions on the leader instead of decoding leader TRL on followers. Each poll answer now carries
  the leader's latest non-application commit position (a new flag in the binary peer poll response), and a follower
  whose canonical base precedes it detaches. `SchemaTrlScanner` is removed; it ran before every comparison and
  scanned small transactions at about 4 MB/s because it moved its 256 KiB buffer on every native command.

- Write no `TemporaryEndOfFile` or `EndOfFile` markers in replication mode: not for
  `NextCommitTemporaryCloseTransactionLog` (which still forces a writing commit), on dispose, or when rotating to a
  new TRL. Replication reopens a tail at its exact committed end and follows `PreviousFileId` and canonical links
  across files, so the markers only added bytes to the replicated TRL stream. Standalone databases are unchanged;
  replay still accepts both markers.

- `ReplicationFileSet` initialization maps every selected remote file to its own ID and forgets earlier placements.
  A copy cached under another local ID is removed and downloaded again under the remote ID, and prefetch never
  looks for a copy under another ID.

- Download remote files through a sliding window of four in-flight block reads instead of waiting for each batch of
  four, and verify the whole-file SHA-256 of sealed downloads before they establish a PVL placement.

- List the remote inventory once per checkpoint instead of once per allocated PVL/KVI identity, and skip re-marking
  remote files whose deletion deadline is already listed during cleanup.

- Transfer follower TRL bytes from the leader once per step: the schema scan and the byte comparison share fetched
  ranges, comparison reads 256 KiB (the HTTP maximum) instead of 64 KiB, crossing TRL files walks the lineage once
  instead of from the advertised end at every rotation, and resumed schema scans do not reread the file header.

- Batch follower polls: one request per step carries every compared and newly added database with a single
  challenge and grant, and also serves as the detached-node heartbeat, instead of one request per database plus a
  separate heartbeat. The leader returns the TRL bytes each follower's schema scan and comparison need inline with the
  poll (up to 1 MiB), so a caught-up follower makes no range reads.

- Replace the JSON replication peer protocol with a compact versioned binary encoding for every operation; poll
  responses carry inline TRL bytes that the client slices without copying, and bodies with a declared length are read
  directly into an exact buffer. The leader reads inline bytes straight into their chunk arrays and writes them to the
  response without assembling one buffer; peer key comparison uses a stack buffer for ordinary key lengths.

- Remove the unused per-follower confirmation windows; leader liveness is tracked by the single grant of each poll.

- Publish canonical TRLs of all databases concurrently, so one database's large upload no longer delays the others,
  and keep up to four Blob reads in flight while a new leader validates its local history against canonical Blob
  history.

- **Breaking:** remove `RefreshRemoteInventoryAsync`, `PublishPureValuesAsync` and `GetRemoteCount` from
  `IFileReplicatedCollection`; BTDB never called them. Refreshing a leader's inventory could also fail on a TRL it had
  created and published itself. PVL receipts are still revalidated when a checkpoint protects them.

- **Breaking:** `ReplicationMaintenance` requires a positive deletion delay (production assumes at least a day), and
  `AzureReplicationStorage` requires a nonempty prefix per database, because cleanup lists everything below it.

- Skip re-exporting an identical checkpoint when the TRL cut and source files are unchanged since the last published
  one; cleanup still runs so earlier deletion marks become due. An idle database previously uploaded a new KVI and
  rewrote every PVL's metadata at each maintenance interval.

- Hash cached files in parallel during `ReplicationFileSet` initialization (1 GB from disk: 2.7 s serially, 1.1 s with
  four validations), download restore ranges of 4 MiB instead of 256 KiB (Azurite, four 128 MB files: 1.40 s to 1.00 s),
  and stage Azure PVL/KVI blocks up to four at a time (128 MB PVL on Azurite: 595 ms to 330 ms).

- Azure canonical appends reuse the block list of the adapter's own previous commit instead of reading it again, so
  a publication needs one request fewer.

- Followers learn a TRL file's end from inline poll bytes that continue in its successor, so crossing a rotation needs
  no end-of-file range read and the next poll no longer resends bytes of the next file that are already retained.

- Reconcile an ambiguous canonical TRL write by comparing only its appended bytes; the expected version's prefix is
  unchanged by a successful CAS.

- Simplify follower comparison: its start position is mandatory, the comparer no longer acknowledges capture or takes
  its own lane, and test-only constructors are gone. `CanonicalTrlInventory` keeps its chain in explicit order,
  leadership activation stops republishing completed preparation cuts every step, one `ReplicationNodeOptions`
  validation serves both the coordinator and hosting, and the Azure adapter parses file names in one place.

- Remove unused internal replication helpers (`TrlPublication`, `TrlSnapshot`, `IReplicationRandom`,
  `ReplicationFileSet.RememberVerifiedPureValues`) and move the in-process peer transport into the test project. PVL
  reuse placements now come only from restore validation, downloads and confirmed uploads.

### Fixed

- Source generator registers dispatcher methods that a handler inherits as default interface implementations,
  calling them through the interface. Previously such handlers had no entry for that dispatcher, so dispatching
  failed at runtime with "No handler for message ...".
- `OnDiskReplicationFileStorage` releases its read-only mapping before truncating a rewound file, because Windows
  refuses to shorten a file with a mapped view. Rewinding uncommitted transaction logs below the mapped prefix, then
  flushing, sealing or disposing the file, no longer fails with "a user-mapped section open". Readers of that
  prefix wait for the file to be mapped again.
- Cancel and drain sibling activation range reads immediately when any read fails, preserving the original failure
  instead of waiting indefinitely for an earlier blocked range before retrying activation.
- Fence replication leadership and request restart as soon as any database reports a canonical publication conflict,
  without waiting for other databases' publication calls. Cancel outstanding publications and drain them before
  releasing native sources, including when a provider ignores cancellation.

- Let a replication leader restored from Blob serve its restored history to followers before its first local
  commit. Its capture reported no complete prefix, so every follower read failed until new input arrived and followers
  could not compare or release their local TRLs. Found by the new seeded schedule exploration
  (`RandomSchedulesKeepOneAuthorityAndConvergeOnOneHistory`). Hosts should report the restored cut from `GetProgress`
  until their first local transaction.

- Keep replication restores working while the leader publishes and cleans up. A restore failed with `412` whenever
  the leader appended to the selected TRL tail, or cleanup marked or unmarked a selected file, so under continuous
  publication or frequent checkpoints restores retried indefinitely. `CanonicalTrlInventory` now continues reading a
  selected TRL from any newer version that is at least as long (canonical TRLs are append-only), and
  `AzureReplicationStorage` continues reading a PVL/KVI from a newer version with the same length and `btdb_sha256`.
- Never open a replication restore with missing values: a checkpoint published between TRL discovery and the PVL/KVI
  listing could reference TRL bytes beyond the discovered tail, and the database opened with corrupted values.
  `ReplicationFileSet` now ignores a KVI whose TRL cut lies outside the selected inventory, so open uses the newest
  checkpoint that existed at discovery.

- Revalidate completed streaming download results against the actual local cache, so prefetch and streaming reads
  download an evicted file again even when the original reader was abandoned before completion.
- Track streamed cache checksums with downloads: concurrent prefetch shares the verification, abandoning or cancelling
  one reader does not cancel other waiters, and collection disposal cancels and drains checksum work before returning.

- Run leader checkpoint maintenance of all databases concurrently. Previously the jobs ran in sequence and the first
  failure ended the round, so a database whose checkpoint kept failing starved every later database of checkpoints and
  remote cleanup, and one long upload delayed the others.

- Detect follower divergence when a sealed leader TRL is shorter than the local file. Compare short reads immediately
  so different-length values cannot hide a byte mismatch behind an endlessly retried truncated-read error.

- Cancel pending parallel replication downloads after a local write failure before draining their buffers, so reporting
  a disk error does not wait for unrelated remote reads to finish or for external cancellation.

- Fence leadership and request canonical restore when initialization/schema publication conflicts. A predecessor's
  delayed genesis can win after empty-root discovery; previously activation kept retrying the permanently conflicted
  publisher while renewing its lease indefinitely unless a progress watchdog was configured.

- Authenticate replication HTTP Bearer keys against the active leader before reading or decoding request bodies.
  Previously only the header format was checked before decoding; full session authentication and revalidation
  after awaited work remain in place.

- Release local TRL retention of removed and schema-detached databases. They keep executing locally but are never
  compared or published again, so their capture acknowledgement stayed put and local compaction kept every later TRL
  until shutdown; the coordinator now acknowledges their complete local history on every step.

- A node that initialized a new database now records its first publication as that database's canonical base.
  After losing the lease it previously treated the database as uninitialized and restarted for a full restore;
  it now follows the next leader.

- `AzureReplicationStorage.ProtectPureValuesAsync` no longer rewrites the metadata of an unmarked PVL. The rewrite
  changed the version of every reused PVL on each checkpoint, which failed concurrent restores reading it by ETag;
  a deletion mark is still cleared (changing the version) as before.

- Compare database names as a set when a candidate selects leadership, as follower discovery already did. Nodes of
  one generation listing the same databases in a different order restarted instead of becoming leader.

- Checkpoint publication on Azure no longer rewrites the metadata (and so the ETag) of every sealed TRL in the retained
  chain. Concurrent restores and activation validation read those TRLs by ETag and failed, redownloading every TRL;
  a checkpoint interval shorter than restore could keep a restore from ever finishing.

- A failed renewal retries within half the remaining lease instead of waiting the whole `LeaseRetryInterval`, so one
  transient error cannot outlast the lease.

- A failed or cancelled `ReplicationFileSet` initialization no longer leaves a partial inventory that a retry would
  keep, including files removed remotely in between.

- Observe every failed database publication in a leader step, not only the first, so the others are not reported as
  unobserved task exceptions.

- Compare a lagging follower's complete local prefix up to the leader's advertised cut. Under continuous load a
  follower is almost always slightly behind, so it previously never compared anything: its canonical base, local TRL
  retention and `Compared` status stopped advancing.

- Keep leader TRL bytes on the follower until both the schema scan and the comparison have used them and poll only
  for newer bytes; a lagging follower otherwise received the same inline bytes (up to 1 MiB) with every poll.

- Let a node of a newer application generation that adds a database follow an older leader that does not select it.
  Polling the unknown database failed every step, so the node disconnected repeatedly and never offered the prepared
  upgrade handoff.

- Drop a confirmed PVL upload placement on `RefreshRemoteInventoryAsync` when cleanup retired its remote object, so
  the next publication allocates a fresh identity instead of recreating the retired key.

- Report a replication download's failed block instead of the cancellation it caused in sibling reads.

- Treat canonical publication as idle when local capture is behind a restored canonical tail, instead of failing
  with "Cannot shrink canonical TRL" and requesting a restart.

- Stop renewing a consumed lease transfer handle when the node later reacquires the replication lease.

- Ignore a replication request timeout that fires after its request already completed. Scheduler disposal does
  not wait for a started callback, so the late cancel could hit a disposed `CancellationTokenSource` and throw
  `ObjectDisposedException` on the timer thread.

- Stop a replication lease session fenced between its validity check and renewal dispatch (expiry, transfer or
  disqualification) from failing lease maintenance; acquire a fresh session instead, and never dispatch an
  acquisition after disqualification.

- Report a malformed `leader.json` (invalid JSON, wrong field types or missing leader session fields) as invalid
  configuration (`InvalidDataException`), which requests a node restart, instead of crashing the host with
  `InvalidOperationException` or `NullReferenceException`.

- Serialize `ReplicationFileSet` file ID allocation and PVL placement updates under one lock; native file creation
  and session PVL remapping could previously race on the same ID sequence.

- Serve leader TRL ranges to peers outside the leader session lock, so polls, fencing and other peer reads no longer
  wait for disk reads.

- Stage only appended bytes for small canonical TRL appends in Azure, merging partial trailing blocks once they
  would reach 4 MiB or 64 blocks, instead of reuploading up to 4 MiB on every publication. Parse checkpoint KVI names
  with invariant, sign-free number parsing.

- Keep failover and follower rechecks working after local compaction removes startup TRLs. Followers track a
  per-database canonical base (bytes compared with a leader that had already published them, or published locally)
  and use it for local TRL retention, rechecks against a new leader, schema scanning and takeover validation, instead
  of the restored startup cut. Leader polls now report the published cut. Comparison and schema scanning with the
  same leader resume after reconnects and cancelled requests instead of starting over.

- Stop remote inventory handles in `ReplicationFileSet` from blocking a thread on synchronous network reads.
  They are metadata-only; `RandomRead` and `GetExclusiveReader` now throw `NotSupportedException`, because the
  database reads remote files only from the local cache after explicit prefetch.

- Let a lease winner whose local execution lags published canonical history catch up under the same lease instead
  of restarting for a full restore. Activation validates the matching complete local prefix, returns "not yet" and
  resumes from the verified position; only divergent bytes or another local continuation still require restore.

- Stop replication leader work from livelocking behind `RequestTimeout`. Canonical validation, publication backlogs
  and checkpoint uploads longer than one request timeout were cancelled and restarted from scratch forever; they are
  now bounded by lease authority and the optional progress deadlines. Checkpoint maintenance runs beside canonical
  TRL publication instead of blocking it, and fatal recovery also cancels outstanding leader requests.

- Report Azure throttling and other transient failures from blob listings, checkpoint file reads, immutable-file
  staging and commit reconciliation as retryable `IOException`s. Previously they escaped as `RequestFailedException`
  and stopped the replication host.

- Follow native TRL continuation instead of numeric parity during leadership validation, follower comparison and
  schema scanning. Odd-ID allocation skips IDs reserved by legacy or abandoned files, so a legacy tail such as TRL 10
  with KVI 11 continues in TRL 13; the successor is now taken from selected links or PreviousFileId headers.

- Continue replicated writes in a restored TRL whose physical EOF is the exact complete committed boundary.
  Previously, restoring a published tail without a local shutdown marker rotated the follower's TRL and made its
  next identical event diverge from the live leader. Preserve existing handling of incomplete/corrupt/sealed tails
  and standalone recovery.

- Defer relation schema initialization and creation callbacks requested by read-only transactions until a writing
  transaction first accesses the relation. Retry deferred initialization after rollback without opening a hidden writer.

- Allocate odd TRL and even non-TRL file IDs from independent sequences in dedicated in-memory replication
  storage, including after importing an exact file ID.

### Changed

- Narrow the replication hosting API before production: `CanonicalTrlPublisher` construction and publication,
  `TrlPublishResult`, `ReplicationMaintenance.RunDueAsync`, `ReplicationFileSet.RememberVerifiedPureValues` and
  `LeaseAuthority.Deadline` are internal. Hosts only pass the coordinator-owned publisher to `ReplicationMaintenance`.
  Storage provider contracts stay public.

- Persist unified TRL/PVL/KVI deletion deadlines in Azure metadata, preserving deadlines across retries and leader
  replacement. Require elapsed marks and matching ETags for deletion; clear marks/version-protect reused PVLs and
  retained TRLs, and reupload missing remembered PVLs at fresh identities. Bind a retained TRL root to immutable KVI
  publication so native restore survives obsolete genesis pruning. Update startup to distinguish retained roots
  from unpublished genesis; cover metadata ambiguity, stale operations and native restore against Azurite.

- Add optional checkpoint/remote-maintenance no-progress deadlines, with retry-stable forward-step tracking,
  cleanup retry continuity and idle/application-event exclusions. Fence authority immediately and use the existing
  delayed fatal restart. Document operator backup recovery and the planned unified deletion-metadata/PVL reuse policy.

- Fence replication authority and readiness immediately on watchdog expiry, then wait a configured fixed delay
  before invoking fatal host recovery. Keep the timer independent of blocked workers and prevent cleanup/shutdown
  from bypassing it. Replace persistent activation-backoff counters and their file/storage API with this delay.

- Expose `ReplicationApplicationData` for version-bound reads and active-leader lease/ETag-conditional writes to
  opaque `applicationData` in `leader.json`. Preserve authority and unrelated fields, reconcile exact lost-response
  intents, and keep concurrent writes from overwriting newer versions. Application event timeouts, skip mechanisms
  and execution/commit arbitration are explicitly outside replication scope; update the architecture and plan.

- Add opt-in replication activation and per-database publication progress deadlines. Repeated retries do not
  extend deadlines; idle input and follower restore are excluded. Fence leases and readiness before invoking
  an explicit fatal-recovery host callback, independently of cancellation cooperation. Cover delayed callbacks,
  progressing canonical validation, stalled providers, and real subprocess termination followed by takeover.

- Add public sampled replication readiness/progress, ASP.NET health-check integration and bounded host-scoped
  readiness/role/sample-age metrics. Clear readiness synchronously on shutdown and distinguish historical byte
  comparison from Blob publication and live confirmation. Add native progress and hosted shutdown regression coverage.
- Extend subprocess failover qualification to POSIX process suspension/resumption across real lease expiry,
  verifying takeover without optimistic-tail reexecution and canonical rebuild of the resumed genesis owner.

- Merge Azure canonical TRL and checkpoint adapters into `AzureReplicationStorage`, and their publication/cleanup
  contracts into `IReplicationStorage`. Bind selected inventories and leadership authority in separate immutable
  session views. Keep `IRemoteFileCollection` read-only and accept it directly for replicated restore.

- Expose public replication application, restore/maintenance and storage contracts, Azure adapters, and ASP.NET
  hosting extensions. Run the subprocess host without friend-assembly access. Keep authority construction/renewal,
  election transitions and peer sessions internal; redact credentials from public leader/lease record strings.
  Document host ownership and provider/clock requirements. Replication projects remain non-packable pending qualification.

- Add real subprocess replication qualification against Azurite: leader process death and lease-expiry takeover,
  publication of a matching optimistic tail without handler reexecution, cold restore and continued HTTP comparison,
  plus process termination for a divergent follower.

- Add internal replication DI and ASP.NET hosted lifecycle integration. Start coordination after Kestrel is listening,
  reject missing peer routes before lease acquisition, fence leadership immediately on host shutdown, and stop the
  host on rebuild requests or fatal worker exits. Keep application databases owned by the host and reject invalid
  or duplicate configuration before startup.

- Add an internal ASP.NET Core/HTTP replication peer adapter for authenticated progress, bounded native TRL reads
  and prepared handoff. Revalidate leader sessions after asynchronous work, propagate cancellation, reject redirects
  and overload, and preserve missing-file recovery semantics. Test real socket transport and native-history comparison.

- Schedule native replication compaction independently on every node and connect leader checkpoint publication,
  delayed dependency-aware remote cleanup and application-owned exact leak-removal events. Preserve pending
  snapshots across upload failures, condition remote deletion on object versions, and protect reused PVLs against
  stale deletes. Replicated ObjectDB compaction no longer erases leaks independently of the ordered event stream.

- Implement prepared newer-generation handoff with grant drain and Azure lease transfer; confirm transferred
  ownership by target renewal even after a lost response. Bootstrap the initial leader record during discovery.
- Initialize unpublished databases only under selected leadership after existing histories are adopted. Capture
  the input cursor once per completed initialization and publish genesis/startup schema before leader service.
- Detect live schema commits from bounded native TRL inspection before comparison, including lagging followers.
  Preserve rollback semantics, permanently disable detached-node takeover, keep local work running, and request
  graceful restart after fifteen minutes without valid leader evidence.

- Permanently stop lease contention after observing a newer application generation while continuing to follow
  shared databases. Removed databases remain available for local work and receive no further peer comparisons;
  delayed lease replies cannot restore eligibility.

- Connect native restore, authenticated in-process peer following, automatic election/takeover and canonical
  publication through an internal node coordinator. Keep renewal independent of blocked publication, bound requests
  with cancellation, and recheck a new leader from the verified restore cut before accepting its history.
  Cover three-node failover, delayed former-leader writes, outages, authentication and divergence with real BTDB.

- Implement Azure replication storage for finite leases, leader-record CAS, canonical TRL append/adoption and
  immutable PVL/KVI publication with SHA reconciliation. Add an Azurite-backed adapter suite to the solution and CI.
- Select fresh leadership terms under lease, preserve leader-record skip entries, and validate/adopt every selected
  database against Blob history before exposing publishers. Peer acknowledgement does not bypass Blob validation;
  retries preserve optimistic local work and ordinary restart remains the recovery path for divergence.
- Synchronize lease authority and handle snapshots so independent renewal and activation lanes cannot mix sessions.

- Schedule lease maintenance with deterministic renewal timing, outage retries, non-overlapping requests and
  shutdown cancellation, independently of database publication and application work.

- Add lease session recovery after storage outages: renew before expiry or acquire fresh authority after expiry,
  keeping old publisher/reader authority fenced.

- Serve follower TRL reads directly from retained leader files, bounded by the completed capture position;
  reject reads after connection closure or lease expiry without blocking local transactions.

- Add follower comparison sessions with coalesced leader progress, expiring live confirmation, cancellation on
  connection closure and a single restart request on divergence. Local transactions remain independent.

- Compare follower TRL prefixes directly against a leader-session range reader, with no Blob listing or header
  parsing. Preserve bounded reads, capture acknowledgement, lag, sticky divergence and cancellation; Blob history
  validation belongs to takeover and recovery. Transport/coordinator integration remains pending.

- Download remote files in bounded parallel 256 KiB blocks without recomputing their checksum; preserve ordered
  local writes, version-bound reads, short-read handling and cancellation/failure cleanup. Downloads always use the
  remote file ID. Restore records identity mappings without replacing prior upload receipts; publication follows restore.

- Simplify checkpoint retries by reusing their validated mapping, track remote placement IDs with a set, and
  rely on the allocator for nonzero/even ID guarantees while retaining collision and prerequisite checks.

- Remove TRL/parity selection from remote ID reservation; preserve deterministic native TRL IDs unchanged.

- Reserve checkpoint KVI identities before upload and retain the exact snapshot/ID/mapping across uncertain
  responses; block later checkpoints until the pending one is reconciled.

- Allocate remote PVL/KVI IDs from refreshed inventory and conditionally create files with SHA metadata;
  remove separate durable reservation state and fence mismatching or missing SHA on retries.

- Simplify replication bookkeeping: retain only the canonical tail write receipt, keep one selected-inventory
  lookup, and validate TRL identities directly without allocating encoded metadata.

- Select published genesis/TRL links through a version-bound remote inventory adapter. Use ordinary file-set
  initialization and native `OpenAsync` for recovery and new-term publisher resumption; remove the separate restore
  wrapper and duplicate header-validation pass without changing core recovery semantics.

- Connect checkpoint publication to the canonical TRL publisher. Confirm the fixed snapshot cut before PVL/KVI
  uploads, return pending/conflict/authority-loss outcomes, and recheck leadership between uploads. Allow an already
  verified restored cut to satisfy the barrier before the first new local commit.

- Allow the canonical replication publisher to publish through a fixed complete checkpoint cut without waiting for
  later local commits. Preserve unresolved CAS intents, term adoption and authority fencing in the same mutation lane.

- Add explicit remote inventory refresh before leader writes, preserving local files and session mappings while
  atomically replacing remote membership and versions after successful discovery.

- Require explicit implementations for every `IFileReplicatedCollection` method; remove default interface behavior.

- Keep existing in-memory and disk collections standalone, with their original allocation implementations. Clone
  in-memory storage for replication cache/restore instead of adding a shared storage capability interface.

- Enable odd TRL/even non-TRL allocation automatically only for `IFileReplicatedCollection`. Remove the
  `UseOddTransactionLogIds` option and keep standalone collection allocation unchanged.

- Check local cache filename extensions before length and SHA validation. Download files with canonical native
  extensions and remove mismatched/unknown extensions without reading file sizes or bodies.
- Accept BTDB's `IKeyValueDBLogger` in the replicated collection and log local file removal reasons and IDs,
  including failed-download cleanup. The caller can share the logger with `KeyValueDBOptions.Logger`.

- Validate existing mapped cache files during replicated collection initialization: compare extension and length before reading
  bytes, then compare local SHA-256 with remote metadata. Remove mismatches or unverifiable candidates without
  downloading replacements; prefetch downloads missing files and reuses validated files without a second hash.

- Track remote-to-local file ID assignments in memory for each replication session and expose `GetLocalFileId`.
  Use mappings for startup cleanup, cache population and KVI PVL references. Keep assignments stable across eviction;
  after restart, differently numbered local copies may be discarded and downloaded again.
- Expose leader PVL publication as `IFileReplicatedCollection.PublishPureValuesAsync` and use it for checkpoint
  publication. Reuse confirmed mappings, record new ones after upload confirmation, and retry uncertain uploads
  with the same reserved remote ID. Local/follower compaction performs no remote publication.

- Remove local files without a selected remote mapping during replicated collection initialization, after remote
  discovery succeeds. Validate mapped files before retaining them and preserve all local files if discovery fails.
  Repeated initialization keeps files created in the current initialized session.

- Require explicit replicated collection initialization before `BTreeKeyValueDB.OpenAsync`. Neither open nor prefetch
  calls `InitializeAsync`; remote inventory and metadata access reject incomplete initialization instead of reporting
  an empty database. Local cache access stays independent, and cancelled initialization can be retried explicitly.

- Separate replicated collection inventories: `GetCount`, `GetFile`, and `Enumerate` expose local storage only;
  `GetRemoteCount`, `GetRemoteFile`, and `RemoteEnumerate` expose the selected remote inventory. Recovery discovers
  remote files and explicitly prefetches required bodies; local creation/deletion does not change remote inventory,
  and local compaction skips remote-only files.

- Restore the original `IFileCollection` contract. Move replication discovery, metadata, prefetch and parity allocation
  to `IFileReplicatedCollection`; its presence selects replication semantics. `OpenAsync` retains standalone behavior
  for ordinary collections, and synchronous constructors reject replicated collections. Keep exact-ID `ImportFile`
  on the dedicated `InMemoryReplicationFileStorage` implementation, outside the logical replication interface.

- Replace the file-allocation floor overload with `InMemoryReplicationFileStorage.ImportFile(fileId, hint)`, which restores
  exact IDs in any order and rejects collisions without opening or overwriting existing files.
- Add an opt-in replication `BTreeKeyValueDB.OpenAsync` with lazy file metadata over an explicitly initialized remote inventory.
  Discover KVI/TRL candidates from filename extensions and numeric IDs,
  trying newest KVIs first without scanning all headers. Read only attempted KVI and required TRL metadata.
  Historical opening and history retention remain exclusive to standalone collections; replicated opening rejects both options.
  Log capture is initialized only in this replication path; synchronous constructors reject `TransactionLogCapture`.
  Existing constructors retain synchronous opening, eager metadata loading and advisory prefetch. At the end of async open,
  request all files referenced by the selected valid KVI in parallel, or all TRLs when no valid KVI is available,
  and await their availability before returning. Failed or cancelled open releases native roots without appending to TRL.
- Separate replication compaction from the unchanged standalone compactor. Replication ignores native generations,
  retains PVL and old TRL by concrete references and capture boundaries, and creates no local KVI. Reject HID/HPV and
  sub-database creation in replication mode. Serialize local compaction passes and protect all live snapshot roots.
  Retain KVI file references by TRL fileId and offset instead of storing its root, including across pointer-only compaction.
- Expose replication's remote inventory and verified local cache through one transparent `IFileCollection`.
  Read metadata through small version-bound ranges; share and bound lazy downloads, reuse checksum-verified cache
  entries, and retain confirmed PVL placements across checkpoint attempts. Checkpoint destinations remain remote-allocated.

## 35.10.2

### Fixed

- Keep the source generator compatible with Roslyn 4.11 and later, including .NET 10.0.1xx SDKs,
  instead of requiring Roslyn 5.9 from a newer SDK.

## 35.10.1

### Fixed

- Correct in-memory file reader positions after unaligned seeks or skips across buffers, preventing
  skipped bytes and incomplete transaction-log replay.

- Keep read-only and ordinary transaction snapshots on the last published BTree during a transaction batch,
  avoiding reader-triggered TRL replay and premature batch publication. Existing readers retain their snapshots.

### Changed

- Allow AutoAdjustFileSize with a transaction-log size strategy. Keep PVL compaction and chunk-storage sizing
  independent of the active TRL limit; autosizing changes local file targets without changing strategy-controlled TRLs.

- Add ObjectDB.InitializeRelations for startup registration of a complete relation-type list. Check schemas and
  indexes read-only, then persist all changes, including new empty relation schemas and OnCreate callbacks, in
  at most one writing transaction. Preserve registrations and database contents on failed initialization.

- Reduce TRL capture to completed and acknowledged (fileId, offset) positions. Remove per-transaction records,
  index files, sequence counters, capture I/O failure state and wakeups. Coalesce publication into complete native
  prefixes and retain unacknowledged TRLs through the compactor boundary.

- Decode TRL commands directly in the replay loop; remove the intermediate command struct and decoder layer.
  Copy inline values directly into their BTree representation. Remove added command-flag and length validation
  from the hot loop; leave invalid lengths to the existing allocation and reader operations.
  Restore the original end-of-stream-only catch in TRL replay after removing those validations.

- Remove TransactionLogReader and redundant command decoding from native TRL publication. Use byte-range equality
  for replication comparisons; keep command decoding only in database startup and batch replay.

- Remove lazy relation-schema creation and its per-write pending checks. Persist schemas, including empty relations,
  during initialization; legacy transaction registration requires a writer for new or changed schemas. Rollback
  discards the affected registrations so initialization can be retried.

- Defer size-triggered TRL rotation on database reopen until the next transaction, also without a custom size strategy.

- Remove the redundant parity argument from the database-aware file collection API; derive parity from file type
  and the database allocation mode. Add real-compaction coverage showing why unpublished capture bytes need retention.

- Add an internal capture-backed canonical TRL publication lane with streamed conditional append, successor-first
  cross-file/genesis publication and metadata-only tail adoption. Retain ambiguous intents and source retention, reconcile
  exact native prefixes in bounded chunks, and fence new writes after lease loss without stopping local execution.
  Add native restore/interruption/race coverage; production Azure transport and coordinator integration remain pending.

- Add pinned native KVI export directly to a forward-only writer for chunked Blob upload, preserving TRL identities
  and offsets while substituting only whole-PVL file IDs. Keep local compaction and remote export cancellation separate.
- Add an internal checkpoint publication helper that reuses verified downloaded/uploaded PVLs across checkpoints,
  retains uncertain upload destinations for retry, and waits for all PVL/TRL prerequisites before KVI upload starts.
  Production storage, canonical authority and remote cleanup integration remain pending.

- Remove ScratchFileCollection, the core scratch switch and special temporary-file cleanup. Local writes and
  compaction continue normally when remote publication stops; startup cache validation uses Blob history.
  Keep native file-ID restore coverage and verify publication cancellation leaves local execution running.


- Keep native command decoding internal to replay.
- Remove generalized non-application writer admission: replication reconciles secondary indexes in at most the first
  ObjectDB writing transaction, with leadership handled at startup. Keep ordinary writer-queue cancellation.
- Replace per-file pin counting with the compactor's existing used-file protection from the oldest required TRL.
  KVI export sources are retained by their referenced BTree root.

- Set the replication startup performance target to at most 15 minutes for approximately 100 GB, including cold Blob
  restore, validation and replay; qualify download concurrency and pipeline overlap with end-to-end measurements.

- Plan configurable delayed Blob cleanup, provisionally about one day after replacement publication makes files
  obsolete, to reduce restore races while retaining missing-file retry and avoiding follower tracking.

- Add internal replication M1 lease/grant deadlines and native TRL metadata/CAS intents, with deterministic race tests,
  native multifile commit/rollback replay and a live Azure capability probe. Use one bounded grant-drain deadline
  instead of peer revocation messages. Production integration remains open; KVI uses the existing dependency-first
  publication and restore-retry rules without an additional selection protocol.

- Simplify planned peer replication to committed `(eventId, trlFileId, trlPosition)` notifications followed by native
  TRL range pulls. Defer byte piggyback optimizations and remove per-transaction push envelopes; bootstrap KVI/PVL
  is fetched directly from Blob Storage.

- Revise planned replication compaction: every node performs local physical compaction without KVI; only the leader
  compacts Blob files using a separate inventory and a native KVI exported with remote file-ID/offset mappings.
  Remove compaction operation/result distribution to running peers; remote outputs serve ordinary restore only.

- Add the BTDB.Replication M0 test foundation: deterministic node scheduling, seeded fault injection, isolated native
  BTDB application fixtures and independent model-history checks, with an explicit coverage register. The distributed
  replication protocol and production adapters remain unimplemented.

- Add an optional deterministic TRL-ID size strategy with soft rotation between transactions and a strict hard
  limit below 4 GiB, including cross-file metadata/commit handling, reopen, and oversized-command rejection.

- Complete replication file-ID allocation: new TRLs are odd and all non-TRL files are even, enforced centrally
  for KVI, PVL and sub-database allocations; retain unconstrained allocation when the option is disabled.
- Add core replication preparation APIs: application writers automatically set CommitUlong from eventId; optional
  explicit transaction admission and odd TRL allocation preserve standalone defaults and legacy even-tail replay.
- Require completed PVL and canonical TRL publication through the KVI cursor before starting any KVI upload,
  including remote block staging; completing KVI last alone is insufficient.
- Consolidate replication lifecycle and publisher state transitions, interruption ordering and KVI cleanup proof
  obligations; separate engineering backlog from owner decisions, allow local genesis readiness before async flush,
  and clarify that the application event log supplies durability while Blob KVI/TRL accelerates recovery.
- Disqualify schema-detached replication sessions from all leadership contention and handoff; request graceful
  restart after 15 continuous minutes without a valid observable leader.
- Clarify replication writer admission automatically sets CommitUlong from eventId; non-application commits trigger
  immediate asynchronous flush without a durability wait, and recent leader tail reads have a short timeout before
  input replay fallback. Initialize relation schemas and indexes together before application processing.
- Record replication API choices for writer admission context, non-application commit completion and ObjectDB
  schema initialization; align remote cleanup wording with optional deferred deletion after KVI publication.
- Simplify replication cache reuse to sealed files: always redownload the last growing TRL on restore and calculate
  its whole-file checksum only after sealing.
- Revise replication startup design to reuse checksum-verified local files, bound parallel downloads by replay
  dependencies and local disk, and replay every canonical TRL in ascending order when no KVI exists; prioritize
  local disk space over aggressive remote cleanup and withdraw blanket cache deletion on restart.
- Simplify replication restore publication to native KVI written after all prerequisite files; allow obsolete-file
  deletion only after successful KVI publication and remove separate checkpoint manifests, pointers and selection CAS.
- Select replication prepared-successor TRL publication, CommitUlong-based non-application classification, ordinary
  rollback log comparison and asynchronous writer admission with readonly-first ObjectDB initialization; use
  native KVI-last publication for checkpoint discovery without a separate pointer.
- Review replication recovery and application boundaries: restore cross-file transactions, record publication/discovery,
  schema metadata, rollback comparison and API choices; clarify activation authority, full-position compaction checks,
  reader-safe file identity and unbounded graceful waits for application transactions. Existing DB is already in Blob.
- Define replication compatibility with legacy TRL numbering: preserve existing files and close an even append target
  before starting a new transaction in a fresh odd-numbered TRL.
- Simplify replication durability to CAS directly on canonical TRL; remove the additional per-batch state.json CAS
  and align non-application publication, recovery and takeover rules with the TRL publication point.
- Require leader authority before replication genesis/schema writes and immediate Blob publication after their commits;
  initialize CommitUlong before the first applied event and replace provisional new databases with pending initialization.
- Extend replication design with canonical schema transactions: live followers continue independently in volatile
  storage, while compatible replacement nodes restore schema changes from TRL or checkpoints.
- Define application-owned replication transactions, identical rollbacks, failure policy and external effects;
  use local commit/read semantics, follower-first Blob restore before election, and genesis at the current input end.
- Select short-lived replication confirmation grants and clarify optional direct confirmation, rejection of predecessor
  messages after takeover, and grant draining before planned lease transfer.
- Correct replication design inconsistencies in per-event transactions, shared input ordering, leader revision identity,
  read visibility during virtual batching, restart recovery, and non-reusable remote object names.
- Align replication architecture notes with byte-preserving virtual transaction batching and fatal local disk
  exhaustion: terminate old readers, discard cache, and restore the latest published Blob checkpoint before replay.

## 35.10.0

### Changed

- Update NuGet dependencies to their latest stable versions.
  Correct benchmark parameter types for the updated BenchmarkDotNet analyzer.
  Update the Microsoft DI compatibility test for its .NET 10 `AnyKey` exception behavior.

- Add npm release scripts for interactive release selection and direct major, minor and patch releases.
- Reduce secondary-index key allocations during relation deletion and updates by using caller-owned
  stack-backed writers for serialized keys, reusing their buffers across indexes with heap fallback for long keys.
  Batch deletion retains these buffers across rows for prefix, partial and range deletion;
  partial deletion also reuses its secondary-index cursor across rows.
- Add `MemReader.CopyStringToOrdered` for direct stored-to-ordered string conversion,
  and use it for built-in string fields when merging serialized rows into secondary keys.
  Preserve null, Unicode and surrogate handling without materializing an intermediate UTF-16 string.
- Add direct copying of encoded VInt64/VUInt64 values and use it for matching built-in Signed/Unsigned
  handlers in secondary-key mergers, avoiding numeric decoding and re-encoding.

## 35.9.0

### Changed

- Added opt-in BTree transaction batching via `StartWritingTransaction(inBatch: true)` (also forwarded by ObjectDB),
  preserving per-transaction TRL and durability semantics, reader snapshots, and rollback through committed-prefix replay.
  Ordinary writer requests finish a pending batch when granted; `FinishTransactionBatchAfterCurrentTransaction()`
  ends it immediately when idle or after the active writer completes, before granting the next queued writer.
  The finish method is also available through `IObjectDB`, forwarding to the underlying key-value database.
  `InMemoryKeyValueDB` accepts and ignores the `inBatch` optimization hint, preserving ordinary transaction semantics.
  Added regression coverage and a benchmark comparing BTree batching, application batching, and individual transactions.
  Referencing the last published root, statistics, and checkpoint creation do not implicitly publish a pending batch.
  Live replay uses buffered reads; memory-mapped file growth and hard flush synchronize with concurrent reads.
- Added reusable `FileCollectionFileReader`, a bounded `IMemReader` using `RandomRead` directly. Batch replay
  starts reading at the requested offset and reuses one reader and buffer across TRL files, avoiding the stream adapter.
- Made the compactor's TRL durability barrier a serialized hard flush without appending an empty transaction or
  temporary-end marker; the existing temporary-close API and file format remain compatible.

### Fixed

- Removed the redundant TerraFX package feed whose invalid TLS certificate prevented CI dependency restore.
  All dependencies, including `TerraFX.Interop.Mimalloc`, are available on NuGet.org.

## 35.8.0

### Changed

- Removed the primary-key-only limitation from `IterateById`: reusable projections can now include stored value fields,
  deserialized using the row's schema version while preserving `[NotStored]` context.
- Added `FastIterateNoCursor` for callbacks that do not access the cursor, with deferred cursor/index updates
  and optimized short key suffix copies. Existing `FastIterate` continues to expose the current cursor during callbacks.
  Primary-key-only `IterateById` scans use the new API.
- Further optimized `FastIterateNoCursor` with padded short-suffix word copies, fewer bounds checks, and
  a single per-key iteration counter.
- Reduced work when scanning prefixed BTree leaves by combining scratch-buffer capacity checks, copying short
  prefixes with a single word, reusing adjacent suffix offsets, and simplifying branch cursor-position updates.

### Added

- Added `IterateById` relation methods that hydrate a projection into one caller-provided instance for every matching
  primary-key row, allowing allocation-free callbacks and preserving `[NotStored]` context on the reused instance.

## 35.7.0

### Added

- Added `IObjectDB.RunLeakDetection(...)` and `IObjectDB.RunLeakRemovalAsync(...)` for immediately detecting or
  removing leaked object and dictionary keys independently of periodic compactor leak detection, with affected key
  counts and object type names in the returned results.

## 35.6.0

### Added

- Added `ILazyUlongList`, an ObjectDB-backed append-only `ulong` list with streaming rebuild support, fast indexed
  enumeration, and delta-encoded records of up to 4096 values, ideal for speeding up persistence of filtered and sorted
  lists of indexes.

## 35.5.0

### Changed

- `IRelation.Query<T>()` now evaluates chained `Where` predicates with the bytecode interpreter directly from
  serialized relation fields, skipping unreferenced fields and materializing only matching rows.

### Added

- `DBOptions.WithoutFreeContentInNativeObject()` disables value free-content handling during direct lazy-dictionary
  mutations, allowing nested dictionary IDs to be shared by multiple stored objects. Relation free-content cleanup stays
  enabled, while content that becomes unreferenced can still be reclaimed by the compactor leak detector in
  `CompactorLeakDetectorMode.Erase` mode.

## 35.4.1

### Fixed

- ObjectDB can remove or update older relation records whose secondary-key collection field changed from
  `IList<T>` to `List<T>`, including when another schema change keeps the older record version persisted.

## 35.4.0

## 35.3.19

## 35.3.18

## 35.3.17

### Fixed

- ObjectDB relation free-content cleanup again tolerates unknown inline object types while discovering nested
  dictionary content to free.

## 35.3.16

### Fixed

- ObjectDB relation free-content cleanup again tolerates unknown inline object types while discovering nested
  dictionary content to free.

## 35.3.15

## 35.3.14

### Fixed

- Simplified ObjectDB relation free-content cleanup for repeated inline native object references without re-reading or
  skipping object bodies.

## 35.3.13

## 35.3.12

## 35.3.11

### Fixed

- ObjectDB relation free-content cleanup now registers inline objects before traversing their nested free content,
  preventing list-held inline objects from being re-read from an invalid reader position.

## 35.3.10

### Fixed

- ObjectDB relation free-content cleanup now preserves inline object reference tracking while traversing OID-backed
  native object fields.

## 35.3.9

### Fixed

- ObjectDB relation free-content cleanup now reuses the reader's inline object reference tracking for native object
  fields, preventing repeated inline references from being interpreted as new object bodies.

## 35.3.8

### Fixed

- ObjectDB relation row removal now preserves inline object reference tracking while freeing nested dictionary content,
  preventing reads past the end of complex inline object payloads.

## 35.3.7

## 35.3.6

## 35.3.5

## 35.3.4

## 35.3.3

### Fixed

- Source generator hint names are now sanitized for nullable generic type arguments.
- Source-generated relation implementation classes are now marked as internal-use-only obsolete types so reflection-based
  scans do not treat them as user relation contracts.

## 35.3.2

### Fixed

- Source-generated relations now read primary and secondary key attribute arguments from metadata for external generic
  base classes, preventing false duplicate-key diagnostics.

## 35.3.1

### Fixed

- Source-generated relations now fully qualify method parameter types and normalize fully qualified primitive key types
  before emitting key writer calls.

## 35.3.0

### Fixed

- ObjectDB lazy dictionaries now free nested dictionary content when a stored dictionary value is replaced, removed, or
  cleared directly through the dictionary API.

## 35.2.0

### Added

- ObjectDB now periodically detects leaked object and dictionary keys during compaction, starting on the fifth
  compaction after opening and then every twentieth compaction, while logging affected object type names and leaked
  key count with elapsed time spent. `DBOptions.WithCompactorLeakDetectorMode(...)` can disable the detector or enable
  erasing leaked keys.

### Fixed

- KeyValueDB now releases partially built native BTree nodes when loading a KVI fails before falling back to an older
  index.

## 35.1.1

### Fixed

- BON builder now keeps nested payload buffers alive while copying them into the root BON buffer, preventing intermittent
  access violations during class, object, array, tuple, and dictionary finalization.

## 35.1.0

### Added

- Relations now expose a `Query<T>()` API returning `IQueryable<T>` with initial interpreted `Where` support over
  expression equality, boolean conjunction, and boolean disjunction.
- The low-level interpreter now supports try/catch/finally metadata, exception throw/rethrow opcodes, stack struct
  allocation via resolved `RawData` stack allocators, stack byte allocation, resolved metadata getter/setter calls,
  resolved type converters, and finally-aware nested `Stop` handling.
- The low-level interpreter builder now rejects non-empty programs that do not end with `Stop` and programs with
  unclosed try blocks, and automatically wraps stack allocation nested programs in try/finally `Stop` blocks.
- Relations can now expose an `AllocateId()` method that allocates persisted `ulong` identifiers independently per
  relation, starting at `1`.
- Added low-level `RoaringBitmaps` helpers for 16-bit bitmap containers over `Span<byte>` and `ReadOnlySpan<byte>`,
  including bit updates, compression, boolean operations, negation, tests, and BenchmarkDotNet scenarios.
- `RoaringBitmaps` can now enumerate encoded 16-bit containers as `ulong` values with a caller-provided offset through
  a concrete struct enumerable.
- `RoaringBitmaps` now has an allocation-free dense bitmap iterator over `ReadOnlySpan<byte>` for callers that already
  have the raw 8192-byte bitmap.
- `RoaringBitmaps` now exposes in-place dense bitmap mutation operations for negation, union, intersection, union with
  complement, and difference against encoded containers.
- `RoaringBitmaps` now exposes optimized encoded-container truncation for dropping values above a 16-bit maximum.
- ObjectDB relations now support lazy `IRoaringBitmap` fields backed by the existing external-content keyspace, with
  explicit flushing and automatic free-content cleanup on normal relation removes and updates.
- `IRoaringBitmap` now supports command-buffer initialization through `RoaringBitmap.BuildAsync(...)`, including
  sorted enumerable sources and fused boolean operations.

### Changed

- `RoaringBitmap.BuildAsync(...)` now writes generated page payloads directly into the command buffer with fixed
  `UInt16` page lengths, aligned page data, optimized array-source `Or`, `And`, and `Not` paths, and direct reuse of
  stored `IRoaringBitmap` page payloads, reducing temporary allocations during bulk `IRoaringBitmap` initialization.
- Raw object-data copying now calls `Buffer.BulkMoveWithWriteBarrier` through `UnsafeAccessor` instead of resolving it
  with reflection during `RawData` initialization.
- `RawData.GetOffsets` now supports tuple layouts with three or more fields.
- `BonSerializer` now supports `Tuple` and `ValueTuple` values with three or more items.

## 35.0.5

### Fixed

- `BTDB.AzureStorage` now reopens existing transaction-log cache files without holding a write handle open until an
  append is actually requested, which fixes Windows startup scenarios that needed to read a locally longer `.trl`
  file while `AzureBlobFileCollection` queued its initial synchronization.

## 35.0.4

## 35.0.3

### Added

- `UseBtdbIoc(...)` and BTDB's internal `IServiceProvider` integration now expose `IRootContainer` as a singleton so
  the BTDB root container can be resolved safely from the ASP.NET root `IServiceProvider` while `IContainer` remains
  scope-bound.

### Changed

- BTDB IOC now caches `IServiceProvider` fallback factories and removes redundant service-provider availability checks,
  which reduces overhead when BTDB resolves services from `Microsoft.Extensions.DependencyInjection`.
- BTDB IOC no longer tracks cross-container dependency cycles at runtime when resolving through
  `Microsoft.Extensions.DependencyInjection`, which reduces bridge resolve overhead but leaves such cycles to recurse
  until one of the containers fails naturally.
- BTDB IOC now uses a root-provider fast path for exported registrations resolved from the ASP.NET root
  `IServiceProvider`, avoiding extra `RootScopeIdentity` and `ContainerImpl` lookups on the steady-state root path.
- BTDB IOC now bypasses export enumeration bookkeeping for the common single-registration `registrationIndex == 0`
  path, which further reduces steady-state resolve overhead for BTDB services exposed through
  `Microsoft.Extensions.DependencyInjection`.
- BTDB IOC now keeps three distinct integration modes: pure BTDB, BTDB resolving services from
  `ContainerBuilder.ServiceCollection`, and full bidirectional BTDB <-> ASP.NET DI bridging only through
  `UseBtdbIoc(...)`. Building a plain `ContainerBuilder` no longer auto-exports BTDB registrations into an internal
  `IServiceProvider`.
- BTDB IOC root resolves now cache direct BTDB factory lookups separately for keyless `Type` and keyed `KeyAndType`
  resolutions, and scoped containers reuse those root caches instead of rebuilding the same resolve factories.
- BTDB IOC singleton registrations now keep their shared singleton instance state on the registration itself instead of
  in a separate root singleton array, which removes an extra indirection from steady-state singleton resolves while
  preserving shared instances for alias and multi-registration paths.
- BTDB IOC now reuses a single lifetime factory-cache slot and a single in-progress locker type for both singleton and
  scoped registrations, reducing per-registration bookkeeping in the resolve hot path.
- BTDB IOC exports resolved from `Microsoft.Extensions.DependencyInjection` no longer re-resolve
  `ServiceProviderIntegration` from the service provider on every call, reducing steady-state bridge overhead for
  `UseBtdbIoc(...)` service exports.

## 35.0.2

### Fixed

- BTDB IOC singletons are now resolved correctly from the ASP.NET root `IServiceProvider` when scope validation is
  enabled, which fixes `UseBtdbIoc(...)` root-provider resolutions that previously failed because `ContainerScope`
  could not be resolved from the root scope.

## 35.0.1

### Fixed

- BTDB IOC singleton factories resolved through `Microsoft.Extensions.DependencyInjection` now always use the root
  container for singleton storage and follow-up resolutions, which fixes failures when a singleton first created from a
  disposed DI scope later resolves additional services.
- ObjectDB relation upgrades now report field-handler incompatibilities for `UseNoEmitForRelations` loaders with the
  concrete destination handler when available, which fixes missing diagnostics for cases like
  `ODBDictionaryFieldHandler` to `DictionaryFieldHandler`.

## 35.0.0

### Added

- Added the `BTDB.AzureStorage` package with `AzureBlobFileCollection`, an `IFileCollection` implementation that
  maintains a local memory-mapped cache synchronized with Azure Blob Storage, reconciles local and remote file lengths
  on startup, uploads regular files as block blobs, batches transaction-log block commits on a configurable timer, and
  drains uploads and deletes through a FIFO background queue.
- `AzureBlobFileCollectionOptions.DeleteLocalCacheDirectoryOnDispose` makes removal of the local cache directory on
  dispose an explicit opt-in behavior.
- `AzureBlobStorageBackend` now accepts an optional path prefix, allowing BTDB files to be scoped under a subdirectory
  within a shared blob container.
- BTDB IOC registrations are now automatically available from `Microsoft.Extensions.DependencyInjection`, including
  keyed registrations, per-scope `IContainer` resolution, and cycle detection for dependency chains that cross between
  BTDB IOC and the ASP.NET service provider.

### Changed

- `AzureBlobFileCollection` is now intentionally optimized only for BTDB KeyValueDB file types (`trl`, `pvl`, `kvi`),
  uses specialized file implementations per type, and writes `pvl`/`kvi` data through a 128 KB buffered exclusive
  writer instead of the previous memory-mapped writer path.
- `AzureBlobFileCollectionOptions.Logger` can now observe enqueue, execution, retry failures, and queue length for the
  Azure Blob FIFO worker, plus startup downloads from Azure Blob Storage.
- Transaction-log synchronization now passes only the target length plus a random-access reader into the blob backend;
  `AzureBlobStorageBackend` computes the committed block plan itself with an internal 128 KB preferred block size,
  reuses existing committed blocks only when their IDs and lengths exactly match the expected layout, and otherwise
  rebuilds the blob so committed block counts stay bounded even under very small flush increments.

### Fixed

- ObjectDB relation helper methods now accept a nullable collection placeholder for `FirstById`/`LastById` primary-key
  lookups, which removes nullable warnings from generated relation code that intentionally passes `null`.

## 34.5.1

### Added

- IOC now supports `Scoped()` registrations and `IContainer.CreateScope()`. `IContainer` is now
  `IAsyncDisposable`-only, child scopes own scoped instances, the root container owns singletons and registered
  instances, and BTDB scopes now align with `ContainerBuilder.ServiceCollection` scoped services. `CreateScope()`
  is forbidden after disposal, while disposal only blocks tracking of newly created owned disposable instances.

## 34.5.0

### Added

`Constraint.NullableDateTime.Predicate` and `Constraint.NullableDateTime.UpTo` for filtering nullable `DateTime`
relation keys.

## 34.4.2

### Fixed

- Source generated ObjectDB relation methods now accept `List<T>` parameters for `IList<T>` key fields, including
  secondary keys, so relations can keep querying indexes created with older collection key declarations.
- ObjectDB relation metadata now appends omitted primary-key parts to secondary keys in declared primary-key order and
  builds reflection-based relation metadata deterministically, which fixes generated relation startup failures for some
  secondary keys that combine explicit fields with implicit primary-key suffixes.

## 34.4.1

### Fixed

- ObjectDB and EventStore list serialization now unwrap wrapper collections through their first referenced backing
  instance before retrying fast-path handling, which fixes `IList<T>` values backed by
  `System.Collections.ObjectModel.Collection<T>`.

## 34.4.0

### Fixed

- KeyValueDB transactions now make repeated `Dispose()` calls idempotent and clear pooled cursor reuse slots before
  releasing cursors, which fixes crashes when a disposed read-only transaction is disposed again after its cursor has
  been reused by another transaction.
- Source generator now always registers reflection metadata for generated types, including empty classes and IOC-only
  generated registrations, which fixes polymorphic ObjectDB storage for generated marker implementations and reuses
  existing constructor accessors for private parameterless constructors.
- EventStore dictionary serialization now writes the live item count instead of the internal slot count, which fixes
  failures when serializing `Dictionary<TKey, TValue>` instances after removals created free entries.
- Inline `HashSet<T>` serialization in ObjectDB and EventStore now reads the runtime `HashSet<T>` free-entry count from
  the correct field and writes the live item count, which fixes corrupted set payloads on .NET 10, including failures
  after removals created free entries.

## 34.3.14

### Fixed

- EventStore2 now deserializes `object`-typed payloads containing plain `new object()` instances without crashing when
  `System.Object` metadata is registered manually.

## 34.3.13

### Changed

- `Sample3rdPartyLib` now targets `net10.0`, matching the rest of the solution's .NET 10 sample and test projects.

### Fixed

- Source generator now emits separate `where` clauses for each constrained generic type parameter, which fixes invalid
  generated code for concrete classes that inherit generic base classes implementing generated interfaces with
  constraints.
- Source generator now suppresses `SYSLIB0057` in emitted files, which removes new .NET 10 warnings from generated
  IOC, metadata, and relation helper code.

## 34.3.12

### Fixed

- Source generator now treats `[Generate]` on classes transitively across the full base-type hierarchy, so descendants
  of generated classes are generated the same way as descendants of generated interfaces.
- Source generator now accepts `AdvancedEnumeratorParam<TEnum>` on relation `FindBy...` methods for secondary keys
  that include additional primary-key prefix fields, which fixes incorrect `BTDB0016` validation errors on valid
  advanced enumerator overloads.
- `RawData.GetDictionaryEntriesLayout` now recognizes `IOrderedDictionary<TKey, TValue>` metadata, which fixes
  failures when resolving entry layout information for ordered dictionaries.

## 34.3.11

### Fixed

- Source generator now supports `AdvancedEnumeratorParam<TEnum>` on relation methods that enumerate or filter by enum
  secondary keys, which fixes runtime failures when building advanced key ranges for enum-backed indexes.

## 34.3.10

### Fixed

- Object DB relation loading no longer double-reads `Int128`-sized value-type members that are populated through
  metadata `PropRefSetter` adapters, which fixes corrupted trailing fields when reopening stored rows.

## 34.3.9

### Fixed

- Object DB relations now accept `IList<T>` properties backed by arrays when saving/upserting rows.

## 34.3.8

### Fixed

- Source generator now preserves the closed generic member type when emitting metadata accessor adapters for inherited
  properties, which fixes generated metadata for derived types that inherit generic auto-properties, including bases
  defined in external assemblies.

## 34.3.7

### Fixed

- Source generator now emits metadata getter/setter adapters for inherited properties declared on generic base types,
  which fixes `InvalidProgramException` when accessing those properties through generated metadata.

## 34.3.6

### Fixed

- Source generator now handles `GenerateFor(typeof(IRelation<...>))` and similar closed relation interfaces by
  emitting metadata for the concrete relation item type.

## 34.3.5

### Fixed

- Fixed loading `CommandHistory` records after reopening the database when nested items contain polymorphic
  `List<UndoableEvent>` values.

## 34.3.4

### Fixed

- BtdbInternalNextInChain is not skipped in metadata generation.

## 34.3.3

### Fixed

- Source generator now emits generic field accessor adapters for metadata on inherited members from generic owner types,
  which fixes runtime failures when storing derived classes that close generic base properties.

## 34.3.2

## 34.3.1

### Fixed

- Source generator now ignores inherited `GetEnumerator` methods when validating relation interfaces, which fixes
  covariant relation hierarchies that also expose `FindBy...` methods.

## 34.3.0

### Fixed

- `CompactorScheduler` now tracks compaction requests per `IKeyValueDB`, so shared schedulers compact only the databases
  that were advised to run.

## 34.2.0

### Added

- Source generator relations now support class relation items that use interface contracts for persisted
  properties/indexes and lifecycle hooks (`OnSerialize`, `OnBeforeRemove`), including inherited interface members.

## 34.1.5

### Fixed

- Fixed creating the first write mapping in `OnDiskMemoryMappedFileCollection` when the backing file is still empty.

## 34.1.4

### Fixed

- Fixed the crash after compaction with OnDiskMemoryMappedFileCollection.

## 34.1.3

### Fixed

- Source generator no longer emits an extra blank line in dispatcher factories.

## 34.1.2

### Fixed

- Source generator now accepts `GatherBy...` targets that implement `ICollection<T>` (for example `List<T>`), and allows
  `AdvancedEnumeratorParam` to target implicit primary key fields on secondary keys.

## 34.1.1

### Fixed

Source generator no longer crashes when relation method validation fails mid-iteration (e.g., `UpdateById` missing PK
parameters now reports `BTDB0040` instead of throwing).

## 34.1.0

### Fixed

Rare corruption of DB when TRL after KVI is completely waste, and it was removed in compaction before creating new KVI.

### Added

Support for `LastBy...` methods.

## 34.0.6

## 34.0.5

## 34.0.4

## 34.0.3

## 34.0.2

## 34.0.1

## 34.0.0

### Fixed

Secondary key upgrade could now introduce List<> field (till now it crashes with null reference exception).

## 33.8.3

## 33.8.2

## 33.8.1

### Fixed

Disabled one useless warning in source generated code.

## 33.8.0

### Added

Constraint StartsWith for List<ulong>.

## 33.7.3

### Changed

Automatic split size minimum is now 32MB, and it chooses higher value sooner.

## 33.7.2

### Fixed

Fast Gather was not really fast in many cases.

## 33.7.1

### Fixed

Crash when using OnDiskMemoryMappedFileCollection with BTreeKeyValueDB.

## 33.7.0

### Improve

Better error message for handler type incompatibility.

## 33.6.4

### Fixed

Fixed seemingly random wrong iteration when inserting new values in parallel.

## 33.6.3

### Fixed

Compactor should never run synchronously after disposing writing transaction.

## 33.6.2

## 33.6.1

### Added

Allow to register IOC though `ContainerBuilder.ServiceCollection`. Native registrations are only used as fallback.

## 33.6.0

## 33.5.0

### Improve

KVDB: Make Cursors more robust, to survive programmers errors. Writing transactions do not reuse pooled cursors. Inside
current transaction reuse last 2 cursors. Faster disposed checks.

## 33.4.4

### Fixed

Relations: UpdateById didn't work with Computed fields.

## 33.4.3

### Fixed

Relations: Start proposition Excluded in ListBy didn't work correctly when it was not last property of key another case
fixed.

Relations: IDict finder didn't skipped computed and inkeyvalue fields another case fixed.

## 33.4.2

### Fixed

Relations: IDict finder didn't skipped computed and inkeyvalue fields.

Relations: Start proposition Excluded in ListBy didn't work correctly when it was not last property of key.

## 33.4.1

### Fixed

EventLayer2: Tuple and ValueType serialization works.

## 33.4.0

### Added

IOC: RegisterGeneric to register open generic types. As(Type) and Keyed(object, Type) now works for open generic types.
For now, you need to use \[GenerateFor(typeof(...))]] to manually register all used instances of generic type.

IOC: Supports FromKeyedServices attribute (only for string type)

IOC: Dependency attribute with optional name is supported for properties, fields and parameters.

## 33.3.0

### Added

`Utf16Utf8Dictionary` for fast searching by string in UTF-16 and UTF-8. `DerivedUtf16Utf8Dictionary` for fast searching
by string in UTF-16 and UTF-8 from parent dictionary with attached another values.

## 33.2.0

### Improved

Compactor was rewritten to be async. Also it gives more time to writing transactions to run during its running.
Frequency of big compactions is reduced because condition now takes in account also maximum waste per file and the
condition for total waste was 4 times increased.

## 33.1.3

### Fixed

Upsert with changing secondary key during enumeration by same secondary key. Correctly continue enumeration after such
change.

## 33.1.2

### Fixed

Enumerator MoveNext return always false after end of iteration.

## 33.1.1

### Fixed

RemoveById with relations with secondary indexes didn't work correctly.

## 33.1.0

### Added

Disposing DB when there are still some transactions now throws BTDBException.

### Fixed

- Position setter in IOrderedDictionaryEnumerator was returned to behaviour before 33.0.0.

## 33.0.7

### Fixed

Constraint Enumerator could be not correctly iterated multiple times. Now really fixed.

## 33.0.6

### Fixed

Constraint Enumerator could be not correctly iterated multiple times.

## 33.0.5

### Fixed

Cursor leak. And another bug in BTree Cursor.

## 33.0.4

### Fixed

Long time bug in FindLastWithPrefix in InMemoryKeyValueDB.

## 33.0.3

### Fixed

Workaround crash in ExtractPropertyFromGetter in source generator.

## 33.0.2

### Fixed

Support of nameof expression in name of SecondaryKey in Relation.

## 33.0.1

### Fixed

Regression in Source Generator for Relations.

## 33.0.0

### Added

.Net 9.0 support. Cursors in KeyValueDB. Parallel enumeration and modification of DB in ObjectDB. Optimizations in
ScanBy and GatherBy.

### Breaking changes

Major version bump, read README.md for more info.

## 32.17.0

### Added

Add support for skipping unknown types and logging when it happens in ObjectDB transactions. See test
`CanRemoveClassWhenPropertyExistsWithAutoSkipUnknownTypes` for example.

## 32.16.4

### Fixed

Serialization of boxed enums in Layer1 and Layer2. It stay unsupported for ObjectDB.

## 32.16.3

### Improved

Logging of reason why kvi is unusable.

## 32.16.2

### Optimized

Constraint find could use exception flow making it slower than necessary.

## 32.16.1

## 32.16.0

### Fixed

Corruption of DB in rare cases with compaction and lazy flushing data.

Allowed to serialize to EventLayer relations with computed fields.

### Optimized

SeqLock is even faster.

## 32.15.1

### Fixed

Make SourceGenerator code valid :D

## 32.15.0

### Added

Support for PersistedName in Metadata generated by SourceGenerator.

## 32.14.1

### Fixed

Iterating over Relation with computed fields.

## 32.14.0

BonSerializer knows how to serialize `Type`. Only simple types and registered types are supported.

## 32.13.1

### Added

`record` support in ObjectDB. Also added validations for missing getters and setters in ObjectDB.

## 32.13.0

### Added

New KeyValueDBOptions.AutoAdjustFileSize which allows to automatically adjust split size for DB to be in range of 16MB
up to 1GB. FileSplitSize does not need to be set as it is ignored when AutoAdjustFileSize is true.

## 32.12.1

### Fixed

Dictionary should support `ReadOnlyMemory<byte>` as value.

## 32.12.0

### Added

New option in EventLayer and EventLayer2 to forbid serialization of lazy DB objects like `IDictionary<,>`

## 32.11.0

### Fixed

`Excluded` in start of range in `ListBy` when it is not last property of key now works correctly.

## 32.10.0

## 32.9.0

### Improved

Compaction does not need so much free space anymore.

## 32.8.0

### Fixed

RemoveById rare random crash in tables with multiple secondary indexes, and value which fits to 7 bytes.

### Added

Relations support for computed fields.

## 32.7.1

### Fixed

Default Conversion Generator was not thread safe.

## 32.7.0

### Updated

for .Net SDK 8.0.300

### Improved

RelationInfo.Prefix is now public.

## 32.6.2

## 32.6.1

## 32.6.0

## 32.5.0

### Added

Constraint String ExactCaseInsensitive.

### Fixed

Restore some IOC behavior from 31.x: ResolveOptionalKeyedIEnumerableShouldReturnNullForNothingRegistered

## 32.4.29

## 32.4.28

## 32.4.27

## 32.4.26

### Improved

Check type validity for serialization also in release build.

## 32.4.25

### Improved (another attempt)

Better exceptions in some cases.

## 32.4.24

### Improved

Better exceptions in some cases.

## 32.4.23

### Fixed

Critical bug in Brotli decompression MemReader.

## 32.4.22

### Fixed

Fixed false positive report in ReportTypeIncompatibility.

## 32.4.21

### Fixed

IOC Transient in Singleton verification works correctly for Func dependencies.

## 32.4.20

### Fixed

IOC skipping copy constructor.
SourceGenerator: support for [Generate] on records.

## 32.4.19

### Fixed

IOC ResolveOptionalKeyed should not fallback to keyless registration.

## 32.4.18

## 32.4.17

## 32.4.16

## 32.4.15

## 32.4.14

## 32.4.13

## 32.4.12

## 32.4.11

## 32.4.10

## 32.4.9

## 32.4.8

## 32.4.7

### Added

Allow to use `[GenerateFor(...)]` on interfaces.

## 32.4.6

### Fixed

IOC: Obsolete is ignored. Required fields not make it not compilable. Protected classes are skipped in generation.

## 32.4.5

## 32.4.4

### Fixed

IOC and Singletons are now much faster and correct in complex cases.

## 32.4.3

### Fixed

IOC: Func and delegates didn't worked in some cases.

## 32.4.2

### Fixed

IOC: Lazy has now correct type (not just always Lazy<object>)

## 32.4.1

## 32.4.0

## 32.3.2

### Fixed

Multiple partial classes in IOC. Generated one class from BTDB, so it could be used in IOC.

## 32.3.1

### Fixed

SourceGenerator: one more place where global:: is needed. And it must ignore static fields and properties.

## 32.3.0

### Added

New ContainerBuilder method `RegisterTypeWithFallback` which allows to register type with fallback to invokers if type
is not generated (useful for generic types for now).

### Fixed

IOC Default parameters for constructors are now correctly generated.
Skip generic classes in generation.

## 32.2.0

## 32.1.0

### Added

IOC RegisterTypeWithConstructorParameters which uses reflection to use different constructor than with most parameters.
IOC Also by default does not fallback to Reflection.
Bon has Span based api and is faster.

## 32.0.3

### Fixed

Source Generator nuget package is now correctly published.

## 32.0.2

## 32.0.1

## 32.0.0

### Added

`Constraint.Exact<T>(T value)` for bool, string, DateTime, (un)signed, Enum, Guid including nullable form.
`Constraint<T>.Any` for bool, string, DateTime, (un)signed, Enum, Guid including nullable form.
`Constraint.DateTime.Range` faster than Predicate.

### Fixed

Random rare corruption of DB when using InKeyValue.

### Added

Contraint.Exact for Guids.

### Improved

Exception message for impossible state.

### Fixed

Detection of NeedFreeContent was too pessimistic for some cases making some methods slower.

### Breaking change / Improvement

IOC does not use Runtime IL code generation, but SourceGenerator. Some features were lost, some features could be sill
added back if there will be demand. Assembly scanning was changed most. And all resolvable classes had to be marked by
`[Generate]` attribute on itself or any parent classes or interfaces.
It does allow to use IOC in AOT builds. More features will follow.

## 31.12.7

### Fixed

Fixed StackOverflowException in NeedFreeContent detection.

## 31.12.6

### Fixed

Last 2 versions reverted and NeedFreeContent detection is now using different approach.

## 31.12.5

### Fixed

"I" prefixes are now removed only on first level of generic types.

## 31.12.4

### Fixed

Prefixed RemoveById now correctly works with OnBeforeRemove.

## 31.12.1

### Fixed

Fix crash in ReadStringInUtf8

## 31.12.0

### Added

Automatic conversion from Nullable to its underlying value.

## 31.11.3

### Fixed

Upgrading to nullable property in secondary key.

## 31.11.2

### Fixed

Now "ById" methods should really work.

## 31.11.1

### Fixed

InKeyValues should not be required in most "ById" methods.

## 31.11.0

### Added

Validation that InKeyValues must be after all PrimaryKeys.

### Fixed

Regression ShallowUpdate when value is complex.

## 31.10.0

### Added

New more succinct way to declare InKeyValue.

## 31.9.0

### Fixed

Bug in ReadOnlySequenceSpanReader.ReadBlock.

### Added

Huge feature in relations InKeyValue allowing optimize modification and search of small number of fields into Primary
Key. Documentation will come in future commit.

`OnBeforeRemove` attribute to help to implement foreign keys prevent removal or cascading delete.

## 31.8.0

### Removed

Now DB trl files are never resized, so method `SetSize` is never used now and its implementation does not matter.

### Optimized

Speed of Compactor scanning all used files.

## 31.7.0

### Added

Async versions of DB Import and Export.

## 31.6.0

### Added

XXHash64 implementation, very fast noncryptographic hash. Fletcher checksum slightly optimized.

## 31.5.2

### Fixed

Change type of db field from enum to nullable enum does not report upgrade incompatibility.

## 31.5.1

### Fixed

ReadString and SkipString could rarely randomly crash. (had pointer instead of managed reference without pinning buffer)

## 31.5.0

### Added

`BonBuilder.builder.EstimateLowerBoundSize()` method.

## 31.4.3

### Fixed

IOC: Lazy Singletons were not threadsafe

## 31.4.3

### Fixed

# IOC: Lazy Singletons were not threadsafe

### Added

`BonBuilder.builder.EstimateLowerBoundSize()` method.

> > > > > > > e7279773... BonBuilder.builder.EstimateLowerBoundSize()

## 31.4.2

### Fixed

`ReadOnlySequenceSpanReader`

## 31.4.1

## 31.4.0

### Added

`ReadOnlySequenceSpanReader`

### Improved

ODbDump `leakscode` has more memory optimal variant db on disk to reduce memory usage on big db's

ODbDump `trldump` command outputs statistic about used space

## 31.3.2

### Fixed

In EventLayers Nested ValueTuples containing objects generated invalid IL code.
`SpanReader.ReadByteArrayAsMemory()` didn't worked correctly with `SpanReader.Controller!=null`.

## 31.3.1

### Improved

Diagnosing EventLayer serialization exceptions also for another another case.

## 31.3.0

### Added

`Constraint<Guid>.Any` is supported.

## 31.2.1

### Fixed

ReadOnlyMemory<byte> was not working in EventLayers.

## 31.2.0

### Improved

Diagnosing EventLayer serialization exceptions also for another case.

### Optimized

SpanReader.ReadStringOrdered is now up 15 times for long ascii only strings.

## 31.1.1

### Improved

Diagnosing EventLayer serialization exceptions.

## 31.1.0

### Added

CustomConverter now could be defined for EventLayer and it works for class types in both EventLayer and EventLayer2.

## 31.0.0

### Optimized

Decreased Compaction memory usage.

SpanReader.SkipStringOrdered is now 80+ times faster for long ascii only strings.

## 30.5.0

### Added

CustomConverter for Relations could be set even after opening DB.

## 30.4.0

## 30.3.0

### Added

Exposed `CompactorReadBytesPerSecondLimit` and `CompactorWriteBytesPerSecondLimit` properties so they could be modified
on the fly.

## 30.2.2

### Fixed

Regression in WriteString allocating too much memory if called multiple times.

## 30.2.1

### Fixed

Regression from 30.2.0 in SkipString.

### Optimized

SpanWriter.WriteStringOrdered is now faster especially for ascii only strings.

## 30.2.0

### Optimized

SpanWriter.WriteString is now 12+ times faster for long (100+) ascii only strings.
SpanReader.ReadString is now up to 14+ times faster for long ascii only strings.
SpanReader.SkipString is now 100+ times faster for long ascii only strings.

### Added

Support for `ReadOnlyMemory<byte>` type and end to end sample to stream Key,Bon relation enumerable though Stream.

## 30.1.0

### Added

New Bon constructor.

### Fixed

Very complex case of calling Remove before any deserialization on object with IDictionary changed to Dictionary.

### WIP

ReadOnlyDatabase = wait for final version.

## 30.0.0

### Breaking changes

Requires C#11 and .Net 7.0

## 29.12.1

### Fixed Regression from 29.12.0

Allow to register interfaces not just classes.

## 29.12.0

### Added

New OnSerializeAttribute to mark method on relation item to be run before `Insert`, `Upsert`, `Update` and all their
variants.

DB now throws exception when scalar value is tried to be stored as inline object (it didn't worked anyway).

DB Iterator notify and skips about missing values in Relations. ODbDump `dumpnull` prints number of such missing values.

New function `CalcBTreeStats` to calc count of node sizes.

ODbDump has new command `leakscodeapply` which enable apply output file from `leakscode` command on db (then can be
compared dump before and after clean - must be the same)

ODbDump `check` command was improved to not stop on first error, but to count all corrupted pairs.

ODbDump has new `fix` command to erase all corrupted pairs and also clear all secondary indexes so they could be rebuild
on next app start.

## 29.11.0

### Added

Exposed property `IObjectDB.AutoRegisterTypes`.

New method
`(ulong AllocSize, ulong AllocCount, ulong DeallocSize, ulong DeallocCount) IKeyValueDB.GetNativeMemoryStats();`.

## 29.10.1

### Fixed

FirstBySecondaryKey throwed when secondary key was modified new version.

## 29.10.0

### Added

New relation method `void RemoveAll()`, you should guess what it does.
New relation method `UpsertRange` and `As<T>`.
`OnCreate` callback for relations which helps to write upgrade methods.

### Optimized

VUInt pack unpack calculations.

## 29.9.1

### Fixed

Bug in ScanBy with Constraint.First (didn't properly skipped nonfirst) when used on last property of key.

## 29.9.0

### Added

Relations new FirstByIndex(OrDefault) methods. It is similar to GatherBy just sorting is much faster because it does not
need to be sorted.

`Orderer`.`GenericAscending` and `Orderer`.`GenericDescending` methods which just don't check property owner type
equality with relation item type.

`Bon` Binary object notation is allows creating and reading JavaScript/C# values with extensions like Dictionary/Map
into binary notation. It is much faster to parse, write, skip, search by keys than JSON, size will be also smaller in
most cases, in some cases much more smaller.

## 29.8.0

### Added

Non-generic `RegisterFactory` for your low level needs.

## 29.7.0

### Added

EventSerializer layer2 has new option to ignore known descriptors on input instances.

## 29.6.0

### Added

Generic `Constraint.First(ofConstraint)` - takes only first row from every group.

## 29.5.0

### Added

New `IKeyValueDBLogger` callback `ReportCompactorException`. Needs override with better logging of exception in
production applications.

## 29.4.0

## 29.3.0

### Added

Locale/Culture based orderer (`Orderer.AscendingStringByLocale`).
Also added `Orderer.Backwards(orderer)` order modifier.

## 29.2.0

### Fixed

Force ReferenceEquality when comparing objects serialized in DB.

## 29.1.1

### Fixed

Skipper from previous version generated invalid IL code which make violent crash, not in tests though :-(

## 29.1.0

### Changed

Unregistered object inside another object is skipped without throwing exception.

### Added

Improved ODbDump diskdump and trldump commands.

## 29.0.0

### Added

New RequireContentFree attribute to mark class to always use detection of IDictionaries on it to prevent Data Leaks.

Allow to use Brotli compression for Kvi. For now disabled by default. Added new parameter to logger so it is breaking
change, but easily fixable.

## 28.3.2

### Fixed

Previous fix was incomplete.

## 28.3.1

### Fixed

Kvi in Native Heap KVDB must be created in Exclusive mode.

## 28.3.0

### Added

Upgrade from IDictionary<K1,T1> to Dictionary<K2,T2> is possible and tested in Relations.

## 28.2.0

### Added

Enum Constraints.

Generic `Constraint<T>.Any` (not really generic just supporting all currently available constraints (bool, string,
DateTime, (un)signed, Enum))

## 28.1.2

### Fixed

ScanBy and GatherBy secondary index when DB is upgraded with secondary index removed.

## 28.1.1

### Fixed

Enums with multiple synonym labels for the same value (for example HttpStatusCode 300) can be stored

## 28.1.0

### Fixed

Loose Constraint on first position didn't found all matched records.

### Added

Relations `UpdateById` method for faster updates.

## 28.0.0

### Added

UpTo Constraints, which quickly skip rest of values.

## 27.0.3

### Fixed

ScanBy of empty relation does not throw Exception anymore.

## 27.0.2

### Fixed

Hopefully last bug in native heap fixed.

## 27.0.1

## 27.0.0

### Breaking Change

Added support for .Net 6.0. Removed support for .Net 5.0.

## 26.14.0

### Added

Constraints and ScanById relation method.

### Changed

Enum compatibility is now always like it was with BinaryCompatibilityOnlyAttribute. Please remove all usages of this
attribute from client code as it does not have any meaning.

## 26.13.0

DefaultTypeConvertorGenerator now supports `T` to `T?` conversion. Also `U` to `T?` if `U` is convertible to `T`.

## 26.12.1

Fixed native heap implementation.

## 26.12.0

## 26.11.0

### Added

A lot of important stuff in ObjectDB are public now.
Expanded interactive mode in ODbDump.

## 26.10.0

### Added

Relations now supports Items without default constructor.

## 26.9.1

### Fixed

Iterate only strictly abstract methods in Relation interfaces.

## 26.9.0

### Added

Name in DBOptions and show it in interesting exceptions. Same Name is also used for Key in deduplication of Relations.

## 26.8.0

### Added

Protection against using singletons for database objects or relation items.

## 26.7.0

### Added

ObjectDB+Relations+EventLayers support for Tuple and ValueTuple.

### BREAKING CHANGE

During enumeration of relation range is prevented also modification of secondary keys (until now was possible)

## 26.6.0

### Added

IOC can enforce uniqueness of registrations.

## 26.5.0

### Added

EventStore2Layer now correctly serializes generic classes.

`dynamic` deserialization of object now implements `IEnumerable<KeyValuePair<string, object>>` for easy iteration of
over all values and fields. `ITypeDescriptor` has new property
`IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields { get; }` which returns list of all fields on Object
descriptor, for all other descriptors it returns empty list.

## 26.4.0

### Added

Optimization to skip reading value in relations when partial view is subset of primary key.

## 26.3.1

### Fixed

Tried to workaround rare problem with compactor corrupting DB.

## 26.3.0

### Removed

Support for .NetCore 3.1. Only .Net 5.0 is supported now.

### Added

ODbDump has new command `fulldiskdump` containing also content of secondary keys

ODbDump has new command `trldump` listing operations in .trl files

New interface `IFieldHandlerLogger` for logging type incompatibilities. Sample usage
`new DBOptions().WithFieldHandlerLogger(new DefaultFieldHandlerLogger(s => Console.WriteLine(s)))`.

Possibility to iterate all transactions. Each transaction also has new getter `CreatedTime` and `IsDisposed()`.

## 26.2.2

### Fixed

Relations GetEnumerator didn't reset enumerator correctly in all cases.

## 26.2.1

### Fixed

Random crash in IsFreeContentNeeded due to multiple threads could run this in parallel.

## 26.2.0

### Added

In debug mode throws when using in listing by partial key excluded start proposition. In release mode kept current
behavior.

### Fixed

IOC Nullable values as instance dependencies.

## 26.1.1

### Fixed

Deleting indirect will not throw when value is null.

## 26.1.0

### Added

ODbDump has new command `findsplitbrain` which can find first commit when content of relation in two instances of DB
diverted - DBs needs to contain enough preserved history

`IRelation` has new `SerializeInsert` method which allows speed up DB insert by preparing simple CreateKeyValue commands
ahead of writing transaction.

## 26.0.0

### Fixed

Compactor sometimes removed unused files too late.

### Removed

`ReadOnlyListArrayWrapper` not used internally anymore and it is too confusing.

### Added

New methods`TransactionLogCreated` and `FileMarkedForDelete` in `IKeyValueDBLogger` for logging file collection
operations

## 25.5.0

### Fixed

PositionLessStreamProxy supports also stream which does not read all bytes, but are not at the end.

## 25.4.0

### Added

Default conversion from `T` to `IList<T>`.

## 25.3.0

### Fixed

Regression with Kvi file contain garbage at end. Enable `LenientOpen` to allow open DB with this corruption.

## 25.2.0

### Added

netcoreapp3.1 framework could be targeted again.

## 25.1.0

### Added

`LenientOpen` to `KeyValueDBOptions` (default false), in strict mode it does not try to recover as much as possible
data, but starts with empty DB. When such problem is detected it will call new method `LogWarning` from
`IKeyValueDBLogger`.
Also you can newly pass implementation of `IKeyValueDBLogger` into `Logger` field in `KeyValueDBOptions`.

## 25.0.1

### Fixed

Regression in SpanReader.CheckMagic which caused incomplete DB open in some cases.

Detect transaction leaks for writing transactions as well.

## 25.0.0

### Breaking change

Whole code was spanificated and cleaned from old unused in production parts.

### Added

IOC Container Build can verify that Singletons does not depend on transient objects.

## 24.8.0

### Added

Some missing IL Helpers.

`Dependency` attribute inheriting from `NotStored` attribute.

## 24.7.0

### Added

Support for serialization of type `StringValues` from Microsoft.Extensions.Primitives.

Support for serialization of properties with private setter in base class.

## 24.6.1

### Fixed

EventLayer Dictionary, List deserialization didn't reset variable, so instead of null there could be previous reference
when nested in another List or Dictionary.

## 24.6.0

### Added

Support for `DateTimeOffset` type in serialization.

## 24.5.0

### Added

IL: Allow to define parameter names on constructor so it is usable in IOC.

### Fixed

IOC: Do not crash on constructors without defined parameter names.

## 24.4.0

### Added

Relations: Skip already implemented methods in Relation interface.

## 24.3.0

### Added

ObjectDB: Generic classes supported as DB objects.

## 24.2.0

### Added

ObjectDB: Classic DB objects and Singletons could be created by IOC.

ObjectDB: Iterator decrypting encrypted strings.

## 24.1.0

### Added

IOC: `ResolveOptional` new method works like in Autofac.

Relations: ObjectDB in DB options allow to specify IOC Container which is then used for creating items in relation with
fallback to simple new if not registered.

## 24.0.2

### Fixed

Ignore static properties on DB Objects.

## 24.0.1

### Fixed

KeyValueDB: Value of length exactly 7 bytes are in memory again.

## 24.0.0

### Fixed

Relation self healing when primary key is changed works in more cases.

### Added

Made public Owner getters in Transactions. Allowed to change Max Transaction Log size using public property.
Some performance optimizations.

## 23.1.0

### Added

Relations throws informative exception when trying to use unsupported RemoveBySecondaryKey.

### Fixed

Performance regression in RemoveById when type didn't contain any IDictionaries.
Failure to cast in FreeContent in ODBDictionary.

## 23.0.0

### Breaking changes

- Visitor StartRelation gets whole info instead of just name of relation.
- Public fields on Relation Rows and Database objects are forbidden unless they have `[NotStored]`. In next version they
  could became supported.

## 22.2.2

### Fixed

- Final fix for IDictionaries

## 22.2.1

### Fixed

- IDictionaries has uniqueness fix also for keys.
- RelationInfo.GetProperties skips "NextInChain" IRelation property.

## 22.2.0

Important note: Don't forget to commit Transactions which calls GetRelation first time (auto registering them).

### Added

- `ICovariantRelation<out T>` cannot have upsert, but it will implement `IRelation<T>` anyway.
- removed class constraint because it created strange problems. It is now enforced in runtime.

### Fixed

- IDictionaries now supports types with same name but different namespaces again.

## 22.1.0

### Added

- `IsReadOnly()` on `IKeyValueDBTransaction` and used internally in auto registering Relations directly not just in
  writing transactions, but also in all non read only.

## 22.0.0

### BREAKING CHANGE

- GetRelation(Type type) now returns IRelation type
- RelationInfo public getters changed many types to ReadOnlyMemory.
- Removed useless UniqueIndexAttribute

### Added

- big chunk of IL generated code for Relations and ODBDictionary/Set is now cached forever, making it faster to
  instantiate Relations repeatedly. It adds new limitation that instances of `ITypeConvertorGenerator` and
  `IFieldHandlerFactory` needs to be same over process runtime.

## 21.0.0

### BREAKING CHANGE

- Relations interfaces needs to be inherited from `IRelation<T>`

### Added

- `IRelation<T>` and `IRelation` interfaces. You will get `Upsert` method for free.

```C#
    public interface IRelation<T> : IReadOnlyCollection<T>, IRelation where T : class
    {
        bool Upsert(T item);
    }

    public interface IRelation
    {
        Type BtdbInternalGetRelationInterfaceType();
        IRelation? BtdbInternalNextInChain { get; set; }
    }
```

- `IObjectDbTransaction` has new methods

```C#
    object GetRelation(Type type);

    T GetRelation<T>() where T : class, IRelation
    {
        return (T)GetRelation(typeof(T));
    }
```

These lazily creates instance of relation for current transaction. If it is for first time it will also create it (in
current transaction if it is writable, or in new writable transaction). It automatically names relation by
`T.ToSimpleName()` or uses `PersistentName` attribute on `T`.

You can register your own custom relation factory by using
`void IObjectDB.RegisterCustomRelation(Type type, Func<IObjectDBTransaction, object> factory);`.

You can forbid automatic registration of relations by `IObjectDB.AllowAutoRegistrationOfRelations = false`. Good in
production code to allow auto registration only during initial transaction.

### Fixed

- regression in 20.x in compatibility of Enums in relations with `BinaryCompatibilityOnly` attribute.

## 20.3.0

### Added

- ReadOnly option to opening DB. ODbDump using it and allows to pass ulongcommit as third parameter to open DB in
  historical moment.

### Fixed

- Mixing Lists, Sets and Arrays in EventLayers

## 20.2.0

### Added

- IOC now support `Dependency` attribute for properties injection. Also it could be used for renaming dependency
  resolved name. Nullable reference types are optional dependencies.

## 20.1.0

### Added

- IOC now supports public properties injection. Registration needs to be done with `PropertiesAutowired()`. Setters does
  not need to be public. Nullable reference types are optional dependencies, all other properties are required.

## 20.0.0

### Added

- Added support for `IOrderedSet<T>` lazily stored set.
- EventLayers deserialization can now unwrap `IIndirect<T>`, making it compatible change (`IIndirect<T>` => `T`, or
  `IDictionary<TKey, IIndirect<T>>` => `IDictionary<TKey, T>`).
- New documentation for [supported types](Doc/SupportedTypes.md)
- Added support for `ISet<T>`, `HashSet<T>` with identical serialization as `IList<T>`.
- Removed some allocations from `IOrderedDictionary`

## 19.9.3

### Fixed

- Regression with DB loading `IDictionary<Key,IIndirect<SomeAbstractClass>>`

## 19.9.2

## 19.9.1

### Fixed

- Regression from 19.8.0 with NullReferenceException in some special cases.

## 19.9.0

### Added

- Suffixes for partial deserializations in methods (FindBy,ListBy) does not need to be separated by underscore anymore.

## 19.8.0

### Added

- Relations now support returning only partial classes. For example it allows to speed up table scanning because you can
  deserialize only fields you need when enumerating relation.

## 19.7.1

### Fixed

- Made AesGcmSymmetricCipher thread safe.

## 19.7.0

### Added

- Support `EncryptedString` in DB indexes (orderable).

## 19.6.0

### Added

- New `EncryptedString` type to be able to store string in its encrypted form. You need to pass `ISymmetricCipher`
  implementation to `DBOptions`, `TypeSerializersOptions`, `EventSerializer` and `EventDeserializer`. There is class
  `AesGcmSymmetricCipher` implementing `ISymmetricCipher`, which provides perfect security by just passing 32 bytes key
  to its constructor.

## 19.5.0

- Added possibility to deserialize event with Nullable to dynamic (usable for dumping EventStore)

## 19.4.0

### Added

- All serializations DB, Event now supports `System.Version` type. Default conversion allows to upgrade from `string` to
  `Version`. When `Version` is used in ordering, keys it behaves as expected.

### Fixed

Removed control flow by exceptions from `EnumerateSingletonTypes`. Fixes #85.

## 19.3.0

### Added

Relations new methods `AnyById` and `AnyBy{SecKeyName}` supported.

## 19.2.0

### Added

#### Relations

- New methods `CountById` and `CountBy{SecKeyName}` supported.
- `IEnumerator` and `IEnumerable` could be freely exchanged as result types.
- `ListById` and `ListBy{SecKeyName}` does not require `AdvancedEnumeratorParam`.

## 19.1.0

### Changed

Range defined by EndKey s KeyProposition.Included now contains all keys with passed prefix, used in methods:
`RemoveById` `ListById` `ListBy{SecKeyName}`

## 19.0.0

### Breaking change

Needs to be compiled with in csproj:

    <LangVersion>8</LangVersion>
    <Nullable>annotations</Nullable>

`StructList.Add()` renamed to `AddRef()`.

### Added

IOC: IAsyncDisposable is not registered by AsImplementedInterfaces (same behavior as IDisposable).

## 18.2.2

### Fixed

Another bug in FindLast made me to delete it and rewrite again from managed heap implementation.

## 18.2.1

### Fixed

Bug in FindLast in native heap KVDB.

## 18.2.0

### Added

EventLayers Deserialization now supports classes without parameter-less constructor.

## 18.1.0

### Fixed

Find with prefix sometimes found records not matching prefix.

### Added

Improved CalcStats information.
Some small speed optimizations.

## 18.0.0

### Changed

Supports only .Net Core 3.0 or better.

### Added

New BTreeKeyValueDB implementation which uses native heap.

## 17.10.0

### Added

New method in relations: ShallowRemoveById
StartWritingTransaction returns ValueTask and optimized allocations.

## 17.9.0

### Added

ODBIterator extended to be able to seek and display only what is needed.

## 17.8.0

### Added

New method `IPlatformMethods.RealPath` for platform independent expanding of symlinks.

## 17.7.0

### Added

New method `ByteBuffer ByteBuffer.NewAsync(ReadOnlyMemory<byte> buffer)`.

FullNameTypeMapper improved support for generics. Types can migrate assemblies even for generic arguments. (
by <https://github.com/JanVargovsky>)

### Changed

Supports only .Net Core 2.2 or better.

## 17.6.0

### Fixed

- When preserving history KVDB did not advising compaction without restarting application.

### Changed

- Default CompactorScheduler wait time to 30-45 minutes.

## 17.5.2

## 17.5.1

### Fixed

- Compactor does not ends in endless cycle when DB is opened with more than 4 times smaller split size than it was
  created.

## 17.5.0

### Added

ODbDump has new commands

- `leaks` which prints out unreachable objects in DB.
- `frequency` which prints number of items in relations and top level dictionaries in singletons

### Fixed

ODBLayer correctly supports interfaces in properties.

## 17.4.2

### Fixed

Additional nonderministic info removed from compare mode of ODbDump.

## 17.4.1

### Fixed

ODbDump is now published in way it works not just on my machine.

## 17.4.0

### Added

ODbDump is now part of release. ODbDump has new dump mode useful for comparing DBs.

## 17.3.0

RemoveById supports advanced enumeration param in relations

## 17.2.0

### Added

Extend TypeSerializers with optional configuration options.

Options consist of one option for `IIndirect<T>`, whether it is serialized or ignored.

## 17.1.0

### Added

Way to limit Compactor Write and Read Speed by setting `KeyValueDBOptions`. Default is unlimited.

## 17.0.1

### Changed

Added new method into IFileCollectionFile.AdvisePrefetch. It is called during DB open on files which are expected to be
read by RandomRead.

## 17.0.0

### Changed

IFileCollection modified to allow faster implementations possible.

## 16.2.1

### Fixed

PRead behavior on Windows file end. Fixed Nested Dictionaries type gathering exception.

## 16.2.0

### Fixed

Failure to open DB in special case after erasing and compaction.
Type check when generating apart fields in relations.

## 16.1.0

### Added

Reintroduced PossitionLessStream and rename FileStream one to PossitionLessFileStream

## 16.0.0

### Improved

Much faster compaction when a lot of changes were done. New IKeyValueDB.CompactorRamLimitInMb does limit RAM usage for
longer time.
Speed of OnDiskFileCollection improved by using new PRead and PWrite methods implemented for Windows and Posix.
Better exception in WriteInlineObject when object type could not be stored.

### Breaking Changes

Modified IKeyValueDBLogger and IKeyValueDB so implementation needs to be modified.

### Fixed

Skipping Events in EventStoreLayer Deserialization

## 15.1.0

### Added

Added way to skip Events in EventStoreLayer Deserialization.

### Fixed

Deletion of dictionaries during update/delete in relation in subclasses when not defined in declaration by interface.

## 15.0.0

### Added

ShallowUpsert and ShallowUpdate relation methods which does not try to prevent leaks, but are much faster.

### Changed

IIndirect objects are not automatically deleted during removal from relations.

## 14.12.2

### Fixed

Calling ListBy{SecondaryKey}OrDefault for not existing item during enumerating relations cooperates well.

## 14.12.1

### Fixed

Exception in EventStore2Layer serialization does not corrupt next serializations anymore.
Serialization of non Dictionary in EventStore does not fail.

## 14.12.0

### Fixed

Skipping removed field (inline object) when deserializing older version in relations

## 14.11.0

### Added

EventLayer serializers support IOrderedDictionary<K,V> type

## 14.10.0

### Added

ArtInMemoryKeyValueDB - less memory hungry KVDB - use it only in .NetCore 2.1 target
Generics classes now supported in EventLayer serializers
DBOptions.WithSelfHealing switches db to try self heal rather then fail fast mode
IObjectDBLogger for ObjectDB, actually for reporting deletion of incompatible data in self heal mode.

## 14.9.0

### Fixed

Rare exception during checking possibility of usage of optimized version of prefix based remove when so far unseen
objects was used as key in IDictionary

## 14.8.0

### Changed

Dumping JsonLike output from TypeDescriptor is now more JSON compliant.

## 14.7.0

### Added

Delegate constrains are now supported in C# 7.3, so it now makes compile time errors instead of runtime where possible.

### Fixed

Rare failure in IOC when running in parallel.
Better exception message when types of fields in Deserialization are different.

## 14.6.0

### Added

Relations could be now iterated.

## 14.5.2

### Fixed

Event Deserialization does not eagerly require Types exists in List and Dictionary.
Iterator fix for back reference in inlined lists and dictionaries. Now really works ;-)

## 14.5.1

### Fixed

Shut up Coverity. DiskChunkCache findings are false positives.
Iterator fix for back reference in inlined lists and dictionaries.
EventStore2Layer Deserializer bug in specific case.

## 14.5.0

### Added

Allow to use DateTime.MinValue and DateTime.MaxValue in ordered context - they will be automatically converted to UTC.

## 14.4.0

### Fixed

ListBy... methods in relations now correct type of AdvancedEnumeratorParam<T>

## 14.3.0

### Fixed

Made order of properties in EventSerializers stable by sorting them by name.

## 14.2.1

### Fixed

EventLayer2 serialization of `Dictionary<int, Dictionary<int, ComplexObject>>` property.

## 14.2.0

### Changed

IOC RegisterInstance<T>(object value) now must be explicitly used so new RegisterInstance(object value) could be used.

## 14.1.0

### Added

IOC RegisterInstance(object value) overload.

## 14.0.0

### Added

RollbackAdvised property on KV and Object transactions interfaces to simplify notification of some infrastructure code
to rollback transaction instead of committing it.

Relations support inheriting of methods from other interfaces.

### Breaking change

IOC RegisterInstance<T>(T value) now allows also value types as T, and value is not registered as value.GetType() but as
typeof(T), which is same behavior as AutoFac.

## 13.1.0

### Added

Possibility to limit number of removed items at once in prefix based remove in relations.

## 13.0.0

### Added

Ported to .NetCore 2.0.

Simplified Nuget. Now it embed pdb and with SourceLink. GitHub releases contains zipped BTDB sources.

New releaser project to automate releasing.

## 12.7.0.0

### Fixed

Compactor Inducing Latency for writting transaction is now capped.

## 12.6.1.0

### Fixed

Fixed Nullable support in EventStore2Layer.

## 12.6.0.0

### Fixed

Uninterupted influx of data after db opening can prevent compactor from running.

## 12.5.1.0

### Fixed

IPAddress can now serialize and deserialize null value.

## 12.5.0.0

### Added

- Synchronization lock in EventLayer2 Deserialization to be on safe side.

## 12.4.0.0

### Added

- PersistedNameAttribute is supported on Apart Fields in relation interfaces

## 12.3.0.0

### Added

- IOC now resolves optional parameters that are not registered with its provided value

### Fixed

- Fixed problem with calculating index from older version value in specific case

## 12.2.0.0

### Added

- new method DeleteAllData() on ObjectDBTransaction
- PersistedNameAttribute is additionally allowed on interfaces - useful for Relations

## 12.1.0.0

### Added

- Event deserialization now automatically converts Enums to integer types.

## 12.0.0.0

### Added

- Changelog
- Nullable support in both ODb and EventStore
