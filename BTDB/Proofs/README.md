# BTDB proofs

Lean 4 models of core BTDB logic, with machine-checked proofs. They use only the Lean core library (no Mathlib).

```bash
lake build                 # checks every proof
lake env lean Check.lean   # prints the axioms each main theorem uses
```

`lean-toolchain` pins Lean 4.35.0; install it with [elan](https://github.com/leanprover/elan). The theorems depend
only on the standard axioms `propext`, `Quot.sound` and `Classical.choice`; there is no `sorry`.

## Models

`BtdbProofs/KeyRange.lean` models the range test of relation queries (`RelationQueryRange.Contains` and `IsPastStop`
in `../ODBLayer/RelationQuery.cs`). Keys are byte lists compared like `ReadOnlySpan<byte>.SequenceCompareTo`. A key in
a range is the range prefix, the encoded value of the bounded field and the rest of the key. The field's encoding is
any prefix-free encoding that orders bytes like values, which is what `RelationQueryKeyRanges.IsOrderPreserving`
requires of the handlers it admits (`RelationQueryKeyOrderTest` checks the handlers).

`BtdbProofs/VUInt.lean` models `PackUnpack.UnsafePackVUInt`, the key encoding of the "Unsigned" field handler, for
every 64-bit value: a length marker in the top bits, then the value in big-endian bytes. Its constants are taken from
the code by inspection.

`BtdbProofs/VInt.lean` models `PackUnpack.UnsafePackVInt`, the key encoding of the "Signed" field handler, for every
64-bit value. Values fall into 15 classes of one length and sign; a class encodes `base + v` in big-endian bytes, where
the code's XOR of mask bits that are all set is a subtraction from the positive base. Its constants are also taken
from the code by inspection.

`BtdbProofs/Lengths.lean` connects those models to the branchless length functions `PackUnpack` runs: `LengthVUInt`
and `LengthVInt` from the leading zero count of the value, and `LengthVUIntByFirstByte` and `LengthVIntByFirstByte`,
which readers use to decode, from the first byte.

`BtdbProofs/Unpack.lean` models `UnsafeUnpackVUInt` and `UnsafeUnpackVInt` for each length, as the code reads the
bytes: big-endian reads of 1, 2, 4 or 8 bytes, masks, shifts and the sign correction chosen by the first byte.

Enums write signed values with `WriteVInt*` and unsigned ones with `WriteVUInt*`, which forward to the 64-bit
encoders, so they use the encodings above. `KeyRange.byte_encoding` covers the one-byte handlers. Together, every
handler `RelationQueryKeyRanges.IsOrderPreserving` admits is an encoding for which `lower_correct` and `upper_correct`
hold, over the values of its type.

`BtdbProofs/Cursor.lean` models `BTreeKeyValueDBCursor.Find(key, prefixLen)`: the B-tree's `Find(key)` followed by
the restriction to keys with the prefix, against the contract of `IKeyValueDBCursor.Find`. The B-tree's own `Find` is
assumed to meet that contract over all keys.

`BtdbProofs/Seek.lean` models `Seek` and `SeekLast`, which position a relation query scan on the first key of a range
with a lower bound and on the last key with an upper bound, from that contract and those of `FindNextKey`,
`FindPreviousKey` and `FindLastKey`.

`BtdbProofs/Count.lean` models `RelationDBManipulator.CountWithProposition`, which counts the keys of an advanced
enumerator parameter range from the key indices `Find` reaches for each bound, after moving an included end and an
excluded start to the last key extending them (`FindLastKeyWithPrefix`). It backs the generated `CountBy` and `AnyBy`
methods and bounds the items `RemoveByIdAdvancedParam` visits.

`BtdbProofs/StringOrdered.lean` models `MemWriter.WriteStringOrderedPrefix` and `WriteStringOrdered`, the key encoding
of the "StringOrderable" handler, over UTF-16 code units: a surrogate pair is one token, every other unit its own, each
written as a variable-length integer. It justifies the prefix scan that relation queries use for an ordinal
`StartsWith`.

`BtdbProofs/StringKeyOrder.lean` uses that model for the order of whole string keys.

`BtdbProofs/ReadOnlyCompare.lean` models `ReadOnlyKeyValueDBCursor.Compare`, which compares a key the read-only
database stores as a node's shared prefix and a suffix, cut to a maximum length, with a search target.

`BtdbProofs/Boundary.lean` models the binary search that `ReadOnlyKeyValueDBCursor.FindBoundary` runs in each node,
and its descent through branch nodes whose separators are the first keys of all children but the first, as
`ReadOnlyCreator` writes them.

`BtdbProofs/Creator.lean` models how `ReadOnlyCreator.WriteBranches` groups the children of a level into branch nodes,
greedily up to `MaxChildren` children and `MaxNodeKeyBytes` bytes of separators.

`BtdbProofs/RangeEnumeration.lean` models how the advanced enumerators of relations, `ODBSet` and `ODBDictionary` end
a descending range while the caller removes each listed key.

`BtdbProofs/Replay.lean` models how `BTreeKeyValueDB.LoadTransactionLogCore` replays the transaction log: it applies a
transaction at its commit and ends at content it cannot apply, dropping the open transaction.

`BtdbProofs/Separators.lean` models how `BTreeBranch.EraseOne` and `CreateOrUpdate` of the in-memory B-tree rebuild a
branch's separators with array copies, after erasing a key and after splits.

`BtdbProofs/GrowRead.lean` models how `MemReader.ReadByteArray` reads a declared length beyond the buffered bytes:
it starts with the buffered bytes (at least 4096, at most the length), reads, and doubles up to the length until the
array is full or the input ends. It also models `ReadStringBeyondBuffer`, which grows a character array when the next
one or two UTF-16 units do not fit.

`BtdbProofs/FileGuid.lean` models how `FileCollectionWithFileInfos` picks the database Guid from its files' headers;
files with another Guid are foreign and deleted on open.

`BtdbProofs/FileIds.lean` models `FileIdAllocator`, which hands out odd IDs to transaction logs and even ones to other
files.

`BtdbProofs/ReadBounds.lean` models the `ulong` test the file collections use to refuse a read past a file's end.

`BtdbProofs/SeqLockRead.lean` models the read protocol of `SeqLock` over a counter that only grows and data that changes
only while the counter is odd.

`BtdbProofs/ReadGrowth.lean` models `MemReader.ReadBlockGrowing`, which reads a block of a declared length into an array
that doubles as bytes arrive.

`BtdbProofs/ChunkIndexLoad.lean` models how `ChunkStorageInKV` loads its hash index files, one written per open.

`BtdbProofs/EquatableHash.lean` models the equality and hash of `EquatableArray<T>`, whose default value equals an empty
array.

`BtdbProofs/IocSelfWait.lean` models the IOC container's wait for a singleton or scoped instance another resolve is
creating, under a reentrant lock.

`BtdbProofs/BonItemBound.lean` bounds the items a Bon container can hold by the bytes of its buffer.

`BtdbProofs/SingletonCache.lean` models `TableInfo.SingletonContent` and `CacheSingletonContent`, the singleton content
cache shared by the transactions of an `ObjectDB`, with readers storing what they read at any later time.

`BtdbProofs/SingletonOid.lean` models how the oid of a table's singleton, allocated by whichever transaction first asks
for it, gets stored with the table info by a writer.

`BtdbProofs/CompactionBatch.lean` models how `CompactorScheduler` ends a batch of due compactions that a removed
database cancelled or a compaction failed.

`BtdbProofs/RelationRegistry.lean` models how `ObjectDB.InitializeRelations` publishes the relation registry it
copied, while `InitRelation` adds relations to the current one in place.

`BtdbProofs/TypeNames.lean` models how `Type2NameRegistry.RegisterType` gives ObjectDB types their stored names.

`BtdbProofs/EnumerateDirty.lean` models how `ObjectDBTransaction.Enumerate` adds the transaction's objects not stored
yet to the stored ones.

`BtdbProofs/ObjectRestore.lean` models how `ObjectDBTransaction` tracks a transaction's objects across `Store`,
`Delete` and `Singleton`.

`BtdbProofs/EventBlocks.lean` models how `AppendingEventStore.Store` splits an event into blocks with headers, and
which of them `ReadOnlyEventStore.ReadToEnd` reads within the maximum file size.

`BtdbProofs/InlineNumbers.lean` models the bound `DBReaderCtx` puts on the inline object numbers it reads.

`BtdbProofs/ReadOnlyDepth.lean` models how deep the read-only database's descents go, and how deep the trees
`ReadOnlyCreator` builds are.

`BtdbProofs/KeyIndexOrder.lean` models the key order check `BTreeKeyValueDB.LoadKeyIndex` makes while building the
tree from a key index.

`BtdbProofs/FletcherDetect.lean` builds on the Fletcher-32 model for what the checksum guarding event store blocks
detects.

`BtdbProofs/CipherBlocks.lean` models the block check of `AesGcmSymmetricCipher.OrderedDecrypt`.

`BtdbProofs/Utf8Length.lean` models how many bytes `Encoding.UTF8` writes for UTF-16 code units, which sizes the stack
buffer of `ReflectionMetadata.FindByName`.

`BtdbProofs/HiddenProperties.lean` models how `EmitHelpers.GetVisibleProperties` leaves out properties hidden by one
of the same name in a derived class.

`BtdbProofs/InheritedAttributes.lean` models how the source generator reads the attributes of a property with those
of the properties it overrides.

`BtdbProofs/PropertyOrder.lean` models the order in which `EmitHelpers.GetVisibleProperties` lists a type's properties
for storage, private properties of base classes included.

`BtdbProofs/DependencyMembers.lean` models how the IOC container and the source generator collect the `[Dependency]`
members of a type and its base classes.

`BtdbProofs/UpdateByIdParams.lean` models how the source generator checks the key parameters of `UpdateById`,
InKeyValue fields included.

`BtdbProofs/PowerOfTwo.lean` models `HashHelpers.PowerOf2`, which rounds the bucket counts of the hash collections in
`BTDB/Collections/` up to a power of two.

`BtdbProofs/CapacityHint.lean` models `CollectionMetadata.CapacityFor`, the capacity collections read from data are
created with.

`BtdbProofs/TrlChain.lean` models how `BTreeKeyValueDB.LinkTransactionLogFileIds` walks the transaction logs back
over the predecessor IDs in their headers.

`BtdbProofs/KviDurability.lean` models the write order of `BTreeKeyValueDB.CreateKeyIndexFile`: what a crash keeps
of the transaction log and of a key index that points into it.

`BtdbProofs/PreservedIndex.lean` models which key indexes `BTreeKeyValueDB.CreateIndexFile` keeps when it writes a new
one under `PreserveHistoryUpToCommitUlong`.

`BtdbProofs/DictionaryFree.lean` models how `ODBDictionaryFieldHandler.FreeContent` frees nested dictionaries
(`../ODBLayer/`), over any graph of dictionaries damage can leave, with a bound on the nesting it may reach.

`BtdbProofs/EventMetadataIds.lean` models the type ids `EventSerializer` writes into a metadata log and the checks
`EventDeserializer.ProcessMetadataLog` makes on them (`../EventStore2Layer/`).

`BtdbProofs/BonReusedSlot.lean` models how `BonSerializer` loads the items of a list or dictionary through one reused
storage slot (`../Serialization/`).

`BtdbProofs/HashSetEntries.lean` models the walk over a `HashSet`'s entries with which Bon, the event store and ObjectDB
save it.

`BtdbProofs/InlineCycle.lean` models how `ObjectDBTransaction.WriteInlineObject` writes inline objects inside the
objects holding them (`../ODBLayer/`), over any graph of objects, with a bound on the nesting it may reach.

`BtdbProofs/ConverterStaging.lean` models where converting loaders and savers stage a value
(`../FieldHandler/Extensions.cs`).

`BtdbProofs/EmittedTry.lean` models the evaluation stack depth of the IL that `ListFieldOrderedHandler` emits to save a
list key part inside relation methods (`../FieldHandler/`, `../ODBLayer/RelationBuilder.cs`).

`BtdbProofs/DeserializerPlaceholder.lean` models how `BonSerializerFactory` creates the deserializers of the types a type
holds, caching placeholders for registered classes (`../Serialization/`), over any graph of types.

`BtdbProofs/SkipOnce.lean` models where the deserializers of a Bon class's members start reading
(`../Serialization/BonSerializer.cs`).

`BtdbProofs/CountCache.lean` models the count of a lazy `ODBDictionary` or `ODBSet` read through two instances over the
same keys (`../ODBLayer/`).

`BtdbProofs/MergeCycle.lean` models how `EventSerializer.MergeDescriptor` maps the descriptor an object loaded without its
type remembers (`../EventStore2Layer/`), over any graph of descriptors.

`BtdbProofs/ConvertingSaver.lean` models how a type descriptor's `GenerateSave` falls back to `BuildConvertingSaver`
(`../FieldHandler/Extensions.cs`, `../EventStoreLayer/ByteArrayTypeDescriptor.cs`).

`BtdbProofs/NestedRegistration.lean` models how `EventSerializer` registers a descriptor merged from an object loaded
without its type, and the descriptors nested in it (`../EventStore2Layer/`).

`BtdbProofs/LruCapacity.lean` models how `LruCache` and `SpanByteLruCache` take slots, grow and evict
(`../Collections/`).

`BtdbProofs/RemovePartial.lean` models how `RemoveByIdPartial` removes the rows of a key prefix that `OnBeforeRemove`
does not keep (`../ODBLayer/RelationDBManipulator.cs`).

`BtdbProofs/DispatcherOverloads.lean` models how the source generator picks the methods a generated dispatcher calls
on a handler (`../../BTDB.SourceGenerator/Main.cs`).

`BtdbProofs/DispatcherContainers.lean` models how a generated dispatcher keeps the handler factories of each root
container (`../IOC/IContainer.cs`).

`BtdbProofs/BonInteger.lean` models how Bon stores an integer as a sign and a magnitude and reads it as a `long` or a
narrower member (`../Bon/Bon.cs`, `../Serialization/BonSerializer.cs`).

`BtdbProofs/FirstDifference.lean` models how `TreeNodeUtils.FindFirstDifference` compares keys given in parts and
how `CalcCommonPrefix` builds on it (`../Buffer/TreeNodeUtils.cs`).

`BtdbProofs/PtrLenExclude.lean` models how `PtrLenList.TryExclude` removes the rest of a range that starts in a gap
(`../KVDBLayer/Helpers/PtrLenList.cs`).

`BtdbProofs/Fletcher.lean` models how `Checksum.CalcFletcher32` adds words to its sums and folds them
(`../Buffer/Checksum.cs`).

`BtdbProofs/DisposeRace.lean` models how a container tracks instances it owns while another thread disposes it
(`../IOC/ContainerImpl.cs`).

`BtdbProofs/MemoryStreamSize.lean` models the bytes and size of a memory stream under writes and `SetSize`
(`../StreamLayer/MemoryPositionLessStream.cs`).

`BtdbProofs/ProviderDisposal.lean` models which instances the BTDB container and the service provider dispose with
`UseBtdbIoc` (`../IOC/ServiceProviderIntegration.cs`, `../IOC/ContainerImpl.cs`).

`BtdbProofs/PtrLenInclude.lean` models how `PtrLenList.TryInclude` merges the ranges an included range starting in a
gap reaches (`../KVDBLayer/Helpers/PtrLenList.cs`).

`BtdbProofs/PtrLenFree.lean` models the scan of `PtrLenList.FindFreeSizeAfter` after its binary search and
`TryFindLenAndRemove` (`../KVDBLayer/Helpers/PtrLenList.cs`).

`BtdbProofs/XxHashStream.lean` models the buffering of `XxHash64.Update` over any block function
(`../Buffer/XxHash64.cs`).

`BtdbProofs/ExportFormat.lean` models the fields `KeyValueDBExportImporter.Export` writes and `Import` reads
(`../KVDBLayer/Helpers/KeyValueDBExportImporter.cs`).

`BtdbProofs/IocDefaults.lean` models how a container collects the registrations of a type and picks its default
(`../IOC/ContainerRegistrationContext.cs`, `../IOC/CReg.cs`).

`BtdbProofs/ChunkCachePut.lean` models how `DiskChunkCache.Put` starts new value files until its content fits
(`../ChunkCache/DiskChunkCache.cs`).

`BtdbProofs/SnappyExpansion.lean` models the input each Snappy element takes and the output it writes
(`../SnappyCompression/SnappyDecompress.cs`).

`BtdbProofs/ListKey.lean` models how `ListFieldOrderedHandler` and generated list key lookups write a list key: its
count, then its items.

`BtdbProofs/KeyConcat.lean` models a key of several fields written one after another, each but the last with a prefix
free encoding that keeps the order of its values.

`BtdbProofs/RoaringCount.lean` models how `ODBRoaringBitmap` stores, caches and counts its values when page commands
arrive without a count (`../ODBLayer/ODBRoaringBitmap.cs`).

`BtdbProofs/RoaringSource.lean` models how `RoaringBitmap.Source` reads its values up to the maximum index
(`../ODBLayer/RoaringBitmap.cs`).

`BtdbProofs/RoaringKind.lean` models how `RoaringBitmaps` chooses the encoding of a 16-bit container and reads its kind
back from its length (`../Buffer/RoaringBitmaps.cs`).

`BtdbProofs/QueryTopK.lean` models how a relation query ordered and then paged sorts its rows keeping only the first
`skip + take` (`../ODBLayer/RelationQuery.cs`).

`BtdbProofs/QueryPaging.lean` models how `RelationQuerySource.Bind` folds the `Skip` and `Take` calls of a relation
query into one skip and an optional take (`../ODBLayer/RelationQuery.cs`).

`BtdbProofs/InheritedDependencies.lean` models how the source generator collects the `[Dependency]` members of a type
and its base types (`../../BTDB.SourceGenerator/Main.cs`).

`BtdbProofs/BonNesting.lean` models the checks Bon readers make on container pointers (`../Bon/Bon.cs`), with damaged
item counts that let a container's items run anywhere after its body.

`BtdbProofs/SerializerPlaceholder.lean` models the placeholder `BonSerializerFactory` caches while it creates a class's
serializer or deserializer (`../Serialization/BonSerializer.cs`), with creations of one type running concurrently.

`BtdbProofs/HintNames.lean` models `UniqueHintName` of the source generator (`../../BTDB.SourceGenerator/Main.cs`),
which keeps the names of generated files unique ignoring case, as Roslyn requires: candidates are case-folded keys.

Not modelled: the B-tree itself, whose `Find` is assumed to meet the contract.

## Results

| Theorem | Statement |
| --- | --- |
| `KeyRange.lower_correct` | The lower-bound test accepts a key exactly when its value is above an exclusive bound, or at or above an inclusive one. |
| `VUInt.vuint_order`, `VUInt.vuint_prefix_free` | A smaller value has a smaller encoding, and no encoding is a proper prefix of another: the "Unsigned" encoding satisfies what `KeyRange` assumes. |
| `VInt.vint_order`, `VInt.vint_prefix_free` | The same for the "Signed" encoding, negative values included. |
| `VUInt.encoding`, `VInt.encoding`, `KeyRange.byte_encoding` | Each order-preserving handler's encoding is a `KeyRange.Encoding` over the values of its type. |
| `Lengths.length_vuint64`, `length_vuint32`, `length_vint64`, `length_vint32` | The branchless `LengthVUInt` and `LengthVInt` for 64- and 32-bit values, including the `LzcToVIntLen` table, give the lengths of the models above. |
| `Lengths.vuint_first_byte`, `Lengths.vint_first_byte` | The first byte of every encoding gives its length through `LengthVUIntByFirstByte` and `LengthVIntByFirstByte`. |
| `Unpack.vuint_unpack`, `Unpack.vint_unpack` | Unpacking the encoding of every 64-bit unsigned and signed value returns the value. |
| `Cursor.block` | Keys with a common prefix form a contiguous block in key order. |
| `Cursor.previous_in_block` … `Cursor.next_outside` | Each branch of `Find(key, prefixLen)` answers what the contract requires: the greatest smaller key of the block, else its smallest larger key, else not found. |
| `Seek.seek_exact` … `Seek.seek_exclusive_none` | Each branch of `Seek` reaches the first key of the range that passes the lower bound, inclusive or exclusive, or proves there is none. |
| `Seek.seek_last_inclusive` … `Seek.seek_last_next_none` | Each branch of `SeekLast` reaches the last key of the range not past the upper bound, inclusive or exclusive, or proves there is none. |
| `Count.window_correct`, `Count.window_bounds` | A key's index lies between the start and end index `CountWithProposition` computes exactly when the key is in the range, for every pair of propositions, and that window lies within the prefix, so `Math.Max(0, endIndex - startIndex + 1)` is the number of keys in the range. An excluded start skips every key extending the start bytes; an included end keeps every key extending the end bytes. |
| `StringOrdered.starts_with_correct` | For a prefix that does not end with a high surrogate, a string's key starts with the encoded prefix exactly when the string starts with the prefix (ordinal). |
| `StringOrdered.encOpt_prefix_free` | No key of a string or null is a prefix of another's, so an ordinal equality on a string field can be part of a key prefix (`UsesEncodedEquals`). |
| `StringOrdered.high_surrogate_needed` | The planner's exclusion of a prefix ending with a high surrogate (`UsesEncodedStartsWith`) is needed: such a prefix encodes the surrogate alone, so the scan would miss strings that continue the pair. |
| `StringOrdered.high_surrogate_match` | `Constraint.String.StartsWith` with a value ending with a high surrogate matches exactly the strings starting with it: it scans the prefix without the surrogate and accepts the surrogate alone or a pair it starts next. Before the 2026-10-29 fix it scanned the encoded value and missed strings continuing the pair (`RelationConstraintDifferentialTest`). |
| `StringKeyOrder.enc_order`, `StringKeyOrder.differs_from_ordinal` | String keys sort as the token lists of their strings, so in code-point order, which is not ordinal UTF-16 order: "😀" (a surrogate pair) sorts after "\uE000" as a key but before it ordinally. So an ordinal comparison of a string field cannot be a key range scan; the query engine filters it on values, matching LINQ. |
| `ReadOnlyCompare.compare_correct` | `Compare(prefix, suffix, target, maxLength)` equals comparing the first `maxLength` bytes of `prefix ++ suffix` with the target, in every branch of its shortcut. |
| `Boundary.search_correct`, `Boundary.before_lt_mono`, `Boundary.before_cut_mono` | The node search returns the number of keys below the target, or of keys whose cut to the target's length is not above it, for keys in increasing order. |
| `Boundary.descend`, `Boundary.child_before`, `Boundary.child_after` | In a branch node the search picks the child holding the boundary, and the boundary of the node is the keys of the children before it, `CountBefore`, plus the boundary inside it. |
| `Boundary.seek_index` | `SeekIndex` picks the child holding a key index from the children's cumulative counts, and the index lies in it at the remaining offset. |
| `Creator.flatten_groups`, `Creator.group_fits` | The branches of a level hold its children in order, each once, with at most `MaxChildren` children and at most `MaxNodeKeyBytes` bytes of separators, so the 16-bit separator ends and the child count byte fit. |
| `Creator.groups_shrink` | Since `Run` rejects keys longer than `MaxNodeKeyBytes`, every branch but the last takes at least two children, so each level is smaller and the loop over levels ends with one root. |
| `RangeEnumeration.index_stop_leaves_range` | Before the 2026-10-28 fix, a descending enumeration that stopped by comparing key indexes went one key below the range once the caller removed the range's first key, which then reports the index of the key before it (`ObjectDbAdvancedEnumeratorBoundaryTest.RemovingListedItemsKeepsEnumerationInRange`). |
| `RangeEnumeration.key_stop_lists_range` | Stopping by comparing keys with the bound lists exactly the live keys of the range in descending order while the caller removes each listed key. |
| `Replay.replay_stops_at_damage`, `Replay.replay_drops_unfinished` | A log of complete transactions followed by part of one more, ended by damage (followed by anything) or by the end of the log, replays to exactly the complete transactions. Since the 2026-10-28 fixes, negative or oversized lengths, impossible key suffix updates and impossible ulong indexes count as damage instead of failing the open (`TransactionLogDamageTest`). |
| `GrowRead.fixed_returns_iff` | The loop returns the array exactly when the input holds the declared length. |
| `GrowRead.fixed_alloc_bound` | Whatever length the prefix declares, the loop allocates at most `4 · max(A, 4096)` bytes in all, where `A` is what the input holds. |
| `GrowRead.string_alloc_bound` | Decoding characters from `A` bytes, each taking at least one byte, allocates at most `6 · max(A, 256)` characters in all, whatever length the prefix declares. |
| `GrowRead.previous_unbounded` | Before the 2026-10-28 fix the reader allocated the declared length at once, beyond any such bound (`MemReaderWriterRoundTripTest.LengthsBeyondTheInputFailWithoutAllocatingThem`). |
| `Replay.replay_after_abandoned` | A log cut by a crash inside a transaction, after complete commands, followed by the complete transactions of the next session, replays to every complete transaction: a start drops the transaction the crash left open. |
| `Replay.replay_split` | A log split into files at any command boundary, inside a transaction too, as `TransactionLogSizeStrategy` limits do, replays to the same transactions as the unsplit log. |
| `Replay.previous_loses_later_commit` | Before the 2026-10-28 fix, replay ended at a start inside an open transaction, so the next session's commits were lost on the following open (`TransactionLogDamageTest.CommitsAfterACrashInsideATransactionSurviveTheNextOpen`). |
| `FileGuid.majority_chosen` | The Guid most files carry is the database's, so a Guid more than half of the files carry is picked in every listing order. |
| `FileGuid.previous_picks_foreign` | Before the 2026-10-28 fix the first listed file decided, so one foreign or damaged file listed first made every database file foreign, and opening deleted them (`KeyValueDBFileInfosTest.ForeignFileListedFirstDoesNotDeleteTheDatabase`). |
| `FileIds.allocate_fresh`, `FileIds.allocate_inv` | An allocated file ID is not 0, has the requested parity and is none of the observed IDs, and observing it keeps that true for every later allocation. |
| `FileIds.load_max_ge`, `FileIds.next_fresh` | A standalone collection's largest ID found on open is at least every file's ID in any listing order, and the next ID is not 0 and above them all; before the 2026-10-29 fix an `int` maximum wrapped to 0 after a file named with the largest `uint` (`FileCollectionTests.FileAfterTheLargestPossibleIdIsRefused`). |
| `ReadBounds.refused_iff`, `ReadBounds.naive_wraps` | `position > capacity \|\| length > capacity - position` on `ulong`s refuses exactly the reads with `position + length > capacity`, though the subtraction and the naive sum can wrap; the naive sum accepts some reads past the end. Before the 2026-10-29 fix such reads were not refused (`FileCollectionTests.ReadFarPastTheEndIsEndOfStream`). |
| `SeqLockRead.accepted_read_is_consistent`, `SeqLockRead.late_read_can_tear` | A read accepted because the counter was even and unchanged saw the data as at its first counter read, provided the data read happened between the two counter reads; a data read landing after the second one, as loads may on arm64 without the barrier `RetryRead` issues since the 2026-10-29 fix, can see a half-done write (`SeqLockTests.ReadsThatPassRetryNeverSeeAHalfDoneWrite`). |
| `ReadGrowth.allocation_bounded`, `ReadGrowth.full_read_ends_at_length`, `ReadGrowth.importer_allocation_bounded` | Whatever length damaged data declares, reading it allocates at most `max(4096, buffered) + 4 * present` bytes, `present` being what the input holds, and a complete block ends in an array of exactly its length; before the 2026-10-29 fix four readers allocated the declared length at once (`MemReaderWriterRoundTripTest.LengthsBeyondTheInputFailWithoutAllocatingThem`). |
| `ChunkIndexLoad.load_is_latest`, `ChunkIndexLoad.newest_only_loses` | Loading every hash index file oldest first, a later entry replacing an earlier, maps each key to its last entry in the whole history; before the 2026-10-29 fix only the newest file was loaded, first entry winning, which loses chunks stored before the previous reopen (`ChunkStorageTest.ItRemembersContentStoredBeforeAndAfterAReopen`). |
| `EquatableHash.hash_respects_equal`, `EquatableHash.old_hash_breaks` | Hashing the default and an empty array both as 0 gives equal arrays equal hashes; before the 2026-10-29 fix the empty array hashed as `HashCode` of no items, a per-process seed, unlike the default array it equals (`EquatableArrayTests.DefaultAndEmptyArraysHashAlike`). |
| `IocSelfWait.self_wait_never_ends`, `IocSelfWait.fixed_self_wait_fails`, `IocSelfWait.other_wait_unchanged` | The thread creating an instance and resolving it again passes its own reentrant lock at once and never sees the locker replaced, so before the 2026-10-29 fix it looped forever; the fixed wait fails at once for it and waits as before for every other thread (`IocTests.ServiceResolvingItselfWhileCreatedFails`). |
| `BonItemBound.distinct_offsets_bound`, `BonItemBound.pairs_double_fits`, `BonItemBound.unbounded_pairs_wrap` | Items taking their own bytes start at distinct offsets below the buffer's length, so a valid container holds at most that many and `Bon.ReadItemCount` refuses only damage; before the 2026-10-29 fix a damaged count past `int.MaxValue` became a negative list capacity (`BonDamageFuzzTest.DamagedBonDeserializesOrThrows`), and an unbounded dictionary count doubled in a `uint` wrapped (`BonCoverageTest.DamagedDictionaryCountDoesNotWrap`). |
| `CapacityHint.capacity_bounded`, `CapacityHint.valid_capacity_exact`, `CapacityHint.capacity_fits_int` | The capacity is at most the stored count and the bytes left, and fits an `int`, so a damaged count can neither be a negative capacity nor size a collection beyond the data, while a valid count is used as it is; before the 2026-10-29 fixes such counts were negative capacities (`RelationDamagedCollectionCountTest`, `EventDamagedCollectionCountTest`, `RelationVersionInfoDamageTest`). |
| `SingletonCache.reachable_inv`, `SingletonCache.lookup_correct`, `SingletonCache.writer_lookup_correct`, `SingletonCache.writer_store_ignored` | A reader taking the singleton from the cache gets the one of its own snapshot and the writer the committed one, and the writer's uncommitted content is never cached, whether the writer's number is the committed one (`LsmKeyValueDB`) or the next (`BTreeKeyValueDB`), as long as every change of the singleton first sets the cache to the writer's commit number; before the 2026-10-29 fix three ways of changing it did not (`ObjectDbSingletonCacheTest`). |
| `SingletonOid.reachable_inv`, `SingletonOid.old_loses_oid` | An allocated singleton oid stays durable or flagged for the next writer to store, as a commit clears the flag only when it stored the oid; before the 2026-10-29 fix a commit cleared it also when the oid was allocated after it stored the table info, and the singleton was lost on reopen (`ObjectDbSingletonCacheTest`). |
| `CompactionBatch.stop_keeps_due`, `CompactionBatch.old_drops` | A batch stopping early schedules again every database it took, except those compacted with no work left; before the 2026-10-29 fix it scheduled none, so removing one database left the interrupted and later ones uncompacted (`KeyValueDBCompactorSchedulerTest`). |
| `RelationRegistry.reachable_inv`, `RelationRegistry.old_drops` | Publishing a copied registry only while its version is unchanged keeps every relation ever registered; before the 2026-10-29 fix the copy was published regardless, dropping relations registered while the call waited for the writer (`ObjectDbInitializeRelationsTest`). |
| `TypeNames.reachable_inv`, `TypeNames.lookup_storable`, `TypeNames.old_shares` | No two storable types are registered under one name, so objects stored under a name load as the type stored; and once a storable type is registered under a name it is the one loaded, also when its interface took the name first; before the 2026-10-29 fixes two classes of the same name in different namespaces shared it, and one loaded as the other or as its interface (`ObjectDbTypeNameClashTest`). |
| `EnumerateDirty.enumerate_complete`, `EnumerateDirty.enumerate_nodup`, `EnumerateDirty.old_misses` | Every stored object and every one created in the transaction is enumerated, none twice; before the 2026-10-29 fix a new object created before one flushed with `StoreAndFlush` was missed (`ObjectDBTransactionCoverageTest`). |
| `ObjectRestore.reachable_inv`, `ObjectRestore.old_breaks` | An object pending for the commit is dirty under its oid and cached under it, and every dirty object is pending, also when a deleted object is stored again after another was created under its oid; before the 2026-10-29 fix a deleted object stored again stayed out of the cache (`ObjectDBTransactionCoverageTest`). |
| `EventBlocks.ends_le_space`, `EventBlocks.old_check_loses` | Every block of an event ends within its `SpaceNeeded`, so an event fitting the file with all its block headers is read whole; before the 2026-10-29 fix the store checked the length with one header only and accepted an event whose last block ends past the maximum file size (`EventStoreFileCoverageTest`). |
| `InlineNumbers.valid_number_accepted` | Every inline object number of a valid record is below the objects registered so far plus the record length, so the bound added in the 2026-10-29 fix refuses only damage, also for a reader skipping nested content (`DBReaderCtxDamageTest`). |
| `ReadOnlyDepth.descent_bounded`, `ReadOnlyDepth.valid_depth` | A descent through children at strictly decreasing offsets ends within the root offset's steps, and a tree the creator builds over at most 2^32 leaves has at most 33 levels, so the 2026-10-29 fix stops descents through damaged pointers and refuses no valid image (`ReadOnlyKeyValueDBFuzzTest`, seed 3025). |
| `KeyIndexOrder.check_iff_sorted`, `KeyIndexOrder.unsorted_search_misses` | Comparing each loaded key with the one before accepts exactly strictly increasing keys; before the 2026-10-29 fix keys out of order were loaded, and a binary search, like the tree's lookups, missed a key the tree listed (`KeyIndexFuzzTest`). |
| `FletcherDetect.sum1_mod`, `FletcherDetect.flip_detected` | The low half of Fletcher-32 is its start plus all words modulo 65535, so flipping any one bit of a block changes the checksum and the event store reader refuses the block (`EventStoreFileCoverageTest.DamagedStoreReadsOnlyPrefixOfStoredEvents`). |
| `CipherBlocks.and15_eq_mod`, `CipherBlocks.refuses_iff_partial` | The test `(length & 15) != 0` refuses exactly the lengths that are no whole number of 16-byte AES blocks; before the 2026-10-29 fix CBC decryption threw `CryptographicException` for them (`AesGcmSymmetricCipherTest`). |
| `Utf8Length.utf8_le_three` | UTF-8 takes at most three bytes per UTF-16 code unit, surrogate pairs and lone surrogates included, so the stack buffer `FindByName` keeps for names up to 256 code units always fits; longer ones use the heap since the 2026-10-29 fix. |
| `HiddenProperties.visible_unique`, `HiddenProperties.visible_complete` | Keeping only properties no more derived class hides leaves exactly one per name, the most derived, and drops no name; before the 2026-10-29 fix a property hidden with `new` gave ObjectDB and the event store two fields of one name (`ObjectDbMemberShapesTest`). |
| `InheritedAttributes.own_kept`, `InheritedAttributes.fromOverridden_fresh`, `InheritedAttributes.inherited_kept` | The generator keeps a property's own attributes, takes an `Inherited` attribute of an overridden property unless a more derived member declares its single-use class, and so never duplicates one; before the 2026-10-29 fix it lost `[PrimaryKey]` and others on overrides (`RelationTests.VerifyRelationWithOverriddenKeyProperties`). |
| `PropertyOrder.new_eq_old_without_private_bases`, `PropertyOrder.new_has_private_bases` | Listing every class's declared properties, the most derived first, adds the private properties of base classes the generator stores and leaves the listing of every other type unchanged, field for field and in order, so their stored schemas stay (`ObjectDbMemberShapesTest`). |
| `DependencyMembers.collect_nodup`, `DependencyMembers.collect_complete`, `DependencyMembers.collect_first` | Walking from the most derived class and taking a member only for a name not met before injects every name once, from its most derived member, so a dependency hidden with `new` or overridden is taken from the derived class, as reflection based and generated factories both do. |
| `UpdateByIdParams.full_check_sound`, `UpdateByIdParams.cut_misses` | Comparing parameters with every key field, InKeyValue fields included, makes each key field written from a parameter matching it; the old check stopped at the first InKeyValue field and accepted `UpdateById(ulong id, string? name)` for a key `Id, [InKeyValue] Version`, writing `name` in place of `Version`. |
| `PowerOfTwo.smear_eq`, `PowerOfTwo.power_of_two_of_and_pred`, `PowerOfTwo.powerOf2_correct` | Or-ing a value with its shifts by 1, 2, 4, 8 and 16 sets every bit below its highest, and `v & (v - 1)` is 0 only for powers of two, so for `v` from 1 to `2^30` the result is the smallest power of two not below `v` and a valid `int` array length. |
| `TrlChain.walk_descending` | Refusing a predecessor ID that is not below the file's own, the walk always ends and lists strictly descending IDs. |
| `TrlChain.previous_never_ends`, `TrlChain.fixed_refuses_cycle` | Before the 2026-10-28 fix one damaged predecessor ID closed the chain into a cycle that opening followed forever; the walk now refuses it (`TransactionLogDamageTest.PredecessorLinkCycleFailsTheOpen`). |
| `KviDurability.fixed_safe` | Flushing the log before the key index, with the index's root taken under the same exclusion, keeps every durable index at or before the durable log end in every reachable state. |
| `KviDurability.is_durable_sound` | The check `CreateKvi` makes beside a writer accepts only roots whose log bytes are durable, also when it reads a current log ID that a switch has since made stale. |
| `KviDurability.previous_unsafe` | Before the 2026-10-28 fix one commit and an index were enough for a crash to keep the index and lose the log bytes it points at (`KeyValueDBCrashSimulationTest`). |
| `PreservedIndex.fixed_keeps_preserved` | Writing a key index keeps the newest one at or before the preserved commit, so history stays openable from it. |
| `PreservedIndex.previous_drops_preserved` | Before the 2026-10-28 fix `CreateKvi` kept only its new index, so opening at the preserved commit opened a later one (`KeyValueDBHistoryOpenTest`). |
| `Separators.erase_first` … `Separators.shrink_later` | After `EraseOne` removes or shrinks a child, the separators it copies are the first keys of all new children but the first, which `BTreeRoot.FindKey` relies on. |
| `Separators.split_child`, `Separators.split_branch` | So do the copies `CreateOrUpdate` makes when a child splits and when the branch itself splits. |
| `DictionaryFree.walk_ends` | Skipping a dictionary already registered, freeing ends within one more level of nesting than there are dictionaries, whatever they hold, and registers each one once. |
| `DictionaryFree.unguarded_never_ends` | Without that check a dictionary damaged to hold itself is walked without end, which overflowed the stack once the 2026-10-28 fix freed nested dictionaries of recursive types (`ObjectDbTableFreeContentTest.FreeEndsOnDictionaryHoldingItself`). |
| `EventMetadataIds.written_index_below_length`, `EventMetadataIds.written_indices_fill` | Every type id the serializer writes passes the deserializer's length check, and the ids fill the type table without gaps, so the checks reject no valid metadata log. |
| `EventMetadataIds.large_index_rejected` | A type id whose index the log cannot hold, as damage leaves, is rejected before the table grows to it (`EventStore2CoverageTest.DamagedMetadataTypeIdIsReported`). |
| `BonReusedSlot.fixed_loads_each` | Clearing the storage slot before each item, every item of a Bon list or dictionary loads as itself and a null one as null, whatever the item before it was. |
| `BonReusedSlot.previous_copies_before` | Before the 2026-10-28 fix a null string or byte array item loaded as the item before it (`BonSerializerCoverageTest.NullItemsAndMembersRoundTrip`). |
| `HashSetEntries.skipped_iff_free`, `HashSetEntries.written_count` | Skipping entries whose `Next` is below `-1` skips exactly the removed ones, the end of the free list included, so the walk writes the `_count - _freeCount` items the event store and ObjectDB announce before them. |
| `HashSetEntries.stricter_check_writes_freed` | A check that took only `Next ≤ -3` for removed would write the last removed entry. |
| `InlineCycle.guarded_ends` | Refusing an object already in the chain of inline objects being written, writing ends within one more level than there are objects, whatever they hold. |
| `InlineCycle.refuses_self`, `InlineCycle.shared_written` | An object holding itself is refused, while one held in two places with no cycle is written. |
| `InlineCycle.unguarded_never_ends` | Before the 2026-10-28 fix an object holding itself was written without end, which overflowed the stack (`ObjectDbInlineCycleTest`). |
| `ConverterStaging.fixed_holds` | The staging chosen for the type a converting loader loads or a converting saver writes always holds a value of that type. |
| `ConverterStaging.previous_saver_unsafe`, `ConverterStaging.previous_loader_missing` | Before the 2026-10-28 fix savers chose by the source type, staging a 16-byte value in an 8-byte object slot or a reference in an `Int128`, and loaders had nowhere to stage `DateTimeOffset?` (`FieldHandlerUpgradeConversionTest.WideNullableToPlainConversions`). |
| `EmittedTry.fixed_accepted` | Saving a list key part without a try block is accepted under any call arguments already pushed and leaves them in place. |
| `EmittedTry.previous_rejected`, `EmittedTry.previous_alone` | Before the 2026-10-28 fix its try block was accepted only on an empty stack, so emitted `ListBy`, `CountBy` and `AnyBy` methods on a list key failed with `InvalidProgramException` (`ObjectDbIListArrayRegressionTest.EmittedListingByListKeyPrefixWorks`). |
| `DeserializerPlaceholder.fixed_ends` | With placeholders cached for registered classes, creating a deserializer ends from any type when other types hold no cycle of their own, within `(n + 1) * (R + 2)` levels. |
| `DeserializerPlaceholder.previous_never_ends` | Before the 2026-10-28 fix the lookup by type cached none, so a class holding itself, first reached as a member type, was created without end (`BonSerializerCoverageTest.RecursiveClassFirstReachedAsMemberTypeDeserializes`). |
| `SkipOnce.fixed_aligned` | When each member deserializer consumes one value, member `i` reads value `i`. |
| `SkipOnce.previous_shifted` | Before the 2026-10-28 fix a member holding a class of another name consumed two values, so every member after it read the next member's value (`BonSerializerCoverageTest.UnrelatedClassInMemberIsSkippedOnce`). |
| `CountCache.fixed_count` | Counting the keys on each call, every instance reports the number of keys whatever changes the instances made. |
| `CountCache.previous_stale` | Before the 2026-10-28 fix an instance's cached count missed a key the other instance added (`ObjectDbLazyCollectionCountTest`). |
| `MergeCycle.fixed_ends` | Recording every descriptor met, merging a re-serialized descriptor ends within one more level than there are descriptors. |
| `MergeCycle.previous_never_ends` | Before the 2026-10-28 fix only the root was recorded, so a descriptor below it nesting itself was mapped without end (`EventLayerDynamicReserializeTest.DynamicRecursiveObjectsFailWithExceptionInsteadOfCrash`). |
| `ConvertingSaver.fixed_ends` | Converting from the target type to a type it saves, a descriptor creates the saver of any type within two calls. |
| `ConvertingSaver.previous_never_ends` | Before the 2026-10-28 fix the byte array descriptor converted with the types swapped, so a type it does not save asked to save itself without end (`EventLayerDynamicReserializeTest.DynamicListOfByteArraysCanBeSerializedAgain`). |
| `NestedRegistration.register_grows` | Registering the descriptors nested in a merged one only adds, so descriptors registered before stay registered. |
| `NestedRegistration.fixed_storable`, `NestedRegistration.previous_unstorable` | Registering every nested descriptor, a merged descriptor can be stored; before the 2026-10-29 fix only it was registered, and storing a dynamic list of nullables failed on its nested descriptor (`EventLayerDynamicReserializeTest.DynamicNestedCollectionsCanBeSerializedAgain`). |
| `InheritedDependencies.collect_nodup`, `InheritedDependencies.collect_complete` | Walking a type's members and then its base types' members, most derived first, injects no name twice, and injects every `[Dependency]` member that no earlier member hides. |
| `InheritedDependencies.previous_misses_base` | Before the 2026-10-29 fix only the type's own members were walked, so a dependency declared in its base was not injected (`IOCInheritedDependencyTest`). |
| `QueryPaging.bind_correct` | For any rows and any sequence of `Skip` and `Take` calls, the skip and take a relation query binds give exactly the rows the calls give applied one after another, as LINQ does, with nonpositive skips ignored and negative takes taking nothing. |
| `QueryTopK.top_k_correct` | Sorting with a limit, inserting each row at its sorted place, skipping one placed at the limit or later and dropping the last entry past it, keeps exactly the first `limit` rows of all rows sorted, whatever their order. |
| `RoaringKind.choose_shortest`, `RoaringKind.kind_roundtrip` | The encoding `ChooseOutputKind` picks for a container (array, runs or raw bitmap) is the shortest of the three, and for a container with a set bit `ValidateAndGetKind` reads the same kind back from its length. |
| `RoaringSource.fixed_keeps_valid`, `RoaringSource.previous_loses_valid` | For values in ascending order of their pages, `RoaringBitmap.Source` reads exactly those up to the maximum index, in order; before the 2026-10-29 fix the first value above the maximum ended them and lost later values of its page (`ObjectDbTableFreeContentTest.RoaringBitmapSourceSkipsValuesAboveMaximumInItsLastPage`). |
| `RoaringCount.fixed_count_correct`, `RoaringCount.previous_wrong_count` | Through any page commands, sets, counts and flushes, `Count` of a roaring bitmap returns the number of its values; before the 2026-10-29 fix pages applied without their count and a set bit cached a count of 1 for 3 values (`ODBRoaringBitmapAndLazyListCoverageTest.BitmapWithPagesButNoCountYetCountsItsPages`). |
| `KeyConcat.key_order`, `KeyConcat.key_order_needs_prefix_free` | A key starting with a field whose encoding is prefix free and keeps the order of its values sorts by that field first and by the rest of the key on a tie, so a key of several fields sorts by its fields in order; without prefix freedom the rest of the key can decide against the field's order. |
| `ListKey.list_prefix_free`, `ListKey.needs_count` | With prefix free encodings of the count and the items, no list key is a prefix of another, so a list key can be followed by other fields and a lookup by it matches exactly the equal list; without the count the key of `[1]` would be a prefix of the key of `[1, 2]`. |
| `SnappyExpansion.expansion_bound` | Snappy elements write at most 22 times the input bytes they take, so refusing a declared size above 22 times the input refuses no data that would decompress (`SnappyTest.ImpossibleDecompressedSizeIsRefusedBeforeAllocating`). |
| `ChunkCachePut.fixed_starts_at_most_one`, `ChunkCachePut.previous_never_fits` | `DiskChunkCache.Put` of content no longer than a file's limit less the largest header fits after at most one new value file; before the 2026-10-29 fix content too long for an empty file made it start new files forever (`DiskChunkCacheTest.ContentLargerThanValueFileIsNotCached`). |
| `RemovePartial.fixed_leaves_only_kept`, `RemovePartial.previous_leaves_removable` | When `RemoveByIdPartial` removes fewer rows than its limit, every row left is one `OnBeforeRemove` keeps; before the 2026-10-29 fix kept rows counted towards the limit and a call could remove nothing while a removable row was left (`ObjectDbBeforeRemovePartialTest`). |
| `DispatcherOverloads.select_ok`, `DispatcherOverloads.select_nodup`, `DispatcherOverloads.select_complete` | A generated dispatcher registers each message type a handler handles once, with an accessible instance method, and registers every message type for which the handler or a base class has a callable method. |
| `DispatcherOverloads.previous_picks_uncallable`, `DispatcherOverloads.previous_misses_second` | Before the 2026-10-29 fix the first method with the name and one parameter was used, so a static or private overload broke compilation and a handler of two message types was registered for one (`IOCCoverageTests.DispatcherRegistersEveryAccessibleOverload`, `IocDispatcherTests`). |
| `DispatcherContainers.get_built_for_root`, `DispatcherContainers.get_stable` | After any sequence of calls, a dispatcher call gets the handler factory built for the root container it is called with, and keeps getting the same one. |
| `DispatcherContainers.previous_wrong_root` | Before the 2026-10-29 fix one factory per process was kept, so creating a dispatcher for a second container made the first container's calls use the second's handlers (`IocDispatcherTests.DispatcherOfEachContainerCallsItsHandlers`). |
| `BonInteger.read_write`, `BonInteger.read_exact`, `BonInteger.fits_exact` | Every `long` Bon writes reads back as itself; a value `TryGetLong` returns, or a typed deserializer loads into a member, is the stored integer, within the range of `long` and of the member. |
| `BonInteger.previous_wraps` | Before the 2026-10-29 fix the magnitude was cast to `long`, so `ulong.MaxValue` read as `-1` (`BonTests.IntegersOutsideOfLongAreNotReadAsLong`, `BonSerializerCoverageTest.IntegersOutsideOfMemberTypeAreLeftDefault`). |
| `FirstDifference.diff3_eq`, `FirstDifference.diff4_eq` | `FindFirstDifference` of a key and a key in two parts, and of two keys in two parts each, returns the common prefix length of the joined keys, without joining them. |
| `FirstDifference.common_prefix_eq` | `CalcCommonPrefix`, shortening the first key to its common prefix with each other key and stopping at 0, returns the smallest common prefix of the first key with any key, the common prefix of all of them. |
| `PtrLenExclude.cut_mem`, `PtrLenExclude.cut_sorted` | On sorted, disjoint ranges, the rest of an exclusion that starts in a gap leaves a position included exactly when it was and is not below the end of the exclusion, and keeps the ranges sorted and disjoint. |
| `PtrLenExclude.previous_adds` | Before the 2026-10-29 fix the first range after the gap was trimmed even when the exclusion ended before it, so excluding `[5, 8)` from `[20, 24)` included position 10 (`PtrLenListDifferentialTest`). |
| `PtrLenInclude.absorb_mem`, `PtrLenInclude.absorb_sorted` | Once an included range starts in a gap, merging the ranges it reaches leaves a position included exactly when it was or lies in the included range, and keeps the ranges sorted and disjoint. |
| `PtrLenFree.fit_free`, `PtrLenFree.fit_first` | `FindFreeSizeAfter(pos, len)` returns a position at or after `pos` from which `len` positions are free, and from every earlier position at or after `pos` they would hit a range, so it is the first fit. |
| `PtrLenFree.take_mem`, `PtrLenFree.take_sorted` | `TryFindLenAndRemove(len)` takes `len` positions from the front of the first range at least that long; afterwards a position is included exactly when it was and is not one of them, and the ranges stay sorted and disjoint. |
| `XxHashStream.update_eq_go`, `XxHashStream.stream_eq_whole` | One `XxHash64.Update`, filling a pending partial block before whole blocks, consumes what the one-shot hash consumes of the pending bytes and the input, so updating with any chunks gives the state and pending bytes of hashing them all at once. |
| `ExportFormat.import_export` | Importing what `Export` writes, length-prefixed pairs and the commit ulong and ulongs only when they are not all zero, gives back the pairs, commit ulong and ulongs. |
| `Fletcher.block_fits`, `Fletcher.odd_fits`, `Fletcher.result_fits`, `Fletcher.fold_mod` | Fletcher-32 sums stay below 2^32 through every block of up to 360 words and the last odd byte, folding returns them to the bounds a block starts from without changing them modulo 65535, and the result keeps both sums in 16 bits. |
| `DisposeRace.fixed_disposes_once` | In every interleaving of two threads tracking new instances and one disposing the container, each instance is disposed exactly once and none stays owned. |
| `DisposeRace.previous_leaks` | Before the 2026-10-29 fix, reading the dispose flag before adding the instance left some interleaving with an instance never disposed (`IOCDisposeRaceTest`). |
| `IocDefaults.all_listed`, `IocDefaults.default_last` | Every registration of a type is listed in registration order, which `IEnumerable` resolution and singleton verification walk, and the default is the last registration not preserving existing defaults, or the first one when all later ones did. |
| `MemoryStreamSize.inv_preserved`, `MemoryStreamSize.grown_reads_zero` | After any writes and size changes every byte of a memory stream from its size on is zero, so a stream shrunk and grown again reads zeros where it was cut off, like a file. |
| `MemoryStreamSize.previous_reads_old` | Before the 2026-10-29 fix `SetSize` kept the bytes beyond the new size, so growing the stream again read them back (`StreamLayerCoverageTest.MemoryStreamReadsZerosWhereItWasCutOff`). |
| `ProviderDisposal.fixed_once` | With `UseBtdbIoc`, the container disposing the instances it owns except those the service provider tracks, and the provider disposing those, disposes each instance exactly once. |
| `ProviderDisposal.previous_twice` | Before the 2026-10-29 fix an instance both owned and tracked was disposed twice (`IocTests.UseBtdbIocDisposesEachInstanceOnce`). |
| `LruCapacity.fixed_within_max`, `LruCapacity.previous_exceeds_max` | An LRU cache holds at most its maximum capacity after any adds, removes, clears and explicit capacity increases; before the 2026-10-29 fix an increase beyond the maximum let adds pass it (`LruCacheDifferentialTest`). |
| `BonNesting.fixed_depth` | Containers nested in accepted Bon have strictly decreasing code positions, so nesting is at most one deeper than the outermost position and ends within the data. |
| `BonNesting.previous_unbounded` | Checking only that a pointer points back accepts nesting of every depth within twelve bytes; before the 2026-10-28 fix such damage overflowed the stack (`BonDamageFuzzTest`). |
| `SerializerPlaceholder.fixed_no_stale_placeholder` | Once no creation of a type runs, its placeholder is not cached, so no later serialization waits for it and no deserialization recurses through it. |
| `SerializerPlaceholder.previous_stale_placeholder` | Before the 2026-10-28 fix one failed creation left the placeholder cached, which hung serialization and overflowed the stack in deserialization (`BonSerializerCoverageTest.SerializingAfterFailedMemberTypeFailsAgain`). |
| `HintNames.terminates` | `UniqueHintName` finds a free name within one more candidate than names it returned, so its unbounded loop ends. |
| `HintNames.unique_keeps_free_name` | A requested name nobody took yet is returned unchanged, so generated file names stay as before the 2026-10-28 fix. |
| `HintNames.run_nodup` | All names a generator run returns differ ignoring case, so types whose names differ only by case no longer make the generator fail (`IOCTests.VerifyTypesWhoseNamesDifferOnlyByNestingOrCase`). |
| `KeyRange.upper_correct` | `IsPastStop` rejects a key exactly when its value is above an inclusive bound, or at or above an exclusive one. |
