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

`BtdbProofs/FirstDifference.lean` models how `TreeNodeUtils.FindFirstDifference` compares keys given in parts
(`../Buffer/TreeNodeUtils.cs`).

`BtdbProofs/PtrLenExclude.lean` models how `PtrLenList.TryExclude` removes the rest of a range that starts in a gap
(`../KVDBLayer/Helpers/PtrLenList.cs`).

`BtdbProofs/Fletcher.lean` models how `Checksum.CalcFletcher32` adds words to its sums and folds them
(`../Buffer/Checksum.cs`).

`BtdbProofs/DisposeRace.lean` models how a container tracks instances it owns while another thread disposes it
(`../IOC/ContainerImpl.cs`).

`BtdbProofs/MemoryStreamSize.lean` models the bytes and size of a memory stream under writes and `SetSize`
(`../StreamLayer/MemoryPositionLessStream.cs`).

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
| `PtrLenExclude.cut_mem`, `PtrLenExclude.cut_sorted` | On sorted, disjoint ranges, the rest of an exclusion that starts in a gap leaves a position included exactly when it was and is not below the end of the exclusion, and keeps the ranges sorted and disjoint. |
| `PtrLenExclude.previous_adds` | Before the 2026-10-29 fix the first range after the gap was trimmed even when the exclusion ended before it, so excluding `[5, 8)` from `[20, 24)` included position 10 (`PtrLenListDifferentialTest`). |
| `Fletcher.block_fits`, `Fletcher.odd_fits`, `Fletcher.result_fits`, `Fletcher.fold_mod` | Fletcher-32 sums stay below 2^32 through every block of up to 360 words and the last odd byte, folding returns them to the bounds a block starts from without changing them modulo 65535, and the result keeps both sums in 16 bits. |
| `DisposeRace.fixed_disposes_once` | In every interleaving of two threads tracking new instances and one disposing the container, each instance is disposed exactly once and none stays owned. |
| `DisposeRace.previous_leaks` | Before the 2026-10-29 fix, reading the dispose flag before adding the instance left some interleaving with an instance never disposed (`IOCDisposeRaceTest`). |
| `MemoryStreamSize.inv_preserved`, `MemoryStreamSize.grown_reads_zero` | After any writes and size changes every byte of a memory stream from its size on is zero, so a stream shrunk and grown again reads zeros where it was cut off, like a file. |
| `MemoryStreamSize.previous_reads_old` | Before the 2026-10-29 fix `SetSize` kept the bytes beyond the new size, so growing the stream again read them back (`StreamLayerCoverageTest.MemoryStreamReadsZerosWhereItWasCutOff`). |
| `LruCapacity.fixed_within_max`, `LruCapacity.previous_exceeds_max` | An LRU cache holds at most its maximum capacity after any adds, removes, clears and explicit capacity increases; before the 2026-10-29 fix an increase beyond the maximum let adds pass it (`LruCacheDifferentialTest`). |
| `BonNesting.fixed_depth` | Containers nested in accepted Bon have strictly decreasing code positions, so nesting is at most one deeper than the outermost position and ends within the data. |
| `BonNesting.previous_unbounded` | Checking only that a pointer points back accepts nesting of every depth within twelve bytes; before the 2026-10-28 fix such damage overflowed the stack (`BonDamageFuzzTest`). |
| `SerializerPlaceholder.fixed_no_stale_placeholder` | Once no creation of a type runs, its placeholder is not cached, so no later serialization waits for it and no deserialization recurses through it. |
| `SerializerPlaceholder.previous_stale_placeholder` | Before the 2026-10-28 fix one failed creation left the placeholder cached, which hung serialization and overflowed the stack in deserialization (`BonSerializerCoverageTest.SerializingAfterFailedMemberTypeFailsAgain`). |
| `HintNames.terminates` | `UniqueHintName` finds a free name within one more candidate than names it returned, so its unbounded loop ends. |
| `HintNames.unique_keeps_free_name` | A requested name nobody took yet is returned unchanged, so generated file names stay as before the 2026-10-28 fix. |
| `HintNames.run_nodup` | All names a generator run returns differ ignoring case, so types whose names differ only by case no longer make the generator fail (`IOCTests.VerifyTypesWhoseNamesDifferOnlyByNestingOrCase`). |
| `KeyRange.upper_correct` | `IsPastStop` rejects a key exactly when its value is above an inclusive bound, or at or above an exclusive one. |
