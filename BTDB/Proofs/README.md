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
| `Separators.erase_first` … `Separators.shrink_later` | After `EraseOne` removes or shrinks a child, the separators it copies are the first keys of all new children but the first, which `BTreeRoot.FindKey` relies on. |
| `Separators.split_child`, `Separators.split_branch` | So do the copies `CreateOrUpdate` makes when a child splits and when the branch itself splits. |
| `KeyRange.upper_correct` | `IsPastStop` rejects a key exactly when its value is above an inclusive bound, or at or above an exclusive one. |
