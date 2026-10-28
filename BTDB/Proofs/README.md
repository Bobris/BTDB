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

Enums write signed values with `WriteVInt*` and unsigned ones with `WriteVUInt*`, which forward to the 64-bit
encoders, so they use the encodings above. `KeyRange.byte_encoding` covers the one-byte handlers. Together, every
handler `RelationQueryKeyRanges.IsOrderPreserving` admits is an encoding for which `lower_correct` and `upper_correct`
hold, over the values of its type.

`BtdbProofs/Cursor.lean` models `BTreeKeyValueDBCursor.Find(key, prefixLen)`: the B-tree's `Find(key)` followed by
the restriction to keys with the prefix, against the contract of `IKeyValueDBCursor.Find`. The B-tree's own `Find` is
assumed to meet that contract over all keys.

`BtdbProofs/Seek.lean` models `Seek`, which positions a relation query scan on the first key of a range with a lower
bound, from that contract and those of `FindNextKey` and `FindLastKey`.

Not modelled: the B-tree itself, and `SeekLast`, the descending counterpart of `Seek`, which was checked against the
contract by hand.

## Results

| Theorem | Statement |
| --- | --- |
| `KeyRange.lower_correct` | The lower-bound test accepts a key exactly when its value is above an exclusive bound, or at or above an inclusive one. |
| `VUInt.vuint_order`, `VUInt.vuint_prefix_free` | A smaller value has a smaller encoding, and no encoding is a proper prefix of another: the "Unsigned" encoding satisfies what `KeyRange` assumes. |
| `VInt.vint_order`, `VInt.vint_prefix_free` | The same for the "Signed" encoding, negative values included. |
| `VUInt.encoding`, `VInt.encoding`, `KeyRange.byte_encoding` | Each order-preserving handler's encoding is a `KeyRange.Encoding` over the values of its type. |
| `Cursor.block` | Keys with a common prefix form a contiguous block in key order. |
| `Cursor.previous_in_block` … `Cursor.next_outside` | Each branch of `Find(key, prefixLen)` answers what the contract requires: the greatest smaller key of the block, else its smallest larger key, else not found. |
| `Seek.seek_exact` … `Seek.seek_exclusive_none` | Each branch of `Seek` reaches the first key of the range that passes the lower bound, inclusive or exclusive, or proves there is none. |
| `KeyRange.upper_correct` | `IsPastStop` rejects a key exactly when its value is above an inclusive bound, or at or above an exclusive one. |
