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

Not modelled: the cursor seeks that position a scan on a range (`Seek`, `SeekLast`), and the other order-preserving
encodings (`UnsafePackVInt` for "Signed", single bytes and enums).

## Results

| Theorem | Statement |
| --- | --- |
| `KeyRange.lower_correct` | The lower-bound test accepts a key exactly when its value is above an exclusive bound, or at or above an inclusive one. |
| `VUInt.vuint_order`, `VUInt.vuint_prefix_free` | A smaller value has a smaller encoding, and no encoding is a proper prefix of another: the "Unsigned" encoding satisfies what `KeyRange` assumes. |
| `KeyRange.upper_correct` | `IsPastStop` rejects a key exactly when its value is above an inclusive bound, or at or above an exclusive one. |
