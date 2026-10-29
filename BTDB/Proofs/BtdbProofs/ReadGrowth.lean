/-!
# Growing a read buffer with the bytes that arrive

A model of `MemReader.ReadBlockGrowing` (`../StreamLayer/MemReader.cs`), which `ReadByteArray`, `ReadStringInUtf8`,
`ReadBlockAsSpan` and `ReadByteArrayAsMemory` use to read a block whose `length` comes from the data:

```csharp
var result = new byte[min(length, max(4096, End - Current))];
while (true) { fill result (EndOfStream when the input ends); if (result.Length == length) return result;
               grow result to min(length, 2 * result.Length); }
```

The input holds `present` more bytes. `total length present s` is what the loop allocates from an array of `s` bytes
on: the array itself, and the next ones while it filled and is shorter than `length`.

* `allocation_bounded`: whatever `length` a damaged input declares, the loop allocates at most
  `max(4096, buffered) + 4 * present` bytes before it fails or returns. Before the 2026-10-29 fix
  `ReadStringInUtf8` and the span and memory readers allocated `length` at once
  (`MemReaderWriterRoundTripTest.LengthsBeyondTheInputFailWithoutAllocatingThem`).
* `full_read_ends_at_length`: when the input holds the whole block, the last array has exactly `length` bytes.
* `importer_allocation_bounded`: the export importer's `ReadSized`, growing its reused buffer the same way, allocates at
  most the buffer it starts with plus four times what the stream holds, for any damaged key or value size.
-/

namespace Btdb.ReadGrowth

def total (length present : Nat) (s : Nat) : Nat :=
  if _h : 0 < s ∧ s ≤ present ∧ s < length then s + total length present (min length (2 * s)) else s
termination_by length - s
decreasing_by omega

/-- Size of the array the loop ends with. -/
def last (length present : Nat) (s : Nat) : Nat :=
  if _h : 0 < s ∧ s ≤ present ∧ s < length then last length present (min length (2 * s)) else s
termination_by length - s
decreasing_by omega

theorem total_stop (length present s : Nat) (h : ¬(0 < s ∧ s ≤ present ∧ s < length)) :
    total length present s = s := by
  rw [total, dite_eq_right h]

theorem total_filled (length present s : Nat) : s ≤ present → total length present s + s ≤ 4 * present := by
  induction s using total.induct length present with
  | case1 s h ih =>
    intro hs
    rw [total, dite_eq_left h]
    by_cases hnext : min length (2 * s) ≤ present
    · have := ih hnext
      by_cases hcap : 2 * s ≤ length
      · rw [Nat.min_eq_right hcap] at this
        omega
      · rw [Nat.min_eq_left (by omega), total_stop length present length (by omega)]
        omega
    · rw [total_stop length present _ (by omega)]
      have : min length (2 * s) ≤ 2 * s := Nat.min_le_right _ _
      omega
  | case2 s h =>
    intro hs
    rw [total_stop length present s h]
    omega

theorem allocation_bounded (length present buffered : Nat) :
    total length present (min length (max 4096 buffered)) ≤ max 4096 buffered + 4 * present := by
  by_cases hs : min length (max 4096 buffered) ≤ present
  · have := total_filled length present _ hs
    omega
  · rw [total_stop length present _ (by omega)]
    have := Nat.min_le_right length (max 4096 buffered)
    omega

theorem full_read_ends_at_length (length present s : Nat) (hp : length ≤ present) :
    0 < s → s ≤ length → last length present s = length := by
  induction s using last.induct length present with
  | case1 s h ih =>
    intro hpos hle
    rw [last, dite_eq_left h]
    exact ih (by have := h.1; omega) (Nat.min_le_left _ _)
  | case2 s h =>
    intro hpos hle
    rw [last, dite_eq_right h]
    omega

/-- Growing from an array of any size `s` allocates at most `s` plus four times the bytes present. -/
theorem allocation_from (length present s : Nat) : total length present s ≤ s + 4 * present := by
  by_cases hs : s ≤ present
  · have := total_filled length present s hs
    omega
  · rw [total_stop length present s (by omega)]
    omega

/-- `KeyValueDBExportImporter.ReadSized` and `ReadSizedAsync` grow their buffer the same way, starting from 4096 bytes or
from what an earlier key or value grew it to: a damaged size makes an import allocate at most that plus four times what
the stream holds. -/
theorem importer_allocation_bounded (length present buffer : Nat) :
    total length present buffer ≤ buffer + 4 * present :=
  allocation_from length present buffer

end Btdb.ReadGrowth
