/-!
# Bounds of a read past the end of a file

The file collections refuse a read of `length` bytes at `position` from a file of `capacity` readable bytes with

```csharp
if (position > capacity || (ulong)data.Length > capacity - position) throw new EndOfStreamException();
```

(`InMemoryFileCollection`, `InMemoryReplicationFileStorage`, `OnDiskMemoryMappedFileCollection`, and against the
writer's buffer in `OnDiskFileCollection`). The values are `ulong`s, where `position + length` can wrap around and
pass a naive check, and `capacity - position` wraps when `position > capacity`.

* `refused_iff`: the test refuses exactly the reads that do not fit, `position + length > capacity` over the
  naturals, so the subtraction is only relied on when it does not wrap.
* `naive_wraps`: the naive `position + length > capacity` on `ulong`s accepts some reads that do not fit.
-/

namespace Btdb.ReadBounds

def refused (position length capacity : UInt64) : Bool :=
  position > capacity || length > capacity - position

theorem refused_iff (position length capacity : UInt64) :
    refused position length capacity = true ↔ capacity.toNat < position.toNat + length.toNat := by
  unfold refused
  simp only [Bool.or_eq_true, decide_eq_true_eq]
  constructor
  · rintro (h | h)
    · have := UInt64.lt_iff_toNat_lt.mp h
      omega
    · by_cases hp : position ≤ capacity
      · have hsub := UInt64.toNat_sub_of_le capacity position hp
        have hle := UInt64.le_iff_toNat_le.mp hp
        have := UInt64.lt_iff_toNat_lt.mp h
        omega
      · have := UInt64.lt_iff_toNat_lt.mp (UInt64.not_le.mp hp)
        omega
  · intro h
    by_cases hp : position ≤ capacity
    · right
      have hsub := UInt64.toNat_sub_of_le capacity position hp
      have hle := UInt64.le_iff_toNat_le.mp hp
      apply UInt64.lt_iff_toNat_lt.mpr
      omega
    · left
      exact UInt64.not_le.mp hp

theorem naive_wraps : ∃ position length capacity : UInt64,
    ¬(position + length > capacity) ∧ capacity.toNat < position.toNat + length.toNat :=
  ⟨UInt64.ofNat (2 ^ 64 - 4), 16, 100, by decide, by decide⟩

end Btdb.ReadBounds
