/-!
# The capacity hint for collections read from data

A model of `CollectionMetadata.CapacityFor` (`../Serialization/CollectionMetadata.cs`), which ObjectDB, the event
stores and stored relation metadata use for a collection whose count comes from the data:
`min(count, bytes left)` when the bytes left are known, else `min(count, 4096)`. Each item takes at least a byte, so a
valid count never exceeds the bytes left.

* `capacity_bounded`: the capacity is at most the count and at most the bytes left, so a damaged count, which past
  `int.MaxValue` was a negative capacity, can neither be one nor size a collection beyond the data
  (`RelationDamagedCollectionCountTest`, `EventDamagedCollectionCountTest`, `RelationVersionInfoDamageTest`).
* `valid_capacity_exact`: for valid data the capacity is the count, so valid reads presize as before.
* `capacity_fits_int`: the capacity fits an `int` whenever the bytes left do, as they do for any span.
-/

namespace Btdb.CapacityHint

def capacityFor (count : Nat) (bytesLeft : Option Nat) : Nat :=
  match bytesLeft with
  | some left => min count left
  | none => min count 4096

theorem capacity_bounded (count : Nat) (bytesLeft : Option Nat) :
    capacityFor count bytesLeft ≤ count ∧ ∀ left, bytesLeft = some left → capacityFor count bytesLeft ≤ left := by
  cases bytesLeft with
  | none => exact ⟨Nat.min_le_left _ _, fun _ h => by cases h⟩
  | some left =>
    refine ⟨Nat.min_le_left _ _, fun l h => ?_⟩
    cases h
    exact Nat.min_le_right _ _

theorem valid_capacity_exact (count left : Nat) (valid : count ≤ left) :
    capacityFor count (some left) = count := Nat.min_eq_left valid

theorem capacity_fits_int (count : Nat) (bytesLeft : Option Nat) (hleft : ∀ left, bytesLeft = some left → left < 2 ^ 31) :
    capacityFor count bytesLeft < 2 ^ 31 := by
  cases bytesLeft with
  | none => exact Nat.lt_of_le_of_lt (Nat.min_le_right _ _) (by decide)
  | some left => exact Nat.lt_of_le_of_lt (Nat.min_le_right _ _) (hleft left rfl)

end Btdb.CapacityHint
