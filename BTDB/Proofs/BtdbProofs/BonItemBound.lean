/-!
# A Bon container holds at most as many items as its buffer has bytes

`Bon.ReadItemCount` (`../Bon/Bon.cs`) refuses an array or tuple declaring more items than the buffer has bytes. It
rests on each item taking its own bytes of the buffer, at least one, so distinct items start at distinct offsets
below the buffer's length.

* `distinct_offsets_bound`: distinct offsets below `length` number at most `length`, so every valid container passes
  the check, and a count above it can only be damage. Before the 2026-10-29 fix such a count, above `int.MaxValue`,
  became a negative list capacity (`BonDamageFuzzTest.DamagedBonDeserializesOrThrows`, seeds 10, 145 and 564).
* `pairs_double_fits`, `unbounded_pairs_wrap`: within the bound a dictionary's pairs double into items without
  wrapping a `uint`; before the fix 2^31 + 1 pairs wrapped to one pair (`BonCoverageTest.DamagedDictionaryCountDoesNotWrap`).
-/

namespace Btdb.BonItemBound

theorem distinct_offsets_bound (length : Nat) :
    ∀ offsets : List Nat, offsets.Nodup → (∀ x ∈ offsets, x < length) → offsets.length ≤ length := by
  induction length with
  | zero =>
    intro offsets _ hb
    cases offsets with
    | nil => exact Nat.le_refl _
    | cons x _ => exact absurd (hb x List.mem_cons_self) (Nat.not_lt_zero _)
  | succ n ih =>
    intro offsets hnd hb
    by_cases hmem : n ∈ offsets
    · have hlen := List.length_erase_of_mem hmem
      have hrest := ih (offsets.erase n) (hnd.erase n) (fun x hx => by
        have hne : x ≠ n := fun h => by
          subst h
          exact (List.Nodup.not_mem_erase hnd) hx
        have := hb x (List.mem_of_mem_erase hx)
        omega)
      have : 0 < offsets.length := List.length_pos_of_mem hmem
      omega
    · have := ih offsets hnd (fun x hx => by
        have hne : x ≠ n := fun h => hmem (h ▸ hx)
        have := hb x hx
        omega)
      omega

/-- A dictionary's pairs become twice as many items in a `uint`; within the bound the doubling cannot wrap, since a
buffer holds at most `int.MaxValue` bytes. -/
theorem pairs_double_fits (pairs length : Nat) (hpairs : pairs ≤ length) (hlength : length ≤ 2 ^ 31 - 1) :
    pairs * 2 < 2 ^ 32 := by
  omega

/-- Without the bound, a damaged count of 2^31 + 1 pairs doubled in a `uint` wraps to 2 items, one pair. -/
theorem unbounded_pairs_wrap : ((2 ^ 31 + 1) * 2) % 2 ^ 32 = 2 := by decide

end Btdb.BonItemBound
