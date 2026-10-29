/-!
# A Bon container holds at most as many items as its buffer has bytes

`Bon.ReadItemCount` (`../Bon/Bon.cs`) refuses an array or tuple declaring more items than the buffer has bytes. It
rests on each item taking its own bytes of the buffer, at least one, so distinct items start at distinct offsets
below the buffer's length.

* `distinct_offsets_bound`: distinct offsets below `length` number at most `length`, so every valid container passes
  the check, and a count above it can only be damage. Before the 2026-10-29 fix such a count, above `int.MaxValue`,
  became a negative list capacity (`BonDamageFuzzTest.DamagedBonDeserializesOrThrows`, seeds 10, 145 and 564).
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

end Btdb.BonItemBound
