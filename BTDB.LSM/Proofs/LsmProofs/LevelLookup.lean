import LsmProofs.BlockSeek
import LsmProofs.TombstoneFind

/-!
# Looking up a key in a level

A model of `LevelComponent.TryGet` (`../Engine/LevelComponent.cs`). A level's tables `i < n` span `[l i, u i)`, sorted
and disjoint: `l i < u i ≤ l (i + 1)`. `LastStartingAtOrBefore` finds the last table starting at or before the key by
searching for the first starting after it (modelled by its result, `BlockSeek.scan`), and `TryGet` reads that table
only when the key is below its upper bound.

* `lookup_some`: the lookup reads table `i` exactly when `i` contains the key, so a key in a gap between tables reads
  none and no other table could hold it.
* `overlaps_iff`: `LsmVersion.Overlapping` keeps a table, `Upper > lower` and `Lower < upper`, exactly when its span
  and `[lower, upper)` share a key, so a compaction neither misses an overlapping table nor takes one that only
  touches the range.
-/

namespace Lsm.LevelLookup

open Lsm.BlockSeek

def lookup (l u : Nat → Nat) (n k : Nat) : Option Nat :=
  let lo := scan (fun i => decide (k < l i)) 0 n
  if 0 < lo ∧ k < u (lo - 1) then some (lo - 1) else none

/-- The lookup is `TombstoneSet.Find` over the table spans: the same search, the same test. -/
theorem lookup_eq_find (l u : Nat → Nat) (n k : Nat) : lookup l u n k = (TombstoneFind.find l u n k).1 := by
  unfold lookup TombstoneFind.find
  dsimp only
  split <;> rfl

theorem lookup_some (l u : Nat → Nat) (n k i : Nat) (hpos : ∀ j, j < n → l j < u j)
    (hsorted : ∀ j, j + 1 < n → u j ≤ l (j + 1)) :
    lookup l u n k = some i ↔ i < n ∧ l i ≤ k ∧ k < u i := by
  rw [lookup_eq_find]
  exact TombstoneFind.find_some l u n hpos hsorted k i

theorem overlaps_iff (tl tu lower upper : Nat) (ht : tl < tu) (hr : lower < upper) :
    (lower < tu ∧ tl < upper) ↔ ∃ k, tl ≤ k ∧ k < tu ∧ lower ≤ k ∧ k < upper := by
  constructor
  · rintro ⟨h1, h2⟩
    exact ⟨max tl lower, by omega, by omega, by omega, by omega⟩
  · rintro ⟨k, h1, h2, h3, h4⟩
    exact ⟨by omega, by omega⟩

end Lsm.LevelLookup
