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
* `replace_sorted`: `LsmVersion.Replace`, sorting the kept and the new tables by `Lower`, gives a level where every table
  ends before each later one starts, as `lookup_some` assumes, when they are not empty and do not overlap.
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

def Disjoint (a b : Nat × Nat) : Prop := a.2 ≤ b.1 ∨ b.2 ≤ a.1

theorem replace_sorted (ts : List (Nat × Nat)) (hne : ∀ t ∈ ts, t.1 < t.2) (hdis : ts.Pairwise Disjoint) :
    (ts.mergeSort fun a b => decide (a.1 ≤ b.1)).Pairwise fun a b => a.2 ≤ b.1 := by
  have hperm := List.mergeSort_perm ts fun a b => decide (a.1 ≤ b.1)
  have hle := List.pairwise_mergeSort (le := fun a b : Nat × Nat => decide (a.1 ≤ b.1))
    (fun a b c h1 h2 => by simp only [decide_eq_true_eq] at *; omega)
    (fun a b => by simp only [Bool.or_eq_true, decide_eq_true_eq]; omega) ts
  have hd := hperm.symm.pairwise hdis (fun {x y} h => by unfold Disjoint at *; omega)
  refine (hle.and hd).imp_of_mem ?_
  intro a b ha hb ⟨h1, h2⟩
  have hbne := hne b (hperm.mem_iff.1 hb)
  simp only [decide_eq_true_eq] at h1
  unfold Disjoint at h2
  omega

end Lsm.LevelLookup
