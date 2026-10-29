import BtdbProofs.PtrLenExclude

/-!
# Including a range into `PtrLenList` adds exactly that range

A model of the rest of `PtrLenList.TryInclude` (`../KVDBLayer/Helpers/PtrLenList.cs`) once the included range
`[pos, end)` is found to start in a gap: every later range starts after `pos`. A range the included one reaches
(`end ≥ start`, so a touching one too) is merged, extending the end to the furthest; the first range starting after
the end stops it, and the merged range goes before it. The C# compares later starts with the original end rather than
the extended one; `PtrLenList` keeps ranges non-adjacent, so an extended end stops before the next range and both
merge the same ranges.

* `absorb_mem`: afterwards a position is in the ranges exactly when it was before or lies in `[pos, end)`.
* `absorb_sorted`: the ranges stay sorted and disjoint.
-/

namespace Btdb.PtrLenInclude

open Btdb.PtrLenExclude

def absorb (p e : Nat) : List (Nat × Nat) → List (Nat × Nat)
  | [] => [(p, e - p)]
  | (s, n) :: rest => if e < s then (p, e - p) :: (s, n) :: rest else absorb p (max e (s + n)) rest

/-- Every range starts after `p`. -/
def StartsAfter (p : Nat) : List (Nat × Nat) → Prop
  | [] => True
  | (s, _) :: rest => p < s ∧ StartsAfter p rest

theorem starts_after_of_sorted (p s n : Nat) (rest : List (Nat × Nat)) (h : Sorted ((s, n) :: rest)) (hp : p < s) :
    StartsAfter p rest := by
  induction rest generalizing s n with
  | nil => trivial
  | cons a rest ih =>
    obtain ⟨t, m⟩ := a
    obtain ⟨hst, hs⟩ := h
    exact ⟨by omega, ih t m hs (by omega)⟩

theorem absorb_mem (p e x : Nat) (rs : List (Nat × Nat)) (hs : Sorted rs) (hp : StartsAfter p rs) (hpe : p ≤ e) :
    Mem x (absorb p e rs) ↔ (p ≤ x ∧ x < e) ∨ Mem x rs := by
  induction rs generalizing e with
  | nil => simp [absorb, Mem]; omega
  | cons a rest ih =>
    obtain ⟨s, n⟩ := a
    obtain ⟨hps, hprest⟩ := hp
    by_cases h : e < s
    · simp only [absorb, h, ite_true, Mem]
      constructor
      · rintro (h1 | h1)
        · exact Or.inl ⟨h1.1, by omega⟩
        · exact Or.inr h1
      · rintro (h1 | h1)
        · exact Or.inl ⟨h1.1, by omega⟩
        · exact Or.inr h1
    · simp only [absorb, h, ite_false]
      rw [ih (max e (s + n)) (sorted_tail _ _ hs) hprest (by omega)]
      simp only [Mem]
      by_cases hm : Mem x rest
      · simp [hm]
      · simp only [hm, or_false]
        omega

theorem absorb_sorted (p e : Nat) (rs : List (Nat × Nat)) (hs : Sorted rs) (hp : StartsAfter p rs) (hpe : p ≤ e) :
    Sorted (absorb p e rs) := by
  induction rs generalizing e with
  | nil => trivial
  | cons a rest ih =>
    obtain ⟨s, n⟩ := a
    obtain ⟨_, hprest⟩ := hp
    by_cases h : e < s
    · simp only [absorb, h, ite_true]
      exact ⟨by omega, hs⟩
    · simp only [absorb, h, ite_false]
      exact ih (max e (s + n)) (sorted_tail _ _ hs) hprest (by omega)

end Btdb.PtrLenInclude
