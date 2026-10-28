/-!
# Descending range enumeration while the caller removes listed keys

A model of `MoveNext` and `NextKey` of `RelationAdvancedEnumerator`, `ODBSet` and `ODBDictionary` enumerators going
down to an included lower bound `lo`. Keys are naturals in key order and `S` holds the live keys. Each step moves to
the largest live key below the current one (`FindPreviousKey`); the caller then removes the key it was given, as code
deleting what it lists does.

* `index_stop_leaves_range`: the rule before the 2026-10-28 fix stopped when the index of the next key was below the
  index of the bound cursor, which a removed bound key gives as the index of the key before it, so the enumeration
  went one key below the range.
* `key_stop_lists_range`: comparing the next key with `lo` lists exactly the live keys from the start down to `lo`,
  in descending order.
-/

namespace Btdb.RangeEnumeration

/-- The largest live key below `c`. -/
def prevKey (S : Nat → Bool) : Nat → Option Nat
  | 0 => none
  | c + 1 => if S c then some c else prevKey S c

def erase (S : Nat → Bool) (k : Nat) (j : Nat) : Bool := if j = k then false else S j

/-- Number of live keys below `x`: a cursor's key index. -/
def cnt (S : Nat → Bool) : Nat → Nat
  | 0 => 0
  | x + 1 => cnt S x + (S x).toNat

/-- The bound cursor's index: its key's index, or after the key was removed the index of the key before it. -/
def boundIndex (S : Nat → Bool) (lo : Nat) : Int := if S lo then cnt S lo else (cnt S lo : Int) - 1

/-- Before the fix, the enumeration yields the previous key unless its index is below the bound cursor's. -/
def indexRuleYields (S : Nat → Bool) (lo c : Nat) : Option Nat :=
  match prevKey S c with
  | none => none
  | some k => if (cnt S k : Int) < boundIndex S lo then none else some k

/-- Keys 1 to 5, range 2 to 4: after listing and removing 4, 3 and 2, the old rule yields 1. -/
theorem index_stop_leaves_range :
    indexRuleYields (fun k => k == 1 || k == 5) 2 2 = some 1 := by decide

/-- The fixed rule: list the previous key while it is at least `lo`, and remove it. -/
def run (lo : Nat) : Nat → (Nat → Bool) → Nat → List Nat
  | 0, _, _ => []
  | fuel + 1, S, c =>
    match prevKey S c with
    | none => []
    | some k => if k < lo then [] else k :: run lo fuel (erase S k) k

/-- The live keys from `lo` up to below `c`, descending. -/
def desc (S : Nat → Bool) (lo : Nat) : Nat → List Nat
  | 0 => []
  | c + 1 => if lo ≤ c then (if S c then c :: desc S lo c else desc S lo c) else []

theorem prevKey_none {S : Nat → Bool} : ∀ {c}, prevKey S c = none → ∀ j, j < c → S j = false
  | 0, _, j, h => absurd h (Nat.not_lt_zero j)
  | c + 1, h, j, hj => by
    simp only [prevKey] at h
    by_cases hs : S c = true
    · simp [hs] at h
    · simp only [hs, Bool.false_eq_true, ite_false] at h
      rcases Nat.lt_succ_iff_lt_or_eq.mp hj with h' | rfl
      · exact prevKey_none h j h'
      · simpa using hs

theorem prevKey_some {S : Nat → Bool} : ∀ {c k}, prevKey S c = some k →
    k < c ∧ S k = true ∧ ∀ j, k < j → j < c → S j = false
  | 0, _, h => by simp [prevKey] at h
  | c + 1, k, h => by
    simp only [prevKey] at h
    by_cases hs : S c = true
    · simp only [hs, ite_true, Option.some.injEq] at h
      subst h
      exact ⟨by omega, hs, fun j h1 h2 => by omega⟩
    · simp only [hs, Bool.false_eq_true, ite_false] at h
      obtain ⟨h1, h2, h3⟩ := prevKey_some h
      refine ⟨by omega, h2, fun j hj1 hj2 => ?_⟩
      rcases Nat.lt_succ_iff_lt_or_eq.mp hj2 with h' | rfl
      · exact h3 j hj1 h'
      · simpa using hs

theorem desc_below {S : Nat → Bool} {lo : Nat} : ∀ c, c ≤ lo → desc S lo c = []
  | 0, _ => rfl
  | c + 1, h => by simp [desc, show ¬ lo ≤ c by omega]

/-- Keys that are not live, or below `lo`, add nothing between `a` and `c`. -/
theorem desc_skip {S : Nat → Bool} {lo a : Nat} : ∀ c, a ≤ c → (∀ j, a ≤ j → j < c → S j = false) →
    desc S lo c = desc S lo a
  | 0, h, _ => by rw [show a = 0 by omega]
  | c + 1, h, hs => by
    by_cases ha : a = c + 1
    · rw [ha]
    have ih := desc_skip (lo := lo) (a := a) c (by omega) (fun j h1 h2 => hs j h1 (by omega))
    simp only [desc, hs c (by omega) (by omega), Bool.false_eq_true, ite_false]
    by_cases hl : lo ≤ c
    · simp only [hl, ite_true]; exact ih
    · simp only [hl, ite_false]
      -- Below lo the range is empty from a on as well.
      rw [desc_below a (by omega)]

theorem desc_erase {S : Nat → Bool} {lo k : Nat} : ∀ c, c ≤ k → desc (erase S k) lo c = desc S lo c
  | 0, _ => rfl
  | c + 1, h => by
    simp only [desc, erase, show c ≠ k by omega, ite_false, desc_erase (S := S) (lo := lo) (k := k) c (by omega)]

theorem prevKey_erase {S : Nat → Bool} {k : Nat} : ∀ c, c ≤ k → prevKey (erase S k) c = prevKey S c
  | 0, _ => rfl
  | c + 1, h => by
    simp only [prevKey, erase, show c ≠ k by omega, ite_false, prevKey_erase (S := S) (k := k) c (by omega)]

theorem key_stop_lists_range (lo : Nat) : ∀ fuel (S : Nat → Bool) c, c ≤ fuel → run lo fuel S c = desc S lo c
  | 0, S, c, h => by rw [show c = 0 by omega]; rfl
  | fuel + 1, S, c, h => by
    simp only [run]
    cases hp : prevKey S c with
    | none =>
      simp only
      rw [desc_skip c (Nat.zero_le c) (fun j _ hj => prevKey_none hp j hj)]
      rfl
    | some k =>
      obtain ⟨hkc, hsk, hgap⟩ := prevKey_some hp
      simp only
      have skip := desc_skip (S := S) (lo := lo) c (show k + 1 ≤ c by omega) (fun j h1 h2 => hgap j (by omega) h2)
      by_cases hlt : k < lo
      · simp only [hlt, ite_true]
        rw [skip, desc_below (k + 1) (by omega)]
      · simp only [hlt, ite_false]
        rw [key_stop_lists_range lo fuel (erase S k) k (by omega), desc_erase k (Nat.le_refl k), skip]
        simp [desc, show lo ≤ k by omega, hsk]

end Btdb.RangeEnumeration
