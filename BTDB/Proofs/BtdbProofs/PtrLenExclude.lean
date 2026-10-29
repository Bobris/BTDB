/-!
# Excluding a range from `PtrLenList` removes exactly that range

`PtrLenList.TryExclude` (`../KVDBLayer/Helpers/PtrLenList.cs`) keeps sorted, disjoint ranges `(start, length)`. Once the
excluded range `[pos, end)` is found to start in a gap, the ranges after the gap all start at `pos` or later and the
rest of the exclusion is `cut end`: ranges ending by `end` are removed, one reaching past it is trimmed to start at
`end`, and the first range starting at `end` or later stops it. Before the 2026-10-29 fix the first range was trimmed
even when it started at `end` or later (`previous`).

* `cut_mem`: after the cut a position is in the ranges exactly when it was before and is not below `end`.
* `cut_sorted`: the cut keeps the ranges sorted and disjoint.
* `previous_adds`: before the fix excluding `[5, 8)` from `[20, 24)` made position 10 included.
-/

namespace Btdb.PtrLenExclude

def Mem (x : Nat) : List (Nat × Nat) → Prop
  | [] => False
  | (s, n) :: rest => (s ≤ x ∧ x < s + n) ∨ Mem x rest

/-- Each range ends at or before the next one starts. -/
def Sorted : List (Nat × Nat) → Prop
  | [] => True
  | [_] => True
  | (s, n) :: (t, m) :: rest => s + n ≤ t ∧ Sorted ((t, m) :: rest)

def cut (e : Nat) : List (Nat × Nat) → List (Nat × Nat)
  | [] => []
  | (s, n) :: rest => if e ≤ s then (s, n) :: rest else if e < s + n then (e, s + n - e) :: rest else cut e rest

/-- Before the fix: the first range was trimmed to start at `end` whenever it reached past `end`. -/
def previous (e : Nat) : List (Nat × Nat) → List (Nat × Nat)
  | [] => []
  | (s, n) :: rest => if e < s + n then (e, s + n - e) :: rest else cut e rest

theorem sorted_tail (a : Nat × Nat) (rest : List (Nat × Nat)) (h : Sorted (a :: rest)) : Sorted rest := by
  match rest, h with
  | [], _ => trivial
  | _ :: _, h => exact h.2

/-- A position in the ranges after `(s, n)` is at least `s + n`. -/
theorem mem_after (s n x : Nat) (rest : List (Nat × Nat)) (h : Sorted ((s, n) :: rest)) (hm : Mem x rest) :
    s + n ≤ x := by
  induction rest generalizing s n with
  | nil => exact absurd hm id
  | cons b rest ih =>
    obtain ⟨t, m⟩ := b
    obtain ⟨hst, hs⟩ := h
    rcases hm with hm | hm
    · omega
    · have := ih t m hs hm; omega

theorem cut_mem (e x : Nat) (rs : List (Nat × Nat)) (h : Sorted rs) : Mem x (cut e rs) ↔ Mem x rs ∧ e ≤ x := by
  induction rs with
  | nil => simp [cut, Mem]
  | cons a rest ih =>
    obtain ⟨s, n⟩ := a
    have hrest := sorted_tail _ _ h
    have hafter := fun hm => mem_after s n x rest h hm
    by_cases h1 : e ≤ s
    · simp only [cut, h1, ite_true]
      constructor
      · intro hm
        refine ⟨hm, ?_⟩
        rcases hm with hm | hm
        · omega
        · have := hafter hm; omega
      · exact fun hm => hm.1
    · by_cases h2 : e < s + n
      · simp only [cut, h1, h2, ite_true, ite_false, Mem]
        constructor
        · rintro (hm | hm)
          · exact ⟨Or.inl ⟨by omega, by omega⟩, hm.1⟩
          · exact ⟨Or.inr hm, by have := hafter hm; omega⟩
        · rintro ⟨hm | hm, hx⟩
          · exact Or.inl ⟨hx, by omega⟩
          · exact Or.inr hm
      · simp only [cut, h1, h2, ite_false, Mem]
        rw [ih hrest]
        constructor
        · rintro ⟨hm, hx⟩; exact ⟨Or.inr hm, hx⟩
        · rintro ⟨hm | hm, hx⟩
          · omega
          · exact ⟨hm, hx⟩

/-- The first range of the cut starts at the first range of the input or later. -/
def StartsAfter (t : Nat) : List (Nat × Nat) → Prop
  | [] => True
  | (s, _) :: _ => t ≤ s

theorem cut_starts_after (e t : Nat) (rs : List (Nat × Nat)) (h : Sorted rs) (ht : StartsAfter t rs) :
    StartsAfter t (cut e rs) := by
  induction rs with
  | nil => trivial
  | cons a rest ih =>
    obtain ⟨s, n⟩ := a
    simp only [StartsAfter] at ht
    by_cases h1 : e ≤ s
    · simp only [cut, h1, ite_true, StartsAfter]; exact ht
    · by_cases h2 : e < s + n
      · simp only [cut, h1, h2, ite_true, ite_false, StartsAfter]; omega
      · simp only [cut, h1, h2, ite_false]
        apply ih (sorted_tail _ _ h)
        match rest, h with
        | [], _ => trivial
        | (u, _) :: _, h => simp only [StartsAfter]; have := h.1; omega

theorem sorted_cons (s n : Nat) (rest : List (Nat × Nat)) (hs : Sorted rest) (ht : StartsAfter (s + n) rest) :
    Sorted ((s, n) :: rest) := by
  match rest, hs, ht with
  | [], _, _ => trivial
  | (_, _) :: _, hs, ht => exact ⟨ht, hs⟩

theorem cut_sorted (e : Nat) (rs : List (Nat × Nat)) (h : Sorted rs) : Sorted (cut e rs) := by
  induction rs with
  | nil => trivial
  | cons a rest ih =>
    obtain ⟨s, n⟩ := a
    have hrest := sorted_tail _ _ h
    have hstart : StartsAfter (s + n) rest := by
      match rest, h with
      | [], _ => trivial
      | (_, _) :: _, h => exact h.1
    by_cases h1 : e ≤ s
    · simp only [cut, h1, ite_true]; exact h
    · by_cases h2 : e < s + n
      · simp only [cut, h1, h2, ite_true, ite_false]
        apply sorted_cons _ _ _ hrest
        have : e + (s + n - e) = s + n := by omega
        rw [this]; exact hstart
      · simp only [cut, h1, h2, ite_false]; exact ih hrest

theorem previous_adds : Mem 10 (previous 8 [(20, 4)]) ∧ ¬ Mem 10 [(20, 4)] := by
  simp [previous, Mem]

end Btdb.PtrLenExclude
