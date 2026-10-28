import BtdbProofs.Cursor

/-!
# The binary search of `FindBoundary`

A model of the node search in `ReadOnlyKeyValueDBCursor.FindBoundary`: `l = 0, r = count; while (l < r) { m = (l + r)
/ 2; if (before(m)) l = m + 1; else r = m; }`. `before(m)` is "key `m` is smaller than the target" or, for
`FindLastKey`, "key `m` cut to the target's length is not bigger than it" (`Compare` < 0, or ≤ 0 with
`includePrefixed`; `ReadOnlyCompare.lean` shows `Compare` is the comparison of the cut key).

* `search_correct`: for any predicate that holds on a first part of the indexes only, the loop returns the boundary
  between that part and the rest.
* `before_lt_mono`, `before_cut_mono`: over keys in increasing order both predicates are of that kind, so the search
  returns the number of keys before the boundary.
-/

namespace Btdb.Boundary

open KeyRange Cursor

def search (before : Nat → Bool) (l r : Nat) : Nat :=
  if h : l < r then
    if before ((l + r) / 2) then search before ((l + r) / 2 + 1) r else search before l ((l + r) / 2)
  else l
termination_by r - l

/-- `before` holds on a first part of the indexes and nowhere after it. -/
def Monotone (before : Nat → Bool) : Prop := ∀ i j, i ≤ j → before j = true → before i = true

theorem search_correct (before : Nat → Bool) (hm : Monotone before) :
    ∀ l r, l ≤ r → let x := search before l r
      l ≤ x ∧ x ≤ r ∧ (∀ i, l ≤ i → i < x → before i = true) ∧ (∀ i, x ≤ i → i < r → before i = false) := by
  intro l r
  induction l, r using search.induct before with
  | case1 l r h hb ih =>
    intro hlr
    rw [search, dite_eq_left h, ite_eq_left hb]
    obtain ⟨h1, h2, h3, h4⟩ := ih (by omega)
    refine ⟨by omega, h2, fun i hi1 hi2 => ?_, h4⟩
    by_cases hi : i ≤ (l + r) / 2
    · exact hm i _ hi hb
    · exact h3 i (by omega) hi2
  | case2 l r h hb ih =>
    intro hlr
    rw [search, dite_eq_left h, ite_eq_right hb]
    obtain ⟨h1, h2, h3, h4⟩ := ih (by omega)
    refine ⟨h1, by omega, h3, fun i hi1 hi2 => ?_⟩
    by_cases hi : i < (l + r) / 2
    · exact h4 i hi1 hi
    · cases hbi : before i with
      | false => rfl
      | true =>
        have := hm ((l + r) / 2) i (by omega) hbi
        simp [this] at hb
  | case3 l r h =>
    intro hlr
    rw [search, dite_eq_right h]
    exact ⟨Nat.le_refl _, by omega, fun i h1 h2 => by omega, fun i h1 h2 => by omega⟩

/-- Keys in increasing order. -/
def Sorted (keys : Nat → List Nat) : Prop := ∀ i j, i < j → lt (keys i) (keys j)

theorem before_lt_mono (keys : Nat → List Nat) (hs : Sorted keys) (t : List Nat) :
    Monotone (fun m => cmp (keys m) t == .lt) := by
  intro i j hij hj
  simp only [beq_iff_eq] at hj ⊢
  rcases Nat.lt_or_eq_of_le hij with h | rfl
  · exact lt_trans _ _ _ (hs i j h) hj
  · exact hj

/-- Cutting both keys to `n` bytes keeps their order, possibly making them equal. -/
theorem cut_le : ∀ n (a b : List Nat), lt a b → cmp (a.take n) (b.take n) ≠ .gt
  | 0, _, _, _ => by simp [cmp]
  | _ + 1, [], [], h => by simp [lt, cmp] at h
  | _ + 1, [], _ :: _, _ => by simp [cmp]
  | _ + 1, _ :: _, [], h => by simp [lt, cmp] at h
  | n + 1, x :: xs, y :: ys, h => by
    simp only [lt, cmp] at h
    simp only [List.take_succ_cons, cmp]
    by_cases h1 : x < y
    · simp [h1]
    · by_cases h2 : y < x
      · simp [h1, h2] at h
      · simp only [h1, h2, ite_false] at h ⊢
        exact cut_le n xs ys h

theorem cmp_self : ∀ a, cmp a a = .eq
  | [] => rfl
  | x :: xs => by simp [cmp, cmp_self xs]

theorem not_gt_iff (a b : List Nat) : cmp a b ≠ .gt ↔ lt a b ∨ a = b := by
  constructor
  · intro h
    cases hc : cmp a b with
    | lt => exact Or.inl hc
    | eq => exact Or.inr (cmp_eq a b hc)
    | gt => exact absurd hc h
  · rintro (h | rfl)
    · unfold lt at h; rw [h]; simp
    · rw [cmp_self a]; simp

theorem not_gt_trans (a b c : List Nat) (h1 : cmp a b ≠ .gt) (h2 : cmp b c ≠ .gt) : cmp a c ≠ .gt := by
  rw [not_gt_iff] at h1 h2 ⊢
  rcases h1 with h1 | rfl
  · rcases h2 with h2 | rfl
    · exact Or.inl (lt_trans _ _ _ h1 h2)
    · exact Or.inl h1
  · exact h2

theorem before_cut_mono (keys : Nat → List Nat) (hs : Sorted keys) (t : List Nat) :
    Monotone (fun m => cmp ((keys m).take t.length) t != .gt) := by
  intro i j hij hj
  simp only [bne_iff_ne, ne_eq] at hj ⊢
  rcases Nat.lt_or_eq_of_le hij with h | rfl
  · have := cut_le t.length _ _ (hs i j h)
    exact not_gt_trans _ _ _ this hj
  · exact hj

end Btdb.Boundary
