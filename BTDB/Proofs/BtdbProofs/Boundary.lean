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
* `descend`, `child_before`, `child_after`: in a branch node, whose separators are the first keys of all children but
  the first, the search picks the child holding the boundary, and the boundary of the node is the keys of the
  children before it plus the boundary inside it.
* `seek_index`: `SeekIndex` picks the child holding a key index from the children's cumulative counts.
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

/-! ## Descending through a branch node -/

/-- A predicate on keys that holds below some boundary in key order and nowhere above it. -/
def KeyMono (before : List Nat → Bool) : Prop := ∀ a b, lt a b → before b = true → before a = true

theorem lt_key_mono (t : List Nat) : KeyMono (fun k => cmp k t == .lt) := by
  intro a b h hb
  simp only [beq_iff_eq] at hb ⊢
  exact lt_trans _ _ _ h hb

theorem cut_key_mono (t : List Nat) : KeyMono (fun k => cmp (k.take t.length) t != .gt) := by
  intro a b h hb
  simp only [bne_iff_ne, ne_eq] at hb ⊢
  exact not_gt_trans _ _ _ (cut_le _ _ _ h) hb

theorem takeWhile_none {p : List Nat → Bool} : ∀ ys : List (List Nat), (∀ a ∈ ys, p a = false) → ys.takeWhile p = []
  | [], _ => rfl
  | y :: _, h => by simp [List.takeWhile, h y (by simp)]

/-- The keys of a child are below the next separator, the next child's first key: when the separator is before the
boundary, so is the whole child. -/
theorem child_before {before : List Nat → Bool} (hm : KeyMono before) {c : List (List Nat)} {s : List Nat}
    (hc : ∀ k ∈ c, lt k s) (hs : before s = true) : ∀ k ∈ c, before k = true :=
  fun k hk => hm k s (hc k hk) hs

/-- The keys of a later child are at or above the separator: when it is not before the boundary, neither is the
child. -/
theorem child_after {before : List Nat → Bool} (hm : KeyMono before) {c : List (List Nat)} {s : List Nat}
    (hc : ∀ k ∈ c, lt s k ∨ s = k) (hs : before s = false) : ∀ k ∈ c, before k = false := by
  intro k hk
  cases hb : before k with
  | false => rfl
  | true =>
    rcases hc k hk with h | rfl
    · rw [hm s k h hb] at hs; cases hs
    · rw [hb] at hs; cases hs

/-- With the children before `l` wholly before the boundary and those after `l` wholly after it, the boundary of the
node's keys is the keys of the children before `l` plus the boundary inside child `l`: `FindBoundary` adds
`CountBefore(l)` and descends into child `l`. -/
theorem descend (before : List Nat → Bool) :
    ∀ (cs : List (List (List Nat))) (l : Nat) (c : List (List Nat)), cs[l]? = some c →
      (∀ i d, i < l → cs[i]? = some d → ∀ k ∈ d, before k = true) →
      (∀ i d, l < i → cs[i]? = some d → ∀ k ∈ d, before k = false) →
      cs.flatten.takeWhile before = (cs.take l).flatten ++ c.takeWhile before
  | [], _, _, h, _, _ => by simp at h
  | d :: cs, 0, c, hc, _, h2 => by
    simp only [List.getElem?_cons_zero, Option.some.injEq] at hc
    subst hc
    have hrest : ∀ k ∈ cs.flatten, before k = false := by
      intro k hk
      obtain ⟨e, he, hke⟩ := List.mem_flatten.1 hk
      obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem he
      exact h2 (i + 1) cs[i] (by omega) (by simp) k hke
    simp only [List.flatten_cons, List.take_zero, List.flatten_nil, List.nil_append]
    rw [List.takeWhile_append]
    split
    · rename_i hlen
      rw [takeWhile_none _ hrest, List.append_nil]
      exact ((List.takeWhile_prefix before).eq_of_length hlen).symm
    · rfl
  | d :: cs, l + 1, c, hc, h1, h2 => by
    simp only [List.getElem?_cons_succ] at hc
    have hd := h1 0 d (by omega) rfl
    simp only [List.flatten_cons, List.take_succ_cons, List.append_assoc]
    rw [List.takeWhile_append_of_pos hd]
    congr 1
    exact descend before cs l c hc (fun i e hi he => h1 (i + 1) e (by omega) (by simpa using he))
      (fun i e hi he => h2 (i + 1) e (by omega) (by simpa using he))

/-! ## Finding a key by its index -/

/-- The number of keys in the children before child `k`, `CountBefore(k)`; `CountUpTo(m)` is `before (m + 1)`. -/
def countBefore (count : Nat → Nat) : Nat → Nat
  | 0 => 0
  | k + 1 => countBefore count k + count k

theorem countBefore_mono (count : Nat → Nat) : ∀ i j, i ≤ j → countBefore count i ≤ countBefore count j := by
  intro i j h
  induction j with
  | zero => rw [Nat.le_zero.1 h]; exact Nat.le_refl _
  | succ j ih =>
    rcases Nat.lt_or_eq_of_le h with h' | rfl
    · have := ih (by omega); simp only [countBefore]; omega
    · exact Nat.le_refl _

/-- `SeekIndex` searches the children `0 … n - 2` for the first whose `CountUpTo` exceeds the remaining index, and takes
the last child otherwise; the index then lies in that child, at `remaining - CountBefore(l)`. -/
theorem seek_index (count : Nat → Nat) (n remaining : Nat) (hn : 1 ≤ n)
    (htotal : remaining < countBefore count n) :
    let l := search (fun m => countBefore count (m + 1) ≤ remaining) 0 (n - 1)
    l < n ∧ countBefore count l ≤ remaining ∧ remaining < countBefore count (l + 1) := by
  intro l
  have hm : Monotone (fun m => countBefore count (m + 1) ≤ remaining) := by
    intro i j hij hj
    simp only [decide_eq_true_eq] at hj ⊢
    exact Nat.le_trans (countBefore_mono count _ _ (by omega)) hj
  obtain ⟨_, h2, h3, h4⟩ := search_correct _ hm 0 (n - 1) (Nat.zero_le _)
  refine ⟨by omega, ?_, ?_⟩
  · cases hl : l with
    | zero => simp [countBefore]
    | succ k =>
      have := h3 k (Nat.zero_le _) (by omega)
      simpa using this
  · by_cases hlast : l < n - 1
    · have := h4 l (Nat.le_refl _) hlast
      simpa using this
    · rw [show l + 1 = n by omega]; exact htotal

end Btdb.Boundary
