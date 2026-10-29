/-!
# Finding the first difference of keys split into parts

`TreeNodeUtils.FindFirstDifference` (`../Buffer/TreeNodeUtils.cs`) returns the length of the common prefix of two keys.
B-tree nodes keep a key as a prefix and a suffix, so overloads compare a key with one given in two parts, and two keys
given in two parts each, without joining the parts. `cpl` is the common prefix length; `diff3` and `diff4` follow the
overloads step by step.

* `diff3_eq`: comparing with a key in two parts finds the common prefix of the joined key.
* `diff4_eq`: comparing two keys in two parts each finds the common prefix of the joined keys.
* `common_prefix_eq`: `CalcCommonPrefix`, shortening the first key to its common prefix with each other key and
  stopping at 0, returns the smallest common prefix of the first key with every key, the common prefix of them all.
-/

namespace Btdb.FirstDifference

/-- The length of the common prefix, `ReadOnlySpan.CommonPrefixLength`. -/
def cpl : List Nat → List Nat → Nat
  | x :: xs, y :: ys => if x = y then cpl xs ys + 1 else 0
  | _, _ => 0

/-- `FindFirstDifference(buf1, buf2a, buf2b)`. -/
def diff3 (a b1 b2 : List Nat) : Nat :=
  let pos := cpl a b1
  if pos < b1.length ∨ pos = a.length then pos else b1.length + cpl (a.drop b1.length) b2

/-- `FindFirstDifference(buf1a, buf1b, buf2a, buf2b)`. -/
def diff4 (a1 a2 b1 b2 : List Nat) : Nat :=
  let pos := diff3 a1 b1 b2
  if pos < a1.length then pos
  else if pos < b1.length then pos + diff3 a2 (b1.drop pos) b2
  else if pos = b1.length + b2.length then pos
  else pos + cpl a2 (b2.drop (pos - b1.length))

theorem cpl_comm (a c : List Nat) : cpl a c = cpl c a := by
  induction a generalizing c with
  | nil => cases c <;> rfl
  | cons x xs ih =>
    cases c with
    | nil => rfl
    | cons y ys =>
      simp only [cpl]
      by_cases h : x = y
      · subst h; simp [ih]
      · simp [h, Ne.symm h]

theorem cpl_le (a c : List Nat) : cpl a c ≤ a.length := by
  induction a generalizing c with
  | nil => cases c <;> simp [cpl]
  | cons x xs ih =>
    cases c with
    | nil => simp [cpl]
    | cons y ys =>
      simp only [cpl]
      split
      · simp only [List.length_cons]; have := ih ys; omega
      · omega

theorem cpl_le_right (a c : List Nat) : cpl a c ≤ c.length := cpl_comm a c ▸ cpl_le c a

theorem cpl_nil_right (a : List Nat) : cpl a [] = 0 := by cases a <;> rfl

/-- A difference within `a` stays where it is when `a` is extended. -/
theorem cpl_append_short (a c d : List Nat) (h : cpl a c < a.length) : cpl (a ++ d) c = cpl a c := by
  induction a generalizing c with
  | nil => simp at h
  | cons x xs ih =>
    cases c with
    | nil => simp [cpl]
    | cons y ys =>
      simp only [List.cons_append, cpl] at *
      by_cases hxy : x = y
      · simp only [hxy, ite_true, List.length_cons] at *
        rw [ih ys (by omega)]
      · simp [hxy]

/-- When `a` is a prefix of `c`, extending `a` compares the extension with the rest of `c`. -/
theorem cpl_append_full (a c d : List Nat) (h : cpl a c = a.length) :
    cpl (a ++ d) c = a.length + cpl d (c.drop a.length) := by
  induction a generalizing c with
  | nil => simp
  | cons x xs ih =>
    cases c with
    | nil => simp [cpl] at h
    | cons y ys =>
      simp only [List.cons_append, cpl, List.length_cons, List.drop_succ_cons] at *
      by_cases hxy : x = y
      · simp only [hxy, ite_true] at *
        rw [ih ys (by omega)]
        omega
      · simp [hxy] at h

theorem cpl_append_short_right (a c d : List Nat) (h : cpl a c < c.length) : cpl a (c ++ d) = cpl a c := by
  rw [cpl_comm, cpl_append_short c a d (by rw [cpl_comm]; exact h), cpl_comm]

theorem cpl_append_full_right (a c d : List Nat) (h : cpl a c = c.length) :
    cpl a (c ++ d) = c.length + cpl (a.drop c.length) d := by
  rw [cpl_comm, cpl_append_full c a d (by rw [cpl_comm]; exact h), cpl_comm]

theorem diff3_eq (a b1 b2 : List Nat) : diff3 a b1 b2 = cpl a (b1 ++ b2) := by
  unfold diff3
  have hle := cpl_le a b1
  have hler := cpl_le_right a b1
  dsimp only
  by_cases hs : cpl a b1 < b1.length
  · rw [ite_eq_left (Or.inl hs), cpl_append_short_right a b1 b2 hs]
  · have hfull : cpl a b1 = b1.length := by omega
    rw [cpl_append_full_right a b1 b2 hfull]
    by_cases ha : cpl a b1 = a.length
    · rw [ite_eq_left (Or.inr ha)]
      have : a.drop b1.length = [] := List.drop_eq_nil_of_le (by omega)
      rw [this]
      cases b2 <;> simp [cpl] <;> omega
    · rw [ite_eq_right (by omega)]

theorem diff4_eq (a1 a2 b1 b2 : List Nat) : diff4 a1 a2 b1 b2 = cpl (a1 ++ a2) (b1 ++ b2) := by
  unfold diff4
  dsimp only
  rw [diff3_eq]
  have hle := cpl_le a1 (b1 ++ b2)
  have hler := cpl_le_right a1 (b1 ++ b2)
  simp only [List.length_append] at hler
  by_cases hs : cpl a1 (b1 ++ b2) < a1.length
  · rw [ite_eq_left hs, cpl_append_short _ _ _ hs]
  · rw [ite_eq_right hs]
    have hfull : cpl a1 (b1 ++ b2) = a1.length := by omega
    rw [cpl_append_full _ _ _ hfull, hfull]
    by_cases hb : a1.length < b1.length
    · rw [ite_eq_left hb, diff3_eq, List.drop_append_of_le_length (by omega)]
    · rw [ite_eq_right hb]
      have hdrop : (b1 ++ b2).drop a1.length = b2.drop (a1.length - b1.length) := by
        rw [List.drop_append]
        simp [List.drop_eq_nil_of_le (show b1.length ≤ a1.length by omega)]
      by_cases he : a1.length = b1.length + b2.length
      · rw [ite_eq_left he, hdrop]
        have : b2.drop (a1.length - b1.length) = [] := List.drop_eq_nil_of_le (by omega)
        rw [this, cpl_nil_right]
        omega
      · rw [ite_eq_right he, hdrop]

theorem cpl_take (a c : List Nat) (m : Nat) : cpl (a.take m) c = min m (cpl a c) := by
  induction a generalizing c m with
  | nil => cases c <;> simp [cpl]
  | cons x xs ih =>
    cases m with
    | zero => cases c <;> simp [cpl]
    | succ m =>
      cases c with
      | nil => simp [cpl]
      | cons y ys =>
        simp only [List.take_succ_cons, cpl]
        by_cases h : x = y
        · simp only [h, ite_true, ih]; omega
        · simp [h]

/-- `CalcCommonPrefix` from the prefix kept so far over the other keys. -/
def commonPrefix (p : List Nat) : List (List Nat) → Nat
  | [] => p.length
  | k :: ks => let r := cpl p k; if r = 0 then 0 else commonPrefix (p.take r) ks

/-- The smallest common prefix of `a` with the keys, starting from `m`. -/
def minCpl (a : List Nat) (m : Nat) : List (List Nat) → Nat
  | [] => m
  | k :: ks => minCpl a (min m (cpl a k)) ks

theorem min_cpl_zero (a : List Nat) (ks : List (List Nat)) : minCpl a 0 ks = 0 := by
  induction ks with
  | nil => rfl
  | cons k ks ih => simp [minCpl, ih]

theorem common_prefix_take (a : List Nat) (m : Nat) (hm : m ≤ a.length) (ks : List (List Nat)) :
    commonPrefix (a.take m) ks = minCpl a m ks := by
  induction ks generalizing m with
  | nil => simp [commonPrefix, minCpl]; omega
  | cons k ks ih =>
    simp only [commonPrefix, minCpl, cpl_take]
    split
    · next h => rw [h, min_cpl_zero]
    · rw [List.take_take, ih _ (by omega)]
      rw [show min (min m (cpl a k)) m = min m (cpl a k) by omega]

theorem common_prefix_eq (k0 : List Nat) (ks : List (List Nat)) :
    commonPrefix k0 ks = minCpl k0 k0.length ks := by
  have := common_prefix_take k0 k0.length (Nat.le_refl _) ks
  rwa [List.take_length] at this

end Btdb.FirstDifference
