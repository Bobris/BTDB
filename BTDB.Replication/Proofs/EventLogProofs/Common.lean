/-!
# Shared helpers

Finite maps are total functions `Nat → α` updated pointwise with `upd`.
-/

namespace EventLog

/-- `omega` after reducing structure projections such as `{ σ with v := σ.v + 1 }.v`. -/
macro "arith" : tactic => `(tactic| ((try dsimp only at *); omega))

def upd {α : Type} (f : Nat → α) (i : Nat) (x : α) : Nat → α := fun j => if j = i then x else f j

@[simp] theorem upd_same {α : Type} (f : Nat → α) (i : Nat) (x : α) : upd f i x i = x := by
  simp [upd]

theorem upd_ne {α : Type} (f : Nat → α) {i j : Nat} (x : α) (h : j ≠ i) : upd f i x j = f j := by
  simp [upd, h]

/-- Every value of an updated map is the new value or an old one. -/
theorem upd_cases {α : Type} (f : Nat → α) (i j : Nat) (x : α) :
    (j = i ∧ upd f i x j = x) ∨ (j ≠ i ∧ upd f i x j = f j) := by
  by_cases h : j = i
  · exact Or.inl ⟨h, by simp [upd, h]⟩
  · exact Or.inr ⟨h, by simp [upd, h]⟩

theorem range'_split : ∀ (A B : List Nat) (a n : Nat), A ++ B = List.range' a n →
    A = List.range' a A.length ∧ B = List.range' (a + A.length) (n - A.length)
  | [], B, a, n, h => by simpa using h
  | x :: A, B, a, n, h => by
    cases n with
    | zero => simp at h
    | succ n =>
      rw [List.range'_succ] at h
      simp only [List.cons_append, List.cons.injEq] at h
      obtain ⟨rfl, h⟩ := h
      obtain ⟨h1, h2⟩ := range'_split A B (x + 1) n h
      refine ⟨?_, ?_⟩
      · simp only [List.length_cons, List.range'_succ]
        exact congrArg _ h1
      · rw [h2]; simp only [List.length_cons]
        congr 1 <;> omega

theorem range'_app (a m n : Nat) : List.range' a m ++ List.range' (a + m) n = List.range' a (m + n) := by
  induction m generalizing a with
  | zero => simp
  | succ m ih =>
    rw [List.range'_succ, Nat.add_right_comm, List.range'_succ, List.cons_append]
    congr 1
    have := ih (a + 1)
    rw [show a + 1 + m = a + (m + 1) by omega] at this
    exact this

theorem mem_range'_iff {x a n : Nat} : x ∈ List.range' a n ↔ a ≤ x ∧ x < a + n := by
  induction n generalizing a with
  | zero => simp
  | succ n ih => rw [List.range'_succ, List.mem_cons, ih]; omega

theorem mem_take_or_drop {α : Type} {e : α} {l : List α} (n : Nat) (h : e ∈ l) :
    e ∈ l.take n ∨ e ∈ l.drop n := by
  rw [← List.take_append_drop n l, List.mem_append] at h
  exact h

theorem getElem?_of_prefix {α : Type} {l t : List α} {k : Nat} {e : α} (hp : l <+: t)
    (he : l[k]? = some e) : t[k]? = some e := by
  obtain ⟨u, rfl⟩ := hp
  have hk : k < l.length := by
    rcases Nat.lt_or_ge k l.length with h | h
    · exact h
    · rw [List.getElem?_eq_none h] at he; cases he
  rw [List.getElem?_append_left hk]
  exact he

theorem upd_some {α : Type} {f : Nat → Option α} {i j : Nat} {x y : α} (h : upd f i (some x) j = some y) :
    (j = i ∧ y = x) ∨ (j ≠ i ∧ f j = some y) := by
  by_cases hj : j = i
  · subst hj; simp [upd] at h; exact Or.inl ⟨rfl, h.symm⟩
  · simp [upd, hj] at h; exact Or.inr ⟨hj, h⟩

theorem upd_none {α : Type} {f : Nat → Option α} {i j : Nat} {y : α} (h : upd f i none j = some y) :
    j ≠ i ∧ f j = some y := by
  by_cases hj : j = i
  · subst hj; simp [upd] at h
  · simp [upd, hj] at h; exact ⟨hj, h⟩

end EventLog
