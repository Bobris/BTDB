/-!
# Shared helpers

Finite maps are total functions `Nat → α` updated pointwise with `upd`.
-/

namespace Lsm

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

end Lsm
