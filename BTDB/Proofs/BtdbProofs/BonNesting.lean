/-!
# Damaged Bon cannot nest containers forever

A Bon container is a type code at some position followed by a pointer to its body, whose items start there. The builder
writes the body, items included, before the pointer, so in valid data every container lies before the pointer to the
container holding it. Readers (`Bon.ReadContainerOffset` in `../Bon/Bon.cs`) read each container through a Bon whose
limit is the position of the pointer to the container holding its values, and accept the container only when its code
lies before that limit and its pointer points before its code. Damage can make a container's items run anywhere after
its body start.

* `fixed_depth`: every chain of nested containers that readers accept has strictly decreasing code positions, so it is
  at most one longer than the position of the outermost code, which is within the data.
* `previous_unbounded`: checking only that a pointer points before its code accepts chains of every length within
  twelve bytes, as a container whose items run past its pointer reads itself again; deserializing such data overflowed
  the stack before the 2026-10-28 fix.
-/

namespace Btdb.BonNesting

/-- A container read at type code position `code`, whose pointer points to `body`, through a Bon limited by `limit`. -/
structure Read where
  code : Nat
  body : Nat
  limit : Nat

/-- The reader's checks; `fixed` adds the limit. -/
def Accepted (fixed : Bool) (r : Read) : Prop :=
  r.body < r.code ∧ (fixed = true → r.code < r.limit)

/-- The child is read among the parent's items, which start at the parent's body and, damaged, may run anywhere after
it, through a Bon limited by the parent's code. -/
def Nested (parent child : Read) : Prop :=
  parent.body ≤ child.code ∧ child.limit = parent.code

/-- Containers each read among the items of the one before. -/
def Chain (fixed : Bool) : List Read → Prop
  | [] => True
  | [r] => Accepted fixed r
  | p :: c :: rest => Accepted fixed p ∧ Nested p c ∧ Chain fixed (c :: rest)

theorem fixed_depth : ∀ (r : Read) (rest : List Read), Chain true (r :: rest) → (r :: rest).length ≤ r.code + 1
  | r, [], _ => by simp
  | r, c :: rest, h => by
    obtain ⟨_, ⟨_, hl⟩, hc⟩ := h
    have hchild : c.code < c.limit := by
      cases rest with
      | nil => exact hc.2 rfl
      | cons _ _ => exact hc.1.2 rfl
    have := fixed_depth c rest hc
    simp only [List.length_cons] at this ⊢
    omega

/-- With only the pointer check, one container whose items run past its pointer is read inside itself without end. -/
theorem previous_unbounded (n : Nat) :
    ∃ chain : List Read, chain.length = n + 1 ∧ Chain false chain ∧ ∀ r ∈ chain, r.code < 12 := by
  let r : Read := ⟨11, 5, 11⟩
  have hr : Accepted false r := ⟨by decide, fun h => by cases h⟩
  have hn : Nested r r := ⟨by decide, rfl⟩
  have chain : ∀ k, Chain false (List.replicate (k + 1) r) := by
    intro k
    induction k with
    | zero => exact hr
    | succ k ih =>
      show Chain false (r :: r :: List.replicate k r)
      exact ⟨hr, hn, ih⟩
  refine ⟨List.replicate (n + 1) r, by simp, chain n, ?_⟩
  intro x hx
  rw [List.eq_of_mem_replicate hx]
  decide

end Btdb.BonNesting
