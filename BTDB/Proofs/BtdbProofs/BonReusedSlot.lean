/-!
# A null item in a Bon list or dictionary loads as itself

`BonSerializer` deserializes the items of a list or dictionary one after another into one storage slot, and adds the
slot's content to the collection after each item. An item the item deserializer does not write, as a null string or
byte array was before the 2026-10-28 fix, leaves the slot holding whatever it held. Since the fix the slot is cleared to
the default before each item. Items are `some v` for an item that loads as `v` and `none` for one that writes nothing.

* `fixed_loads_each`: with the clear every item loads as itself, and an item that writes nothing as the default,
  whatever the slot held before.
* `previous_copies_before`: without it an item that writes nothing loads as the item before it.
-/

namespace Btdb.BonReusedSlot

variable {α : Type}

/-- The items the collection gets; `clear` empties the slot to `d` before each item. -/
def load (clear : Bool) (d : α) : α → List (Option α) → List α
  | _, [] => []
  | slot, item :: rest =>
    let v := item.getD (if clear then d else slot)
    v :: load clear d v rest

theorem fixed_loads_each (d : α) : ∀ (slot : α) (items : List (Option α)),
    load true d slot items = items.map (·.getD d)
  | _, [] => rfl
  | _, item :: rest => by
    simp only [load, ite_true, List.map_cons, fixed_loads_each d _ rest]

/-- Before the fix, a null after `a` loaded as `a` instead of the default. -/
theorem previous_copies_before (d a : α) (h : a ≠ d) :
    load false d d [some a, none] = [a, a] ∧ load false d d [some a, none] ≠ [some a, none].map (·.getD d) := by
  refine ⟨rfl, ?_⟩
  simp only [load, Bool.false_eq_true, ite_false, Option.getD_some, Option.getD_none, List.map_cons, List.map_nil,
    ne_eq, List.cons.injEq, true_and, and_true]
  exact h

end Btdb.BonReusedSlot
