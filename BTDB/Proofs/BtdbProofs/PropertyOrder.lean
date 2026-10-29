/-!
# Listing private properties of base classes keeps every other schema

A model of how `EmitHelpers.GetVisibleProperties` (`../../IL/EmitHelpers.cs`) lists a type's properties since the
2026-10-29 fix: each class's own declared properties, the most derived class first, each in declaration order, as the
source generator's metadata has them. Reflection's `Type.GetProperties` listed them in the same order but left out
private properties of base classes. A class is the list of its declared properties, each private or not.

* `new_eq_old_without_private_bases`: for a type whose base classes have no private properties, the new listing is the
  old one, field for field and in order, so stored relations and tables of such types keep their schema.
* `new_has_private_bases`: the new listing has the private properties of base classes too, which the generator stores.
-/

namespace Btdb.PropertyOrder

structure Prop' where
  name : String
  isPrivate : Bool

/-- `Type.GetProperties`: all properties of the type itself, then those of its base classes that are not private. -/
def oldListing : List (List Prop') → List Prop'
  | [] => []
  | own :: bases => own ++ (bases.map fun c => c.filter fun p => !p.isPrivate).flatten

/-- The fix: all declared properties of every class, the most derived first. -/
def newListing (classes : List (List Prop')) : List Prop' := classes.flatten

theorem new_eq_old_without_private_bases (own : List Prop') (bases : List (List Prop'))
    (h : ∀ c ∈ bases, ∀ p ∈ c, p.isPrivate = false) : newListing (own :: bases) = oldListing (own :: bases) := by
  simp only [newListing, oldListing, List.flatten_cons]
  congr 1
  congr 1
  conv => lhs; rw [← List.map_id bases]
  apply List.map_congr_left
  intro c hc
  symm
  show List.filter _ c = c
  rw [List.filter_eq_self]
  intro p hp
  simp [h c hc p hp]

theorem new_has_private_bases (classes : List (List Prop')) (c : List Prop') (hc : c ∈ classes) (p : Prop')
    (hp : p ∈ c) : p ∈ newListing classes :=
  List.mem_flatten.2 ⟨c, hc, hp⟩

end Btdb.PropertyOrder
