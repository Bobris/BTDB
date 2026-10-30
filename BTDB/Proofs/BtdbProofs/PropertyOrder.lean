/-!
# Listing marked private properties of base classes keeps every other schema

A model of how `EmitHelpers.GetVisibleProperties` (`../../IL/EmitHelpers.cs`) lists a type's properties: each class's
own declared properties, the most derived class first, each in declaration order, as the source generator's metadata
has them. Of a base class, a private property is listed only when a storage attribute (`PersistedName`, `PrimaryKey`,
`SecondaryKey`, `InKeyValue`) marks it; the generator leaves out the same ones and warns of them (BTDB0059).
Reflection's `Type.GetProperties` listed the properties in the same order but left out all private properties of base
classes. A class is the list of its declared properties, each private or not, marked or not.

* `new_eq_old_without_marked_private_bases`: for a type whose base classes have no marked private property, the
  listing is the old one, field for field and in order, so stored relations and tables of such types keep their schema.
* `new_has_marked_private_bases`: a marked private property of a base class is listed, so it is stored.
* `new_drops_unmarked_private_bases`: an unmarked private property of a base class is not listed.
-/

namespace Btdb.PropertyOrder

structure Prop' where
  name : String
  isPrivate : Bool
  marked : Bool

/-- `Type.GetProperties`: all properties of the type itself, then those of its base classes that are not private. -/
def oldListing : List (List Prop') → List Prop'
  | [] => []
  | own :: bases => own ++ (bases.map fun c => c.filter fun p => !p.isPrivate).flatten

/-- All declared properties of the type itself, then those of its base classes that are not private or are marked. -/
def newListing : List (List Prop') → List Prop'
  | [] => []
  | own :: bases => own ++ (bases.map fun c => c.filter fun p => !p.isPrivate || p.marked).flatten

theorem new_eq_old_without_marked_private_bases (own : List Prop') (bases : List (List Prop'))
    (h : ∀ c ∈ bases, ∀ p ∈ c, p.isPrivate = true → p.marked = false) :
    newListing (own :: bases) = oldListing (own :: bases) := by
  simp only [newListing, oldListing]
  congr 2
  apply List.map_congr_left
  intro c hc
  apply List.filter_congr
  intro p hp
  cases hpriv : p.isPrivate
  · simp
  · simp [h c hc p hp hpriv]

theorem new_has_marked_private_bases (own : List Prop') (bases : List (List Prop')) (c : List Prop')
    (hc : c ∈ bases) (p : Prop') (hp : p ∈ c) (hm : p.marked = true) : p ∈ newListing (own :: bases) := by
  simp only [newListing, List.mem_append, List.mem_flatten, List.mem_map]
  exact Or.inr ⟨_, ⟨c, hc, rfl⟩, List.mem_filter.2 ⟨hp, by simp [hm]⟩⟩

theorem new_drops_unmarked_private_bases (own : List Prop') (bases : List (List Prop')) (p : Prop')
    (hown : p ∉ own) (hpriv : p.isPrivate = true) (hm : p.marked = false) : p ∉ newListing (own :: bases) := by
  simp only [newListing, List.mem_append, List.mem_flatten, List.mem_map, not_or]
  refine ⟨hown, ?_⟩
  rintro ⟨_, ⟨c, _, rfl⟩, hp⟩
  simp [hpriv, hm] at hp

end Btdb.PropertyOrder
