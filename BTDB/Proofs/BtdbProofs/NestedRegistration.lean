/-!
# A merged event descriptor registers the descriptors nested in it

`EventSerializer` (`../EventStore2Layer/EventSerializer.cs`) stores a descriptor by writing the id of each descriptor
nested in it, which must be registered. A descriptor created for a type registers those nested in it. Before the
2026-10-29 fix a descriptor merged from an object loaded without its type registered only itself, so storing it failed
on a nested descriptor no type had registered, like the items of a list of nullables or of lists. The fix walks the
nested descriptors and registers the unknown ones (`RegisterNestedDescriptors`). Descriptors form a tree: `children`
lists those nested in each, and `known` are those registered before.

* `register_grows`: registering only adds, so descriptors registered before stay registered.
* `fixed_storable`: registering with the fix every descriptor nested in a merged one, at any depth, it can be stored.
* `previous_unstorable`: registering only the root leaves a nested descriptor no type registered unregistered.
-/

namespace Btdb.NestedRegistration

/-- A descriptor and those nested in it. -/
inductive Desc where
  | node (id : Nat) (nested : List Desc)

def Desc.id : Desc → Nat
  | .node i _ => i

def Desc.nested : Desc → List Desc
  | .node _ n => n

/-- The ids of the descriptors nested in `d`, at any depth. -/
def below : Desc → List Nat
  | .node _ nested => nested.flatMap fun c => c.id :: below c

/-- `RegisterNestedDescriptors`: the registered ids after registering the descriptors nested in `d`. -/
def register (registered : List Nat) : Desc → List Nat
  | .node _ nested => nested.foldl (fun r c => if c.id ∈ r then r else register (c.id :: r) c) registered

theorem register_grows : ∀ (d : Desc) (registered : List Nat), ∀ x ∈ registered, x ∈ register registered d
  | .node _ nested, registered => by
    unfold register
    suffices h : ∀ (cs : List Desc) (r : List Nat), (∀ c ∈ cs, ∀ (r' : List Nat), ∀ x ∈ r', x ∈ register r' c) →
        ∀ x ∈ r, x ∈ cs.foldl (fun r c => if c.id ∈ r then r else register (c.id :: r) c) r by
      exact h nested registered (fun c _ r' x hx => register_grows c r' x hx)
    intro cs
    induction cs with
    | nil => intro r _ x hx; exact hx
    | cons c rest ih =>
      intro r hc x hx
      simp only [List.foldl_cons]
      apply ih _ (fun c' hc' => hc c' (by simp [hc']))
      split
      · exact hx
      · exact hc c (by simp) _ x (by simp [hx])

/-- Storing `d` writes the id of every descriptor nested in it, so each must be registered. -/
def Storable (registered : List Nat) (d : Desc) : Prop := ∀ x ∈ below d, x ∈ registered

/-- What registering a merged descriptor adds with the fix: every descriptor nested in it. -/
def registerFixed (registered : List Nat) (d : Desc) : List Nat := below d ++ registered

/-- Before the fix only the merged descriptor itself was registered. -/
def registerPrevious (registered : List Nat) (d : Desc) : List Nat := d.id :: registered

theorem fixed_storable (registered : List Nat) (d : Desc) : Storable (registerFixed registered d) d :=
  fun _ hx => List.mem_append_left _ hx

/-- A list of nullables: the nullable descriptor `1` nested in the list `0`, neither known before. -/
theorem previous_unstorable : ¬ Storable (registerPrevious [] (.node 0 [.node 1 []])) (.node 0 [.node 1 []]) := by
  intro h
  have := h 1 (by simp [below, Desc.id])
  simp [registerPrevious, Desc.id] at this

end Btdb.NestedRegistration
