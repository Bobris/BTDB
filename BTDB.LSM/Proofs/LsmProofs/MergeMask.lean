/-!
# Skipping components a range tombstone masks loses no key

A model of how `MergingIterator` resolves a key (`../Engine/MergingIterator.cs`). Components are listed newest first.
In a component a key has a point entry (`some true` set, `some false` deleted) or none, and may lie in one of its
range-tombstone fragments. A key is live when the newest component that decides it, by a point or else by a fragment,
sets it; a component that decides neither leaves the key to the older ones. When the iterator meets a fragment of a
component that holds the key, it seeks every older component past the fragment end in one step.

* `decided_ignores_older`: once a component decides a key, older components cannot change it, so seeking them past a
  fragment of a newer component skips no live key.
* `newest_point_wins`: a component with a point at the key decides it by the point, whatever the older components hold.
-/

namespace Lsm.MergeMask

structure Component where
  point : Nat → Option Bool
  fragment : Nat → Bool

/-- Whether a key is live in a stack of components, newest first. -/
def live : List Component → Nat → Bool
  | [], _ => false
  | c :: older, k =>
    match c.point k with
    | some set => set
    | none => if c.fragment k then false else live older k

def Decides (c : Component) (k : Nat) : Prop := c.point k ≠ none ∨ c.fragment k = true

theorem decided_ignores_older (newer older older' : List Component) (c : Component) (k : Nat)
    (h : Decides c k) : live (newer ++ c :: older) k = live (newer ++ c :: older') k := by
  induction newer with
  | nil =>
    simp only [List.nil_append, live]
    cases hp : c.point k with
    | some set => rfl
    | none =>
      have hf : c.fragment k = true := h.resolve_left (by simp [hp])
      simp [hf]
  | cons d newer ih =>
    simp only [List.cons_append, live]
    cases d.point k with
    | some set => rfl
    | none => simp only [ih]

theorem newest_point_wins (newer older : List Component) (c : Component) (k : Nat) (set : Bool)
    (hnewer : ∀ d ∈ newer, d.point k = none ∧ d.fragment k = false) (hc : c.point k = some set) :
    live (newer ++ c :: older) k = set := by
  induction newer with
  | nil => simp [live, hc]
  | cons d newer ih =>
    obtain ⟨hp, hf⟩ := hnewer d List.mem_cons_self
    simp only [List.cons_append, live, hp, hf]
    exact ih (fun e he => hnewer e (List.mem_cons_of_mem _ he))

end Lsm.MergeMask
