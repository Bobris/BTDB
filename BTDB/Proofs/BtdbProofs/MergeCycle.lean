import BtdbProofs.DictionaryFree

/-!
# Merging a re-serialized event descriptor ends

`EventSerializer.MergeDescriptor` (`../EventStore2Layer/EventSerializer.cs`) maps the descriptor an object loaded without
its type remembers, and the descriptors nested in it, to the serializer's own. A descriptor met again becomes a
placeholder instead of being mapped again. Before the 2026-10-28 fix only the root was recorded as met, so a type holding
itself below the root, like a tree node inside an event, was mapped inside itself until the stack overflowed. The walk
that records every descriptor is the one `DictionaryFree.lean` models: descriptors are numbers below `n` and `children`
lists the descriptors nested in each.

* `fixed_ends`: recording every descriptor, merging ends within `n + 1` levels whatever the descriptors nest.
* `previous_never_ends`: recording only the root, a descriptor below it nesting itself exhausts any bound.
-/

namespace Btdb.MergeCycle

open Btdb.DictionaryFree (walk walk_ends)

variable (children : Nat → List Nat)

theorem fixed_ends (n : Nat) (hc : ∀ x, ∀ c ∈ children x, c < n) (root : Nat) (h : root < n) :
    ∃ r, walk children (n + 1) [] root = some r ∧ r.Nodup :=
  walk_ends children n hc root h

/-- Merging before the fix: a descriptor is a placeholder only when it is the root met again. -/
def previous (root : Nat) : Nat → Nat → Option Unit
  | 0, _ => none
  | fuel + 1, d => (children d).foldlM (fun _ c => if c = root then some () else previous root fuel c) ()

theorem previous_never_ends (root d : Nat) (hd : d ≠ root) (hself : children d = [d]) :
    ∀ fuel, previous children root fuel d = none
  | 0 => rfl
  | fuel + 1 => by
    simp only [previous, hself, List.foldlM, hd, ite_false, previous_never_ends root d hd hself fuel]
    rfl

end Btdb.MergeCycle
