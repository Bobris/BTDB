/-!
# Inherited attributes of overridden properties

A model of `GetMemberAttributesWithInherited` (`../../../BTDB.SourceGenerator/Extensions.cs`), which the source generator
uses since the 2026-10-29 fix to read the attributes of a property together with those of the properties it overrides,
as reflection's `Attribute.GetCustomAttributes(member, type, inherit: true)` does. An attribute is its class, whether its
usage is `Inherited` and whether it `AllowMultiple`. The property comes first, then each property it overrides, from the
nearest to the farthest; a single-use attribute is kept only from the most derived member declaring it.

* `own_kept`: every attribute of the property itself is kept.
* `fromOverridden_fresh`: a single-use attribute taken from an overridden property is of no class a more derived
  member declares, so it never duplicates one, C# allowing one per member.
* `inherited_kept`: an `Inherited` attribute of an overridden property is kept unless it is single use and a more
  derived member declares its class, so a `[PrimaryKey]` on a virtual property stays on its override, which before the
  fix the generator lost (`RelationTests.VerifyRelationWithOverriddenKeyProperties`).
-/

namespace Btdb.InheritedAttributes

structure Attr where
  cls : Nat
  inherited : Bool
  allowMultiple : Bool
  deriving DecidableEq

/-- Attributes of overridden properties, nearest first, given the single-use classes kept so far. -/
def fromOverridden : List Nat → List (List Attr) → List Attr
  | _, [] => []
  | seen, level :: rest =>
    let kept := level.filter fun a => a.inherited && (a.allowMultiple || !seen.contains a.cls)
    kept ++ fromOverridden (seen ++ kept.map Attr.cls) rest

def collect (own : List Attr) (overridden : List (List Attr)) : List Attr :=
  own ++ fromOverridden (own.map Attr.cls) overridden

theorem own_kept (own : List Attr) (overridden : List (List Attr)) : ∀ a ∈ own, a ∈ collect own overridden :=
  fun _ h => List.mem_append_left _ h

theorem fromOverridden_fresh : ∀ (seen : List Nat) (levels : List (List Attr)) (a : Attr),
    a ∈ fromOverridden seen levels → a.allowMultiple = false → a.cls ∉ seen
  | _, [], _, h, _ => by simp [fromOverridden] at h
  | seen, level :: rest, a, h, hm => by
    simp only [fromOverridden, List.mem_append, List.mem_filter, Bool.and_eq_true, Bool.or_eq_true,
      Bool.not_eq_true', List.contains_eq_any_beq] at h
    rcases h with ⟨_, _, h | h⟩ | h
    · simp [h] at hm
    · intro hs; simp at h; exact h a.cls hs (by simp)
    · intro hs; exact fromOverridden_fresh _ rest a h hm (List.mem_append_left _ hs)

theorem inherited_kept (own : List Attr) (level : List Attr) (rest : List (List Attr)) (a : Attr)
    (ha : a ∈ level) (hi : a.inherited = true) (hfree : a.allowMultiple = true ∨ a.cls ∉ own.map Attr.cls) :
    a ∈ collect own (level :: rest) := by
  unfold collect fromOverridden
  apply List.mem_append_right
  apply List.mem_append_left
  simp only [List.mem_filter, Bool.and_eq_true, Bool.or_eq_true, Bool.not_eq_true', List.contains_eq_any_beq]
  refine ⟨ha, hi, ?_⟩
  cases hfree with
  | inl h => exact Or.inl h
  | inr h => right; simp only [List.any_eq_false, beq_iff_eq]; intro x hx e; exact h (e ▸ hx)

end Btdb.InheritedAttributes
