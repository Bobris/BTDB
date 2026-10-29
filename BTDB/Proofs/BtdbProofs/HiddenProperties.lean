/-!
# Hidden properties leave one field per name

A model of `EmitHelpers.GetVisibleProperties` (`../../IL/EmitHelpers.cs`), which ObjectDB and the event store use since
the 2026-10-29 fix to list the stored properties of a type. Reflection lists the properties of a class and of its base
classes; a property is `(name, depth)`, `depth` being how far down the inheritance chain its declaring class is, and a
class declares at most one property of a name, so no two listed properties share both. The filter drops a property
when another of its name is declared by a more derived class.

* `visible_unique`: no two kept properties share a name, so a type gets one field per name, as in the source
  generator's metadata; before the fix a property hidden with `new` made two
  (`ObjectDbMemberShapesTest`).
* `visible_complete`: every name listed keeps a property, the most derived one, so the filter drops no field.
-/

namespace Btdb.HiddenProperties

abbrev Prop' := String × Nat

def visible (props : List Prop') : List Prop' :=
  props.filter fun p => !props.any fun o => o.1 = p.1 && p.2 < o.2

theorem visible_unique (props : List Prop') (hdecl : ∀ a ∈ props, ∀ b ∈ props, a.1 = b.1 → a.2 = b.2 → a = b)
    (a b : Prop') (ha : a ∈ visible props) (hb : b ∈ visible props) (hn : a.1 = b.1) : a = b := by
  simp only [visible, List.mem_filter, Bool.not_eq_eq_eq_not, Bool.not_true, List.any_eq_false,
    Bool.and_eq_true, decide_eq_true_eq, not_and] at ha hb
  obtain ⟨ha, hna⟩ := ha
  obtain ⟨hb, hnb⟩ := hb
  apply hdecl a ha b hb hn
  have h1 : ¬ a.2 < b.2 := hna b hb hn.symm
  have h2 : ¬ b.2 < a.2 := hnb a ha hn
  omega

theorem visible_complete (props : List Prop') (name : String) (h : ∃ p ∈ props, p.1 = name) :
    ∃ p ∈ visible props, p.1 = name := by
  -- The deepest property of the name is kept.
  let named := props.filter fun p => p.1 = name
  have hne : named ≠ [] := by
    obtain ⟨p, hp, hpn⟩ := h
    exact List.ne_nil_of_mem (List.mem_filter.2 ⟨hp, by simp [hpn]⟩)
  let deepest := named.foldl (fun m p => if m.2 < p.2 then p else m) (named.head hne)
  have key : ∀ (l : List Prop') (m : Prop'), (m ∈ props ∧ m.1 = name) → (∀ p ∈ l, p ∈ props ∧ p.1 = name) →
      let r := l.foldl (fun m p => if m.2 < p.2 then p else m) m
      (r ∈ props ∧ r.1 = name) ∧ m.2 ≤ r.2 ∧ ∀ p ∈ l, p.2 ≤ r.2 := by
    intro l
    induction l with
    | nil => intro m hm _; exact ⟨hm, Nat.le_refl _, fun p hp => by simp at hp⟩
    | cons x xs ih =>
      intro m hm hl
      simp only [List.foldl_cons]
      have hx := hl x List.mem_cons_self
      have hxs : ∀ p ∈ xs, p ∈ props ∧ p.1 = name := fun p hp => hl p (List.mem_cons_of_mem _ hp)
      by_cases hlt : m.2 < x.2
      · simp only [hlt, ↓reduceIte]
        obtain ⟨h1, h2, h3⟩ := ih x hx hxs
        refine ⟨h1, by omega, fun p hp => ?_⟩
        cases List.mem_cons.1 hp with
        | inl e => subst e; exact h2
        | inr e => exact h3 p e
      · simp only [hlt, ↓reduceIte]
        obtain ⟨h1, h2, h3⟩ := ih m hm hxs
        refine ⟨h1, h2, fun p hp => ?_⟩
        cases List.mem_cons.1 hp with
        | inl e => subst e; omega
        | inr e => exact h3 p e
  have hhead : named.head hne ∈ props ∧ (named.head hne).1 = name := by
    have := List.mem_filter.1 (List.head_mem hne)
    exact ⟨this.1, by simpa using this.2⟩
  have hall : ∀ p ∈ named, p ∈ props ∧ p.1 = name := fun p hp => by
    have := List.mem_filter.1 hp; exact ⟨this.1, by simpa using this.2⟩
  obtain ⟨⟨hd, hdn⟩, -, hmax⟩ := key named (named.head hne) hhead hall
  refine ⟨deepest, ?_, hdn⟩
  simp only [visible, List.mem_filter, Bool.not_eq_eq_eq_not, Bool.not_true, List.any_eq_false,
    Bool.and_eq_true, decide_eq_true_eq, not_and]
  refine ⟨hd, fun o ho hon => ?_⟩
  simp only [deepest] at hon ⊢
  have hon' : o.1 = name := by rw [hon]; exact hdn
  have := hmax o (List.mem_filter.2 ⟨ho, by simp [hon']⟩)
  omega

end Btdb.HiddenProperties
