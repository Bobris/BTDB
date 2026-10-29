/-!
# The source generator injects each `[Dependency]` member of a type and its base types once

`DependencyMembers` (`../../BTDB.SourceGenerator/Main.cs`) walks the members of a type and then of each base type,
most derived first, and keeps the names it has seen. A member whose name was seen before is hidden or overridden in a
derived type and is skipped; any other member marked `[Dependency]` is injected. Before the 2026-10-29 fix only the
members of the type itself were walked. `members` lists the members in the order the walk meets them, with `dep` set
for those marked `[Dependency]`.

* `collect_nodup`: no name is injected twice.
* `collect_complete`: a `[Dependency]` member that no earlier member hides is injected, whichever base declares it.
* `previous_misses_base`: walking only the type itself missed a dependency declared in its base.
-/

namespace Btdb.InheritedDependencies

structure Member where
  name : Nat
  dep : Bool
  deriving DecidableEq

/-- One member of the walk: the names seen and the members injected so far. -/
def step : List Nat × List Member → Member → List Nat × List Member
  | (seen, out), m => if m.name ∈ seen then (seen, out) else (m.name :: seen, if m.dep then out ++ [m] else out)

/-- The injected members of a type whose members, then those of each base type, are `levels`. -/
def collect (levels : List (List Member)) : List Member := (levels.flatten.foldl step ([], [])).2

/-- Before the fix: the `[Dependency]` members of the type itself. -/
def previous (levels : List (List Member)) : List Member := (levels.headD []).filter (·.dep)

def Inv (s : List Nat × List Member) : Prop := (s.2.map (·.name)).Nodup ∧ ∀ x ∈ s.2.map (·.name), x ∈ s.1

theorem step_inv (s : List Nat × List Member) (m : Member) (h : Inv s) : Inv (step s m) := by
  obtain ⟨seen, out⟩ := s
  obtain ⟨hnd, hsub⟩ := h
  unfold step
  by_cases hm : m.name ∈ seen
  · simp only [hm, ite_true]; exact ⟨hnd, hsub⟩
  · simp only [hm, ite_false]
    refine ⟨?_, ?_⟩
    · split
      · rw [List.map_append, List.nodup_append]
        refine ⟨hnd, by simp, ?_⟩
        intro a ha b hb
        simp only [List.map_cons, List.map_nil, List.mem_singleton] at hb
        subst hb
        intro hab; subst hab; exact hm (hsub _ ha)
      · exact hnd
    · intro x hx
      split at hx
      · rw [List.map_append, List.mem_append] at hx
        rcases hx with hx | hx
        · exact List.mem_cons_of_mem _ (hsub x hx)
        · simp at hx; simp [hx]
      · exact List.mem_cons_of_mem _ (hsub x hx)

theorem foldl_inv (l : List Member) (s : List Nat × List Member) (h : Inv s) : Inv (l.foldl step s) := by
  induction l generalizing s with
  | nil => exact h
  | cons m rest ih => exact ih _ (step_inv s m h)

theorem collect_nodup (levels : List (List Member)) : ((collect levels).map (·.name)).Nodup :=
  (foldl_inv _ ([], []) ⟨List.nodup_nil, fun _ h => by simp at h⟩).1

/-- After walking `l`, the names seen are those seen before and those of `l`. -/
theorem foldl_seen (l : List Member) (s : List Nat × List Member) (x : Nat) :
    x ∈ (l.foldl step s).1 ↔ x ∈ s.1 ∨ x ∈ l.map (·.name) := by
  induction l generalizing s with
  | nil => simp
  | cons m rest ih =>
    simp only [List.foldl_cons, List.map_cons, List.mem_cons]
    rw [ih]
    obtain ⟨seen, out⟩ := s
    unfold step
    by_cases hm : m.name ∈ seen
    · simp only [hm, ite_true]
      constructor
      · rintro (h | h)
        · exact Or.inl h
        · exact Or.inr (Or.inr h)
      · rintro (h | h | h)
        · exact Or.inl h
        · subst h; exact Or.inl hm
        · exact Or.inr h
    · simp only [hm, ite_false, List.mem_cons]
      constructor
      · rintro ((h | h) | h)
        · exact Or.inr (Or.inl h)
        · exact Or.inl h
        · exact Or.inr (Or.inr h)
      · rintro (h | h | h)
        · exact Or.inl (Or.inr h)
        · exact Or.inl (Or.inl h)
        · exact Or.inr h

/-- A member injected stays injected. -/
theorem foldl_keeps (l : List Member) (s : List Nat × List Member) (m : Member) (h : m ∈ s.2) :
    m ∈ (l.foldl step s).2 := by
  induction l generalizing s with
  | nil => exact h
  | cons c rest ih =>
    apply ih
    obtain ⟨seen, out⟩ := s
    show m ∈ (step (seen, out) c).2
    simp only [step]
    split
    · exact h
    · dsimp only
      split
      · exact List.mem_append_left _ h
      · exact h

theorem collect_complete (levels : List (List Member)) (pre post : List Member) (m : Member)
    (hsplit : levels.flatten = pre ++ m :: post) (hdep : m.dep = true) (hfirst : m.name ∉ pre.map (·.name)) :
    m ∈ collect levels := by
  unfold collect
  rw [hsplit, List.foldl_append, List.foldl_cons]
  apply foldl_keeps
  generalize hs : pre.foldl step ([], []) = s
  have hseen : m.name ∉ s.1 := by
    rw [← hs, foldl_seen]
    simp only [List.not_mem_nil, false_or]
    exact hfirst
  obtain ⟨seen, out⟩ := s
  unfold step
  simp only [hseen, ite_false, hdep, ite_true]
  exact List.mem_append_right _ (List.mem_singleton_self m)

/-- A type with no members of its own whose base declares one `[Dependency]` member. -/
theorem previous_misses_base :
    ⟨0, true⟩ ∉ previous [[], [⟨0, true⟩]] ∧ ⟨0, true⟩ ∈ collect [[], [⟨0, true⟩]] := by
  decide

end Btdb.InheritedDependencies
