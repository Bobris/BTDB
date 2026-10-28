/-!
# A failed Bon serializer creation leaves no placeholder behind

`BonSerializerFactory` caches a placeholder for a class while it creates the class's serializer or deserializer, so
creating a recursive type finds it. Creation replaces the placeholder with the result only while the placeholder is
still cached (`TryUpdate`). A placeholder that stays cached once no creation runs is fatal: serializing the class waits
for its replacement forever, and the deserializer placeholder deserializes the same value again until the stack
overflows. Before the 2026-10-28 fix a creation that threw left it cached; now it removes it only while it is still the
placeholder (`TryRemove` of the key and value).

Creations of the same type may run concurrently, and each may succeed or fail.

* `fixed_no_stale_placeholder`: once no creation runs, the placeholder is not cached.
* `previous_stale_placeholder`: before the fix one failed creation left it cached with no creation running.
-/

namespace Btdb.SerializerPlaceholder

inductive Entry
  | none
  | placeholder
  | real
  deriving DecidableEq

structure St where
  entry : Entry
  /-- Creations of the type that started and did not finish. -/
  running : List Nat

/-- `fixed` selects the current failure path, which removes the placeholder while it is still cached. -/
inductive Step (fixed : Bool) : St → St → Prop
  /-- `TryAdd` of the placeholder: it is cached only when nothing is. -/
  | begin (s : St) (i : Nat) (h : i ∉ s.running) :
      Step fixed s ⟨if s.entry = .none then .placeholder else s.entry, i :: s.running⟩
  /-- `TryUpdate` of the placeholder with the created serializer. -/
  | succeed (s : St) (i : Nat) (h : i ∈ s.running) :
      Step fixed s ⟨if s.entry = .placeholder then .real else s.entry, s.running.erase i⟩
  | fail (s : St) (i : Nat) (h : i ∈ s.running) :
      Step fixed s ⟨if fixed && s.entry = .placeholder then .none else s.entry, s.running.erase i⟩

inductive Reach (fixed : Bool) : St → Prop
  | init : Reach fixed ⟨.none, []⟩
  | step {s t : St} : Reach fixed s → Step fixed s t → Reach fixed t

/-- The placeholder is cached only while some creation runs. -/
theorem inv {s : St} (h : Reach true s) : s.entry = .placeholder → s.running ≠ [] := by
  induction h with
  | init => intro h; cases h
  | step _ hs ih =>
    cases hs with
    | begin i _ => intro _; simp
    | succeed i _ =>
      intro he
      simp only at he
      split at he <;> simp_all
    | fail i _ =>
      intro he
      simp only [Bool.true_and] at he
      split at he <;> simp_all

theorem fixed_no_stale_placeholder {s : St} (h : Reach true s) (hidle : s.running = []) :
    s.entry ≠ .placeholder :=
  fun he => inv h he hidle

/-- Before the fix: one creation starts, caching the placeholder, and fails. -/
theorem previous_stale_placeholder : ∃ s, Reach false s ∧ s.running = [] ∧ s.entry = .placeholder := by
  have r1 : Reach false ⟨.placeholder, [0]⟩ := .step .init (.begin _ 0 (by simp))
  have r2 : Reach false ⟨.placeholder, []⟩ := .step r1 (.fail _ 0 (by simp))
  exact ⟨_, r2, rfl, rfl⟩

end Btdb.SerializerPlaceholder
