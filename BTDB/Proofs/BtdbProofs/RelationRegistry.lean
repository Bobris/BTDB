/-!
# Initializing relations drops no relation registered meanwhile

A model of how `ObjectDB.InitializeRelations` (`../../ODBLayer/ObjectDB.cs`) publishes the relation registry
(`../../ODBLayer/RelationsInfo.cs`). `InitRelation`, as auto-registration runs it, adds a relation to the current
registry in place, bumping its version; one that finds the registry replaced retries on the current one, so it always
adds to the current registry. `InitializeRelations`, one call at a time, copies the registry with its version, prepares
its relations in the copy, possibly waiting for the writer meanwhile, and then publishes the copy with them only when the
version is still the copied one (`TryPublishRelations`); otherwise it starts again.

* `reachable_inv`: every relation ever registered is in the current registry.
* `old_drops`: before the 2026-10-29 fix the copy was published whatever happened since it was taken, and a relation
  registered meanwhile was dropped (`ObjectDbInitializeRelationsTest.InitializationKeepsRelationInitializedWhileItWaitedForWriter`).
-/

namespace Btdb.RelationRegistry

structure State where
  current : List Nat
  version : Nat
  /-- Every relation registered so far. -/
  registered : List Nat
  /-- The copy an `InitializeRelations` call took, with the version. -/
  pending : Option (List Nat × Nat)

inductive Step : State → State → Prop
  | register (s : State) (x : Nat) :
      Step s { s with current := x :: s.current, version := s.version + 1, registered := x :: s.registered }
  | copy (s : State) (h : s.pending = none) : Step s { s with pending := some (s.current, s.version) }
  | publish (s : State) (l : List Nat) (v : Nat) (adds : List Nat) (h : s.pending = some (l, v)) (hv : s.version = v) :
      Step s { current := adds ++ l, version := s.version + 1, registered := adds ++ s.registered, pending := none }
  | retry (s : State) (l : List Nat) (v : Nat) (h : s.pending = some (l, v)) (hv : s.version ≠ v) :
      Step s { s with pending := none }

inductive Reachable : State → Prop
  | init : Reachable ⟨[], 0, [], none⟩
  | step {s s'} : Reachable s → Step s s' → Reachable s'

structure Inv (s : State) : Prop where
  kept : ∀ x ∈ s.registered, x ∈ s.current
  copied : ∀ l v, s.pending = some (l, v) → v ≤ s.version ∧ (s.version = v → s.current = l)

theorem reachable_inv {s : State} (h : Reachable s) : Inv s := by
  induction h with
  | init => exact ⟨fun _ h => by simp at h, fun _ _ h => by simp at h⟩
  | @step s s' _ hs ih =>
    obtain ⟨hk, hc⟩ := ih
    cases hs with
    | register x =>
      refine ⟨fun y hy => ?_, fun l v hp => ?_⟩
      · simp only [List.mem_cons] at hy ⊢
        cases hy with
        | inl h => exact Or.inl h
        | inr h => exact Or.inr (hk y h)
      · obtain ⟨h1, _⟩ := hc l v hp
        exact ⟨by simp only; omega, fun h => by simp only at h; omega⟩
    | copy hp => exact ⟨hk, fun l v h => by simp at h; obtain ⟨rfl, rfl⟩ := h; exact ⟨Nat.le_refl _, fun _ => rfl⟩⟩
    | publish l v adds hp hv =>
      refine ⟨fun y hy => ?_, fun _ _ h => by simp at h⟩
      simp only [List.mem_append] at hy ⊢
      cases hy with
      | inl h => exact Or.inl h
      | inr h => exact Or.inr (((hc l v hp).2 hv) ▸ hk y h)
    | retry l v hp hv => exact ⟨hk, fun _ _ h => by simp at h⟩

/-- Publishing before the fix: the copy, whatever the version. -/
def oldPublish (s : State) (l : List Nat) (adds : List Nat) : State :=
  { current := adds ++ l, version := s.version + 1, registered := adds ++ s.registered, pending := none }

/-- A call copies the empty registry, relation 1 is registered meanwhile, and the call publishes relation 2. -/
theorem old_drops :
    let s₁ : State := ⟨[], 0, [], some ([], 0)⟩
    let s₂ : State := { s₁ with current := 1 :: s₁.current, version := s₁.version + 1, registered := 1 :: s₁.registered }
    let s₃ := oldPublish s₂ [] [2]
    1 ∈ s₃.registered ∧ 1 ∉ s₃.current := by
  decide

end Btdb.RelationRegistry
