/-!
# An allocated singleton oid is never forgotten

A model of how `ObjectDB` stores the oid of a table's singleton (`../../ODBLayer/TableInfo.cs`,
`../../ODBLayer/ObjectDBTransaction.cs`). The first transaction asking for a singleton that has none allocates its oid
in the shared `TableInfo` and sets the shared `NeedStoreSingletonOid`. Any transaction, read-only ones included, can
do so at any time. The writer stores the oid with the table info while the flag is set (`PersistTableInfo`); its commit
makes that stored oid durable.

* `reachable_inv`: once an oid is allocated it is durable or still flagged, so the next writer storing the table's info,
  as storing the singleton does, stores it.
* `old_loses_oid`: before the 2026-10-29 fix a commit cleared the flag for every table whose info it stored, also when
  the oid was allocated after that; the oid then was neither durable nor flagged, and the singleton saved under it was
  lost on reopen (`ObjectDbSingletonCacheTest.SingletonOidAllocatedDuringOtherCommitIsStored`).
-/

namespace Btdb.SingletonOid

structure State where
  /-- 0 until allocated. -/
  oid : Nat
  flag : Bool
  durable : Bool
  /-- The writer stored the oid in the write in progress. -/
  stored : Bool

inductive Step : State → State → Prop
  | allocate (s : State) (n : Nat) (hn : n ≠ 0) (h0 : s.oid = 0) : Step s { s with oid := n, flag := true }
  | persist (s : State) : Step s { s with stored := s.stored || s.flag }
  | commit (s : State) :
      Step s { s with durable := s.durable || s.stored, flag := if s.stored then false else s.flag, stored := false }
  | rollback (s : State) : Step s { s with stored := false }

inductive Reachable : State → Prop
  | init : Reachable ⟨0, false, false, false⟩
  | step {s s'} : Reachable s → Step s s' → Reachable s'

structure Inv (s : State) : Prop where
  kept : s.oid ≠ 0 → s.durable = true ∨ s.flag = true
  flagged : s.flag = true → s.oid ≠ 0
  storedOid : s.stored = true → s.oid ≠ 0

theorem reachable_inv {s : State} (h : Reachable s) : Inv s := by
  induction h with
  | init => exact ⟨fun h => absurd rfl h, fun h => by simp at h, fun h => by simp at h⟩
  | @step s s' _ hs ih =>
    obtain ⟨hk, hf, hst⟩ := ih
    cases hs with
    | allocate n hn h0 => exact ⟨fun _ => Or.inr rfl, fun _ => hn, fun h => absurd h0 (hst h)⟩
    | persist =>
      refine ⟨hk, hf, fun h => ?_⟩
      simp only [Bool.or_eq_true] at h
      cases h with
      | inl h => exact hst h
      | inr h => exact hf h
    | commit =>
      refine ⟨fun h => ?_, fun h => ?_, fun h => by simp at h⟩
      · simp only at h ⊢
        cases hst2 : s.stored
        · simpa using hk h
        · simp
      · simp only at h ⊢
        cases hst2 : s.stored
        · simp [hst2] at h
          exact hf h
        · simp [hst2] at h
    | rollback => exact ⟨hk, hf, fun h => by simp at h⟩

/-- The commit before the fix: the flag cleared for a table whose info the write stored. -/
def oldCommit (s : State) : State := { s with durable := s.durable || s.stored, flag := false, stored := false }

theorem old_loses_oid :
    let s₀ : State := ⟨0, false, false, false⟩
    -- The writer stores the table info while no oid is allocated, then a reader allocates one, then the writer commits.
    let s₁ := { s₀ with stored := s₀.stored || s₀.flag }
    let s₂ := { s₁ with oid := 1, flag := true }
    let s₃ := oldCommit s₂
    s₃.oid ≠ 0 ∧ s₃.durable = false ∧ s₃.flag = false := by
  decide

end Btdb.SingletonOid
