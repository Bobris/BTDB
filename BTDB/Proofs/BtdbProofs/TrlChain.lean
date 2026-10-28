/-!
# Linking the transaction log chain terminates

A model of `BTreeKeyValueDB.LinkTransactionLogFileIds`, which walks from the newest transaction log back over the
predecessor IDs in the file headers (0 ends the chain).

* `walk_descending`: with the rule since the 2026-10-28 fix, which refuses a predecessor ID that is not below the
  file's own, the walk is a total function and lists strictly descending IDs, so it visits no file twice.
* `previous_never_ends`: before the fix, one damaged predecessor ID, here the first log pointing back at the fourth,
  closed the chain into a cycle that the walk followed forever, however many steps it was given
  (`TransactionLogDamageTest.PredecessorLinkCycleFailsTheOpen`).
-/

namespace Btdb.TrlChain

/-- The walk since the fix; `none` is the thrown `BTDBException`. -/
def walk (prev : Nat → Nat) (id : Nat) : Option (List Nat) :=
  if id = 0 then some []
  else if _h : prev id < id then (walk prev (prev id)).map (id :: ·)
  else none
termination_by id
decreasing_by omega

theorem walk_descending (prev : Nat → Nat) : ∀ (id : Nat) (ids : List Nat), walk prev id = some ids →
    ids.Pairwise (· > ·) ∧ ∀ x ∈ ids, x ≤ id := by
  intro id
  induction id using Nat.strongRecOn with
  | ind id ih =>
    intro ids h
    rw [walk] at h
    by_cases h0 : id = 0
    · simp only [h0, ite_true, Option.some.injEq] at h
      subst h; simp
    · simp only [h0, ite_false] at h
      by_cases hp : prev id < id
      · simp only [hp, dite_true] at h
        cases hw : walk prev (prev id) with
        | none => simp [hw] at h
        | some rest =>
          simp only [hw, Option.map_some, Option.some.injEq] at h
          subst h
          have ⟨hd, hb⟩ := ih (prev id) hp rest hw
          refine ⟨List.pairwise_cons.mpr ⟨fun x hx => by have := hb x hx; omega, hd⟩, ?_⟩
          intro x hx
          simp only [List.mem_cons] at hx
          cases hx with
          | inl e => omega
          | inr m => have := hb x m; omega
      · simp [hp] at h

/-- The walk before the fix, given `fuel` steps; `none` means it had not ended. -/
def previousWalk (prev : Nat → Nat) : Nat → Nat → Option (List Nat)
  | 0, _ => none
  | fuel + 1, id => if id = 0 then some [] else (previousWalk prev fuel (prev id)).map (id :: ·)

/-- Four logs, the first one's predecessor damaged from 0 to 4. -/
def damaged (id : Nat) : Nat := if id = 1 then 4 else id - 1

theorem previous_never_ends : ∀ fuel id, 1 ≤ id → id ≤ 4 → previousWalk damaged fuel id = none
  | 0, _, _, _ => rfl
  | fuel + 1, id, h1, h4 => by
    have hn : 1 ≤ damaged id ∧ damaged id ≤ 4 := by unfold damaged; split <;> omega
    simp only [previousWalk, Nat.ne_of_gt h1, ite_false]
    rw [previous_never_ends fuel (damaged id) hn.1 hn.2]
    rfl

theorem fixed_refuses_cycle : walk damaged 4 = none := by decide +kernel

end Btdb.TrlChain
