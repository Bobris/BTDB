/-!
# A stopped compaction batch drops no due database

A model of how `CompactorScheduler.OnTimer` (`../../KVDBLayer/Implementation/CompactorScheduler.cs`) ends a batch of
due compactions early. It takes the due databases, clearing their due times, and compacts them in order; one reporting
more work goes to `neededAgain`. Removing any database cancels the batch, and a compaction can fail. The batch stops
before the database at `next`, having compacted those before it.

`stop` reschedules `neededAgain` and every database from `next` on (`StopRunningUnsafe`); before the 2026-10-29 fix it
rescheduled none (`oldStop`), so they were not compacted until their transaction log moved to a new file
(`KeyValueDBCompactorSchedulerTest.RemovingOtherDbKeepsInterruptedCompactionsScheduled`).

* `stop_keeps_due`: after `stop` every database of the batch is scheduled again or was compacted with no work left.
* `old_drops`: `oldStop` leaves an interrupted database neither.
-/

namespace Btdb.CompactionBatch

/-- The databases scheduled again when the batch stops before `next`. -/
def stop (due : List Nat) (next : Nat) (neededAgain : List Nat) : List Nat := neededAgain ++ due.drop next

def oldStop (_due : List Nat) (_next : Nat) (_neededAgain : List Nat) : List Nat := []

/-- Compacted with no work left: before `next` and not reporting more. -/
def done (due : List Nat) (next : Nat) (neededAgain : List Nat) (d : Nat) : Prop :=
  d ∈ due.take next ∧ d ∉ neededAgain

theorem stop_keeps_due (due : List Nat) (next : Nat) (neededAgain : List Nat) (d : Nat) (hd : d ∈ due) :
    d ∈ stop due next neededAgain ∨ done due next neededAgain d := by
  rw [← List.take_append_drop next due, List.mem_append] at hd
  unfold stop done
  by_cases hn : d ∈ neededAgain
  · exact Or.inl (List.mem_append.2 (Or.inl hn))
  · cases hd with
    | inl h => exact Or.inr ⟨h, hn⟩
    | inr h => exact Or.inl (List.mem_append.2 (Or.inr h))

/-- The batch of databases 1 and 2 cancelled while compacting 1. -/
theorem old_drops : ¬ (1 ∈ oldStop [1, 2] 0 [] ∨ done [1, 2] 0 [] 1) := by
  simp [oldStop, done]

end Btdb.CompactionBatch
