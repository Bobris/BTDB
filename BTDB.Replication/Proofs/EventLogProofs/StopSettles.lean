/-!
# A stopped owner settles every accepted record

`EventLogOwnerLane` keeps accepted records in its queue until a write takes them as a batch, which is then in neither
the queue nor a session. `Stop` fails the queued records. Before the 2026-10-28 fix a batch whose write failed after an
external `Stop` (a takeover, a disposal) was dropped: `Stop` with the batch found the lane already stopped and
returned, so its publications never completed.

* `fixed_settles`: once the lane is stopped and no write is in flight, every accepted record has its outcome.
* `previous_hangs`: before the fix a record stays unsettled after the lane stopped and its write ended.
-/

namespace EventLog.StopSettles

inductive Phase
  | none
  | queued
  | inflight
  | settled
  deriving DecidableEq

structure St where
  stopped : Bool
  writing : Bool
  phase : Nat → Phase

def move (p : Nat → Phase) (a b : Phase) : Nat → Phase := fun m => if p m = a then b else p m

/-- `fixed` selects the current `Stop`, which settles the failed batch even when the lane already stopped. -/
inductive Step (fixed : Bool) : St → St → Prop
  | submit (s : St) (r : Nat) (h : s.stopped = false) (hr : s.phase r = .none) :
      Step fixed s { s with phase := fun m => if m = r then .queued else s.phase m }
  | take (s : St) (h : s.stopped = false) (hw : s.writing = false) :
      Step fixed s { s with writing := true, phase := move s.phase .queued .inflight }
  | ok (s : St) (hw : s.writing = true) :
      Step fixed s { s with writing := false, phase := move s.phase .inflight .settled }
  /-- A failed write while running stops the lane, failing the queue and the batch. -/
  | failRunning (s : St) (hw : s.writing = true) (h : s.stopped = false) :
      Step fixed s ⟨true, false, move (move s.phase .queued .settled) .inflight .settled⟩
  /-- A failed write after an external stop: only the fix still fails the batch. -/
  | failStopped (s : St) (hw : s.writing = true) (h : s.stopped = true) :
      Step fixed s { s with writing := false,
                            phase := if fixed then move s.phase .inflight .settled else s.phase }
  | stop (s : St) (h : s.stopped = false) :
      Step fixed s { s with stopped := true, phase := move s.phase .queued .settled }

inductive Reach (fixed : Bool) : St → Prop
  | init : Reach fixed ⟨false, false, fun _ => .none⟩
  | step {s t : St} : Reach fixed s → Step fixed s t → Reach fixed t

def Inv (s : St) : Prop :=
  (∀ r, s.phase r = .inflight → s.writing = true) ∧ (s.stopped = true → ∀ r, s.phase r ≠ .queued)

theorem inv {s : St} (h : Reach true s) : Inv s := by
  induction h with
  | init => exact ⟨fun r hr => by simp at hr, fun h => by simp at h⟩
  | step _ hs ih =>
    obtain ⟨hi, hq⟩ := ih
    cases hs with
    | submit r h hr =>
      refine ⟨fun m hm => hi m ?_, fun h' => by simp_all⟩
      simp only at hm; split at hm
      · cases hm
      · exact hm
    | take h hw => exact ⟨fun _ _ => rfl, fun h' => by simp_all⟩
    | ok hw =>
      refine ⟨fun m hm => ?_, fun h' m hm => ?_⟩
      · simp only [move] at hm; split at hm <;> simp_all
      · simp only [move] at hm; split at hm
        · cases hm
        · exact hq h' m hm
    | failRunning hw h =>
      refine ⟨fun m hm => ?_, fun _ m hm => ?_⟩
      · simp only [move] at hm; split at hm
        · cases hm
        · split at hm <;> simp_all
      · simp only [move] at hm; split at hm
        · cases hm
        · split at hm <;> simp_all
    | failStopped hw h =>
      refine ⟨fun m hm => ?_, fun h' m hm => ?_⟩
      · simp only [move, ite_true] at hm; split at hm <;> simp_all
      · simp only [move, ite_true] at hm; split at hm
        · cases hm
        · exact hq h' m hm
    | stop h =>
      refine ⟨fun m hm => hi m ?_, fun _ m hm => ?_⟩
      · simp only [move] at hm; split at hm
        · cases hm
        · exact hm
      · simp only [move] at hm; split at hm
        · cases hm
        · simp_all

/-- With the fix, a stopped lane without a write in flight has no accepted record left without its outcome. -/
theorem fixed_settles {s : St} (h : Reach true s) (hs : s.stopped = true) (hw : s.writing = false) (r : Nat) :
    s.phase r = .none ∨ s.phase r = .settled := by
  obtain ⟨hi, hq⟩ := inv h
  cases hr : s.phase r with
  | none => exact .inl rfl
  | queued => exact absurd hr (hq hs r)
  | inflight => have := hi r hr; simp_all
  | settled => exact .inr rfl

/-- Before the fix: accept a record, take it as a batch, stop externally, fail the write; the record hangs. -/
theorem previous_hangs : ∃ s, Reach false s ∧ s.stopped = true ∧ s.writing = false ∧ s.phase 0 = .inflight := by
  let s1 : St := ⟨false, false, fun m => if m = 0 then .queued else .none⟩
  let s2 : St := ⟨false, true, move s1.phase .queued .inflight⟩
  let s3 : St := ⟨true, true, move s2.phase .queued .settled⟩
  let s4 : St := ⟨true, false, s3.phase⟩
  have r1 : Reach false s1 := .step .init (.submit _ 0 rfl rfl)
  have r2 : Reach false s2 := .step r1 (.take _ rfl rfl)
  have r3 : Reach false s3 := .step r2 (.stop _ rfl)
  have r4 : Reach false s4 := .step r3 (.failStopped _ rfl rfl)
  exact ⟨s4, r4, rfl, rfl, by simp [s4, s3, s2, s1, move]⟩

end EventLog.StopSettles
