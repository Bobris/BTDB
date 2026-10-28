/-!
# A disposed topic holds no owner lane

`EventLogService.DisposeAsync` cancels its disposal token and then disposes each topic, which detaches and stops the
topic's lane under the topic lock. A takeover running meanwhile creates a candidate lane and attaches it. Before the
2026-10-28 fix the attach could come after the detach, so the disposed topic kept a lane nothing stopped; now the
attach checks the token under the same lock and stops the candidate instead.

* `fixed_detached`: once disposal detached the lane, the topic holds no lane, whatever attaches come later.
* `previous_keeps_lane`: before the fix an attach after disposal leaves the disposed topic with a lane.
-/

namespace EventLog.DisposeAttach

structure St where
  cancelled : Bool
  detached : Bool
  lane : Option Nat

/-- `fixed` selects the current attach, which refuses once the disposal token is cancelled. -/
inductive Step (fixed : Bool) : St → St → Prop
  | cancel (s : St) : Step fixed s { s with cancelled := true }
  /-- Disposing the topic: after the cancel, under the topic lock, it stops and drops the lane it finds. -/
  | detach (s : St) (h : s.cancelled = true) : Step fixed s { s with detached := true, lane := none }
  | attach (s : St) (l : Nat) :
      Step fixed s { s with lane := if fixed && s.cancelled then s.lane else some l }

inductive Reach (fixed : Bool) : St → Prop
  | init : Reach fixed ⟨false, false, none⟩
  | step {s t : St} : Reach fixed s → Step fixed s t → Reach fixed t

theorem inv {s : St} (h : Reach true s) : s.detached = true → s.cancelled = true ∧ s.lane = none := by
  induction h with
  | init => intro h; cases h
  | step _ hs ih =>
    cases hs with
    | cancel => intro hd; exact ⟨rfl, (ih hd).2⟩
    | detach hc => intro _; exact ⟨hc, rfl⟩
    | attach l =>
      intro hd
      obtain ⟨hc, hl⟩ := ih hd
      exact ⟨hc, by simp [hc, hl]⟩

/-- With the fix, a topic whose disposal detached its lane holds none afterwards. -/
theorem fixed_detached {s : St} (h : Reach true s) (hd : s.detached = true) : s.lane = none := (inv h hd).2

/-- Before the fix: disposal cancels and detaches, then a takeover that was already running attaches its lane. -/
theorem previous_keeps_lane : ∃ s, Reach false s ∧ s.detached = true ∧ s.lane = some 0 := by
  have r1 : Reach false ⟨true, false, none⟩ := .step .init (.cancel _)
  have r2 : Reach false ⟨true, true, none⟩ := .step r1 (.detach _ rfl)
  have r3 : Reach false ⟨true, true, some 0⟩ := .step r2 (.attach _ 0)
  exact ⟨_, r3, rfl, rfl⟩

end EventLog.DisposeAttach
