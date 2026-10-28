/-!
# Losing the writer role reloads a state without lost commits

A model of one writer instance (`CommitWritingTransaction`, `LoseWriterAsync`). Commits publish into the local state;
uploads make the local state durable while the instance is the writer. Losing the role clears the writer flag and then
reloads the local state from storage. Every step is atomic and may happen at any time.

`Rule.current` is the order before the 2026-10-28 fix: a commit checks the writer flag in one step and publishes in a
later one. `Rule.fixed` checks and publishes in one step, under the state lock that clearing the flag also takes.

* `current_lost_commit_visible`: a commit that passed the check publishes after the reload, so the instance shows a
  commit storage does not have.
* `fixed_reload_matches`: after the reload, the local state is the durable state.
-/

namespace Lsm.Reload

inductive Rule where
  | current
  | fixed
deriving DecidableEq

structure State where
  writer : Bool
  /-- A commit that passed the check and has not published yet (current rule only). -/
  checked : Option Nat
  /-- Losing the role: 0 not started, 1 flag cleared, 2 reloaded. -/
  lost : Nat
  /-- The state local transactions see. -/
  shown : List Nat
  storage : List Nat

def init : State := ⟨true, none, 0, [], []⟩

inductive Step (r : Rule) : State → State → Prop
  | check (σ : State) (c : Nat) (hr : r = .current) (hw : σ.writer = true) (hc : σ.checked = none) :
      Step r σ { σ with checked := some c }
  | publish (σ : State) (c : Nat) (hc : σ.checked = some c) :
      Step r σ { σ with checked := none, shown := σ.shown ++ [c] }
  /-- Check and publish under the state lock. -/
  | commit (σ : State) (c : Nat) (hr : r = .fixed) (hw : σ.writer = true) :
      Step r σ { σ with shown := σ.shown ++ [c] }
  | upload (σ : State) (hw : σ.writer = true) : Step r σ { σ with storage := σ.shown }
  | lose (σ : State) (hw : σ.writer = true) : Step r σ { σ with writer := false, lost := 1 }
  | reload (σ : State) (hl : σ.lost = 1) : Step r σ { σ with lost := 2, shown := σ.storage }

inductive Reach (r : Rule) : State → Prop
  | init : Reach r init
  | step {σ τ : State} : Reach r σ → Step r σ τ → Reach r τ

/-- A commit passes the check, the role is lost and reloaded, and the commit then publishes. -/
theorem current_lost_commit_visible : ∃ σ, Reach .current σ ∧ σ.lost = 2 ∧ σ.shown ≠ σ.storage := by
  have r1 := Reach.step Reach.init (Step.check (r := .current) init 7 rfl rfl rfl)
  have r2 := Reach.step r1 (Step.lose _ rfl)
  have r3 := Reach.step r2 (Step.reload _ rfl)
  have r4 := Reach.step r3 (Step.publish _ 7 rfl)
  exact ⟨_, r4, rfl, by decide⟩

structure Inv (σ : State) : Prop where
  no_checked : σ.checked = none
  lost_writer : σ.lost ≠ 0 → σ.writer = false
  reloaded : σ.lost = 2 → σ.shown = σ.storage

theorem inv_step {σ τ : State} (hI : Inv σ) (h : Step .fixed σ τ) : Inv τ := by
  cases h with
  | check _ hr => cases hr
  | publish c hc => rw [hI.no_checked] at hc; cases hc
  | commit c _ hw =>
    exact ⟨hI.no_checked, fun hl => by have := hI.lost_writer hl; simp_all,
      fun hl => by have := hI.lost_writer (by dsimp only at hl; omega); simp_all⟩
  | upload hw =>
    exact ⟨hI.no_checked, hI.lost_writer, fun hl => by have := hI.lost_writer (by dsimp only at hl; omega); simp_all⟩
  | lose hw => exact ⟨hI.no_checked, fun _ => rfl, fun hl => by dsimp only at hl; omega⟩
  | reload hl => exact ⟨hI.no_checked, fun _ => hI.lost_writer (by omega), fun _ => rfl⟩

theorem reach_inv {σ : State} (h : Reach .fixed σ) : Inv σ := by
  induction h with
  | init => exact ⟨rfl, fun h => absurd rfl h, fun h => by simp [init] at h⟩
  | step _ hs ih => exact inv_step ih hs

theorem fixed_reload_matches {σ : State} (h : Reach .fixed σ) (hl : σ.lost = 2) : σ.shown = σ.storage :=
  (reach_inv h).reloaded hl

end Lsm.Reload
