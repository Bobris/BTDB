/-!
# A failed refresh never publishes a mixed state

A model of reader refresh rounds (`CatchUpAsync`). The writer publishes manifests `1, 2, …`; manifest `m` holds every
generation up to `m` in tables. A round takes the newest manifest it finds, at least its published one, and advances
the replay past the generations that manifest flushed (`AdvanceFlushed`). It then reads WAL chunks, which may fail.
The published state is consistent when its tables come from the manifest whose flushed generations its replay dropped.
A round may find no newer manifest even when one exists: the probed name may be deleted and listing throttled.

`Rule.current` is the round before the 2026-10-28 fix: a failure after advancing leaves the replay advanced.
`Rule.fixed` marks the replay stale then, and the next round reloads from storage.

* `current_mixed_state`: after a failed round, the next round publishes older tables with a replay that dropped newer
  generations.
* `fixed_consistent`: every published state is consistent.
-/

namespace Lsm.Refresh

inductive Rule where
  | current
  | fixed
deriving DecidableEq

structure State where
  newest : Nat
  /-- The manifest of the published tables, and the generations the published replay dropped. -/
  tables : Nat
  dropped : Nat
  /-- The replay's dropped generations, possibly ahead of the published state. -/
  replay : Nat
  stale : Bool

def init : State := ⟨0, 0, 0, 0, false⟩

inductive Step (r : Rule) : State → State → Prop
  | write (σ : State) : Step r σ { σ with newest := σ.newest + 1 }
  /-- A round that found manifest `m` (perhaps not the newest) and published. -/
  | round (σ : State) (m : Nat) (hm : σ.tables ≤ m ∧ m ≤ σ.newest) (hs : σ.stale = false) :
      Step r σ { σ with tables := m, dropped := max σ.replay m, replay := max σ.replay m }
  /-- A round that advanced to manifest `m` and then failed. -/
  | fail (σ : State) (m : Nat) (hm : σ.tables < m ∧ m ≤ σ.newest) (hs : σ.stale = false) :
      Step r σ { σ with replay := max σ.replay m, stale := r = .fixed }
  | reload (σ : State) (hs : σ.stale = true) :
      Step r σ { σ with tables := σ.newest, dropped := σ.newest, replay := σ.newest, stale := false }

inductive Reach (r : Rule) : State → Prop
  | init : Reach r init
  | step {σ τ : State} : Reach r σ → Step r σ τ → Reach r τ

/-- Manifest 1 appears; a round advances to it and fails; the next round finds no newer manifest than 0. -/
theorem current_mixed_state : ∃ σ, Reach .current σ ∧ σ.tables ≠ σ.dropped := by
  have r1 := Reach.step Reach.init (Step.write (r := .current) init)
  have r2 := Reach.step r1 (Step.fail _ 1 (by decide) rfl)
  have r3 := Reach.step r2 (Step.round _ 0 (by decide) rfl)
  exact ⟨_, r3, by decide⟩

structure Inv (σ : State) : Prop where
  consistent : σ.tables = σ.dropped
  replay : σ.stale = false → σ.replay = σ.tables
  bound : σ.tables ≤ σ.newest

theorem inv_step {σ τ : State} (hI : Inv σ) (h : Step .fixed σ τ) : Inv τ := by
  cases h with
  | write => exact ⟨hI.consistent, hI.replay, by have := hI.bound; dsimp only; omega⟩
  | round m hm hs =>
    have := hI.replay hs
    exact ⟨by dsimp only; omega, fun _ => by dsimp only; omega, by dsimp only; omega⟩
  | fail m hm hs => exact ⟨hI.consistent, fun h => by simp at h, hI.bound⟩
  | reload hs => exact ⟨rfl, fun _ => rfl, Nat.le_refl _⟩

theorem fixed_consistent {σ : State} (h : Reach .fixed σ) : σ.tables = σ.dropped := by
  have : Inv σ := by
    induction h with
    | init => exact ⟨rfl, fun _ => rfl, Nat.le_refl _⟩
    | step _ hs ih => exact inv_step ih hs
  exact this.consistent

end Lsm.Refresh
