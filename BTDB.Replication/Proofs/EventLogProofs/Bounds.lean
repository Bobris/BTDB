import EventLogProofs.Common

/-!
# Fresh bounds from an owner

A model of `EventLogOwnerLane.ValidateAsync`, which `GetBoundsAsync` uses. The answer must cover every receipt
completed before the call started (section 3 of `EventLogImplementationPlan.md`). Every receipt lies below the
canonical next offset `h`, so the answer must be at least `h` at the call's start.

The topic is one versioned history: only its length `h` and version `v` matter (`Chain.lean` justifies this). A
lane's conditional write is dispatched, applies only on the current version, and its response arrives later. Lanes
take over at any time. A call answers from a successful write of its lane or from a tail check that finds the lane's
version current.

`Rule.current` trusts a successful write whose response arrived after the call started. `Rule.fixed` trusts only a
write dispatched after the call started.

* `current_bounds_stale`: lane 0's write applies, lane 1 takes over and commits, a call on lane 0 starts, and lane
  0's late response makes it answer an end below a completed receipt.
* `fixed_bounds_fresh`: every answer is at least the canonical next offset at the call's start.
-/

namespace EventLog.Bounds

/-- A dispatched write: its dispatch number, expected version and record count, and after it applied, the version
and next offset it produced. -/
structure Pending where
  id : Nat
  ver : Nat
  n : Nat
  applied : Option (Nat × Nat)

/-- A lane: the version and next offset it holds, the writes dispatched so far, the dispatch number of its last
successful write, and its write in flight. -/
structure Lane where
  ver : Nat
  next : Nat
  dispatched : Nat
  okId : Nat
  pending : Option Pending

/-- A `ValidateAsync` call: its lane, the canonical next offset when it started, and the lane's dispatch count and
last successful write then. -/
structure Call where
  lane : Nat
  start : Nat
  d0 : Nat
  ok0 : Nat

structure State where
  h : Nat
  v : Nat
  lanes : Nat → Option Lane
  calls : List Call
  answers : List (Call × Nat)

def init : State := ⟨0, 0, fun _ => none, [], []⟩

inductive Rule where
  | current
  | fixed
deriving DecidableEq

/-- When a call may answer from its lane's last successful write. -/
def Trusted : Rule → Call → Lane → Prop
  | .current, c, L => c.ok0 < L.okId
  | .fixed, c, L => c.d0 < L.okId

inductive Step (r : Rule) : State → State → Prop
  | newLane (σ : State) (i : Nat) (hi : σ.lanes i = none) :
      Step r σ { σ with lanes := upd σ.lanes i (some ⟨σ.v, σ.h, 0, 0, none⟩) }
  | dispatch (σ : State) (i n : Nat) (L : Lane) (hL : σ.lanes i = some L) (hp : L.pending = none) :
      Step r σ { σ with
        lanes := upd σ.lanes i (some
          { L with dispatched := L.dispatched + 1, pending := some ⟨L.dispatched + 1, L.ver, n, none⟩ }) }
  | apply (σ : State) (i : Nat) (L : Lane) (p : Pending) (hL : σ.lanes i = some L) (hp : L.pending = some p)
      (hn : p.applied = none) (hv : p.ver = σ.v) :
      Step r σ { σ with
        h := σ.h + p.n
        v := σ.v + 1
        lanes := upd σ.lanes i (some { L with pending := some { p with applied := some (σ.v + 1, σ.h + p.n) } }) }
  | respond (σ : State) (i : Nat) (L : Lane) (p : Pending) (ver next : Nat) (hL : σ.lanes i = some L)
      (hp : L.pending = some p) (ha : p.applied = some (ver, next)) :
      Step r σ { σ with
        lanes := upd σ.lanes i (some { L with ver := ver, next := next, okId := p.id, pending := none }) }
  | begin (σ : State) (i : Nat) (L : Lane) (hL : σ.lanes i = some L) :
      Step r σ { σ with calls := σ.calls ++ [⟨i, σ.h, L.dispatched, L.okId⟩] }
  | answerWrite (σ : State) (c : Call) (L : Lane) (hc : c ∈ σ.calls) (hL : σ.lanes c.lane = some L)
      (ht : Trusted r c L) :
      Step r σ { σ with answers := σ.answers ++ [(c, L.next)] }
  | answerCheck (σ : State) (c : Call) (L : Lane) (hc : c ∈ σ.calls) (hL : σ.lanes c.lane = some L)
      (hv : L.ver = σ.v) :
      Step r σ { σ with answers := σ.answers ++ [(c, L.next)] }

inductive Reach (r : Rule) : State → Prop
  | init : Reach r init
  | step {σ τ : State} : Reach r σ → Step r σ τ → Reach r τ

theorem current_bounds_stale : ∃ σ, Reach .current σ ∧ ∃ a ∈ σ.answers, a.2 < a.1.start := by
  have r1 := Reach.step Reach.init (Step.newLane (r := .current) init 0 rfl)
  have r2 := Reach.step r1 (Step.dispatch _ 0 1 _ rfl rfl)
  have r3 := Reach.step r2 (Step.apply _ 0 _ _ rfl rfl rfl rfl)
  have r4 := Reach.step r3 (Step.newLane _ 1 rfl)
  have r5 := Reach.step r4 (Step.dispatch _ 1 1 _ rfl rfl)
  have r6 := Reach.step r5 (Step.apply _ 1 _ _ rfl rfl rfl rfl)
  have r7 := Reach.step r6 (Step.respond _ 1 _ _ 2 2 rfl rfl rfl)
  have r8 := Reach.step r7 (Step.begin _ 0 _ rfl)
  have r9 := Reach.step r8 (Step.respond _ 0 _ _ 1 1 rfl rfl rfl)
  have r10 := Reach.step r9 (Step.answerWrite _ ⟨0, 2, 1, 0⟩ _ (by simp [init]) rfl (by simp [Trusted]))
  exact ⟨_, r10, (⟨0, 2, 1, 0⟩, 1), by simp, by decide⟩

/-! ## The fixed rule -/

structure LaneOk (h v : Nat) (L : Lane) : Prop where
  ver_le : L.ver ≤ v
  current : L.ver = v → L.next = h
  ok_le : L.okId ≤ L.dispatched
  pending_id : ∀ p, L.pending = some p → p.id = L.dispatched
  applied : ∀ p ver next, L.pending = some p → p.applied = some (ver, next) →
    ver ≤ v ∧ (ver = v → next = h) ∧ next ≤ h
  next_le : L.next ≤ h

/-- A call's lane exists, and every write the lane dispatched after the call started lands at or above its start. -/
structure CallOk (h : Nat) (lanes : Nat → Option Lane) (c : Call) : Prop where
  present : lanes c.lane ≠ none
  start_le : c.start ≤ h
  lane : ∀ L, lanes c.lane = some L → c.d0 ≤ L.dispatched ∧
    (c.d0 < L.okId → c.start ≤ L.next) ∧
    (∀ p ver next, L.pending = some p → c.d0 < p.id → p.applied = some (ver, next) → c.start ≤ next)

structure Inv (σ : State) : Prop where
  lanes : ∀ i L, σ.lanes i = some L → LaneOk σ.h σ.v L
  calls : ∀ c ∈ σ.calls, CallOk σ.h σ.lanes c
  answers : ∀ a ∈ σ.answers, a.1.start ≤ a.2

theorem upd_present {f : Nat → Option Lane} {i j : Nat} {x : Lane} (h : f j ≠ none) : upd f i (some x) j ≠ none := by
  by_cases hj : j = i <;> simp [upd, hj, h]

theorem inv_init : Inv init where
  lanes _ _ h := by simp [init] at h
  calls _ h := by simp [init] at h
  answers _ h := by simp [init] at h

theorem inv_newLane {σ : State} (hI : Inv σ) (i : Nat) (hi : σ.lanes i = none) :
    Inv { σ with lanes := upd σ.lanes i (some ⟨σ.v, σ.h, 0, 0, none⟩) } where
  lanes j L hL := by
    rcases upd_some hL with ⟨_, rfl⟩ | ⟨_, hL⟩
    · exact ⟨Nat.le_refl _, fun _ => rfl, Nat.le_refl _, fun p hp => by simp at hp,
        fun p _ _ hp => by simp at hp, Nat.le_refl _⟩
    · exact hI.lanes j L hL
  calls c hc := by
    have hk := hI.calls c hc
    have hne : c.lane ≠ i := fun he => hk.present (he ▸ hi)
    exact ⟨by dsimp only; rw [upd_ne _ _ hne]; exact hk.present, hk.start_le,
      fun L hL => hk.lane L (by dsimp only at hL; rwa [upd_ne _ _ hne] at hL)⟩
  answers := hI.answers

theorem inv_dispatch {σ : State} (hI : Inv σ) (i n : Nat) (L : Lane) (hL : σ.lanes i = some L) :
    Inv { σ with
      lanes := upd σ.lanes i (some
        { L with dispatched := L.dispatched + 1, pending := some ⟨L.dispatched + 1, L.ver, n, none⟩ }) } := by
  have hok := hI.lanes i L hL
  exact {
    lanes := fun j L' hL' => by
      rcases upd_some hL' with ⟨_, rfl⟩ | ⟨_, hL'⟩
      · refine ⟨hok.ver_le, hok.current, by have := hok.ok_le; dsimp only; omega,
          fun q hq => by simp at hq; rw [← hq], fun q _ _ hq ha => ?_, hok.next_le⟩
        simp at hq; rw [← hq] at ha; simp at ha
      · exact hI.lanes j L' hL'
    calls := fun c hc => by
      have hk := hI.calls c hc
      refine ⟨upd_present hk.present, hk.start_le, fun L' hL' => ?_⟩
      rcases upd_some hL' with ⟨he, rfl⟩ | ⟨_, hL'⟩
      · obtain ⟨h1, h2, _⟩ := hk.lane L (by rw [he]; exact hL)
        refine ⟨by dsimp only; omega, h2, fun q _ _ hq _ ha => ?_⟩
        simp at hq; rw [← hq] at ha; simp at ha
      · exact hk.lane L' hL'
    answers := hI.answers }

theorem inv_apply {σ : State} (hI : Inv σ) (i : Nat) (L : Lane) (p : Pending) (hL : σ.lanes i = some L)
    (hp : L.pending = some p) :
    Inv { σ with
      h := σ.h + p.n
      v := σ.v + 1
      lanes := upd σ.lanes i (some { L with pending := some { p with applied := some (σ.v + 1, σ.h + p.n) } }) } := by
  have hok := hI.lanes i L hL
  exact {
    lanes := fun j L' hL' => by
      rcases upd_some hL' with ⟨_, rfl⟩ | ⟨_, hL'⟩
      · refine ⟨by have := hok.ver_le; dsimp only; omega, fun h => by have := hok.ver_le; dsimp only at h; omega,
          hok.ok_le, fun q hq => by simp at hq; rw [← hq]; exact hok.pending_id p hp,
          fun q ver next hq ha => ?_, by have := hok.next_le; dsimp only; omega⟩
        simp at hq; rw [← hq] at ha; simp at ha; obtain ⟨rfl, rfl⟩ := ha
        exact ⟨Nat.le_refl _, fun _ => rfl, Nat.le_refl _⟩
      · have h2 := hI.lanes j L' hL'
        refine ⟨by have := h2.ver_le; dsimp only; omega, fun h => by have := h2.ver_le; dsimp only at h; omega,
          h2.ok_le, h2.pending_id, fun q ver next hq ha => ?_, by have := h2.next_le; dsimp only; omega⟩
        obtain ⟨a1, _, a3⟩ := h2.applied q ver next hq ha
        exact ⟨by dsimp only; omega, fun h => by dsimp only at h; omega, by dsimp only; omega⟩
    calls := fun c hc => by
      have hk := hI.calls c hc
      refine ⟨upd_present hk.present, by have := hk.start_le; dsimp only; omega, fun L' hL' => ?_⟩
      rcases upd_some hL' with ⟨he, rfl⟩ | ⟨_, hL'⟩
      · obtain ⟨h1, h2, _⟩ := hk.lane L (by rw [he]; exact hL)
        refine ⟨h1, h2, fun q ver next hq _ ha => ?_⟩
        simp at hq; rw [← hq] at ha; simp at ha; obtain ⟨_, rfl⟩ := ha
        have := hk.start_le; omega
      · exact hk.lane L' hL'
    answers := hI.answers }

theorem inv_respond {σ : State} (hI : Inv σ) (i : Nat) (L : Lane) (p : Pending) (ver next : Nat)
    (hL : σ.lanes i = some L) (hp : L.pending = some p) (ha : p.applied = some (ver, next)) :
    Inv { σ with
      lanes := upd σ.lanes i (some { L with ver := ver, next := next, okId := p.id, pending := none }) } := by
  have hok := hI.lanes i L hL
  obtain ⟨a1, a2, a3⟩ := hok.applied p ver next hp ha
  exact {
    lanes := fun j L' hL' => by
      rcases upd_some hL' with ⟨_, rfl⟩ | ⟨_, hL'⟩
      · exact ⟨a1, a2, Nat.le_of_eq (hok.pending_id p hp), fun q hq => by simp at hq,
          fun q _ _ hq => by simp at hq, a3⟩
      · exact hI.lanes j L' hL'
    calls := fun c hc => by
      have hk := hI.calls c hc
      refine ⟨upd_present hk.present, hk.start_le, fun L' hL' => ?_⟩
      rcases upd_some hL' with ⟨he, rfl⟩ | ⟨_, hL'⟩
      · obtain ⟨h1, _, h3⟩ := hk.lane L (by rw [he]; exact hL)
        exact ⟨h1, fun hlt => h3 p ver next hp hlt ha, fun q _ _ hq => by simp at hq⟩
      · exact hk.lane L' hL'
    answers := hI.answers }

theorem inv_begin {σ : State} (hI : Inv σ) (i : Nat) (L : Lane) (hL : σ.lanes i = some L) :
    Inv { σ with calls := σ.calls ++ [⟨i, σ.h, L.dispatched, L.okId⟩] } := by
  have hok := hI.lanes i L hL
  exact {
    lanes := hI.lanes
    calls := fun c hc => by
      rcases List.mem_append.1 hc with hc | hc
      · exact hI.calls c hc
      · simp at hc; subst hc
        refine ⟨by simp [hL], Nat.le_refl _, fun L' hL' => ?_⟩
        simp only at hL'; rw [hL] at hL'; cases hL'
        refine ⟨Nat.le_refl _, fun h => absurd h (by have := hok.ok_le; arith), fun q _ _ hq hlt _ => ?_⟩
        rw [hok.pending_id q hq] at hlt; arith
    answers := hI.answers }

theorem inv_step {σ τ : State} (hI : Inv σ) (h : Step .fixed σ τ) : Inv τ := by
  cases h with
  | newLane i hi => exact inv_newLane hI i hi
  | dispatch i n L hL _ => exact inv_dispatch hI i n L hL
  | apply i L p hL hp _ _ => exact inv_apply hI i L p hL hp
  | respond i L p ver next hL hp ha => exact inv_respond hI i L p ver next hL hp ha
  | begin i L hL => exact inv_begin hI i L hL
  | answerWrite c L hc hL ht =>
    exact {
      lanes := hI.lanes
      calls := hI.calls
      answers := fun a ha => by
        rcases List.mem_append.1 ha with ha | ha
        · exact hI.answers a ha
        · simp at ha; subst ha; exact ((hI.calls c hc).lane L hL).2.1 ht }
  | answerCheck c L hc hL hv =>
    exact {
      lanes := hI.lanes
      calls := hI.calls
      answers := fun a ha => by
        rcases List.mem_append.1 ha with ha | ha
        · exact hI.answers a ha
        · simp at ha; subst ha
          rw [(hI.lanes c.lane L hL).current hv]; exact (hI.calls c hc).start_le }

theorem fixed_inv {σ : State} (h : Reach .fixed σ) : Inv σ := by
  induction h with
  | init => exact inv_init
  | step _ hs ih => exact inv_step ih hs

/-- Every answer covers every receipt completed before its call started. -/
theorem fixed_bounds_fresh {σ : State} (h : Reach .fixed σ) : ∀ a ∈ σ.answers, a.1.start ≤ a.2 :=
  (fixed_inv h).answers

end EventLog.Bounds
