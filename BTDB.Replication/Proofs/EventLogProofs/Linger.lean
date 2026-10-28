/-!
# One writer awaiting each commit never makes the owner linger

`CommitLingerEstimator` (also compiled into the BTDB.LSM WAL uploader) chooses how long a write waits for more
records. It bins arrivals by their delay after the moment a write would start without waiting: the commit when
records are already queued, otherwise the first record arriving after it, which only marks that moment. A window is
chosen, or kept by hysteresis, only while the arrivals within it beat the share of the cycle it costs by a margin:
`cumulative / total - window / write > MinimumGain`, here multiplied by `total * write`, both positive, so
`cumulative * write > window * total + margin` with a positive `margin`. Decay multiplies every bin by the same positive
factor, so it keeps a bin zero exactly when it was; the model keeps bins unchanged.

* `single_writer_never_lingers`: when every commit finds nothing queued and at most one record arrives before the next
  commit, as with one publisher awaiting each receipt, no bin ever counts an arrival and the window stays zero.
* `answering_publishers_fill_bins`: a second record after a commit, as publishers answering commits send, is binned.
-/

namespace EventLog.Linger

structure Params where
  bins : Nat
  bin : Nat
  write : Nat
  margin : Nat
  hmargin : 0 < margin

structure St where
  awaiting : Bool
  origin : Bool
  /-- Records arrived since the last commit; the single writer's constraint bounds it. -/
  sinceCommit : Nat
  arrivals : Nat → Nat
  total : Nat
  current : Nat

def cumulative (a : Nat → Nat) : Nat → Nat
  | 0 => a 0
  | k + 1 => cumulative a k + a (k + 1)

/-- The scaled gain test of `Recompute` for the window of bins `0 .. k`. -/
def Profitable (p : Params) (s : St) (k : Nat) : Prop :=
  cumulative s.arrivals k * p.write > (k + 1) * p.bin * s.total + p.margin

/-- What `Recompute` may set: no window, a profitable one, or the current one while it stays profitable. -/
def Recomputed (p : Params) (s : St) (r : Nat) : Prop :=
  r = 0 ∨ (∃ k, k < p.bins ∧ Profitable p s k ∧ r = (k + 1) * p.bin) ∨
    (r = s.current ∧ ∃ k, s.current = (k + 1) * p.bin ∧ Profitable p s k)

inductive Step (p : Params) : St → St → Prop
  /-- `Arrival` while awaiting the first record after a commit: it marks the origin and is not binned. -/
  | first (s : St) (h : s.awaiting = true) :
      Step p s { s with awaiting := false, origin := true, sinceCommit := s.sinceCommit + 1, total := s.total + 1 }
  /-- `Arrival` after the origin: binned at its delay `k`. -/
  | binned (s : St) (k : Nat) (h : s.awaiting = false) (ho : s.origin = true) :
      Step p s { s with sinceCommit := s.sinceCommit + 1, total := s.total + 1,
                        arrivals := fun i => if i = k then s.arrivals i + 1 else s.arrivals i }
  /-- `Arrival` before any origin: counted, not binned. -/
  | early (s : St) (h : s.awaiting = false) (ho : s.origin = false) :
      Step p s { s with sinceCommit := s.sinceCommit + 1, total := s.total + 1 }
  /-- `Committed`: the origin is the commit when records are queued, otherwise the next arrival. -/
  | commit (s : St) (queued : Bool) (r : Nat) (hr : Recomputed p s r) :
      Step p s { s with origin := queued, awaiting := !queued, sinceCommit := 0, current := r }

/-- With `single`, one writer awaiting each commit: commits find nothing queued, and at most one record arrives
between commits. -/
inductive Reach (p : Params) (single : Bool) : St → Prop
  | init : Reach p single ⟨false, false, 0, fun _ => 0, 0, 0⟩
  | step {s t : St} : Reach p single s → Step p s t →
      (single = true → t.sinceCommit ≤ 1 ∧ (t.sinceCommit = 0 → t.origin = false)) → Reach p single t

theorem cumulative_zero (a : Nat → Nat) (h : ∀ i, a i = 0) : ∀ k, cumulative a k = 0
  | 0 => by simp [cumulative, h]
  | k + 1 => by simp [cumulative, cumulative_zero a h k, h]

theorem not_profitable (p : Params) (s : St) (h : ∀ i, s.arrivals i = 0) (k : Nat) : ¬ Profitable p s k := by
  unfold Profitable
  rw [cumulative_zero _ h k]
  have := p.hmargin
  omega

/-- With empty bins, `Recompute` sets no window, whatever the current one was. -/
theorem recomputed_zero (p : Params) (s : St) (h : ∀ i, s.arrivals i = 0) (r : Nat) (hr : Recomputed p s r) :
    r = 0 := by
  rcases hr with hr | ⟨k, _, hk, _⟩ | ⟨_, k, _, hk⟩
  · exact hr
  · exact absurd hk (not_profitable p s h k)
  · exact absurd hk (not_profitable p s h k)

/-- The single writer's arrivals: between commits only the first record arrives, so none is binned. -/
def Inv (s : St) : Prop :=
  (∀ i, s.arrivals i = 0) ∧ s.current = 0 ∧ (s.origin = true → s.awaiting = false ∧ s.sinceCommit = 1) ∧
    (s.awaiting = true → s.origin = false ∧ s.sinceCommit = 0)

theorem inv {p : Params} {s : St} (h : Reach p true s) : Inv s := by
  induction h with
  | init => exact ⟨fun _ => rfl, rfl, (fun h => by cases h), (fun h => by cases h)⟩
  | step _ hs hc ih =>
    obtain ⟨ha, hcur, ho, haw⟩ := ih
    have hc := hc rfl
    cases hs with
    | first h =>
      obtain ⟨_, h0⟩ := haw h
      exact ⟨ha, hcur, (fun _ => ⟨rfl, by simp [h0]⟩), (fun h' => by cases h')⟩
    | binned k h hor =>
      -- An arrival after the origin is the second since the commit, which the single writer never sends.
      have := (ho hor).2
      simp at hc
      omega
    | early h hor =>
      exact ⟨ha, hcur, (fun h' => by simp [hor] at h'), (fun h' => by simp [h] at h')⟩
    | commit queued r hr =>
      have hr0 := recomputed_zero p _ ha r hr
      have hq : queued = false := by
        cases queued
        · rfl
        · simp at hc
      subst hq
      exact ⟨ha, hr0, (fun h' => by cases h'), (fun _ => ⟨rfl, rfl⟩)⟩

/-- One writer awaiting each commit keeps the window zero after every commit. -/
theorem single_writer_never_lingers {p : Params} {s : St} (h : Reach p true s) :
    s.current = 0 ∧ ∀ i, s.arrivals i = 0 :=
  ⟨(inv h).2.1, (inv h).1⟩

/-- Two records after one commit, as publishers answering commits send: the second is binned. -/
theorem answering_publishers_fill_bins (p : Params) :
    ∃ s, Reach p false s ∧ s.arrivals 0 = 1 := by
  let s0 : St := ⟨false, false, 0, fun _ => 0, 0, 0⟩
  let s1 : St := { s0 with origin := false, awaiting := true, sinceCommit := 0, current := 0 }
  let s2 : St := { s1 with awaiting := false, origin := true, sinceCommit := 1, total := 1 }
  let s3 : St := { s2 with sinceCommit := 2, total := 2,
                           arrivals := fun i => if i = 0 then s2.arrivals i + 1 else s2.arrivals i }
  have r1 : Reach p false s1 := .step .init (.commit _ false 0 (.inl rfl)) (by simp)
  have r2 : Reach p false s2 := .step r1 (.first _ rfl) (by simp)
  have r3 : Reach p false s3 := .step r2 (.binned _ 0 rfl rfl) (by simp)
  exact ⟨s3, r3, by simp [s3, s2, s1, s0]⟩

end EventLog.Linger
