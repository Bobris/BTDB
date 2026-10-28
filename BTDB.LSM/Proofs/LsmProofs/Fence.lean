import LsmProofs.Common

/-!
# Manifest fencing hands out fresh epochs

A model of how a takeover gets its epoch (`BecomeWriterAsync`). It reads manifest name `m`, its writer epoch and the
highest epoch `x` in the WAL. Its epoch is one more than the highest it read, and it creates manifest `m + 1` if
absent (`CreateObject` with `fence`). A create that finds the name taken reads the object back. Equal content counts
as the session's own lost response; other content fences it. A fenced-in session then writes manifests at the
following names with its epoch. Reads may be arbitrarily old, so the model also covers requests that land late.
Deleting replaced manifest names is not modelled.

`Rule.current` is the protocol before the 2026-10-28 fix, whose manifest holds nothing of the session. Two takeovers
that read the same state and write the same pending deletions write equal manifests, so each takes the other's for
its own. `Rule.fixed` adds the session's random number (`WriterSession`), modelled as a fresh one.

* `current_shares_epoch`: under the current rule, two sessions fence in with the same epoch at the same name.
* `fixed_fresh_epochs`: under the fixed rule, sessions that fenced in have different epochs, and the one fenced at the
  later name has the higher epoch. That is the assumption of `Wal.lean` and `WalFlush.lean`.
-/

namespace Lsm.Fence

structure Man where
  epoch : Nat
  session : Nat
  payload : Nat
deriving DecidableEq

inductive Rule where
  | current
  | fixed
deriving DecidableEq

/-- The session field of a manifest: the session's number under the fixed rule, and nothing (0) before it. -/
def tag (r : Rule) (s : Nat) : Nat := if r = .fixed then s else 0

structure State where
  /-- Manifests by name; name 0 stands for the empty database. -/
  man : Nat → Option Man
  /-- Sessions whose fence succeeded, with their fence name and epoch. -/
  fenced : Nat → Option (Nat × Nat)
  /-- The next session number; sessions are numbered from 1. -/
  next : Nat

def init : State := ⟨upd (fun _ => none) 0 (some ⟨0, 0, 0⟩), fun _ => none, 1⟩

inductive Step (r : Rule) : State → State → Prop
  /-- A takeover that read manifest `m` and WAL epoch `x` creates manifest `m + 1`; `p` stands for the rest of its
  content. -/
  | fence (σ : State) (m x p : Nat) (M : Man) (hm : σ.man m = some M) (hfree : σ.man (m + 1) = none) :
      Step r σ ⟨upd σ.man (m + 1) (some ⟨max M.epoch x + 1, tag r σ.next, p⟩),
        upd σ.fenced σ.next (some (m + 1, max M.epoch x + 1)), σ.next + 1⟩
  /-- The name already holds equal content, which the takeover takes for its own lost response. -/
  | adopt (σ : State) (m x p : Nat) (M : Man) (hm : σ.man m = some M)
      (heq : σ.man (m + 1) = some ⟨max M.epoch x + 1, tag r σ.next, p⟩) :
      Step r σ ⟨σ.man, upd σ.fenced σ.next (some (m + 1, max M.epoch x + 1)), σ.next + 1⟩
  /-- A session that fenced in writes the manifest after one of its own. -/
  | write (σ : State) (s k p f e : Nat) (M : Man) (hs : σ.fenced s = some (f, e)) (hk : σ.man k = some M)
      (hM : M.epoch = e ∧ M.session = tag r s) (hfree : σ.man (k + 1) = none) :
      Step r σ { σ with man := upd σ.man (k + 1) (some ⟨e, tag r s, p⟩) }

inductive Reach (r : Rule) : State → Prop
  | init : Reach r init
  | step {σ τ : State} : Reach r σ → Step r σ τ → Reach r τ

/-- Two takeovers read the empty database. The first creates manifest 1; the second finds equal content there and
takes it for its own. Both write with epoch 1. -/
theorem current_shares_epoch : ∃ σ, Reach .current σ ∧ σ.fenced 1 = some (1, 1) ∧ σ.fenced 2 = some (1, 1) := by
  have r1 := Reach.step Reach.init (Step.fence (r := .current) init 0 0 0 ⟨0, 0, 0⟩ rfl rfl)
  have r2 := Reach.step r1 (Step.adopt _ 0 0 0 ⟨0, 0, 0⟩ rfl rfl)
  exact ⟨_, r2, rfl, rfl⟩

structure Inv (σ : State) : Prop where
  next_pos : 1 ≤ σ.next
  session_lt : ∀ n M, σ.man n = some M → M.session < σ.next
  chain : ∀ n M, σ.man (n + 1) = some M → ∃ M', σ.man n = some M' ∧ M'.epoch ≤ M.epoch
  fence : ∀ s f e, σ.fenced s = some (f, e) →
    s < σ.next ∧ (∃ p, σ.man f = some ⟨e, s, p⟩) ∧ 1 ≤ f ∧ ∃ M, σ.man (f - 1) = some M ∧ M.epoch < e

theorem inv_init : Inv init where
  next_pos := by decide
  session_lt n M h := by
    simp only [init] at h
    rcases upd_some h with ⟨_, rfl⟩ | ⟨_, h⟩
    · decide
    · cases h
  chain n M h := by
    simp only [init] at h
    rcases upd_some h with ⟨h1, _⟩ | ⟨_, h⟩
    · omega
    · cases h
  fence _ _ _ h := by cases h

/-- Creating a free name keeps every manifest that already existed. -/
theorem keep {man : Nat → Option Man} {i n : Nat} {M x : Man} (hfree : man i = none) (h : man n = some M) :
    upd man i (some x) n = some M := by
  have : n ≠ i := by intro he; subst he; rw [hfree] at h; cases h
  rw [upd_ne _ _ this]; exact h

theorem inv_step {σ τ : State} (hI : Inv σ) (h : Step .fixed σ τ) : Inv τ := by
  cases h with
  | fence m x p M hm hfree =>
    have hm' : upd σ.man (m + 1) (some ⟨max M.epoch x + 1, tag .fixed σ.next, p⟩) m = some M :=
      keep hfree hm
    exact {
      next_pos := by arith
      session_lt := fun n N hn => by
        rcases upd_some hn with ⟨_, rfl⟩ | ⟨_, hn⟩
        · simp [tag]
        · have := hI.session_lt n N hn; arith
      chain := fun n N hn => by
        rcases upd_some hn with ⟨h1, rfl⟩ | ⟨h1, hn⟩
        · have : n = m := by omega
          subst this
          exact ⟨M, hm', by dsimp only; omega⟩
        · obtain ⟨N', h2, h3⟩ := hI.chain n N hn
          exact ⟨N', keep hfree h2, h3⟩
      fence := fun s f e hs => by
        rcases upd_some hs with ⟨rfl, he⟩ | ⟨hne, hs⟩
        · cases he
          exact ⟨by arith, ⟨p, by simp [tag]⟩, by omega, M, by simpa using hm', by omega⟩
        · obtain ⟨h1, ⟨q, h2⟩, h3, N, h4, h5⟩ := hI.fence s f e hs
          exact ⟨by arith, ⟨q, keep hfree h2⟩, h3, N, keep hfree h4, h5⟩ }
  | adopt m x p M hm heq =>
    have := hI.session_lt _ _ heq
    simp [tag] at this
  | write s k p f e M hs hk hM hfree =>
    obtain ⟨hs1, _⟩ := hI.fence s f e hs
    exact {
      next_pos := hI.next_pos
      session_lt := fun n N hn => by
        rcases upd_some hn with ⟨_, rfl⟩ | ⟨_, hn⟩
        · simpa [tag] using hs1
        · exact hI.session_lt n N hn
      chain := fun n N hn => by
        rcases upd_some hn with ⟨h1, rfl⟩ | ⟨h1, hn⟩
        · have : n = k := by omega
          subst this
          exact ⟨M, keep hfree hk, by dsimp only; omega⟩
        · obtain ⟨N', h2, h3⟩ := hI.chain n N hn
          exact ⟨N', keep hfree h2, h3⟩
      fence := fun s' f' e' hs' => by
        obtain ⟨h1, ⟨q, h2⟩, h3, N, h4, h5⟩ := hI.fence s' f' e' hs'
        exact ⟨h1, ⟨q, keep hfree h2⟩, h3, N, keep hfree h4, h5⟩ }

theorem reach_inv {σ : State} (h : Reach .fixed σ) : Inv σ := by
  induction h with
  | init => exact inv_init
  | step _ hs ih => exact inv_step ih hs

/-- Epochs never decrease along manifest names. -/
theorem mono {σ : State} (hI : Inv σ) {n : Nat} {M : Man} (hn : σ.man n = some M) :
    ∀ d M', σ.man (n + d) = some M' → M.epoch ≤ M'.epoch
  | 0, M', h => by rw [Nat.add_zero, hn] at h; cases h; exact Nat.le_refl _
  | d + 1, M', h => by
    obtain ⟨N, h1, h2⟩ := hI.chain (n + d) M' h
    exact Nat.le_trans (mono hI hn d N h1) h2

theorem fresh_of_lt {σ : State} (hI : Inv σ) {s t f e f' e' : Nat} (hs : σ.fenced s = some (f, e))
    (ht : σ.fenced t = some (f', e')) (hlt : f < f') : e < e' := by
  obtain ⟨_, ⟨p, hp⟩, _⟩ := hI.fence s f e hs
  obtain ⟨_, _, h3, N, h4, h5⟩ := hI.fence t f' e' ht
  have := mono hI hp (f' - 1 - f) N (by rw [show f + (f' - 1 - f) = f' - 1 by omega]; exact h4)
  dsimp only at this
  omega

theorem fixed_fresh_epochs {σ : State} (h : Reach .fixed σ) {s t f e f' e' : Nat} (hst : s ≠ t)
    (hs : σ.fenced s = some (f, e)) (ht : σ.fenced t = some (f', e')) : e ≠ e' ∧ (f < f' → e < e') := by
  have hI := reach_inv h
  refine ⟨?_, fresh_of_lt hI hs ht⟩
  rcases Nat.lt_trichotomy f f' with hlt | heq | hgt
  · exact Nat.ne_of_lt (fresh_of_lt hI hs ht hlt)
  · subst heq
    obtain ⟨_, ⟨p, hp⟩, _⟩ := hI.fence s f e hs
    obtain ⟨_, ⟨q, hq⟩, _⟩ := hI.fence t f e' ht
    rw [hp] at hq
    cases hq
    exact absurd rfl hst
  · exact Nat.ne_of_gt (fresh_of_lt hI ht hs hgt)

end Lsm.Fence
