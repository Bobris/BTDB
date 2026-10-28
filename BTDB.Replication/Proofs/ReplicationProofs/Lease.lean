/-!
# Lease authority and confirmation grants

A model of the arithmetic of `LeaseAuthority` and `ConfirmationGrants`. Real time and clock readings are ticks. Each
node's clock `c` is monotone, and with scale `S` (`1 000 000`) and drift bound `ρ < S` (`MaximumClockDriftPpm`) it
reads an interval of real length `Δ` as between `(S − ρ)·Δ / S` and `(S + ρ)·Δ / S`. Divisions round down, as the code
does.

* `lease_safe`: while the local clock is below the deadline, request dispatch plus `UsableDuration`, the
  provider-guaranteed lease still holds.
* `grant_within_lease`: a follower holds the leader's grant as evidence from its poll's dispatch for the confirmation
  duration on its own clock. If the leader issued the grant only when `CanGrant` held, the lease held at every real
  time the follower holds the evidence.
* `drained_grants_expired`: once the leader's clock reaches the drain bound of every grant it issued (`IsDrained`), no
  follower holds any of them as evidence any more, so the lease can move.
-/

namespace Replication.Lease

/-- Clock `c` maps real time to readings with drift at most `ρ` per `S`. -/
structure Clock (S ρ : Nat) (c : Nat → Nat) : Prop where
  mono : ∀ t t', t ≤ t' → c t ≤ c t'
  slow : ∀ t t', t ≤ t' → (S - ρ) * (t' - t) ≤ S * (c t' - c t)
  fast : ∀ t t', t ≤ t' → S * (c t' - c t) ≤ (S + ρ) * (t' - t)

/-- `LeaseAuthority.UsableDuration`: the guaranteed duration `G` slowed by the drift bound, less the margin `m`. -/
def usable (S ρ m G : Nat) : Nat := G * (S - ρ) / S - m

/-- `LeaseAuthority.BoundPeerWindow`: a peer's window `d` on a clock as fast as allowed, read by one as slow as
allowed, rounded up. -/
def bound (S ρ d : Nat) : Nat := (d * (S + ρ) + (S - ρ) - 1) / (S - ρ)

theorem lease_safe {S ρ m G : Nat} {L : Nat → Nat} (hL : Clock S ρ L) (hS : 0 < S) {td t : Nat} (ht : td ≤ t)
    (hv : L t < L td + usable S ρ m G) : t < td + G := by
  apply Nat.lt_of_not_le
  intro hG
  have h1 : (S - ρ) * G ≤ (S - ρ) * (t - td) := Nat.mul_le_mul_left _ (by omega)
  have h2 := hL.slow td t ht
  have h3 : S * (G * (S - ρ) / S) ≤ G * (S - ρ) := Nat.mul_div_le (G * (S - ρ)) S
  have h4 : S * usable S ρ m G ≤ S * (L t - L td) :=
    Nat.le_trans (Nat.mul_le_mul_left S (Nat.sub_le _ _))
      (Nat.le_trans h3 (by rw [Nat.mul_comm G]; exact Nat.le_trans h1 h2))
  have h5 := Nat.le_of_mul_le_mul_left h4 hS
  have := hL.mono td t ht
  omega

theorem le_bound {S ρ : Nat} (hρ : ρ < S) (d : Nat) : d * (S + ρ) ≤ bound S ρ d * (S - ρ) := by
  have := Nat.lt_div_mul_add (a := d * (S + ρ) + (S - ρ) - 1) (b := S - ρ) (by omega)
  unfold bound
  omega

/-- From a grant issued at `tg` to a poll dispatched at `tp ≤ tg`, every later reading of the leader's clock while the
follower holds the evidence stays below the issue reading plus the drain bound. -/
theorem within {S ρ d : Nat} {L P : Nat → Nat} (hL : Clock S ρ L) (hP : Clock S ρ P) (hρ : ρ < S) {tp tg t : Nat}
    (hpg : tp ≤ tg) (hgt : tg ≤ t) (he : P t < P tp + d) : L t < L tg + bound S ρ d := by
  have hS : 0 < S := by omega
  have hpt : tp ≤ t := Nat.le_trans hpg hgt
  have hd : P t - P tp < d := by have := hP.mono tp t hpt; omega
  -- The follower's clock bounds the real time since the poll.
  have h1 : (S - ρ) * (t - tp) < S * d :=
    Nat.lt_of_le_of_lt (hP.slow tp t hpt) (Nat.mul_lt_mul_of_pos_left hd hS)
  -- The leader's clock advanced at most that fast.
  have h2 : S * (L t - L tg) ≤ (S + ρ) * (t - tp) :=
    Nat.le_trans (hL.fast tg t hgt) (Nat.mul_le_mul_left _ (by omega))
  have h3 : (S - ρ) * (S * (L t - L tg)) ≤ (S + ρ) * ((S - ρ) * (t - tp)) := by
    have := Nat.mul_le_mul_left (S - ρ) h2
    rwa [Nat.mul_left_comm (S - ρ) (S + ρ)] at this
  have h4 : (S + ρ) * ((S - ρ) * (t - tp)) < (S + ρ) * (S * d) :=
    Nat.mul_lt_mul_of_pos_left h1 (by omega)
  have h5 : S * ((S - ρ) * (L t - L tg)) < S * (d * (S + ρ)) := by
    have := Nat.lt_of_le_of_lt h3 h4
    rw [Nat.mul_left_comm (S - ρ) S, Nat.mul_left_comm (S + ρ) S, Nat.mul_comm (S + ρ) d] at this
    exact this
  have h6 : (S - ρ) * (L t - L tg) < (S - ρ) * bound S ρ d := by
    have := Nat.lt_of_mul_lt_mul_left h5
    rw [Nat.mul_comm (S - ρ) (bound S ρ d)]
    exact Nat.lt_of_lt_of_le this (le_bound hρ d)
  have h7 := Nat.lt_of_mul_lt_mul_left h6
  have := hL.mono tg t hgt
  omega

/-- A grant issued when `CanGrant` held covers only real times at which the lease holds. -/
theorem grant_within_lease {S ρ m G d : Nat} {L P : Nat → Nat} (hL : Clock S ρ L) (hP : Clock S ρ P) (hρ : ρ < S)
    {td tp tg t : Nat} (hpg : tp ≤ tg)
    (hgrant : L tg + bound S ρ d < L td + usable S ρ m G) (he : P t < P tp + d) : t < td + G := by
  have hS : 0 < S := by omega
  by_cases htd : t < td
  · omega
  by_cases htg : tg ≤ t
  · exact lease_safe hL hS (by omega) (Nat.lt_trans (within hL hP hρ hpg htg he) hgrant)
  · have := hL.mono t tg (by omega)
    exact lease_safe (m := m) hL hS (by omega) (by omega)

/-- Once the leader's clock reached the grant's issue reading plus its drain bound, the follower no longer holds it. -/
theorem drained_grants_expired {S ρ d : Nat} {L P : Nat → Nat} (hL : Clock S ρ L) (hP : Clock S ρ P) (hρ : ρ < S)
    {tp tg tn t : Nat} (hpg : tp ≤ tg) (hgn : tg ≤ tn) (hnt : tn ≤ t) (hdrained : L tg + bound S ρ d ≤ L tn) :
    P tp + d ≤ P t := by
  apply Nat.le_of_not_lt
  intro he
  have := within hL hP hρ hpg (Nat.le_trans hgn hnt) he
  have := hL.mono tn t hnt
  omega

end Replication.Lease
