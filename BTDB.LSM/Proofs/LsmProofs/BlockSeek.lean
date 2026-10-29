/-!
# Seeking in a data block finds the first entry at or after a key

A model of `RawBlock.LowerBound` (`../Format/DataBlock.cs`). A block holds `n` entries sorted by key and a restart
every `R` entries, `R > 0`; `p i` says whether entry `i` is at or after the sought key (after it, when the key itself is
excluded). Keys are sorted, so `p` is monotone. `LowerBound` finds the first restart whose entry satisfies `p`, then
scans the entries of the restart before it, up to the next restart or the end of the block. The binary search for the
restart is not modelled: `scan` stands for its result, the first restart that satisfies `p`, or the restart count.

* `lower_bound_correct`: the result is the index of the first entry that satisfies `p`, or `n` when none does.
-/

namespace Lsm.BlockSeek

/-- The first index in `[i, e)` satisfying `p`, or `e`. -/
def scan (p : Nat → Bool) (i e : Nat) : Nat :=
  if i < e then (if p i then i else scan p (i + 1) e) else e
termination_by e - i

/-- `j` is the first index in `[i, e)` satisfying `p`, or `e` when none does. -/
def First (p : Nat → Bool) (i e j : Nat) : Prop :=
  i ≤ j ∧ j ≤ e ∧ (∀ k, i ≤ k → k < j → p k = false) ∧ (j < e → p j = true)

theorem scan_first (p : Nat → Bool) (i e : Nat) (h : i ≤ e) : First p i e (scan p i e) := by
  induction h' : e - i generalizing i with
  | zero =>
    have : i = e := by omega
    subst this
    unfold scan
    simp only [Nat.lt_irrefl, ite_false]
    exact ⟨Nat.le_refl _, Nat.le_refl _, fun k h1 h2 => absurd h2 (by omega), fun h => absurd h (Nat.lt_irrefl _)⟩
  | succ d ih =>
    have hie : i < e := by omega
    unfold scan
    rw [ite_eq_left hie]
    by_cases hp : p i
    · rw [ite_eq_left hp]
      exact ⟨Nat.le_refl _, h, fun k h1 h2 => absurd h2 (by omega), fun _ => hp⟩
    · rw [ite_eq_right hp]
      obtain ⟨h1, h2, h3, h4⟩ := ih (i + 1) (by omega) (by omega)
      refine ⟨by omega, h2, ?_, h4⟩
      intro k hk1 hk2
      rcases Nat.lt_or_ge k (i + 1) with hk | hk
      · have : k = i := by omega
        subst this
        simpa using hp
      · exact h3 k hk hk2

theorem first_unique (p : Nat → Bool) (i e a b : Nat) (ha : First p i e a) (hb : First p i e b) : a = b := by
  obtain ⟨a1, a2, a3, a4⟩ := ha
  obtain ⟨b1, b2, b3, b4⟩ := hb
  rcases Nat.lt_trichotomy a b with h | h | h
  · have := b3 a a1 h; have := a4 (by omega); simp_all
  · exact h
  · have := a3 b b1 h; have := b4 (by omega); simp_all

/-- `LowerBound` with `rc` restarts: restart `r` is entry `r * R`. -/
def lowerBound (p : Nat → Bool) (n R rc : Nat) : Nat :=
  let lo := scan (fun r => p (r * R)) 0 rc
  if lo = 0 then 0 else scan p ((lo - 1) * R) (min n ((lo - 1) * R + R))

/-- With `rc` restarts covering the block, seeking finds the first entry satisfying `p`. -/
theorem lower_bound_correct (p : Nat → Bool) (n R rc : Nat)
    (hmono : ∀ i j, i ≤ j → j < n → p i = true → p j = true)
    (hcover : n ≤ rc * R) (hexist : ∀ r, r < rc → r * R < n) :
    lowerBound p n R rc = scan p 0 n := by
  apply first_unique p 0 n _ _ _ (scan_first p 0 n (Nat.zero_le _))
  obtain ⟨l1, l2, l3, l4⟩ := scan_first (fun r => p (r * R)) 0 rc (Nat.zero_le _)
  generalize hlo : scan (fun r => p (r * R)) 0 rc = lo at l1 l2 l3 l4
  unfold lowerBound
  simp only [hlo]
  by_cases h0 : lo = 0
  · rw [ite_eq_left h0]
    subst h0
    refine ⟨Nat.le_refl _, Nat.zero_le _, fun k h1 h2 => absurd h2 (by omega), fun hn => ?_⟩
    have hrc : 0 < rc := by
      rcases Nat.eq_zero_or_pos rc with h | h
      · rw [h, Nat.zero_mul] at hcover; omega
      · exact h
    simpa using l4 hrc
  · rw [ite_eq_right h0]
    obtain ⟨m, rfl⟩ : ∃ m, lo = m + 1 := ⟨lo - 1, by omega⟩
    simp only [Nat.add_sub_cancel]
    have hstep : m * R + R = (m + 1) * R := (Nat.succ_mul m R).symm
    -- The restart before the found one does not satisfy p, so no entry before it does.
    have hprev : p (m * R) = false := l3 m (Nat.zero_le _) (by omega)
    have hmn : m * R < n := hexist m (by omega)
    have hbefore : ∀ k, k ≤ m * R → p k = false := by
      intro k hk
      cases hpk : p k
      · rfl
      · have := hmono k (m * R) hk hmn hpk; rw [hprev] at this; exact absurd this (by simp)
    have hle : m * R ≤ min n (m * R + R) := by omega
    obtain ⟨s1, s2, s3, s4⟩ := scan_first p (m * R) (min n (m * R + R)) hle
    generalize hres : scan p (m * R) (min n (m * R + R)) = res at s1 s2 s3 s4
    rw [hstep] at s2 s4
    refine ⟨Nat.zero_le _, by omega, ?_, ?_⟩
    · intro k _ hk
      rcases Nat.lt_or_ge k (m * R) with h | h
      · exact hbefore k (by omega)
      · exact s3 k h hk
    · intro hr
      rcases Nat.lt_or_ge res (min n ((m + 1) * R)) with h | h
      · exact s4 h
      · -- The scan reached the next restart, which satisfies p.
        have hres_eq : res = (m + 1) * R := by
          rcases Nat.lt_or_ge (m + 1) rc with hr1 | hr1
          · have := hexist (m + 1) hr1; omega
          · have : m + 1 = rc := by omega
            subst this
            omega
        have hlt : m + 1 < rc := by
          rcases Nat.lt_or_ge (m + 1) rc with hr1 | hr1
          · exact hr1
          · have : m + 1 = rc := by omega
            subst this
            omega
        rw [hres_eq]
        simpa using l4 hlt

end Lsm.BlockSeek
