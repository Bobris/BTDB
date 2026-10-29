/-!
# Reads under a sequence lock

A model of the read protocol of `SeqLock` (`../Locks/SeqLock.cs`). Time is a sequence of steps; `c t` is the lock's
counter and `d t` the protected data at step `t`. A writer makes the counter odd (`StartWrite`), changes the data, and
makes it even again (`EndWrite`), so the counter never decreases and the data changes only while the counter is odd.

A reader reads an even counter at `t1` (`StartRead`), reads the data at `t2`, and reads the counter again at `t3`
(`RetryRead`), accepting the data when the counter did not change.

* `accepted_read_is_consistent`: when `t1 ≤ t2 ≤ t3`, an accepted read saw the data as it was at `t1`, written by no
  half-done write.
* `late_read_can_tear`: when the data read lands after the second counter read, as a CPU reordering loads may do
  without the read barrier `RetryRead` issues since the 2026-10-29 fix, an accepted read can see data changed by a
  write that started after `t1` (`SeqLockTests.ReadsThatPassRetryNeverSeeAHalfDoneWrite`).
-/

namespace Btdb.SeqLockRead

/-- The counter never decreases, and the data changes only in a step that starts with an odd counter. -/
structure Protocol (c d : Nat → Nat) : Prop where
  mono : ∀ t, c t ≤ c (t + 1)
  writes_odd : ∀ t, d (t + 1) ≠ d t → c t % 2 = 1

theorem mono_le {c d : Nat → Nat} (h : Protocol c d) {t t' : Nat} (ht : t ≤ t') : c t ≤ c t' := by
  induction t' with
  | zero => simp at ht; subst ht; exact Nat.le_refl _
  | succ t' ih =>
    rcases Nat.lt_or_eq_of_le ht with hlt | heq
    · exact Nat.le_trans (ih (by omega)) (h.mono t')
    · subst heq; exact Nat.le_refl _

theorem accepted_read_is_consistent {c d : Nat → Nat} (h : Protocol c d) {t1 t2 t3 : Nat} (heven : c t1 % 2 = 0)
    (h12 : t1 ≤ t2) (h23 : t2 ≤ t3) (hsame : c t3 = c t1) : d t2 = d t1 := by
  have stable : ∀ t, t1 ≤ t → t ≤ t3 → c t = c t1 := fun t ha hb =>
    Nat.le_antisymm (hsame ▸ mono_le h hb) (mono_le h ha)
  have : ∀ k, t1 + k ≤ t2 → d (t1 + k) = d t1 := by
    intro k
    induction k with
    | zero => intro _; rfl
    | succ k ih =>
      intro hk
      have hprev := ih (by omega)
      by_cases hne : d (t1 + k + 1) = d (t1 + k)
      · rw [← hprev, ← hne]; rfl
      · have hodd := h.writes_odd (t1 + k) hne
        rw [stable (t1 + k) (by omega) (by omega)] at hodd
        omega
  have := this (t2 - t1) (by omega)
  rwa [Nat.add_sub_cancel' h12] at this

/-- A write starting after step 1: the counter goes 0, 0, 1, 1, 2, … and the data changes from step 2 to step 3. -/
def cEx (t : Nat) : Nat := min 2 (t / 2)
def dEx (t : Nat) : Nat := min 1 (t / 3)

theorem late_read_can_tear : Protocol cEx dEx ∧ cEx 0 % 2 = 0 ∧ cEx 1 = cEx 0 ∧ dEx 3 ≠ dEx 0 := by
  refine ⟨⟨fun t => ?_, fun t ht => ?_⟩, by decide, by decide, by decide⟩
  · unfold cEx; omega
  · unfold dEx at ht; unfold cEx
    have : t = 2 := by omega
    subst this
    decide

end Btdb.SeqLockRead
