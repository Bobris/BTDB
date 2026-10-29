/-!
# Level size targets

A model of `LsmCompaction.LevelTarget` (`../Engine/LsmCompaction.cs`). Level 1 has target `LevelBaseBytes`; each
deeper level multiplies it by `LevelMultiplier`, and a product past `long.MaxValue` becomes `long.MaxValue`: the step
is `if t > max / m then max else t * m`.

* `target_eq`: the target of level `k + 1` is `min (base * m ^ k) max`, the exact product capped at `max`.
* `target_mono`: deeper levels never get a smaller target. Before the 2026-10-29 fix the product wrapped, so with a
  multiplier of 1000 level 5 got a negative target and never compacted
  (`LsmRegressionTest.LevelTargetSaturatesInsteadOfWrapping`).
-/

namespace Lsm.LevelTarget

/-- One deeper level, saturating at `max`. -/
def step (max m t : Nat) : Nat := if t > max / m then max else t * m

/-- The target of level `k + 1`. -/
def target (max m base : Nat) : Nat → Nat
  | 0 => base
  | k + 1 => step max m (target max m base k)

theorem step_min (max m x : Nat) (hm : 0 < m) : step max m (min x max) = min (x * m) max := by
  unfold step
  by_cases hx : x ≤ max
  · rw [Nat.min_eq_left hx]
    split
    · next h =>
      have : max < x * m := (Nat.div_lt_iff_lt_mul hm).mp h
      omega
    · next h =>
      have : ¬max < x * m := fun h' => h ((Nat.div_lt_iff_lt_mul hm).mpr h')
      omega
  · have hmx : max ≤ x * m := Nat.le_trans (by omega) (Nat.le_mul_of_pos_right x hm)
    rw [Nat.min_eq_right (by omega), Nat.min_eq_right hmx]
    split
    · rfl
    · next h =>
      have : ¬max < max * m := fun h' => h ((Nat.div_lt_iff_lt_mul hm).mpr h')
      have : max ≤ max * m := Nat.le_mul_of_pos_right max hm
      omega

theorem target_eq (max m base : Nat) (hm : 0 < m) (hb : base ≤ max) (k : Nat) :
    target max m base k = min (base * m ^ k) max := by
  induction k with
  | zero => simp [target, hb]
  | succ k ih => rw [target, ih, step_min _ _ _ hm, Nat.pow_succ, Nat.mul_assoc]

theorem target_mono (max m base : Nat) (hm : 0 < m) (hb : base ≤ max) (k : Nat) :
    target max m base k ≤ target max m base (k + 1) := by
  rw [target_eq _ _ _ hm hb, target_eq _ _ _ hm hb, Nat.pow_succ, ← Nat.mul_assoc]
  have := Nat.le_mul_of_pos_right (base * m ^ k) hm
  simp only [Nat.min_def]
  split <;> split <;> omega

end Lsm.LevelTarget
