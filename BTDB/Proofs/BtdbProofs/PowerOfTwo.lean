/-!
# Rounding a capacity up to a power of two

A model of `HashHelpers.PowerOf2` (`../../Collections/HashHelpers.cs`), which `RefDictionary`,
`SpanByteNoRemoveDictionary`, `LruCache`, `SpanByteLruCache` and `Utf16Utf8Dictionary` size their buckets with, so
that a hash masked by `capacity - 1` picks a bucket. It returns `v` when `v & (v - 1)` is 0, and otherwise smears the
highest bit of `v - 1` into all bits below it by or-ing in its shifts by 1, 2, 4, 8 and 16, then adds 1.

For `v` from 1 to `2^30` every value involved is a non-negative `int`, so the arithmetic shifts are logical and
natural numbers model the C# exactly. Callers pass at least 1 (most at least 2).

* `smear_eq`: the smearing turns any `w` below `2^32` but 0 into `2^(log2 w + 1) - 1`.
* `power_of_two_of_and_pred`: `v & (v - 1) = 0` holds only for powers of two (and 0).
* `powerOf2_correct`: the result is a power of two `r` with `v ≤ r < 2 v`, the smallest power of two not below `v`,
  and at most `2^30`, so it is a valid `int` array length.
-/

namespace Btdb.PowerOfTwo

def smear (w : Nat) : Nat :=
  let a := w ||| w >>> 1
  let b := a ||| a >>> 2
  let c := b ||| b >>> 4
  let d := c ||| c >>> 8
  d ||| d >>> 16

def powerOf2 (v : Nat) : Nat :=
  if v &&& (v - 1) = 0 then v else smear (v - 1) + 1

/-- The bits from `m` down to `m - k + 1` of `x` are set, and none above `m`. -/
def Top (m k x : Nat) : Prop := x < 2 ^ (m + 1) ∧ ∀ i, i ≤ m → m < i + k → x.testBit i = true

theorem top_step {m k x : Nat} (h : Top m k x) : Top m (2 * k) (x ||| x >>> k) := by
  obtain ⟨hlt, hbits⟩ := h
  refine ⟨Nat.or_lt_two_pow hlt ?_, fun i hi hk => ?_⟩
  · rw [Nat.shiftRight_eq_div_pow]
    exact Nat.lt_of_le_of_lt (Nat.div_le_self _ _) hlt
  · rw [Nat.testBit_or, Nat.testBit_shiftRight]
    by_cases hik : m < i + k
    · simp [hbits i hi hik]
    · simp [hbits (k + i) (by omega) (by omega)]

theorem top_start {w : Nat} (hw : w ≠ 0) : Top w.log2 1 w :=
  ⟨Nat.lt_log2_self, fun i hi hk => by
    have : i = w.log2 := by omega
    subst this
    exact Nat.testBit_log2 hw⟩

theorem smear_eq (w : Nat) (hw : w ≠ 0) (hlt : w < 2 ^ 32) : smear w = 2 ^ (w.log2 + 1) - 1 := by
  have hm : w.log2 < 32 := (Nat.log2_lt hw).2 hlt
  have ht : Top w.log2 32 (smear w) := top_step (top_step (top_step (top_step (top_step (top_start hw)))))
  obtain ⟨hs, hbits⟩ := ht
  apply Nat.eq_of_testBit_eq
  intro i
  rw [Nat.testBit_two_pow_sub_one]
  by_cases hi : i ≤ w.log2
  · rw [hbits i hi (by omega)]
    simp
    omega
  · rw [Nat.testBit_lt_two_pow (Nat.lt_of_lt_of_le hs (Nat.pow_le_pow_right (by decide) (by omega)))]
    simp
    omega

theorem testBit_of_bounds {x m : Nat} (h1 : 2 ^ m ≤ x) (h2 : x < 2 ^ (m + 1)) : x.testBit m = true := by
  rw [Nat.testBit_eq_decide_div_mod_eq]
  have : x / 2 ^ m = 1 := Nat.div_eq_of_lt_le (by omega) (by rw [Nat.pow_succ] at h2; omega)
  simp [this]

theorem power_of_two_of_and_pred {v : Nat} (hv : v ≠ 0) (h : v &&& (v - 1) = 0) : v = 2 ^ v.log2 := by
  have hle := Nat.log2_self_le hv
  have hlt : v < 2 ^ (v.log2 + 1) := Nat.lt_log2_self
  apply Nat.le_antisymm _ hle
  apply Nat.le_of_not_lt
  intro hgt
  have hbit : (v &&& (v - 1)).testBit v.log2 = true := by
    rw [Nat.testBit_and, Nat.testBit_log2 hv, testBit_of_bounds (by omega) (by omega)]
    rfl
  rw [h, Nat.zero_testBit] at hbit
  exact Bool.false_ne_true hbit

theorem powerOf2_correct (v : Nat) (h1 : 1 ≤ v) (h2 : v ≤ 2 ^ 30) :
    ∃ n, powerOf2 v = 2 ^ n ∧ v ≤ 2 ^ n ∧ 2 ^ n < 2 * v ∧ 2 ^ n ≤ 2 ^ 30 := by
  unfold powerOf2
  by_cases hand : v &&& (v - 1) = 0
  · have hp := power_of_two_of_and_pred (by omega) hand
    refine ⟨v.log2, by simp [hand, ← hp], by omega, by omega, by omega⟩
  · simp only [hand, ↓reduceIte]
    have hw : v - 1 ≠ 0 := by
      intro h0
      have : v = 1 := by omega
      subst this
      exact hand rfl
    rw [smear_eq (v - 1) hw (by omega)]
    have hpos : 0 < 2 ^ ((v - 1).log2 + 1) := Nat.two_pow_pos _
    have hlt : v - 1 < 2 ^ ((v - 1).log2 + 1) := Nat.lt_log2_self
    have hle : 2 ^ (v - 1).log2 ≤ v - 1 := Nat.log2_self_le hw
    have hsucc : 2 ^ ((v - 1).log2 + 1) = 2 * 2 ^ (v - 1).log2 := by rw [Nat.pow_succ]; omega
    refine ⟨(v - 1).log2 + 1, by omega, by omega, by omega, ?_⟩
    have : (v - 1).log2 < 30 := (Nat.log2_lt hw).2 (by omega)
    exact Nat.pow_le_pow_right (by decide) (by omega)

end Btdb.PowerOfTwo
