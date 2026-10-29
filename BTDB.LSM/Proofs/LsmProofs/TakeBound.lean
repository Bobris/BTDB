/-!
# Taking a slice refuses exactly what does not fit

A model of `LsmDecoder.Take` (`../Format/LsmEncoding.cs`), which returns the next `length` bytes of the data or reports
it corrupted. The position is between 0 and the data length `n`, and `n` and `length` are `int`s, below 2^31.

* `take_check_correct`: the check `length > n - position`, which cannot overflow, refuses exactly the lengths that run
  past the data.
* `old_check_wraps`: the check before the 2026-10-29 fix, `position + length > n` in 32-bit arithmetic, wraps
  negative for a length near `int.MaxValue` and lets it through, so the slice threw `ArgumentOutOfRangeException`
  instead (`LsmEncodingTest.LengthPastTheEndIsDamageEvenNearIntMaxValue`).
-/

namespace Lsm.TakeBound

/-- A 32-bit sum read as a signed `int`. -/
def int32Add (a b : Nat) : Int :=
  let s : Nat := (a + b) % 2 ^ 32
  if s < 2 ^ 31 then (s : Int) else (s : Int) - 2 ^ 32

/-- `Take` refuses the length. -/
def refuses (n position length : Nat) : Bool := decide (length > n - position)

def oldRefuses (n position length : Nat) : Bool := decide (int32Add position length > n)

theorem take_check_correct (n position length : Nat) (hp : position ≤ n) :
    refuses n position length = true ↔ position + length > n := by
  unfold refuses
  simp only [decide_eq_true_eq]
  omega

/-- One byte read of ten, then a length of `int.MaxValue`. -/
theorem old_check_wraps :
    oldRefuses 10 1 (2 ^ 31 - 1) = false ∧ 1 + (2 ^ 31 - 1) > 10 ∧ refuses 10 1 (2 ^ 31 - 1) = true := by
  simp [oldRefuses, int32Add, refuses]

end Lsm.TakeBound
