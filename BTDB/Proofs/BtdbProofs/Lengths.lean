import BtdbProofs.VInt

/-!
# The branchless length functions of the variable-length integers

`VUInt.lean` and `VInt.lean` take each value's length from the reference code kept in comments in
`PackUnpack`. The code runs branchless forms instead, from the leading zero count of the value or of its first byte.
`BitOperations.LeadingZeroCount(x)` of a `w`-bit `x` is `w − b` for the bit length `b` of `x` (`BitLen x b`).

* `length_vuint64`, `length_vuint32`: `LengthVUInt(ulong)` and `LengthVUInt(uint)` give `VUInt.len`.
* `length_vint64`, `length_vint32`: `LengthVInt(long)`, through the `LzcToVIntLen` table, and `LengthVInt(int)` give
  the length of the value's class in `VInt.lean`.
* `vuint_first_byte`, `vint_first_byte`: `LengthVUIntByFirstByte` and `LengthVIntByFirstByte`, which readers use to
  decode, return the length of the encoding from its first byte.
-/

namespace Btdb.Lengths

open VUInt

/-- `x` has bit length `b`. -/
def BitLen (x b : Nat) : Prop := x < 2 ^ b ∧ (0 < b → 2 ^ (b - 1) ≤ x)

/-- Bit length of a byte, for computing. -/
def blen : Nat → Nat → Nat
  | 0, _ => 0
  | f + 1, x => if x = 0 then 0 else blen f (x / 2) + 1

theorem blen_byte : ∀ x, x < 256 → BitLen x (blen 8 x) := by
  unfold BitLen; decide +kernel

theorem threshold {x b : Nat} (h : BitLen x b) (t : Nat) : x < 2 ^ t ↔ b ≤ t := by
  constructor
  · intro hx
    apply Nat.le_of_not_lt
    intro hbt
    have := Nat.pow_le_pow_right (by decide : 0 < 2) (show t ≤ b - 1 by omega)
    have := h.2 (by omega)
    omega
  · intro hbt
    exact Nat.lt_of_lt_of_le h.1 (Nat.pow_le_pow_right (by decide) hbt)

/-- `LengthVUInt(ulong)`: `(20441 − lzc · 287) >> 11`. -/
theorem length_vuint64 {x b : Nat} (h : BitLen x b) (hb : b ≤ 64) :
    (20441 - (64 - b) * 287) / 2048 = len x := by
  simp only [len, threshold h 7, threshold h 14, threshold h 21, threshold h 28, threshold h 35, threshold h 42,
    threshold h 49, threshold h 56]
  clear h
  revert hb
  revert b
  decide +kernel

/-- `LengthVUInt(uint)`: `(352 − lzc · 9) >> 6`. -/
theorem length_vuint32 {x b : Nat} (h : BitLen x b) (hb : b ≤ 32) :
    (352 - (32 - b) * 9) / 64 = len x := by
  simp only [len, threshold h 7, threshold h 14, threshold h 21, threshold h 28, threshold h 35, threshold h 42,
    threshold h 49, threshold h 56]
  clear h
  revert hb
  revert b
  decide +kernel

/-- `LengthVUIntByFirstByte`: `LeadingZeroCount(first ^ 0xff) + 9 − 32`. -/
def vuintByFirst (first : Nat) : Nat := 32 - blen 8 (first ^^^ 0xff) + 9 - 32

theorem vuint_by_first_table : ∀ L, L < 10 → 1 ≤ L → ∀ first, first < 256 → lo L ≤ first → first < hi L →
    vuintByFirst first = L := by
  decide +kernel

/-- The first byte of every encoding gives its length. -/
theorem vuint_first_byte {v : Nat} (hv : v < 2 ^ 64) : ∃ first rest, vuint v = first :: rest ∧
    vuintByFirst first = len v := by
  obtain ⟨f1, f2, -, -, -, -, -⟩ := facts v hv
  obtain ⟨rest, he⟩ := head_eq (N := base (len v) + v) f1
  obtain ⟨h1, h2⟩ := head_bounds v hv
  have hhi : hi (len v) ≤ 256 := by
    have : ∀ L, L < 10 → 1 ≤ L → hi L ≤ 256 := by decide
    exact this _ (by omega) f1
  exact ⟨_, rest, he, vuint_by_first_table _ (by omega) f1 _ (by omega) h1 h2⟩

/-- `LengthVIntByFirstByte`: a first byte of a negative value is inverted, then the leading zero count picks a
nibble of `0x976543211`. -/
def vintByFirst (first : Nat) : Nat :=
  let x := if 128 ≤ first then first ^^^ 0xff else first
  0x976543211 / 16 ^ (32 - blen 8 x + 8 - 32) % 16

theorem vint_by_first_table : ∀ k, k < 15 → ∀ first, first < 256 → VInt.lo k ≤ first → first < VInt.hi k →
    vintByFirst first = VInt.len k := by
  decide +kernel

theorem vint_first_byte {v : Int} (hv : -2 ^ 63 ≤ v ∧ v < 2 ^ 63) : ∃ first rest, VInt.vint v = first :: rest ∧
    vintByFirst first = VInt.len (VInt.cls v) := by
  obtain ⟨f0, f1, -, -, -, -, -, -⟩ := VInt.facts v hv
  obtain ⟨rest, he⟩ := head_eq (N := VInt.num v) f1
  obtain ⟨h1, h2⟩ := VInt.head_bounds v hv
  have hhi : VInt.hi (VInt.cls v) ≤ 256 := by
    have : ∀ k, k < 15 → VInt.hi k ≤ 256 := by decide
    exact this _ (by omega)
  exact ⟨_, rest, he, vint_by_first_table _ (by omega) _ (by omega) h1 h2⟩

/-- `LzcToVIntLen`, indexed by the leading zero count of `value ^ (value >> 63)`. -/
def lzcToVIntLen : List Nat :=
  [9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 7, 7, 7, 7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5,
    5, 4, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1]

/-- The magnitude `LengthVInt` measures: `value ^ (value >> 63)` is `value`, or `−value − 1` when negative. -/
def magnitude (v : Int) : Nat := if v < 0 then (-v - 1).toNat else v.toNat

/-- The class length from the bit length of the magnitude. -/
def lenOfBits (b : Nat) : Nat :=
  if b ≤ 6 then 1 else if b ≤ 13 then 2 else if b ≤ 20 then 3 else if b ≤ 27 then 4 else if b ≤ 34 then 5
  else if b ≤ 41 then 6 else if b ≤ 48 then 7 else 9

theorem class_length {v : Int} {b : Nat} (hv : -2 ^ 63 ≤ v ∧ v < 2 ^ 63) (h : BitLen (magnitude v) b) :
    VInt.len (VInt.cls v) = lenOfBits b := by
  have t := threshold h
  have c : ∀ t : Nat, ((2 ^ t : Nat) : Int) = 2 ^ t := fun t => by simp
  unfold magnitude at t
  by_cases hn : v < 0
  · simp only [hn, ite_true] at t
    have n : ∀ t : Nat, (v < -2 ^ t ↔ ¬ b ≤ t) := fun s => by
      rw [← t s, ← c s]; omega
    have p : ∀ t : Nat, v < 2 ^ t := fun s => by have : (0 : Int) < 2 ^ s := Int.pow_pos (by decide); omega
    simp only [VInt.cls, n 48, n 41, n 34, n 27, n 20, n 13, n 6, p 6]
    clear n p t c h
    unfold lenOfBits
    by_cases b48 : b ≤ 48 <;> by_cases b41 : b ≤ 41 <;> by_cases b34 : b ≤ 34 <;> by_cases b27 : b ≤ 27 <;>
      by_cases b20 : b ≤ 20 <;> by_cases b13 : b ≤ 13 <;> by_cases b6 : b ≤ 6 <;> simp [b48, b41, b34, b27, b20, b13, b6, VInt.len] <;> omega
  · simp only [hn, ite_false] at t
    have p : ∀ t : Nat, (v < 2 ^ t ↔ b ≤ t) := fun s => by
      rw [← t s, ← c s]; omega
    have n : ∀ t : Nat, ¬ v < -2 ^ t := fun s => by have : (0 : Int) < 2 ^ s := Int.pow_pos (by decide); omega
    simp only [VInt.cls, n 48, n 41, n 34, n 27, n 20, n 13, n 6, p 6, p 13, p 20, p 27, p 34, p 41, p 48,
      ite_false]
    clear n p t c h
    unfold lenOfBits
    by_cases b48 : b ≤ 48 <;> by_cases b41 : b ≤ 41 <;> by_cases b34 : b ≤ 34 <;> by_cases b27 : b ≤ 27 <;>
      by_cases b20 : b ≤ 20 <;> by_cases b13 : b ≤ 13 <;> by_cases b6 : b ≤ 6 <;> simp [b48, b41, b34, b27, b20, b13, b6, VInt.len] <;> omega

/-- `LengthVInt(long)`: the table entry at the leading zero count of the magnitude. -/
theorem length_vint64 {v : Int} {b : Nat} (hv : -2 ^ 63 ≤ v ∧ v < 2 ^ 63) (h : BitLen (magnitude v) b)
    (hb : b ≤ 64) : lzcToVIntLen.getD (64 - b) 0 = VInt.len (VInt.cls v) := by
  rw [class_length hv h]
  clear h
  revert hb
  revert b
  decide +kernel

/-- `LengthVInt(int)`: `((40 − lzc) · 9) >> 6` of the magnitude. -/
theorem length_vint32 {v : Int} {b : Nat} (hv : -2 ^ 31 ≤ v ∧ v < 2 ^ 31) (h : BitLen (magnitude v) b)
    (hb : b ≤ 32) : (40 - (32 - b)) * 9 / 64 = VInt.len (VInt.cls v) := by
  rw [class_length ⟨by omega, by omega⟩ h]
  clear h
  revert hb
  revert b
  decide +kernel

end Btdb.Lengths
