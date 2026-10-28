import BtdbProofs.VInt

/-!
# Unpacking returns the packed value

Models of `PackUnpack.UnsafeUnpackVUInt` and `UnsafeUnpackVInt` for each length, as the code reads the bytes: big-endian
reads of 1, 2, 4 or 8 bytes, masks, shifts and, for signed values, the subtraction chosen by the top bit of the first
byte. A mask `x & (2^k − 1)` is `x % 2^k` (`mask_mod`).

* `vuint_unpack`: unpacking the encoding of every 64-bit unsigned value returns it.
* `vint_unpack`: unpacking the encoding of every 64-bit signed value returns it.
-/

namespace Btdb.Unpack

open VUInt

theorem mask_mod (x k : Nat) : x &&& (2 ^ k - 1) = x % 2 ^ k := Nat.and_two_pow_sub_one_eq_mod x k

def be16 (a b : Nat) : Nat := a * 256 + b
def be32 (a b c d : Nat) : Nat := ((a * 256 + b) * 256 + c) * 256 + d

/-- `UnsafeUnpackVUInt` by the length its first byte gives. -/
def unpackVUInt : List Nat → Nat
  | [b0] => b0
  | [b0, b1] => be16 b0 b1 % 2 ^ 14
  | [b0, b1, b2] => b0 % 2 ^ 5 * 2 ^ 16 + be16 b1 b2
  | [b0, b1, b2, b3] => be32 b0 b1 b2 b3 % 2 ^ 28
  | [b0, b1, b2, b3, b4] => b0 % 2 ^ 3 * 2 ^ 32 + be32 b1 b2 b3 b4
  | [b0, b1, b2, b3, b4, b5] => be16 b0 b1 % 2 ^ 10 * 2 ^ 32 + be32 b2 b3 b4 b5
  | [b0, b1, b2, b3, b4, b5, b6] => b0 % 2 * 2 ^ 48 + be16 b1 b2 * 2 ^ 32 + be32 b3 b4 b5 b6
  | [b0, b1, b2, b3, b4, b5, b6, b7] => (be32 b0 b1 b2 b3 * 2 ^ 32 + be32 b4 b5 b6 b7) % 2 ^ 56
  | [_, b1, b2, b3, b4, b5, b6, b7, b8] => be32 b1 b2 b3 b4 * 2 ^ 32 + be32 b5 b6 b7 b8
  | _ => 0

/-- `(long)` of a 64-bit unsigned value. -/
def twos (x : Nat) : Int := if x < 2 ^ 63 then x else (x : Int) - 2 ^ 64

/-- `UnsafeUnpackVInt`: the subtracted constant applies when the top bit of the first byte is clear. -/
def unpackVInt : List Nat → Int
  | [b0] => (b0 : Int) - 0x80
  | [b0, b1] => ((be16 b0 b1 % 2 ^ 13 : Nat) : Int) - (if b0 < 128 then 2 ^ 13 else 0)
  | [b0, b1, b2] => ((b0 % 2 ^ 4 * 2 ^ 16 + be16 b1 b2 : Nat) : Int) - (if b0 < 128 then 2 ^ 20 else 0)
  | [b0, b1, b2, b3] => ((be32 b0 b1 b2 b3 % 2 ^ 27 : Nat) : Int) - (if b0 < 128 then 2 ^ 27 else 0)
  | [b0, b1, b2, b3, b4] => ((b0 % 2 ^ 2 * 2 ^ 32 + be32 b1 b2 b3 b4 : Nat) : Int) - (if b0 < 128 then 2 ^ 34 else 0)
  | [b0, b1, b2, b3, b4, b5] =>
    ((be16 b0 b1 % 2 ^ 9 * 2 ^ 32 + be32 b2 b3 b4 b5 : Nat) : Int) - (if b0 < 128 then 2 ^ 41 else 0)
  | [b0, b1, b2, b3, b4, b5, b6] =>
    ((be16 b1 b2 * 2 ^ 32 + be32 b3 b4 b5 b6 : Nat) : Int) - (if b0 < 128 then 2 ^ 48 else 0)
  | [_, b1, b2, b3, b4, b5, b6, b7, b8] => twos (be32 b1 b2 b3 b4 * 2 ^ 32 + be32 b5 b6 b7 b8)
  | _ => 0

theorem vuint_unpack {v : Nat} (hv : v < 2 ^ 64) : unpackVUInt (vuint v) = v := by
  obtain ⟨f1, f2, f3, -, -, f6, f7⟩ := facts v hv
  unfold vuint
  have : len v = 1 ∨ len v = 2 ∨ len v = 3 ∨ len v = 4 ∨ len v = 5 ∨ len v = 6 ∨ len v = 7 ∨ len v = 8 ∨
      len v = 9 := by omega
  rcases this with h | h | h | h | h | h | h | h | h <;> rw [h] at f3 f6 f7 ⊢ <;>
    simp only [be, base, first, unpackVUInt, be16, be32, Nat.reducePow, Nat.reduceAdd, Nat.div_one]
      at f3 f6 f7 ⊢ <;> omega


theorem vint_unpack {v : Int} (hv : -2 ^ 63 ≤ v ∧ v < 2 ^ 63) : unpackVInt (VInt.vint v) = v := by
  obtain ⟨k, hc, hk, h1, h2⟩ := VInt.cls_spec v hv.1
  have hb := (VInt.facts v hv).2.2.1
  have hn := (VInt.facts v hv).2.2.2.1
  unfold VInt.vint VInt.num at *
  rw [hc] at hb hn ⊢
  obtain ⟨N, hN⟩ : ∃ N, (VInt.base k + v).toNat = N := ⟨_, rfl⟩
  have e : (N : Int) = VInt.base k + v := by rw [← hN]; exact Int.toNat_of_nonneg hb
  rw [hN] at hn ⊢
  clear hN hb hc
  have : k = 0 ∨ k = 1 ∨ k = 2 ∨ k = 3 ∨ k = 4 ∨ k = 5 ∨ k = 6 ∨ k = 7 ∨ k = 8 ∨ k = 9 ∨ k = 10 ∨ k = 11 ∨
      k = 12 ∨ k = 13 ∨ k = 14 := by omega
  rcases this with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
    simp only [VInt.len, VInt.base, VInt.first, Nat.reducePow, Nat.reduceAdd, Int.reducePow, Int.reduceNeg]
      at e hn h1 h2 ⊢ <;>
    simp only [be, Nat.reducePow, Nat.div_one] <;> rw [unpackVInt] <;> (try unfold twos) <;> (try unfold be16) <;> (try unfold be32) <;>
    (repeat' split) <;> omega

end Btdb.Unpack
