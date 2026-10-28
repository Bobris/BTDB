import BtdbProofs.VUInt

/-!
# Signed variable-length integers order like their values

A model of `PackUnpack.UnsafePackVInt`, the key encoding of the "Signed" field handler, for every 64-bit value. Values
fall into 15 classes, from the most negative to the most positive. A class has a length (`LengthVInt`: 1 to 7 bytes,
or 9) and its encoding is that many big-endian bytes of `base + v`. For a negative value the code adds the positive
base and XORs mask bits that are all set in the result, which is subtracting them, so negative values of each length
get their own base below the positive ones. Length 9 is a sign byte and the 8 bytes of the two's complement value.
The constants are taken from the code by inspection.

* `vint_order`: a smaller value has a smaller encoding.
* `vint_prefix_free`: no encoding is a proper prefix of another.
-/

namespace Btdb.VInt

open KeyRange VUInt

/-- The class of a value, from the most negative 9-byte values (0) to the most positive ones (14). -/
def cls (v : Int) : Nat :=
  if v < -2 ^ 48 then 0 else if v < -2 ^ 41 then 1 else if v < -2 ^ 34 then 2 else if v < -2 ^ 27 then 3
  else if v < -2 ^ 20 then 4 else if v < -2 ^ 13 then 5 else if v < -2 ^ 6 then 6 else if v < 2 ^ 6 then 7
  else if v < 2 ^ 13 then 8 else if v < 2 ^ 20 then 9 else if v < 2 ^ 27 then 10 else if v < 2 ^ 34 then 11
  else if v < 2 ^ 41 then 12 else if v < 2 ^ 48 then 13 else 14

def len : Nat → Nat
  | 0 => 9 | 1 => 7 | 2 => 6 | 3 => 5 | 4 => 4 | 5 => 3 | 6 => 2 | 7 => 1
  | 8 => 2 | 9 => 3 | 10 => 4 | 11 => 5 | 12 => 6 | 13 => 7 | _ => 9

def base : Nat → Int
  | 0 => 0x10000000000000000 | 1 => 0x2000000000000 | 2 => 0x40000000000 | 3 => 0x800000000
  | 4 => 0x10000000 | 5 => 0x200000 | 6 => 0x4000 | 7 => 0x80
  | 8 => 0xC000 | 9 => 0xE00000 | 10 => 0xF0000000 | 11 => 0xF800000000
  | 12 => 0xFC0000000000 | 13 => 0xFE000000000000 | _ => 0xFF0000000000000000

/-- The range of first bytes of each class. -/
def lo : Nat → Nat
  | 0 => 0 | 1 => 1 | 2 => 2 | 3 => 4 | 4 => 8 | 5 => 16 | 6 => 32 | 7 => 64
  | 8 => 192 | 9 => 224 | 10 => 240 | 11 => 248 | 12 => 252 | 13 => 254 | _ => 255

def hi : Nat → Nat
  | 0 => 1 | 1 => 2 | 2 => 4 | 3 => 8 | 4 => 16 | 5 => 32 | 6 => 64 | 7 => 192
  | 8 => 224 | 9 => 240 | 10 => 248 | 11 => 252 | 12 => 254 | 13 => 255 | _ => 256

/-- The first value of each class. -/
def first : Nat → Int
  | 0 => -2 ^ 63 | 1 => -2 ^ 48 | 2 => -2 ^ 41 | 3 => -2 ^ 34 | 4 => -2 ^ 27 | 5 => -2 ^ 20 | 6 => -2 ^ 13
  | 7 => -2 ^ 6 | 8 => 2 ^ 6 | 9 => 2 ^ 13 | 10 => 2 ^ 20 | 11 => 2 ^ 27 | 12 => 2 ^ 34 | 13 => 2 ^ 41
  | _ => 2 ^ 48

def num (v : Int) : Nat := (base (cls v) + v).toNat

def vint (v : Int) : List Nat := be (num v) (len (cls v))

/-- The class of a value is the one whose range holds it. -/
theorem cls_spec (v : Int) (hv : -2 ^ 63 ≤ v) :
    ∃ k, cls v = k ∧ k ≤ 14 ∧ first k ≤ v ∧ (k < 14 → v < first (k + 1)) := by
  unfold cls
  by_cases c0 : v < -2 ^ 48
  · rw [ite_eq_left c0]; exact ⟨0, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c0]
  by_cases c1 : v < -2 ^ 41
  · rw [ite_eq_left c1]; exact ⟨1, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c1]
  by_cases c2 : v < -2 ^ 34
  · rw [ite_eq_left c2]; exact ⟨2, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c2]
  by_cases c3 : v < -2 ^ 27
  · rw [ite_eq_left c3]; exact ⟨3, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c3]
  by_cases c4 : v < -2 ^ 20
  · rw [ite_eq_left c4]; exact ⟨4, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c4]
  by_cases c5 : v < -2 ^ 13
  · rw [ite_eq_left c5]; exact ⟨5, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c5]
  by_cases c6 : v < -2 ^ 6
  · rw [ite_eq_left c6]; exact ⟨6, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c6]
  by_cases c7 : v < 2 ^ 6
  · rw [ite_eq_left c7]; exact ⟨7, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c7]
  by_cases c8 : v < 2 ^ 13
  · rw [ite_eq_left c8]; exact ⟨8, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c8]
  by_cases c9 : v < 2 ^ 20
  · rw [ite_eq_left c9]; exact ⟨9, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c9]
  by_cases c10 : v < 2 ^ 27
  · rw [ite_eq_left c10]; exact ⟨10, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c10]
  by_cases c11 : v < 2 ^ 34
  · rw [ite_eq_left c11]; exact ⟨11, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c11]
  by_cases c12 : v < 2 ^ 41
  · rw [ite_eq_left c12]; exact ⟨12, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c12]
  by_cases c13 : v < 2 ^ 48
  · rw [ite_eq_left c13]; exact ⟨13, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun _ => by simp only [first, Nat.reduceAdd, Int.reducePow, Int.reduceNeg] at *; omega⟩
  rw [ite_eq_right c13]
  exact ⟨14, rfl, by decide, by simp only [first, Int.reducePow, Int.reduceNeg] at *; omega, fun h => absurd h (by decide)⟩

theorem facts (v : Int) (hv : -2 ^ 63 ≤ v ∧ v < 2 ^ 63) :
    cls v ≤ 14 ∧ 1 ≤ len (cls v) ∧ 0 ≤ base (cls v) + v ∧ num v < 256 ^ len (cls v) ∧
    lo (cls v) * 256 ^ (len (cls v) - 1) ≤ num v ∧ num v < hi (cls v) * 256 ^ (len (cls v) - 1) ∧
    first (cls v) ≤ v ∧ (cls v < 14 → v < first (cls v + 1)) := by
  obtain ⟨k, hc, hk, h1, h2⟩ := cls_spec v hv.1
  unfold num
  rw [hc]
  have : k = 0 ∨ k = 1 ∨ k = 2 ∨ k = 3 ∨ k = 4 ∨ k = 5 ∨ k = 6 ∨ k = 7 ∨ k = 8 ∨ k = 9 ∨ k = 10 ∨ k = 11 ∨
      k = 12 ∨ k = 13 ∨ k = 14 := by omega
  rcases this with rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl | rfl <;>
    simp only [len, base, lo, hi, first, Nat.reducePow, Nat.reduceSub, Nat.reduceMul, Nat.reduceAdd,
      Int.reducePow, Int.reduceNeg] at * <;> omega

theorem first_mono : ∀ a, a < 15 → ∀ b, b < 15 → a ≤ b → first a ≤ first b := by decide

theorem cls_mono {v w : Int} (h : v ≤ w) (hv : -2 ^ 63 ≤ v) (hw : w < 2 ^ 63) : cls v ≤ cls w := by
  obtain ⟨f1, -, -, -, -, -, f7, -⟩ := facts v ⟨hv, by omega⟩
  obtain ⟨g1, -, -, -, -, -, -, g8⟩ := facts w ⟨by omega, hw⟩
  apply Nat.le_of_not_lt
  intro hlt
  have := first_mono (cls w + 1) (by omega) (cls v) (by omega) hlt
  have := g8 (by omega)
  omega

theorem hi_le_lo : ∀ k, k < 15 → ∀ k', k' < 15 → k < k' → hi k ≤ lo k' := by decide

theorem head_bounds (v : Int) (hv : -2 ^ 63 ≤ v ∧ v < 2 ^ 63) :
    lo (cls v) ≤ num v / 256 ^ (len (cls v) - 1) ∧ num v / 256 ^ (len (cls v) - 1) < hi (cls v) := by
  obtain ⟨_, _, _, _, h4, h5, -, -⟩ := facts v hv
  have hP : 0 < 256 ^ (len (cls v) - 1) := Nat.pow_pos (by decide)
  exact ⟨(Nat.le_div_iff_mul_le hP).2 h4, (Nat.div_lt_iff_lt_mul hP).2 h5⟩

theorem vint_order {v w : Int} (h : v < w) (hv : -2 ^ 63 ≤ v) (hw : w < 2 ^ 63) :
    cmp (vint v) (vint w) = .lt := by
  have hv' : -2 ^ 63 ≤ v ∧ v < 2 ^ 63 := ⟨hv, by omega⟩
  have hw' : -2 ^ 63 ≤ w ∧ w < 2 ^ 63 := ⟨by omega, hw⟩
  obtain ⟨fv1, fv2, fv3, fv4, -, -, -, -⟩ := facts v hv'
  obtain ⟨fw1, fw2, fw3, fw4, -, -, -, -⟩ := facts w hw'
  have hl := cls_mono (Int.le_of_lt h) hv hw
  unfold vint
  by_cases he : cls v = cls w
  · have hn : num v < num w := by unfold num; rw [he] at fv3 ⊢; omega
    rw [he] at fv4 ⊢
    exact be_cmp _ _ _ hn fw4
  · obtain ⟨tv, hv''⟩ := head_eq (N := num v) fv2
    obtain ⟨tw, hw''⟩ := head_eq (N := num w) fw2
    have bv := head_bounds v hv'
    have bw := head_bounds w hw'
    have := hi_le_lo (cls v) (by omega) (cls w) (by omega) (by omega)
    rw [hv'', hw'']
    simp only [cmp]
    have : num v / 256 ^ (len (cls v) - 1) < num w / 256 ^ (len (cls w) - 1) := by omega
    simp [this]

theorem vint_prefix_free {v w : Int} (hv : -2 ^ 63 ≤ v ∧ v < 2 ^ 63) (hw : -2 ^ 63 ≤ w ∧ w < 2 ^ 63)
    (h : vint v <+: vint w) : v = w := by
  obtain ⟨fv1, fv2, fv3, fv4, -, -, -, -⟩ := facts v hv
  obtain ⟨fw1, fw2, fw3, fw4, -, -, -, -⟩ := facts w hw
  unfold vint at h
  by_cases he : cls v = cls w
  · rw [he] at h fv4
    have := h.eq_of_length (by rw [be_length, be_length])
    have := be_inj _ _ _ fv4 fw4 this
    unfold num at this
    rw [he] at fv3 this
    omega
  · obtain ⟨tv, hv'⟩ := head_eq (N := num v) fv2
    obtain ⟨tw, hw'⟩ := head_eq (N := num w) fw2
    rw [hv', hw', List.cons_prefix_cons] at h
    have bv := head_bounds v hv
    have bw := head_bounds w hw
    rcases Nat.lt_or_gt_of_ne he with hlt | hgt
    · have := hi_le_lo (cls v) (by omega) (cls w) (by omega) hlt; omega
    · have := hi_le_lo (cls w) (by omega) (cls v) (by omega) hgt; omega

end Btdb.VInt
