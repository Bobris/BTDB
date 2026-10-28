import BtdbProofs.KeyRange

/-!
# Unsigned variable-length integers order like their values

A model of `PackUnpack.UnsafePackVUInt`, the key encoding of the "Unsigned" field handler. A value of length `L ≤ 8`
is `L` big-endian bytes of `base L + v`, whose top `L` bits are `L - 1` ones and a zero; length 9 is `0xFF` and 8
big-endian bytes of the value.

* `vuint_order`: a smaller value has a smaller encoding.
* `vuint_prefix_free`: no encoding is a proper prefix of another.

Together they are what `KeyRange.Encoding` assumes of a key encoding.
-/

namespace Btdb.VUInt

open KeyRange

/-- `n` big-endian base-256 digits of `a`. -/
def be : Nat → Nat → List Nat
  | _, 0 => []
  | a, n + 1 => a / 256 ^ n :: be (a % 256 ^ n) n

theorem be_length : ∀ n a, (be a n).length = n
  | 0, _ => rfl
  | n + 1, a => by simp [be, be_length n]

theorem be_cmp : ∀ n a b, a < b → b < 256 ^ n → cmp (be a n) (be b n) = .lt
  | 0, a, b, h, hb => by simp at hb; omega
  | n + 1, a, b, h, hb => by
    have hP : 0 < 256 ^ n := Nat.pow_pos (by decide)
    have ha := Nat.div_add_mod a (256 ^ n)
    have hb' := Nat.div_add_mod b (256 ^ n)
    have hq : a / 256 ^ n ≤ b / 256 ^ n := Nat.div_le_div_right (Nat.le_of_lt h)
    simp only [be, cmp]
    by_cases hlt : a / 256 ^ n < b / 256 ^ n
    · simp [hlt]
    · have he : a / 256 ^ n = b / 256 ^ n := by omega
      have hr : a % 256 ^ n < b % 256 ^ n := by
        rw [he] at ha
        generalize 256 ^ n * (b / 256 ^ n) = t at ha hb'
        omega
      simp only [he, Nat.lt_irrefl, ite_false]
      exact be_cmp n _ _ hr (Nat.mod_lt _ hP)

theorem be_inj : ∀ n a b, a < 256 ^ n → b < 256 ^ n → be a n = be b n → a = b
  | 0, a, b, ha, hb, _ => by simp at ha hb; omega
  | n + 1, a, b, _, _, h => by
    have hP : 0 < 256 ^ n := Nat.pow_pos (by decide)
    simp only [be, List.cons.injEq] at h
    have hr := be_inj n _ _ (Nat.mod_lt _ hP) (Nat.mod_lt _ hP) h.2
    have ha := Nat.div_add_mod a (256 ^ n)
    have hb := Nat.div_add_mod b (256 ^ n)
    rw [h.1, hr] at ha
    omega

/-- `PackUnpack.LengthVUInt`. -/
def len (v : Nat) : Nat :=
  if v < 2 ^ 7 then 1 else if v < 2 ^ 14 then 2 else if v < 2 ^ 21 then 3 else if v < 2 ^ 28 then 4
  else if v < 2 ^ 35 then 5 else if v < 2 ^ 42 then 6 else if v < 2 ^ 49 then 7 else if v < 2 ^ 56 then 8 else 9

/-- The length marker in the top bits, as `UnsafePackVUInt` adds it. -/
def base : Nat → Nat
  | 1 => 0
  | 2 => 0x8000
  | 3 => 0xC00000
  | 4 => 0xE0000000
  | 5 => 0xF000000000
  | 6 => 0xF80000000000
  | 7 => 0xFC000000000000
  | 8 => 0xFE00000000000000
  | _ => 0xFF0000000000000000

/-- The range of first bytes of each length. -/
def lo : Nat → Nat
  | 1 => 0 | 2 => 128 | 3 => 192 | 4 => 224 | 5 => 240 | 6 => 248 | 7 => 252 | 8 => 254 | _ => 255

def hi : Nat → Nat
  | 1 => 128 | 2 => 192 | 3 => 224 | 4 => 240 | 5 => 248 | 6 => 252 | 7 => 254 | 8 => 255 | _ => 256

/-- The values of each length start here. -/
def first : Nat → Nat
  | 1 => 0 | 2 => 2 ^ 7 | 3 => 2 ^ 14 | 4 => 2 ^ 21 | 5 => 2 ^ 28 | 6 => 2 ^ 35 | 7 => 2 ^ 42 | 8 => 2 ^ 49
  | _ => 2 ^ 56

def vuint (v : Nat) : List Nat := be (base (len v) + v) (len v)

theorem facts (v : Nat) (hv : v < 2 ^ 64) :
    1 ≤ len v ∧ len v ≤ 9 ∧ base (len v) + v < 256 ^ len v ∧
    lo (len v) * 256 ^ (len v - 1) ≤ base (len v) + v ∧ base (len v) + v < hi (len v) * 256 ^ (len v - 1) ∧
    first (len v) ≤ v ∧ (len v < 9 → v < first (len v + 1)) := by
  unfold len
  repeat' split
  all_goals simp only [base, lo, hi, first, Nat.reducePow, Nat.reduceSub, Nat.reduceMul, Nat.reduceAdd] at *
  all_goals omega

theorem first_mono : ∀ a, a < 10 → ∀ b, b < 10 → 1 ≤ a → a ≤ b → first a ≤ first b := by decide

theorem len_mono {v w : Nat} (h : v ≤ w) (hw : w < 2 ^ 64) : len v ≤ len w := by
  obtain ⟨f1, f2, -, -, -, f6, -⟩ := facts v (by omega)
  obtain ⟨g1, g2, -, -, -, -, g7⟩ := facts w hw
  apply Nat.le_of_not_lt
  intro hlt
  have := first_mono (len w + 1) (by omega) (len v) (by omega) (by omega) hlt
  have := g7 (by omega)
  omega

theorem hi_le_lo : ∀ L, L < 10 → ∀ L', L' < 10 → 1 ≤ L → L < L' → hi L ≤ lo L' := by decide

theorem head_eq {N L : Nat} (h : 1 ≤ L) : ∃ t, be N L = N / 256 ^ (L - 1) :: t := by
  cases L with
  | zero => omega
  | succ n => exact ⟨_, rfl⟩

theorem head_bounds (v : Nat) (hv : v < 2 ^ 64) :
    lo (len v) ≤ (base (len v) + v) / 256 ^ (len v - 1) ∧ (base (len v) + v) / 256 ^ (len v - 1) < hi (len v) := by
  obtain ⟨_, _, _, h4, h5, -, -⟩ := facts v hv
  have hP : 0 < 256 ^ (len v - 1) := Nat.pow_pos (by decide)
  exact ⟨(Nat.le_div_iff_mul_le hP).2 h4, (Nat.div_lt_iff_lt_mul hP).2 h5⟩

theorem vuint_order {v w : Nat} (h : v < w) (hw : w < 2 ^ 64) : cmp (vuint v) (vuint w) = .lt := by
  have hv : v < 2 ^ 64 := by omega
  obtain ⟨fv1, fv2, fv3, -, -, -, -⟩ := facts v hv
  obtain ⟨fw1, fw2, fw3, -, -, -, -⟩ := facts w hw
  have hl := len_mono (Nat.le_of_lt h) hw
  unfold vuint
  by_cases he : len v = len w
  · rw [he] at fv3 ⊢
    exact be_cmp _ _ _ (by omega) fw3
  · obtain ⟨tv, hv'⟩ := head_eq (N := base (len v) + v) fv1
    obtain ⟨tw, hw'⟩ := head_eq (N := base (len w) + w) fw1
    have bv := head_bounds v hv
    have bw := head_bounds w hw
    have := hi_le_lo (len v) (by omega) (len w) (by omega) fv1 (by omega)
    rw [hv', hw']
    simp only [cmp]
    have : (base (len v) + v) / 256 ^ (len v - 1) < (base (len w) + w) / 256 ^ (len w - 1) := by omega
    simp [this]

theorem vuint_prefix_free {v w : Nat} (hv : v < 2 ^ 64) (hw : w < 2 ^ 64) (h : vuint v <+: vuint w) : v = w := by
  obtain ⟨fv1, fv2, fv3, -, -, -, -⟩ := facts v hv
  obtain ⟨fw1, fw2, fw3, -, -, -, -⟩ := facts w hw
  unfold vuint at h
  by_cases he : len v = len w
  · rw [he] at h fv3
    have := h.eq_of_length (by rw [be_length, be_length])
    have := be_inj _ _ _ fv3 fw3 this
    omega
  · obtain ⟨tv, hv'⟩ := head_eq (N := base (len v) + v) fv1
    obtain ⟨tw, hw'⟩ := head_eq (N := base (len w) + w) fw1
    rw [hv', hw', List.cons_prefix_cons] at h
    have bv := head_bounds v hv
    have bw := head_bounds w hw
    rcases Nat.lt_or_gt_of_ne he with hlt | hgt
    · have := hi_le_lo (len v) (by omega) (len w) (by omega) fv1 hlt; omega
    · have := hi_le_lo (len w) (by omega) (len v) (by omega) fw1 hgt; omega

/-- The "Unsigned" encoding, for unsigned enums too, satisfies what `KeyRange` assumes. -/
theorem encoding : Encoding (fun v => 0 ≤ v ∧ v < 2 ^ 64) (fun v => vuint v.toNat) where
  prefixFree v w dv dw h := by
    have := vuint_prefix_free (by omega) (by omega) h
    omega
  order v w dv dw h := vuint_order (by omega) (by omega)

end Btdb.VUInt
