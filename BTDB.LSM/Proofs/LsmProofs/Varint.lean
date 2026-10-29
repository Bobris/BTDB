/-!
# LSM variable-length integers

A model of `LsmEncoder.WriteVUInt64` and `LsmDecoder.ReadVUInt64` (`../Format/LsmEncoding.cs`): LEB128, seven bits
per byte, low bits first, the top bit set on every byte but the last. Byte `j`, counted from 0, carries bits
`7j … 7j + 6`, so the decoded value is the low seven bits of the first byte plus 128 times the value of the rest. The
tenth byte (`j = 9`) carries only bit 63; since the 2026-10-29 fix the decoder rejects it when it is above 1.

* `round_trip`: every value below 2^64 decodes back from its encoding, leaving the bytes after it.
* `decoded_fits`: every value the decoder accepts is below 2^64, so `ulong` holds it without losing bits.
-/

namespace Lsm.Varint

def encode (v : Nat) : List Nat :=
  if v < 128 then [v] else (v % 128 + 128) :: encode (v / 128)
termination_by v
decreasing_by omega

/-- Decodes from byte position `j`, returning the value of the bytes from there and the bytes after it. -/
def decode (j : Nat) : List Nat → Option (Nat × List Nat)
  | [] => none
  | b :: bs =>
    if j = 9 ∧ 1 < b then none
    else if b < 128 then some (b, bs)
    else match decode (j + 1) bs with
      | some (v, r) => some (b - 128 + 128 * v, r)
      | none => none

theorem pow_step (j : Nat) (hj : j < 9) : 2 ^ (64 - 7 * j) = 128 * 2 ^ (64 - 7 * (j + 1)) := by
  rw [show 64 - 7 * j = 7 + (64 - 7 * (j + 1)) by omega, Nat.pow_add]

theorem round_trip_from (v j : Nat) (r : List Nat) (hj : j ≤ 9) (hv : v < 2 ^ (64 - 7 * j)) :
    decode j (encode v ++ r) = some (v, r) := by
    rw [encode]
    by_cases hsmall : v < 128
    · simp only [hsmall, ite_true, List.singleton_append, decode]
      have : ¬ (j = 9 ∧ 1 < v) := by
        rintro ⟨rfl, h⟩
        have : v < 2 := by simpa using hv
        omega
      simp [this]
    · simp only [hsmall, ite_false, List.cons_append, decode]
      have hj9 : j < 9 := by
        rcases Nat.lt_or_ge j 9 with h | h
        · exact h
        · have : j = 9 := by omega
          subst this
          have : v < 2 := by simpa using hv
          omega
      have hcheck : ¬ (j = 9 ∧ 1 < v % 128 + 128) := by omega
      have hbig : ¬ v % 128 + 128 < 128 := by omega
      have hrest : v / 128 < 2 ^ (64 - 7 * (j + 1)) := by
        rw [pow_step j hj9] at hv
        omega
      rw [ite_eq_right hcheck, ite_eq_right hbig, round_trip_from (v / 128) (j + 1) r (by omega) hrest]
      simp only [Option.some.injEq, Prod.mk.injEq, and_true]
      omega
termination_by v
decreasing_by omega

theorem round_trip (v : Nat) (r : List Nat) (hv : v < 2 ^ 64) : decode 0 (encode v ++ r) = some (v, r) :=
  round_trip_from v 0 r (by omega) hv

theorem decoded_fits_from (bs : List Nat) (j v : Nat) (r : List Nat) (hj : j ≤ 9)
    (hbytes : ∀ b ∈ bs, b < 256) (h : decode j bs = some (v, r)) : v < 2 ^ (64 - 7 * j) := by
  induction bs generalizing j v r with
  | nil => simp [decode] at h
  | cons b bs ih =>
    have hb : b < 256 := hbytes b List.mem_cons_self
    simp only [decode] at h
    by_cases hcheck : j = 9 ∧ 1 < b
    · simp [hcheck] at h
    · rw [ite_eq_right hcheck] at h
      by_cases hsmall : b < 128
      · rw [ite_eq_left hsmall] at h
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        obtain ⟨rfl, _⟩ := h
        rcases Nat.lt_or_ge j 9 with hj9 | hj9
        · rw [pow_step j hj9]
          have : 1 ≤ 2 ^ (64 - 7 * (j + 1)) := Nat.one_le_two_pow
          omega
        · have : j = 9 := by omega
          subst this
          have : b ≤ 1 := by omega
          simp
          omega
      · rw [ite_eq_right hsmall] at h
        have hj9 : j < 9 := by omega
        split at h
        · next v' r' hrec =>
          simp only [Option.some.injEq, Prod.mk.injEq] at h
          obtain ⟨rfl, _⟩ := h
          have := ih (j + 1) v' r' (by omega) (fun c hc => hbytes c (List.mem_cons_of_mem _ hc)) hrec
          rw [pow_step j hj9]
          omega
        · simp at h

theorem decoded_fits (bs : List Nat) (v : Nat) (r : List Nat) (hbytes : ∀ b ∈ bs, b < 256)
    (h : decode 0 bs = some (v, r)) : v < 2 ^ 64 :=
  decoded_fits_from bs 0 v r (by omega) hbytes h

end Lsm.Varint
