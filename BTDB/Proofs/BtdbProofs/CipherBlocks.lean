/-!
# Ordered decryption refuses exactly partial blocks

`AesGcmSymmetricCipher.OrderedDecrypt` (`../../Encrypted/AesGcmSymmetricCipher.cs`) decrypts AES-CBC, which works on
whole 16-byte blocks. Since the 2026-10-29 fix it returns false instead of throwing `CryptographicException` when
`(length & 15) != 0`.

* `and15_eq_mod`, `refuses_iff_partial`: `length & 15` is `length % 16`, so the test refuses exactly the lengths that
  are no whole number of blocks, every other one being decrypted
  (`AesGcmSymmetricCipherTest.OrderedDecryptOfPartialBlockFails`).
-/

namespace Btdb.CipherBlocks

theorem and15_eq_mod (length : Nat) : length &&& 15 = length % 16 := by
  have : (15 : Nat) = 2 ^ 4 - 1 := by decide
  rw [this, Nat.and_two_pow_sub_one_eq_mod]

theorem refuses_iff_partial (length : Nat) : (length &&& 15 ≠ 0) ↔ ¬ 16 ∣ length := by
  rw [and15_eq_mod, Nat.dvd_iff_mod_eq_zero]

end Btdb.CipherBlocks
