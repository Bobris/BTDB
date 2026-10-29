import BtdbProofs.Fletcher

/-!
# Fletcher-32 catches every flipped bit

`Checksum.CalcFletcher32` (`../../Buffer/Checksum.cs`) guards each block of the event store (`AppendingEventStore`,
`ReadOnlyEventStore`). Its low half, `sum1`, starts at `0xffff`, adds every 16-bit word, the odd last byte too, and is
folded after each run of words and at the end; folding keeps it modulo 65535 (`Fletcher.fold_mod`).

* `sum1_mod`: the final `sum1` is the start plus all words, modulo 65535.
* `flip_detected`: flipping one bit of a word or of the odd byte changes the total by `2^k`, `k < 16`, which is no
  multiple of 65535, so `sum1` and with it the checksum changes, and the reader refuses the block
  (`EventStoreFileCoverageTest.DamagedStoreReadsOnlyPrefixOfStoredEvents`).
-/

namespace Btdb.FletcherDetect

open Btdb.Fletcher

/-- `sum1` over runs of words, folded after each and once more at the end; the odd byte is a last run of one. -/
def sum1 : Nat → List (List Nat) → Nat
  | s, [] => fold s
  | s, run :: runs => sum1 (fold (s + run.sum)) runs

def total (runs : List (List Nat)) : Nat := (runs.map List.sum).sum

theorem sum1_mod : ∀ (s : Nat) (runs : List (List Nat)), sum1 s runs % 65535 = (s + total runs) % 65535
  | s, [] => by simp [sum1, total, fold_mod]
  | s, run :: runs => by
    simp only [sum1, total, List.map_cons, List.sum_cons]
    rw [sum1_mod, Nat.add_mod, fold_mod, ← Nat.add_mod]
    simp only [total]
    rw [Nat.add_assoc]

theorem flip_detected (runs runs' : List (List Nat)) (k : Nat) (hk : k < 16)
    (h : total runs' = total runs + 2 ^ k ∨ total runs = total runs' + 2 ^ k) : sum1 65535 runs ≠ sum1 65535 runs' := by
  intro heq
  have h1 := sum1_mod 65535 runs
  have h2 := sum1_mod 65535 runs'
  rw [heq] at h1
  have hp1 : 1 ≤ 2 ^ k := Nat.one_le_two_pow
  have hp2 : 2 ^ k ≤ 2 ^ 15 := Nat.pow_le_pow_right (by decide) (by omega)
  have : (2 : Nat) ^ 15 = 32768 := by decide
  cases h with
  | inl h => rw [h] at h2; omega
  | inr h => rw [h] at h1; omega

end Btdb.FletcherDetect
