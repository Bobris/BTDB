/-!
# Fletcher-32 sums never overflow 32 bits

`Checksum.CalcFletcher32` (`../Buffer/Checksum.cs`) adds 16-bit words to `sum1` and `sum1` to `sum2` in `uint`, folding
both sums (`fold x = (x & 0xffff) + (x >> 16)`) after each block of at most 360 words; the textbook bound is 359. The sums
start at `0xffff`. Here sums are natural numbers, so a bound below 2^32 shows the `uint` arithmetic never wraps; the sums
only grow within a block, so the bound at its end covers every step.

* `block_fits`: a block from folded sums keeps both sums below 2^32, and folding returns them within the bounds a block
  starts from (`sum1 ≤ 65896`, `sum2 ≤ 131070`).
* `odd_fits`: the last odd byte keeps the sums below 2^32 and within the bounds.
* `result_fits`: the final fold leaves both sums in 16 bits, so `sum2 << 16 | sum1` keeps them apart.
* `fold_mod`: folding keeps a sum modulo 65535, so it does not change the checksum.
-/

namespace Btdb.Fletcher

def fold (x : Nat) : Nat := x % 65536 + x / 65536

def MaxSum1 : Nat := 65896
def MaxSum2 : Nat := 131070

/-- One block: each word is added to `sum1`, then `sum1` to `sum2`. -/
def block : Nat → Nat → List Nat → Nat × Nat
  | s1, s2, [] => (s1, s2)
  | s1, s2, w :: ws => block (s1 + w) (s2 + (s1 + w)) ws

/-- The bound of `sum2` after `k` words of a block. -/
def bound2 : Nat → Nat
  | 0 => MaxSum2
  | k + 1 => bound2 k + MaxSum1 + 65535 * (k + 1)

theorem block_bound (ws : List Nat) (k s1 s2 : Nat) (hw : ∀ w ∈ ws, w ≤ 65535)
    (h1 : s1 ≤ MaxSum1 + 65535 * k) (h2 : s2 ≤ bound2 k) :
    (block s1 s2 ws).1 ≤ MaxSum1 + 65535 * (k + ws.length) ∧ (block s1 s2 ws).2 ≤ bound2 (k + ws.length) := by
  induction ws generalizing k s1 s2 with
  | nil => exact ⟨h1, h2⟩
  | cons w ws ih =>
    have hw1 := hw w List.mem_cons_self
    have h1' : s1 + w ≤ MaxSum1 + 65535 * (k + 1) := by omega
    have h2' : s2 + (s1 + w) ≤ bound2 (k + 1) := by simp only [bound2]; omega
    have := ih (k + 1) (s1 + w) (s2 + (s1 + w)) (fun x hx => hw x (List.mem_cons_of_mem _ hx)) h1' h2'
    simp only [block, List.length_cons]
    rw [show k + (ws.length + 1) = k + 1 + ws.length by omega]
    exact this

theorem bound2_mono (a b : Nat) (h : a ≤ b) : bound2 a ≤ bound2 b := by
  induction b with
  | zero => rw [Nat.le_zero.1 h]; exact Nat.le_refl _
  | succ b ih =>
    rcases Nat.lt_or_eq_of_le h with h | h
    · have := ih (by omega); simp only [bound2]; omega
    · rw [h]; exact Nat.le_refl _

set_option maxRecDepth 100000 in
theorem bound2_360 : bound2 360 < 2 ^ 32 := by decide

theorem block_fits (ws : List Nat) (s1 s2 : Nat) (hw : ∀ w ∈ ws, w ≤ 65535) (hl : ws.length ≤ 360)
    (h1 : s1 ≤ MaxSum1) (h2 : s2 ≤ MaxSum2) :
    (block s1 s2 ws).1 < 2 ^ 32 ∧ (block s1 s2 ws).2 < 2 ^ 32 ∧
      fold (block s1 s2 ws).1 ≤ MaxSum1 ∧ fold (block s1 s2 ws).2 ≤ MaxSum2 := by
  obtain ⟨b1, b2⟩ := block_bound ws 0 s1 s2 hw (by omega) (by simpa [bound2] using h2)
  have hm := bound2_mono (0 + ws.length) 360 (by omega)
  have h360 := bound2_360
  unfold MaxSum1 MaxSum2 fold at *
  refine ⟨by omega, by omega, by omega, by omega⟩

theorem odd_fits (b s1 s2 : Nat) (hb : b ≤ 255) (h1 : s1 ≤ MaxSum1) (h2 : s2 ≤ MaxSum2) :
    s1 + b < 2 ^ 32 ∧ s2 + (s1 + b) < 2 ^ 32 ∧ fold (s1 + b) ≤ MaxSum1 ∧ fold (s2 + (s1 + b)) ≤ MaxSum2 := by
  unfold MaxSum1 MaxSum2 fold at *
  refine ⟨by omega, by omega, by omega, by omega⟩

theorem result_fits (s1 s2 : Nat) (h1 : s1 ≤ MaxSum1) (h2 : s2 ≤ MaxSum2) : fold s1 < 65536 ∧ fold s2 < 65536 := by
  unfold MaxSum1 MaxSum2 fold at *
  omega

theorem fold_mod (x : Nat) : fold x % 65535 = x % 65535 := by
  unfold fold
  omega

end Btdb.Fletcher
