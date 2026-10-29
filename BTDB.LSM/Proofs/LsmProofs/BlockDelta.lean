/-!
# A data block's delta prefix from its restarts

A model of `RawBlock.DeltaPrefix` (`../Format/DataBlock.cs`): the sum of the deltas of the first `count` entries of a
block with a restart every `R` entries. `DataBlockBuilder.Add` stores at each restart the sum of the deltas of
the entries before it (`psum d (r * R)`). `DeltaPrefix` takes the restart of the last counted entry, capped at the last
restart, adds the deltas of the entries from it to `count`, and returns 0 for no entries.

* `delta_prefix_correct`: the result is the sum of the deltas of the first `count` entries.
-/

namespace Lsm.BlockDelta

/-- Sum of the deltas of entries below `m`. -/
def psum (d : Nat → Int) : Nat → Int
  | 0 => 0
  | m + 1 => psum d m + d m

/-- Sum of the deltas of entries in `[a, b)`, as the decoding loop adds them. -/
def rangeSum (d : Nat → Int) (a b : Nat) : Int :=
  if a < b then d a + rangeSum d (a + 1) b else 0
termination_by b - a

theorem psum_split (d : Nat → Int) (a k : Nat) : psum d (a + k) = psum d a + rangeSum d a (a + k) := by
  induction k generalizing a with
  | zero =>
    unfold rangeSum
    simp
  | succ k ih =>
    rw [show a + (k + 1) = a + 1 + k by omega, ih (a + 1)]
    conv => rhs; unfold rangeSum
    rw [ite_eq_left (by omega)]
    simp only [psum]
    omega

def deltaPrefix (d : Nat → Int) (R rc count : Nat) : Int :=
  if count = 0 then 0
  else
    let r := min ((count - 1) / R) (rc - 1)
    psum d (r * R) + rangeSum d (r * R) count

theorem delta_prefix_correct (d : Nat → Int) (R rc count : Nat) :
    deltaPrefix d R rc count = psum d count := by
  unfold deltaPrefix
  split
  · next h => subst h; rfl
  · next h =>
    dsimp only
    -- The restart is at or before the last counted entry.
    have hle : min ((count - 1) / R) (rc - 1) * R ≤ count := by
      have h1 : min ((count - 1) / R) (rc - 1) * R ≤ (count - 1) / R * R :=
        Nat.mul_le_mul_right R (Nat.min_le_left _ _)
      have h2 : (count - 1) / R * R ≤ count - 1 := Nat.div_mul_le_self _ _
      omega
    generalize min ((count - 1) / R) (rc - 1) * R = a at hle
    obtain ⟨k, rfl⟩ : ∃ k, count = a + k := ⟨count - a, by omega⟩
    rw [psum_split]

end Lsm.BlockDelta
