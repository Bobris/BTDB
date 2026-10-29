/-!
# A data block's delta prefix from its restarts

A model of `RawBlock.DeltaPrefix` (`../Format/DataBlock.cs`): the sum of the deltas of the first `count` entries of a
block with a restart every `R` entries. `DataBlockBuilder.Add` stores at each restart the sum of the deltas of
the entries before it (`psum d (r * R)`). `DeltaPrefix` takes the restart of the last counted entry, capped at the last
restart, adds the deltas of the entries from it to `count`, and returns 0 for no entries.

* `delta_prefix_correct`: the result is the sum of the deltas of the first `count` entries.
* `bounds_cover`: the `MinPrefix` and `MaxPrefix` the builder tracks, starting at 0 and updated after each entry,
  bound every prefix sum of the block, the empty one included, which `Table.PointDeltaBounds` relies on.
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

/-- The builder's running delta, minimum and maximum prefix after `c` entries. -/
def build (d : Nat → Int) : Nat → Int × Int × Int
  | 0 => (0, 0, 0)
  | c + 1 =>
    let (delta, lo, hi) := build d c
    let delta' := delta + d c
    (delta', min lo delta', max hi delta')

theorem bounds_cover (d : Nat → Int) (c : Nat) :
    (build d c).1 = psum d c ∧ ∀ j, j ≤ c → (build d c).2.1 ≤ psum d j ∧ psum d j ≤ (build d c).2.2 := by
  induction c with
  | zero =>
    refine ⟨rfl, fun j hj => ?_⟩
    have : j = 0 := by omega
    subst this
    simp [build, psum]
  | succ c ih =>
    obtain ⟨hdelta, hbound⟩ := ih
    simp only [build]
    refine ⟨by rw [hdelta]; rfl, fun j hj => ?_⟩
    rcases Nat.lt_or_ge j (c + 1) with h | h
    · obtain ⟨h1, h2⟩ := hbound j (by omega)
      constructor
      · exact Int.le_trans (Int.min_le_left _ _) h1
      · exact Int.le_trans h2 (Int.le_max_left _ _)
    · have : j = c + 1 := by omega
      subst this
      simp only [psum, ← hdelta]
      exact ⟨Int.min_le_right _ _, Int.le_max_right _ _⟩

end Lsm.BlockDelta
