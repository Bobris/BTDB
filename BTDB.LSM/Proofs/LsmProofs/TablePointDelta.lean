import LsmProofs.BlockSeek
import LsmProofs.BlockDelta

/-!
# A table's delta before a key from one block

A model of `Table.PointDeltaBefore` (`../Format/Table.cs`). A table's `N` entries are sorted; `q i` says whether entry
`i` is at or after the key, so it is monotone, and the entries below the key are the first `scan q 0 N` ones. The
index stores the delta before each block. `PointDeltaBefore` takes the last block whose first key is at or before the
key, `[a, e)`: every entry before it is below the key, and the next block, if any, starts at or after the key. It adds
to the block's delta before the deltas of its entries below the key, those before `LowerBound(key, inclusive)`
(`BlockSeek.scan q a e`), which `DeltaPrefix` sums (`BlockDelta.delta_prefix_correct`). When the block starts at the
key, `LowerBound` is the block start and nothing is added, as the shortcut reading no block returns.

* `point_delta_before_correct`: the result is the sum of the deltas of all entries below the key.
-/

namespace Lsm.TablePointDelta

open Lsm.BlockSeek Lsm.BlockDelta

theorem point_delta_before_correct (q : Nat → Bool) (d : Nat → Int) (N a e : Nat) (hae : a ≤ e) (heN : e ≤ N)
    (hbefore : ∀ i, i < a → q i = false) (hnext : e < N → q e = true) :
    psum d a + rangeSum d a (scan q a e) = psum d (scan q 0 N) := by
  -- The first entry at or after the key is the first one in the block, or the next block's first entry.
  have hsame : scan q a e = scan q 0 N := by
    apply first_unique q 0 N _ _ _ (scan_first q 0 N (Nat.zero_le _))
    obtain ⟨b1, b2, b3, b4⟩ := scan_first q a e hae
    refine ⟨Nat.zero_le _, by omega, fun k _ hk => ?_, fun hk => ?_⟩
    · rcases Nat.lt_or_ge k a with h | h
      · exact hbefore k h
      · exact b3 k h hk
    · rcases Nat.lt_or_ge (scan q a e) e with h | h
      · exact b4 h
      · have : scan q a e = e := by omega
        rw [this] at hk ⊢
        exact hnext hk
  obtain ⟨b1, _, _, _⟩ := scan_first q a e hae
  obtain ⟨k, hk⟩ : ∃ k, scan q a e = a + k := ⟨scan q a e - a, by omega⟩
  rw [← hsame, hk, psum_split]

end Lsm.TablePointDelta
