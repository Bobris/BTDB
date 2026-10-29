/-!
# Descending a read-only database tree ends

A model of the descents of `ReadOnlyKeyValueDBCursor` (`SeekIndex`, `FindBoundary`) and of
`ReadOnlyKeyValueDBTransaction.CalcBTreeStats` (`../../KVDBLayer/ReadOnly/`). Since the 2026-10-29 fix every step
reads a child only when its offset is below its parent's (`Node.ReadChild`), as `ReadOnlyCreator` writes children
before their parents, and only within 64 levels.

* `descent_bounded`: a chain of nodes with strictly decreasing offsets from the root has at most `root + 1` nodes, so
  no damaged pointer makes a descent go round forever, as one pointing back up the tree did before the fix.
* `valid_depth`: the creator builds each level above from groups of at most `m ≥ 2` nodes of the one below, so a tree
  of at most 2^32 leaves (key counts are `uint`) has at most 33 levels, and the 64-level limit refuses no valid image.
-/

namespace Btdb.ReadOnlyDepth

/-- Offsets of nodes on a descent, each below the one before. -/
def Decreasing : List Nat → Prop
  | a :: b :: r => b < a ∧ Decreasing (b :: r)
  | _ => True

theorem descent_bounded : ∀ (root : Nat) (rest : List Nat), Decreasing (root :: rest) → rest.length ≤ root
  | _, [], _ => Nat.zero_le _
  | root, b :: r, ⟨hb, hr⟩ => by
    have := descent_bounded b r hr
    simp only [List.length_cons]
    omega

/-- Levels of a tree over `c` nodes when each level above groups at most `m` of them. -/
def levels (m c : Nat) : Nat :=
  if c ≤ 1 ∨ m < 2 then 1 else 1 + levels m ((c + m - 1) / m)
termination_by c
decreasing_by
  have : (c + m - 1) / m < c := by
    apply Nat.div_lt_of_lt_mul
    have h1 : 2 * c ≤ m * c := Nat.mul_le_mul_right c (by omega)
    have h2 : m * 2 ≤ m * c := Nat.mul_le_mul_left m (by omega)
    omega
  exact this

theorem levels_le (m : Nat) (hm : 2 ≤ m) : ∀ d c, c ≤ 2 ^ d → levels m c ≤ d + 1
  | 0, c, h => by
    rw [levels]; simp at h; simp [show c ≤ 1 by omega]
  | d + 1, c, h => by
    rw [levels]
    by_cases hc : c ≤ 1 ∨ m < 2
    · simp [hc]
    · simp only [hc, ↓reduceIte]
      have hup : (c + m - 1) / m ≤ 2 ^ d := by
        have hlt : (c + m - 1) / m < 2 ^ d + 1 := by
          apply Nat.div_lt_of_lt_mul
          have : 2 ^ (d + 1) = 2 * 2 ^ d := by rw [Nat.pow_succ]; omega
          have : 2 * 2 ^ d ≤ m * 2 ^ d := Nat.mul_le_mul_right _ hm
          rw [Nat.mul_add, Nat.mul_one]
          omega
        omega
      have := levels_le m hm d _ hup
      omega

theorem valid_depth (m leaves : Nat) (hm : 2 ≤ m) (h : leaves ≤ 2 ^ 32) : levels m leaves ≤ 33 ∧ 33 < 64 :=
  ⟨levels_le m hm 32 leaves h, by decide⟩

end Btdb.ReadOnlyDepth
