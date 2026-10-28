/-!
# Reading a length-prefixed byte array allocates for the input it finds

A model of `MemReader.ReadByteArray` when the declared length `L` exceeds the `b` buffered bytes. The input holds `A`
more bytes. The loop allocates `s = min L (max 4096 b)` bytes and reads them, failing at the end of the input when
`A < s`; otherwise it returns when `s = L`, or else copies into `min L (2 s)` bytes and reads on. Before the
2026-10-28 fix the reader allocated `L` bytes at once, whatever the input held.

* `outcome_iff`, `fixed_returns_iff`: the loop returns the array exactly when the input holds the declared length.
* `alloc_bound`, `fixed_alloc_bound`: it allocates at most `4 · max A 4096` bytes in all, whatever `L` declares.
* `previous_unbounded`: allocating the declared length exceeds every such bound for some prefix.
-/

namespace Btdb.GrowRead

/-- True when the loop returns the array; false when a read reaches the end of the input. -/
def outcome (L A s : Nat) : Bool :=
  if A < s then false
  else if s < L ∧ 0 < s then outcome L A (min L (2 * s)) else true
termination_by L - s
decreasing_by omega

/-- The bytes the loop allocates, including the array whose read fails. -/
def alloc (L A s : Nat) : Nat :=
  if A < s then s
  else if s < L ∧ 0 < s then s + alloc L A (min L (2 * s)) else s
termination_by L - s
decreasing_by omega

theorem outcome_iff (L A s : Nat) (h0 : 0 < s) (hs : s ≤ L) : outcome L A s = true ↔ L ≤ A := by
  rw [outcome]
  by_cases hA : A < s
  · simp only [hA, ite_true]
    constructor
    · intro h; cases h
    · intro h; omega
  · by_cases hL : s < L
    · have ih := outcome_iff L A (min L (2 * s)) (by omega) (by omega)
      simp only [hA, ite_false, hL, h0, and_self, ite_true]
      exact ih
    · simp only [hA, ite_false, hL, false_and, ite_false]
      constructor
      · intro _; omega
      · intro _; trivial
termination_by L - s
decreasing_by omega

theorem alloc_bound (L A s : Nat) (h0 : 0 < s) (hs : s ≤ L) :
    alloc L A s ≤ if A < s then s else 4 * A - s := by
  rw [alloc]
  by_cases hA : A < s
  · simp only [hA, ite_true]; omega
  · simp only [hA, ite_false]
    by_cases hL : s < L
    · simp only [hL, h0, and_self, ite_true]
      have ih := alloc_bound L A (min L (2 * s)) (by omega) (by omega)
      by_cases hA' : A < min L (2 * s)
      · simp only [hA', ite_true] at ih; omega
      · simp only [hA', ite_false] at ih
        by_cases hcap : min L (2 * s) = L
        · -- The capped array is the last: the loop allocates it and returns.
          have last : alloc L A (min L (2 * s)) = min L (2 * s) := by
            have hn : ¬(min L (2 * s) < L ∧ 0 < min L (2 * s)) := by omega
            rw [alloc]; simp only [hA', hn, ite_false]
          omega
        · omega
    · simp only [hL, false_and, ite_false]; omega
termination_by L - s
decreasing_by omega

/-- The first array: the buffered bytes, at least 4096, at most the declared length. -/
def first (L b : Nat) : Nat := min L (max 4096 b)

theorem fixed_returns_iff (L A b : Nat) (hL : 0 < L) : outcome L A (first L b) = true ↔ L ≤ A :=
  outcome_iff L A (first L b) (by unfold first; omega) (by unfold first; omega)

theorem fixed_alloc_bound (L A b : Nat) (hL : 0 < L) (hb : b ≤ A) : alloc L A (first L b) ≤ 4 * max A 4096 := by
  have h := alloc_bound L A (first L b) (by unfold first; omega) (by unfold first; omega)
  unfold first at h ⊢
  by_cases hA : A < min L (max 4096 b)
  · simp only [hA, ite_true] at h; omega
  · simp only [hA, ite_false] at h; omega

/-- Before the fix: the declared length, allocated at once. -/
def previousAlloc (L : Nat) : Nat := L

theorem previous_unbounded (A : Nat) : ∃ L, 4 * max A 4096 < previousAlloc L :=
  ⟨4 * max A 4096 + 1, by unfold previousAlloc; omega⟩

end Btdb.GrowRead
