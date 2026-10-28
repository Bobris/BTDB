/-!
# Reading a length-prefixed byte array allocates for the input it finds

A model of `MemReader.ReadByteArray` when the declared length `L` exceeds the `b` buffered bytes. The input holds `A`
more bytes. The loop allocates `s = min L (max 4096 b)` bytes and reads them, failing at the end of the input when
`A < s`; otherwise it returns when `s = L`, or else copies into `min L (2 s)` bytes and reads on. Before the
2026-10-28 fix the reader allocated `L` bytes at once, whatever the input held.

* `outcome_iff`, `fixed_returns_iff`: the loop returns the array exactly when the input holds the declared length.
* `alloc_bound`, `fixed_alloc_bound`: it allocates at most `4 · max A 4096` bytes in all, whatever `L` declares.
* `previous_unbounded`: allocating the declared length exceeds every such bound for some prefix.

`ReadStringBeyondBuffer` does the same for strings. It starts with `min L (max 256 b)` characters. Each read decodes
one or two UTF-16 units (a surrogate pair) from at least as many bytes, and fails before allocating when they would
pass `L`. When they do not fit, the array grows to `min L (max (2 · cap) (i + count))`.

* `string_alloc_bound`: decoding characters from `A` bytes allocates at most `6 · max A 256` characters in all.
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

/-- The string loop's array length, characters allocated in all, and characters filled. -/
structure St where
  cap : Nat
  total : Nat
  filled : Nat

/-- Decode `count` units: grow first when they do not fit. -/
def step (L : Nat) (st : St) (count : Nat) : St :=
  if st.cap - st.filled < count then
    let cap := min L (max (2 * st.cap) (st.filled + count))
    ⟨cap, st.total + cap, st.filled + count⟩
  else ⟨st.cap, st.total, st.filled + count⟩

def run (L : Nat) : St → List Nat → St
  | st, [] => st
  | st, count :: rest => run L (step L st count) rest

/-- The array holds what was filled and grows at most to the declared length and twice what is filled; the total is
at most twice the array, or three times once the array reached the declared length, which happens at most once. -/
def Inv (L c0 : Nat) (st : St) : Prop :=
  st.filled ≤ st.cap ∧ st.cap ≤ L ∧ st.cap ≤ max c0 (2 * st.filled) ∧
    (st.total ≤ 2 * st.cap ∨ (st.cap = L ∧ st.total ≤ 3 * L))

theorem step_inv (L c0 count : Nat) (st : St) (h : Inv L c0 st) (hc : 0 < count)
    (hfit : st.filled + count ≤ L) : Inv L c0 (step L st count) := by
  unfold Inv at h ⊢
  unfold step
  by_cases hg : st.cap - st.filled < count
  · simp only [hg, ite_true]
    omega
  · simp only [hg, ite_false]
    omega

theorem step_filled (L count : Nat) (st : St) : (step L st count).filled = st.filled + count := by
  unfold step; split <;> rfl

theorem run_inv (L c0 : Nat) : ∀ (counts : List Nat) (st : St), Inv L c0 st → (∀ c ∈ counts, 0 < c) →
    st.filled + counts.sum ≤ L → Inv L c0 (run L st counts) ∧ (run L st counts).filled = st.filled + counts.sum
  | [], st, h, _, _ => ⟨h, by simp [run]⟩
  | count :: rest, st, h, hpos, hfit => by
    have hc := hpos count (by simp)
    simp only [List.sum_cons] at hfit
    have h' := step_inv L c0 count st h hc (by omega)
    have ih := run_inv L c0 rest (step L st count) h' (fun c hm => hpos c (by simp [hm]))
      (by rw [step_filled]; omega)
    refine ⟨ih.1, ?_⟩
    rw [run, ih.2, step_filled, List.sum_cons]
    omega

/-- Decoding characters that take at least one byte each from `A` bytes, starting with the buffered `b ≤ A` bytes,
allocates at most `6 · max A 256` characters, whatever length `L` the prefix declares. -/
theorem string_alloc_bound (L A b : Nat) (counts : List Nat) (hL : 0 < L) (hb : b ≤ A)
    (hpos : ∀ c ∈ counts, 0 < c) (hfit : counts.sum ≤ L) (hbytes : counts.sum ≤ A) :
    (run L ⟨min L (max 256 b), min L (max 256 b), 0⟩ counts).total ≤ 6 * max A 256 := by
  have h0 : Inv L (min L (max 256 b)) ⟨min L (max 256 b), min L (max 256 b), 0⟩ := by
    unfold Inv; simp only; omega
  have ⟨hinv, hfilled⟩ := run_inv L _ counts _ h0 hpos (by simp only; omega)
  simp only [Nat.zero_add] at hfilled
  unfold Inv at hinv
  omega

end Btdb.GrowRead
