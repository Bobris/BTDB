import LsmProofs.BlockSeek

/-!
# Finding the range tombstone that contains a key

A model of `TombstoneSet.Find` (`../Engine/LsmComponent.cs`). A component's fragments `[s i, e i)`, `i < n`, are
sorted and disjoint: `s i < e i ≤ s (i + 1)`. `Find` searches for the first fragment starting after the key; the
fragment before it contains the key when the key is below its end. `coveredBefore` is the sum of `CoveredBelow` of the
fragments before the returned index (the candidate when the key is covered, else the first fragment after the key),
here the index `j` that `find` returns second. The binary search is modelled by its result (`BlockSeek.scan`).

* `find_some`: `Find` returns fragment `i` exactly when it contains the key.
* `find_covered`: the fragments before `j` are exactly those ending at or before the key, so `coveredBefore` is the sum
  over them, as `Rank.lean` assumes.
-/

namespace Lsm.TombstoneFind

open Lsm.BlockSeek

def find (s e : Nat → Nat) (n k : Nat) : Option Nat × Nat :=
  let lo := scan (fun i => decide (k < s i)) 0 n
  if 0 < lo ∧ k < e (lo - 1) then (some (lo - 1), lo - 1) else (none, lo)

section

variable (s e : Nat → Nat) (n : Nat)
  (hpos : ∀ i, i < n → s i < e i) (hsorted : ∀ i, i + 1 < n → e i ≤ s (i + 1))

include hpos hsorted in
theorem end_le_start (i d : Nat) (h : i + 1 + d < n) : e i ≤ s (i + 1 + d) := by
  induction d with
  | zero => exact hsorted i h
  | succ d ih =>
    have h1 := ih (by omega)
    have h2 := hpos (i + 1 + d) (by omega)
    have h3 := hsorted (i + 1 + d) (by omega)
    rw [show i + 1 + (d + 1) = i + 1 + d + 1 by omega]
    omega

include hpos hsorted in
theorem start_mono (i j : Nat) (hij : i ≤ j) (hj : j < n) : s i ≤ s j := by
  rcases Nat.eq_or_lt_of_le hij with h | h
  · rw [h]; exact Nat.le_refl _
  · obtain ⟨d, rfl⟩ : ∃ d, j = i + 1 + d := ⟨j - i - 1, by omega⟩
    have := end_le_start s e n hpos hsorted i d hj
    have := hpos i (by omega)
    omega

include hpos hsorted in
theorem end_mono (i j : Nat) (hij : i ≤ j) (hj : j < n) : e i ≤ e j := by
  rcases Nat.eq_or_lt_of_le hij with h | h
  · rw [h]; exact Nat.le_refl _
  · obtain ⟨d, rfl⟩ : ∃ d, j = i + 1 + d := ⟨j - i - 1, by omega⟩
    have := end_le_start s e n hpos hsorted i d hj
    have := hpos (i + 1 + d) hj
    omega

/-- What the search returns: every fragment before `lo` starts at or before the key, and fragment `lo` after it. -/
theorem search (k : Nat) :
    let lo := scan (fun i => decide (k < s i)) 0 n
    lo ≤ n ∧ (∀ i, i < lo → s i ≤ k) ∧ (lo < n → k < s lo) := by
  intro lo
  obtain ⟨_, h2, h3, h4⟩ := scan_first (fun i => decide (k < s i)) 0 n (Nat.zero_le _)
  refine ⟨h2, fun i hi => ?_, fun hl => by simpa using h4 hl⟩
  have := h3 i (Nat.zero_le _) hi
  simp only [decide_eq_false_iff_not, Nat.not_lt] at this
  exact this

include hpos hsorted in
theorem find_some (k i : Nat) : (find s e n k).1 = some i ↔ i < n ∧ s i ≤ k ∧ k < e i := by
  obtain ⟨l1, l2, l3⟩ := search s n k
  generalize hlo : scan (fun i => decide (k < s i)) 0 n = lo at l1 l2 l3
  unfold find
  simp only [hlo]
  constructor
  · intro h
    split at h
    · next hc =>
      simp only [Option.some.injEq] at h
      subst h
      exact ⟨by omega, l2 (lo - 1) (by omega), hc.2⟩
    · simp at h
  · rintro ⟨hi, hs, he⟩
    have hilo : i < lo := by
      rcases Nat.lt_or_ge i lo with h | h
      · exact h
      · have := l3 (by omega)
        have := start_mono s e n hpos hsorted lo i h hi
        omega
    have hlo' : lo ≤ i + 1 := by
      rcases Nat.lt_or_ge (i + 1) n with h | h
      · rcases Nat.lt_or_ge (i + 1) lo with h' | h'
        · have := l2 (i + 1) h'
          have := hsorted i h
          omega
        · exact h'
      · omega
    have : lo - 1 = i := by omega
    rw [ite_eq_left ⟨by omega, by rw [this]; exact he⟩, this]

include hpos hsorted in
theorem find_covered (k : Nat) :
    (∀ i, i < (find s e n k).2 → e i ≤ k) ∧ (∀ i, (find s e n k).2 ≤ i → i < n → k < e i) := by
  obtain ⟨l1, l2, l3⟩ := search s n k
  generalize hlo : scan (fun i => decide (k < s i)) 0 n = lo at l1 l2 l3
  unfold find
  simp only [hlo]
  -- Fragments before the last one starting at or before the key end at or before it.
  have hbefore : ∀ i, i + 1 < lo → e i ≤ k := by
    intro i hi
    have := hsorted i (by omega)
    have := l2 (i + 1) hi
    omega
  -- Fragments starting after the key end after it.
  have hafter : ∀ i, lo ≤ i → i < n → k < e i := by
    intro i hi hn
    have := l3 (by omega)
    have := start_mono s e n hpos hsorted lo i hi hn
    have := hpos i hn
    omega
  split
  · next hc =>
    refine ⟨fun i hi => hbefore i (by omega), fun i hi hn => ?_⟩
    have := end_mono s e n hpos hsorted (lo - 1) i hi hn
    omega
  · next hc =>
    refine ⟨fun i hi => ?_, hafter⟩
    rcases Nat.lt_or_ge (i + 1) lo with h | h
    · exact hbefore i h
    · have : i = lo - 1 := by omega
      subst this
      omega

end

end Lsm.TombstoneFind
