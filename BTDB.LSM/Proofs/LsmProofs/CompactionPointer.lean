/-!
# Round-robin choice of the table to compact

A model of how `LsmCompaction.Pick` (`../Engine/LsmCompaction.cs`) chooses the table of a level to compact down:

```csharp
var chosen = pointer == null ? tables[0] : tables.FirstOrDefault(t => LsmKeys.Compare(t.Lower, pointer) > 0) ?? tables[0];
pointers[bestLevel] = chosen.Lower;
```

A level's tables are sorted by their lower keys, which are distinct (`LevelLookup.replace_sorted`); the model keeps
just those keys, ordered, as naturals.

* `pick_after`: picked after the table at index `i`, the next pick is the table at `(i + 1) % n`.
* `picks_cycle`: `k` picks after the table at `i` end at the table at `(i + k) % n`.
* `every_table_picked`: while the level does not change, every table is picked within `n` picks of any table, so
  no table is starved of compaction.
-/

namespace Lsm.CompactionPointer

/-- The lower key of the table picked after the pointer; the first table when none is past it. -/
def pick (l : List Nat) (h : 0 < l.length) (pointer : Nat) : Nat := (l.find? (· > pointer)).getD (l[0]'h)

theorem find_after (l : List Nat) (hs : l.Pairwise (· < ·)) (i : Nat) (hi : i < l.length) :
    l.find? (· > l[i]) = l[i + 1]? := by
  induction l generalizing i with
  | nil => simp at hi
  | cons x l ih =>
    rw [List.pairwise_cons] at hs
    cases i with
    | zero =>
      simp only [List.getElem_cons_zero, List.getElem?_cons_succ]
      rw [List.find?_cons]
      simp only [gt_iff_lt, Nat.lt_irrefl, decide_false]
      cases l with
      | nil => rfl
      | cons y l =>
        simp only [List.getElem?_cons_zero]
        rw [List.find?_cons]
        have : x < y := hs.1 y List.mem_cons_self
        simp [this]
    | succ i =>
      have hil : i < l.length := by simp at hi; omega
      simp only [List.getElem_cons_succ, List.getElem?_cons_succ]
      rw [List.find?_cons]
      have hx : x < l[i] := hs.1 _ (List.getElem_mem hil)
      have : ¬(x > l[i]) := by omega
      simp only [this, decide_false]
      exact ih hs.2 i hil

theorem pick_after (l : List Nat) (hs : l.Pairwise (· < ·)) (h : 0 < l.length) (i : Nat) (hi : i < l.length) :
    pick l h l[i] = l[(i + 1) % l.length]'(Nat.mod_lt _ h) := by
  unfold pick
  rw [find_after l hs i hi]
  by_cases hn : i + 1 < l.length
  · rw [List.getElem?_eq_getElem hn, Option.getD_some]
    congr 1
    exact (Nat.mod_eq_of_lt hn).symm
  · have he : i + 1 = l.length := by omega
    rw [List.getElem?_eq_none (by omega), Option.getD_none]
    congr 1
    rw [he, Nat.mod_self]

/-- The table index after `k` picks starting after the table at `i`. -/
def after (n i : Nat) : Nat → Nat
  | 0 => i
  | k + 1 => (after n i k + 1) % n

theorem after_eq (n i k : Nat) (hi : i < n) : after n i k = (i + k) % n := by
  induction k with
  | zero => simp [after, Nat.mod_eq_of_lt hi]
  | succ k ih =>
    rw [after, ih, Nat.add_mod, Nat.mod_mod, ← Nat.add_mod, Nat.add_assoc]

theorem after_lt (n i k : Nat) (hn : 0 < n) (hi : i < n) : after n i k < n := by
  rw [after_eq n i k hi]
  exact Nat.mod_lt _ hn

/-- The lower key after `k` picks starting from pointer `p`. -/
def picks (l : List Nat) (h : 0 < l.length) : Nat → Nat → Nat
  | 0, p => p
  | k + 1, p => pick l h (picks l h k p)

/-- Picking repeatedly from the table at `i`. -/
theorem picks_cycle (l : List Nat) (hs : l.Pairwise (· < ·)) (h : 0 < l.length) (i : Nat) (hi : i < l.length)
    (k : Nat) :
    picks l h k l[i] = l[(i + k) % l.length]'(Nat.mod_lt _ h) := by
  have key : ∀ k, picks l h k l[i] = l[after l.length i k]'(after_lt _ _ _ h hi) := by
    intro k
    induction k with
    | zero => rfl
    | succ k ih =>
      rw [picks, ih, pick_after l hs h _ (after_lt _ _ _ h hi)]
      rfl
  rw [key k]
  congr 1
  exact after_eq _ _ _ hi

theorem every_table_picked (l : List Nat) (hs : l.Pairwise (· < ·)) (h : 0 < l.length) (i j : Nat)
    (hi : i < l.length) (hj : j < l.length) :
    ∃ k, 0 < k ∧ k ≤ l.length ∧ picks l h k l[i] = l[j] := by
  refine ⟨(l.length - 1 - i + j) % l.length + 1, by omega, ?_, ?_⟩
  · have := Nat.mod_lt (l.length - 1 - i + j) h
    omega
  · rw [picks_cycle l hs h i hi]
    congr 1
    have : i + ((l.length - 1 - i + j) % l.length + 1) = (i + 1) + (l.length - 1 - i + j) % l.length := by omega
    rw [this, Nat.add_mod, Nat.mod_mod, ← Nat.add_mod]
    have : i + 1 + (l.length - 1 - i + j) = l.length + j := by omega
    rw [this, Nat.add_mod_left, Nat.mod_eq_of_lt hj]

end Lsm.CompactionPointer
