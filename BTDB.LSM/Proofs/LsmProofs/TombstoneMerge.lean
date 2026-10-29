/-!
# Merging a range tombstone into a memtable's fragments

A model of how `LsmKeyValueDBTransaction.EraseRangeCore` (`../LsmKeyValueDBTransaction.cs`) adds the range
`[a, b)` to the memtable's range-tombstone fragments, each a range `[s, e)` of keys (ordered, so modelled by
naturals). A fragment ending before `a` or starting after `b` is kept; every other one overlaps or touches the range
and is merged with it into one span from the smallest start to the largest end:

```csharp
if (LsmKeys.Compare(fragment.End, first) < 0 || LsmKeys.Compare(fragment.Start, end) > 0) kept.Add(fragment);
else { spanStart = min(spanStart, fragment.Start); spanEnd = max(spanEnd, fragment.End); }
```

* `merge_covers`: a key is covered by the new fragments exactly when an old fragment covered it or it is in
  `[a, b)`, so merging loses no tombstoned key and adds only the erased range.
* `merge_disjoint`: when the old fragments are non-empty and pairwise disjoint, as `TombstoneSet` keeps them, so
  are the new ones, so sorting them by start gives a valid `TombstoneSet`.

The code then drops the span when it covers no key live below the memtable; dropping a fragment keeps the others
disjoint and only stops masking keys that do not exist below, so neither result changes what the memtable hides.
-/

namespace Lsm.TombstoneMerge

/-- Kept as it is: ends before the range or starts after it. -/
def apart (a b : Nat) (f : Nat × Nat) : Bool := f.2 < a || b < f.1

def merged (a b : Nat) (old : List (Nat × Nat)) : List (Nat × Nat) :=
  let touching := old.filter (fun f => !apart a b f)
  old.filter (apart a b) ++
    [(touching.foldl (fun m f => min m f.1) a, touching.foldl (fun m f => max m f.2) b)]

def Covered (l : List (Nat × Nat)) (x : Nat) : Prop := ∃ f ∈ l, f.1 ≤ x ∧ x < f.2

theorem foldl_min_le (l : List (Nat × Nat)) (a : Nat) : l.foldl (fun m f => min m f.1) a ≤ a := by
  induction l generalizing a with
  | nil => exact Nat.le_refl _
  | cons f l ih => exact Nat.le_trans (ih _) (Nat.min_le_left _ _)

theorem foldl_min_le_mem (l : List (Nat × Nat)) (a : Nat) (f : Nat × Nat) (hf : f ∈ l) :
    l.foldl (fun m f => min m f.1) a ≤ f.1 := by
  induction l generalizing a with
  | nil => simp at hf
  | cons g l ih =>
    simp only [List.foldl_cons]
    rcases List.mem_cons.mp hf with rfl | hf
    · exact Nat.le_trans (foldl_min_le _ _) (Nat.min_le_right _ _)
    · exact ih _ hf

theorem le_foldl_min (l : List (Nat × Nat)) (a c : Nat) (ha : c ≤ a) (hl : ∀ f ∈ l, c ≤ f.1) :
    c ≤ l.foldl (fun m f => min m f.1) a := by
  induction l generalizing a with
  | nil => exact ha
  | cons g l ih =>
    exact ih _ (Nat.le_min.mpr ⟨ha, hl g List.mem_cons_self⟩) (fun f hf => hl f (List.mem_cons_of_mem _ hf))

theorem foldl_min_attained (l : List (Nat × Nat)) (a : Nat) :
    l.foldl (fun m f => min m f.1) a = a ∨ ∃ f ∈ l, l.foldl (fun m f => min m f.1) a = f.1 := by
  induction l generalizing a with
  | nil => exact Or.inl rfl
  | cons g l ih =>
    simp only [List.foldl_cons]
    rcases ih (min a g.1) with h | ⟨f, hf, h⟩
    · rw [h]
      rcases Nat.le_total a g.1 with hle | hle
      · exact Or.inl (Nat.min_eq_left hle)
      · exact Or.inr ⟨g, List.mem_cons_self, Nat.min_eq_right hle⟩
    · exact Or.inr ⟨f, List.mem_cons_of_mem _ hf, h⟩

theorem le_foldl_max (l : List (Nat × Nat)) (b : Nat) : b ≤ l.foldl (fun m f => max m f.2) b := by
  induction l generalizing b with
  | nil => exact Nat.le_refl _
  | cons f l ih => exact Nat.le_trans (Nat.le_max_left _ _) (ih _)

theorem mem_le_foldl_max (l : List (Nat × Nat)) (b : Nat) (f : Nat × Nat) (hf : f ∈ l) :
    f.2 ≤ l.foldl (fun m f => max m f.2) b := by
  induction l generalizing b with
  | nil => simp at hf
  | cons g l ih =>
    simp only [List.foldl_cons]
    rcases List.mem_cons.mp hf with rfl | hf
    · exact Nat.le_trans (Nat.le_max_right _ _) (le_foldl_max _ _)
    · exact ih _ hf

theorem foldl_max_le (l : List (Nat × Nat)) (b c : Nat) (hb : b ≤ c) (hl : ∀ f ∈ l, f.2 ≤ c) :
    l.foldl (fun m f => max m f.2) b ≤ c := by
  induction l generalizing b with
  | nil => exact hb
  | cons g l ih =>
    exact ih _ (Nat.max_le.mpr ⟨hb, hl g List.mem_cons_self⟩) (fun f hf => hl f (List.mem_cons_of_mem _ hf))

theorem foldl_max_attained (l : List (Nat × Nat)) (b : Nat) :
    l.foldl (fun m f => max m f.2) b = b ∨ ∃ f ∈ l, l.foldl (fun m f => max m f.2) b = f.2 := by
  induction l generalizing b with
  | nil => exact Or.inl rfl
  | cons g l ih =>
    simp only [List.foldl_cons]
    rcases ih (max b g.2) with h | ⟨f, hf, h⟩
    · rw [h]
      rcases Nat.le_total b g.2 with hle | hle
      · exact Or.inr ⟨g, List.mem_cons_self, Nat.max_eq_right hle⟩
      · exact Or.inl (Nat.max_eq_left hle)
    · exact Or.inr ⟨f, List.mem_cons_of_mem _ hf, h⟩

theorem merge_covers (a b : Nat) (old : List (Nat × Nat)) (x : Nat) :
    Covered (merged a b old) x ↔ Covered old x ∨ (a ≤ x ∧ x < b) := by
  simp only [merged, Covered, List.mem_append, List.mem_filter, List.mem_singleton]
  constructor
  · rintro ⟨f, hf | rfl, hx⟩
    · exact Or.inl ⟨f, hf.1, hx⟩
    · obtain ⟨hlo, hhi⟩ := hx
      by_cases hxa : x < a
      · rcases foldl_min_attained (old.filter (fun f => !apart a b f)) a with h | ⟨g, hg, h⟩
        · simp only at hlo
          omega
        · rw [List.mem_filter] at hg
          simp only [apart, Bool.not_or, Bool.and_eq_true, Bool.not_eq_true', decide_eq_false_iff_not] at hg
          simp only at hlo
          exact Or.inl ⟨g, hg.1, by omega, by omega⟩
      · by_cases hxb : x < b
        · exact Or.inr ⟨by omega, hxb⟩
        · rcases foldl_max_attained (old.filter (fun f => !apart a b f)) b with h | ⟨g, hg, h⟩
          · simp only at hhi
            omega
          · rw [List.mem_filter] at hg
            simp only [apart, Bool.not_or, Bool.and_eq_true, Bool.not_eq_true', decide_eq_false_iff_not] at hg
            simp only at hhi
            exact Or.inl ⟨g, hg.1, by omega, by omega⟩
  · rintro (⟨f, hf, hx⟩ | ⟨ha, hb⟩)
    · by_cases hap : apart a b f = true
      · exact ⟨f, Or.inl ⟨hf, hap⟩, hx⟩
      · have hmem : f ∈ old.filter (fun f => !apart a b f) := List.mem_filter.mpr ⟨hf, by simpa using hap⟩
        refine ⟨_, Or.inr rfl, ?_, ?_⟩
        · exact Nat.le_trans (foldl_min_le_mem _ _ f hmem) hx.1
        · exact Nat.lt_of_lt_of_le hx.2 (mem_le_foldl_max _ _ f hmem)
    · refine ⟨_, Or.inr rfl, ?_, ?_⟩
      · exact Nat.le_trans (foldl_min_le _ _) ha
      · exact Nat.lt_of_lt_of_le hb (le_foldl_max _ _)

/-- Two fragments share no key. -/
def Apart (f g : Nat × Nat) : Prop := f.2 ≤ g.1 ∨ g.2 ≤ f.1

theorem pairwise_mem {l : List (Nat × Nat)} (h : l.Pairwise Apart) {f g : Nat × Nat} (hf : f ∈ l) (hg : g ∈ l)
    (hne : f ≠ g) : Apart f g := by
  induction l with
  | nil => simp at hf
  | cons x l ih =>
    rw [List.pairwise_cons] at h
    rcases List.mem_cons.mp hf with hfx | hfl <;> rcases List.mem_cons.mp hg with hgx | hgl
    · exact absurd (hfx.trans hgx.symm) hne
    · subst hfx
      exact h.1 g hgl
    · subst hgx
      have := h.1 f hfl
      unfold Apart at this ⊢
      omega
    · exact ih h.2 hfl hgl

theorem merge_disjoint (a b : Nat) (old : List (Nat × Nat)) (hvalid : ∀ f ∈ old, f.1 < f.2)
    (hold : old.Pairwise Apart) : (merged a b old).Pairwise Apart := by
  simp only [merged]
  rw [List.pairwise_append]
  refine ⟨hold.sublist List.filter_sublist, List.pairwise_singleton _ _, ?_⟩
  intro g hg s hs
  rw [List.mem_singleton] at hs
  subst hs
  rw [List.mem_filter] at hg
  obtain ⟨hgold, hgap⟩ := hg
  have hgv := hvalid g hgold
  simp only [apart, Bool.or_eq_true, decide_eq_true_eq] at hgap
  have touching_apart : ∀ f ∈ old.filter (fun f => !apart a b f), Apart g f ∧ a ≤ f.2 ∧ f.1 ≤ b := by
    intro f hf
    rw [List.mem_filter] at hf
    simp only [apart, Bool.not_or, Bool.and_eq_true, Bool.not_eq_true', decide_eq_false_iff_not] at hf
    have hne : g ≠ f := by
      rintro rfl
      omega
    exact ⟨pairwise_mem hold hgold hf.1 hne, by omega, by omega⟩
  rcases hgap with hga | hgb
  · left
    apply le_foldl_min _ _ _ (by omega)
    intro f hf
    obtain ⟨hgf, hfa, _⟩ := touching_apart f hf
    have := hvalid f (List.mem_filter.mp hf).1
    unfold Apart at hgf
    omega
  · right
    apply foldl_max_le _ _ _ (by omega)
    intro f hf
    obtain ⟨hgf, _, hfb⟩ := touching_apart f hf
    have := hvalid f (List.mem_filter.mp hf).1
    unfold Apart at hgf
    omega

end Lsm.TombstoneMerge
