import BtdbProofs.PtrLenExclude

/-!
# Finding free space in `PtrLenList`

A model of `PtrLenList.FindFreeSizeAfter(pos, len)` (`../KVDBLayer/Helpers/PtrLenList.cs`), the first position at or
after `pos` where `len` positions are free, after its binary search: if `pos` lies in a range it moves to the range's
end, so every range scanned starts at or after `pos`. The scan returns `pos` when `len` positions fit before the next
range, and otherwise continues from that range's end. Ranges are sorted, disjoint and not empty; `len > 0`, as the C#
returns `pos` for `len = 0`.

* `fit_after`, `fit_free`: the result is at or after `pos` and the `len` positions from it are free.
* `fit_first`: from every earlier position at or after `pos`, `len` positions would hit a range, so the result is the
  first fit.
* `take_mem`, `take_sorted`: `TryFindLenAndRemove(len)` takes `len` positions from the front of the first range at
  least that long; afterwards a position is in the ranges exactly when it was before and is not one of them, and the
  ranges stay sorted and disjoint.
-/

namespace Btdb.PtrLenFree

open Btdb.PtrLenExclude

def fit (pos len : Nat) : List (Nat × Nat) → Nat
  | [] => pos
  | (s, n) :: rest => if pos + len ≤ s then pos else fit (s + n) len rest

def StartsFrom (pos : Nat) : List (Nat × Nat) → Prop
  | [] => True
  | (s, _) :: rest => pos ≤ s ∧ StartsFrom pos rest

def NonEmpty : List (Nat × Nat) → Prop
  | [] => True
  | (_, n) :: rest => 0 < n ∧ NonEmpty rest

theorem starts_from_tail (s n : Nat) (rest : List (Nat × Nat)) (h : Sorted ((s, n) :: rest)) :
    StartsFrom (s + n) rest := by
  induction rest generalizing s n with
  | nil => trivial
  | cons a rest ih =>
    obtain ⟨t, m⟩ := a
    obtain ⟨hst, hs⟩ := h
    have := ih t m hs
    exact ⟨hst, starts_from_mono _ _ _ (by omega) this⟩
where
  starts_from_mono (a b : Nat) (rs : List (Nat × Nat)) (hab : a ≤ b) (h : StartsFrom b rs) : StartsFrom a rs := by
    induction rs with
    | nil => trivial
    | cons r rs ih => exact ⟨by have := h.1; omega, ih h.2⟩

theorem fit_after (pos len : Nat) (rs : List (Nat × Nat)) (hs : Sorted rs) (hst : StartsFrom pos rs) :
    pos ≤ fit pos len rs := by
  induction rs generalizing pos with
  | nil => exact Nat.le_refl _
  | cons a rest ih =>
    obtain ⟨s, n⟩ := a
    simp only [fit]
    split
    · exact Nat.le_refl _
    · have : pos ≤ s := hst.1
      have := ih (s + n) (sorted_tail _ _ hs) (starts_from_tail s n rest hs)
      omega

/-- No range of a list starting from `a` holds a position below `a`. -/
theorem not_mem_below (a x : Nat) (rs : List (Nat × Nat)) (h : StartsFrom a rs) (hx : x < a) : ¬ Mem x rs := by
  induction rs with
  | nil => exact id
  | cons r rs ih =>
    obtain ⟨s, n⟩ := r
    rintro (h1 | h1)
    · have := h.1; omega
    · exact ih h.2 h1

theorem fit_free (pos len : Nat) (rs : List (Nat × Nat)) (hs : Sorted rs) (hst : StartsFrom pos rs) (x : Nat)
    (hx : fit pos len rs ≤ x ∧ x < fit pos len rs + len) : ¬ Mem x rs := by
  induction rs generalizing pos with
  | nil => exact id
  | cons a rest ih =>
    obtain ⟨s, n⟩ := a
    simp only [fit] at hx
    split at hx
    · next h =>
      -- The window ends by the next range's start, and later ranges start after it.
      rintro (h1 | h1)
      · omega
      · exact not_mem_below s x rest
          (starts_from_tail.starts_from_mono _ _ _ (by omega) (starts_from_tail s n rest hs)) (by omega) h1
    · rintro (h1 | h1)
      · have := fit_after (s + n) len rest (sorted_tail _ _ hs) (starts_from_tail s n rest hs)
        omega
      · exact ih (s + n) (sorted_tail _ _ hs) (starts_from_tail s n rest hs) hx h1

theorem fit_first (pos len : Nat) (rs : List (Nat × Nat)) (hs : Sorted rs) (hne : NonEmpty rs) (hlen : 0 < len)
    (q : Nat) (hq : pos ≤ q ∧ q < fit pos len rs) : ∃ x, q ≤ x ∧ x < q + len ∧ Mem x rs := by
  induction rs generalizing pos with
  | nil => simp [fit] at hq; omega
  | cons a rest ih =>
    obtain ⟨s, n⟩ := a
    obtain ⟨hn, hnerest⟩ := hne
    simp only [fit] at hq
    split at hq
    · omega
    · next h =>
      rcases Nat.lt_or_ge q (s + n) with hqs | hqs
      · -- The window from q reaches this range.
        refine ⟨max q s, by omega, by omega, Or.inl ⟨by omega, by omega⟩⟩
      · obtain ⟨x, h1, h2, h3⟩ := ih (s + n) (sorted_tail _ _ hs) hnerest ⟨hqs, hq.2⟩
        exact ⟨x, h1, h2, Or.inr h3⟩

/-- `TryFindLenAndRemove`: the start taken and the ranges after, or none when no range is long enough. -/
def take (len : Nat) : List (Nat × Nat) → Option (Nat × List (Nat × Nat))
  | [] => none
  | (s, n) :: rest =>
    if len ≤ n then some (s, if len = n then rest else (s + len, n - len) :: rest)
    else (take len rest).map fun (at', rs) => (at', (s, n) :: rs)

theorem take_start (len a : Nat) (rs : List (Nat × Nat)) (h : StartsFrom a rs) (at' : Nat)
    (rs' : List (Nat × Nat)) (ht : take len rs = some (at', rs')) : a ≤ at' := by
  induction rs generalizing at' rs' with
  | nil => simp [take] at ht
  | cons r rest ih =>
    obtain ⟨s, n⟩ := r
    simp only [take] at ht
    split at ht
    · simp only [Option.some.injEq, Prod.mk.injEq] at ht
      obtain ⟨rfl, _⟩ := ht
      exact h.1
    · cases hr : take len rest with
      | none => simp [hr] at ht
      | some p =>
        obtain ⟨at'', rs''⟩ := p
        simp only [hr, Option.map_some, Option.some.injEq, Prod.mk.injEq] at ht
        obtain ⟨rfl, _⟩ := ht
        exact ih h.2 _ rs'' hr

theorem take_mem (len : Nat) (rs : List (Nat × Nat)) (hs : Sorted rs) (at' : Nat) (rs' : List (Nat × Nat))
    (h : take len rs = some (at', rs')) (x : Nat) : Mem x rs' ↔ Mem x rs ∧ ¬ (at' ≤ x ∧ x < at' + len) := by
  induction rs generalizing at' rs' with
  | nil => simp [take] at h
  | cons r rest ih =>
    obtain ⟨s, n⟩ := r
    simp only [take] at h
    split at h
    · next hlen =>
      simp only [Option.some.injEq, Prod.mk.injEq] at h
      obtain ⟨rfl, rfl⟩ := h
      simp only [Mem]
      by_cases heq : len = n
      · rw [ite_eq_left heq]
        constructor
        · intro hm
          have := mem_after s n x rest hs hm
          exact ⟨Or.inr hm, by omega⟩
        · rintro ⟨h1 | h1, h2⟩
          · omega
          · exact h1
      · rw [ite_eq_right heq]
        simp only [Mem]
        constructor
        · rintro (h1 | h1)
          · exact ⟨Or.inl ⟨by omega, by omega⟩, by omega⟩
          · have := mem_after s n x rest hs h1
            exact ⟨Or.inr h1, by omega⟩
        · rintro ⟨h1 | h1, h2⟩
          · exact Or.inl ⟨by omega, by omega⟩
          · exact Or.inr h1
    · next hlen =>
      cases hr : take len rest with
      | none => simp [hr] at h
      | some p =>
        obtain ⟨at'', rs''⟩ := p
        simp only [hr, Option.map_some, Option.some.injEq, Prod.mk.injEq] at h
        obtain ⟨rfl, rfl⟩ := h
        have hge := take_start len (s + n) rest (starts_from_tail s n rest hs) at'' rs'' hr
        simp only [Mem]
        rw [ih (sorted_tail _ _ hs) at'' rs'' hr]
        constructor
        · rintro (h1 | ⟨h1, h2⟩)
          · exact ⟨Or.inl h1, by omega⟩
          · exact ⟨Or.inr h1, h2⟩
        · rintro ⟨h1 | h1, h2⟩
          · exact Or.inl h1
          · exact Or.inr ⟨h1, h2⟩

theorem take_starts_from (len a : Nat) (rs : List (Nat × Nat)) (h : StartsFrom a rs) (at' : Nat)
    (rs' : List (Nat × Nat)) (ht : take len rs = some (at', rs')) : StartsFrom a rs' := by
  induction rs generalizing at' rs' with
  | nil => simp [take] at ht
  | cons r rest ih =>
    obtain ⟨s, n⟩ := r
    simp only [take] at ht
    split at ht
    · simp only [Option.some.injEq, Prod.mk.injEq] at ht
      obtain ⟨_, rfl⟩ := ht
      split
      · exact h.2
      · exact ⟨by have := h.1; omega, h.2⟩
    · cases hr : take len rest with
      | none => simp [hr] at ht
      | some p =>
        obtain ⟨at'', rs''⟩ := p
        simp only [hr, Option.map_some, Option.some.injEq, Prod.mk.injEq] at ht
        obtain ⟨_, rfl⟩ := ht
        exact ⟨h.1, ih h.2 at'' rs'' hr⟩

theorem take_sorted (len : Nat) (rs : List (Nat × Nat)) (hs : Sorted rs) (at' : Nat) (rs' : List (Nat × Nat))
    (h : take len rs = some (at', rs')) : Sorted rs' := by
  induction rs generalizing at' rs' with
  | nil => simp [take] at h
  | cons r rest ih =>
    obtain ⟨s, n⟩ := r
    simp only [take] at h
    split at h
    · next hlen =>
      simp only [Option.some.injEq, Prod.mk.injEq] at h
      obtain ⟨_, rfl⟩ := h
      split
      · exact sorted_tail _ _ hs
      · match rest, hs with
        | [], _ => trivial
        | (t, m) :: rest', hs => exact ⟨by have := hs.1; omega, hs.2⟩
    · cases hr : take len rest with
      | none => simp [hr] at h
      | some p =>
        obtain ⟨at'', rs''⟩ := p
        simp only [hr, Option.map_some, Option.some.injEq, Prod.mk.injEq] at h
        obtain ⟨_, rfl⟩ := h
        have hrs := ih (sorted_tail _ _ hs) at'' rs'' hr
        have hfrom := take_starts_from len (s + n) rest (starts_from_tail s n rest hs) at'' rs'' hr
        match rs'', hrs, hfrom with
        | [], _, _ => trivial
        | (t, m) :: rest', hrs, hfrom => exact ⟨hfrom.1, hrs⟩

end Btdb.PtrLenFree
