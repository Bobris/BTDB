import BtdbProofs.Cursor

/-!
# Seeking the first key of a relation query range

A model of `RelationQueryEnumerator.Seek` (`BTDB/ODBLayer/RelationQuery.cs`), which positions a scan on the first key
of a range with a lower bound, using the cursor contract of `Cursor.lean`. The range keys have the prefix `p`; the
lower bound `start` extends `p`, and with an exclusive bound the keys extending `start` (those whose field equals the
bound) are skipped. A key passes the lower bound when it is not below `start` and, for an exclusive bound, does not
extend it, which is `KeyRange.lowerOk`.

* `seek_exact`, `seek_next`, `seek_previous`, `seek_previous_none`: `Find(start, |p|)`, followed after a smaller key
  by `FindNextKey(p)`, reaches the first key of the block at or above `start`, or proves there is none.
* `seek_exclusive_kept`, `seek_exclusive`, `seek_exclusive_none`: with an exclusive bound the scan stays on a first
  key that does not extend `start`; otherwise `FindLastKey(start)` and `FindNextKey(p)` reach the first key above
  every key extending it, or prove there is none.
-/

namespace Btdb.Seek

open KeyRange Cursor

/-- A key that extends `s` is not below it. -/
theorem not_below_of_prefix : ∀ (s x : List Nat), s <+: x → ¬ lt x s
  | [], x, _, h => by cases x <;> simp [lt, cmp] at h
  | c :: s, x, ⟨t, ht⟩, h => by
    subst ht
    simp only [lt, List.cons_append, cmp, Nat.lt_irrefl, ite_false] at h
    exact not_below_of_prefix s (s ++ t) (List.prefix_append _ _) h

/-- A key at or above `s` that does not extend it is above every key that does. -/
theorem above_block : ∀ (s l x : List Nat), s <+: l → ¬ s <+: x → ¬ lt x s → lt l x
  | [], _, _, _, h, _ => absurd List.nil_prefix h
  | c :: s, l, x, ⟨t, ht⟩, hx, hb => by
    subst ht
    cases x with
    | nil => exact absurd rfl hb
    | cons d x' =>
      simp only [lt, List.cons_append, cmp] at hb ⊢
      by_cases h1 : d < c
      · simp [h1] at hb
      · by_cases h2 : c < d
        · simp [h2]
        · have e : d = c := by omega
          subst e
          simp only [Nat.lt_irrefl, ite_false] at hb ⊢
          exact above_block s (s ++ t) x' (List.prefix_append _ _)
            (fun h => hx (List.cons_prefix_cons.2 ⟨rfl, h⟩)) hb

variable {S : List Nat → Prop} {p start k n l : List Nat}

/-- `k` is the first key with property `P`. -/
def First (P : List Nat → Prop) (k : List Nat) : Prop := P k ∧ ∀ x, P x → x = k ∨ lt k x

/-- The keys of the block at or above `start`. -/
def AtOrAbove (S : List Nat → Prop) (p start x : List Nat) : Prop := Block S p x ∧ ¬ lt x start

theorem seek_exact (hs : S start) (hp : p <+: start) : First (AtOrAbove S p start) start := by
  refine ⟨⟨⟨hs, hp⟩, lt_irrefl _⟩, fun x hx => ?_⟩
  rcases trichotomy x start with h | h | h
  · exact absurd h hx.2
  · exact Or.inl h
  · exact Or.inr h

theorem seek_next (hs : ¬ S start) (hn : IsNext (Block S p) start k) : First (AtOrAbove S p start) k := by
  refine ⟨⟨hn.1, fun h => lt_irrefl _ (lt_trans _ _ _ hn.2.1 h)⟩, fun x hx => ?_⟩
  rcases trichotomy x start with h | rfl | h
  · exact absurd h hx.2
  · exact absurd hx.1.1 hs
  · exact hn.2.2 x hx.1 h

theorem seek_previous (hs : ¬ S start) (hk : IsPrev (Block S p) start k) (hn : IsNext S k n) (hpn : p <+: n) :
    First (AtOrAbove S p start) n := by
  have hnb : Block S p n := ⟨hn.1, hpn⟩
  refine ⟨⟨hnb, fun h => ?_⟩, fun x hx => ?_⟩
  · rcases hk.2.2 n hnb h with rfl | h'
    · exact lt_irrefl _ hn.2.1
    · exact lt_irrefl _ (lt_trans _ _ _ hn.2.1 h')
  · rcases trichotomy x start with h | rfl | h
    · exact absurd h hx.2
    · exact absurd hx.1.1 hs
    · exact hn.2.2 x hx.1.1 (lt_trans _ _ _ hk.2.1 h)

theorem seek_previous_none (hp : p <+: start) (hs : ¬ S start) (hk : IsPrev (Block S p) start k)
    (hn : (IsNext S k n ∧ ¬ p <+: n) ∨ ∀ x, S x → ¬ lt k x) : ∀ x, ¬ AtOrAbove S p start x := by
  intro x hx
  have hsx : lt start x := by
    rcases trichotomy x start with h | rfl | h
    · exact absurd h hx.2
    · exact absurd hx.1.1 hs
    · exact h
  have hkx := lt_trans _ _ _ hk.2.1 hsx
  rcases hn with ⟨hn, hpn⟩ | hn
  · rcases hn.2.2 x hx.1.1 hkx with rfl | hnx
    · exact hpn hx.1.2
    · exact hpn (block p k n x hk.1.2 hx.1.2 hn.2.1 hnx)
  · exact hn x hx.1.1 hkx

/-- The keys passing an exclusive lower bound. -/
def Above (S : List Nat → Prop) (p start x : List Nat) : Prop := AtOrAbove S p start x ∧ ¬ start <+: x

/-- The first key at or above `start` does not extend it: it passes an exclusive bound and the scan stays there. -/
theorem seek_exclusive_kept (hk : First (AtOrAbove S p start) k) (hs : ¬ start <+: k) : First (Above S p start) k :=
  ⟨⟨hk.1, hs⟩, fun x hx => hk.2 x hx.1⟩

/-- With `l` the last key extending `start` (`FindLastKey(start)`) and `n` the key after it (`FindNextKey(p)`). -/
theorem seek_exclusive (hlast : S l ∧ start <+: l ∧ ∀ x, S x → start <+: x → x = l ∨ lt x l) (hn : IsNext S l n)
    (hpn : p <+: n) :
    First (Above S p start) n := by
  have hns : ¬ start <+: n := fun h => by
    rcases hlast.2.2 n hn.1 h with rfl | h'
    · exact lt_irrefl _ hn.2.1
    · exact lt_irrefl _ (lt_trans _ _ _ hn.2.1 h')
  refine ⟨⟨⟨⟨hn.1, hpn⟩, fun h => ?_⟩, hns⟩, fun x hx => ?_⟩
  · exact not_below_of_prefix start l hlast.2.1 (lt_trans _ _ _ hn.2.1 h)
  · exact hn.2.2 x hx.1.1.1 (above_block start l x hlast.2.1 hx.2 hx.1.2)

theorem seek_exclusive_none (hlast : S l ∧ start <+: l ∧ ∀ x, S x → start <+: x → x = l ∨ lt x l)
    (hn : (IsNext S l n ∧ ¬ p <+: n ∧ p <+: l) ∨ ∀ x, S x → ¬ lt l x) : ∀ x, ¬ Above S p start x := by
  intro x hx
  have hlx := above_block start l x hlast.2.1 hx.2 hx.1.2
  rcases hn with ⟨hn, hpn, hpl⟩ | hn
  · rcases hn.2.2 x hx.1.1.1 hlx with rfl | hnx
    · exact hpn hx.1.1.2
    · exact hpn (block p l n x hpl hx.1.1.2 hn.2.1 hnx)
  · exact hn x hx.1.1.1 hlx

end Btdb.Seek
