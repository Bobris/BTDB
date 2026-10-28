import LsmProofs.Rank

/-!
# Cursors keep their place when a key suffix changes

A model of `LsmKeyValueDBCursor.NotifyRenamed`. Keys are naturals in key order; `S` is the set of live keys. A cursor
remembers a key `r` and a flag `before`. It is current on `r` when `r` is live and the flag is clear, fuzzy at the
erased `r` when `r` is not live, and fuzzy right before the live `r` when the flag is set. In every case its position,
the index of the key it is on or the index of the next key, is the number of live keys below `r` (`cnt S r`).

`UpdateKeySuffix` replaces the live key `old` by `new`, the only key with the prefix, so no other live key lies
between them, and it keeps the key's index. `new + 1` stands for `LsmKeys.Successor(new)`, which extends `new`, so it
has the prefix and is not live either.

* `notify_position`: after the rename every cursor has the position it had before.
* `notify_kind`: a current cursor stays current and a fuzzy one stays fuzzy, and a cursor right before a key is right
  before a live key.

The 2026-10-28 fix made fuzzy cursors move with the rename (before, their erased key could end up on the other side
of the renamed key) and made a cursor whose erased key was created again follow the rename as a current cursor.
-/

namespace Lsm.FuzzyCursor

open Rank

structure Cursor where
  key : Nat
  before : Bool

/-- The live keys after the rename. -/
def rename (S : Nat → Bool) (old new : Nat) (k : Nat) : Bool :=
  if k = old then false else if k = new then true else S k

/-- `NotifyRenamed`. -/
def notify (old new : Nat) (c : Cursor) : Cursor :=
  if c.key = old then ⟨new, c.before⟩
  else if c.before then c
  else if old < new then (if old < c.key ∧ c.key ≤ new then ⟨new + 1, false⟩ else c)
  else (if new ≤ c.key ∧ c.key < old then ⟨new, true⟩ else c)

/-- What `UpdateKeySuffix` guarantees: `old` is live, and no other key from `old` to `new` is, nor `new`'s successor. -/
structure Renamable (S : Nat → Bool) (old new : Nat) : Prop where
  old_live : S old = true
  ne : old ≠ new
  between : ∀ k, k ≠ old → min old new ≤ k → k ≤ max old new → S k = false
  succ : S (new + 1) = false

theorem cnt_succ_true {p : Nat → Bool} {x : Nat} (h : p x = true) : cnt p (x + 1) = cnt p x + 1 := by
  simp [cnt, h]

theorem cnt_succ_false {p : Nat → Bool} {x : Nat} (h : p x = false) : cnt p (x + 1) = cnt p x := by
  simp [cnt, h]

/-- Counts of two sets that agree on `[a, b)` grow by the same amount there. -/
theorem agree_shift {p q : Nat → Bool} (a : Nat) : ∀ d, (∀ k, a ≤ k → k < a + d → p k = q k) →
    cnt p (a + d) + cnt q a = cnt q (a + d) + cnt p a
  | 0, _ => by simp; omega
  | d + 1, h => by
    have ih := agree_shift a d (fun k h1 h2 => h k h1 (by omega))
    have e := h (a + d) (by omega) (by omega)
    rw [show a + (d + 1) = a + d + 1 by omega]
    simp only [cnt, e]
    omega

theorem agree_above {p q : Nat → Bool} {a x : Nat} (h : ∀ k, a ≤ k → p k = q k) (hx : a ≤ x) :
    cnt p x + cnt q a = cnt q x + cnt p a := by
  have := agree_shift (p := p) (q := q) a (x - a) (fun k h1 _ => h k h1)
  rwa [show a + (x - a) = x by omega] at this

theorem agree_below {p q : Nat → Bool} {x : Nat} (h : ∀ k, k < x → p k = q k) : cnt p x = cnt q x :=
  cnt_congr x h

/-- How the counts change, as `notify_position` uses them, when the key grew. -/
theorem grew_counts {S : Nat → Bool} {old new : Nat} (hR : Renamable S old new) (hlt : old < new) :
    (∀ x, x ≤ old → cnt (rename S old new) x = cnt S x) ∧
    (∀ x, old + 1 ≤ x → x ≤ new + 1 → cnt S x = cnt S old + 1) ∧
    (∀ x, old ≤ x → x ≤ new → cnt (rename S old new) x = cnt S old) ∧
    cnt (rename S old new) (new + 1) = cnt S old + 1 ∧
    (∀ x, new + 1 ≤ x → cnt (rename S old new) x = cnt S x) := by
  have below : ∀ x, x ≤ old → cnt (rename S old new) x = cnt S x := fun x hx =>
    agree_below (fun k hk => by simp [rename, show k ≠ old by omega, show k ≠ new by omega])
  have flatS : ∀ x, old + 1 ≤ x → x ≤ new + 1 → cnt S x = cnt S old + 1 := fun x h1 h2 => by
    rw [cnt_flat S (old + 1) x h1 (fun k h3 h4 => hR.between k (by omega) (by omega) (by omega)),
      cnt_succ_true hR.old_live]
  have flatR : ∀ x, old ≤ x → x ≤ new → cnt (rename S old new) x = cnt S old := fun x h1 h2 => by
    rw [cnt_flat _ old x h1 (fun k h3 h4 => by
      by_cases ko : k = old
      · simp [rename, ko]
      · simp only [rename, ko, show k ≠ new by omega, ite_false]
        exact hR.between k ko (by omega) (by omega)), below old (Nat.le_refl _)]
  have top : cnt (rename S old new) (new + 1) = cnt S old + 1 := by
    rw [cnt_succ_true (by simp [rename, show new ≠ old by omega]), flatR new (by omega) (Nat.le_refl _)]
  refine ⟨below, flatS, flatR, top, fun x hx => ?_⟩
  have := agree_above (p := rename S old new) (q := S) (a := new + 1) (x := x)
    (fun k hk => by simp [rename, show k ≠ old by omega, show k ≠ new by omega]) hx
  have := flatS (new + 1) (by omega) (Nat.le_refl _)
  omega

/-- How the counts change when the key shrank. -/
theorem shrank_counts {S : Nat → Bool} {old new : Nat} (hR : Renamable S old new) (hlt : new < old) :
    (∀ x, x ≤ new → cnt (rename S old new) x = cnt S x) ∧
    (∀ x, new ≤ x → x ≤ old → cnt S x = cnt S new) ∧
    (∀ x, old + 1 ≤ x → cnt (rename S old new) x = cnt S x) := by
  have below : ∀ x, x ≤ new → cnt (rename S old new) x = cnt S x := fun x hx =>
    agree_below (fun k hk => by simp [rename, show k ≠ old by omega, show k ≠ new by omega])
  have flatS : ∀ x, new ≤ x → x ≤ old → cnt S x = cnt S new := fun x h1 h2 =>
    cnt_flat S new x h1 (fun k h3 h4 => hR.between k (by omega) (by omega) (by omega))
  refine ⟨below, flatS, fun x hx => ?_⟩
  have r1 : cnt (rename S old new) (new + 1) = cnt S new + 1 := by
    rw [cnt_succ_true (by simp [rename, show new ≠ old by omega]), below new (Nat.le_refl _)]
  have r2 : cnt (rename S old new) (old + 1) = cnt S new + 1 := by
    rw [cnt_flat _ (new + 1) (old + 1) (by omega) (fun k h3 h4 => by
      by_cases ko : k = old
      · simp [rename, ko]
      · simp only [rename, ko, show k ≠ new by omega, ite_false]
        exact hR.between k ko (by omega) (by omega)), r1]
  have s1 : cnt S (old + 1) = cnt S new + 1 := by
    rw [cnt_succ_true hR.old_live, flatS old (by omega) (Nat.le_refl _)]
  have := agree_above (p := rename S old new) (q := S) (a := old + 1) (x := x)
    (fun k hk => by simp [rename, show k ≠ old by omega, show k ≠ new by omega]) hx
  omega

theorem notify_position {S : Nat → Bool} {old new : Nat} (hR : Renamable S old new) (c : Cursor)
    (hv : c.before = true → S c.key = true) :
    cnt (rename S old new) (notify old new c).key = cnt S c.key := by
  rcases Nat.lt_or_gt_of_ne hR.ne with hlt | hgt
  · obtain ⟨below, flatS, flatR, top, above⟩ := grew_counts hR hlt
    unfold notify
    by_cases ho : c.key = old
    · simp only [ho, ite_true]; exact flatR new (by omega) (Nat.le_refl _)
    simp only [ho, ite_false, hlt, ite_true]
    -- A live key other than old lies outside old .. new.
    have live_out : S c.key = true → c.key < old ∨ new < c.key := fun hs => by
      rcases Nat.lt_or_ge c.key old with h | h
      · exact Or.inl h
      rcases Nat.lt_or_ge new c.key with h' | h'
      · exact Or.inr h'
      have := hR.between c.key ho (by omega) (by omega)
      simp_all
    by_cases hb : c.before = true
    · simp only [hb, ite_true]
      rcases live_out (hv hb) with h | h
      · exact below _ (by omega)
      · exact above _ (by omega)
    simp only [hb, Bool.false_eq_true, ite_false]
    by_cases hin : old < c.key ∧ c.key ≤ new
    · simp only [hin, and_self, ite_true]; rw [top, flatS c.key (by omega) (by omega)]
    · simp only [hin, ite_false]
      rcases Nat.lt_or_ge c.key old with h | h
      · exact below _ (by omega)
      · exact above _ (by omega)
  · obtain ⟨below, flatS, above⟩ := shrank_counts hR hgt
    unfold notify
    by_cases ho : c.key = old
    · simp only [ho, ite_true]; rw [below new (Nat.le_refl _), flatS old (by omega) (Nat.le_refl _)]
    simp only [ho, ite_false, show ¬ old < new by omega]
    have live_out : S c.key = true → c.key < new ∨ old < c.key := fun hs => by
      rcases Nat.lt_or_ge c.key new with h | h
      · exact Or.inl h
      rcases Nat.lt_or_ge old c.key with h' | h'
      · exact Or.inr h'
      have := hR.between c.key ho (by omega) (by omega)
      simp_all
    by_cases hb : c.before = true
    · simp only [hb, ite_true]
      rcases live_out (hv hb) with h | h
      · exact below _ (by omega)
      · exact above _ (by omega)
    simp only [hb, Bool.false_eq_true, ite_false]
    by_cases hin : new ≤ c.key ∧ c.key < old
    · simp only [hin, and_self, ite_true]; rw [below new (Nat.le_refl _), flatS c.key hin.1 (by omega)]
    · simp only [hin, ite_false]
      rcases Nat.lt_or_ge c.key new with h | h
      · exact below _ (by omega)
      · exact above _ (by omega)

/-- A current cursor stays current, a fuzzy one stays fuzzy, and a cursor right before a key stays before a live key. -/
theorem notify_kind {S : Nat → Bool} {old new : Nat} (hR : Renamable S old new) (c : Cursor)
    (hv : c.before = true → S c.key = true) :
    ((rename S old new (notify old new c).key && !(notify old new c).before) = (S c.key && !c.before)) ∧
    ((notify old new c).before = true → rename S old new (notify old new c).key = true) := by
  have hnew : rename S old new new = true := by simp [rename, show new ≠ old from fun h => hR.ne h.symm]
  unfold notify
  by_cases ho : c.key = old
  · simp only [ho, ite_true, hnew, hR.old_live]
    constructor <;> simp_all
  simp only [ho, ite_false]
  have same : ∀ k, k ≠ old → k ≠ new → rename S old new k = S k := fun k h1 h2 => by simp [rename, h1, h2]
  by_cases hb : c.before = true
  · simp only [hb, ite_true]
    have hs := hv hb
    have hne : c.key ≠ new := fun h => by
      have := hR.between c.key ho (by omega) (by omega)
      simp_all
    rw [same c.key ho hne, hs]
    constructor <;> simp_all
  simp only [hb, Bool.false_eq_true, ite_false, Bool.not_false, Bool.and_true]
  by_cases hlt : old < new
  · simp only [hlt, ite_true]
    by_cases hin : old < c.key ∧ c.key ≤ new
    · simp only [hin, and_self, ite_true, Bool.not_false, Bool.and_true]
      have r1 : rename S old new (new + 1) = false := by
        simp [rename, show new + 1 ≠ old by omega, hR.succ]
      have r2 : S c.key = false := hR.between c.key ho (by omega) (by omega)
      rw [r1, r2]
      constructor <;> simp_all
    · simp only [hin, ite_false, hb, Bool.not_false, Bool.and_true]
      have hne : c.key ≠ new := fun h => hin ⟨by omega, by omega⟩
      rw [same c.key ho hne]
      constructor <;> simp_all
  · simp only [hlt, ite_false]
    by_cases hin : new ≤ c.key ∧ c.key < old
    · simp only [hin, and_self, ite_true, Bool.not_true, Bool.and_false, hnew]
      have r2 : S c.key = false := hR.between c.key ho (by omega) (by omega)
      rw [r2]
      constructor <;> simp_all
    · simp only [hin, ite_false, hb, Bool.not_false, Bool.and_true]
      have hne : c.key ≠ new := fun h => hin ⟨by omega, by omega⟩
      rw [same c.key ho hne]
      constructor <;> simp_all

end Lsm.FuzzyCursor
