import BtdbProofs.DictionaryFree

/-!
# Creating the deserializer of recursive types ends

`BonSerializerFactory.CreateCachedDeserializerForType` (`../Serialization/BonSerializer.cs`) creates a type's
deserializer, which first creates those of the types it holds. For a registered class it caches a placeholder before,
so meeting the class again inside itself takes the placeholder; value types, interfaces and collections are created
directly. Before the 2026-10-28 fix only the lookup by class name cached placeholders, so a class holding itself, first
reached as a member type, was created inside itself until the stack overflowed. Types are numbers below `n`, `children
t` lists the types `t` holds, `guarded t` is whether it is a registered class, and fuel bounds the nesting.

Types other than classes cannot hold themselves, even through each other: a struct cannot contain itself, and a
collection's element type is a smaller type. A rank that decreases from such a type to such a type it holds says so.

* `fixed_ends`: then creation ends from any type within `(n + 1) * (R + 2)` levels, `R` bounding the rank.
* `previous_never_ends`: without the placeholder a class holding itself exhausts any bound.
-/

namespace Btdb.DeserializerPlaceholder

open Btdb.DictionaryFree (bounded_nodup_length)

variable (children : Nat → List Nat) (guarded : Nat → Bool)

/-- Creation with the cache of classes that have a placeholder; `none` when the nesting bound runs out. -/
def create : Nat → List Nat → Nat → Option (List Nat)
  | 0, _, _ => none
  | fuel + 1, cache, t =>
    if t ∈ cache then some cache
    else (children t).foldlM (fun c x => create fuel c x) (if guarded t then t :: cache else cache)

def Bounded (n : Nat) (l : List Nat) : Prop := ∀ x ∈ l, x < n

/-- The nesting creation may still need: `R + 2` levels per class not yet cached, plus the rank of a non-class. -/
def need (n R : Nat) (cache : List Nat) (t : Nat) (rank : Nat → Nat) : Nat :=
  (n - cache.length) * (R + 2) + (if guarded t then 0 else rank t + 1)

theorem shrink (n W a b : Nat) (h : a ≤ b) : (n - b) * W ≤ (n - a) * W :=
  Nat.mul_le_mul_right _ (Nat.sub_le_sub_left h n)

theorem step_down (n W k : Nat) (h : k < n) : (n - (k + 1)) * W + W = (n - k) * W := by
  have : n - k = (n - (k + 1)) + 1 := by omega
  rw [this, Nat.succ_mul]

theorem create_ok (n R : Nat) (rank : Nat → Nat) (hc : ∀ x, ∀ c ∈ children x, c < n) (hr : ∀ x, rank x ≤ R)
    (hdec : ∀ x, guarded x = false → ∀ c ∈ children x, guarded c = false → rank c < rank x) :
    ∀ fuel cache t, Bounded n cache → cache.Nodup → t < n → need guarded n R cache t rank < fuel →
      ∃ r, create children guarded fuel cache t = some r ∧ Bounded n r ∧ r.Nodup ∧ cache.length ≤ r.length
  | 0, _, _, _, _, _, h => absurd h (Nat.not_lt_zero _)
  | fuel + 1, cache, t, hb, hnd, ht, hfuel => by
    by_cases hin : t ∈ cache
    · exact ⟨cache, by simp [create, hin], hb, hnd, Nat.le_refl _⟩
    · -- The cache the children start from, and what each child may need.
      let start := if guarded t then t :: cache else cache
      have hstart_b : Bounded n start := by
        intro x hx
        by_cases hg : guarded t <;> simp only [start, hg, ite_true, ite_false, Bool.false_eq_true] at hx
        · simp only [List.mem_cons] at hx
          rcases hx with rfl | hx
          · exact ht
          · exact hb x hx
        · exact hb x hx
      have hstart_nd : start.Nodup := by
        by_cases hg : guarded t <;> simp only [start, hg, ite_true, ite_false, Bool.false_eq_true]
        · exact List.nodup_cons.mpr ⟨hin, hnd⟩
        · exact hnd
      have hlen_le : cache.length ≤ start.length := by
        by_cases hg : guarded t <;> simp [start, hg]
      have hchild : ∀ c ∈ children t, ∀ r, Bounded n r → r.Nodup → start.length ≤ r.length →
          need guarded n R r c rank < fuel := by
        intro c hcm r hbr hndr hlr
        have hrn := bounded_nodup_length n r hndr hbr
        have hcr := hr c
        unfold need at hfuel ⊢
        by_cases hg : guarded t
        · simp only [start, hg, ite_true, List.length_cons] at hlr
          simp only [hg, ite_true, Nat.add_zero] at hfuel
          have h1 := shrink n (R + 2) (cache.length + 1) r.length hlr
          have h2 := step_down n (R + 2) cache.length (by omega)
          by_cases hgc : guarded c <;> simp only [hgc, ite_true, ite_false, Bool.false_eq_true] <;> omega
        · simp only [start, hg, Bool.false_eq_true, ite_false] at hlr
          simp only [hg, Bool.false_eq_true, ite_false] at hfuel
          have h1 := shrink n (R + 2) cache.length r.length hlr
          by_cases hgc : guarded c
          · simp only [hgc, ite_true]
            omega
          · have := hdec t (by simpa using hg) c hcm (by simpa using hgc)
            simp only [hgc, Bool.false_eq_true, ite_false]
            omega
      -- Walk the children, each with the cache the ones before left.
      have fold : ∀ (cs : List Nat) (acc : List Nat), (∀ c ∈ cs, c ∈ children t) → Bounded n acc → acc.Nodup →
          start.length ≤ acc.length →
          ∃ r, cs.foldlM (fun c x => create children guarded fuel c x) acc = some r ∧ Bounded n r ∧ r.Nodup ∧
            acc.length ≤ r.length := by
        intro cs
        induction cs with
        | nil => intro acc _ hb hnd _; exact ⟨acc, rfl, hb, hnd, Nat.le_refl _⟩
        | cons c rest ihc =>
          intro acc hcs hba hnda hla
          have hcm := hcs c (by simp)
          obtain ⟨r1, h1, hb1, hnd1, hl1⟩ :=
            create_ok n R rank hc hr hdec fuel acc c hba hnda (hc t c hcm) (hchild c hcm acc hba hnda hla)
          obtain ⟨r2, h2, hb2, hnd2, hl2⟩ :=
            ihc r1 (fun x hx => hcs x (by simp [hx])) hb1 hnd1 (by omega)
          exact ⟨r2, by simp [List.foldlM, h1, h2], hb2, hnd2, by omega⟩
      obtain ⟨r, hr', hbr, hndr, hlr⟩ := fold (children t) start (fun _ h => h) hstart_b hstart_nd (Nat.le_refl _)
      refine ⟨r, ?_, hbr, hndr, by omega⟩
      simp only [create, hin, ite_false]
      exact hr'

/-- With placeholders for classes, creation from any type ends. -/
theorem fixed_ends (n R : Nat) (rank : Nat → Nat) (hc : ∀ x, ∀ c ∈ children x, c < n) (hr : ∀ x, rank x ≤ R)
    (hdec : ∀ x, guarded x = false → ∀ c ∈ children x, guarded c = false → rank c < rank x) (t : Nat) (ht : t < n) :
    ∃ r, create children guarded ((n + 1) * (R + 2)) [] t = some r := by
  have hlt : need guarded n R [] t rank < (n + 1) * (R + 2) := by
    unfold need
    have := hr t
    rw [Nat.succ_mul]
    simp only [List.length_nil, Nat.sub_zero]
    split <;> omega
  obtain ⟨r, h, _⟩ := create_ok children guarded n R rank hc hr hdec _ [] t (fun _ h => by simp at h)
    List.nodup_nil ht hlt
  exact ⟨r, h⟩

/-- Before the fix the lookup by type cached no placeholder: a class holding itself is created without end. -/
theorem previous_never_ends (t : Nat) (hself : children t = [t]) :
    ∀ fuel, create children (fun _ => false) fuel [] t = none
  | 0 => rfl
  | fuel + 1 => by
    simp only [create, List.not_mem_nil, ite_false, Bool.false_eq_true, hself, List.foldlM,
      previous_never_ends t hself fuel]
    rfl

end Btdb.DeserializerPlaceholder
