/-!
# Freeing nested dictionaries ends, also when damage makes them cyclic

`ODBDictionaryFieldHandler.FreeContent` registers a dictionary for deletion and frees the content of its values, which
can hold further dictionaries. Dictionaries are never shared, but damage can make a dictionary's values hold that
dictionary, or an enclosing one. Since the 2026-10-28 fix freeing registers each dictionary once and skips one already
registered (`DBReaderWithFreeInfoCtx.TryRegisterDict`). The model's dictionaries are numbers below `n`, `children d`
lists the dictionaries the values of `d` hold, and fuel bounds the nesting the walk may reach, as the stack does.

* `walk_ends`: with the guard, freeing from any dictionary ends within `n + 1` levels of nesting, whatever the
  dictionaries hold, and registers each dictionary once.
* `unguarded_never_ends`: without it, a dictionary whose values hold itself exhausts any nesting bound.
-/

namespace Btdb.DictionaryFree

/-- Distinct numbers below `n` are at most `n`. -/
theorem bounded_nodup_length : ∀ (n : Nat) (l : List Nat), l.Nodup → (∀ x ∈ l, x < n) → l.length ≤ n
  | 0, [], _, _ => Nat.le_refl _
  | 0, x :: _, _, h => absurd (h x (by simp)) (Nat.not_lt_zero _)
  | n + 1, l, hnd, h => by
    by_cases hn : n ∈ l
    · have hlen := List.length_erase_of_mem hn
      have hnd' := hnd.erase n
      have hbound : ∀ x ∈ l.erase n, x < n := by
        intro x hx
        have hxl : x ∈ l := List.erase_sublist.subset hx
        have hlt := h x hxl
        by_cases hxn : x = n
        · subst hxn
          exact absurd hx (List.Nodup.not_mem_erase hnd)
        · omega
      have := bounded_nodup_length n (l.erase n) hnd' hbound
      have : 0 < l.length := List.length_pos_of_mem hn
      omega
    · have hbound : ∀ x ∈ l, x < n := by
        intro x hx
        have := h x hx
        have : x ≠ n := fun he => hn (he ▸ hx)
        omega
      have := bounded_nodup_length n l hnd hbound
      omega

variable (children : Nat → List Nat)

/-- `FreeContent` with the guard: `none` when the nesting bound runs out. -/
def walk : Nat → List Nat → Nat → Option (List Nat)
  | 0, _, _ => none
  | fuel + 1, registered, d =>
    if d ∈ registered then some registered
    else (children d).foldlM (fun r c => walk fuel r c) (d :: registered)

/-- `FreeContent` before the fix, which walked a dictionary's values however often it met it. -/
def unguarded : Nat → Nat → Option Unit
  | 0, _ => none
  | fuel + 1, d => (children d).foldlM (fun _ c => unguarded fuel c) ()

def Bounded (n : Nat) (l : List Nat) : Prop := ∀ x ∈ l, x < n

theorem fold_ends (n fuel : Nat) (hc : ∀ x, ∀ c ∈ children x, c < n)
    (ih : ∀ registered d, Bounded n registered → registered.Nodup → d < n → n - registered.length < fuel →
      ∃ r, walk children fuel registered d = some r ∧ Bounded n r ∧ r.Nodup ∧ registered.length ≤ r.length)
    (base : Nat) :
    ∀ (cs : List Nat) (r : List Nat), (∀ c ∈ cs, c < n) → Bounded n r → r.Nodup → base ≤ r.length →
      n - base < fuel →
      ∃ r', cs.foldlM (fun r c => walk children fuel r c) r = some r' ∧ Bounded n r' ∧ r'.Nodup ∧
        r.length ≤ r'.length
  | [], r, _, hb, hnd, _, _ => ⟨r, rfl, hb, hnd, Nat.le_refl _⟩
  | c :: cs, r, hcs, hb, hnd, hbase, hfuel => by
    obtain ⟨r1, h1, hb1, hnd1, hl1⟩ :=
      ih r c hb hnd (hcs c (by simp)) (by omega)
    obtain ⟨r2, h2, hb2, hnd2, hl2⟩ :=
      fold_ends n fuel hc ih base cs r1 (fun x hx => hcs x (by simp [hx])) hb1 hnd1 (by omega) hfuel
    exact ⟨r2, by simp [List.foldlM, h1, h2], hb2, hnd2, by omega⟩

theorem walk_ok (n : Nat) (hc : ∀ x, ∀ c ∈ children x, c < n) :
    ∀ fuel registered d, Bounded n registered → registered.Nodup → d < n → n - registered.length < fuel →
      ∃ r, walk children fuel registered d = some r ∧ Bounded n r ∧ r.Nodup ∧ registered.length ≤ r.length
  | 0, _, _, _, _, _, h => absurd h (Nat.not_lt_zero _)
  | fuel + 1, registered, d, hb, hnd, hd, hfuel => by
    by_cases hin : d ∈ registered
    · exact ⟨registered, by simp [walk, hin], hb, hnd, Nat.le_refl _⟩
    · have hnd' : (d :: registered).Nodup := List.nodup_cons.mpr ⟨hin, hnd⟩
      have hb' : Bounded n (d :: registered) := by
        intro x hx
        simp only [List.mem_cons] at hx
        rcases hx with rfl | hx
        · exact hd
        · exact hb x hx
      have hlen := bounded_nodup_length n _ hnd' hb'
      simp only [List.length_cons] at hlen
      obtain ⟨r, hr, hbr, hndr, hlr⟩ :=
        fold_ends children n fuel hc (walk_ok n hc fuel) (registered.length + 1) (children d) (d :: registered)
          (hc d) hb' hnd' (by simp) (by omega)
      refine ⟨r, ?_, hbr, hndr, by simp only [List.length_cons] at hlr; omega⟩
      simp only [walk, hin, ite_false]
      exact hr

/-- With the guard, freeing ends within `n + 1` levels from any dictionary and registers each one once. -/
theorem walk_ends (n : Nat) (hc : ∀ x, ∀ c ∈ children x, c < n) (d : Nat) (hd : d < n) :
    ∃ r, walk children (n + 1) [] d = some r ∧ r.Nodup := by
  obtain ⟨r, h, _, hnd, _⟩ := walk_ok children n hc (n + 1) [] d (fun _ h => by simp at h) List.nodup_nil hd
    (by simp)
  exact ⟨r, h, hnd⟩

/-- Without the guard, a dictionary whose values hold itself never ends, whatever the bound. -/
theorem unguarded_never_ends (d : Nat) (hself : children d = [d]) : ∀ fuel, unguarded children fuel d = none
  | 0 => rfl
  | fuel + 1 => by
    simp only [unguarded, hself, List.foldlM, unguarded_never_ends d hself fuel]
    rfl

end Btdb.DictionaryFree
