import EventLogProofs.Common

/-!
# Background merging never leaves a gap

A model of `EventLogMerger` with two merged levels and fan-out `F > 0`. Split `g + 1` is level-0 object `g`; level-1
object `g` covers splits `g·F + 1 … (g + 1)·F`, and level-2 object `m` covers level-1 objects `m·F … m·F + F − 1`.
A level is merged only once a later split exists (`last < maxSplit`). An object is deleted when a higher-level object
covering it existed at some point: mergers run concurrently, also on a deposed owner, and decide from listings and
verification caches that may be stale, so the covering object may already be gone. Deadlines only protect readers in
the middle of a pass and are not modelled.

* `coverage`: every split is still readable from some existing object of level 0, 1 or 2.
* `latest_split_exists`: the latest split, where tail discovery starts, is never deleted.
-/

namespace EventLog.Merge

structure State where
  /-- Number of splits; split IDs are `1 … splits`. -/
  splits : Nat
  s0 : Nat → Bool
  s1 : Nat → Bool
  s2 : Nat → Bool
  /-- Ghost: objects that ever existed, for example in a stale listing or verification cache. -/
  e0 : Nat → Bool
  e1 : Nat → Bool
  e2 : Nat → Bool

def init : State := ⟨0, fun _ => false, fun _ => false, fun _ => false, fun _ => false, fun _ => false, fun _ => false⟩

def set (f : Nat → Bool) (i : Nat) (b : Bool) : Nat → Bool := fun j => if j = i then b else f j

inductive Step (F : Nat) : State → State → Prop
  | append (σ : State) :
      Step F σ { σ with splits := σ.splits + 1, s0 := set σ.s0 σ.splits true, e0 := set σ.e0 σ.splits true }
  | merge1 (σ : State) (g : Nat) (h : (g + 1) * F < σ.splits) :
      Step F σ { σ with s1 := set σ.s1 g true, e1 := set σ.e1 g true }
  | merge2 (σ : State) (m : Nat) (h : (m + 1) * F * F < σ.splits) :
      Step F σ { σ with s2 := set σ.s2 m true, e2 := set σ.e2 m true }
  /-- A split goes once a level-1 or level-2 object covering it existed. -/
  | delete0 (σ : State) (g : Nat) (h : σ.e1 (g / F) = true ∨ σ.e2 (g / F / F) = true) :
      Step F σ { σ with s0 := set σ.s0 g false }
  /-- A level-1 object goes once a level-2 object covering it existed. -/
  | delete1 (σ : State) (g : Nat) (h : σ.e2 (g / F) = true) :
      Step F σ { σ with s1 := set σ.s1 g false }

inductive Reach (F : Nat) : State → Prop
  | init : Reach F init
  | step {σ τ : State} : Reach F σ → Step F σ τ → Reach F τ

structure Inv (F : Nat) (σ : State) : Prop where
  top : ∀ m, σ.e2 m = true → σ.s2 m = true
  mid : ∀ g, σ.e1 g = true → σ.s1 g = true ∨ σ.s2 (g / F) = true
  low : ∀ g, σ.e0 g = true → σ.s0 g = true ∨ σ.s1 (g / F) = true ∨ σ.s2 (g / F / F) = true
  splits : ∀ g, g < σ.splits → σ.e0 g = true
  eligible1 : ∀ g, σ.e1 g = true → (g + 1) * F < σ.splits
  eligible2 : ∀ m, σ.e2 m = true → (m + 1) * F * F < σ.splits
  ever1 : ∀ g, σ.s1 g = true → σ.e1 g = true
  ever2 : ∀ m, σ.s2 m = true → σ.e2 m = true

theorem set_apply (f : Nat → Bool) (i j : Nat) (b : Bool) : set f i b j = if j = i then b else f j := rfl

theorem inv_init (F : Nat) : Inv F init where
  top _ h := by simp [init] at h
  mid _ h := by simp [init] at h
  low _ h := by simp [init] at h
  splits _ h := by simp [init] at h
  eligible1 _ h := by simp [init] at h
  eligible2 _ h := by simp [init] at h
  ever1 _ h := by simp [init] at h
  ever2 _ h := by simp [init] at h

theorem inv_step {F : Nat} {σ τ : State} (hI : Inv F σ) (h : Step F σ τ) : Inv F τ := by
  cases h with
  | append =>
    exact {
      top := hI.top
      mid := hI.mid
      low := fun g hg => by
        simp only [set_apply] at hg ⊢
        by_cases hgs : g = σ.splits
        · simp [hgs]
        · simp only [hgs, ite_false] at hg ⊢; exact hI.low g hg
      splits := fun g hg => by
        simp only [set_apply]
        by_cases hgs : g = σ.splits
        · simp [hgs]
        · simp only [hgs, ite_false]; exact hI.splits g (by arith)
      eligible1 := fun g hg => by have := hI.eligible1 g hg; dsimp only; omega
      eligible2 := fun m hm => by have := hI.eligible2 m hm; dsimp only; omega
      ever1 := hI.ever1
      ever2 := hI.ever2 }
  | merge1 g hg =>
    exact {
      top := hI.top
      mid := fun g' hg' => by
        simp only [set_apply] at hg' ⊢
        by_cases he : g' = g
        · simp [he]
        · simp only [he, ite_false] at hg' ⊢; exact hI.mid g' hg'
      low := fun g' hg' => by
        simp only [set_apply]
        rcases hI.low g' hg' with h | h | h
        · exact Or.inl h
        · right; left; by_cases he : g' / F = g <;> simp [he, h]
        · right; right; exact h
      splits := hI.splits
      eligible1 := fun g' hg' => by
        simp only [set_apply] at hg'
        by_cases he : g' = g
        · subst he; exact hg
        · simp only [he, ite_false] at hg'; exact hI.eligible1 g' hg'
      eligible2 := hI.eligible2
      ever1 := fun g' hg' => by
        simp only [set_apply] at hg' ⊢
        by_cases he : g' = g
        · simp [he]
        · simp only [he, ite_false] at hg' ⊢; exact hI.ever1 g' hg'
      ever2 := hI.ever2 }
  | merge2 m hm =>
    exact {
      top := fun m' hm' => by
        simp only [set_apply] at hm' ⊢
        by_cases he : m' = m
        · simp [he]
        · simp only [he, ite_false] at hm' ⊢; exact hI.top m' hm'
      mid := fun g hg => by
        simp only [set_apply]
        rcases hI.mid g hg with h | h
        · exact Or.inl h
        · right; by_cases he : g / F = m <;> simp [he, h]
      low := fun g hg => by
        simp only [set_apply]
        rcases hI.low g hg with h | h | h
        · exact Or.inl h
        · right; left; exact h
        · right; right; by_cases he : g / F / F = m <;> simp [he, h]
      splits := hI.splits
      eligible1 := hI.eligible1
      eligible2 := fun m' hm' => by
        simp only [set_apply] at hm'
        by_cases he : m' = m
        · subst he; exact hm
        · simp only [he, ite_false] at hm'; exact hI.eligible2 m' hm'
      ever1 := hI.ever1
      ever2 := fun m' hm' => by
        simp only [set_apply] at hm' ⊢
        by_cases he : m' = m
        · simp [he]
        · simp only [he, ite_false] at hm' ⊢; exact hI.ever2 m' hm' }
  | delete0 g hcover =>
    -- The covering object, or the level-2 object covering it in turn, still exists.
    have hrest : σ.s1 (g / F) = true ∨ σ.s2 (g / F / F) = true := by
      rcases hcover with h | h
      · exact hI.mid _ h
      · exact Or.inr (hI.top _ h)
    exact {
      top := hI.top
      mid := hI.mid
      low := fun g' hg' => by
        simp only [set_apply]
        by_cases he : g' = g
        · subst he; simp only [ite_true]; exact Or.inr hrest
        · simp only [he, ite_false]; exact hI.low g' hg'
      splits := hI.splits
      eligible1 := hI.eligible1
      eligible2 := hI.eligible2
      ever1 := hI.ever1
      ever2 := hI.ever2 }
  | delete1 g hcover =>
    have h2 := hI.top _ hcover
    exact {
      top := hI.top
      mid := fun g' hg' => by
        simp only [set_apply]
        by_cases he : g' = g
        · subst he; simp only [ite_true]; exact Or.inr h2
        · simp only [he, ite_false]; exact hI.mid g' hg'
      low := fun g' hg' => by
        simp only [set_apply]
        rcases hI.low g' hg' with h | h | h
        · exact Or.inl h
        · by_cases he : g' / F = g
          · right; right; rw [he]; exact h2
          · right; left; simp only [he, ite_false]; exact h
        · right; right; exact h
      splits := hI.splits
      eligible1 := hI.eligible1
      eligible2 := hI.eligible2
      ever1 := fun g' hg' => by
        simp only [set_apply] at hg'
        by_cases he : g' = g
        · simp [he] at hg'
        · simp only [he, ite_false] at hg'; exact hI.ever1 g' hg'
      ever2 := hI.ever2 }

theorem reach_inv {F : Nat} {σ : State} (h : Reach F σ) : Inv F σ := by
  induction h with
  | init => exact inv_init F
  | step _ hs ih => exact inv_step ih hs

/-- Every split is still readable from an existing object, whatever deletions concurrent mergers made. -/
theorem coverage {F : Nat} {σ : State} (h : Reach F σ) (g : Nat) (hg : g < σ.splits) :
    σ.s0 g = true ∨ σ.s1 (g / F) = true ∨ σ.s2 (g / F / F) = true :=
  let hI := reach_inv h
  hI.low g (hI.splits g hg)

theorem lt_succ_div_mul (a F : Nat) (hF : 0 < F) : a < (a / F + 1) * F := by
  have := Nat.lt_div_mul_add (a := a) hF
  rw [Nat.add_mul, Nat.one_mul]; omega

/-- The latest split is never deleted: no merged object ever covered it. -/
theorem latest_split_exists {F : Nat} (hF : 0 < F) {σ : State} (h : Reach F σ) (hs : 0 < σ.splits) :
    σ.s0 (σ.splits - 1) = true := by
  have hI := reach_inv h
  rcases hI.low (σ.splits - 1) (hI.splits (σ.splits - 1) (by omega)) with h0 | h1 | h2
  · exact h0
  · -- A level-1 object containing the latest split would have been merged without a later split.
    have := hI.eligible1 ((σ.splits - 1) / F) (hI.ever1 _ h1)
    have := lt_succ_div_mul (σ.splits - 1) F hF
    omega
  · have := hI.eligible2 ((σ.splits - 1) / F / F) (hI.ever2 _ h2)
    have h3 := lt_succ_div_mul ((σ.splits - 1) / F) F hF
    have h4 := lt_succ_div_mul (σ.splits - 1) F hF
    have h5 : ((σ.splits - 1) / F + 1) * F ≤ ((σ.splits - 1) / F / F + 1) * F * F :=
      Nat.mul_le_mul_right F (by omega)
    omega

end EventLog.Merge
