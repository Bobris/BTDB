/-!
# Background merging never leaves a gap

A model of `EventLogMerger` with `L` merged levels (`EventLogOptions.MergeLevels`) and fan-out `F > 0`
(`MergeFanOut`). Split `g + 1` is level-0 object `g`; level-`l` object `g` covers splits `g·Fˡ + 1 … (g + 1)·Fˡ`, and
the level-`l'` object covering it is `g / F^(l' − l)`. A level is merged only once a later split exists
(`last < maxSplit`). An object below the top level is deleted when a higher-level object covering it existed at some
point: mergers run concurrently, also on a deposed owner, and decide from listings and verification caches that may be
stale, so the covering object may already be gone. Deadlines only protect readers in the middle of a pass and are not
modelled.

* `coverage`: every split is still readable from some existing object.
* `latest_split_exists`: the latest split, where tail discovery starts, is never deleted.
* `old_listing_ends_early`: a split after the last one of an old listing can be deleted while history continues, so
  a reader walking past an old listing must not take a missing split for the end of history
  (`EventLogStorageReader.TryLocateAsync`).
* `fresh_listing_end`: in a listing taken now, the last split is the latest one, so a split missing after it is past
  the end of history.
* `torn_listing_ends_early`: a listing returns splits before merged objects (`l1/` sorts after the digits), and its
  later page may show a merged object made after it listed the splits. That object covers every listed split while
  history continues.
* `merged_has_successor`: after every merged object anyone listed, history continues with a readable split, so a
  reader walks on from it instead of ending there.
-/

namespace EventLog.Merge

structure State where
  /-- Number of splits; split IDs are `1 … splits`. -/
  splits : Nat
  /-- Existing objects by level and number. -/
  obj : Nat → Nat → Bool
  /-- Ghost: objects that ever existed, for example in a stale listing or verification cache. -/
  ever : Nat → Nat → Bool

def init : State := ⟨0, fun _ _ => false, fun _ _ => false⟩

def set (f : Nat → Nat → Bool) (l g : Nat) (b : Bool) : Nat → Nat → Bool :=
  fun l' g' => if l' = l ∧ g' = g then b else f l' g'

inductive Step (L F : Nat) : State → State → Prop
  | append (σ : State) :
      Step L F σ { σ with splits := σ.splits + 1, obj := set σ.obj 0 σ.splits true, ever := set σ.ever 0 σ.splits true }
  | merge (σ : State) (l g : Nat) (hl : 1 ≤ l ∧ l ≤ L) (h : (g + 1) * F ^ l < σ.splits) :
      Step L F σ { σ with obj := set σ.obj l g true, ever := set σ.ever l g true }
  /-- An object goes once a higher-level object covering it existed. -/
  | delete (σ : State) (l g l' : Nat) (hl : l < l' ∧ l' ≤ L) (h : σ.ever l' (g / F ^ (l' - l)) = true) :
      Step L F σ { σ with obj := set σ.obj l g false }

inductive Reach (L F : Nat) : State → Prop
  | init : Reach L F init
  | step {σ τ : State} : Reach L F σ → Step L F σ τ → Reach L F τ

/-- Steps from one state to a later one. -/
inductive Steps (L F : Nat) : State → State → Prop
  | refl (σ : State) : Steps L F σ σ
  | step {σ τ υ : State} : Steps L F σ τ → Step L F τ υ → Steps L F σ υ

theorem reach_steps {L F : Nat} {σ τ : State} (h : Reach L F σ) (hs : Steps L F σ τ) : Reach L F τ := by
  induction hs with
  | refl => exact h
  | step _ hst ih => exact Reach.step ih hst

theorem steps_splits {L F : Nat} {σ τ : State} (hs : Steps L F σ τ) : σ.splits ≤ τ.splits := by
  induction hs with
  | refl => exact Nat.le_refl _
  | step _ hst ih => cases hst <;> dsimp only <;> omega

/-- An object is covered by an existing object of its own or a higher level. -/
def Covered (L F : Nat) (obj : Nat → Nat → Bool) (l g : Nat) : Prop :=
  ∃ l', l ≤ l' ∧ l' ≤ L ∧ obj l' (g / F ^ (l' - l)) = true

structure Inv (L F : Nat) (σ : State) : Prop where
  covered : ∀ l g, l ≤ L → σ.ever l g = true → Covered L F σ.obj l g
  levels : ∀ l g, σ.ever l g = true → l ≤ L
  splits : ∀ g, g < σ.splits → σ.ever 0 g = true
  eligible : ∀ l g, 1 ≤ l → σ.ever l g = true → (g + 1) * F ^ l < σ.splits
  ever_of_obj : ∀ l g, σ.obj l g = true → σ.ever l g = true
  splits_lt : ∀ g, σ.ever 0 g = true → g < σ.splits

theorem div_pow_div (g F a b : Nat) : g / F ^ a / F ^ b = g / F ^ (a + b) := by
  rw [Nat.div_div_eq_div_mul, Nat.pow_add]

theorem set_apply (f : Nat → Nat → Bool) (l g l' g' : Nat) (b : Bool) :
    set f l g b l' g' = if l' = l ∧ g' = g then b else f l' g' := rfl

theorem inv_init (L F : Nat) : Inv L F init where
  covered _ _ _ h := by simp [init] at h
  levels _ _ h := by simp [init] at h
  splits _ h := by simp [init] at h
  eligible _ _ _ h := by simp [init] at h
  ever_of_obj _ _ h := by simp [init] at h
  splits_lt _ h := by simp [init] at h

/-- Adding an object keeps every coverage. -/
theorem covered_add {L F : Nat} {obj : Nat → Nat → Bool} {l g l₀ g₀ : Nat} (h : Covered L F obj l g) :
    Covered L F (set obj l₀ g₀ true) l g := by
  obtain ⟨l', h1, h2, h3⟩ := h
  refine ⟨l', h1, h2, ?_⟩
  simp only [set_apply]; split <;> simp_all

theorem ever_set {f : Nat → Nat → Bool} {l g l' g' : Nat} (h : set f l g true l' g' = true) :
    (l' = l ∧ g' = g) ∨ f l' g' = true := by
  simp only [set_apply] at h
  split at h
  · rename_i he; exact Or.inl he
  · exact Or.inr h

theorem inv_step {L F : Nat} {σ τ : State} (hI : Inv L F σ) (h : Step L F σ τ) : Inv L F τ := by
  cases h with
  | append =>
    exact {
      covered := fun l g hl hg => by
        rcases ever_set hg with ⟨rfl, rfl⟩ | hg
        · exact ⟨0, Nat.le_refl _, hl, by simp [set_apply]⟩
        · exact covered_add (hI.covered l g hl hg)
      levels := fun l g hg => by
        rcases ever_set hg with ⟨rfl, _⟩ | hg
        · exact Nat.zero_le _
        · exact hI.levels l g hg
      splits := fun g hg => by
        simp only [set_apply]
        by_cases he : g = σ.splits
        · simp [he]
        · simp only [he, and_false, ite_false]; exact hI.splits g (by dsimp only at hg; omega)
      eligible := fun l g hl hg => by
        rcases ever_set hg with ⟨rfl, _⟩ | hg
        · omega
        · have := hI.eligible l g hl hg; dsimp only; omega
      ever_of_obj := fun l g hg => by
        rcases ever_set hg with ⟨rfl, rfl⟩ | hg
        · simp [set_apply]
        · have := hI.ever_of_obj l g hg; simp only [set_apply]; split <;> simp_all
      splits_lt := fun g hg => by
        rcases ever_set hg with ⟨_, rfl⟩ | hg
        · dsimp only; omega
        · have := hI.splits_lt g hg; dsimp only; omega }
  | merge l₀ g₀ hl h =>
    exact {
      covered := fun l g hlL hg => by
        rcases ever_set hg with ⟨rfl, rfl⟩ | hg
        · exact ⟨l, Nat.le_refl _, hlL, by simp [set_apply]⟩
        · exact covered_add (hI.covered l g hlL hg)
      levels := fun l g hg => by
        rcases ever_set hg with ⟨rfl, _⟩ | hg
        · exact hl.2
        · exact hI.levels l g hg
      splits := fun g hg => by
        have := hI.splits g hg; simp only [set_apply]; split <;> simp_all
      eligible := fun l g hl' hg => by
        rcases ever_set hg with ⟨rfl, rfl⟩ | hg
        · exact h
        · exact hI.eligible l g hl' hg
      ever_of_obj := fun l g hg => by
        rcases ever_set hg with ⟨rfl, rfl⟩ | hg
        · simp [set_apply]
        · have := hI.ever_of_obj l g hg; simp only [set_apply]; split <;> simp_all
      splits_lt := fun g hg => by
        rcases ever_set hg with ⟨he, rfl⟩ | hg
        · omega
        · exact hI.splits_lt g hg }
  | delete l₀ g₀ l₁ hl h =>
    -- The covering object, or one covering it in turn, still exists and is not the deleted one.
    obtain ⟨l₂, h1, h2, h3⟩ := hI.covered l₁ _ hl.2 h
    have hcov : ∀ l g, l ≤ l₀ → g / F ^ (l₀ - l) = g₀ → σ.obj l₂ (g / F ^ (l₂ - l)) = true := by
      intro l g hle hg
      rw [← hg, div_pow_div, div_pow_div] at h3
      rwa [show l₀ - l + (l₁ - l₀ + (l₂ - l₁)) = l₂ - l by omega] at h3
    exact {
      covered := fun l g hlL hg => by
        obtain ⟨l', h1', h2', h3'⟩ := hI.covered l g hlL hg
        by_cases he : l' = l₀ ∧ g / F ^ (l' - l) = g₀
        · obtain ⟨hl', hg'⟩ := he
          subst hl'
          have h4 := hcov l g h1' hg'
          have h5 : l₂ ≠ l' := by omega
          exact ⟨l₂, by omega, h2, by simp only [set_apply]; simp [h5, h4]⟩
        · exact ⟨l', h1', h2', by simp only [set_apply]; simp [he, h3']⟩
      levels := hI.levels
      splits := hI.splits
      eligible := hI.eligible
      ever_of_obj := fun l g hg => by
        simp only [set_apply] at hg
        split at hg
        · cases hg
        · exact hI.ever_of_obj l g hg
      splits_lt := hI.splits_lt }

theorem reach_inv {L F : Nat} {σ : State} (h : Reach L F σ) : Inv L F σ := by
  induction h with
  | init => exact inv_init L F
  | step _ hs ih => exact inv_step ih hs

/-- Every split is still readable from an existing object, whatever deletions concurrent mergers made. -/
theorem coverage {L F : Nat} {σ : State} (h : Reach L F σ) (g : Nat) (hg : g < σ.splits) :
    ∃ l, l ≤ L ∧ σ.obj l (g / F ^ l) = true := by
  have hI := reach_inv h
  obtain ⟨l, _, h2, h3⟩ := hI.covered 0 g (Nat.zero_le _) (hI.splits g hg)
  exact ⟨l, h2, by simpa using h3⟩

theorem lt_succ_div_mul (a G : Nat) (hG : 0 < G) : a < (a / G + 1) * G := by
  have := Nat.lt_div_mul_add (a := a) hG
  rw [Nat.add_mul, Nat.one_mul]; omega

/-- The latest split is never deleted: no merged object ever covered it. -/
theorem latest_split_exists {L F : Nat} (hF : 0 < F) {σ : State} (h : Reach L F σ) (hs : 0 < σ.splits) :
    σ.obj 0 (σ.splits - 1) = true := by
  have hI := reach_inv h
  obtain ⟨l, _, hobj⟩ := coverage h (σ.splits - 1) (by omega)
  by_cases hl : l = 0
  · subst hl; simpa using hobj
  · have := hI.eligible l _ (by omega) (hI.ever_of_obj _ _ hobj)
    have := lt_succ_div_mul (σ.splits - 1) (F ^ l) (Nat.pow_pos hF)
    omega

/-- After a listing of splits 1 and 2, three more splits arrive, splits 3 and 4 merge into a level-1 object and split
3 is deleted. A reader walking on from split 2 of its old listing finds split 3 missing, although splits 3 to 5 are
history. -/
theorem old_listing_ends_early : ∃ σ₀ σ, Reach 1 2 σ₀ ∧ Reach 1 2 σ ∧ σ₀.splits = 2 ∧
    σ₀.obj 0 1 = true ∧ σ.obj 0 2 = false ∧ 2 < σ.splits ∧ (∀ g, g < σ.splits → ∃ l, l ≤ 1 ∧ σ.obj l (g / 2 ^ l) = true) := by
  have r1 := Reach.step Reach.init (Step.append (L := 1) (F := 2) init)
  have r2 := Reach.step r1 (Step.append _)
  have r3 := Reach.step r2 (Step.append _)
  have r4 := Reach.step r3 (Step.append _)
  have r5 := Reach.step r4 (Step.append _)
  have r6 := Reach.step r5 (Step.merge _ 1 1 (by decide) (by decide))
  have r7 := Reach.step r6 (Step.delete _ 0 2 1 (by decide) (by decide))
  exact ⟨_, _, r2, r7, rfl, rfl, rfl, by decide, fun g hg => coverage r7 g hg⟩

/-- In a listing taken now, every split after the last listed one is past the end of history. -/
theorem fresh_listing_end {L F : Nat} (hF : 0 < F) {σ : State} (h : Reach L F σ) (hs : 0 < σ.splits) :
    σ.obj 0 (σ.splits - 1) = true ∧ ∀ g, σ.splits ≤ g → σ.obj 0 g = false := by
  have hI := reach_inv h
  refine ⟨latest_split_exists hF h hs, fun g hg => ?_⟩
  cases ho : σ.obj 0 g
  · rfl
  · have := hI.splits_lt g (hI.ever_of_obj 0 g ho); omega

/-- A listing returns splits 1 and 2, then three more splits arrive, splits 1 and 2 merge into level-1 object 0 and
the listing's later page returns it. It covers every listed split, yet split 3 exists and holds history. -/
theorem torn_listing_ends_early : ∃ σ₀ σ, Reach 1 2 σ₀ ∧ Steps 1 2 σ₀ σ ∧ σ₀.splits = (0 + 1) * 2 ^ 1 ∧
    σ.obj 1 0 = true ∧ σ.obj 0 2 = true ∧ 2 < σ.splits := by
  have r2 := Reach.step (Reach.step Reach.init (Step.append (L := 1) (F := 2) init)) (Step.append _)
  refine ⟨_, _, r2, Steps.step (Steps.step (Steps.refl _) (Step.append _)) (Step.merge _ 1 0 (by decide) ?_),
    rfl, rfl, rfl, by decide⟩
  decide

/-- Once a merged object existed, in that state and every later one the split after it is history and readable from
an existing object. -/
theorem merged_has_successor {L F : Nat} {σ τ : State} (h : Reach L F σ) (hs : Steps L F σ τ) {l g : Nat}
    (hl : 1 ≤ l) (hg : σ.ever l g = true) :
    (g + 1) * F ^ l < τ.splits ∧ ∃ l', l' ≤ L ∧ τ.obj l' ((g + 1) * F ^ l / F ^ l') = true := by
  have := (reach_inv h).eligible l g hl hg
  have hlt : (g + 1) * F ^ l < τ.splits := Nat.lt_of_lt_of_le this (steps_splits hs)
  exact ⟨hlt, coverage (reach_steps h hs) _ hlt⟩

end EventLog.Merge
