/-!
# Generated file names are unique ignoring case

Roslyn requires the hint names of a generator's files to differ ignoring case, which names of types need not. The
source generator's `UniqueHintName` keeps the names it returned, folded, and tries the requested name, then its stem
with the suffixes `_2`, `_3`, ... until a candidate is free. Candidates are modelled by their folded keys `g i`: `g 0`
is the requested name, `g (i + 1)` the stem with suffix `i + 2`. Distinct suffixes keep candidates distinct ignoring
case, so `g` is injective.

* `terminates`: the unbounded search succeeds within `used.length + 1` candidates (pigeonhole: `pigeon`).
* `unique_keeps_free_name`: a name nobody took yet is returned unchanged, so existing file names stay as they were.
* `run_nodup`: every name a run of requests returns differs from all others ignoring case.
-/

namespace Btdb.HintNames

variable {K : Type}

def Inj (g : Nat → K) : Prop := ∀ i j, g i = g j → i = j

/-- More distinct keys than `used` holds cannot all be in it. -/
theorem pigeon [DecidableEq K] : ∀ (used : List K) (g : Nat → K), Inj g → ∀ n,
    (∀ i, i < n → g i ∈ used) → n ≤ used.length
  | [], g, _, n, h => by
    cases n with
    | zero => exact Nat.le_refl _
    | succ n => exact absurd (h 0 (Nat.succ_pos _)) (by simp)
  | a :: rest, g, hg, n, h => by
    by_cases hk : ∃ k, k < n ∧ g k = a
    · obtain ⟨k, hkn, hka⟩ := hk
      -- Skip the one candidate equal to `a`; the others are all in `rest`.
      let g' : Nat → K := fun i => g (if i < k then i else i + 1)
      have hg' : Inj g' := by
        intro i j hij
        have := hg _ _ hij
        by_cases hi : i < k <;> by_cases hj : j < k <;> simp [hi, hj] at this <;> omega
      have h' : ∀ i, i < n - 1 → g' i ∈ rest := by
        intro i hi
        have hmem := h (if i < k then i else i + 1) (by split <;> omega)
        simp only [List.mem_cons] at hmem
        rcases hmem with he | hr
        · exfalso
          have := hg _ _ (he.trans hka.symm)
          split at this <;> omega
        · exact hr
      have := pigeon rest g' hg' (n - 1) h'
      simp only [List.length_cons]
      omega
    · have h' : ∀ i, i < n → g i ∈ rest := by
        intro i hi
        have hmem := h i hi
        simp only [List.mem_cons] at hmem
        rcases hmem with he | hr
        · exact absurd ⟨i, hi, he⟩ hk
        · exact hr
      have := pigeon rest g hg n h'
      simp only [List.length_cons]
      omega

/-- The first of the candidates `0 .. n - 1` that is free. -/
def firstFresh [DecidableEq K] (used : List K) (g : Nat → K) : Nat → Option Nat
  | 0 => none
  | n + 1 =>
    match firstFresh used g n with
    | some i => some i
    | none => if g n ∈ used then none else some n

theorem firstFresh_some [DecidableEq K] {used : List K} {g : Nat → K} :
    ∀ {n i}, firstFresh used g n = some i → i < n ∧ g i ∉ used
  | 0, _, h => by simp [firstFresh] at h
  | n + 1, i, h => by
    simp only [firstFresh] at h
    split at h
    · have := firstFresh_some ‹_›
      cases h
      exact ⟨by omega, this.2⟩
    · split at h
      · cases h
      · cases h
        exact ⟨by omega, ‹_›⟩

theorem firstFresh_none [DecidableEq K] {used : List K} {g : Nat → K} :
    ∀ {n}, firstFresh used g n = none → ∀ i, i < n → g i ∈ used
  | 0, _, i, hi => absurd hi (Nat.not_lt_zero _)
  | n + 1, h, i, hi => by
    simp only [firstFresh] at h
    split at h
    · cases h
    · rename_i hn
      split at h
      · rename_i hgn
        by_cases hin : i < n
        · exact firstFresh_none hn i hin
        · have : i = n := by omega
          subst this
          exact hgn
      · cases h

theorem firstFresh_mono [DecidableEq K] {used : List K} {g : Nat → K} {i : Nat} :
    ∀ {n} m, firstFresh used g n = some i → firstFresh used g (n + m) = some i
  | _, 0, h => h
  | n, m + 1, h => by
    have ih := firstFresh_mono m h
    show firstFresh used g (n + m + 1) = some i
    simp only [firstFresh, ih]

/-- The search ends within `used.length + 1` candidates, at a free one. -/
theorem terminates [DecidableEq K] (used : List K) (g : Nat → K) (hg : Inj g) :
    ∃ i, firstFresh used g (used.length + 1) = some i ∧ i ≤ used.length ∧ g i ∉ used := by
  cases h : firstFresh used g (used.length + 1) with
  | none =>
    have := pigeon used g hg (used.length + 1) (firstFresh_none h)
    omega
  | some i =>
    have := firstFresh_some h
    exact ⟨i, rfl, by omega, this.2⟩

/-- `UniqueHintName`: the first free candidate; the search bound is never reached for injective candidates. -/
def unique [DecidableEq K] (used : List K) (g : Nat → K) : K :=
  match firstFresh used g (used.length + 1) with
  | some i => g i
  | none => g 0

theorem unique_fresh [DecidableEq K] (used : List K) (g : Nat → K) (hg : Inj g) : unique used g ∉ used := by
  obtain ⟨i, h, _, hi⟩ := terminates used g hg
  simp only [unique, h]
  exact hi

theorem unique_keeps_free_name [DecidableEq K] (used : List K) (g : Nat → K) (h0 : g 0 ∉ used) :
    unique used g = g 0 := by
  have h1 : firstFresh used g 1 = some 0 := by simp [firstFresh, h0]
  have := firstFresh_mono used.length h1
  rw [Nat.add_comm] at this
  simp only [unique, this]

/-- Requests in order, each adding the name returned to the names taken. -/
def run [DecidableEq K] : List K → List (Nat → K) → List K
  | used, [] => used
  | used, g :: gs => run (unique used g :: used) gs

theorem run_nodup [DecidableEq K] : ∀ (used : List K) (gs : List (Nat → K)), used.Nodup →
    (∀ g, g ∈ gs → Inj g) → (run used gs).Nodup
  | _, [], h, _ => h
  | used, g :: gs, h, hs =>
    run_nodup (unique used g :: used) gs
      (List.nodup_cons.mpr ⟨unique_fresh used g (hs g (by simp)), h⟩)
      (fun g' hg' => hs g' (by simp [hg']))

end Btdb.HintNames
