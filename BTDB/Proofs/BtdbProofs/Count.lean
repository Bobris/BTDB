import BtdbProofs.Seek

/-!
# Counting a relation range by key indices

A model of `RelationDBManipulator.CountWithProposition`, which counts the keys of an advanced enumerator parameter
range (`Count` and `Any` of the generated `ListBy`/`CountBy`/`AnyBy` methods, and the bound on the items
`RemoveByIdAdvancedParam` visits) without scanning them. It finds each bound with `Find(key, prefixLen)` and takes key
indices relative to the first key of the relation prefix. An included end first moves to the last key extending the
end bytes (`FindLastKeyWithPrefix`), and an excluded start to the last key extending the start bytes, when there is
one.

The keys of the prefix are `keys 0 < … < keys (n - 1)`. `Find` answers by its contract (`IKeyValueDBCursor.Find`, see
`Cursor.lean`): the key itself, or the greatest smaller key, or the smallest larger key.

* `window_correct`: a key's index lies between the computed start and end index exactly when the key is in the range:
  at or above an included start, above an excluded start and every key extending it, at or below an included end or
  extending it, below an excluded end.
* `window_bounds`: the window lies within the prefix, so `Math.Max(0, endIndex - startIndex + 1)` is the number of keys
  in the range.
-/

namespace Btdb.Count

open KeyRange Cursor Seek

def Sorted (keys : Nat → List Nat) (n : Nat) : Prop := ∀ i j, i < j → j < n → lt (keys i) (keys j)

def le (a b : List Nat) : Prop := lt a b ∨ a = b

inductive Proposition where
  | ignored
  | included
  | excluded
deriving DecidableEq

/-- The answer of `Find`: the result and the index of the key the cursor is on. -/
inductive Found where
  | exact (i : Nat)
  | previous (i : Nat)
  | next (i : Nat)

/-- The contract of `Find` among the keys of the prefix. -/
def FindOk (keys : Nat → List Nat) (n : Nat) (t : List Nat) : Found → Prop
  | .exact i => i < n ∧ keys i = t
  | .previous i => i < n ∧ lt (keys i) t ∧ ∀ j, i < j → j < n → lt t (keys j)
  | .next i => i < n ∧ lt t (keys i) ∧ ∀ j, j < i → lt (keys j) t

def endIndex (n : Nat) : Proposition → Found → Int
  | .ignored, _ => n - 1
  | .excluded, .exact i => i - 1
  | _, .exact i => i
  | _, .previous i => i
  | _, .next i => (i : Int) - 1

def startIndex : Proposition → Found → Int
  | .ignored, _ => 0
  | .excluded, .exact i => i + 1
  | _, .exact i => i
  | _, .previous i => i + 1
  | _, .next i => i

/-- The bytes `Find` looks for: the last key extending `t` when there is one, else `t` (`FindLastKeyWithPrefix`). -/
def RealLast (keys : Nat → List Nat) (n : Nat) (t r : List Nat) : Prop :=
  (∃ i, i < n ∧ keys i = r ∧ t <+: r ∧ ∀ j, j < n → t <+: keys j → le (keys j) r) ∨
  (r = t ∧ ∀ j, j < n → ¬ t <+: keys j)

def LowerOk : Proposition → List Nat → List Nat → Prop
  | .ignored, _, _ => True
  | .included, s, k => ¬ lt k s
  | .excluded, s, k => ¬ (le k s ∨ s <+: k)

def UpperOk : Proposition → List Nat → List Nat → Prop
  | .ignored, _, _ => True
  | .included, e, k => le k e ∨ e <+: k
  | .excluded, e, k => lt k e

theorem not_le_of_lt {a b : List Nat} (h : lt a b) : ¬ le b a := by
  rintro (h' | rfl)
  · exact lt_irrefl _ (lt_trans _ _ _ h h')
  · exact lt_irrefl _ h

theorem not_lt_of_lt {a b : List Nat} (h : lt a b) : ¬ lt b a :=
  fun h' => lt_irrefl _ (lt_trans _ _ _ h h')

theorem le_trans {a b c : List Nat} (h1 : le a b) (h2 : le b c) : le a c := by
  rcases h1 with h1 | rfl
  · rcases h2 with h2 | rfl
    · exact Or.inl (lt_trans _ _ _ h1 h2)
    · exact Or.inl h1
  · exact h2

theorem lt_or_le (a b : List Nat) : lt a b ∨ le b a := by
  rcases trichotomy a b with h | rfl | h
  · exact Or.inl h
  · exact Or.inr (Or.inr rfl)
  · exact Or.inr (Or.inl h)

/-- Key `j` is at or below the key `Find` looked for exactly when its index is at most the end index. -/
theorem le_end {keys : Nat → List Nat} {n : Nat} (hs : Sorted keys n) {t : List Nat} {f : Found}
    (hf : FindOk keys n t f) {j : Nat} (hj : j < n) : (j : Int) ≤ endIndex n .included f ↔ le (keys j) t := by
  cases f with
  | exact i =>
    obtain ⟨hin, hi⟩ := hf
    simp only [endIndex]
    rcases Nat.lt_trichotomy j i with h | rfl | h
    · exact ⟨fun _ => Or.inl (hi ▸ hs j i h hin), fun _ => by omega⟩
    · exact ⟨fun _ => Or.inr hi, fun _ => by omega⟩
    · exact ⟨fun _ => by omega, fun hl => absurd hl (not_le_of_lt (hi ▸ hs i j h hj))⟩
  | previous i =>
    obtain ⟨hin, hi, hafter⟩ := hf
    simp only [endIndex]
    rcases Nat.lt_trichotomy j i with h | rfl | h
    · exact ⟨fun _ => Or.inl (lt_trans _ _ _ (hs j i h hin) hi), fun _ => by omega⟩
    · exact ⟨fun _ => Or.inl hi, fun _ => by omega⟩
    · exact ⟨fun _ => by omega, fun hl => absurd hl (not_le_of_lt (hafter j h hj))⟩
  | next i =>
    obtain ⟨hin, hi, hbefore⟩ := hf
    simp only [endIndex]
    by_cases h : j < i
    · exact ⟨fun _ => Or.inl (hbefore j h), fun _ => by omega⟩
    · have hgt : lt t (keys j) := by
        rcases Nat.lt_or_ge i j with h' | h'
        · exact lt_trans _ _ _ hi (hs i j h' hj)
        · rw [show j = i by omega]; exact hi
      exact ⟨fun _ => by omega, fun hl => absurd hl (not_le_of_lt hgt)⟩

/-- Key `j` is below the key `Find` looked for exactly when its index is at most the excluded end index. -/
theorem lt_end {keys : Nat → List Nat} {n : Nat} (hs : Sorted keys n) {t : List Nat} {f : Found}
    (hf : FindOk keys n t f) {j : Nat} (hj : j < n) : (j : Int) ≤ endIndex n .excluded f ↔ lt (keys j) t := by
  cases f with
  | exact i =>
    obtain ⟨hin, hi⟩ := hf
    simp only [endIndex]
    by_cases h : j < i
    · exact ⟨fun _ => hi ▸ hs j i h hin, fun _ => by omega⟩
    · have hle : le t (keys j) := by
        rcases Nat.lt_or_ge i j with h' | h'
        · exact Or.inl (hi ▸ hs i j h' hj)
        · rw [show j = i by omega, hi]; exact Or.inr rfl
      exact ⟨fun _ => by omega, fun hl => absurd hle (not_le_of_lt hl)⟩
  | previous i =>
    obtain ⟨hin, hi, hafter⟩ := hf
    simp only [endIndex]
    rcases Nat.lt_trichotomy j i with h | rfl | h
    · exact ⟨fun _ => lt_trans _ _ _ (hs j i h hin) hi, fun _ => by omega⟩
    · exact ⟨fun _ => hi, fun _ => by omega⟩
    · exact ⟨fun _ => by omega, fun hl => absurd hl (not_lt_of_lt (hafter j h hj))⟩
  | next i =>
    obtain ⟨hin, hi, hbefore⟩ := hf
    simp only [endIndex]
    by_cases h : j < i
    · exact ⟨fun _ => hbefore j h, fun _ => by omega⟩
    · have hgt : lt t (keys j) := by
        rcases Nat.lt_or_ge i j with h' | h'
        · exact lt_trans _ _ _ hi (hs i j h' hj)
        · rw [show j = i by omega]; exact hi
      exact ⟨fun _ => by omega, fun hl => absurd hl (not_lt_of_lt hgt)⟩

/-- Key `j` is not below the key `Find` looked for exactly when its index is at least the start index. -/
theorem start_le {keys : Nat → List Nat} {n : Nat} (hs : Sorted keys n) {t : List Nat} {f : Found}
    (hf : FindOk keys n t f) {j : Nat} (hj : j < n) : startIndex .included f ≤ (j : Int) ↔ ¬ lt (keys j) t := by
  have h := lt_end hs hf hj
  cases f <;> simp only [startIndex, endIndex] at h ⊢ <;> rw [← h] <;> omega

/-- Key `j` is above the key `Find` looked for exactly when its index is at least the excluded start index. -/
theorem start_lt {keys : Nat → List Nat} {n : Nat} (hs : Sorted keys n) {t : List Nat} {f : Found}
    (hf : FindOk keys n t f) {j : Nat} (hj : j < n) : startIndex .excluded f ≤ (j : Int) ↔ ¬ le (keys j) t := by
  have h := le_end hs hf hj
  cases f <;> simp only [startIndex, endIndex] at h ⊢ <;> rw [← h] <;> omega

/-- Below the last key extending `t` means at or below `t`, or extending it. -/
theorem real_le {keys : Nat → List Nat} {n : Nat} {t r : List Nat} (h : RealLast keys n t r) {j : Nat}
    (hj : j < n) : le (keys j) r ↔ le (keys j) t ∨ t <+: keys j := by
  rcases h with ⟨i, hin, hi, htr, hlast⟩ | ⟨rfl, hnone⟩
  · constructor
    · intro hk
      by_cases hp : t <+: keys j
      · exact Or.inr hp
      · left
        rcases lt_or_le t (keys j) with hlt | hle
        · exfalso
          rcases hk with hk | hk
          · exact hp (block t t (keys j) r (List.prefix_refl t) htr hlt hk)
          · exact hp (hk ▸ htr)
        · exact hle
    · have htr' : le t r := by
        rcases lt_or_le r t with h | h
        · exact absurd h (not_below_of_prefix t r htr)
        · exact h
      rintro (hk | hk)
      · exact le_trans hk htr'
      · exact hlast j hj hk
  · exact ⟨Or.inl, fun h => h.elim id (fun hp => absurd hp (hnone j hj))⟩

theorem window_correct {keys : Nat → List Nat} {n : Nat} (hs : Sorted keys n)
    {ps pe : Proposition} {s e rs re : List Nat} {fs fe : Found}
    (hrs : if ps = .excluded then RealLast keys n s rs else rs = s) (hfs : ps ≠ .ignored → FindOk keys n rs fs)
    (hre : if pe = .included then RealLast keys n e re else re = e) (hfe : pe ≠ .ignored → FindOk keys n re fe)
    {j : Nat} (hj : j < n) :
    (startIndex ps fs ≤ (j : Int) ∧ (j : Int) ≤ endIndex n pe fe) ↔
      LowerOk ps s (keys j) ∧ UpperOk pe e (keys j) := by
  have hlower : startIndex ps fs ≤ (j : Int) ↔ LowerOk ps s (keys j) := by
    cases ps with
    | ignored => simp only [startIndex, LowerOk, iff_true]; omega
    | included =>
      simp only [ite_false, reduceCtorEq] at hrs
      subst hrs
      exact start_le hs (hfs (by decide)) hj
    | excluded =>
      simp only [ite_true] at hrs
      rw [start_lt hs (hfs (by decide)) hj, real_le hrs hj]
      rfl
  have hupper : (j : Int) ≤ endIndex n pe fe ↔ UpperOk pe e (keys j) := by
    cases pe with
    | ignored => simp only [endIndex, UpperOk, iff_true]; omega
    | included =>
      simp only [ite_true] at hre
      rw [le_end hs (hfe (by decide)) hj, real_le hre hj]
      rfl
    | excluded =>
      simp only [ite_false, reduceCtorEq] at hre
      subst hre
      exact lt_end hs (hfe (by decide)) hj
  rw [hlower, hupper]

theorem window_bounds {keys : Nat → List Nat} {n : Nat} {ps pe : Proposition} {rs re : List Nat}
    {fs fe : Found} (hfs : ps ≠ .ignored → FindOk keys n rs fs) (hfe : pe ≠ .ignored → FindOk keys n re fe) :
    0 ≤ startIndex ps fs ∧ endIndex n pe fe ≤ (n : Int) - 1 := by
  constructor
  · cases ps <;> cases fs <;> simp only [startIndex] <;> omega
  · cases pe with
    | ignored => simp only [endIndex]; omega
    | included =>
      have := hfe (by decide)
      cases fe <;> simp only [FindOk] at this <;> simp only [endIndex] <;> omega
    | excluded =>
      have := hfe (by decide)
      cases fe <;> simp only [FindOk] at this <;> simp only [endIndex] <;> omega

end Btdb.Count
