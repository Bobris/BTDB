import LsmProofs.PrefixSuccessor

/-!
# Keys with a prefix are contiguous

`LsmKeyValueDBCursor.FindKeyIndex(prefix, index)` (`../LsmKeyValueDBCursor.cs`) finds the first key with the prefix,
steps `index` keys on by rank, and accepts the key when it still has the prefix. That is right only when the keys
with a prefix form one run of the sorted keys. Keys are byte strings in lexicographic order, `PrefixSuccessor.Lt`.

* `prefix_between`: a key between two keys with a prefix has the prefix too.
* `prefix_run`: in a strictly sorted list of keys, every key between two positions holding keys with the prefix has
  it, so the keys with the prefix are one run: once a key `index` places after the first of them lacks the prefix,
  no later one has it, and the cursor's final prefix check is all that is needed.
* `nearest_below_decides`, `nearest_above_decides`: when the key nearest below (above) an absent searched key lacks
  the searched key's prefix, no key below (above) it has the prefix, so `LsmKeyValueDBCursor.Find`, checking only
  those neighbours, answers `Previous`, `Next` and `NotFound` right.
-/

namespace Lsm.PrefixRange

open Lsm.PrefixSuccessor

theorem prefix_between (p a b c : List Nat) (ha : IsPrefix p a) (hc : IsPrefix p c) (hab : ¬Lt b a)
    (hbc : ¬Lt c b) : IsPrefix p b := by
  induction p generalizing a b c with
  | nil => trivial
  | cons x p ih =>
    cases a with
    | nil => exact absurd ha id
    | cons a as =>
      cases c with
      | nil => exact absurd hc id
      | cons c cs =>
        obtain ⟨hxa, ha⟩ := ha
        obtain ⟨hxc, hc⟩ := hc
        cases b with
        | nil => exact absurd trivial hab
        | cons y bs =>
          simp only [Lt, not_or, not_and] at hab hbc
          have hya : y = a := by omega
          have hcy : c = y := by omega
          exact ⟨by omega, ih as bs cs ha hc (hab.2 hya) (hbc.2 hcy)⟩

theorem lt_asymm (a b : List Nat) (h : Lt a b) : ¬Lt b a := by
  induction a generalizing b with
  | nil => cases b <;> simp [Lt]
  | cons x as ih =>
    cases b with
    | nil => exact absurd h id
    | cons y bs =>
      simp only [Lt] at h ⊢
      rcases h with h | ⟨rfl, h⟩
      · omega
      · rintro (h' | ⟨_, h'⟩)
        · omega
        · exact ih bs h h'

/-- Keys strictly increasing, as a component's keys are. -/
def Sorted (l : List (List Nat)) : Prop := ∀ i j (hi : i < j) (hj : j < l.length), Lt (l[i]'(by omega)) l[j]

theorem prefix_run (p : List Nat) (l : List (List Nat)) (hs : Sorted l) (i j k : Nat) (hij : i ≤ j) (hjk : j ≤ k)
    (hk : k < l.length) (hi : IsPrefix p (l[i]'(by omega))) (hkp : IsPrefix p l[k]) :
    IsPrefix p (l[j]'(by omega)) := by
  apply prefix_between p (l[i]'(by omega)) _ l[k] hi hkp
  · rcases Nat.lt_or_eq_of_le hij with h | h
    · exact lt_asymm _ _ (hs i j h (by omega))
    · subst h
      intro h'
      exact lt_asymm _ _ h' h'
  · rcases Nat.lt_or_eq_of_le hjk with h | h
    · exact lt_asymm _ _ (hs j k h hk)
    · subst h
      intro h'
      exact lt_asymm _ _ h' h'

/-- `LsmKeyValueDBCursor.Find(key, prefixLen)` reports the key `SeekLE` finds below an absent key as the previous one
only when it has the prefix. When the nearest key below lacks it, no key below has it: the searched key has the
prefix, so a key with it further down would put the nearest one in between. -/
theorem nearest_below_decides (p k x : List Nat) (keys : List (List Nat)) (hk : IsPrefix p k)
    (hx : Lt x k) (hnearest : ∀ y ∈ keys, Lt y k → ¬Lt x y) (hxp : ¬IsPrefix p x) :
    ∀ y ∈ keys, Lt y k → ¬IsPrefix p y := by
  intro y hy hyk hyp
  exact hxp (prefix_between p y x k hyp hk (hnearest y hy hyk) (lt_asymm x k hx))

/-- Likewise `Find` reports the key after an absent one as the next one only when it has the prefix: when the nearest
key above lacks it, no key above has it. -/
theorem nearest_above_decides (p k x : List Nat) (keys : List (List Nat)) (hk : IsPrefix p k)
    (hx : Lt k x) (hnearest : ∀ y ∈ keys, Lt k y → ¬Lt y x) (hxp : ¬IsPrefix p x) :
    ∀ y ∈ keys, Lt k y → ¬IsPrefix p y := by
  intro y hy hky hyp
  exact hxp (prefix_between p k x y hk hyp (lt_asymm k x hx) (hnearest y hy hky))

end Lsm.PrefixRange
