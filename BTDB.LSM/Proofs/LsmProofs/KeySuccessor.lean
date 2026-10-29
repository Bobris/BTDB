import LsmProofs.PrefixSuccessor

/-!
# The successor of a key

A model of `LsmKeys.Successor` (`../Format/LsmEncoding.cs`), which appends a zero byte to a key. Keys are byte strings
in lexicographic order, `PrefixSuccessor.Lt`, as `SequenceCompareTo` orders them.

* `lt_successor`: the key is below its successor.
* `nothing_between`: no key lies strictly between a key and its successor, so `k ++ [0]` is the next key after `k`.
  `FuzzyCursor.lean` counts keys by position and writes the successor of `new` as `new + 1`; this is the fact that
  justifies it, so a cursor `NotifyRenamed` moves to `Successor(newKey)` sits right after the renamed key.
-/

namespace Lsm.KeySuccessor

open Lsm.PrefixSuccessor

theorem not_lt_nil (x : List Nat) : ¬Lt x [] := by
  cases x <;> simp [Lt]

theorem lt_successor (k : List Nat) : Lt k (k ++ [0]) := by
  induction k with
  | nil => trivial
  | cons a k ih => exact Or.inr ⟨rfl, ih⟩

theorem nothing_between (k x : List Nat) (h1 : Lt k x) (h2 : Lt x (k ++ [0])) : False := by
  induction k generalizing x with
  | nil =>
    cases x with
    | nil => exact h1
    | cons b xs =>
      rcases h2 with h | ⟨_, h⟩
      · exact Nat.not_lt_zero _ h
      · exact not_lt_nil xs h
  | cons a k ih =>
    cases x with
    | nil => exact h1
    | cons b xs =>
      rcases h1 with h1 | ⟨rfl, h1⟩ <;> rcases h2 with h2 | ⟨h2, h2'⟩
      · omega
      · omega
      · omega
      · exact ih xs h1 h2'

end Lsm.KeySuccessor
