/-!
# A database keeps its own files whatever order they are listed in

A model of how `FileCollectionWithFileInfos` picks the database Guid from the Guids in its files' headers; files with
another Guid are foreign, and opening deletes them. Files are the list of their Guids in listing order.

* `majority_chosen`: the rule since the 2026-10-28 fix picks a Guid at least as common as any other, so a Guid more
  than half of the files carry is picked, in every listing order.
* `previous_picks_foreign`: before the fix the first listed file decided, so one foreign file listed first, as a
  directory listing may do, made every file of the database foreign.
-/

namespace Btdb.FileGuid

def count (g : Nat) (files : List Nat) : Nat := (files.filter (· == g)).length

theorem count_pair (c g : Nat) (h : c ≠ g) : ∀ files : List Nat, count c files + count g files ≤ files.length
  | [] => by simp [count]
  | f :: rest => by
    have ih := count_pair c g h rest
    unfold count at ih ⊢
    by_cases hc : f = c
    · subst hc
      have : (f == g) = false := by simp [h]
      simp [this]; omega
    · by_cases hg : f = g
      · subst hg
        have : (f == c) = false := by simp [hc]
        simp [this]; omega
      · have h1 : (f == c) = false := by simp [hc]
        have h2 : (f == g) = false := by simp [hg]
        simp [h1, h2]; omega

/-- Whatever Guid the rule picks is at least as common as every other, so it is the strict majority when one exists. -/
theorem majority_chosen (files : List Nat) (chosen g : Nat) (hmax : ∀ h, count h files ≤ count chosen files)
    (hmajority : files.length < 2 * count g files) : chosen = g := by
  by_cases hc : chosen = g
  · exact hc
  · have := count_pair chosen g hc files
    have := hmax g
    omega

/-- Before the fix: the Guid of the first listed file. -/
def previous (files : List Nat) : Option Nat := files.head?

theorem previous_picks_foreign : previous [7, 1, 1, 1] = some 7 ∧ [7, 1, 1, 1].length < 2 * count 1 [7, 1, 1, 1] := by
  decide

end Btdb.FileGuid
