/-!
# RoaringBitmap.Source keeps exactly the values up to the maximum index

`SourceEnumerablePageReader.TryReadValue` (`../ODBLayer/RoaringBitmap.cs`) reads the values of `RoaringBitmap.Source`
up to the maximum index. Values must come in ascending order of their pages (`value >> 16`), in any order within a
page. Since the 2026-10-29 fix a value of a page after the maximum's ends the values, and a larger value of the
maximum's page is skipped; before, the first value above the maximum ended them.

* `fixed_keeps_valid`: for page ascending values, the values read are exactly those up to the maximum, in order.
* `previous_loses_valid`: stopping at the first value above the maximum lost a later value of the same page.
-/

namespace Btdb.RoaringSource

def page (v : Nat) : Nat := v / 65536

/-- The values read since the fix. -/
def readFixed (max : Nat) (values : List Nat) : List Nat :=
  (values.takeWhile fun v => page v ≤ page max).filter (· ≤ max)

/-- The values read before the fix. -/
def readPrevious (max : Nat) (values : List Nat) : List Nat := values.takeWhile (· ≤ max)

theorem fixed_keeps_valid (max : Nat) (values : List Nat)
    (ascending : values.Pairwise fun a b => page a ≤ page b) :
    readFixed max values = values.filter (· ≤ max) := by
  unfold readFixed
  induction values with
  | nil => rfl
  | cons a rest ih =>
    rw [List.pairwise_cons] at ascending
    by_cases ha : page a ≤ page max
    · simp only [List.takeWhile_cons, ha, decide_true, ite_true, List.filter_cons]
      rw [ih ascending.2]
    · simp only [List.takeWhile_cons, ha, decide_false, Bool.false_eq_true, ite_false, List.filter_nil]
      unfold page at ha ascending
      symm
      rw [List.filter_eq_nil_iff]
      intro b hb
      simp only [List.mem_cons] at hb
      rcases hb with rfl | hb
      · simp only [decide_eq_true_eq]; omega
      · have := ascending.1 b hb; simp only [decide_eq_true_eq]; omega

theorem previous_loses_valid : readPrevious 100 [5, 200, 50] = [5] ∧ readFixed 100 [5, 200, 50] = [5, 50] := by
  decide

end Btdb.RoaringSource
