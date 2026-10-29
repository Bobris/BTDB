/-!
# RemoveByIdPartial leaves only kept rows once it removes fewer than its limit

`RelationDBManipulator.RemoveByPrimaryKeyPrefixPartial` (`../ODBLayer/RelationDBManipulator.cs`) removes the rows of a
key prefix in order, skipping the rows an `OnBeforeRemove` method keeps, until it has removed `maxCount` rows. Before
the 2026-10-29 fix it stopped after visiting `maxCount` rows, kept ones included. Rows are their keep flags here.

* `fixed_leaves_only_kept`: when it removes fewer than `maxCount` rows, every row left is kept, so calling it until it
  removes fewer than its limit removes every removable row.
* `previous_leaves_removable`: before the fix a call removed nothing although a removable row was left.
-/

namespace Btdb.RemovePartial

/-- Since the fix: the number removed and the rows left. -/
def removeFixed : Nat → List Bool → Nat × List Bool
  | _, [] => (0, [])
  | 0, rows => (0, rows)
  | max + 1, keep :: rest =>
    if keep then
      let (removed, left) := removeFixed (max + 1) rest
      (removed, keep :: left)
    else
      let (removed, left) := removeFixed max rest
      (removed + 1, left)

/-- Before the fix: at most `max` rows were visited. -/
def removePrevious : Nat → List Bool → Nat × List Bool
  | _, [] => (0, [])
  | 0, rows => (0, rows)
  | max + 1, keep :: rest =>
    let (removed, left) := removePrevious max rest
    if keep then (removed, keep :: left) else (removed + 1, left)

theorem fixed_leaves_only_kept : ∀ (max : Nat) (rows : List Bool),
    (removeFixed max rows).1 < max → ∀ keep ∈ (removeFixed max rows).2, keep = true
  | _, [], _ => by simp [removeFixed]
  | 0, _ :: _, h => by simp [removeFixed] at h
  | max + 1, keep :: rest, h => by
    cases keep with
    | true =>
      simp only [removeFixed, ite_true] at h ⊢
      intro k hk
      simp only [List.mem_cons] at hk
      rcases hk with rfl | hk
      · rfl
      · exact fixed_leaves_only_kept (max + 1) rest h k hk
    | false =>
      simp only [removeFixed, Bool.false_eq_true, ite_false] at h ⊢
      exact fixed_leaves_only_kept max rest (by omega)

/-- Three kept rows then a removable one, with a limit of 3. -/
theorem previous_leaves_removable :
    removePrevious 3 [true, true, true, false] = (0, [true, true, true, false]) ∧
    removeFixed 3 [true, true, true, false] = (1, [true, true, true]) := by
  decide

end Btdb.RemovePartial
