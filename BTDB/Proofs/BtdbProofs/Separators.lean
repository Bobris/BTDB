/-!
# In-memory B-tree separators after erasing one key

A model of how `BTreeBranch.EraseOne` (`BTDB/KVDBLayer/BTreeMem`) rebuilds a branch's separators. A branch with
children `cs` keeps `keys = (cs.drop 1).map first`: the first key of every child but the first, which
`BTreeRoot.FindKey` relies on. With `n` the new child count, the code copies:

* a removed first child: `Array.Copy(_keys, 1, newKeys, 0, n - 1)`, that is `keys.drop 1`;
* a removed child `r > 0`: `keys[0, r - 1)` then `keys[r, r + n - r)`, that is `keys.take (r - 1) ++ keys.drop r`;
* a child `r` that kept keys: all keys, with key `r - 1` replaced by the child's new first key when `r > 0`.

* `erase_first`, `erase_later`, `shrink_first`, `shrink_later`: each copy is the first keys of the new children.
-/

namespace Btdb.Separators

variable {α β : Type} (first : α → β)

theorem erase_first (cs : List α) :
    ((cs.eraseIdx 0).drop 1).map first = ((cs.drop 1).map first).drop 1 := by
  cases cs <;> simp

theorem erase_later (cs : List α) (r : Nat) (hr : 0 < r) :
    ((cs.eraseIdx r).drop 1).map first = ((cs.drop 1).map first).take (r - 1) ++ ((cs.drop 1).map first).drop r := by
  cases cs with
  | nil => simp
  | cons c cs =>
    obtain ⟨r, rfl⟩ : ∃ k, r = k + 1 := ⟨r - 1, by omega⟩
    simp only [List.eraseIdx_cons_succ, List.drop_succ_cons, List.drop_zero, Nat.add_sub_cancel]
    rw [List.eraseIdx_eq_take_drop_succ, List.map_append, List.map_take, List.map_drop]

theorem shrink_first (cs : List α) (c : α) :
    ((cs.set 0 c).drop 1).map first = (cs.drop 1).map first := by
  cases cs <;> simp

theorem shrink_later (cs : List α) (r : Nat) (hr : 0 < r) (c : α) :
    ((cs.set r c).drop 1).map first = ((cs.drop 1).map first).set (r - 1) (first c) := by
  cases cs with
  | nil => simp
  | cons d cs =>
    obtain ⟨r, rfl⟩ : ∃ k, r = k + 1 := ⟨r - 1, by omega⟩
    simp [List.map_set]

end Btdb.Separators
