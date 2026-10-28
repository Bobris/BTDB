/-!
# In-memory B-tree separators after erasing one key

A model of how `BTreeBranch.EraseOne` (`BTDB/KVDBLayer/BTreeMem`) rebuilds a branch's separators. A branch with
children `cs` keeps `keys = (cs.drop 1).map first`: the first key of every child but the first, which
`BTreeRoot.FindKey` relies on. With `n` the new child count, the code copies:

* a removed first child: `Array.Copy(_keys, 1, newKeys, 0, n - 1)`, that is `keys.drop 1`;
* a removed child `r > 0`: `keys[0, r - 1)` then `keys[r, r + n - r)`, that is `keys.take (r - 1) ++ keys.drop r`;
* a child `r` that kept keys: all keys, with key `r - 1` replaced by the child's new first key when `r > 0`.

* `erase_first`, `erase_later`, `shrink_first`, `shrink_later`: each copy is the first keys of the new children.
* `split_child`, `split_branch`: so are the copies `CreateOrUpdate` makes when a child splits and when the branch
  itself splits. An insert changes no other first key: a branch routes a key to a child only when it is not below
  that child's first key, so only the leftmost path, which has no separator, can get a new first key.
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

/-- A child `index` split into `n1` and `n2`, where `n1` keeps the child's first key: `CreateOrUpdate` copies
`keys[0, index)`, then `n2`'s first key, then `keys[index, …)`. -/
theorem split_child (cs : List α) (index : Nat) (h : index < cs.length) (n1 n2 : α)
    (hn1 : first n1 = first cs[index]) :
    ((cs.take index ++ [n1, n2] ++ cs.drop (index + 1)).drop 1).map first =
      ((cs.drop 1).map first).take index ++ [first n2] ++ ((cs.drop 1).map first).drop index := by
  cases index with
  | zero =>
    cases cs with
    | nil => simp at h
    | cons c cs => simp
  | succ i =>
    cases cs with
    | nil => simp at h
    | cons c cs =>
      have hi : i < cs.length := by simp at h; omega
      simp only [List.getElem_cons_succ] at hn1
      simp only [List.take_succ_cons, List.drop_succ_cons, List.cons_append, List.drop_zero, List.map_append,
        List.map_cons, List.map_nil, List.map_take, List.map_drop]
      rw [List.take_add_one, List.getElem?_map, List.getElem?_eq_getElem hi]
      simp [hn1]

/-- A full branch split after `kL` children: the left branch keeps `keys[0, kL - 1)`, the right one `keys[kL, …)`, and
the separator between them is the right branch's first key, which the parent inserts. -/
theorem split_branch (cs : List α) (kL : Nat) :
    ((cs.take kL).drop 1).map first = ((cs.drop 1).map first).take (kL - 1) ∧
      ((cs.drop kL).drop 1).map first = ((cs.drop 1).map first).drop kL := by
  constructor
  · rw [List.drop_take, List.map_take]
  · simp only [List.drop_drop, List.map_drop, Nat.add_comm]

end Btdb.Separators
