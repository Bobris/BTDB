/-!
# Loading a key index accepts only keys in order

A model of the check `BTreeKeyValueDB.LoadKeyIndex` (`../../KVDBLayer/Implementation/BTreeKeyValueDB.cs`) makes since
the 2026-10-29 fix while it builds the tree from the stored keys: each key must be greater than the one before
(`CheckOrder`). Keys are compared as byte strings; numbers stand for them here, compared the same way.

* `check_iff_sorted`: the check passes exactly for lists whose keys strictly increase, which the tree being built
  relies on.
* `unsorted_search_misses`: on keys out of order, as damage left them before the fix, a binary search, like the tree's
  lookups, misses a key the tree lists.
-/

namespace Btdb.KeyIndexOrder

/-- `CheckOrder` applied to every key in turn, `last` being the previous key if any. -/
def check : Option Nat → List Nat → Bool
  | _, [] => true
  | none, k :: r => check (some k) r
  | some last, k :: r => decide (last < k) && check (some k) r

def Sorted : List Nat → Prop
  | a :: b :: r => a < b ∧ Sorted (b :: r)
  | _ => True

theorem check_some (last : Nat) : ∀ r, check (some last) r = true ↔ Sorted (last :: r)
  | [] => by simp [check, Sorted]
  | k :: r => by
    simp only [check, Bool.and_eq_true, decide_eq_true_eq, Sorted]
    rw [check_some k r]

theorem check_iff_sorted (keys : List Nat) : check none keys = true ↔ Sorted keys := by
  cases keys with
  | nil => simp [check, Sorted]
  | cons k r => simp only [check]; exact check_some k r

/-- A binary search for `k` in `keys[lo, hi)`, as on sorted keys. -/
def search (keys : Array Nat) (k : Nat) (lo hi : Nat) : Bool :=
  if h : lo < hi then
    let m := (lo + hi) / 2
    if keys[m]! = k then true
    else if keys[m]! < k then search keys k (m + 1) hi else search keys k lo m
  else false
termination_by hi - lo

/-- Keys 1, 7, 5, 3: the key 3, out of order, is listed but not found. -/
theorem unsorted_search_misses :
    check none [1, 7, 5, 3] = false ∧ search #[1, 7, 5, 3] 3 0 4 = false ∧
      search #[1, 3, 5, 7] 3 0 4 = true := by
  refine ⟨by decide, ?_, ?_⟩ <;> simp [search]

end Btdb.KeyIndexOrder
