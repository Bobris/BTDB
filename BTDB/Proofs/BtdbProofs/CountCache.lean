/-!
# Counting a lazy collection on each call stays right

`ODBDictionary` and `ODBSet` (`../ODBLayer/`) are lazy collections over a key prefix. Loading the owner of one twice in
a transaction gives two instances over the same keys. Before the 2026-10-28 fix each instance cached its count once
read, and adjusted only for its own changes. Now `Count` counts the keys with the prefix on each call.

* `fixed_count`: after any changes made through any instances, every instance's count is the number of keys.
* `previous_stale`: with the cache, one instance reports the old count after the other added a key.
-/

namespace Btdb.CountCache

/-- A change made through instance `by`: a key added or removed, or a count read. -/
inductive Op where
  | add (by_ : Bool) (key : Nat)
  | remove (by_ : Bool) (key : Nat)
  | read (by_ : Bool)

structure State where
  keys : List Nat
  cache : Bool → Option Nat

def State.count (s : State) : Nat := s.keys.length

/-- One change before the fix: a read fills the reader's cache, a change adjusts only the changer's cache. -/
def step (s : State) : Op → State
  | .add i k =>
    if k ∈ s.keys then s
    else { keys := k :: s.keys, cache := fun j => if j = i then (s.cache j).map (· + 1) else s.cache j }
  | .remove i k =>
    if k ∈ s.keys then
      { keys := s.keys.erase k, cache := fun j => if j = i then (s.cache j).map (· - 1) else s.cache j }
    else s
  | .read i => { s with cache := fun j => if j = i then some ((s.cache j).getD s.count) else s.cache j }

/-- What instance `i` reports before the fix and now. -/
def previousCount (s : State) (i : Bool) : Nat := (s.cache i).getD s.count
def fixedCount (s : State) (_ : Bool) : Nat := s.count

theorem fixed_count (ops : List Op) (i : Bool) :
    fixedCount (ops.foldl step ⟨[], fun _ => none⟩) i = (ops.foldl step ⟨[], fun _ => none⟩).keys.length := rfl

/-- Both instances read 1, then `true` adds key 2: `false` still reports 1 of the 2 keys. -/
theorem previous_stale :
    let s := [Op.add true 1, .read true, .read false, .add true 2].foldl step ⟨[], fun _ => none⟩
    previousCount s false = 1 ∧ s.count = 2 := by
  decide

end Btdb.CountCache
