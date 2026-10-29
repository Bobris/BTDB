/-!
# Loading the hash index of the chunk storage

A model of `ChunkStorageInKV.LoadFiles` (`../KVDBLayer/Implementation/ChunkStorageInKV.cs`). Each open of the chunk
storage writes a new hash index file with the entries it stores, in order; an entry for a key already stored (a
chunk that stopped being a leaf) comes later. Opening loads every index file oldest first, and a later entry for a
key replaces an earlier one (`_dict20[key] = value`).

Keys and values are naturals; the history is all entries of all files, oldest file first.

* `load_is_latest`: after loading, each key maps to the value of its last entry in the whole history, so chunks
  stored before any earlier reopen stay found, and a key keeps its latest state.
* `newest_only_loses`: the loading before the 2026-10-29 fix, which read only the newest file and kept a key's first
  entry, loses a chunk stored before the previous reopen
  (`ChunkStorageTest.ItRemembersContentStoredBeforeAndAfterAReopen`).
-/

namespace Btdb.ChunkIndexLoad

abbrev Entry := Nat × Nat

/-- The value of the last entry for `k`. -/
def latest (k : Nat) : List Entry → Option Nat
  | [] => none
  | (k', v) :: rest => match latest k rest with
    | some w => some w
    | none => if k' = k then some v else none

def put (m : Nat → Option Nat) (e : Entry) : Nat → Option Nat := fun k => if k = e.1 then some e.2 else m k

/-- Every file, oldest first, every entry in order, a later one replacing an earlier. -/
def load (files : List (List Entry)) : Nat → Option Nat := files.flatten.foldl put (fun _ => none)

theorem foldl_put (es : List Entry) (m : Nat → Option Nat) (k : Nat) :
    es.foldl put m k = match latest k es with
      | some v => some v
      | none => m k := by
  induction es generalizing m with
  | nil => rfl
  | cons e rest ih =>
    obtain ⟨k', v⟩ := e
    simp only [List.foldl_cons, ih, latest, put]
    cases latest k rest with
    | some w => rfl
    | none =>
      simp only
      by_cases h : k' = k
      · subst h; simp
      · simp [h, Ne.symm h]

theorem load_is_latest (files : List (List Entry)) (k : Nat) : load files k = latest k files.flatten := by
  rw [load, foldl_put]
  cases latest k files.flatten <;> rfl

/-- The loading before the fix: the newest file only, a key's first entry winning. -/
def loadNewestFirstWins (files : List (List Entry)) (k : Nat) : Option Nat :=
  ((files.getLast?.getD []).find? (·.1 = k)).map (·.2)

theorem newest_only_loses :
    let files : List (List Entry) := [[(0, 1)], [(2, 3)]]
    load files 0 = some 1 ∧ loadNewestFirstWins files 0 = none := by
  decide

end Btdb.ChunkIndexLoad
