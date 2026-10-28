/-!
# Walking a HashSet's entries writes exactly its items

Bon, the event store and ObjectDB save a `HashSet<T>` by walking the first `_count` of its entries and skipping those
whose `Next` is below `-1` (`RawData.HashSetEntries`, `../Serialization/BonSerializer.cs`,
`../EventStoreLayer/ListTypeDescriptor.cs`, `../FieldHandler/ListFieldHandler.cs`). The event store and ObjectDB write
`_count - _freeCount` as the number of items before them, so a reader takes the stream apart right only when the walk
writes that many. In a live entry `Next` is the index of the next entry in its bucket chain or `-1` at the chain's end; a
removed entry joins the free list, and its `Next` is `-3` minus the next free index, or `-2` at the list's end.

* `skipped_iff_free`: the check skips exactly the removed entries, the last one on the free list included.
* `written_count`: so the walk writes `_count - _freeCount` items, the number written before them.
* `stricter_check_writes_freed`: a check that took only `Next ≤ -3` for removed would write the last removed entry.
-/

namespace Btdb.HashSetEntries

/-- An entry among the first `_count`; the index is where its `Next` points, `none` at the end of its list. -/
inductive Entry where
  | live (next : Option Nat)
  | free (next : Option Nat)

def Entry.isFree : Entry → Bool
  | .live _ => false
  | .free _ => true

/-- The `Next` field `HashSet` keeps. -/
def Entry.next : Entry → Int
  | .live none => -1
  | .live (some n) => n
  | .free none => -2
  | .free (some n) => -3 - n

/-- The savers' check. -/
def skipped (e : Entry) : Bool := decide (e.next < -1)

theorem skipped_iff_free (e : Entry) : skipped e = e.isFree := by
  cases e with
  | live n =>
    cases n with
    | none => decide
    | some n =>
      show decide ((n : Int) < -1) = false
      exact decide_eq_false (by omega)
  | free n =>
    cases n with
    | none => decide
    | some n =>
      show decide (-3 - (n : Int) < -1) = true
      exact decide_eq_true (by omega)

theorem filter_add : ∀ entries : List Entry,
    (entries.filter (fun e => !e.isFree)).length + (entries.filter Entry.isFree).length = entries.length
  | [] => rfl
  | .live n :: rest => by
    rw [List.filter_cons_of_pos (by rfl), List.filter_cons_of_neg (by simp [Entry.isFree])]
    have := filter_add rest
    simp only [List.length_cons]
    omega
  | .free n :: rest => by
    rw [List.filter_cons_of_neg (by simp [Entry.isFree]), List.filter_cons_of_pos (by rfl)]
    have := filter_add rest
    simp only [List.length_cons]
    omega

/-- `_freeCount` counts the removed entries, so the walk writes the number the savers write before the items. -/
theorem written_count (entries : List Entry) (freeCount : Nat)
    (h : freeCount = (entries.filter Entry.isFree).length) :
    (entries.filter (fun e => !skipped e)).length = entries.length - freeCount := by
  have hs : (fun e => !skipped e) = (fun e : Entry => !e.isFree) := funext fun e => by rw [skipped_iff_free]
  have := filter_add entries
  rw [hs]
  omega

/-- Taking only `Next ≤ -3` for removed misses the end of the free list. -/
theorem stricter_check_writes_freed :
    ∃ e : Entry, e.isFree = true ∧ ¬ (e.next ≤ -3) := ⟨.free none, rfl, by decide⟩

end Btdb.HashSetEntries
