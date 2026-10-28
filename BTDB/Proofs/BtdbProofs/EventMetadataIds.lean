/-!
# The checks on event metadata type ids never reject what the serializer writes

`EventSerializer` writes the `n` new types of a metadata log with ids `-n, -(n - 1), ..., -1`, each followed by its
category byte and descriptor, and ends the log with id `0`. `EventDeserializer.ProcessMetadataLog` stores type `-id - 1`
at that index of its table, and since the 2026-10-28 fix rejects an id that is positive or whose index is not below the
log's length, which could otherwise make the table exhaust memory, and a table left with gaps.

* `written_index_below_length`: every index the serializer writes is below the length of its log, so no valid log is
  rejected for its ids.
* `written_indices_fill`: the indices it writes are exactly `0 .. n - 1`, so no valid log leaves a gap.
* `large_index_rejected`: an index the log cannot hold, as damage leaves, is rejected before the table grows.
-/

namespace Btdb.EventMetadataIds

/-- The ids the serializer writes for `n` new types, in order. -/
def writtenIds (n : Nat) : List Int := (List.range n).reverse.map fun (k : Nat) => -(k : Int) - 1

/-- The index the deserializer stores type `id` at. -/
def index (id : Int) : Int := -id - 1

/-- A log of `n` types whose entries take `sizes`, each at least an id byte and a category byte, and a final id byte. -/
def logLength (sizes : List Nat) : Nat := sizes.sum + 1

/-- The check the deserializer makes on each id of a log of `length` bytes. -/
def Accepted (length : Nat) (id : Int) : Prop := id < 0 ∧ index id < length

theorem sum_ge (sizes : List Nat) (h : ∀ s ∈ sizes, 2 ≤ s) : 2 * sizes.length ≤ sizes.sum := by
  induction sizes with
  | nil => simp
  | cons s rest ih =>
    simp only [List.length_cons, List.sum_cons]
    have := h s (by simp)
    have := ih (fun t ht => h t (by simp [ht]))
    omega

theorem written_index_below_length (sizes : List Nat) (h : ∀ s ∈ sizes, 2 ≤ s) :
    ∀ id ∈ writtenIds sizes.length, Accepted (logLength sizes) id := by
  intro id hid
  simp only [writtenIds, List.mem_map, List.mem_reverse, List.mem_range] at hid
  obtain ⟨k, hk, rfl⟩ := hid
  have := sum_ge sizes h
  simp only [Accepted, index, logLength]
  omega

theorem written_indices_fill (n : Nat) :
    ∀ i : Nat, i < n ↔ (i : Int) ∈ (writtenIds n).map index := by
  intro i
  simp only [writtenIds, List.map_map, List.mem_map, List.mem_reverse, List.mem_range, Function.comp_def, index]
  constructor
  · intro hi
    exact ⟨i, hi, by omega⟩
  · rintro ⟨k, hk, he⟩
    omega

theorem large_index_rejected (length : Nat) (id : Int) (h : length ≤ index id) : ¬ Accepted length id :=
  fun ⟨_, hlt⟩ => by omega

end Btdb.EventMetadataIds
