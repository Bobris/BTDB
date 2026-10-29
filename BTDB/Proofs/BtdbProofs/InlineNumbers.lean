/-!
# Valid inline object numbers stay within their record

A model of the bound `DBReaderCtx.RetrieveObj` (`../../ODBLayer/DBReaderCtx.cs`) puts on inline object numbers since the
2026-10-29 fix. `DBWriterCtx` numbers inline objects as it first meets them, so a reference to an object first met at
reference `i` of a record carries the number of distinct objects met before it, at most `i`. References take at least
a byte each, so the one at index `i` starts at a byte position `pos i ≥ i` and below the record length `len`. The reader
refuses a number `ido` only when `ido ≥ count + len`, `count` being the objects it registered so far.

* `valid_number_accepted`: the number of every reference in a valid record passes the check, whatever `count` is, so
  the check refuses only damage, and even a reader skipping nested content without registering it reads on.
-/

namespace Btdb.InlineNumbers

/-- References at strictly increasing byte positions start no earlier than their index. -/
theorem index_le_pos (pos : Nat → Nat) (hinc : ∀ i, pos i < pos (i + 1)) : ∀ i, i ≤ pos i
  | 0 => Nat.zero_le _
  | i + 1 => by have := index_le_pos pos hinc i; have := hinc i; omega

/-- The number of distinct objects met before reference `i`: the references before it that met a new one. -/
def numberAt (isFirst : Nat → Bool) (i : Nat) : Nat := (List.range i).countP isFirst

theorem number_le_index (isFirst : Nat → Bool) (i : Nat) : numberAt isFirst i ≤ i := by
  unfold numberAt
  have := List.countP_le_length (p := isFirst) (l := List.range i)
  simpa using this

/-- `RetrieveObj` refuses a number. -/
def refuses (count len ido : Nat) : Bool := decide (ido ≥ count + len)

theorem valid_number_accepted (pos : Nat → Nat) (hinc : ∀ i, pos i < pos (i + 1)) (isFirst : Nat → Bool)
    (len count i : Nat) (hi : pos i < len) : refuses count len (numberAt isFirst i) = false := by
  have h1 := number_le_index isFirst i
  have h2 := index_le_pos pos hinc i
  unfold refuses
  simp only [decide_eq_false_iff_not]
  omega

end Btdb.InlineNumbers
