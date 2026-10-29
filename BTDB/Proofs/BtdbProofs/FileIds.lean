/-!
# Allocating file IDs by parity

A model of `FileIdAllocator` (`../KVDBLayer/Implementation/FileIdAllocator.cs`). It remembers the largest odd and the
largest even file ID observed, 0 when none. `Allocate` takes the largest ID of the requested parity, or of both for
`Any`, adds one, adds one more when the parity is wrong, and observes the result:

```csharp
var next = (ulong)lastId + 1;
if (parity != FileIdParity.Any && (next & 1) != (parity == FileIdParity.Odd ? 1ul : 0ul)) next++;
```

`Observe` is applied to every file ID found when a collection opens and to every allocated one. Overflow past
`uint.MaxValue` throws, so the model uses naturals.

* `allocate_fresh`: the allocated ID is not 0, has the requested parity, and is none of the observed IDs, so a new file
  never takes the ID of an existing one; transaction logs get odd IDs and other files even ones.
* `allocate_inv`: observing the allocated ID keeps the remembered maxima above every observed ID, so every later
  allocation is fresh too.

The standalone collections (`OnDiskFileCollection`, `OnDiskMemoryMappedFileCollection`, `InMemoryFileCollection`)
remember one largest ID, the largest found on open, and `FileIdAllocator.Next` advances it, refusing to go past
`uint.MaxValue`:

* `load_max_ge`: the largest ID found on open is at least every file's ID, in any listing order.
* `next_fresh`: the next ID is not 0 and is above every file's ID. Before the 2026-10-29 fix the largest ID was an
  `int`, so a file named with the largest `uint` made the next ID wrap to 0, which opening skips as no file
  (`FileCollectionTests.FileAfterTheLargestPossibleIdIsRefused`).
-/

namespace Btdb.FileIds

inductive Parity
  | odd
  | even
  | any

structure State where
  lastOdd : Nat
  lastEven : Nat

def observe (s : State) (id : Nat) : State :=
  if id % 2 = 1 then { s with lastOdd := max s.lastOdd id } else { s with lastEven := max s.lastEven id }

def allocate (s : State) (p : Parity) : Nat :=
  let last := match p with
    | .odd => s.lastOdd
    | .even => s.lastEven
    | .any => max s.lastOdd s.lastEven
  let next := last + 1
  match p with
  | .odd => if next % 2 = 1 then next else next + 1
  | .even => if next % 2 = 0 then next else next + 1
  | .any => next

def Matches : Parity → Nat → Prop
  | .odd, id => id % 2 = 1
  | .even, id => id % 2 = 0
  | .any, _ => True

/-- Every observed ID is at most the remembered maximum of its parity. -/
def Inv (s : State) (observed : List Nat) : Prop :=
  ∀ id ∈ observed, (id % 2 = 1 → id ≤ s.lastOdd) ∧ (id % 2 = 0 → id ≤ s.lastEven)

theorem inv_empty : Inv ⟨0, 0⟩ [] := by simp [Inv]

theorem observe_inv (s : State) (observed : List Nat) (id : Nat) (h : Inv s observed) :
    Inv (observe s id) (id :: observed) := by
  intro x hx
  unfold observe
  rcases List.mem_cons.mp hx with rfl | hx
  · split <;> simp only <;> omega
  · have := h x hx
    split <;> simp only <;> omega

theorem allocate_fresh (s : State) (observed : List Nat) (p : Parity) (h : Inv s observed) :
    allocate s p ≠ 0 ∧ Matches p (allocate s p) ∧ allocate s p ∉ observed := by
  refine ⟨?_, ?_, ?_⟩
  · cases p <;> simp only [allocate] <;> (try split) <;> omega
  · cases p <;> simp only [allocate, Matches] <;> (try split) <;> omega
  · intro hmem
    have := h _ hmem
    cases p <;> simp only [allocate] at this <;> (try split at this) <;> omega

theorem allocate_inv (s : State) (observed : List Nat) (p : Parity) (h : Inv s observed) :
    Inv (observe s (allocate s p)) (allocate s p :: observed) := observe_inv _ _ _ h

/-- The largest ID found on open, as the constructors' `if (id > _maxFileId) _maxFileId = id` computes it. -/
def loadMax (ids : List Nat) : Nat := ids.foldl max 0

theorem foldl_max_ge (ids : List Nat) (m : Nat) : m ≤ ids.foldl max m ∧ ∀ id ∈ ids, id ≤ ids.foldl max m := by
  induction ids generalizing m with
  | nil => simp
  | cons x ids ih =>
    obtain ⟨h1, h2⟩ := ih (max m x)
    refine ⟨Nat.le_trans (Nat.le_max_left _ _) h1, ?_⟩
    intro id hid
    rcases List.mem_cons.mp hid with rfl | hid
    · exact Nat.le_trans (Nat.le_max_right _ _) h1
    · exact h2 id hid

theorem load_max_ge (ids : List Nat) : ∀ id ∈ ids, id ≤ loadMax ids := (foldl_max_ge ids 0).2

/-- `FileIdAllocator.Next`: none past the largest `uint`. -/
def next (uintMax m : Nat) : Option Nat := if m < uintMax then some (m + 1) else none

theorem next_fresh (uintMax : Nat) (ids : List Nat) (r : Nat) (h : next uintMax (loadMax ids) = some r) :
    r ≠ 0 ∧ ∀ id ∈ ids, id < r := by
  unfold next at h
  split at h
  · simp only [Option.some.injEq] at h
    subst h
    exact ⟨by omega, fun id hid => Nat.lt_succ_of_le (load_max_ge ids id hid)⟩
  · simp at h

end Btdb.FileIds
