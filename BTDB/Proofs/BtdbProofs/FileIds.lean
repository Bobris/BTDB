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

end Btdb.FileIds
