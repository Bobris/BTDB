/-!
# Saving a list key needs no empty evaluation stack

`ListFieldOrderedHandler.Save` (`../FieldHandler/ListFieldOrderedHandler.cs`) emits the IL that writes a list key part:
its count, then each item. Relation methods like `ListByKey` (`../ODBLayer/RelationBuilder.cs`) emit it after pushing
their own call arguments, so it runs on a non-empty evaluation stack. The CLR rejects a method whose try block starts
on a non-empty stack. Before the 2026-10-28 fix the saver wrapped its item loop in a try block for the enumerator's
`Dispose`; it now disposes after the loop. The model tracks only the stack depth.

* `previous_rejected`: under any pushed argument, the old saver is rejected, while alone it is accepted.
* `fixed_accepted`: the new saver is accepted under any number of arguments and leaves them in place.
-/

namespace Btdb.EmittedTry

inductive Op where
  | push
  | pop
  | enterTry
  | leaveTry

/-- The depth after `op`; `none` when the CLR rejects it. Leaving a try block empties the stack. -/
def step : Nat → Op → Option Nat
  | d, .push => some (d + 1)
  | 0, .pop => none
  | d + 1, .pop => some d
  | 0, .enterTry => some 0
  | _ + 1, .enterTry => none
  | _, .leaveTry => some 0

def run (d : Nat) (ops : List Op) : Option Nat := ops.foldlM step d

/-- A call of a method with two arguments and no result, like `MemWriter.WriteVUInt32(writer, count)`. -/
def call2 : List Op := [.push, .push, .pop, .pop]

/-- The count, then `n` items each written by such a call. -/
def items (n : Nat) : List Op := (List.replicate n call2).flatten

def previous (n : Nat) : List Op := call2 ++ [.enterTry] ++ items n ++ [.leaveTry]

def fixed (n : Nat) : List Op := call2 ++ items n

theorem run_append (d : Nat) (a b : List Op) : run d (a ++ b) = (run d a).bind fun d' => run d' b := by
  simp [run, List.foldlM_append]

theorem run_call2 (d : Nat) : run d call2 = some d := by
  simp [run, call2, List.foldlM, step]

theorem run_items (n d : Nat) : run d (items n) = some d := by
  induction n with
  | zero => rfl
  | succ n ih =>
    simp only [items, List.replicate_succ, List.flatten_cons] at ih ⊢
    rw [run_append, run_call2]
    exact ih

theorem previous_rejected (n d : Nat) (h : 0 < d) : run d (previous n) = none := by
  obtain ⟨k, rfl⟩ : ∃ k, d = k + 1 := ⟨d - 1, by omega⟩
  simp only [previous, run_append, run_call2, Option.bind_some]
  rfl

theorem previous_alone (n : Nat) : run 0 (previous n) = some 0 := by
  simp only [previous, run_append, run_call2, Option.bind_some]
  have h : run 0 [Op.enterTry] = some 0 := rfl
  rw [h, Option.bind_some, run_items, Option.bind_some]
  rfl

theorem fixed_accepted (n d : Nat) : run d (fixed n) = some d := by
  simp only [fixed, run_append, run_call2, Option.bind_some, run_items]

end Btdb.EmittedTry
