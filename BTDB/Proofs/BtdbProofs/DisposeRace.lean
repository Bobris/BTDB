/-!
# An instance created while its container is disposed is disposed exactly once

`ContainerImpl.TrackOwnedInstance` (`../IOC/ContainerImpl.cs`) adds a new instance to the owned list with an atomic
exchange, then reads the dispose flag; when it is set, it takes the whole list, disposes it and throws. `DisposeAsync`
sets the flag, then takes the whole list and disposes it. Taking the list leaves it empty, so an instance is disposed by
whoever takes it. Before the 2026-10-29 fix tracking read the flag first and threw without adding or disposing the
instance, and an instance added after disposal took the list stayed in it.

The model runs two tracking threads, instances `0` and `1`, and one disposing thread in every interleaving of their
atomic steps.

* `fixed_disposes_once`: in every interleaving each instance is disposed exactly once.
* `previous_leaks`: before the fix some interleaving leaves an instance undisposed.
-/

namespace Btdb.DisposeRace

structure State where
  list : List Nat
  disposed : List Nat
  flag : Bool
  -- Tracking threads: 0 add, 1 read the flag, 2 take the list, 3 done.
  t0 : Nat
  t1 : Nat
  -- Disposing thread: 0 set the flag, 1 take the list, 2 done.
  d : Nat
  deriving DecidableEq

def init : State := ⟨[], [], false, 0, 0, 0⟩

def tracker (i : Nat) (pc : Nat) (s : State) : Option (Nat × State) :=
  match pc with
  | 0 => some (1, { s with list := i :: s.list })
  | 1 => some (if s.flag then 2 else 3, s)
  | 2 => some (3, { s with list := [], disposed := s.disposed ++ s.list })
  | _ => none

/-- Before the fix: read the flag, then add the instance only when it was clear. -/
def previousTracker (i : Nat) (pc : Nat) (s : State) : Option (Nat × State) :=
  match pc with
  | 0 => some (if s.flag then 3 else 1, s)
  | 1 => some (3, { s with list := i :: s.list })
  | _ => none

def disposer (s : State) : Option State :=
  match s.d with
  | 0 => some { s with flag := true, d := 1 }
  | 1 => some { s with list := [], disposed := s.disposed ++ s.list, d := 2 }
  | _ => none

/-- Every instance that was created is disposed exactly once. The factory creates the instance before tracking it. -/
def Good (s : State) (created : Nat → Bool) : Bool :=
  s.list.isEmpty && [0, 1].all fun i => s.disposed.count i == if created i then 1 else 0

/-- Explores every interleaving; `step` runs a tracking thread. -/
def explore (step : Nat → Nat → State → Option (Nat × State)) (created : State → Nat → Bool) :
    Nat → State → Bool
  | 0, _ => false
  | fuel + 1, s =>
    let moves : List State :=
      ((step 0 s.t0 s).map fun (pc, s') => { s' with t0 := pc }).toList ++
      ((step 1 s.t1 s).map fun (pc, s') => { s' with t1 := pc }).toList ++
      (disposer s).toList
    if moves.isEmpty then Good s (created s) else moves.all (explore step created fuel)

def created (_ : State) (_ : Nat) : Bool := true

theorem fixed_disposes_once : explore tracker created 12 init = true := by decide

theorem previous_leaks : explore previousTracker created 12 init = false := by decide

end Btdb.DisposeRace
