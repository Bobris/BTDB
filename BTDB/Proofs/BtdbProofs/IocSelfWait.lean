/-!
# Waiting for an instance being created

A model of `ContainerImpl.WaitForSingleton` and `WaitForScoped` (`../IOC/ContainerImpl.cs`). While a thread creates a
singleton or scoped instance, the slot holds a locker that thread has entered. A waiting thread loops: when the slot
holds a locker it takes and releases the locker's lock (`lock (locker) { }`), which waits for the creating thread,
and looks again. `Monitor` locks are reentrant, so the creating thread itself passes the lock at once, and only it can
replace the locker in the slot.

* `self_wait_never_ends`: before the 2026-10-29 fix, the creating thread resolving its own service while creating it
  looped forever: every iteration left the slot holding its locker
  (`IocTests.ServiceResolvingItselfWhileCreatedFails`).
* `fixed_self_wait_fails`: the fixed loop, checking `Monitor.IsEntered(locker)` first, fails on its first iteration.
* `other_wait_unchanged`: for any other thread the fixed loop does what the old one did.
-/

namespace Btdb.IocSelfWait

inductive Slot
  | locker (owner : Nat)
  | instance (value : Nat)

inductive Step
  | again (slot : Slot)
  | done (value : Nat)
  | fail

/-- One iteration of the old loop by thread `me`; `afterLock` is the slot once the lock was passed. When `me` owns the
locker, nobody else can change the slot while `me` loops, so passing the lock leaves it as it was. -/
def oldStep (me : Nat) (afterLock : Slot) : Slot → Step
  | .instance v => .done v
  | .locker owner => if owner = me then .again (.locker owner) else .again afterLock

def fixedStep (me : Nat) (afterLock : Slot) : Slot → Step
  | .instance v => .done v
  | .locker owner => if owner = me then .fail else .again afterLock

/-- The slot after `n` iterations of the old loop, or the result once it ended. -/
def oldRun (me : Nat) (afterLock : Slot) : Nat → Slot → Step
  | 0, slot => .again slot
  | n + 1, slot => match oldStep me afterLock slot with
    | .again next => oldRun me afterLock n next
    | result => result

theorem self_wait_never_ends (me : Nat) (afterLock : Slot) (n : Nat) :
    oldRun me afterLock n (.locker me) = .again (.locker me) := by
  induction n with
  | zero => rfl
  | succ n ih => simp [oldRun, oldStep, ih]

theorem fixed_self_wait_fails (me : Nat) (afterLock : Slot) : fixedStep me afterLock (.locker me) = .fail := by
  simp [fixedStep]

theorem other_wait_unchanged (me owner : Nat) (afterLock : Slot) (h : owner ≠ me) :
    fixedStep me afterLock (.locker owner) = oldStep me afterLock (.locker owner) := by
  simp [fixedStep, oldStep, h]

end Btdb.IocSelfWait
