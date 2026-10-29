/-!
# A generated dispatcher calls handlers built for the root container it is called with

`DispatcherExecutes.Get` (`../IOC/IContainer.cs`) maps a container to its root, which scopes share, returns the handler
factory of the last root it served when the root is the same, and otherwise takes the one its table keeps for the root,
creating it the first time. Before the 2026-10-29 fix one factory per process was kept and each dispatcher creation
replaced it with one built for its container. A factory is modelled by the root it was built for and a serial number.

* `get_built_for_root`: every factory returned was built for the root of the container it is called with.
* `get_stable`: once a root got a factory, it keeps getting the same one.
* `previous_wrong_root`: before the fix, creating a dispatcher for a second root made calls for the first one use the
  factory of the second.
-/

namespace Btdb.DispatcherContainers

structure Execute where
  root : Nat
  serial : Nat
  deriving DecidableEq

structure State where
  table : List (Nat × Execute)
  last : Option (Nat × Execute)
  next : Nat

def lookup (table : List (Nat × Execute)) (root : Nat) : Option Execute := (table.find? (·.1 == root)).map (·.2)

/-- `Get` of a container whose root is `root`. -/
def get (s : State) (root : Nat) : Execute × State :=
  match s.last with
  | some (r, e) => if r = root then (e, s) else getSlow s root
  | none => getSlow s root
where
  getSlow (s : State) (root : Nat) : Execute × State :=
    match lookup s.table root with
    | some e => (e, { s with last := some (root, e) })
    | none =>
      let e : Execute := ⟨root, s.next⟩
      (e, { table := s.table ++ [(root, e)], last := some (root, e), next := s.next + 1 })

/-- Each kept factory was built for the root it is kept under, and the last one is the one the table keeps. -/
def Inv (s : State) : Prop :=
  (∀ p ∈ s.table, p.2.root = p.1) ∧ ∀ r e, s.last = some (r, e) → lookup s.table r = some e

theorem lookup_root (table : List (Nat × Execute)) (h : ∀ p ∈ table, p.2.root = p.1) (r : Nat) (e : Execute)
    (hl : lookup table r = some e) : e.root = r := by
  unfold lookup at hl
  obtain ⟨p, hp, rfl⟩ := Option.map_eq_some_iff.1 hl
  have hmem := List.mem_of_find?_eq_some hp
  have hkey := List.find?_some hp
  simp only [beq_iff_eq] at hkey
  rw [h p hmem, hkey]

theorem lookup_append_of_some (table : List (Nat × Execute)) (q : Nat × Execute) (r : Nat) (e : Execute)
    (h : lookup table r = some e) : lookup (table ++ [q]) r = some e := by
  unfold lookup at *
  rw [List.find?_append]
  obtain ⟨p, hp, rfl⟩ := Option.map_eq_some_iff.1 h
  simp [hp]

theorem lookup_append_new (table : List (Nat × Execute)) (r : Nat) (e : Execute) (h : lookup table r = none) :
    lookup (table ++ [(r, e)]) r = some e := by
  unfold lookup at *
  rw [List.find?_append]
  have hf : table.find? (·.1 == r) = none := by simpa using h
  simp [hf]

theorem getSlow_spec (s : State) (root : Nat) (h : Inv s) :
    (get.getSlow s root).1.root = root ∧ Inv (get.getSlow s root).2 ∧
      lookup (get.getSlow s root).2.table root = some (get.getSlow s root).1 ∧
      ∀ r e, lookup s.table r = some e → lookup (get.getSlow s root).2.table r = some e := by
  obtain ⟨hroot, hlast⟩ := h
  unfold get.getSlow
  cases hl : lookup s.table root with
  | some e =>
    refine ⟨lookup_root _ hroot _ _ hl, ⟨hroot, ?_⟩, hl, fun _ _ h => h⟩
    intro r e' he
    simp only [Option.some.injEq, Prod.mk.injEq] at he
    obtain ⟨rfl, rfl⟩ := he
    exact hl
  | none =>
    refine ⟨rfl, ⟨?_, ?_⟩, lookup_append_new _ _ _ hl, fun r e h => lookup_append_of_some _ _ _ _ h⟩
    · intro p hp
      rcases List.mem_append.1 hp with hp | hp
      · exact hroot p hp
      · simp only [List.mem_singleton] at hp; subst hp; rfl
    · intro r e he
      simp only [Option.some.injEq, Prod.mk.injEq] at he
      obtain ⟨rfl, rfl⟩ := he
      exact lookup_append_new _ _ _ hl

/-- `get` keeps the invariant, returns what the table keeps for the root, and changes no kept factory. -/
theorem get_spec (s : State) (root : Nat) (h : Inv s) :
    (get s root).1.root = root ∧ Inv (get s root).2 ∧ lookup (get s root).2.table root = some (get s root).1 ∧
      ∀ r e, lookup s.table r = some e → lookup (get s root).2.table r = some e := by
  unfold get
  cases hl : s.last with
  | none => exact getSlow_spec s root h
  | some p =>
    obtain ⟨r, e⟩ := p
    dsimp only
    split
    · next hr =>
      subst hr
      have he := h.2 r e hl
      exact ⟨lookup_root _ h.1 _ _ he, h, he, fun _ _ h => h⟩
    · exact getSlow_spec s root h

def run (s : State) : List Nat → State
  | [] => s
  | root :: rest => run (get s root).2 rest

theorem run_spec (s : State) (roots : List Nat) (h : Inv s) :
    Inv (run s roots) ∧ ∀ r e, lookup s.table r = some e → lookup (run s roots).table r = some e := by
  induction roots generalizing s with
  | nil => exact ⟨h, fun _ _ h => h⟩
  | cons root rest ih =>
    obtain ⟨_, hinv, _, hkeep⟩ := get_spec s root h
    obtain ⟨hinv', hkeep'⟩ := ih _ hinv
    exact ⟨hinv', fun r e he => hkeep' r e (hkeep r e he)⟩

def empty : State := ⟨[], none, 0⟩

theorem empty_inv : Inv empty := ⟨fun _ h => by simp [empty] at h, fun _ _ h => by simp [empty] at h⟩

theorem get_built_for_root (roots : List Nat) (root : Nat) : (get (run empty roots) root).1.root = root :=
  (get_spec _ root (run_spec empty roots empty_inv).1).1

theorem get_stable (roots middle : List Nat) (root : Nat) :
    (get (run (get (run empty roots) root).2 middle) root).1 = (get (run empty roots) root).1 := by
  have hinv := (run_spec empty roots empty_inv).1
  obtain ⟨_, hinv1, hkept, _⟩ := get_spec _ root hinv
  obtain ⟨hinv2, hkeep⟩ := run_spec _ middle hinv1
  obtain ⟨_, _, hkept2, hkeep2⟩ := get_spec _ root hinv2
  have h1 := hkeep _ _ hkept
  have h2 := hkeep2 _ _ h1
  rw [hkept2] at h2
  exact Option.some.inj h2

/-- Before the fix: one factory, replaced by each dispatcher creation, built for the container it was created for. -/
def previousPrepare (_ : Option Nat) (root : Nat) : Option Nat := some root

theorem previous_wrong_root : (previousPrepare (previousPrepare none 0) 1) ≠ some 0 := by decide

end Btdb.DispatcherContainers
