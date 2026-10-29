/-!
# Which registration of a type the container resolves

A model of `ContainerRegistrationContext.AddCReg` and `CReg.Add` (`../IOC/ContainerRegistrationContext.cs`,
`../IOC/CReg.cs`). The first registration of a type is kept as it is. A second one turns it into a multi registration
listing both, and every later one is appended. Each registration replaces the default unless it preserves existing
defaults. Registrations are numbered; `preserve r` says whether `r` was registered with `PreserveExistingDefaults`.

* `all_listed`: every registration of a type is listed, in registration order, so `IEnumerable` and the verification of
  singletons see all of them.
* `default_last`: the default is the last registration that did not preserve the defaults, or the first one when all
  later ones did.
-/

namespace Btdb.IocDefaults

structure Reg where
  default : Nat
  multi : List Nat

def first (r : Nat) : Reg := ⟨r, []⟩

/-- A further registration: the multi list starts with the first one when it was single. -/
def add (preserve : Nat → Bool) (reg : Reg) (r : Nat) : Reg :=
  let multi := if reg.multi.isEmpty then [reg.default] else reg.multi
  ⟨if preserve r then reg.default else r, multi ++ [r]⟩

def register (preserve : Nat → Bool) (r0 : Nat) (rs : List Nat) : Reg := rs.foldl (add preserve) (first r0)

/-- The registrations a type lists: the single one, or the multi list. -/
def listed (reg : Reg) : List Nat := if reg.multi.isEmpty then [reg.default] else reg.multi

/-- The default the code keeps: the last registration that does not preserve the defaults. -/
def expectedDefault (preserve : Nat → Bool) (r0 : Nat) : List Nat → Nat
  | [] => r0
  | rs => match (rs.filter (fun r => !preserve r)).getLast? with
    | some r => r
    | none => r0

theorem listed_add (preserve : Nat → Bool) (reg : Reg) (r : Nat) :
    listed (add preserve reg r) = listed reg ++ [r] := by
  by_cases h : reg.multi.isEmpty <;> simp [listed, add, h]

theorem all_listed (preserve : Nat → Bool) (r0 : Nat) (rs : List Nat) :
    listed (register preserve r0 rs) = r0 :: rs := by
  unfold register
  suffices ∀ (reg : Reg) (pre : List Nat), listed reg = pre → listed (rs.foldl (add preserve) reg) = pre ++ rs by
    simpa using this (first r0) [r0] (by simp [listed, first])
  induction rs with
  | nil => intro reg pre h; simpa using h
  | cons r rs ih =>
    intro reg pre h
    simp only [List.foldl_cons]
    rw [ih (add preserve reg r) (pre ++ [r]) (by rw [listed_add, h])]
    simp

theorem default_foldl (preserve : Nat → Bool) (reg : Reg) (rs : List Nat) :
    (rs.foldl (add preserve) reg).default =
      match (rs.filter (fun r => !preserve r)).getLast? with
      | some r => r
      | none => reg.default := by
  induction rs generalizing reg with
  | nil => rfl
  | cons r rs ih =>
    simp only [List.foldl_cons]
    rw [ih]
    cases hl : (rs.filter (fun r => !preserve r)).getLast? with
    | some x =>
      by_cases hp : preserve r <;> simp [hp, List.getLast?_cons, hl]
    | none =>
      have hnil : rs.filter (fun r => !preserve r) = [] := by simpa using hl
      by_cases hp : preserve r
      · simp [add, hp, hnil]
      · simp [add, hp, hnil]

theorem default_last (preserve : Nat → Bool) (r0 : Nat) (rs : List Nat) :
    (register preserve r0 rs).default = expectedDefault preserve r0 rs := by
  unfold register expectedDefault
  rw [default_foldl]
  cases rs <;> rfl

end Btdb.IocDefaults
