/-!
# A type name never stands for two storable types

A model of `Type2NameRegistry.RegisterType` (`../../ODBLayer/Type2NameRegistry.cs`). Types are numbers, each storable
or not (an interface or abstract class is not). The registry keeps each registered type's name, and loading an object
stored under a name creates the type the name was registered for first. `RegisterType` refuses a type already
registered under another name and, since the 2026-10-29 fix, a storable type whose name a storable type took.

* `reachable_inv`: no two storable types are registered under one name, so the type loaded under a name is the one
  stored under it.
* `old_shares`: the registry before the fix registered two storable types under one name
  (`ObjectDbTypeNameClashTest`).
-/

namespace Btdb.TypeNames

/-- Registered types with their names. -/
abbrev Registry := List (Nat × String)

def nameOf (r : Registry) (t : Nat) : Option String := (r.find? (·.1 = t)).map (·.2)

/-- `RegisterType`: `none` when refused. -/
def register (storable : Nat → Bool) (r : Registry) (t : Nat) (name : String) : Option Registry :=
  match nameOf r t with
  | some n => if n = name then some r else none
  | none => if storable t && r.any (fun p => p.2 = name && storable p.1) then none else some ((t, name) :: r)

def oldRegister (r : Registry) (t : Nat) (name : String) : Option Registry :=
  match nameOf r t with
  | some n => if n = name then some r else none
  | none => some ((t, name) :: r)

inductive Reachable (storable : Nat → Bool) : Registry → Prop
  | init : Reachable storable []
  | step {r r'} (t : Nat) (name : String) : Reachable storable r → register storable r t name = some r' →
      Reachable storable r'

/-- Two storable types registered under one name are the same type. -/
def Unique (storable : Nat → Bool) (r : Registry) : Prop :=
  ∀ a b n, (a, n) ∈ r → (b, n) ∈ r → storable a = true → storable b = true → a = b

theorem reachable_inv {storable : Nat → Bool} {r : Registry} (h : Reachable storable r) : Unique storable r := by
  induction h with
  | init => intro a b n ha; simp at ha
  | @step r r' t name _ hreg ih =>
    unfold register at hreg
    split at hreg
    · split at hreg
      · simp at hreg; subst hreg; exact ih
      · simp at hreg
    · split at hreg
      · simp at hreg
      · rename_i hfree
        simp at hreg; subst hreg
        intro a b n ha hb sa sb
        simp only [List.mem_cons, Prod.mk.injEq] at ha hb
        have clash : ∀ c, (c, name) ∈ r → storable c = true → storable t = true → False := by
          intro c hc sc st
          apply hfree
          simp only [Bool.and_eq_true, List.any_eq_true, decide_eq_true_eq]
          exact ⟨st, ⟨(c, name), hc, rfl, sc⟩⟩
        rcases ha with ⟨rfl, rfl⟩ | ha <;> rcases hb with ⟨rfl, hn⟩ | hb
        · rfl
        · exact (clash b hb sb sa).elim
        · subst hn; exact (clash a ha sa sb).elim
        · exact ih a b n ha hb sa sb

/-- Types 1 and 2, both storable, as `First.Person` and `Second.Person` both registered as `Person`. -/
theorem old_shares :
    (oldRegister [] 1 "Person" >>= fun r => oldRegister r 2 "Person") = some [(2, "Person"), (1, "Person")] ∧
      register (fun _ => true) [(1, "Person")] 2 "Person" = none := by
  decide

end Btdb.TypeNames
