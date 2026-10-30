/-!
# A type name never stands for two storable types

A model of `Type2NameRegistry.RegisterType` (`../../ODBLayer/Type2NameRegistry.cs`). Types are numbers, each storable
or not (an interface or abstract class is not). The registry keeps each registered type's name, and loading an object
stored under a name creates the storable type registered under it (since the 2026-10-29 fix also when an interface took
the name first). `RegisterType` refuses a type already registered under another name and, since the 2026-10-29 fix, a
storable type whose name a storable type took.

* `reachable_inv`: no two storable types are registered under one name, so the storable type loaded under a name is
  the one stored under it.
* `lookup_storable`: once a storable type is registered under a name, `FindTypeByName` gives a storable type, also
  when its interface took the name first.
* `old_shares`: the registry before the fix registered two storable types under one name
  (`ObjectDbTypeNameClashTest`).
* `alias_yields`, `find_storable`: `RegisterAlias` names a storable type objects stored under a name load as, without
  storing it there. A storable type registered under the name keeps it, and the type found is storable whenever one
  was registered under the name or as its alias.
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

/-- The name to type map `FindTypeByName` reads: the first type registered under a name keeps it, except that a
storable type takes it from one that is not. -/
def lookupAfter (storable : Nat → Bool) : Option Nat → List Nat → Option Nat
  | cur, [] => cur
  | none, t :: ts => lookupAfter storable (some t) ts
  | some c, t :: ts => lookupAfter storable (if !storable c && storable t then some t else some c) ts

/-- Once a storable type is registered under the name, `FindTypeByName` gives a storable type, which by
`reachable_inv` is the one stored under it; before the fix an interface registered first kept the name and loading
created an instance of it (`ObjectDbTypeNameClashTest.ImplementationRegisteredAfterItsInterfaceLoads`). -/
theorem lookup_storable (storable : Nat → Bool) :
    ∀ (cur : Option Nat) (ts : List Nat), (∃ t ∈ ts, storable t = true) ∨ (∃ c, cur = some c ∧ storable c = true) →
      ∃ r, lookupAfter storable cur ts = some r ∧ storable r = true
  | cur, [], h => by
    rcases h with ⟨t, ht, _⟩ | ⟨c, hc, hs⟩
    · simp at ht
    · exact ⟨c, by simp [lookupAfter, hc], hs⟩
  | none, t :: ts, h => by
    apply lookup_storable storable (some t) ts
    rcases h with ⟨u, hu, hs⟩ | ⟨c, hc, _⟩
    · cases List.mem_cons.1 hu with
      | inl e => exact Or.inr ⟨t, rfl, e ▸ hs⟩
      | inr e => exact Or.inl ⟨u, e, hs⟩
    · simp at hc
  | some c, t :: ts, h => by
    apply lookup_storable storable _ ts
    by_cases hc : storable c = true
    · right
      exact ⟨c, by simp [hc], hc⟩
    · rcases h with ⟨u, hu, hs⟩ | ⟨c', hc', hs'⟩
      · cases List.mem_cons.1 hu with
        | inl e => subst e; right; exact ⟨u, by simp [hc, hs], hs⟩
        | inr e => left; exact ⟨u, e, hs⟩
      · simp at hc'; subst hc'; exact absurd hs' hc

/-- `FindTypeByName` with aliases: the type `lookupAfter` gives when it is storable, else the alias, else the type
`lookupAfter` gives. `RegisterAlias` takes only storable types. -/
def findWithAlias (storable : Nat → Bool) (registered alias : Option Nat) : Option Nat :=
  match registered with
  | some c => if storable c then some c else (alias <|> some c)
  | none => alias

/-- An alias never takes a name from a storable type registered under it, which is the type stored there. -/
theorem alias_yields (storable : Nat → Bool) (ts : List Nat) (alias : Option Nat) (h : ∃ t ∈ ts, storable t = true) :
    findWithAlias storable (lookupAfter storable none ts) alias = lookupAfter storable none ts := by
  obtain ⟨r, hr, hs⟩ := lookup_storable storable none ts (Or.inl h)
  simp [findWithAlias, hr, hs]

/-- With a storable type registered under the name or as its alias, loading creates a storable type. -/
theorem find_storable (storable : Nat → Bool) (registered alias : Option Nat)
    (h : (∃ c, registered = some c ∧ storable c = true) ∨ ∃ a, alias = some a ∧ storable a = true) :
    ∃ r, findWithAlias storable registered alias = some r ∧ storable r = true := by
  rcases h with ⟨c, rfl, hc⟩ | ⟨a, rfl, ha⟩
  · exact ⟨c, by simp [findWithAlias, hc], hc⟩
  · cases registered with
    | none => exact ⟨a, rfl, ha⟩
    | some c =>
      by_cases hc : storable c = true
      · exact ⟨c, by simp [findWithAlias, hc], hc⟩
      · exact ⟨a, by simp [findWithAlias, hc], ha⟩

end Btdb.TypeNames
