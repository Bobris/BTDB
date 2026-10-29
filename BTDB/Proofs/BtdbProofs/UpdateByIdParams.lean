/-!
# UpdateById checks every key parameter

A model of how the source generator checks the key parameters of `UpdateById` (`CheckParamsNamesAndTypes` in
`../../../BTDB.SourceGenerator/Main.cs`). The parameters are compared with the key fields position by position, up to
a count: `ok p f` says parameter `p` has the name (or stored name) and a type compatible with field `f`. The generated
code writes the first parameters into the key in the order of the key fields, InKeyValue fields included.

* `cut_misses`: before the 2026-10-29 fix the count stopped at the first InKeyValue field, so
  `UpdateById(ulong id, string? name)` passed for a key `Id, [InKeyValue] Version` and `name` was written into the key
  in place of `Version` (`RelationDiagnosticsCoverageTests.ReportsUpdateByIdInKeyValueParameterMismatch`).
* `full_check_sound`: counting every key field, with at least as many parameters as key fields (BTDB0040), every key
  field is written from a parameter that matches it.

Methods taking a key prefix (`ListById`, `CountByName`, a counting `RemoveById`, ...) write every parameter into the
key, against the fields of the whole index.

* `prefix_cut_misses`: the old check counted only the fields before the first InKeyValue field (for a secondary key,
  its declared fields), so `CountById(ulong id, string name)` and parameters past the index passed.
* `prefix_check_sound`: refusing more parameters than index fields (BTDB0016) and checking every parameter, every
  parameter written matches the key field it is written as.
-/

namespace Btdb.UpdateByIdParams

variable {α β : Type}

def checkUpTo (ok : α → β → Bool) : Nat → List α → List β → Bool
  | n + 1, p :: ps, f :: fs => ok p f && checkUpTo ok n ps fs
  | _, _, _ => true

theorem checkUpTo_sound (ok : α → β → Bool) :
    ∀ (params : List α) (fields : List β) (n : Nat), checkUpTo ok n params fields = true →
      ∀ (i : Nat) (_hi : i < n) (hp : i < params.length) (hf : i < fields.length), ok params[i] fields[i] = true := by
  intro params
  induction params with
  | nil => intro _ _ _ i _ hp; simp at hp
  | cons p ps ih =>
    intro fields n h i hi hp hf
    cases fields with
    | nil => simp at hf
    | cons f fs =>
      cases n with
      | zero => omega
      | succ n =>
        simp only [checkUpTo, Bool.and_eq_true] at h
        cases i with
        | zero => exact h.1
        | succ i =>
          simp only [List.length_cons] at hp hf
          exact ih fs n h.2 i (by omega) (by omega) (by omega)

theorem full_check_sound (ok : α → β → Bool) (params : List α) (fields : List β)
    (h : checkUpTo ok fields.length params fields = true) (hlen : fields.length ≤ params.length)
    (i : Nat) (hf : i < fields.length) : ok (params[i]'(by omega)) fields[i] = true :=
  checkUpTo_sound ok params fields fields.length h i hf (by omega) hf

/-- Key `Id : ulong, [InKeyValue] Version : int`, parameters `id : ulong, name : string`; the old count is the index of
the first InKeyValue field, 1. -/
theorem cut_misses :
    let fields := [("id", "ulong"), ("version", "int")]
    let params := [("id", "ulong"), ("name", "string")]
    let ok := fun (p f : String × String) => p == f
    checkUpTo ok 1 params fields = true ∧ ok params[1] fields[1] = false ∧
      checkUpTo ok fields.length params fields = false := by
  decide

def checkPrefix (ok : α → β → Bool) (params : List α) (fields : List β) : Bool :=
  decide (params.length ≤ fields.length) && checkUpTo ok params.length params fields

theorem prefix_check_sound (ok : α → β → Bool) (params : List α) (fields : List β)
    (h : checkPrefix ok params fields = true) (i : Nat) (hp : i < params.length) :
    ok params[i] (fields[i]'(by simp only [checkPrefix, Bool.and_eq_true, decide_eq_true_eq] at h; omega)) = true := by
  simp only [checkPrefix, Bool.and_eq_true, decide_eq_true_eq] at h
  exact checkUpTo_sound ok params fields params.length h.2 i hp hp (by omega)

/-- `CountById(ulong id, string name)` for the key `Id, [InKeyValue] Version`: the old count 1 passes it, the whole
index refuses it, and a third parameter is refused as too many. -/
theorem prefix_cut_misses :
    let fields := [("id", "ulong"), ("version", "int")]
    let ok := fun (p f : String × String) => p == f
    checkUpTo ok 1 [("id", "ulong"), ("name", "string")] fields = true ∧
      checkPrefix ok [("id", "ulong"), ("name", "string")] fields = false ∧
      checkPrefix ok [("id", "ulong"), ("version", "int"), ("x", "int")] fields = false ∧
      checkPrefix ok [("id", "ulong"), ("version", "int")] fields = true := by
  decide

end Btdb.UpdateByIdParams
