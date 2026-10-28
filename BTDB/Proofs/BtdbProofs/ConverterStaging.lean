/-!
# A converting loader or saver stages each value where it fits

`FieldHandler.Extensions.BuildConvertingLoader` and `BuildConvertingSaver` (`../FieldHandler/Extensions.cs`) load or
convert a value into a temporary before passing it on. The temporary is an `Int128`, which holds up to 16 bytes but
hides references from the GC, an object slot, which holds one reference, or since the 2026-10-28 fix a one-item array of
the value's type, which holds anything. A loader stages the type it loads and a saver the type its converter writes.

* `fixed_holds`: the staging chosen for a type always holds a value of it.
* `previous_saver_unsafe`: before the fix savers chose by the source type, so converting `object` to a 16-byte value
  type wrote it into an 8-byte object slot, and converting a value type to `object` hid the reference in an `Int128`.
* `previous_loader_missing`: and loaders had no staging for a value type larger than 16 bytes, like `DateTimeOffset?`.
-/

namespace Btdb.ConverterStaging

structure Ty where
  isValue : Bool
  size : Nat
  hasRefs : Bool

inductive Staging where
  | int128
  | objectSlot
  | array

/-- What each staging holds without losing bytes or hiding a reference. -/
def Holds : Staging → Ty → Prop
  | .int128, t => t.isValue = true ∧ t.hasRefs = false ∧ t.size ≤ 16
  | .objectSlot, t => t.isValue = false
  | .array, _ => True

/-- The staging for a value of type `t`, as both builders choose it now. -/
def stagingFor (t : Ty) : Staging :=
  if t.isValue = true ∧ t.hasRefs = false ∧ t.size ≤ 16 then .int128
  else if t.isValue = false then .objectSlot
  else .array

theorem fixed_holds (t : Ty) : Holds (stagingFor t) t := by
  unfold stagingFor
  split
  · assumption
  · split
    · assumption
    · trivial

/-- A saver before the fix: tests on the source type `src`, stages the target type `dst`; `none` threw. -/
def previousSaver (src dst : Ty) : Option Staging :=
  if src.isValue = true ∧ dst.hasRefs = false ∧ dst.size ≤ 16 then some .int128
  else if src.isValue = false then some .objectSlot
  else none

def object : Ty := ⟨false, 8, true⟩
def guid : Ty := ⟨true, 16, false⟩
def int : Ty := ⟨true, 4, false⟩

/-- `object` is the source of the event serializer's complex savers; `object` as target has no fields of its own. -/
def objectAsTarget : Ty := ⟨false, 8, false⟩

theorem previous_saver_unsafe :
    previousSaver object guid = some .objectSlot ∧ ¬ Holds .objectSlot guid ∧
    previousSaver int objectAsTarget = some .int128 ∧ ¬ Holds .int128 objectAsTarget := by
  refine ⟨rfl, ?_, rfl, ?_⟩ <;> simp [Holds, guid, objectAsTarget]

/-- A loader before the fix, staging the source type it loads. -/
def previousLoader (src : Ty) : Option Staging :=
  if src.isValue = true ∧ src.hasRefs = false ∧ src.size ≤ 16 then some .int128
  else if src.isValue = false then some .objectSlot
  else none

def nullableDateTimeOffset : Ty := ⟨true, 24, false⟩

theorem previous_loader_missing : previousLoader nullableDateTimeOffset = none := by decide

end Btdb.ConverterStaging
