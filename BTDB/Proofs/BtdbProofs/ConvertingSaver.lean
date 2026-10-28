/-!
# A converting saver converts to the type it saves

A type descriptor's `GenerateSave(targetType)` saves the types it knows directly and otherwise returns
`BuildConvertingSaver(from, to)` (`../FieldHandler/Extensions.cs`), which converts a value of `from` to `to` and saves it
with `GenerateSave(to)`. A descriptor must therefore convert from `targetType` to a type it saves. Before the 2026-10-28
fix the byte array descriptor and field handlers passed the two types swapped, from `byte[]` to `targetType`, so saving
an object item of a list loaded without its type asked to save `targetType` again until the stack overflowed. Types are
numbers, `known` are those saved directly, and `preferred` is the one converted to.

* `fixed_ends`: converting to the preferred type, creating the saver of any type takes at most two calls.
* `previous_never_ends`: with the types swapped, a type not saved directly exhausts any bound.
-/

namespace Btdb.ConvertingSaver

variable (known : Nat → Bool) (preferred : Nat)

/-- `GenerateSave`, with `swapped` for the order before the fix; `none` when the call depth runs out. -/
def generateSave (swapped : Bool) : Nat → Nat → Option Unit
  | 0, _ => none
  | fuel + 1, t =>
    if known t then some ()
    else
      -- BuildConvertingSaver(from, to) creates GenerateSave(to)
      let to := if swapped then t else preferred
      generateSave swapped fuel to

theorem fixed_ends (hp : known preferred = true) (t : Nat) : generateSave known preferred false 2 t = some () := by
  simp only [generateSave]
  split
  · rfl
  · simp [hp]

theorem previous_never_ends (t : Nat) (ht : known t = false) : ∀ fuel, generateSave known preferred true fuel t = none
  | 0 => rfl
  | fuel + 1 => by
    simp only [generateSave, ht, Bool.false_eq_true, ite_false, ite_true]
    exact previous_never_ends t ht fuel

end Btdb.ConvertingSaver
