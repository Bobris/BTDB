/-!
# UTF-8 takes at most three bytes per UTF-16 code unit

`ReflectionMetadata.FindByName(ReadOnlySpan<char>)` (`../../Serialization/ReflectionMetadata.cs`) encodes a name of up
to 256 code units into a stack buffer of three bytes per code unit. `Encoding.UTF8` writes a code unit below 0x80 as one
byte, below 0x800 as two, a high surrogate followed by a low one (a code point above 0xFFFF) as four, and any other
unit, a lone surrogate becoming U+FFFD, as three.

* `utf8_le_three`: the encoding of any sequence of code units is at most three bytes per code unit, so the buffer always
  fits it.
-/

namespace Btdb.Utf8Length

def isHigh (c : Nat) : Bool := 0xD800 ≤ c && c < 0xDC00
def isLow (c : Nat) : Bool := 0xDC00 ≤ c && c < 0xE000

/-- Bytes `Encoding.UTF8` writes for UTF-16 code units. -/
def utf8Length : List Nat → Nat
  | [] => 0
  | c :: d :: r => if isHigh c && isLow d then 4 + utf8Length r else unitLength c + utf8Length (d :: r)
  | [c] => unitLength c
where
  unitLength (c : Nat) : Nat := if c < 0x80 then 1 else if c < 0x800 then 2 else 3

theorem unit_le (c : Nat) : utf8Length.unitLength c ≤ 3 := by
  unfold utf8Length.unitLength
  split
  · omega
  · split <;> omega

theorem utf8_le_three : ∀ units : List Nat, utf8Length units ≤ 3 * units.length
  | [] => by simp [utf8Length]
  | [c] => by simp only [utf8Length, List.length_singleton]; have := unit_le c; omega
  | c :: d :: r => by
    simp only [utf8Length, List.length_cons]
    split
    · have := utf8_le_three r; omega
    · have := utf8_le_three (d :: r); have := unit_le c; simp only [List.length_cons] at *; omega

end Btdb.Utf8Length
