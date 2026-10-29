/-!
# A Roaring container's encoding is the shortest, and its length tells its kind

`RoaringBitmaps.ChooseOutputKind` (`../Buffer/RoaringBitmaps.cs`) picks how a container of `c` set bits in `r` runs
is written: an array of `2 * c` bytes, runs of `4 * r + 1` bytes, or the raw bitmap of 8192 bytes, preferring the
bitmap, then the array, on ties. `ValidateAndGetKind` reads the kind back from the length alone: 8192 is the bitmap, an
even length an array, an odd one runs.

* `choose_shortest`: the chosen encoding is no longer than any of the three.
* `kind_roundtrip`: for a container with a set bit, the kind read back from the chosen length is the chosen kind.
-/

namespace Btdb.RoaringKind

inductive Kind where
  | array | rle | bitmap
  deriving DecidableEq

def bitmapSize : Nat := 8192

def lengthFor (c r : Nat) : Kind → Nat
  | .array => 2 * c
  | .rle => 4 * r + 1
  | .bitmap => bitmapSize

/-- `ChooseOutputKind`: the array replaces the bitmap when shorter, then runs replace the best when shorter. -/
def choose (c r : Nat) : Kind :=
  if 2 * c < bitmapSize then (if 4 * r + 1 < 2 * c then .rle else .array)
  else (if 4 * r + 1 < bitmapSize then .rle else .bitmap)

/-- `ValidateAndGetKind` of a nonempty container no longer than the bitmap. -/
def kindOf (length : Nat) : Kind :=
  if length = bitmapSize then .bitmap else if length % 2 = 0 then .array else .rle

theorem choose_shortest (c r : Nat) (k : Kind) : lengthFor c r (choose c r) ≤ lengthFor c r k := by
  unfold choose
  by_cases h1 : 2 * c < bitmapSize
  · rw [ite_eq_left h1]
    by_cases h2 : 4 * r + 1 < 2 * c
    · rw [ite_eq_left h2]; cases k <;> simp only [lengthFor, bitmapSize] at h1 h2 ⊢ <;> omega
    · rw [ite_eq_right h2]; cases k <;> simp only [lengthFor, bitmapSize] at h1 h2 ⊢ <;> omega
  · rw [ite_eq_right h1]
    by_cases h3 : 4 * r + 1 < bitmapSize
    · rw [ite_eq_left h3]; cases k <;> simp only [lengthFor, bitmapSize] at h1 h3 ⊢ <;> omega
    · rw [ite_eq_right h3]; cases k <;> simp only [lengthFor, bitmapSize] at h1 h3 ⊢ <;> omega

theorem kind_roundtrip (c r : Nat) (hc : 0 < c) : kindOf (lengthFor c r (choose c r)) = choose c r := by
  have rle : kindOf (4 * r + 1) = .rle := by
    unfold kindOf bitmapSize
    rw [ite_eq_right (by omega), ite_eq_right (by omega)]
  unfold choose
  by_cases h1 : 2 * c < bitmapSize
  · rw [ite_eq_left h1]
    by_cases h2 : 4 * r + 1 < 2 * c
    · rw [ite_eq_left h2]; exact rle
    · rw [ite_eq_right h2]
      show kindOf (2 * c) = .array
      unfold kindOf bitmapSize
      unfold bitmapSize at h1
      rw [ite_eq_right (by omega), ite_eq_left (by omega)]
  · rw [ite_eq_right h1]
    by_cases h3 : 4 * r + 1 < bitmapSize
    · rw [ite_eq_left h3]; exact rle
    · rw [ite_eq_right h3]; rfl

end Btdb.RoaringKind
