/-!
# Clipping a range tombstone at table boundaries keeps its coverage

A model of `LsmCompaction.Clip` (`../Engine/LsmCompaction.cs`). A compaction splits its output into tables at
boundary keys; a fragment `[s, e)` crossing them is cut into pieces, one per table, and each piece gets
`coveredBelow = Rank(end) − Rank(start)` over the components below. A consistent fragment has
`coveredBelow = Rank(e) − Rank(s)` (`Rank.lean`). `rank` is any function: only its differences are used. Pieces
covering nothing are dropped, which does not change the sum.

* `pieces_cover`: the pieces' coverage adds up to the fragment's, wherever the boundaries are, so a component's
  `CoveredSum`, and with it `CountFrom`, stays exact after clipping.
* `dropped_zero`: dropping the pieces covering nothing keeps the sum.
-/

namespace Lsm.ClipCoverage

/-- Coverage of the pieces of `[s, e)` cut at the boundaries `bs`, each boundary inside the fragment and ascending. -/
def pieces (rank : Nat → Int) (s : Nat) : List Nat → Nat → Int
  | [], e => rank e - rank s
  | b :: bs, e => (rank b - rank s) + pieces rank b bs e

theorem pieces_cover (rank : Nat → Int) (s e : Nat) (bs : List Nat) : pieces rank s bs e = rank e - rank s := by
  induction bs generalizing s with
  | nil => rfl
  | cons b bs ih =>
    simp only [pieces, ih b]
    omega

/-- The coverages kept: zero ones are dropped. -/
def kept : List Int → List Int
  | [] => []
  | c :: cs => if c = 0 then kept cs else c :: kept cs

theorem dropped_zero (cs : List Int) : (kept cs).sum = cs.sum := by
  induction cs with
  | nil => rfl
  | cons c cs ih =>
    simp only [kept]
    split
    · next h => simp [ih, h]
    · simp [ih]

end Lsm.ClipCoverage
