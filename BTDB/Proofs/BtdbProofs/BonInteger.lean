/-!
# Bon reads an integer as a `long` exactly when it is one

Bon (`../Bon/Bon.cs`) writes an integer as a sign and a magnitude: a non-negative value as its magnitude and a negative
one as the magnitude of its negation, so `long.MinValue` has magnitude 2^63. `Helpers.TryGetLong` accepts a
non-negative magnitude up to `long.MaxValue` and a negative one up to 2^63 and reports any other as not a `long`.
Before the 2026-10-29 fix it cast the magnitude to `long`, so a value above `long.MaxValue` came back wrapped.
The typed deserializers then accept a `long` only within the member's range (`fits`).

* `read_write`: every `long` reads back as itself.
* `read_exact`: a value read is the integer that was stored, and is a `long`.
* `fits_exact`: a member is set only to the integer that was stored, within its range.
* `previous_wraps`: before the fix `ulong.MaxValue` read as `-1`.
-/

namespace Btdb.BonInteger

def LongMin : Int := -(2 ^ 63)
def LongMax : Int := 2 ^ 63 - 1

/-- A stored integer: its sign and magnitude. -/
inductive Stored where
  | pos (magnitude : Nat)
  | neg (magnitude : Nat)

def Stored.value : Stored → Int
  | .pos m => m
  | .neg m => -(m : Int)

def write (v : Int) : Stored := if 0 ≤ v then .pos v.toNat else .neg (-v).toNat

def read : Stored → Option Int
  | .pos m => if (m : Int) ≤ LongMax then some m else none
  | .neg m => if (m : Int) ≤ -LongMin then some (-(m : Int)) else none

/-- A deserializer of a member whose type ranges from `lo` to `hi` keeps a value read within it. -/
def fits (lo hi : Int) (s : Stored) : Option Int :=
  match read s with
  | some v => if lo ≤ v ∧ v ≤ hi then some v else none
  | none => none

theorem read_write (v : Int) (h : LongMin ≤ v ∧ v ≤ LongMax) : read (write v) = some v := by
  have hmax : v ≤ LongMax := h.2
  have hmin : -(2 ^ 63 : Int) ≤ v := h.1
  unfold write
  by_cases hv : 0 ≤ v
  · simp [hv, read, Int.toNat_of_nonneg hv, hmax]
  · have hn : 0 ≤ -v := by omega
    have hle : -v ≤ -LongMin := by unfold LongMin at *; omega
    simp [hv, read, Int.toNat_of_nonneg hn, hle]

theorem read_exact (s : Stored) (v : Int) (h : read s = some v) : v = s.value ∧ LongMin ≤ v ∧ v ≤ LongMax := by
  cases s with
  | pos m =>
    by_cases hm : (m : Int) ≤ LongMax
    · simp only [read, hm, ite_true, Option.some.injEq] at h
      subst h
      exact ⟨rfl, by unfold LongMin; omega, hm⟩
    · simp [read, hm] at h
  | neg m =>
    by_cases hm : (m : Int) ≤ -LongMin
    · simp only [read, hm, ite_true, Option.some.injEq] at h
      subst h
      exact ⟨rfl, by omega, by unfold LongMax; omega⟩
    · simp [read, hm] at h

theorem fits_exact (lo hi : Int) (s : Stored) (v : Int) (h : fits lo hi s = some v) :
    v = s.value ∧ lo ≤ v ∧ v ≤ hi := by
  unfold fits at h
  cases hw : read s with
  | none => simp [hw] at h
  | some w =>
    by_cases hr : lo ≤ w ∧ w ≤ hi
    · simp only [hw, hr, and_self, ite_true, Option.some.injEq] at h
      subst h
      exact ⟨(read_exact s w hw).1, hr⟩
    · simp [hw, hr] at h

/-- Before the fix: the magnitude cast to `long`, keeping its low 64 bits as a two's complement number. -/
def previous : Stored → Int
  | .pos m => if m % 2 ^ 64 < 2 ^ 63 then (m % 2 ^ 64 : Nat) else (m % 2 ^ 64 : Nat) - 2 ^ 64
  | .neg m => -(if m % 2 ^ 64 < 2 ^ 63 then ((m % 2 ^ 64 : Nat) : Int) else (m % 2 ^ 64 : Nat) - 2 ^ 64)

theorem previous_wraps : previous (.pos (2 ^ 64 - 1)) = -1 ∧ (Stored.pos (2 ^ 64 - 1)).value ≠ -1 ∧
    read (.pos (2 ^ 64 - 1)) = none := by
  decide

end Btdb.BonInteger
