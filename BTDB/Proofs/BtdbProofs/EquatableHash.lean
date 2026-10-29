/-!
# Hashing an equatable array

A model of `EquatableArray<T>` (`../Collections/EquatableArray.cs`, and its copy in the source generator). The array
is `none` for the default value, whose items are those of an empty array, or `some items`. Two arrays are equal when
their items are (`AsSpan().SequenceEqual`). `GetHashCode` combines the items with `HashCode`, here any function `h`
of the items; `HashCode` of no items is a per-process seed, not 0.

* `hash_respects_equal`: the hash returning 0 for the default and for an empty array gives equal arrays equal
  hashes, as dictionaries and sets need.
* `old_hash_breaks`: the hash before the 2026-10-29 fix, 0 only for the default array, gave the default and an empty
  array, which are equal, different hashes whenever `h []` is not 0
  (`EquatableArrayTests.DefaultAndEmptyArraysHashAlike`).
-/

namespace Btdb.EquatableHash

def items : Option (List Nat) → List Nat
  | none => []
  | some l => l

def Equal (a b : Option (List Nat)) : Prop := items a = items b

def hash (h : List Nat → Nat) : Option (List Nat) → Nat
  | some (x :: xs) => h (x :: xs)
  | _ => 0

def oldHash (h : List Nat → Nat) : Option (List Nat) → Nat
  | none => 0
  | some l => h l

theorem hash_depends_on_items (h : List Nat → Nat) (a : Option (List Nat)) :
    hash h a = match items a with
      | [] => 0
      | x :: xs => h (x :: xs) := by
  cases a with
  | none => rfl
  | some l => cases l <;> rfl

theorem hash_respects_equal (h : List Nat → Nat) (a b : Option (List Nat)) (heq : Equal a b) :
    hash h a = hash h b := by
  rw [hash_depends_on_items, hash_depends_on_items, heq]

theorem old_hash_breaks (h : List Nat → Nat) (hseed : h [] ≠ 0) :
    Equal none (some []) ∧ oldHash h none ≠ oldHash h (some []) :=
  ⟨rfl, fun heq => hseed (by simpa [oldHash] using heq.symm)⟩

end Btdb.EquatableHash
