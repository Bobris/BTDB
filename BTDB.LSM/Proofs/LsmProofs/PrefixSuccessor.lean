/-!
# The successor of a key prefix

A model of `LsmKeys.PrefixSuccessor` (`../Format/LsmEncoding.cs`). Keys are lists of bytes (naturals up to 255) in
lexicographic order. `PrefixSuccessor(p)` drops the trailing `0xff` bytes of `p` and increments the last byte left; it
returns null when none is left. Write `p = q ++ x :: 255…255` with `x < 255`, so the result is `q ++ [x + 1]`.

* `prefix_successor_correct`: a key is below the result exactly when it is below `p` or starts with `p`, so a scan
  from `p` up to the result visits exactly the keys starting with `p`.
* `no_successor`: when `p` is all `0xff` bytes, every key is below `p` or starts with it, so no key is above them all
  and null is right.
-/

namespace Lsm.PrefixSuccessor

/-- Lexicographic order of byte strings, as `SequenceCompareTo` orders them. -/
def Lt : List Nat → List Nat → Prop
  | [], _ :: _ => True
  | a :: as, b :: bs => a < b ∨ (a = b ∧ Lt as bs)
  | _, _ => False

def IsPrefix : List Nat → List Nat → Prop
  | [], _ => True
  | a :: as, b :: bs => a = b ∧ IsPrefix as bs
  | _ :: _, [] => False

def Bytes (k : List Nat) : Prop := ∀ b ∈ k, b ≤ 255

theorem below_or_starts (m : Nat) (k : List Nat) (hk : Bytes k) :
    Lt k (List.replicate m 255) ∨ IsPrefix (List.replicate m 255) k := by
  induction m generalizing k with
  | zero => exact Or.inr (by cases k <;> trivial)
  | succ m ih =>
    cases k with
    | nil => exact Or.inl trivial
    | cons b k =>
      have hb : b ≤ 255 := hk b List.mem_cons_self
      have hk' : Bytes k := fun c hc => hk c (List.mem_cons_of_mem _ hc)
      simp only [List.replicate_succ, Lt, IsPrefix]
      rcases Nat.lt_or_ge b 255 with h | h
      · exact Or.inl (Or.inl h)
      · have hb255 : b = 255 := by omega
        rcases ih k hk' with h' | h'
        · exact Or.inl (Or.inr ⟨hb255, h'⟩)
        · exact Or.inr ⟨hb255.symm, h'⟩

theorem prefix_successor_correct (q : List Nat) (x m : Nat) (k : List Nat) (hk : Bytes k) :
    Lt k (q ++ [x + 1]) ↔ Lt k (q ++ x :: List.replicate m 255) ∨ IsPrefix (q ++ x :: List.replicate m 255) k := by
  induction q generalizing k with
  | nil =>
    cases k with
    | nil => simp [Lt]
    | cons b k =>
      have hk' : Bytes k := fun c hc => hk c (List.mem_cons_of_mem _ hc)
      simp only [List.nil_append, Lt, IsPrefix]
      have hrest := below_or_starts m k hk'
      constructor
      · rintro (h | ⟨h, h'⟩)
        · rcases Nat.lt_or_ge b x with h1 | h1
          · exact Or.inl (Or.inl h1)
          · have : b = x := by omega
            rcases hrest with h2 | h2
            · exact Or.inl (Or.inr ⟨this, h2⟩)
            · exact Or.inr ⟨this.symm, h2⟩
        · cases k <;> simp at h'
      · rintro ((h | ⟨h, _⟩) | ⟨h, _⟩)
        · exact Or.inl (by omega)
        · exact Or.inl (by omega)
        · exact Or.inl (by omega)
  | cons c q ih =>
    cases k with
    | nil => simp [Lt]
    | cons b k =>
      have hk' : Bytes k := fun c hc => hk c (List.mem_cons_of_mem _ hc)
      simp only [List.cons_append, Lt, IsPrefix]
      rw [ih k hk']
      constructor
      · rintro (h | ⟨h, h' | h'⟩)
        · exact Or.inl (Or.inl h)
        · exact Or.inl (Or.inr ⟨h, h'⟩)
        · exact Or.inr ⟨h.symm, h'⟩
      · rintro ((h | ⟨h, h'⟩) | ⟨h, h'⟩)
        · exact Or.inl h
        · exact Or.inr ⟨h, Or.inl h'⟩
        · exact Or.inr ⟨h.symm, Or.inr h'⟩

theorem no_successor (m : Nat) (k : List Nat) (hk : Bytes k) :
    Lt k (List.replicate m 255) ∨ IsPrefix (List.replicate m 255) k :=
  below_or_starts m k hk

end Lsm.PrefixSuccessor
