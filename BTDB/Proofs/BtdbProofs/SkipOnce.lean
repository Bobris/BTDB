/-!
# Each member deserializer consumes exactly its own value

A Bon class's deserializer (`../Serialization/BonSerializer.cs`) reads its members' values one after another: the
deserializer of each member consumes the values from where the one before stopped. Before the 2026-10-28 fix a member
holding a class of another name consumed two values, the class read and then a skip.

* `fixed_aligned`: when every member consumes one value, member `i` reads value `i`.
* `previous_shifted`: when member `i` consumes two, every member after it reads the value after its own, and the last
  one reads past the class.
-/

namespace Btdb.SkipOnce

/-- The position each member starts reading at, given how many values each consumes. -/
def starts : List Nat → Nat → List Nat
  | [], _ => []
  | c :: rest, pos => pos :: starts rest (pos + c)

theorem starts_length : ∀ (cs : List Nat) (pos : Nat), (starts cs pos).length = cs.length
  | [], _ => rfl
  | _ :: rest, pos => by simp [starts, starts_length rest]

theorem fixed_aligned : ∀ (k pos : Nat) (i : Nat) (h : i < k),
    (starts (List.replicate k 1) pos)[i]'(by simp [starts_length]; exact h) = pos + i
  | 0, _, _, h => absurd h (Nat.not_lt_zero _)
  | k + 1, pos, 0, _ => by simp [List.replicate_succ, starts]
  | k + 1, pos, i + 1, h => by
    simp only [List.replicate_succ, starts, List.getElem_cons_succ]
    rw [fixed_aligned k (pos + 1) i (by omega)]
    omega

theorem previous_shifted (before after : Nat) (j : Nat) (hj : j < after) :
    (starts (List.replicate before 1 ++ 2 :: List.replicate after 1) 0)[before + 1 + j]'(by
      simp [starts_length]; omega) = before + 1 + j + 1 := by
  have split : ∀ (b pos : Nat) (rest : List Nat), starts (List.replicate b 1 ++ rest) pos =
      starts (List.replicate b 1) pos ++ starts rest (pos + b) := by
    intro b
    induction b with
    | zero => intro pos rest; simp [starts]
    | succ b ih =>
      intro pos rest
      simp only [List.replicate_succ, List.cons_append, starts]
      rw [ih (pos + 1) rest]
      simp [Nat.add_assoc, Nat.add_comm 1 b]
  simp only [split, starts]
  rw [List.getElem_append_right (by simp [starts_length]; omega)]
  simp only [starts_length, List.length_replicate]
  have : before + 1 + j - before = j + 1 := by omega
  simp only [this, List.getElem_cons_succ]
  rw [fixed_aligned after (0 + before + 2) j hj]
  omega

end Btdb.SkipOnce
