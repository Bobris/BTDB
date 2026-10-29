/-!
# A key of several fields sorts by its fields in order

A relation or dictionary key writes its fields one after another (`RelationInfo`, `TupleFieldHandler`). Each field
but the last is written with an encoding that keeps the order of its values and is prefix free, like VUInt, VInt and
the ordered string (`VUInt.lean`, `VInt.lean`, `StringOrdered.lean`); the last may be raw bytes, as a dictionary's
`byte[]` key is (`ByteArrayLastFieldHandler`). Keys are compared byte by byte, `lexLt`.

* `key_order`: a key starting with such a field sorts by that field first and by the rest of the key on a tie, so by
  induction a key sorts by its fields in order, and the last field's bytes order its values on a tie.
* `key_order_needs_prefix_free`: without prefix freedom the rest of the key can decide against the field's order.
-/

namespace Btdb.KeyConcat

/-- Byte by byte comparison of keys. -/
def lexLt : List Nat → List Nat → Prop
  | [], _ :: _ => True
  | a :: as, b :: bs => a < b ∨ (a = b ∧ lexLt as bs)
  | _, _ => False

theorem lexLt_irrefl : ∀ l : List Nat, ¬ lexLt l l
  | [] => by simp [lexLt]
  | a :: rest => by
    simp only [lexLt, Nat.lt_irrefl, false_or, true_and]
    exact lexLt_irrefl rest

theorem lexLt_append_same : ∀ (p x y : List Nat), lexLt (p ++ x) (p ++ y) ↔ lexLt x y
  | [], _, _ => Iff.rfl
  | a :: p, x, y => by
    simp only [List.cons_append, lexLt, Nat.lt_irrefl, false_or, true_and]
    exact lexLt_append_same p x y

/-- When neither encoding is a prefix of the other, they decide the comparison before either ends. -/
theorem lexLt_diverge : ∀ (u v x y : List Nat), ¬ u <+: v → ¬ v <+: u →
    (lexLt (u ++ x) (v ++ y) ↔ lexLt u v)
  | [], v, _, _, h, _ => absurd (List.nil_prefix (l := v)) h
  | _ :: _, [], _, _, _, h => absurd (List.nil_prefix) h
  | a :: u, b :: v, x, y, huv, hvu => by
    simp only [List.cons_append, lexLt]
    by_cases hab : a = b
    · subst hab
      have huv' : ¬ u <+: v := fun h => huv (List.cons_prefix_cons.2 ⟨rfl, h⟩)
      have hvu' : ¬ v <+: u := fun h => hvu (List.cons_prefix_cons.2 ⟨rfl, h⟩)
      simp only [Nat.lt_irrefl, false_or, true_and]
      exact lexLt_diverge u v x y huv' hvu'
    · simp only [hab, false_and, or_false]

theorem key_order {α : Type} (enc : α → List Nat) (lt : α → α → Prop)
    (prefixFree : ∀ a b, enc a <+: enc b → a = b) (ordered : ∀ a b, lt a b ↔ lexLt (enc a) (enc b))
    (a b : α) (x y : List Nat) :
    lexLt (enc a ++ x) (enc b ++ y) ↔ lt a b ∨ (a = b ∧ lexLt x y) := by
  by_cases hab : a = b
  · subst hab
    rw [lexLt_append_same, ordered]
    simp [lexLt_irrefl]
  · rw [lexLt_diverge _ _ _ _ (fun h => hab (prefixFree a b h)) (fun h => hab (prefixFree b a h).symm),
      ← ordered]
    simp [hab]

/-- An encoding writing 1 as [1] and 2 as [1, 0] keeps their order, yet the key of 2 with an empty rest sorts before
the key of 1 with the rest [5]. -/
theorem key_order_needs_prefix_free : lexLt [1] [1, 0] ∧ lexLt ([1, 0] ++ []) ([1] ++ [5]) := by
  simp [lexLt]

end Btdb.KeyConcat
