import BtdbProofs.StringOrdered

/-!
# The order of string keys

`StringOrdered.lean` shows ordinal `StartsWith` and equality can scan encoded key prefixes. This file looks at the order
of whole keys, `WriteStringOrdered`: the tokens of a string (code points plus one, a surrogate pair being one token)
each packed with `UnsafePackVUInt`, then a zero byte.

* `enc_order`: keys sort as the token lists of their strings, lexicographically, so string keys are in code-point
  order.
* `differs_from_ordinal`: that is not ordinal UTF-16 order: "😀", a surrogate pair, sorts after "" as a key, but
  before it ordinally. So `string.CompareOrdinal` against a key cannot be a scan of a key range; the relation query
  engine evaluates it on the values, matching LINQ to Objects.
-/

namespace Btdb.StringKeyOrder

open Btdb.VUInt
open Btdb.StringOrdered
open Btdb.KeyRange

theorem not_prefix_of_lt {x y : Nat} (hxy : x < y) (hy : y < 2 ^ 64) (hx : x < 2 ^ 64) :
    ¬vuint x <+: vuint y ∧ ¬vuint y <+: vuint x := by
  refine ⟨fun h => ?_, fun h => ?_⟩
  · have := vuint_prefix_free hx hy h
    omega
  · have := vuint_prefix_free hy hx h
    omega

theorem tokens_order : ∀ (a b : List Nat), (∀ t ∈ a, 1 ≤ t ∧ t < 2 ^ 64) → (∀ t ∈ b, 1 ≤ t ∧ t < 2 ^ 64) →
    cmp a b = .lt → cmp (a.flatMap vuint ++ [0]) (b.flatMap vuint ++ [0]) = .lt
  | [], [], _, _, h => by simp [cmp] at h
  | [], y :: ys, _, hb, _ => by
    obtain ⟨c, r, hv, hc⟩ := vuint_head y (hb y List.mem_cons_self).1 (hb y List.mem_cons_self).2
    simp only [List.flatMap_nil, List.nil_append, List.flatMap_cons, hv, List.cons_append, cmp]
    have : 0 < c := Nat.pos_of_ne_zero hc
    simp [this]
  | _ :: _, [], _, _, h => by simp [cmp] at h
  | x :: xs, y :: ys, ha, hb, h => by
    have hx := ha x List.mem_cons_self
    have hy := hb y List.mem_cons_self
    simp only [cmp] at h
    simp only [List.flatMap_cons, List.append_assoc]
    by_cases hlt : x < y
    · obtain ⟨n1, n2⟩ := not_prefix_of_lt hlt hy.2 hx.2
      rw [cmp_append _ _ _ _ n1 n2]
      exact vuint_order hlt hy.2
    · by_cases hgt : y < x
      · simp [hlt, hgt] at h
      · have heq : x = y := by omega
        subst heq
        simp only [hlt, ↓reduceIte] at h
        rw [cmp_prefix]
        exact tokens_order xs ys (fun t ht => ha t (List.mem_cons_of_mem _ ht))
          (fun t ht => hb t (List.mem_cons_of_mem _ ht)) h

theorem enc_order (s t : List Nat) (hs : Units s) (ht : Units t) (h : cmp (toks s) (toks t) = .lt) :
    cmp (enc s) (enc t) = .lt :=
  tokens_order _ _ (toks_bound s hs) (toks_bound t ht) h

/-- "😀" is the surrogate pair D83D DE00, one token 0x1F601; "" is one token 0xE001. -/
theorem differs_from_ordinal :
    cmp (toks [0xD83D, 0xDE00]) (toks [0xE000]) = .gt ∧ cmp [0xD83D, 0xDE00] [0xE000] = .lt := by
  decide

end Btdb.StringKeyOrder
