/-!
# A list key encoding is prefix free

`ListFieldOrderedHandler` (`../FieldHandler/ListFieldOrderedHandler.cs`) writes a list key as its count, then its items;
the source generator writes list key lookups the same way since the 2026-10-29 fix. With a prefix free encoding of
the count (VUInt, `VUInt.lean`) and of the items, no list's key is a prefix of another's, so a list key can be followed
by other fields (`KeyConcat.lean`) and a lookup by it matches exactly the equal list.

* `list_prefix_free`: equal keys, or one a prefix of the other, mean equal lists.
* `needs_count`: without the count, the key of `[1]` is a prefix of the key of `[1, 2]`.
-/

namespace Btdb.ListKey

variable {α : Type}

def PrefixFree {β : Type} (enc : β → List Nat) : Prop := ∀ a b, enc a <+: enc b → a = b

/-- Two keys that are both prefixes of one key are prefixes one of the other. -/
theorem prefix_of_append_prefix (u v x y : List Nat) (h : u ++ x <+: v ++ y) : u <+: v ∨ v <+: u :=
  List.prefix_or_prefix_of_prefix ((List.prefix_append u x).trans h) (List.prefix_append v y)

theorem items_prefix_free (enc : α → List Nat) (hp : PrefixFree enc) :
    ∀ (xs ys : List α), xs.length = ys.length → xs.flatMap enc <+: ys.flatMap enc → xs = ys
  | [], [], _, _ => rfl
  | [], _ :: _, hl, _ => by simp at hl
  | _ :: _, [], hl, _ => by simp at hl
  | x :: xs, y :: ys, hl, h => by
    simp only [List.flatMap_cons] at h
    have hxy : x = y := by
      rcases prefix_of_append_prefix _ _ _ _ h with h' | h'
      · exact hp x y h'
      · exact (hp y x h').symm
    subst hxy
    rw [List.prefix_append_right_inj] at h
    rw [items_prefix_free enc hp xs ys (by simpa using hl) h]

/-- The key of a list: its count, then its items. -/
def encList (count : Nat → List Nat) (enc : α → List Nat) (xs : List α) : List Nat :=
  count xs.length ++ xs.flatMap enc

theorem list_prefix_free (count : Nat → List Nat) (enc : α → List Nat) (hc : PrefixFree count)
    (hp : PrefixFree enc) (xs ys : List α) (h : encList count enc xs <+: encList count enc ys) : xs = ys := by
  unfold encList at h
  have hl : xs.length = ys.length := by
    rcases prefix_of_append_prefix _ _ _ _ h with h' | h'
    · exact hc _ _ h'
    · exact (hc _ _ h').symm
  rw [hl, List.prefix_append_right_inj] at h
  exact items_prefix_free enc hp xs ys hl h

theorem needs_count : ([1] : List Nat).flatMap (fun n => [n]) <+: ([1, 2] : List Nat).flatMap (fun n => [n]) :=
  ⟨[2], rfl⟩

end Btdb.ListKey
