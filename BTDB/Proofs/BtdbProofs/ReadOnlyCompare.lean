import BtdbProofs.KeyRange

/-!
# Comparing a stored key of the read-only database

A model of `ReadOnlyKeyValueDBCursor.Compare`. A node of the read-only database stores the common prefix of its keys
once and each key's suffix after it; the search compares the key, cut to `maxLength` bytes, with a target without
assembling it.

* `compare_correct`: the result is the comparison of the cut key `prefix ++ suffix` with the target.
-/

namespace Btdb.ReadOnlyCompare

open KeyRange

/-- `Compare(prefix, suffix, target, maxLength)` as the code computes it. -/
def compare (p s t : List Nat) (m : Nat) : Ordering :=
  if m ≤ p.length then cmp (p.take m) t
  else
    let c := min p.length t.length
    let first := cmp (p.take c) (t.take c)
    if first ≠ .eq then first
    else if c < p.length then .gt
    else cmp (s.take (m - p.length)) (t.drop p.length)

theorem cmp_self : ∀ a, cmp a a = .eq
  | [] => rfl
  | x :: xs => by simp [cmp, cmp_self xs]

theorem eq_of_cmp_eq : ∀ a b, cmp a b = .eq → a = b
  | [], [], _ => rfl
  | [], _ :: _, h => by simp [cmp] at h
  | _ :: _, [], h => by simp [cmp] at h
  | x :: xs, y :: ys, h => by
    simp only [cmp] at h
    by_cases h1 : x < y
    · simp [h1] at h
    · by_cases h2 : y < x
      · simp [h1, h2] at h
      · simp only [h1, h2, ite_false] at h
        rw [show x = y by omega, eq_of_cmp_eq xs ys h]

/-- A strictly longer key starting with the target is bigger. -/
theorem cmp_extends : ∀ (t r : List Nat), r ≠ [] → cmp (t ++ r) t = .gt
  | [], r, h => by cases r with | nil => exact absurd rfl h | cons _ _ => rfl
  | x :: t, r, h => by simp [cmp, cmp_extends t r h]

theorem compare_correct (p s t : List Nat) (m : Nat) : compare p s t m = cmp ((p ++ s).take m) t := by
  unfold compare
  by_cases hm : m ≤ p.length
  · rw [ite_eq_left hm, List.take_append_of_le_length hm]
  · rw [ite_eq_right hm]
    have hkey : (p ++ s).take m = p ++ s.take (m - p.length) := by
      rw [List.take_append]; congr 1; exact List.take_of_length_le (by omega)
    simp only
    rw [hkey]
    have hl1 : (p.take (min p.length t.length)).length = min p.length t.length := by
      simp only [List.length_take]; omega
    have hl2 : (t.take (min p.length t.length)).length = min p.length t.length := by
      simp only [List.length_take]; omega
    by_cases hf : cmp (p.take (min p.length t.length)) (t.take (min p.length t.length)) = .eq
    · rw [ite_eq_right (by simp [hf])]
      have he := eq_of_cmp_eq _ _ hf
      by_cases hc : min p.length t.length < p.length
      · rw [ite_eq_left hc]
        -- The target ends inside the prefix and equals its start.
        have htl : min p.length t.length = t.length := by omega
        rw [htl, List.take_of_length_le (Nat.le_refl _)] at he
        have hd : p.drop t.length ≠ [] := by
          intro h; have := congrArg List.length h; simp at this; omega
        have key_eq : t ++ (p.drop t.length ++ s.take (m - p.length)) = p ++ s.take (m - p.length) := by
          rw [← List.append_assoc]
          congr 1
          calc t ++ p.drop t.length = p.take t.length ++ p.drop t.length := by rw [he]
            _ = p := List.take_append_drop _ _
        rw [← key_eq]
        exact (cmp_extends t (p.drop t.length ++ s.take (m - p.length)) (by simp [hd])).symm
      · rw [ite_eq_right hc]
        have hpl : min p.length t.length = p.length := by omega
        rw [hpl, List.take_of_length_le (Nat.le_refl _)] at he
        conv => rhs; rw [show t = t.take p.length ++ t.drop p.length from (List.take_append_drop _ _).symm, ← he]
        rw [cmp_prefix]
    · rw [ite_eq_left (by simpa using hf)]
      -- The comparison is decided within the first c bytes.
      have hne : p.take (min p.length t.length) ≠ t.take (min p.length t.length) := fun h =>
        hf (by rw [h]; exact cmp_self _)
      have hnp1 : ¬ p.take (min p.length t.length) <+: t.take (min p.length t.length) := fun h =>
        hne (h.eq_of_length (by rw [hl1, hl2]))
      have hnp2 : ¬ t.take (min p.length t.length) <+: p.take (min p.length t.length) := fun h =>
        hne (h.eq_of_length (by rw [hl1, hl2])).symm
      have h3 := cmp_append _ _ (p.drop (min p.length t.length) ++ s.take (m - p.length))
        (t.drop (min p.length t.length)) hnp1 hnp2
      rw [List.take_append_drop, ← List.append_assoc, List.take_append_drop] at h3
      exact h3.symm

end Btdb.ReadOnlyCompare
