/-!
# Splitting WAL records into chunk parts

A model of the inner loop of `LsmKeyValueDB.PlanChunks` (`../LsmKeyValueDB.Wal.cs`). A record's payload is cut into
parts, each as long as the room left in the current chunk allows (at least 1024 bytes there; the model only needs at
least one), and the loop runs at least once, so an empty payload still gives one part. Each part is flagged first
when it starts at offset 0 and last when it ends the payload. The room of each step depends on the chunks filled so
far; the model takes it from an arbitrary function of the step.

* `split_flatten`: the parts' bytes put together are the payload, so replay rebuilds it.
* `split_first`: only the first part is flagged first.
* `split_last`: only the final part is flagged last.
* `split_empty`: an empty payload is one empty part flagged first and last.
-/

namespace Lsm.WalSplit

structure Part (α : Type) where
  first : Bool
  last : Bool
  data : List α

def split {α : Type} (rooms : Nat → Nat) (i offset : Nat) (p : List α) : List (Part α) :=
  let length := min (max 1 (rooms i)) (p.length - offset)
  let part : Part α := ⟨offset == 0, offset + length == p.length, (p.drop offset).take length⟩
  if offset + length < p.length then part :: split rooms (i + 1) (offset + length) p else [part]
termination_by p.length - offset
decreasing_by omega

theorem split_flatten {α : Type} (rooms : Nat → Nat) (p : List α) (i offset : Nat) :
    offset ≤ p.length → ((split rooms i offset p).map Part.data).flatten = p.drop offset := by
  induction i, offset using split.induct rooms p with
  | case1 i offset len hlt ih =>
    simp only [len] at hlt ih ⊢
    intro h
    rw [split]
    simp only [hlt, ↓reduceIte, List.map_cons, List.flatten_cons]
    rw [ih (by omega)]
    have : List.drop (offset + min (max 1 (rooms i)) (p.length - offset)) p =
        List.drop (min (max 1 (rooms i)) (p.length - offset)) (List.drop offset p) := by
      rw [List.drop_drop]
    rw [this, List.take_append_drop]
  | case2 i offset len hlt =>
    simp only [len] at hlt ⊢
    intro h
    rw [split]
    simp only [hlt, ↓reduceIte, List.map_cons, List.map_nil, List.flatten_cons, List.flatten_nil,
      List.append_nil]
    apply List.take_of_length_le
    simp only [List.length_drop]
    omega

theorem split_tail_first {α : Type} (rooms : Nat → Nat) (p : List α) (i offset : Nat) :
    0 < offset → ∀ r ∈ split rooms i offset p, r.first = false := by
  induction i, offset using split.induct rooms p with
  | case1 i offset len hlt ih =>
    simp only [len] at hlt ih ⊢
    intro hpos r hr
    rw [split] at hr
    simp only [hlt, ↓reduceIte, List.mem_cons] at hr
    rcases hr with rfl | hr
    · simp only [beq_eq_false_iff_ne]
      omega
    · exact ih (by omega) r hr
  | case2 i offset len hlt =>
    simp only [len] at hlt ⊢
    intro hpos r hr
    rw [split] at hr
    simp only [hlt, ↓reduceIte, List.mem_singleton] at hr
    subst hr
    simp only [beq_eq_false_iff_ne]
    omega

theorem split_first {α : Type} (rooms : Nat → Nat) (i : Nat) (p : List α) :
    ∃ q rest, split rooms i 0 p = q :: rest ∧ q.first = true ∧ ∀ r ∈ rest, r.first = false := by
  rw [split]
  split
  · next hlt =>
    refine ⟨_, _, rfl, by simp, split_tail_first rooms p _ _ ?_⟩
    omega
  · exact ⟨_, [], rfl, by simp, by simp⟩

theorem split_last {α : Type} (rooms : Nat → Nat) (p : List α) (i offset : Nat) : offset ≤ p.length →
    ∃ init q, split rooms i offset p = init ++ [q] ∧ q.last = true ∧ ∀ r ∈ init, r.last = false := by
  induction i, offset using split.induct rooms p with
  | case1 i offset len hlt ih =>
    simp only [len] at hlt ih ⊢
    intro hle
    obtain ⟨init, q, h, hq, hinit⟩ := ih (by omega)
    refine ⟨⟨offset == 0, offset + min (max 1 (rooms i)) (p.length - offset) == p.length,
      (p.drop offset).take (min (max 1 (rooms i)) (p.length - offset))⟩ :: init, q, ?_, hq, ?_⟩
    · rw [split]
      simp only [hlt, ↓reduceIte, h, List.cons_append]
    · intro r hr
      simp only [List.mem_cons] at hr
      rcases hr with rfl | hr
      · simp only [beq_eq_false_iff_ne]
        omega
      · exact hinit r hr
  | case2 i offset len hlt =>
    simp only [len] at hlt ⊢
    intro hle
    refine ⟨[], ⟨offset == 0, offset + min (max 1 (rooms i)) (p.length - offset) == p.length,
      (p.drop offset).take (min (max 1 (rooms i)) (p.length - offset))⟩, ?_, ?_, by simp⟩
    · rw [split]
      simp only [hlt, ↓reduceIte, List.nil_append]
    · simp only [beq_iff_eq]
      omega

theorem split_empty {α : Type} (rooms : Nat → Nat) (i : Nat) :
    split rooms i 0 ([] : List α) = [⟨true, true, []⟩] := by
  rw [split]
  simp

end Lsm.WalSplit
