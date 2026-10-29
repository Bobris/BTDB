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
* `replay_record`: replaying a record's parts tagged with its transaction, as the part loop of
  `LsmKeyValueDB.Recovery` does, gives back exactly the record from any state left by earlier parts, and then goes
  on as if nothing were being rebuilt. Epochs, generations and damaged chunks, which the loop also handles, are left
  out.
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

/-- Replay of tagged parts as the WAL recovery loop does it: a part flagged first starts a record of its transaction, a
later part is appended when it belongs to the record being rebuilt and skipped otherwise, and a part flagged last
ends the record. The state is the parts gathered so far and the transaction they belong to. -/
def replay {α : Type} : List (List α) × Int → List (Int × Part α) → List (List α)
  | _, [] => []
  | (acc, cur), (t, ⟨first, last, data⟩) :: ps =>
    if first then
      if last then data :: replay ([], -1) ps else replay ([data], t) ps
    else if cur = t then
      if last then (acc ++ [data]).flatten :: replay ([], -1) ps else replay (acc ++ [data], cur) ps
    else replay (acc, cur) ps

theorem replay_rest {α : Type} (rooms : Nat → Nat) (p : List α) (t : Int) (rest : List (Int × Part α))
    (i offset : Nat) : ∀ acc : List (List α), 0 < offset → offset ≤ p.length →
      replay (acc, t) ((split rooms i offset p).map (t, ·) ++ rest) =
        (acc.flatten ++ p.drop offset) :: replay ([], -1) rest := by
  induction i, offset using split.induct rooms p with
  | case1 i offset len hlt ih =>
    simp only [len] at hlt ih ⊢
    intro acc hpos hle
    rw [split]
    simp only [hlt, ↓reduceIte, List.map_cons, List.cons_append, replay]
    have hfirst : (offset == 0) = false := by simp only [beq_eq_false_iff_ne]; omega
    have hlast : (offset + min (max 1 (rooms i)) (p.length - offset) == p.length) = false := by
      simp only [beq_eq_false_iff_ne]; omega
    simp only [hfirst, hlast, Bool.false_eq_true, ↓reduceIte]
    rw [ih (acc ++ [_]) (by omega) (by omega)]
    have : List.drop (offset + min (max 1 (rooms i)) (p.length - offset)) p =
        List.drop (min (max 1 (rooms i)) (p.length - offset)) (List.drop offset p) := by
      rw [List.drop_drop]
    simp only [List.flatten_append, List.flatten_cons, List.flatten_nil, List.append_nil, List.append_assoc, this,
      List.take_append_drop]
  | case2 i offset len hlt =>
    simp only [len] at hlt ⊢
    intro acc hpos hle
    rw [split]
    simp only [hlt, ↓reduceIte, List.map_cons, List.map_nil, List.cons_append, List.nil_append, replay]
    have hfirst : (offset == 0) = false := by simp only [beq_eq_false_iff_ne]; omega
    have hlast : (offset + min (max 1 (rooms i)) (p.length - offset) == p.length) = true := by
      simp only [beq_iff_eq]; omega
    simp only [hfirst, hlast, Bool.false_eq_true, ↓reduceIte, List.flatten_append, List.flatten_cons,
      List.flatten_nil, List.append_nil]
    rw [List.take_of_length_le (by simp only [List.length_drop]; omega)]

/-- Replaying the parts of a record, from any state, gives the record and leaves no record being rebuilt. -/
theorem replay_record {α : Type} (rooms : Nat → Nat) (p : List α) (t : Int) (rest : List (Int × Part α)) (i : Nat)
    (state : List (List α) × Int) :
    replay state ((split rooms i 0 p).map (t, ·) ++ rest) = p :: replay ([], -1) rest := by
  obtain ⟨acc, cur⟩ := state
  rw [split]
  split
  · next hlt =>
    simp only [List.map_cons, List.cons_append, replay, beq_self_eq_true, ↓reduceIte, Nat.zero_add]
    have hlast : (min (max 1 (rooms i)) (p.length - 0) == p.length) = false := by
      simp only [beq_eq_false_iff_ne]; omega
    simp only [hlast, Bool.false_eq_true, ↓reduceIte]
    rw [replay_rest rooms p t rest (i + 1) _ _ (by omega) (by omega)]
    simp only [List.drop_zero, List.flatten_cons, List.flatten_nil, List.append_nil, List.take_append_drop]
  · next hlt =>
    simp only [List.map_cons, List.map_nil, List.cons_append, List.nil_append, replay, beq_self_eq_true,
      ↓reduceIte, Nat.zero_add]
    have hlast : (min (max 1 (rooms i)) (p.length - 0) == p.length) = true := by
      simp only [beq_iff_eq]; omega
    simp only [hlast, ↓reduceIte, List.drop_zero]
    rw [List.take_of_length_le (by omega)]

end Lsm.WalSplit
