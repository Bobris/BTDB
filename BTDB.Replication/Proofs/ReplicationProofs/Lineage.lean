/-!
# Walking a TRL lineage back

A model of `TrlLineage.SuccessorsAsync`. Native TRL headers name their predecessor (`PreviousFileId`); the walk starts
at the advertised end and follows predecessors until it reaches the current file, failing on a predecessor at or
below the current file or not below the file it came from. `none` stands for the thrown `InvalidDataException`.

* `walk_links`: a successful walk, before it is reversed, is a chain of predecessor links in descending IDs from the
  end down to the file after the current one, whose predecessor is the current file.
* `successors_correct`: so `SuccessorsAsync` returns the files after the current one through the end, in ascending
  order, each following its predecessor.
-/

namespace Replication.Lineage

/-- The files of `d`, in descending order, each the predecessor of the one before it, the last one following `c`. -/
def Links (prev : Nat → Nat) (c : Nat) : List Nat → Prop
  | [] => False
  | [d] => prev d = c ∧ c < d
  | d :: e :: rest => prev d = e ∧ e < d ∧ Links prev c (e :: rest)

/-- The walk back from `id`, collecting `id` and its predecessors above `c`. -/
def walk (prev : Nat → Nat) (c : Nat) (id : Nat) : Option (List Nat) :=
  if prev id = c then some [id]
  else if _h : c < prev id ∧ prev id < id then (walk prev c (prev id)).map (id :: ·)
  else none
termination_by id - c
decreasing_by omega

def successors (prev : Nat → Nat) (c e : Nat) : Option (List Nat) :=
  if e ≤ c then none else (walk prev c e).map List.reverse

theorem walk_links (prev : Nat → Nat) (c : Nat) :
    ∀ id d, c < id → walk prev c id = some d → d.head? = some id ∧ Links prev c d := by
  intro id
  induction id using Nat.strongRecOn with
  | _ id ih =>
    intro d hc h
    rw [walk] at h
    by_cases h1 : prev id = c
    · rw [ite_eq_left h1] at h
      cases h
      exact ⟨rfl, h1, hc⟩
    · rw [ite_eq_right h1] at h
      by_cases h2 : c < prev id ∧ prev id < id
      · rw [dite_eq_left h2] at h
        cases hw : walk prev c (prev id) with
        | none => rw [hw] at h; cases h
        | some d' =>
          rw [hw] at h
          cases h
          obtain ⟨hh, hl⟩ := ih (prev id) h2.2 d' h2.1 hw
          refine ⟨rfl, ?_⟩
          cases d' with
          | nil => exact hl.elim
          | cons e rest =>
            simp only [List.head?_cons, Option.some.injEq] at hh
            subst hh
            exact ⟨rfl, h2.2, hl⟩
      · rw [dite_eq_right h2] at h
        cases h

theorem successors_correct (prev : Nat → Nat) (c e : Nat) (L : List Nat) (h : successors prev c e = some L) :
    ∃ d, L = d.reverse ∧ d.head? = some e ∧ Links prev c d := by
  unfold successors at h
  by_cases he : e ≤ c
  · rw [ite_eq_left he] at h; cases h
  · rw [ite_eq_right he] at h
    cases hw : walk prev c e with
    | none => rw [hw] at h; cases h
    | some d =>
      rw [hw] at h
      cases h
      exact ⟨d, rfl, walk_links prev c e d (by omega) hw⟩

end Replication.Lineage
