/-!
# A create that finds no later split did not reuse a deleted name

Split names are numbers. Merging deletes a split only while a later split exists, which is why the latest split is
never deleted (`Merge.lean`). A create-if-absent of a deleted name succeeds again, as storage cannot tell a deleted
name from one never used, and before the 2026-10-28 fix an owner whose create landed that late, after others had
filled the name, merged and deleted it, acknowledged a batch no reader sees.

* `deleted_has_later`: every deleted name has a later split present, in every reachable state.
* `fixed_not_reused`: so the check `EventLogOwnerLane` now makes after a create, that no later split exists, proves the
  created name was never deleted.
* `previous_reuses`: without the check, a create lands on a deleted name.
-/

namespace EventLog.LateCreate

structure St where
  present : Nat → Bool
  deleted : Nat → Bool

inductive Step : St → St → Prop
  | create (s : St) (n : Nat) (h : s.present n = false) :
      Step s { s with present := fun m => if m = n then true else s.present m }
  | delete (s : St) (k j : Nat) (hk : s.present k = true) (hj : k < j ∧ s.present j = true) :
      Step s { present := fun m => if m = k then false else s.present m,
               deleted := fun m => if m = k then true else s.deleted m }

inductive Reach : St → Prop
  | init : Reach ⟨fun _ => false, fun _ => false⟩
  | step {s t : St} : Reach s → Step s t → Reach t

def Inv (s : St) : Prop := ∀ n, s.deleted n = true → ∃ j, n < j ∧ s.present j = true

theorem deleted_has_later {s : St} (h : Reach s) : Inv s := by
  induction h with
  | init => intro n hn; simp at hn
  | step _ hs ih =>
    cases hs with
    | create n hn =>
      intro m hm
      obtain ⟨j, hj, hp⟩ := ih m hm
      refine ⟨j, hj, ?_⟩
      simp only; split <;> simp_all
    | delete k j hk hj =>
      intro m hm
      simp only at hm
      by_cases hmk : m = k
      · subst hmk
        refine ⟨j, hj.1, ?_⟩
        simp only; rw [if_neg (by omega)]; exact hj.2
      · rw [if_neg hmk] at hm
        obtain ⟨i, hi, hp⟩ := ih m hm
        by_cases hik : i = k
        · subst hik
          refine ⟨j, by omega, ?_⟩
          simp only; rw [if_neg (by omega)]; exact hj.2
        · exact ⟨i, hi, by simp only; rw [if_neg hik]; exact hp⟩

/-- After a create of `n` lands, no later split exists: then `n` was never deleted, so its batch is history. -/
theorem fixed_not_reused {s : St} (h : Reach s) (n : Nat) (hcheck : ∀ j, n < j → s.present j = false) :
    s.deleted n = false := by
  cases hd : s.deleted n with
  | false => rfl
  | true =>
    obtain ⟨j, hj, hp⟩ := deleted_has_later h n hd
    have := hcheck j hj
    simp [hp] at this

/-- Split 1 and 2 exist, merging deletes split 1, and a late create of split 1 lands on the free name. -/
theorem previous_reuses : ∃ s, Reach s ∧ s.deleted 1 = true ∧ s.present 1 = true := by
  let s0 : St := ⟨fun _ => false, fun _ => false⟩
  let s1 : St := { s0 with present := fun m => if m = 1 then true else s0.present m }
  let s2 : St := { s1 with present := fun m => if m = 2 then true else s1.present m }
  let s3 : St := { present := fun m => if m = 1 then false else s2.present m,
                   deleted := fun m => if m = 1 then true else s2.deleted m }
  let s4 : St := { s3 with present := fun m => if m = 1 then true else s3.present m }
  have r1 : Reach s1 := .step .init (.create s0 1 rfl)
  have r2 : Reach s2 := .step r1 (.create s1 2 rfl)
  have r3 : Reach s3 := .step r2 (.delete s2 1 2 rfl ⟨by decide, rfl⟩)
  exact ⟨s4, .step r3 (.create s3 1 rfl), rfl, rfl⟩

end EventLog.LateCreate
