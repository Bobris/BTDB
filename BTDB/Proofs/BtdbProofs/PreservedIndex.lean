/-!
# Writing a key index keeps the one preserved history starts from

A model of how `BTreeKeyValueDB.CreateIndexFile` drops key indexes when it writes a new one. Key indexes are the commit
ulongs of their roots. With `PreserveHistoryUpToCommitUlong = p`, history from `p` opens from the newest key index at
or before `p`, which the new one must not drop.

* `fixed_keeps_preserved`: since the 2026-10-28 fix, `CreateKvi` keeps that index as compaction does, so a key index
  at or before `p` survives whenever one existed.
* `previous_drops_preserved`: before the fix `CreateKvi` kept only the new index, so opening at `p` then opened at a
  later commit (`KeyValueDBHistoryOpenTest`).
-/

namespace Btdb.PreservedIndex

/-- The newest key index at or before `p`. -/
def newestAtMost (p : Nat) : List Nat → Option Nat
  | [] => none
  | x :: xs =>
    match newestAtMost p xs with
    | none => if x ≤ p then some x else none
    | some m => if x ≤ p ∧ m < x then some x else some m

theorem newest_spec (p : Nat) : ∀ (ks : List Nat) (m : Nat), newestAtMost p ks = some m → m ∈ ks ∧ m ≤ p
  | [], _, h => by simp [newestAtMost] at h
  | x :: xs, m, h => by
    unfold newestAtMost at h
    cases hr : newestAtMost p xs with
    | none =>
      rw [hr] at h
      by_cases hx : x ≤ p
      · simp only [hx, ite_true, Option.some.injEq] at h; subst h; simp [hx]
      · simp [hx] at h
    | some n =>
      rw [hr] at h
      have := newest_spec p xs n hr
      by_cases hx : x ≤ p ∧ n < x
      · simp only [hx, and_self, ite_true, Option.some.injEq] at h; subst h; simp [hx.1]
      · simp only [hx, ite_false, Option.some.injEq] at h; subst h; simp [this.1, this.2]

theorem newest_exists (p : Nat) : ∀ (ks : List Nat), (∃ x ∈ ks, x ≤ p) → ∃ m, newestAtMost p ks = some m
  | [], ⟨_, hx, _⟩ => by simp at hx
  | x :: xs, ⟨y, hy, hyp⟩ => by
    unfold newestAtMost
    cases hr : newestAtMost p xs with
    | none =>
      simp only
      by_cases hx : x ≤ p
      · exact ⟨x, by simp [hx]⟩
      · simp only [List.mem_cons] at hy
        cases hy with
        | inl e => subst e; exact absurd hyp hx
        | inr m =>
          obtain ⟨n, hn⟩ := newest_exists p xs ⟨y, m, hyp⟩
          rw [hr] at hn; cases hn
    | some n =>
      simp only
      by_cases hx : x ≤ p ∧ n < x
      · exact ⟨x, by simp [hx]⟩
      · exact ⟨n, by simp [hx]⟩

/-- Since the fix: the new index and the preserved one. -/
def createFixed (p c : Nat) (ks : List Nat) : List Nat := c :: ks.filter (fun x => newestAtMost p ks == some x)

/-- Before the fix: only the new index. -/
def createPrevious (c : Nat) (_ks : List Nat) : List Nat := [c]

theorem fixed_keeps_preserved (p c : Nat) (ks : List Nat) (h : ∃ x ∈ ks, x ≤ p) :
    ∃ y ∈ createFixed p c ks, y ≤ p := by
  obtain ⟨m, hm⟩ := newest_exists p ks h
  have ⟨mem, le⟩ := newest_spec p ks m hm
  exact ⟨m, by simp [createFixed, List.mem_filter, mem, hm], le⟩

theorem previous_drops_preserved : (∃ x ∈ [30], x ≤ 36) ∧ ¬ ∃ y ∈ createPrevious 39 [30], y ≤ 36 := by
  decide

end Btdb.PreservedIndex
