/-!
# A key index never outlives the log commands it points at

A model of the write order of `BTreeKeyValueDB.CreateKeyIndexFile`. The transaction log has `written` bytes, of which
`durable` survive a crash; the last commit ends at `committed`; a durable key index records the log position of its
root, from which replay continues and before which its values are read from the log.

* `fixed_safe`: with the order since the 2026-10-28 fix, which makes the log durable before the index and takes the
  root under the same exclusion, or beside a writer indexes only a commit already durable, every reachable state has
  its durable index at or before the durable log end.
* `previous_unsafe`: before the fix, one commit and an index was enough for a crash to keep the index and lose the log
  bytes it points at (`KeyValueDBCrashSimulationTest`).
-/

namespace Btdb.KviDurability

structure St where
  written : Nat
  durable : Nat
  committed : Nat
  index : Option Nat

inductive Step : St → St → Prop
  | append (s : St) (n : Nat) : Step s { s with written := s.written + n }
  | commit (s : St) : Step s { s with committed := s.written }
  | flush (s : St) : Step s { s with durable := s.written }
  /-- Since the fix: the log is flushed, then the index of the last commit is written and flushed. -/
  | index (s : St) : Step s { s with durable := s.written, index := some s.committed }
  /-- Since the fix, beside a writer that holds the log: the index only of a last commit that is already durable;
  otherwise `CreateKvi` writes none. -/
  | indexBesideWriter (s : St) (h : s.committed ≤ s.durable) : Step s { s with index := some s.committed }

/-- Before the fix: the index of the last commit, without flushing the log. -/
def previousIndex (s : St) : St := { s with index := some s.committed }

def Inv (s : St) : Prop := s.durable ≤ s.written ∧ s.committed ≤ s.written ∧ ∀ p, s.index = some p → p ≤ s.durable

inductive Reach : St → Prop
  | init : Reach ⟨0, 0, 0, none⟩
  | step {s t : St} : Reach s → Step s t → Reach t

theorem fixed_inv {s : St} (h : Reach s) : Inv s := by
  induction h with
  | init => exact ⟨Nat.le_refl _, Nat.le_refl _, fun p hp => by cases hp⟩
  | step _ hs ih =>
    obtain ⟨h1, h2, h3⟩ := ih
    cases hs with
    | append n => exact ⟨by simp only; omega, by simp only; omega, fun p hp => h3 p hp⟩
    | commit => exact ⟨h1, Nat.le_refl _, fun p hp => h3 p hp⟩
    | flush => exact ⟨Nat.le_refl _, h2, fun p hp => by have := h3 p hp; simp only; omega⟩
    | index =>
      refine ⟨Nat.le_refl _, h2, fun p hp => ?_⟩
      simp only [Option.some.injEq] at hp
      subst hp; exact h2
    | indexBesideWriter h =>
      refine ⟨h1, h2, fun p hp => ?_⟩
      simp only [Option.some.injEq] at hp
      subst hp; exact h

theorem fixed_safe {s : St} (h : Reach s) (p : Nat) (hp : s.index = some p) : p ≤ s.durable := (fixed_inv h).2.2 p hp

theorem previous_unsafe :
    let s := previousIndex { written := 5, durable := 0, committed := 5, index := none }
    Reach { written := 5, durable := 0, committed := 5, index := none } ∧ s.index = some 5 ∧ s.durable < 5 :=
  ⟨.step (.step .init (.append ⟨0, 0, 0, none⟩ 5)) (.commit _), rfl, by decide⟩

/-! ## The durability check beside a writer

`IsDurable` decides whether a root is durable from the current log file ID and the durable position the last hard
flush of that file recorded, read without the write lock, so it may see a file ID that was current a moment ago.
Logs before the current one are durable as a whole, since a log is hard flushed when the next one starts; the
current log is durable up to the recorded offset. -/

structure Log where
  /-- The current transaction log file ID; earlier ones are durable as a whole. -/
  current : Nat
  /-- The file and offset the last hard flush of the current log recorded. -/
  flushedFile : Nat
  flushedOffset : Nat

/-- Durable bytes of the logs: a position in an earlier log, or in the log the last hard flush recorded, up to the
offset it recorded. -/
def Durable (log : Log) (file offset : Nat) : Prop :=
  file < log.current ∨ (log.flushedFile = file ∧ offset ≤ log.flushedOffset)

/-- `IsDurable` with the current file ID it read, `seen`. -/
def isDurable (seen : Nat) (log : Log) (file offset : Nat) : Bool :=
  file != seen || (log.flushedFile == file && decide (offset ≤ log.flushedOffset))

/-- The check accepts only durable roots, also when it read a file ID that a log switch has since made stale. The
root's log existed before the check read the ID, which only grows, so `file ≤ seen ≤ current`. -/
theorem is_durable_sound (log : Log) (seen file offset : Nat) (hfile : file ≤ seen) (hseen : seen ≤ log.current)
    (h : isDurable seen log file offset = true) : Durable log file offset := by
  unfold isDurable at h
  unfold Durable
  by_cases hne : file = seen
  · subst hne
    simp only [bne_self_eq_false, Bool.false_or, Bool.and_eq_true, beq_iff_eq, decide_eq_true_eq] at h
    exact Or.inr h
  · exact Or.inl (by omega)

end Btdb.KviDurability
