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

end Btdb.KviDurability
