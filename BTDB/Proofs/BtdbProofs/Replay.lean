/-!
# Transaction log replay keeps the transactions before damage

A model of `BTreeKeyValueDB.LoadTransactionLogCore`. The log is a list of commands: a transaction start, an
operation of the open transaction, a commit, or damage, which stands for every command replay cannot apply (an
unknown command, a length that is negative, does not fit an `int` or exceeds the rest of the file, a key suffix update
with an impossible prefix, a ulong index no array can hold, a truncated command). Replay applies a transaction when it reads its commit and stops
at damage, dropping the open transaction, as after a crash.

* `replay_drops_unfinished`: a log ending inside a transaction replays to the transactions committed before it.
* `replay_stops_at_damage`: a log of complete transactions, then any part of one more, then damage and anything at
  all, replays to exactly the complete transactions.
* `replay_after_abandoned`: complete transactions, an unfinished one that a crash cut after complete commands, and
  the complete transactions of the next session replay to all complete transactions. A start drops the open
  transaction, which only a crash leaves behind; `previous_loses_later_commit` shows the rule before the 2026-10-28
  fix, which ended replay there and lost every later commit.
* `replay_split`: a file boundary anywhere, also inside a transaction, replays like the unsplit log.
-/

namespace Btdb.Replay

inductive Cmd (α : Type) where
  | start
  | op (a : α)
  | commit
  | damage
  /-- The end of one log file; replay continues in the next one, also inside a transaction. -/
  | eof

/-- Replay from `committed` with the open transaction's operations `pending`. -/
def replay {α : Type} : List (Cmd α) → List (List α) → Option (List α) → List (List α)
  | [], committed, _ => committed
  | .start :: rest, committed, _ => replay rest committed (some [])
  | .op a :: rest, committed, some pending => replay rest committed (some (pending ++ [a]))
  | .op _ :: _, committed, none => committed
  | .commit :: rest, committed, some pending => replay rest (committed ++ [pending]) none
  | .commit :: _, committed, none => committed
  | .damage :: _, committed, _ => committed
  | .eof :: rest, committed, pending => replay rest committed pending

/-- A complete transaction as the log writes it. -/
def tx {α : Type} (ops : List α) : List (Cmd α) := .start :: ops.map .op ++ [.commit]

theorem replay_ops {α : Type} (ops : List α) (rest : List (Cmd α)) (committed : List (List α)) (pending : List α) :
    replay (ops.map .op ++ rest) committed (some pending) = replay rest committed (some (pending ++ ops)) := by
  induction ops generalizing pending with
  | nil => simp
  | cons a ops ih => simp [replay, ih]

theorem replay_txs {α : Type} (txs : List (List α)) (rest : List (Cmd α)) (committed : List (List α))
    (pending : Option (List α)) :
    replay ((txs.map tx).flatten ++ rest) committed pending =
      match txs with
      | [] => replay rest committed pending
      | _ => replay rest (committed ++ txs) none := by
  induction txs generalizing committed pending with
  | nil => simp
  | cons t txs ih =>
    simp only [List.map_cons, List.flatten_cons, tx, List.cons_append, List.append_assoc, replay]
    rw [replay_ops t _ committed []]
    simp only [List.nil_append, replay]
    rw [ih]
    cases txs <;> simp

/-- After complete transactions no transaction is open; what follows up to damage commits nothing. -/
theorem replay_cut {α : Type} (cut : Bool) (part : List α) (rest : List (Cmd α)) (committed : List (List α)) :
    replay ((if cut then [] else [Cmd.start]) ++ part.map Cmd.op ++ Cmd.damage :: rest) committed none = committed := by
  cases cut with
  | true =>
    cases part with
    | nil => rfl
    | cons a _ => rfl
  | false =>
    show replay (Cmd.start :: (part.map Cmd.op ++ Cmd.damage :: rest)) committed none = committed
    simp only [replay]
    rw [replay_ops]
    rfl

theorem replay_stops_at_damage {α : Type} (txs : List (List α)) (part : List α) (cut : Bool)
    (rest : List (Cmd α)) :
    replay ((txs.map tx).flatten ++ ((if cut then [] else [.start]) ++ part.map .op ++ .damage :: rest)) [] none
      = txs := by
  rw [replay_txs]
  cases txs with
  | nil => exact replay_cut cut part rest []
  | cons t ts => simp only [List.nil_append]; exact replay_cut cut part rest (t :: ts)

/-- A log that ends inside a transaction, as after a crash, replays to the transactions committed before it. -/
theorem replay_drops_unfinished {α : Type} (txs : List (List α)) (part : List α) :
    replay ((txs.map tx).flatten ++ (Cmd.start :: part.map .op)) [] none = txs := by
  have open_tail : ∀ committed : List (List α),
      replay (Cmd.start :: part.map Cmd.op) committed none = committed := fun committed => by
    simp only [replay]
    rw [show part.map Cmd.op = part.map Cmd.op ++ [] by simp, replay_ops]
    rfl
  rw [replay_txs]
  cases txs with
  | nil => exact open_tail []
  | cons t ts => simp only [List.nil_append]; exact open_tail (t :: ts)

/-- A crash cut the log inside a transaction after complete commands, and the next session appended complete
transactions: replay drops the unfinished transaction and keeps every later commit. -/
theorem replay_after_abandoned {α : Type} (txs later : List (List α)) (part : List α) :
    replay ((txs.map tx).flatten ++ (Cmd.start :: part.map Cmd.op ++ (later.map tx).flatten)) [] none =
      txs ++ later := by
  have tail : ∀ committed : List (List α),
      replay (Cmd.start :: part.map Cmd.op ++ (later.map tx).flatten) committed none = committed ++ later := by
    intro committed
    show replay (Cmd.start :: (part.map Cmd.op ++ (later.map tx).flatten)) committed none = committed ++ later
    simp only [replay]
    rw [replay_ops]
    have h := replay_txs later [] committed (some ([] ++ part))
    rw [List.append_nil] at h
    rw [h]
    cases later with
    | nil => simp [replay]
    | cons _ _ => simp [replay]
  rw [replay_txs]
  cases txs with
  | nil => simpa using tail []
  | cons t ts => exact tail (t :: ts)

/-- Splitting the log into files at any command boundary, inside a transaction too, as small log size limits do,
replays to the same transactions. -/
theorem replay_split {α : Type} (ys : List (Cmd α)) :
    ∀ (xs : List (Cmd α)) (committed : List (List α)) (pending : Option (List α)),
      replay (xs ++ Cmd.eof :: ys) committed pending = replay (xs ++ ys) committed pending
  | [], committed, pending => by simp [replay]
  | x :: xs, committed, pending => by
    cases x with
    | start => simp only [List.cons_append, replay]; exact replay_split ys xs committed (some [])
    | op a =>
      cases pending with
      | none => simp [replay]
      | some p => simp only [List.cons_append, replay]; exact replay_split ys xs committed (some (p ++ [a]))
    | commit =>
      cases pending with
      | none => simp [replay]
      | some p => simp only [List.cons_append, replay]; exact replay_split ys xs (committed ++ [p]) none
    | damage => simp [replay]
    | eof => simp only [List.cons_append, replay]; exact replay_split ys xs committed pending

/-- Replay before the 2026-10-28 fix: a start inside an unfinished transaction ended replay. -/
def replayPrevious {α : Type} : List (Cmd α) → List (List α) → Option (List α) → List (List α)
  | [], committed, _ => committed
  | .start :: _, committed, some _ => committed
  | .start :: rest, committed, none => replayPrevious rest committed (some [])
  | .op a :: rest, committed, some pending => replayPrevious rest committed (some (pending ++ [a]))
  | .op _ :: _, committed, none => committed
  | .commit :: rest, committed, some pending => replayPrevious rest (committed ++ [pending]) none
  | .commit :: _, committed, none => committed
  | .damage :: _, committed, _ => committed
  | .eof :: rest, committed, pending => replayPrevious rest committed pending

/-- Before the fix, the commit of the session after the crash was lost on the next open
(`TransactionLogDamageTest.CommitsAfterACrashInsideATransactionSurviveTheNextOpen`). -/
theorem previous_loses_later_commit :
    replayPrevious (tx [1] ++ [Cmd.start, Cmd.op 2] ++ tx [3]) [] none = [[1]] ∧
      replay (tx [1] ++ [Cmd.start, Cmd.op 2] ++ tx [3]) [] none = [[1], [3]] := by
  decide

end Btdb.Replay
