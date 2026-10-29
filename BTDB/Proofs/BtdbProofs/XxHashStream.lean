/-!
# Streaming `XxHash64` equals hashing the whole input

A model of the buffering in `XxHash64.Update` (`../Buffer/XxHash64.cs`). The hash consumes its input in blocks of 32
bytes (`Process32Bytes`); `step` stands for that and is any function here. `go` consumes every full block of an input
and keeps the rest, as the one-shot `Hash` does. `Update` keeps a partial block between calls: with one pending it
fills it from the new input, consumes it once full and continues with the rest from an empty buffer; with none it
consumes the full blocks of the input and keeps its rest.

* `update_eq_go`: one `Update` equals `go` over the pending bytes followed by the input.
* `stream_eq_whole`: updating with any chunks one after another gives the state and pending bytes of `go` over all of
  them, so the digest does not depend on how the input is split.
-/

namespace Btdb.XxHashStream

variable {σ α : Type} (B : Nat) (step : σ → List α → σ)

def go (s : σ) (l : List α) : σ × List α :=
  if _h : 0 < B ∧ B ≤ l.length then go (step s (l.take B)) (l.drop B) else (s, l)
termination_by l.length
decreasing_by simp only [List.length_drop]; omega

/-- `XxHash64.Update`: fills a pending partial block first, then consumes whole blocks of the rest. -/
def update (s : σ) (pending input : List α) : σ × List α :=
  if pending.isEmpty then go B step s input
  else if pending.length + input.length < B then (s, pending ++ input)
  else go B step (step s (pending ++ input.take (B - pending.length))) (input.drop (B - pending.length))

theorem go_short (s : σ) (l : List α) (h : l.length < B) : go B step s l = (s, l) := by
  rw [go]; rw [dite_eq_right (by omega)]

theorem go_rest (s : σ) (l : List α) (hB : 0 < B) : (go B step s l).2.length < B := by
  induction h' : l.length using Nat.strongRecOn generalizing s l with
  | ind n ih =>
    rw [go]
    split
    · next h => exact ih (l.length - B) (by omega) _ _ (by simp only [List.length_drop])
    · next h => simp only; omega

theorem update_eq_go (s : σ) (pending input : List α) (hB : 0 < B) (hp : pending.length < B) :
    update B step s pending input = go B step s (pending ++ input) := by
  unfold update
  split
  · next h => simp [List.isEmpty_iff.1 h]
  · next h =>
    split
    · next h2 => rw [go_short B step s _ (by simp only [List.length_append]; omega)]
    · next h2 =>
      conv => rhs; rw [go]
      rw [dite_eq_left ⟨hB, by simp only [List.length_append]; omega⟩]
      have hle : pending.length ≤ B := by omega
      rw [List.take_append, List.drop_append, List.take_of_length_le hle, List.drop_eq_nil_of_le hle,
        List.nil_append]

theorem go_step (s : σ) (l : List α) (hB : 0 < B) (hl : B ≤ l.length) :
    go B step s l = go B step (step s (l.take B)) (l.drop B) := by
  rw [go, dite_eq_left ⟨hB, hl⟩]

theorem go_append (s : σ) (a b : List α) (hB : 0 < B) :
    go B step s (a ++ b) = go B step (go B step s a).1 ((go B step s a).2 ++ b) := by
  induction h' : a.length using Nat.strongRecOn generalizing s a with
  | ind n ih =>
    by_cases ha : B ≤ a.length
    · rw [go_step B step s (a ++ b) hB (by simp only [List.length_append]; omega), go_step B step s a hB ha,
        List.take_append_of_le_length ha, List.drop_append_of_le_length ha]
      exact ih (a.length - B) (by omega) (step s (a.take B)) (a.drop B) (by simp only [List.length_drop])
    · rw [go_short B step s a (by omega)]

/-- Updating with chunks one after another. -/
def stream (s : σ) (pending : List α) : List (List α) → σ × List α
  | [] => (s, pending)
  | c :: cs => let r := update B step s pending c; stream r.1 r.2 cs

theorem stream_eq_whole (s : σ) (chunks : List (List α)) (hB : 0 < B) :
    stream B step s [] chunks = go B step s chunks.flatten := by
  suffices h : ∀ (cs : List (List α)) (s : σ) (pending : List α), pending.length < B →
      stream B step s pending cs = go B step s (pending ++ cs.flatten) by
    simpa using h chunks s [] hB
  intro cs
  induction cs with
  | nil => intro s pending hp; simp only [stream, List.flatten_nil, List.append_nil]; rw [go_short B step s _ hp]
  | cons c cs ih =>
    intro s pending hp
    simp only [stream, List.flatten_cons]
    rw [update_eq_go B step s pending c hB hp]
    rw [ih _ _ (go_rest B step _ _ hB)]
    rw [← List.append_assoc, go_append B step s (pending ++ c) cs.flatten hB]

end Btdb.XxHashStream
