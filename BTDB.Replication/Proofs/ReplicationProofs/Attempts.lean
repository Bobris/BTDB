/-!
# The attempt journal skips an event that never completes, and only after its attempts

A model of `EventAttemptJournal.BeforeAttemptAsync` in `../../BTDB.Replication.EventSourcing/EventPoison.cs`. The
journal holds this node's last started attempt: a data offset and how many times it started. Before consuming the event
at `o`, the publisher calls `before`: an offset below the recorded one completed before the recorded crash and keeps
the record; the recorded offset counts one more start, or is skipped once it started `limit` times; a later offset
starts a new record. A run restarts from some offset `R ≤ o` and calls `before` for each offset it consumes.

* `crash_skipped`: an event that crashes every run reaching it is skipped by the `limit + 1`-th such run at the
  latest, whatever ascending offsets below it the runs consume first. The journal must start at or below the event,
  with attempts left if the first run consumes the recorded offset again. No earlier offset is skipped on the way.
* `skip_needs_attempts`: a skip at `p` happens only after at least `limit` earlier calls at `p`.
-/

namespace Replication.Attempts

structure Attempt where
  offset : Nat
  count : Nat

/-- `BeforeAttemptAsync`: whether the event exhausted its attempts, and the journal afterwards. -/
def before (limit : Nat) (j : Option Attempt) (o : Nat) : Bool × Option Attempt :=
  match j with
  | none => (false, some ⟨o, 1⟩)
  | some a =>
    if o < a.offset then (false, j)
    else if o = a.offset then (if limit ≤ a.count then (true, j) else (false, some ⟨o, a.count + 1⟩))
    else (false, some ⟨o, 1⟩)

theorem before_none (limit o : Nat) : before limit none o = (false, some ⟨o, 1⟩) := rfl

theorem before_completed (limit o : Nat) (a : Attempt) (h : o < a.offset) :
    before limit (some a) o = (false, some a) := by
  simp [before, h]

theorem before_again (limit o c : Nat) (h : c < limit) :
    before limit (some ⟨o, c⟩) o = (false, some ⟨o, c + 1⟩) := by
  simp [before, Nat.not_le.mpr h]

theorem before_exhausted (limit o c : Nat) (h : limit ≤ c) : before limit (some ⟨o, c⟩) o = (true, some ⟨o, c⟩) := by
  simp [before, h]

theorem before_new (limit o : Nat) (a : Attempt) (h : a.offset < o) : before limit (some a) o = (false, some ⟨o, 1⟩) := by
  simp [before, Nat.not_lt.mpr (Nat.le_of_lt h), Nat.ne_of_gt h]

/-- A run consuming the offsets `ps`, which complete, and then `o`: whether it skips one of `ps`, whether it skips
`o`, and the journal it leaves. -/
def run (limit : Nat) (j : Option Attempt) (o : Nat) : List Nat → Bool × Bool × Option Attempt
  | [] => let r := before limit j o; (false, r.1, r.2)
  | p :: rest =>
    let r := before limit j p
    let rest' := run limit r.2 o rest
    (r.1 || rest'.1, rest'.2.1, rest'.2.2)

/-- With the journal at `o`, offsets below it are neither skipped nor recorded; `o` counts one more start. -/
theorem run_at (limit o c : Nat) : ∀ (ps : List Nat), (∀ p ∈ ps, p < o) →
    run limit (some ⟨o, c⟩) o ps =
      (false, decide (limit ≤ c), if limit ≤ c then some ⟨o, c⟩ else some ⟨o, c + 1⟩)
  | [], _ => by
    by_cases h : limit ≤ c
    · simp [run, before_exhausted limit o c h, h]
    · simp [run, before_again limit o c (by omega), h]
  | p :: rest, hps => by
    have hp := hps p (by simp)
    have ih := run_at limit o c rest (fun q hq => hps q (by simp [hq]))
    simp only [run, before_completed limit p ⟨o, c⟩ hp, Bool.false_or]
    rw [ih]

def AtOrBelow (o : Nat) : Option Attempt → Prop
  | none => True
  | some a => a.offset ≤ o

/-- A recorded offset that a run will consume again has attempts left. -/
def Good (limit o : Nat) (j : Option Attempt) (ps : List Nat) : Prop :=
  ∀ a, j = some a → a.offset < o → a.offset ∈ ps → a.count < limit

/-- The journal after one completing offset `p` of a run whose later offsets are above `p`. -/
theorem step_good (limit o p : Nat) (rest : List Nat) (j : Option Attempt) (hp : p < o) (hrest : ∀ q ∈ rest, p < q)
    (hj : AtOrBelow o j) (hgood : Good limit o j (p :: rest)) :
    (before limit j p).1 = false ∧ AtOrBelow o (before limit j p).2 ∧ Good limit o (before limit j p).2 rest := by
  have fresh : Good limit o (some ⟨p, 1⟩) rest := by
    intro a ha _ hm; cases ha; have := hrest _ hm; simp at this
  cases j with
  | none => rw [before_none]; exact ⟨rfl, by simp only [AtOrBelow]; omega, fresh⟩
  | some b =>
    simp only [AtOrBelow] at hj
    by_cases h1 : p < b.offset
    · rw [before_completed limit p b h1]
      exact ⟨rfl, hj, fun a ha hlt hm => hgood a ha hlt (by simp [hm])⟩
    · by_cases h2 : p = b.offset
      · obtain ⟨bo, bc⟩ := b
        simp only at h2; subst h2
        have hb := hgood ⟨p, bc⟩ rfl hp (by simp)
        rw [before_again limit p bc hb]
        refine ⟨rfl, by simp only [AtOrBelow]; omega, ?_⟩
        intro a ha _ hm; cases ha; have := hrest _ hm; simp at this
      · rw [before_new limit p b (by omega)]
        exact ⟨rfl, by simp only [AtOrBelow]; omega, fresh⟩

/-- A run over ascending offsets below `o`, from a journal at or below `o` whose recorded offset has attempts left if
the run consumes it again, skips none of them and leaves the journal at `o` after a start, unless it skips `o`. -/
theorem run_first (limit o : Nat) : ∀ (ps : List Nat) (j : Option Attempt), ps.Pairwise (· < ·) →
    (∀ p ∈ ps, p < o) → AtOrBelow o j → Good limit o j ps →
    (run limit j o ps).1 = false ∧
      ((run limit j o ps).2.1 = true ∨ ∃ c, 1 ≤ c ∧ (run limit j o ps).2.2 = some ⟨o, c⟩)
  | [], none, _, _, _, _ => by simp [run, before_none]
  | [], some a, _, _, hj, _ => by
    simp only [AtOrBelow] at hj
    by_cases h2 : a.offset = o
    · obtain ⟨ao, ac⟩ := a
      simp only at h2; subst h2
      by_cases h3 : limit ≤ ac
      · simp [run, before_exhausted limit ao ac h3]
      · simp [run, before_again limit ao ac (by omega)]
    · simp [run, before_new limit o a (by omega)]
  | p :: rest, j, hsorted, hps, hj, hgood => by
    have hp := hps p (by simp)
    have ⟨hs, aj, gj⟩ := step_good limit o p rest j hp (List.pairwise_cons.mp hsorted).1 hj hgood
    have ih := run_first limit o rest _ (List.pairwise_cons.mp hsorted).2 (fun q hq => hps q (by simp [hq])) aj gj
    simp only [run, hs, Bool.false_or]
    exact ih

/-- Runs that each consume offsets below `o` and crash at `o` unless they skip it: the index of the skipping run. -/
def crashes (limit o : Nat) : Option Attempt → List (List Nat) → Option Nat
  | _, [] => none
  | j, ps :: runs => if (run limit j o ps).2.1 then some 0 else (crashes limit o (run limit j o ps).2.2 runs).map (· + 1)

/-- With the journal at `o` after `c` starts, one of the next `limit - c + 1` runs skips `o`. -/
theorem crashes_at (limit o : Nat) : ∀ (runs : List (List Nat)) (c : Nat), (∀ ps ∈ runs, ∀ p ∈ ps, p < o) →
    limit - c < runs.length → ∃ k, k ≤ limit - c ∧ crashes limit o (some ⟨o, c⟩) runs = some k
  | [], _, _, h => by simp at h
  | ps :: runs, c, hruns, hlen => by
    have hr := run_at limit o c ps (hruns ps (by simp))
    by_cases h : limit ≤ c
    · exact ⟨0, by omega, by simp [crashes, hr, h]⟩
    · simp only [List.length_cons] at hlen
      obtain ⟨k, hk, he⟩ := crashes_at limit o runs (c + 1) (fun qs hq => hruns qs (by simp [hq])) (by omega)
      exact ⟨k + 1, by omega, by simp [crashes, hr, h, he]⟩

/-- An event that crashes every run reaching it is skipped by the `limit + 1`-th such run at the latest, whatever
ascending offsets below it the runs consume first, when the journal starts at or below it with attempts left for an
offset the first run consumes again. The first run skips no earlier offset (`run_first`), and later ones skip nothing
before `o` (`run_at`). -/
theorem crash_skipped (limit o : Nat) (j : Option Attempt) (ps : List Nat) (rest : List (List Nat))
    (hsorted : ps.Pairwise (· < ·)) (hps : ∀ p ∈ ps, p < o) (hrest : ∀ qs ∈ rest, ∀ q ∈ qs, q < o)
    (hj : AtOrBelow o j) (hgood : Good limit o j ps) (hl : 0 < limit) (hlen : limit ≤ rest.length) :
    ∃ k, k ≤ limit ∧ crashes limit o j (ps :: rest) = some k := by
  have ⟨_, hfirst⟩ := run_first limit o ps j hsorted hps hj hgood
  by_cases hskip : (run limit j o ps).2.1 = true
  · exact ⟨0, by omega, by simp [crashes, hskip]⟩
  · have ⟨c, hc1, hcj⟩ := hfirst.resolve_left hskip
    obtain ⟨k, hk, he⟩ := crashes_at limit o rest c hrest (by omega)
    exact ⟨k + 1, by omega, by simp [crashes, hskip, hcj, he]⟩

/-- Every call, in order. -/
def calls (limit : Nat) : Option Attempt → List Nat → Option Attempt
  | j, [] => j
  | j, o :: rest => calls limit (before limit j o).2 rest

/-- The recorded count is at most the calls made at the recorded offset. -/
def Counted (j : Option Attempt) (done : List Nat) : Prop :=
  ∀ a, j = some a → a.count ≤ (done.filter (· == a.offset)).length

theorem filter_snoc (done : List Nat) (o x : Nat) :
    ((done ++ [o]).filter (· == x)).length = (done.filter (· == x)).length + if o = x then 1 else 0 := by
  by_cases h : o = x <;> simp [List.filter_append, h]

theorem step_counted (limit o : Nat) (j : Option Attempt) (done : List Nat) (h : Counted j done) :
    Counted (before limit j o).2 (done ++ [o]) := by
  have grow : ∀ x, (done.filter (· == x)).length ≤ ((done ++ [o]).filter (· == x)).length := by
    intro x; rw [filter_snoc]; omega
  have fresh : Counted (some ⟨o, 1⟩) (done ++ [o]) := by
    intro a ha; cases ha; rw [filter_snoc]; simp
  cases j with
  | none => rw [before_none]; exact fresh
  | some b =>
    by_cases h1 : o < b.offset
    · rw [before_completed limit o b h1]
      intro a ha; cases ha; exact Nat.le_trans (h b rfl) (grow _)
    · by_cases h2 : o = b.offset
      · obtain ⟨bo, bc⟩ := b
        simp only at h2; subst h2
        by_cases h3 : limit ≤ bc
        · rw [before_exhausted limit o bc h3]
          intro a ha; cases ha; exact Nat.le_trans (h _ rfl) (grow _)
        · rw [before_again limit o bc (by omega)]
          intro a ha; cases ha
          have := h _ rfl
          rw [filter_snoc]; simp only [ite_true] at this ⊢; omega
      · rw [before_new limit o b (by omega)]; exact fresh

theorem calls_counted (limit : Nat) : ∀ (ts done : List Nat) (j : Option Attempt), Counted j done →
    Counted (calls limit j ts) (done ++ ts)
  | [], done, j, h => by simpa [calls] using h
  | o :: rest, done, j, h => by
    have := calls_counted limit rest (done ++ [o]) _ (step_counted limit o j done h)
    simpa [calls] using this

/-- A skip at `p` after the calls `ts` from an empty journal needs at least `limit` of them at `p`: every one started
the event without a call at another offset recorded after it. -/
theorem skip_needs_attempts (limit p : Nat) (ts : List Nat) (h : (before limit (calls limit none ts) p).1 = true) :
    limit ≤ (ts.filter (· == p)).length := by
  have hc := calls_counted limit ts [] none (by intro a ha; cases ha)
  simp only [List.nil_append] at hc
  revert h hc
  cases calls limit none ts with
  | none => simp [before_none]
  | some a =>
    intro h hc
    have := hc a rfl
    by_cases h1 : p < a.offset
    · simp [before_completed limit p a h1] at h
    · by_cases h2 : p = a.offset
      · obtain ⟨ao, ac⟩ := a
        simp only at h2 this; subst h2
        by_cases h3 : limit ≤ ac
        · omega
        · simp [before_again limit p ac (by omega)] at h
      · simp [before_new limit p a (by omega)] at h

end Replication.Attempts
