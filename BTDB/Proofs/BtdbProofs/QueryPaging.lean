/-!
# Relation query paging computes the rows of its Skip and Take calls

`RelationQuerySource.Bind` (`../ODBLayer/RelationQuery.cs`) folds the `Queryable.Skip` and `Take` calls of a relation
query, in order, into one skip and an optional take, which the query applies to the ordered rows. `Skip` of a count
not above zero is ignored, as `Enumerable.Skip` does, and `Take` of a negative count takes nothing.

* `bind_correct`: for any rows and any sequence of calls, skipping and taking as bound gives exactly the rows the calls
  give applied one after another, as LINQ does.
-/

namespace Btdb.QueryPaging

/-- One `Skip` (`true`) or `Take` (`false`) call with its count. -/
abbrev Call := Bool × Int

/-- LINQ applying one call. -/
def apply {α : Type} (l : List α) : Call → List α
  | (true, n) => l.drop n.toNat
  | (false, n) => l.take n.toNat

/-- The window `Bind` computes: a skip and an optional take. -/
structure Window where
  skip : Nat
  take : Option Nat

def Window.rows {α : Type} (w : Window) (l : List α) : List α :=
  match w.take with
  | none => l.drop w.skip
  | some t => (l.drop w.skip).take t

/-- One iteration of the loop in `Bind`. -/
def step (w : Window) : Call → Window
  | (true, n) =>
    if n ≤ 0 then w else ⟨w.skip + n.toNat, w.take.map (· - n.toNat)⟩
  | (false, n) => ⟨w.skip, some (match w.take with | none => n.toNat | some t => min t n.toNat)⟩

def bind (calls : List Call) : Window := calls.foldl step ⟨0, none⟩

theorem step_rows {α : Type} (w : Window) (c : Call) (l : List α) :
    (step w c).rows l = apply (w.rows l) c := by
  obtain ⟨skip, take⟩ := w
  obtain ⟨isSkip, n⟩ := c
  cases isSkip with
  | true =>
    by_cases hn : n ≤ 0
    · have : n.toNat = 0 := Int.toNat_of_nonpos hn
      simp only [step, hn, ite_true, apply, this, List.drop_zero]
    · simp only [step, hn, ite_false]
      cases take with
      | none => simp [Window.rows, apply, List.drop_drop]
      | some t => simp [Window.rows, apply, List.drop_take, List.drop_drop]
  | false =>
    cases take with
    | none => simp [step, Window.rows, apply]
    | some t => simp [step, Window.rows, apply, List.take_take, Nat.min_comm]

theorem bind_correct {α : Type} (calls : List Call) (l : List α) :
    (bind calls).rows l = calls.foldl apply l := by
  unfold bind
  suffices h : ∀ (w : Window) (l' : List α), w.rows l = l' → (calls.foldl step w).rows l = calls.foldl apply l' by
    exact h _ _ rfl
  induction calls with
  | nil => intro w l' h; exact h
  | cons c rest ih =>
    intro w l' h
    simp only [List.foldl_cons]
    exact ih _ _ (by rw [step_rows, h])

end Btdb.QueryPaging
