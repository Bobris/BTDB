/-!
# Relation query sorting with a limit keeps exactly the first rows of the full sort

`RelationQueryEnumerator.Sort` (`../ODBLayer/RelationQuery.cs`) of a query ordered and then paged keeps only the first
`limit = skip + take` entries: each row goes to its sorted place (a binary search), a row placed at `limit` or later
is skipped, and the last entry is removed once there are more than `limit`. Rows compare by their sort keys and then
by their sequence number, so no two compare equal; here they are numbers, `ins` puts a row at its sorted place and
`place` is the index the binary search finds.

* `top_k_correct`: for any rows in any order, the entries kept are the first `limit` rows of all of them sorted.
-/

namespace Btdb.QueryTopK

/-- Inserting a row at its sorted place. -/
def ins (x : Nat) : List Nat → List Nat
  | [] => [x]
  | y :: ys => if x < y then x :: y :: ys else y :: ins x ys

/-- The index `ins` puts the row at, which the binary search returns. -/
def place (x : Nat) : List Nat → Nat
  | [] => 0
  | y :: ys => if x < y then 0 else place x ys + 1

/-- One row of `Sort` with a limit. -/
def step (limit : Nat) (entries : List Nat) (x : Nat) : List Nat :=
  if limit ≤ place x entries then entries
  else
    let inserted := ins x entries
    if limit < inserted.length then inserted.dropLast else inserted

theorem ins_length (x : Nat) : ∀ l : List Nat, (ins x l).length = l.length + 1
  | [] => rfl
  | y :: ys => by unfold ins; split <;> simp [ins_length x ys]

theorem place_le (x : Nat) : ∀ l : List Nat, place x l ≤ l.length
  | [] => Nat.le_refl 0
  | y :: ys => by unfold place; split <;> simp [place_le x ys]

/-- A row placed after every entry goes last. -/
theorem ins_at_end (x : Nat) : ∀ l : List Nat, place x l = l.length → ins x l = l ++ [x]
  | [] => fun _ => rfl
  | y :: ys => by
    unfold place ins
    split
    · intro h; simp at h
    · intro h; simp at h; simp [ins_at_end x ys h]

/-- Inserting into the first `k` rows and keeping `k` is keeping `k` of the insertion into all rows. -/
theorem ins_take (x : Nat) : ∀ (l : List Nat) (k : Nat), (ins x (l.take k)).take k = (ins x l).take k
  | _, 0 => by simp
  | [], _ + 1 => by simp
  | y :: ys, k + 1 => by
    simp only [List.take_succ_cons, ins]
    split
    · simp only [List.take_succ_cons]
      cases k with
      | zero => simp
      | succ j => simp [List.take_take]
    · simp only [List.take_succ_cons, ins_take x ys k]

theorem step_eq (limit : Nat) (entries : List Nat) (x : Nat) (h : entries.length ≤ limit) :
    step limit entries x = (ins x entries).take limit := by
  unfold step
  split
  · rename_i hp
    have hl := place_le x entries
    have hplace : place x entries = entries.length := by omega
    rw [ins_at_end x entries hplace]
    have : limit = entries.length := by omega
    subst this
    simp
  · simp only
    split
    · rename_i hlen
      rw [List.dropLast_eq_take, ins_length]
      have : entries.length + 1 - 1 = limit := by rw [ins_length] at hlen; omega
      rw [this]
    · rename_i hlen
      rw [List.take_of_length_le (by omega)]

theorem top_k_correct (limit : Nat) (rows : List Nat) :
    rows.foldl (step limit) [] = (rows.foldl (fun sorted x => ins x sorted) []).take limit := by
  suffices h : ∀ (sorted : List Nat),
      rows.foldl (step limit) (sorted.take limit) = (rows.foldl (fun s x => ins x s) sorted).take limit by
    simpa using h []
  induction rows with
  | nil => intro sorted; rfl
  | cons x rest ih =>
    intro sorted
    simp only [List.foldl_cons]
    rw [step_eq limit _ x (by simp only [List.length_take]; exact Nat.min_le_left _ _), ins_take, ih]

end Btdb.QueryTopK
