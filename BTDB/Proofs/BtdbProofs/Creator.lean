/-!
# Read-only database nodes fit their format, and the tree is finite

A model of how `ReadOnlyCreator` groups children into branch nodes (`WriteBranches`): a branch takes the next child, then
more while it has fewer than `C` children (`MaxChildren`) and the separators, the first keys of all its children but the
first, stay within `M` bytes (`MaxNodeKeyBytes`), because their suffix ends are 16-bit. A child is its first key's length.
`Run` rejects any key longer than `M`, so no first key exceeds it.

* `flatten_groups`: the branches hold the children in order, each once.
* `group_fits`: a branch has at most `C` children and its separators at most `M` bytes.
* `groups_shrink`: from two children up, a level has fewer branches than children, so `Run`'s loop over levels ends
  with one root.
-/

namespace Btdb.Creator

/-- Children taken after the first: `sum` bytes of separators and `count` children so far. -/
def grab (C M : Nat) (sum count : Nat) : List Nat → List Nat × List Nat
  | [] => ([], [])
  | x :: xs =>
    if count < C ∧ sum + x ≤ M then ((grab C M (sum + x) (count + 1) xs).1.cons x, (grab C M (sum + x) (count + 1) xs).2)
    else ([], x :: xs)

theorem grab_split (C M : Nat) : ∀ (sum count : Nat) (l : List Nat), (grab C M sum count l).1 ++ (grab C M sum count l).2 = l
  | _, _, [] => rfl
  | sum, count, x :: xs => by
    unfold grab
    by_cases h : count < C ∧ sum + x ≤ M
    · simp only [h, and_self, ite_true, List.cons_append, grab_split C M (sum + x) (count + 1) xs]
    · simp only [h, ite_false, List.nil_append]

theorem grab_length (C M : Nat) (sum count : Nat) (l : List Nat) :
    (grab C M sum count l).2.length ≤ l.length := by
  have := congrArg List.length (grab_split C M sum count l)
  simp only [List.length_append] at this
  omega

/-- `WriteBranches`: one branch after another. -/
def groups (C M : Nat) : List Nat → List (List Nat)
  | [] => []
  | x :: xs => (x :: (grab C M 0 1 xs).1) :: groups C M (grab C M 0 1 xs).2
termination_by l => l.length
decreasing_by have := grab_length C M 0 1 xs; simp only [List.length_cons]; omega

theorem flatten_groups (C M : Nat) (l : List Nat) : (groups C M l).flatten = l := by
  match l with
  | [] => simp [groups]
  | x :: xs =>
    rw [groups, List.flatten_cons, flatten_groups C M (grab C M 0 1 xs).2, List.cons_append, grab_split]
termination_by l.length
decreasing_by have := grab_length C M 0 1 xs; simp only [List.length_cons]; omega

/-- What `grab` takes stays within the bounds. -/
theorem grab_fits (C M : Nat) : ∀ (sum count : Nat) (l : List Nat), count ≤ C → sum ≤ M →
    count + (grab C M sum count l).1.length ≤ C ∧ sum + (grab C M sum count l).1.sum ≤ M
  | _, _, [], hc, hs => by simp [grab]; omega
  | sum, count, x :: xs, hc, hs => by
    unfold grab
    by_cases h : count < C ∧ sum + x ≤ M
    · simp only [h, and_self, ite_true, List.length_cons, List.sum_cons]
      have := grab_fits C M (sum + x) (count + 1) xs (by omega) h.2
      omega
    · simp only [h, ite_false, List.length_nil, List.sum_nil]; omega

theorem group_fits (C M : Nat) (hC : 1 ≤ C) (l : List Nat) :
    ∀ g ∈ groups C M l, g.length ≤ C ∧ g.tail.sum ≤ M := by
  match l with
  | [] => simp [groups]
  | x :: xs =>
    intro g hg
    rw [groups, List.mem_cons] at hg
    rcases hg with rfl | hg
    · have := grab_fits C M 0 1 xs hC (Nat.zero_le _)
      simp only [List.length_cons, List.tail_cons]
      omega
    · exact group_fits C M hC (grab C M 0 1 xs).2 g hg
termination_by l.length
decreasing_by have := grab_length C M 0 1 xs; simp only [List.length_cons]; omega

/-- With room for a second child, a branch that is not the last takes one. -/
theorem grab_takes (C M : Nat) (hC : 2 ≤ C) (x : Nat) (xs : List Nat) (hx : x ≤ M) :
    (grab C M 0 1 (x :: xs)).2.length + 2 ≤ (x :: xs).length + 1 := by
  unfold grab
  have h : 1 < C ∧ 0 + x ≤ M := ⟨by omega, by omega⟩
  simp only [h, and_self, ite_true]
  have := grab_length C M (0 + x) (1 + 1) xs
  simp only [List.length_cons]
  omega

theorem groups_length (C M : Nat) (hC : 2 ≤ C) (l : List Nat) (hl : ∀ x ∈ l, x ≤ M) :
    2 * (groups C M l).length ≤ l.length + 1 := by
  match l with
  | [] => simp [groups]
  | [x] => simp [groups, grab]
  | x :: y :: ys =>
    rw [groups, List.length_cons]
    have hr := grab_takes C M hC y ys (hl y (by simp))
    have ih := groups_length C M hC (grab C M 0 1 (y :: ys)).2 (fun z hz => by
      have := grab_split C M 0 1 (y :: ys)
      exact hl z (List.mem_cons_of_mem x (this ▸ List.mem_append_right _ hz)))
    simp only [List.length_cons] at hr ⊢
    omega
termination_by l.length
decreasing_by have := grab_length C M 0 1 (y :: ys); simp only [List.length_cons] at *; omega

theorem groups_shrink (C M : Nat) (hC : 2 ≤ C) (l : List Nat) (hl : ∀ x ∈ l, x ≤ M) (h2 : 2 ≤ l.length) :
    (groups C M l).length < l.length := by
  have := groups_length C M hC l hl
  omega

end Btdb.Creator
