/-!
# Serving tenants in consumption order needs Ω(log n) comparisons per step

The BackendActions coordinator serves the tenant that consumed least, and starting its action raises its
consumption above the others'. Served n times, a procedure therefore outputs the n tenants in consumption order.

A comparison-based procedure is a decision tree: each inner node asks whether tenant `i` consumed less than tenant
`j`, each leaf is the answer. Its depth is the worst-case number of comparisons. This file proves, without
libraries, that a tree that is right for every order of `n` tenants has depth at least `log₂ n!`, and that for
`n = 2^(j+1)` this is at least `j * 2^j`: `(log₂ n - 1) / 2` comparisons per served tenant. It also proves that
naming one of `n` candidates, the worker that takes an action or the class whose action starts, needs `log₂ n`.
-/

namespace PlannerLowerBound

def fact : Nat → Nat
  | 0 => 1
  | n + 1 => (n + 1) * fact n

/-- A comparison-based procedure answering with an `α`. -/
inductive DTree (α : Type) where
  | leaf : α → DTree α
  | node : Nat → Nat → DTree α → DTree α → DTree α

namespace DTree

/-- Runs the procedure against an oracle telling whether tenant `i` consumed less than tenant `j`. -/
def eval {α : Type} (less : Nat → Nat → Bool) : DTree α → α
  | leaf a => a
  | node i j yes no => if less i j then eval less yes else eval less no

def depth {α : Type} : DTree α → Nat
  | leaf _ => 0
  | node _ _ yes no => max (depth yes) (depth no) + 1

end DTree

theorem length_filter_add (p : β → Bool) : ∀ l : List β,
    (l.filter p).length + (l.filter (fun x => !p x)).length = l.length
  | [] => rfl
  | x :: l => by
    have ih := length_filter_add p l
    cases h : p x <;> simp [List.filter, h] <;> omega

theorem nodup_map_filter {f : β → γ} (p : β → Bool) {l : List β} (h : (l.map f).Nodup) :
    ((l.filter p).map f).Nodup :=
  List.Nodup.sublist (List.Sublist.map f (List.filter_sublist)) h

/-- A tree gives at most `2 ^ depth` different answers: inputs with pairwise different answers number at most that. -/
theorem inputs_le {α : Type} : ∀ (t : DTree α) (inputs : List (Nat → Nat → Bool)),
    (inputs.map (fun less => t.eval less)).Nodup → inputs.length ≤ 2 ^ t.depth
  | .leaf a, inputs, h => by
    match inputs, h with
    | [], _ => simp
    | [_], _ => simp [DTree.depth]
    | _ :: _ :: _, h => simp [DTree.eval] at h
  | .node i j yes no, inputs, h => by
    let left := inputs.filter (fun less => less i j)
    let right := inputs.filter (fun less => !less i j)
    have hl : (left.map (fun less => yes.eval less)).Nodup := by
      have := nodup_map_filter (fun less => less i j) h
      have e : left.map (fun less => (DTree.node i j yes no).eval less) = left.map (fun less => yes.eval less) :=
        List.map_congr_left (fun less m => by simp [DTree.eval, (List.mem_filter.mp m).2])
      exact e ▸ this
    have hr : (right.map (fun less => no.eval less)).Nodup := by
      have := nodup_map_filter (fun less => !less i j) h
      have e : right.map (fun less => (DTree.node i j yes no).eval less) = right.map (fun less => no.eval less) :=
        List.map_congr_left (fun less m => by
          have := (List.mem_filter.mp m).2
          simp at this
          simp [DTree.eval, this])
      exact e ▸ this
    have l1 := inputs_le yes left hl
    have r1 := inputs_le no right hr
    have split : left.length + right.length = inputs.length := length_filter_add (fun less => less i j) inputs
    have py : 2 ^ yes.depth ≤ 2 ^ max yes.depth no.depth := Nat.pow_le_pow_right (by decide) (Nat.le_max_left _ _)
    have pn : 2 ^ no.depth ≤ 2 ^ max yes.depth no.depth := Nat.pow_le_pow_right (by decide) (Nat.le_max_right _ _)
    simp only [DTree.depth, Nat.pow_succ]
    omega

/-! ## Every order of `n` tenants: `n!` of them, all different -/

/-- Inserts `x` before position `d`. -/
def insertAt (x : Nat) : Nat → List Nat → List Nat
  | 0, l => x :: l
  | _ + 1, [] => [x]
  | d + 1, y :: l => y :: insertAt x d l

/-- Position of `x` in the list, or its length when absent. -/
def idx (x : Nat) : List Nat → Nat
  | [] => 0
  | y :: l => if y = x then 0 else idx x l + 1

/-- The list without the first `x`. -/
def drop1 (x : Nat) : List Nat → List Nat
  | [] => []
  | y :: l => if y = x then l else y :: drop1 x l

theorem idx_insertAt (x : Nat) : ∀ (d : Nat) (l : List Nat), x ∉ l → d ≤ l.length → idx x (insertAt x d l) = d
  | 0, l, _, _ => by simp [insertAt, idx]
  | d + 1, [], _, h => by simp at h
  | d + 1, y :: l, hx, h => by
    have hy : y ≠ x := fun e => hx (e ▸ List.mem_cons_self ..)
    have := idx_insertAt x d l (fun m => hx (List.mem_cons_of_mem _ m)) (by simp at h; omega)
    simp [insertAt, idx, hy, this]

theorem drop1_insertAt (x : Nat) : ∀ (d : Nat) (l : List Nat), x ∉ l → drop1 x (insertAt x d l) = l
  | 0, l, _ => by simp [insertAt, drop1]
  | d + 1, [], _ => by simp [insertAt, drop1]
  | d + 1, y :: l, hx => by
    have hy : y ≠ x := fun e => hx (e ▸ List.mem_cons_self ..)
    simp [insertAt, drop1, hy, drop1_insertAt x d l (fun m => hx (List.mem_cons_of_mem _ m))]

theorem mem_insertAt (x : Nat) : ∀ (d : Nat) (l : List Nat) (z : Nat), z ∈ insertAt x d l → z = x ∨ z ∈ l
  | 0, l, z, h => by simpa [insertAt] using h
  | d + 1, [], z, h => by simp [insertAt] at h; exact Or.inl h
  | d + 1, y :: l, z, h => by
    simp only [insertAt, List.mem_cons] at h
    rcases h with h | h
    · exact Or.inr (h ▸ List.mem_cons_self ..)
    · rcases mem_insertAt x d l z h with h | h
      · exact Or.inl h
      · exact Or.inr (List.mem_cons_of_mem _ h)

theorem length_insertAt (x : Nat) : ∀ (d : Nat) (l : List Nat), (insertAt x d l).length = l.length + 1
  | 0, l => by simp [insertAt]
  | d + 1, [] => by simp [insertAt]
  | d + 1, y :: l => by simp [insertAt, length_insertAt x d l]

/-- The orders of tenants `0 … n-1`: each order of `0 … n-2` with `n-1` inserted at each of its `n` positions. -/
def orders : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => (orders n).flatMap (fun o => (List.range (n + 1)).map (fun d => insertAt n d o))

theorem orders_shape : ∀ (n : Nat) (o : List Nat), o ∈ orders n → o.length = n ∧ ∀ z ∈ o, z < n
  | 0, o, h => by simp [orders] at h; subst h; simp
  | n + 1, o, h => by
    simp only [orders, List.mem_flatMap, List.mem_map, List.mem_range] at h
    obtain ⟨p, hp, d, _, rfl⟩ := h
    obtain ⟨len, below⟩ := orders_shape n p hp
    refine ⟨by simp [length_insertAt, len], fun z m => ?_⟩
    rcases mem_insertAt n d p z m with e | e
    · omega
    · have := below z e; omega

theorem length_flatMap_const {f : β → List γ} (k : Nat) :
    ∀ l : List β, (∀ x ∈ l, (f x).length = k) → (l.flatMap f).length = l.length * k
  | [], _ => by simp
  | x :: l, h => by
    have := length_flatMap_const k l (fun y m => h y (List.mem_cons_of_mem _ m))
    simp [List.flatMap_cons, h x (List.mem_cons_self ..), this, Nat.succ_mul]
    omega

theorem nodup_flatMap_of {f : β → List γ} : ∀ {l : List β}, l.Nodup → (∀ x ∈ l, (f x).Nodup) →
    (∀ x ∈ l, ∀ y ∈ l, x ≠ y → ∀ z ∈ f x, z ∉ f y) → (l.flatMap f).Nodup
  | [], _, _, _ => by simp
  | x :: l, nd, each, disjoint => by
    rw [List.flatMap_cons, List.nodup_append]
    have nd' := List.nodup_cons.mp nd
    refine ⟨each x (List.mem_cons_self ..),
      nodup_flatMap_of nd'.2 (fun y m => each y (List.mem_cons_of_mem _ m))
        (fun a ma b mb ne => disjoint a (List.mem_cons_of_mem _ ma) b (List.mem_cons_of_mem _ mb) ne), ?_⟩
    intro a ma b mb same
    obtain ⟨y, my, hb⟩ := List.mem_flatMap.mp mb
    exact disjoint x (List.mem_cons_self ..) y (List.mem_cons_of_mem _ my) (fun h => nd'.1 (h ▸ my)) a ma
      (same ▸ hb)

theorem nodup_map_of_inj {f : β → γ} : ∀ {l : List β}, l.Nodup → (∀ a ∈ l, ∀ b ∈ l, f a = f b → a = b) →
    (l.map f).Nodup
  | [], _, _ => by simp
  | x :: l, nd, inj => by
    have nd' := List.nodup_cons.mp nd
    refine List.nodup_cons.mpr ⟨fun m => ?_, nodup_map_of_inj nd'.2
      (fun a ma b mb => inj a (List.mem_cons_of_mem _ ma) b (List.mem_cons_of_mem _ mb))⟩
    obtain ⟨y, my, same⟩ := List.mem_map.mp m
    exact nd'.1 (inj y (List.mem_cons_of_mem _ my) x (List.mem_cons_self ..) same ▸ my)

theorem length_orders : ∀ n, (orders n).length = fact n
  | 0 => rfl
  | n + 1 => by
    simp only [orders, fact]
    rw [length_flatMap_const (n + 1) _ (fun o _ => by simp), length_orders n, Nat.mul_comm]

theorem nodup_orders : ∀ n, (orders n).Nodup
  | 0 => by simp [orders]
  | n + 1 => by
    have absent : ∀ p ∈ orders n, n ∉ p := fun p hp m => by
      have := (orders_shape n p hp).2 n m; omega
    apply nodup_flatMap_of (nodup_orders n)
    · intro p hp
      apply nodup_map_of_inj List.nodup_range
      intro d hd e he same
      have hd := List.mem_range.mp hd
      have he := List.mem_range.mp he
      have len := (orders_shape n p hp).1
      have := congrArg (idx n) same
      rwa [idx_insertAt n d p (absent p hp) (by omega), idx_insertAt n e p (absent p hp) (by omega)] at this
    · intro p hp q hq ne z hz hz'
      obtain ⟨d, _, rfl⟩ := List.mem_map.mp hz
      obtain ⟨e, _, same⟩ := List.mem_map.mp hz'
      apply ne
      have := congrArg (drop1 n) same
      rw [drop1_insertAt n e q (absent q hq), drop1_insertAt n d p (absent p hp)] at this
      exact this.symm

/-! ## The bound -/

/-- The oracle of an order: tenant `i` consumed less than `j` when it comes first. -/
def lessOf (o : List Nat) (i j : Nat) : Bool := decide (idx i o < idx j o)

/-- A comparison-based procedure that serves every order of `n` tenants correctly asks at least `log₂ n!` questions
in the worst case. -/
theorem serving_needs (n : Nat) (t : DTree (List Nat)) (correct : ∀ o ∈ orders n, t.eval (lessOf o) = o) :
    fact n ≤ 2 ^ t.depth := by
  have answers : ((orders n).map lessOf).map (fun less => t.eval less) = orders n := by
    rw [List.map_map]
    conv => rhs; rw [← List.map_id (orders n)]
    exact List.map_congr_left (fun o m => correct o m)
  have distinct : (((orders n).map lessOf).map (fun less => t.eval less)).Nodup := by
    rw [answers]; exact nodup_orders n
  have := inputs_le t ((orders n).map lessOf) distinct
  simpa [length_orders] using this

/-- `m^k * m! ≤ (m + k)!`, so `m^m ≤ (2m)!`. -/
theorem pow_mul_fact_le (m : Nat) : ∀ k, m ^ k * fact m ≤ fact (m + k)
  | 0 => by simp
  | k + 1 => by
    have ih := pow_mul_fact_le m k
    have : m ^ (k + 1) * fact m = m * (m ^ k * fact m) := by rw [Nat.pow_succ]; ac_rfl
    rw [this, show m + (k + 1) = (m + k) + 1 by omega, fact]
    exact Nat.mul_le_mul (by omega) ih

theorem fact_pos : ∀ n, 0 < fact n
  | 0 => by decide
  | n + 1 => Nat.mul_pos (by omega) (fact_pos n)

/-- For `2^(j+1)` tenants a correct procedure asks at least `j * 2^j` questions: per served tenant at least half of
`log₂ (2^(j+1)) - 1`, so Ω(log n) per step however the work is spread over the steps. -/
theorem serving_needs_log (j : Nat) (t : DTree (List Nat))
    (correct : ∀ o ∈ orders (2 ^ (j + 1)), t.eval (lessOf o) = o) : j * 2 ^ j ≤ t.depth := by
  have bound := serving_needs _ t correct
  have half : 2 ^ (j + 1) = 2 ^ j + 2 ^ j := by rw [Nat.pow_succ]; omega
  have lower : (2 ^ j) ^ (2 ^ j) ≤ fact (2 ^ (j + 1)) := by
    have := pow_mul_fact_le (2 ^ j) (2 ^ j)
    rw [← half] at this
    exact Nat.le_trans (Nat.le_mul_of_pos_right _ (fact_pos _)) this
  rw [← Nat.pow_mul] at lower
  exact (Nat.pow_le_pow_iff_right (by decide)).mp (Nat.le_trans lower bound)

/-! ## Choosing one of `n`: a worker, a class

Picking the worker that takes an action, or the class whose action starts next, has as many possible answers as
there are workers or classes: any one of them may be the only one with room, or the only one open. -/

/-- The oracle in which only `i` qualifies: asked about `j`, it answers whether `j` is `i`. -/
def only (i : Nat) (j _ : Nat) : Bool := decide (j = i)

/-- A comparison-based procedure that names the one qualifying candidate out of `n`, whichever it is, asks at least
`log₂ n` questions in the worst case: picking among `W` workers or `C` classes costs Ω(log W) or Ω(log C). -/
theorem choosing_needs (n : Nat) (t : DTree Nat) (correct : ∀ i, i < n → t.eval (only i) = i) : n ≤ 2 ^ t.depth := by
  have answers : ((List.range n).map only).map (fun less => t.eval less) = List.range n := by
    rw [List.map_map]
    conv => rhs; rw [← List.map_id (List.range n)]
    exact List.map_congr_left (fun i m => correct i (List.mem_range.mp m))
  have distinct : (((List.range n).map only).map (fun less => t.eval less)).Nodup := by
    rw [answers]; exact List.nodup_range
  simpa using inputs_le t ((List.range n).map only) distinct

end PlannerLowerBound
