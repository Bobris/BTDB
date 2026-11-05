/-!
# The indexed planner serves exactly the reference's plan

The reference planner serves in rounds. In each round it visits the round's tenants in the order of their
consumption at the round's start, ties broken by tenant ID, and starts each one's earliest startable action. The
next round consists of the tenants served in this one. A tenant visited without a startable action is skipped.

The indexed planner instead repeatedly takes, among all lanes that may start, the least by its tenant's starts in
this plan, consumption and ID, then the action. This file proves that every run of that rule is the reference's run.
The only assumption is that starting an action never makes another action startable: room only shrinks and groups
only fill during a plan. It also proves the two facts the indexed structures rely on. A lazily repaired minimum is
the true minimum, and a cost fits some worker exactly when it is at most the largest room.

A lane is `(tenant, action)`; actions are numbered in their start order (priority, then enqueue order), so the
earliest startable action of a tenant is its least.
-/

namespace PlannerSelection

abbrev Lane := Nat × Nat

structure Plan where
  /-- The lanes waiting when the plan begins. -/
  lanes : List Lane
  /-- Whether an action may start after the given starts: a worker has room for it and its group is not full. -/
  ok : List Lane → Lane → Bool
  /-- A start never makes an action startable. -/
  antitone : ∀ s x l, ok (s ++ [x]) l = true → ok s l = true
  cost : Lane → Nat
  key0 : Nat → Nat

variable (P : Plan)

/-- Actions started for the tenant in this plan. -/
def served (s : List Lane) (t : Nat) : Nat := (s.filter (fun l => decide (l.1 = t))).length

/-- The tenant's consumption after the starts. -/
def keyOf (s : List Lane) (t : Nat) : Nat :=
  P.key0 t + ((s.filter (fun l => decide (l.1 = t))).map P.cost).foldr (· + ·) 0

/-- The lane waits and may start after the starts. -/
def avail (s : List Lane) (l : Lane) : Prop := l ∈ P.lanes ∧ l ∉ s ∧ P.ok s l = true

theorem served_append (s : List Lane) (l : Lane) (t : Nat) :
    served (s ++ [l]) t = served s t + (if l.1 = t then 1 else 0) := by
  simp only [served, List.filter_append, List.length_append]
  by_cases h : l.1 = t <;> simp [List.filter, h]

theorem keyOf_append_other (s : List Lane) (l : Lane) (t : Nat) (h : l.1 ≠ t) :
    keyOf P (s ++ [l]) t = keyOf P s t := by
  simp [keyOf, List.filter_append, List.filter, h]

theorem avail_of_append {s : List Lane} {x l : Lane} (h : avail P (s ++ [x]) l) : avail P s l :=
  ⟨h.1, fun m => h.2.1 (List.mem_append_left _ m), P.antitone s x l h.2.2⟩

/-! ## The indexed planner's rule -/

/-- Tenant `t` comes before `u` by starts in this plan, then consumption, then ID. -/
def before (s : List Lane) (t u : Nat) : Prop :=
  served s t < served s u ∨ (served s t = served s u ∧ (keyOf P s t < keyOf P s u ∨ (keyOf P s t = keyOf P s u ∧ t < u)))

/-- The lane the indexed planner starts: its tenant comes before every other tenant with a startable lane, and its
action is the tenant's least startable one. -/
def ImplPick (s : List Lane) (l : Lane) : Prop :=
  avail P s l ∧ (∀ l', avail P s l' → l'.1 ≠ l.1 → before P s l.1 l'.1) ∧
    (∀ l', avail P s l' → l'.1 = l.1 → l.2 ≤ l'.2)

/-- A run of the indexed planner: the lanes it starts, in order, until none may start. -/
inductive ImplRun : List Lane → List Lane → Prop
  | done (s : List Lane) : (∀ l, ¬ avail P s l) → ImplRun s []
  | step (s : List Lane) (l : Lane) (rest : List Lane) : ImplPick P s l → ImplRun (s ++ [l]) rest → ImplRun s (l :: rest)

/-- The code's order of lanes: by tenant (starts in this plan, consumption, ID), then action, lexicographically. -/
def lexBefore (s : List Lane) (a b : Lane) : Prop := before P s a.1 b.1 ∨ (a.1 = b.1 ∧ a.2 < b.2)

theorem before_irrefl (s : List Lane) (t : Nat) : ¬ before P s t t := by
  unfold before; omega

/-- The code takes the least startable lane by that order; that lane is the rule's pick. -/
theorem lex_least_is_pick (s : List Lane) (l : Lane) (a : avail P s l)
    (least : ∀ l', avail P s l' → l' ≠ l → lexBefore P s l l') : ImplPick P s l := by
  refine ⟨a, fun l' a' ne => ?_, fun l' a' e => ?_⟩
  · rcases least l' a' (fun h => ne (h ▸ rfl)) with h | ⟨e, _⟩
    · exact h
    · exact absurd e.symm ne
  · by_cases same : l' = l
    · exact same ▸ Nat.le_refl _
    · rcases least l' a' same with h | ⟨_, h⟩
      · exact absurd (e ▸ h) (before_irrefl P s l.1)
      · exact Nat.le_of_lt h

/-! ## The reference planner's rounds -/

structure Round where
  /-- The starts so far. -/
  s : List Lane
  /-- The round's tenants, and their consumption when the round began. -/
  members : List Nat
  snap : Nat → Nat
  /-- The round's tenants served in it so far. -/
  done : List Nat

/-- `t` comes before `u` in the round: by consumption at the round's start, then ID. -/
def Round.before (σ : Round) (t u : Nat) : Prop := σ.snap t < σ.snap u ∨ (σ.snap t = σ.snap u ∧ t < u)

/-- The round's next start: of the round's tenants not served yet that have a startable lane, the first, and its
least startable action. A tenant without one is skipped, and stays without one. -/
def RefPick (σ : Round) (l : Lane) : Prop :=
  avail P σ.s l ∧ l.1 ∈ σ.members ∧ l.1 ∉ σ.done ∧
    (∀ l', avail P σ.s l' → l'.1 ∈ σ.members → l'.1 ∉ σ.done → l'.1 ≠ l.1 → σ.before l.1 l'.1) ∧
    (∀ l', avail P σ.s l' → l'.1 = l.1 → l.2 ≤ l'.2)

/-- No tenant of the round left unserved has a startable lane. -/
def Stuck (σ : Round) : Prop := ∀ l, avail P σ.s l → l.1 ∈ σ.members → l.1 ∈ σ.done

/-- A run of the reference: starts within rounds, a new round of the tenants served in the last one, and the end
when a round served nobody. -/
inductive RefRun : Round → List Lane → Prop
  | finish (σ : Round) : Stuck P σ → σ.done = [] → RefRun σ []
  | advance (σ : Round) (rest : List Lane) : Stuck P σ → σ.done ≠ [] →
      RefRun ⟨σ.s, σ.done, keyOf P σ.s, []⟩ rest → RefRun σ rest
  | start (σ : Round) (l : Lane) (rest : List Lane) : RefPick P σ l →
      RefRun ⟨σ.s ++ [l], σ.members, σ.snap, l.1 :: σ.done⟩ rest → RefRun σ (l :: rest)

/-- The reference's first round: every tenant, by its consumption before the plan. -/
def Round.initial : Round := ⟨[], P.lanes.map (·.1), P.key0, []⟩

/-! ## The correspondence -/

/-- In round `r`: unserved round tenants have `r` starts and their consumption at the round's start, served ones
`r + 1`, and a tenant outside the round has nothing startable. -/
structure Inv (r : Nat) (σ : Round) : Prop where
  waiting : ∀ t ∈ σ.members, t ∉ σ.done → served σ.s t = r ∧ keyOf P σ.s t = σ.snap t
  doneServed : ∀ t ∈ σ.done, served σ.s t = r + 1
  doneMember : ∀ t ∈ σ.done, t ∈ σ.members
  outside : ∀ l, avail P σ.s l → l.1 ∈ σ.members

theorem inv_initial : Inv P 0 (Round.initial P) where
  waiting := fun t _ _ => ⟨by simp [served, Round.initial], by simp [keyOf, Round.initial]⟩
  doneServed := fun t h => by simp [Round.initial] at h
  doneMember := fun t h => by simp [Round.initial] at h
  outside := fun l h => List.mem_map.mpr ⟨l, h.1, rfl⟩

/-- A new round keeps the correspondence, one start further. -/
theorem inv_advance {r : Nat} {σ : Round} (inv : Inv P r σ) (stuck : Stuck P σ) :
    Inv P (r + 1) ⟨σ.s, σ.done, keyOf P σ.s, []⟩ where
  waiting := fun t m _ => ⟨inv.doneServed t m, rfl⟩
  doneServed := fun t h => by simp at h
  doneMember := fun t h => by simp at h
  outside := fun l h => stuck l h (inv.outside l h)

/-- A start keeps the correspondence. -/
theorem inv_start {r : Nat} {σ : Round} {l : Lane} (inv : Inv P r σ) (pick : RefPick P σ l) :
    Inv P r ⟨σ.s ++ [l], σ.members, σ.snap, l.1 :: σ.done⟩ where
  waiting := fun t m nd => by
    have ne : l.1 ≠ t := fun e => nd (e ▸ List.mem_cons_self ..)
    have := inv.waiting t m (fun d => nd (List.mem_cons_of_mem _ d))
    simp only [served_append, ne, ite_false, Nat.add_zero, keyOf_append_other P _ _ _ ne]
    exact this
  doneServed := fun t h => by
    simp only [List.mem_cons] at h
    rcases h with rfl | h
    · simp only [served_append, ite_true]
      rw [(inv.waiting l.1 pick.2.1 pick.2.2.1).1]
    · have ne : l.1 ≠ t := fun e => pick.2.2.1 (e ▸ h)
      simp only [served_append, ne, ite_false, Nat.add_zero]
      exact inv.doneServed t h
  doneMember := fun t h => by
    simp only [List.mem_cons] at h
    rcases h with rfl | h
    · exact pick.2.1
    · exact inv.doneMember t h
  outside := fun l' h => inv.outside l' (avail_of_append P h)

/-- The indexed planner's lane, of a tenant not yet served in the round, is the reference's next start. -/
theorem pick_agrees {r : Nat} {σ : Round} {l : Lane} (inv : Inv P r σ) (pick : ImplPick P σ.s l)
    (waiting : l.1 ∉ σ.done) : RefPick P σ l := by
  have member := inv.outside l pick.1
  obtain ⟨sl, kl⟩ := inv.waiting l.1 member waiting
  refine ⟨pick.1, member, waiting, fun l' a m nd ne => ?_, pick.2.2⟩
  obtain ⟨s', k'⟩ := inv.waiting l'.1 m nd
  rcases pick.2.1 l' a ne with h | ⟨_, h | ⟨e, h⟩⟩
  · omega
  · exact Or.inl (kl ▸ k' ▸ h)
  · exact Or.inr ⟨kl ▸ k' ▸ e, h⟩

/-- When the indexed planner picks a tenant already served in the round, no unserved one has a startable lane. -/
theorem stuck_of_pick {r : Nat} {σ : Round} {l : Lane} (inv : Inv P r σ) (pick : ImplPick P σ.s l)
    (served : l.1 ∈ σ.done) : Stuck P σ := by
  intro l' a m
  by_cases nd : l'.1 ∈ σ.done
  · exact nd
  · have ne : l'.1 ≠ l.1 := fun e => nd (e ▸ served)
    have h := pick.2.1 l' a ne
    have hl := inv.doneServed l.1 served
    have hl' := (inv.waiting l'.1 m nd).1
    unfold before at h
    omega

/-- Every run of the indexed planner from a state in correspondence is a run of the reference. -/
theorem impl_runs_reference : ∀ {s : List Lane} {trace : List Lane}, ImplRun P s trace →
    ∀ (r : Nat) (σ : Round), σ.s = s → Inv P r σ → RefRun P σ trace := by
  intro s trace run
  induction run with
  | done s none =>
    intro r σ hs inv
    have stuck : Stuck P σ := fun l a _ => absurd (hs ▸ a) (none l)
    by_cases e : σ.done = []
    · exact .finish σ stuck e
    · refine .advance σ [] stuck e (.finish _ (fun l a _ => absurd (hs ▸ a) (none l)) rfl)
  | step s l rest pick _ ih =>
    intro r σ hs inv
    subst hs
    by_cases d : l.1 ∈ σ.done
    · -- The round has nobody left who may start: the reference begins the next round, then starts the same lane.
      have stuck := stuck_of_pick P inv pick d
      have ne : σ.done ≠ [] := fun e => by simp [e] at d
      refine .advance σ (l :: rest) stuck ne ?_
      have inv' := inv_advance P inv stuck
      have pick' := pick_agrees P inv' pick (by simp)
      exact .start _ l rest pick' (ih (r + 1) _ rfl (inv_start P inv' pick'))
    · have pick' := pick_agrees P inv pick d
      exact .start σ l rest pick' (ih r _ rfl (inv_start P inv pick'))

/-- The indexed planner's plan is the reference's: every run of it, from the plan's start, is a run of the rounds. -/
theorem indexed_plan_is_reference (trace : List Lane) (run : ImplRun P [] trace) :
    RefRun P (Round.initial P) trace :=
  impl_runs_reference P run 0 _ rfl (inv_initial P)

/-- The reference's next start is determined: two picks from the same state are the same lane. -/
theorem ref_pick_unique {σ : Round} {l l' : Lane} (a : RefPick P σ l) (b : RefPick P σ l') : l = l' := by
  by_cases e : l.1 = l'.1
  · have h1 := a.2.2.2.2 l' b.1 e.symm
    have h2 := b.2.2.2.2 l a.1 e
    exact Prod.ext e (by omega)
  · have h1 := a.2.2.2.1 l' b.1 b.2.1 b.2.2.1 (Ne.symm e)
    have h2 := b.2.2.2.1 l a.1 a.2.1 a.2.2.1 e
    unfold Round.before at h1 h2
    omega

/-! ## The indexed structures -/

/-- Lazy repair: entries may be stale, never too late. When the least stored key is up to date, it is the least true
key: every class's true first entry is at least its stored one. -/
theorem lazy_least {ι : Type} (stored current : ι → Nat) (stale : ∀ i, stored i ≤ current i) (j : ι)
    (least : ∀ i, stored j ≤ stored i) (fresh : stored j = current j) : ∀ i, current j ≤ current i :=
  fun i => fresh ▸ Nat.le_trans (least i) (stale i)

/-- The largest room of a list of workers' rooms; `0` when there is none. -/
def largest : List Nat → Nat
  | [] => 0
  | r :: rooms => max r (largest rooms)

/-- A cost fits some worker's room exactly when it is at most the largest: so the classes that may start are those
whose cost is at most the largest room, a prefix of the classes sorted by cost. -/
theorem fits_iff_le_largest (cost : Nat) : ∀ rooms : List Nat, rooms ≠ [] →
    ((∃ r ∈ rooms, cost ≤ r) ↔ cost ≤ largest rooms)
  | [], h => absurd rfl h
  | [r], _ => by simp [largest]
  | r :: q :: rooms, _ => by
    have ih := fits_iff_le_largest cost (q :: rooms) (by simp)
    simp only [largest] at ih ⊢
    constructor
    · rintro ⟨x, m, h⟩
      simp only [List.mem_cons] at m
      rcases m with rfl | m
      · exact Nat.le_trans h (Nat.le_max_left _ _)
      · exact Nat.le_trans (ih.mp ⟨x, by simpa using m, h⟩) (Nat.le_max_right _ _)
    · intro h
      rcases Nat.le_total (max q (largest rooms)) r with t | t
      · rw [Nat.max_eq_left t] at h
        exact ⟨r, List.mem_cons_self .., h⟩
      · rw [Nat.max_eq_right t] at h
        obtain ⟨x, m, hx⟩ := ih.mpr h
        exact ⟨x, List.mem_cons_of_mem _ m, hx⟩

end PlannerSelection
