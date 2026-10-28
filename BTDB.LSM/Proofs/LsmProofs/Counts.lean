/-!
# Counted entries and the compaction merge

A per-key model of the counted entries of `ImplementationPlan.md`, sections 6.1 to 6.3, and of `LsmMerger`. A
component holds at one key at most one point entry `(set, existsBelow)` and may cover the key with a range-tombstone
fragment; a point in the same component is newer than its fragment. A component contributes to the key count
`δ = [set] − [existsBelow]` for its point and `−[exists below]` for its fragment (its share of `coveredBelow`).

* `total_telescopes`: over a consistent stack of components the contributions sum to
  `[exists on top] − [exists below]`, so counts and ranks are additive across components (L5).
* `merge_final`, `merge_consistent`, `merge_total`: the component `LsmMerger` produces from a run reaches the same
  state at the key, is consistent with the state below the run, and contributes the same count as the run.
-/

namespace Lsm.Counts

structure Point where
  set : Bool
  below : Bool

/-- A component at one key: its point entry and whether one of its fragments covers the key. -/
structure Slot where
  point : Option Point
  tomb : Bool

def ind (b : Bool) : Int := if b then 1 else 0

/-- Whether the key exists above a component, given whether it exists below. -/
def after (s : Bool) (c : Slot) : Bool :=
  match c.point with
  | some p => p.set
  | none => if c.tomb then false else s

def contrib (s : Bool) (c : Slot) : Int :=
  (match c.point with
   | some p => ind p.set - ind p.below
   | none => 0) - (if c.tomb then ind s else 0)

/-- A run of components, newest first, above a state `base`. -/
def final (base : Bool) : List Slot → Bool
  | [] => base
  | c :: cs => after (final base cs) c

def total (base : Bool) : List Slot → Int
  | [] => 0
  | c :: cs => contrib (final base cs) c + total base cs

/-- Every point's `existsBelow` is the state below it: after its own component's fragment, if any. -/
def Consistent (base : Bool) : List Slot → Prop
  | [] => True
  | c :: cs => (∀ p, c.point = some p → p.below = (if c.tomb then false else final base cs)) ∧ Consistent base cs

theorem total_telescopes (base : Bool) : ∀ cs, Consistent base cs → total base cs = ind (final base cs) - ind base
  | [], _ => by simp [total, final]
  | c :: cs, ⟨hc, hcs⟩ => by
    simp only [total, final]
    rw [total_telescopes base cs hcs]
    cases hp : c.point with
    | some p =>
      have hb := hc p hp
      simp only [contrib, after, hp]
      cases ht : c.tomb <;> simp [ht] at hb <;> simp [hb, ind] <;> omega
    | none =>
      simp only [contrib, after, hp]
      cases c.tomb <;> simp [ind] <;> split <;> omega

/-! ## `LsmMerger` at one key -/

def covered (cs : List Slot) : Bool := cs.any (·.tomb)

/-- The newest decision: a point's kind, or a deleting fragment. -/
def decision : List Slot → Option Bool
  | [] => none
  | c :: cs =>
    match c.point with
    | some p => some p.set
    | none => if c.tomb then some false else decision cs

/-- `existsBelow` of the oldest point. -/
def oldestBelow : List Slot → Option Bool
  | [] => none
  | c :: cs =>
    match oldestBelow cs with
    | some b => some b
    | none => c.point.map (·.below)

/-- The merged component: the newest decision's kind; `existsBelow` false under the merged fragment and otherwise
the oldest point's; no point for a delete with nothing below; a fragment wherever the run had one. -/
def merged (cs : List Slot) : Slot :=
  let isSet := (decision cs).getD false
  let eb := if covered cs then false else (oldestBelow cs).getD false
  ⟨if isSet || eb then some ⟨isSet, eb⟩ else none, covered cs⟩

theorem final_eq_decision (base : Bool) : ∀ cs, final base cs = (decision cs).getD base
  | [] => rfl
  | c :: cs => by
    simp only [final, decision, after]
    cases c.point with
    | some p => rfl
    | none => cases c.tomb <;> simp [final_eq_decision base cs]

/-- Without fragments, every point's `existsBelow` is the state below the run. -/
theorem oldestBelow_uncovered (base : Bool) : ∀ cs, covered cs = false → Consistent base cs →
    ∀ b, oldestBelow cs = some b → b = base
  | [], _, _, _, h => by simp [oldestBelow] at h
  | c :: cs, hcov, ⟨hc, hcs⟩, b, h => by
    simp only [covered, List.any_cons, Bool.or_eq_false_iff] at hcov
    have hcs' : covered cs = false := hcov.2
    simp only [oldestBelow] at h
    cases ho : oldestBelow cs with
    | some b' => rw [ho] at h; cases h; exact oldestBelow_uncovered base cs hcs' hcs b ho
    | none =>
      rw [ho] at h
      cases hp : c.point with
      | none => rw [hp] at h; cases h
      | some p =>
        rw [hp] at h; simp at h; subst h
        rw [hc p hp, hcov.1]
        simp only [final_eq_decision]
        rw [decision_none_of cs hcs' ho]; rfl
where
  decision_none_of : ∀ cs, covered cs = false → oldestBelow cs = none → decision cs = none
    | [], _, _ => rfl
    | c :: cs, hcov, ho => by
      simp only [covered, List.any_cons, Bool.or_eq_false_iff] at hcov
      simp only [oldestBelow] at ho
      cases ho' : oldestBelow cs with
      | some _ => rw [ho'] at ho; cases ho
      | none =>
        rw [ho'] at ho
        cases hp : c.point with
        | some _ => rw [hp] at ho; cases ho
        | none => simp [decision, hp, hcov.1, decision_none_of cs hcov.2 ho']

/-- Without fragments, only a point decides, so some point exists. -/
theorem oldestBelow_of_decision : ∀ cs b, decision cs = some b → covered cs = false → oldestBelow cs ≠ none
  | [], _, h, _ => by simp [decision] at h
  | c :: cs, b, h, hcov => by
    simp only [covered, List.any_cons, Bool.or_eq_false_iff] at hcov
    simp only [decision] at h
    simp only [oldestBelow]
    cases ho : oldestBelow cs with
    | some _ => simp
    | none =>
      cases hp : c.point with
      | some _ => simp
      | none =>
        rw [hp, hcov.1] at h
        exact absurd ho (oldestBelow_of_decision cs b h hcov.2)

theorem decision_none : ∀ cs, decision cs = none → covered cs = false ∧ oldestBelow cs = none
  | [], _ => ⟨rfl, rfl⟩
  | c :: cs, hd => by
    simp only [decision] at hd
    cases hp : c.point with
    | some _ => rw [hp] at hd; cases hd
    | none =>
      rw [hp] at hd
      cases ht : c.tomb
      · rw [ht] at hd
        obtain ⟨h1, h2⟩ := decision_none cs hd
        exact ⟨by simp only [covered, List.any_cons, ht, Bool.false_or]; exact h1, by simp [oldestBelow, h2, hp]⟩
      · rw [ht] at hd; cases hd

/-- The merged slot from the run's newest decision `d`, oldest `existsBelow` `o` and coverage `k`. -/
theorem merged_core (base k : Bool) (d o : Option Bool)
    (F1 : k = false → o = none ∨ o = some base) (F2 : ∀ b, d = some b → k = false → o ≠ none)
    (F3 : d = none → k = false ∧ o = none) :
    (∀ p, (if ((d.getD false) || (if k then false else o.getD false)) then
        some (⟨d.getD false, if k then false else o.getD false⟩ : Point) else none) = some p →
      p.below = (if k then false else base)) ∧
    after base ⟨if ((d.getD false) || (if k then false else o.getD false)) then
        some ⟨d.getD false, if k then false else o.getD false⟩ else none, k⟩ = d.getD base := by
  cases k <;> rcases d with _ | ⟨_ | _⟩ <;> rcases o with _ | ⟨_ | _⟩ <;> cases base <;> simp_all [after]

theorem uncovered_below (base : Bool) (cs : List Slot) (h : Consistent base cs) :
    covered cs = false → oldestBelow cs = none ∨ oldestBelow cs = some base := by
  intro hk
  cases ho : oldestBelow cs with
  | none => exact Or.inl rfl
  | some b => exact Or.inr (by rw [oldestBelow_uncovered base cs hk h b ho])

theorem merge_consistent (base : Bool) (cs : List Slot) (h : Consistent base cs) : Consistent base [merged cs] :=
  ⟨(merged_core base (covered cs) (decision cs) (oldestBelow cs) (uncovered_below base cs h)
    (oldestBelow_of_decision cs) (decision_none cs)).1, trivial⟩

theorem merge_final (base : Bool) (cs : List Slot) (h : Consistent base cs) :
    final base [merged cs] = final base cs := by
  rw [final_eq_decision base cs]
  exact (merged_core base (covered cs) (decision cs) (oldestBelow cs) (uncovered_below base cs h)
    (oldestBelow_of_decision cs) (decision_none cs)).2

theorem merge_total (base : Bool) (cs : List Slot) (h : Consistent base cs) :
    total base [merged cs] = total base cs := by
  rw [total_telescopes base [merged cs] (merge_consistent base cs h), total_telescopes base cs h,
    merge_final base cs h]

end Lsm.Counts
