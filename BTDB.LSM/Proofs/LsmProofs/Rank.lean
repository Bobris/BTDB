/-!
# Ranks over a stack of components

A model of `LsmView.RankFrom` and `CountFrom` (ImplementationPlan.md, section 6.3). Keys are naturals in key order.
A component holds point entries (set or deleted) and range-tombstone fragments `[start, end)`, sorted and disjoint as
`TombstoneSet` keeps them. Over the state below it, a key's point decides the key; without a point, a fragment hides
it; otherwise the state below shows through. As a consistent component stores them, a point's delta is
`[set] − [exists below]` with the component's own fragments applied below (`LsmEntry.Delta`), and a fragment's
`CoveredBelow` counts the keys of the state below inside it.

`find` is what `TombstoneSet.Find` returns for a key: the sum of `CoveredBelow` of the fragments ending at or before
the key, and the start of the fragment containing the key, else the key itself. Its binary search is not modelled.

* `rank_component`: one component's step of `RankFrom` is exact: the live keys below `x` are the point deltas below
  `x`, less the covered keys of earlier fragments, plus the rank in the state below at the fragment start.
* `rank_correct`: `RankFrom` over a stack is the number of live keys below `x`.
* `count_correct`: `CountFrom`, the sum of `DeltaSum − CoveredSum`, is the number of live keys.
-/

namespace Lsm.Rank

/-- Number of keys below `x` satisfying `p`. -/
def cnt (p : Nat → Bool) : Nat → Nat
  | 0 => 0
  | x + 1 => cnt p x + (p x).toNat

/-- Sum of `d` over keys below `x`. -/
def isum (d : Nat → Int) : Nat → Int
  | 0 => 0
  | x + 1 => isum d x + d x

structure Comp where
  point : Nat → Option Bool
  frags : List (Nat × Nat)

def inFrag (f : Nat × Nat) (k : Nat) : Bool := decide (f.1 ≤ k ∧ k < f.2)

def covers (fs : List (Nat × Nat)) (k : Nat) : Bool := fs.any (fun f => inFrag f k)

/-- At one key: uncovered by `f :: fs`, plus inside `f`, is uncovered by `fs`, as `f` is outside `fs`. -/
theorem key_split (a c b : Bool) (h : a = true → c = false) :
    (!(a || c) && b).toNat + (a && b).toNat = (!c && b).toNat := by
  cases a <;> cases b <;> cases c <;> simp_all

/-- Fragments are nonempty, sorted and disjoint. -/
def Sorted : List (Nat × Nat) → Prop
  | [] => True
  | [f] => f.1 < f.2
  | f :: g :: fs => f.1 < f.2 ∧ f.2 ≤ g.1 ∧ Sorted (g :: fs)

/-- The key exists below the component with its own fragments applied (`LsmEntry.ExistsBelow`). -/
def existsBelow (c : Comp) (B : Nat → Bool) (k : Nat) : Bool := !covers c.frags k && B k

def apply (c : Comp) (B : Nat → Bool) (k : Nat) : Bool :=
  match c.point k with
  | some b => b
  | none => existsBelow c B k

def delta (c : Comp) (B : Nat → Bool) (k : Nat) : Int :=
  match c.point k with
  | some b => (if b then 1 else 0) - (if existsBelow c B k then 1 else 0)
  | none => 0

def coveredBelow (B : Nat → Bool) (f : Nat × Nat) : Int := (cnt B f.2 : Int) - cnt B f.1

/-- `TombstoneSet.Find(x, out coveredBefore)` and the key `RankFrom` continues with. -/
def find (B : Nat → Bool) : List (Nat × Nat) → Nat → Int × Nat
  | [], x => (0, x)
  | f :: fs, x =>
    if x < f.1 then (0, x)
    else if x < f.2 then (0, f.1)
    else ((find B fs x).1 + coveredBelow B f, (find B fs x).2)

/-- The state of a stack, newest first, over nothing. -/
def state : List Comp → Nat → Bool
  | [] => fun _ => false
  | c :: cs => apply c (state cs)

/-- `LsmView.RankFrom`. -/
def rankFrom : List Comp → Nat → Int
  | [], _ => 0
  | c :: cs, x =>
    isum (delta c (state cs)) x - (find (state cs) c.frags x).1 + rankFrom cs (find (state cs) c.frags x).2

theorem cnt_congr {p q : Nat → Bool} : ∀ x, (∀ k, k < x → p k = q k) → cnt p x = cnt q x
  | 0, _ => rfl
  | x + 1, h => by
    simp only [cnt, cnt_congr x (fun k hk => h k (by omega)), h x (by omega)]

/-- Each key contributes its delta plus whether it exists below. -/
theorem apply_split (c : Comp) (B : Nat → Bool) :
    ∀ x, (cnt (apply c B) x : Int) = isum (delta c B) x + cnt (existsBelow c B) x
  | 0 => rfl
  | x + 1 => by
    have ih := apply_split c B x
    simp only [cnt, isum]
    cases hp : c.point x with
    | none => simp only [apply, delta, hp]; cases existsBelow c B x <;> simp <;> omega
    | some b => simp only [apply, delta, hp]; cases b <;> cases existsBelow c B x <;> simp <;> omega

/-- A run of keys failing `p` adds nothing. -/
theorem cnt_flat (p : Nat → Bool) (a : Nat) :
    ∀ y, a ≤ y → (∀ k, a ≤ k → k < y → p k = false) → cnt p y = cnt p a
  | 0, h, _ => by rw [show a = 0 by omega]
  | y + 1, h, hp => by
    by_cases hy : y + 1 = a
    · rw [hy]
    · simp only [cnt, hp y (by omega) (by omega), Bool.toNat_false, Nat.add_zero]
      exact cnt_flat p a y (by omega) (fun k h1 h2 => hp k h1 (by omega))

/-- Below the start of `[a, b)`, no key of `B` is inside it. -/
theorem frag_below (B : Nat → Bool) (a b y : Nat) (h : y ≤ a) : cnt (fun k => inFrag (a, b) k && B k) y = 0 :=
  cnt_flat _ 0 y (by omega) (fun k _ hk => by simp [inFrag, show ¬ (a ≤ k ∧ k < b) by omega])

/-- Inside `[a, b)`, the keys of `B` inside it are those of `B` from `a`. -/
theorem frag_inside (B : Nat → Bool) (a b : Nat) :
    ∀ y, a ≤ y → y ≤ b → cnt (fun k => inFrag (a, b) k && B k) y + cnt B a = cnt B y
  | 0, h, _ => by rw [show a = 0 by omega]; rfl
  | y + 1, h1, h2 => by
    by_cases hy : y + 1 = a
    · rw [frag_below B a b (y + 1) (by omega), hy]; omega
    · have ih := frag_inside B a b y (by omega) (by omega)
      simp only [cnt]
      rw [show inFrag (a, b) y = true from decide_eq_true ⟨by omega, by omega⟩, Bool.true_and]
      omega

/-- Past the end of `[a, b)`, no more keys are inside it. -/
theorem frag_above (B : Nat → Bool) (a b y : Nat) (h : b ≤ y) :
    cnt (fun k => inFrag (a, b) k && B k) y = cnt (fun k => inFrag (a, b) k && B k) b :=
  cnt_flat _ b y h (fun k hk _ => by simp [inFrag, show ¬ (a ≤ k ∧ k < b) by omega])

/-- Past its end, the keys of `B` inside `[a, b)` are its `CoveredBelow`. -/
theorem cnt_frag (B : Nat → Bool) (a b x : Nat) (hab : a ≤ b) (hx : b ≤ x) :
    (cnt (fun k => inFrag (a, b) k && B k) x : Int) = cnt B b - cnt B a := by
  rw [frag_above B a b x hx]
  have := frag_inside B a b b hab (Nat.le_refl b)
  omega

theorem sorted_tail {f : Nat × Nat} {fs : List (Nat × Nat)} (h : Sorted (f :: fs)) : Sorted fs := by
  cases fs with
  | nil => trivial
  | cons g gs => exact h.2.2

theorem sorted_head {f : Nat × Nat} {fs : List (Nat × Nat)} (h : Sorted (f :: fs)) : f.1 < f.2 := by
  cases fs with
  | nil => exact h
  | cons g gs => exact h.1

/-- No key below the end of the head is covered by a later fragment. -/
theorem tail_above : ∀ {f : Nat × Nat} {fs : List (Nat × Nat)}, Sorted (f :: fs) → ∀ k, k < f.2 → covers fs k = false
  | _, [], _, _, _ => rfl
  | f, g :: gs, h, k, hk => by
    have hg := sorted_head (sorted_tail h)
    have rest := tail_above (sorted_tail h) k (by have := h.2.1; omega)
    simp only [covers, List.any_cons, Bool.or_eq_false_iff] at rest ⊢
    refine ⟨?_, rest⟩
    simp only [inFrag, decide_eq_false_iff_not]
    have := h.2.1
    omega

theorem covers_cons (f : Nat × Nat) (fs : List (Nat × Nat)) (k : Nat) :
    covers (f :: fs) k = (inFrag f k || covers fs k) := by
  simp [covers]

/-- One component's step of `RankFrom` against the state below. -/
theorem uncovered_count (B : Nat → Bool) :
    ∀ (fs : List (Nat × Nat)), Sorted fs → ∀ x,
      (cnt (fun k => !covers fs k && B k) x : Int) = cnt B (find B fs x).2 - (find B fs x).1
  | [], _, x => by simp [covers, find]
  | f :: fs, h, x => by
    have hf := sorted_head h
    have above := tail_above h
    by_cases h1 : x < f.1
    · simp only [find, h1, ite_true]
      rw [cnt_congr x (q := B) (fun k hk => by
        rw [covers_cons, above k (by omega)]; simp [inFrag, show ¬ (f.1 ≤ k ∧ k < f.2) by omega])]
      simp
    · by_cases h2 : x < f.2
      · simp only [find, h1, h2, ite_false, ite_true]
        have e1 : cnt (fun k => !covers (f :: fs) k && B k) x = cnt (fun k => !covers (f :: fs) k && B k) f.1 :=
          cnt_flat _ f.1 x (by omega) (fun k hk1 hk2 => by
            rw [covers_cons]; simp [inFrag, show f.1 ≤ k ∧ k < f.2 from ⟨hk1, by omega⟩])
        rw [e1, cnt_congr f.1 (q := B) (fun k hk => by
          rw [covers_cons, above k (by omega)]; simp [inFrag, show ¬ (f.1 ≤ k ∧ k < f.2) by omega])]
        simp
      · simp only [find, h1, h2, ite_false]
        have ih := uncovered_count B fs (sorted_tail h) x
        -- Keys of `f` are covered by `f` alone; elsewhere the cover is that of `fs`.
        have split : ∀ y, (cnt (fun k => !covers fs k && B k) y : Int) =
            cnt (fun k => !covers (f :: fs) k && B k) y + cnt (fun k => inFrag f k && B k) y := by
          intro y
          induction y with
          | zero => rfl
          | succ y ihy =>
            simp only [cnt]
            rw [covers_cons f fs y]
            have := key_split (inFrag f y) (covers fs y) (B y) (fun h => above y (of_decide_eq_true h).2)
            omega
        have hi := cnt_frag B f.1 f.2 x (by omega) (by omega)
        rw [show ((f.1, f.2) : Nat × Nat) = f from rfl] at hi
        have hs := split x
        rw [ih] at hs
        unfold coveredBelow
        omega

theorem rank_component (c : Comp) (B : Nat → Bool) (h : Sorted c.frags) (x : Nat) :
    (cnt (apply c B) x : Int) = isum (delta c B) x - (find B c.frags x).1 + cnt B (find B c.frags x).2 := by
  rw [apply_split]
  have := uncovered_count B c.frags h x
  unfold existsBelow
  omega

theorem rank_correct : ∀ (cs : List Comp), (∀ c ∈ cs, Sorted c.frags) → ∀ x, rankFrom cs x = cnt (state cs) x
  | [], _, x => by
    simp only [rankFrom, state]
    induction x with
    | zero => rfl
    | succ y ih => simp [cnt, ← ih]
  | c :: cs, h, x => by
    simp only [rankFrom, state]
    rw [rank_component c (state cs) (h c List.mem_cons_self) x,
      rank_correct cs (fun c' hc => h c' (List.mem_cons_of_mem c hc))]

/-- `TombstoneSet.CoveredSum`. -/
def coveredSum (B : Nat → Bool) (fs : List (Nat × Nat)) : Int := (fs.map (coveredBelow B)).sum

/-- `LsmView.CountFrom`: `DeltaSum − CoveredSum` per component, with every key and fragment below `n`. -/
def countFrom (n : Nat) : List Comp → Int
  | [] => 0
  | c :: cs => isum (delta c (state cs)) n - coveredSum (state cs) c.frags + countFrom n cs

theorem find_beyond (B : Nat → Bool) :
    ∀ (fs : List (Nat × Nat)) (n : Nat), Sorted fs → (∀ f ∈ fs, f.2 ≤ n) → find B fs n = (coveredSum B fs, n)
  | [], _, _, _ => by simp [find, coveredSum]
  | f :: fs, n, hs, h => by
    have hf := h f List.mem_cons_self
    have hlt := sorted_head hs
    have rest := find_beyond B fs n (sorted_tail hs) (fun g hg => h g (List.mem_cons_of_mem f hg))
    simp only [find, show ¬ n < f.1 by omega, show ¬ n < f.2 by omega, ite_false, rest, coveredSum,
      List.map_cons, List.sum_cons, Prod.mk.injEq, and_true]
    omega

theorem count_correct (n : Nat) : ∀ (cs : List Comp), (∀ c ∈ cs, Sorted c.frags ∧ ∀ f ∈ c.frags, f.2 ≤ n) →
    countFrom n cs = cnt (state cs) n := by
  intro cs h
  rw [← rank_correct cs (fun c hc => (h c hc).1) n]
  induction cs with
  | nil => rfl
  | cons c cs ih =>
    have hc := h c List.mem_cons_self
    simp only [countFrom, rankFrom, find_beyond (state cs) c.frags n hc.1 hc.2]
    rw [ih (fun c' hc' => h c' (List.mem_cons_of_mem c hc'))]

end Lsm.Rank
