/-!
# CLOCK eviction of the native block cache

A model of `NativeClockCache.EvictIfNeeded` (`../Format/LsmBlockCache.cs`). A shard holds a ring of slots, each pinned
or not and referenced or not, and a hand. While the shard is over capacity and the hand has not passed every slot
pinned in a row, each step looks at the slot under the hand, after moving the hand to 0 when it is past the ring:

* a pinned slot is passed, and the run of pinned slots grows;
* an unpinned referenced slot loses its reference and is passed, and the run starts again;
* an unpinned unreferenced slot is freed: the last slot takes its place, the hand stays, and the run starts again.

The size of the shard only decides whether the loop goes on, so the model leaves it out.

* `stopped_all_pinned`: when the loop stops with the shard still over capacity, every slot is pinned, so no slot it
  could free is left. Before the 2026-10-29 fix a budget of twice the ring's length, used up by every step, could stop
  the loop with an unpinned slot left (`NativeClockCacheTest.EvictionPastPinnedBlocksReachesCapacity`).
* `step_decreases`: every step shortens the ring, or clears a reference, or lengthens the run, so the loop ends.
* `progress`: while the run is shorter than the ring, a step applies.
-/

namespace Lsm.ClockEviction

structure Slot where
  pinned : Bool
  referenced : Bool

structure State where
  ring : List Slot
  hand : Nat
  run : Nat

/-- The slot index under the hand once it is moved to 0 when past the ring. -/
def pos (ring : List Slot) (hand : Nat) : Nat := if hand ≥ ring.length then 0 else hand

inductive Step : State → State → Prop
  | pass {ring : List Slot} {hand run : Nat} (hrun : run < ring.length) (hp : pos ring hand < ring.length)
      (hpin : (ring[pos ring hand]'hp).pinned = true) :
      Step ⟨ring, hand, run⟩ ⟨ring, pos ring hand + 1, run + 1⟩
  | clear {ring : List Slot} {hand run : Nat} (hrun : run < ring.length) (hp : pos ring hand < ring.length)
      (hpin : (ring[pos ring hand]'hp).pinned = false) (href : (ring[pos ring hand]'hp).referenced = true) :
      Step ⟨ring, hand, run⟩ ⟨ring.set (pos ring hand) { ring[pos ring hand]'hp with referenced := false },
        pos ring hand + 1, 0⟩
  | free {ring : List Slot} {hand run : Nat} (hrun : run < ring.length) (hp : pos ring hand < ring.length)
      (hpin : (ring[pos ring hand]'hp).pinned = false) (href : (ring[pos ring hand]'hp).referenced = false) :
      Step ⟨ring, hand, run⟩ ⟨(ring.set (pos ring hand) (ring[ring.length - 1]'(by omega))).dropLast,
        pos ring hand, 0⟩

/-- Slot `i` was passed in the current run of `run` pinned slots ending right before `hand`, wrapping at `n`. -/
def Passed (n hand run i : Nat) : Prop := (i < hand ∧ hand ≤ i + run) ∨ (hand ≤ i ∧ n + hand ≤ i + run)

/-- The slots of the current run are pinned. -/
def Inv (s : State) : Prop :=
  s.run ≤ s.ring.length ∧ (s.run = 0 ∨ s.hand ≤ s.ring.length) ∧
    ∀ i (hi : i < s.ring.length), Passed s.ring.length s.hand s.run i → (s.ring[i]'hi).pinned = true

theorem inv_start (ring : List Slot) (hand : Nat) : Inv ⟨ring, hand, 0⟩ := by
  refine ⟨Nat.zero_le _, Or.inl rfl, ?_⟩
  intro i hi h
  simp only [Passed] at h hi
  omega

theorem inv_step {s t : State} (hs : Inv s) (h : Step s t) : Inv t := by
  cases h with
  | pass hrun hp hpin =>
    rename_i ring hand run
    obtain ⟨hle, hhand, hpassed⟩ := hs
    simp only at hle hhand hpassed hrun
    refine ⟨by simp only; omega, Or.inr (by simp only; omega), ?_⟩
    intro i hi hi'
    simp only at hi hi' ⊢
    by_cases heq : i = pos ring hand
    · subst heq
      exact hpin
    · apply hpassed i hi
      simp only [Passed, pos] at hi' heq ⊢
      split at hi' <;> split at heq <;> omega
  | clear => exact inv_start _ _
  | free => exact inv_start _ _

theorem stopped_all_pinned (s : State) (hs : Inv s) (hstop : s.ring.length ≤ s.run) :
    ∀ i (hi : i < s.ring.length), (s.ring[i]'hi).pinned = true := by
  obtain ⟨hrun, hhand, hpassed⟩ := hs
  intro i hi
  apply hpassed i hi
  simp only [Passed]
  omega

/-- Unpinned slots still referenced. -/
def refs : List Slot → Nat
  | [] => 0
  | s :: l => (if s.pinned = false ∧ s.referenced = true then 1 else 0) + refs l

theorem refs_clear (l : List Slot) (p : Nat) (hp : p < l.length) (hpin : (l[p]'hp).pinned = false)
    (href : (l[p]'hp).referenced = true) : refs (l.set p { l[p]'hp with referenced := false }) + 1 = refs l := by
  induction l generalizing p with
  | nil => simp at hp
  | cons x l ih =>
    cases p with
    | zero =>
      simp only [List.getElem_cons_zero] at hpin href
      simp only [List.set_cons_zero, List.getElem_cons_zero, refs, hpin, href]
      simp
      omega
    | succ p =>
      simp only [List.getElem_cons_succ] at hpin href
      simp only [List.set_cons_succ, refs, List.getElem_cons_succ]
      have := ih p (by simp at hp; omega) hpin href
      simp only at this
      omega

theorem step_decreases {s t : State} (h : Step s t) :
    t.ring.length < s.ring.length ∨
      (t.ring.length = s.ring.length ∧
        (refs t.ring < refs s.ring ∨ (refs t.ring = refs s.ring ∧ t.ring.length - t.run < s.ring.length - s.run))) := by
  cases h with
  | pass hrun => exact Or.inr ⟨rfl, Or.inr ⟨rfl, by simp only; omega⟩⟩
  | clear hrun hp hpin href =>
    refine Or.inr ⟨by simp, Or.inl ?_⟩
    have := refs_clear _ _ hp hpin href
    simp only at this ⊢
    omega
  | free hrun =>
    left
    simp only [List.length_dropLast, List.length_set]
    omega

theorem progress (s : State) (hrun : s.run < s.ring.length) : ∃ t, Step s t := by
  obtain ⟨ring, hand, run⟩ := s
  simp only at hrun
  have hp : pos ring hand < ring.length := by
    simp only [pos]
    split <;> omega
  cases hpin : (ring[pos ring hand]'hp).pinned with
  | true => exact ⟨_, Step.pass hrun hp hpin⟩
  | false =>
    cases href : (ring[pos ring hand]'hp).referenced with
    | true => exact ⟨_, Step.clear hrun hp hpin href⟩
    | false => exact ⟨_, Step.free hrun hp hpin href⟩

end Lsm.ClockEviction
