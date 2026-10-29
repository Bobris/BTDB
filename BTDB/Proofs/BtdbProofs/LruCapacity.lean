/-!
# An LRU cache never holds more items than its maximum capacity

`LruCache` and `SpanByteLruCache` (`../Collections/`) keep `count` items in an entry array of `length` slots, at most
`max`. Adding a new key takes a free slot while `count < length`, grows the array (`IncreaseCapacity(count * 2)`)
while `count < max`, and otherwise evicts the least recently used item. `IncreaseCapacity` can also be called
directly. Before the 2026-10-29 fix it grew the array beyond `max`, after which adds filled slots past `max` before
evicting; it now grows the array at most to `max`. Growing rounds to a power of two, which the model leaves out:
`max` is a power of two, so rounding never passes it.

* `fixed_within_max`: from an empty cache, any sequence of operations keeps `count ≤ max`.
* `previous_exceeds_max`: before the fix, growing beyond `max` and adding let `count` exceed it.
-/

namespace Btdb.LruCapacity

structure Cache where
  count : Nat
  length : Nat

inductive Op where
  | add | remove | clear
  | increase (n : Nat)

/-- `IncreaseCapacity`, which `clamp` limits to `max` since the fix. -/
def increase (clamp : Bool) (max : Nat) (c : Cache) (n : Nat) : Cache :=
  let n := if clamp then min n max else n
  if n ≤ c.length then c else ⟨c.count, n⟩

/-- A new key. -/
def add (clamp : Bool) (max : Nat) (c : Cache) : Cache :=
  if c.count < c.length then ⟨c.count + 1, c.length⟩
  else if c.count < max then
    let grown := increase clamp max c (c.count * 2)
    ⟨c.count + 1, grown.length⟩
  else c

def step (clamp : Bool) (max : Nat) (c : Cache) : Op → Cache
  | .add => add clamp max c
  | .remove => ⟨c.count - 1, c.length⟩
  | .clear => ⟨0, c.length⟩
  | .increase n => increase clamp max c n

def run (clamp : Bool) (max : Nat) (start : Cache) (ops : List Op) : Cache := ops.foldl (step clamp max) start

/-- The constructor allocates at least 2 slots. -/
def Inv (max : Nat) (c : Cache) : Prop := c.count ≤ c.length ∧ c.length ≤ max ∧ 2 ≤ c.length

theorem step_inv (max : Nat) (hmax : 2 ≤ max) (c : Cache) (op : Op) (h : Inv max c) :
    Inv max (step true max c op) := by
  obtain ⟨count, length⟩ := c
  obtain ⟨h1, h2, h3⟩ := h
  simp only at h1 h2 h3
  cases op with
  | add =>
    simp only [step, add, increase, ite_true]
    by_cases ha : count < length
    · simp only [ha, ite_true, Inv]; omega
    · simp only [ha, ite_false]
      by_cases hb : count < max
      · simp only [hb, ite_true, Inv]
        split <;> simp only <;> omega
      · simp only [hb, ite_false, Inv]; omega
  | remove => simp only [step, Inv]; omega
  | clear => simp only [step, Inv]; omega
  | increase n =>
    simp only [step, increase, ite_true]
    split <;> simp only [Inv] <;> omega

theorem fixed_within_max (max : Nat) (hmax : 2 ≤ max) (length : Nat) (hl : length ≤ max) (hl2 : 2 ≤ length)
    (ops : List Op) : (run true max ⟨0, length⟩ ops).count ≤ max := by
  suffices h : ∀ c, Inv max c → Inv max (ops.foldl (step true max) c) by
    have := h ⟨0, length⟩ ⟨Nat.zero_le _, hl, hl2⟩
    exact Nat.le_trans this.1 this.2.1
  induction ops with
  | nil => intro c hc; exact hc
  | cons op rest ih => intro c hc; exact ih _ (step_inv max hmax c op hc)

/-- A cache of at most 2 items grown to 4 slots holds 3 items. -/
theorem previous_exceeds_max : (run false 2 ⟨0, 2⟩ [.increase 4, .add, .add, .add]).count = 3 := by
  decide

end Btdb.LruCapacity
