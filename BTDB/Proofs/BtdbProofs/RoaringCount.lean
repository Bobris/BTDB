/-!
# A roaring bitmap's count stays the number of its values

`ODBRoaringBitmap` (`../ODBLayer/ODBRoaringBitmap.cs`) keeps its count in a key of its own and caches it. Applying a
page command writes values without a count, as `RoaringBitmap.BuildAsync` does across transactions until its count
command comes. Before the 2026-10-29 fix `Count` read a missing count key as 0 and a page command kept the cached
count, so setting a bit then stored a wrong count; now `Count` counts the pages when no count is stored, and a page
command forgets the cached count. The values are a duplicate free list, `stored` the count key and `cached` the
cached count.

* `fixed_count_correct`: from any state whose known counts are right, any operations keep them right, and `Count`
  returns the number of values.
* `previous_wrong_count`: before the fix, applying pages of 2 values without a count and setting a third bit
  cached a count of 1.
-/

namespace Btdb.RoaringCount

structure Bitmap where
  values : List Nat
  stored : Option Nat
  cached : Option Nat

inductive Op where
  /-- Page commands replacing the values, without a count. -/
  | applyPages (values : List Nat) (nodup : values.Nodup)
  | set (value : Nat)
  | count
  | flush

/-- `Count` and the state after it. -/
def count (fixed : Bool) (b : Bitmap) : Nat × Bitmap :=
  match b.cached with
  | some c => (c, b)
  | none =>
    let c := match b.stored with
      | some s => s
      | none => if fixed then b.values.length else 0
    (c, { b with cached := some c })

def step (fixed : Bool) (b : Bitmap) : Op → Bitmap
  | .applyPages values _ => { b with values := values, stored := none, cached := if fixed then none else b.cached }
  | .set v =>
    if v ∈ b.values then b
    else
      let (c, b) := count fixed b
      { b with values := v :: b.values, cached := some (c + 1) }
  | .count => (count fixed b).2
  | .flush =>
    -- Flush counts the pages when no count is known, then stores the count.
    let b := if b.cached.isNone && b.stored.isNone then { b with cached := some b.values.length } else b
    { b with stored := b.cached.or b.stored }

/-- A cached count is the number of values, and so is a stored one while none is cached: `Set` updates the cache, and
the stored count is updated by the next `Flush`. -/
def Inv (b : Bitmap) : Prop :=
  b.values.Nodup ∧ (∀ c, b.cached = some c → c = b.values.length) ∧
    (b.cached = none → ∀ c, b.stored = some c → c = b.values.length)

theorem count_correct (b : Bitmap) (h : Inv b) : (count true b).1 = b.values.length ∧ Inv (count true b).2 := by
  obtain ⟨values, stored, cached⟩ := b
  obtain ⟨hn, hc, hs⟩ := h
  simp only at hn hc hs
  cases cached with
  | some x => exact ⟨hc x rfl, hn, hc, hs⟩
  | none =>
    cases stored with
    | some y =>
      have hy := hs rfl y rfl
      exact ⟨hy, hn, fun c h => (Option.some.inj h) ▸ hy, fun h => absurd h (by simp [count])⟩
    | none =>
      exact ⟨rfl, hn, fun c h => (Option.some.inj h).symm, fun h => absurd h (by simp [count])⟩

theorem count_values (b : Bitmap) : (count true b).2.values = b.values := by
  obtain ⟨values, stored, cached⟩ := b
  cases cached <;> rfl

theorem step_inv (b : Bitmap) (op : Op) (h : Inv b) : Inv (step true b op) := by
  cases op with
  | applyPages values nodup =>
    exact ⟨nodup, fun c h => by simp [step] at h, fun _ c h => by simp [step] at h⟩
  | set v =>
    by_cases hv : v ∈ b.values
    · simp only [step, hv, ite_true]; exact h
    · obtain ⟨hlen, -⟩ := count_correct b h
      have hvals := count_values b
      simp only [step, hv, ite_false]
      refine ⟨?_, ?_, fun h => by simp at h⟩
      · simp only [hvals]; exact List.nodup_cons.2 ⟨hv, h.1⟩
      · intro c hc; simp only [Option.some.injEq] at hc; rw [← hc, hlen, hvals]; simp
  | count => exact (count_correct b h).2
  | flush =>
    obtain ⟨values, stored, cached⟩ := b
    obtain ⟨hn, hc, hs⟩ := h
    simp only at hn hc hs
    cases cached with
    | some x =>
      refine ⟨hn, hc, fun h => by simp [step] at h⟩
    | none =>
      cases stored with
      | some y =>
        refine ⟨hn, fun c h => by simp [step] at h, fun _ c h => ?_⟩
        simp [step] at h; rw [← h]; exact hs rfl y rfl
      | none =>
        refine ⟨hn, fun c h => ?_, fun h => by simp [step] at h⟩
        simp [step] at h; exact h.symm

theorem fixed_count_correct (b : Bitmap) (h : Inv b) (ops : List Op) :
    (count true (ops.foldl (step true) b)).1 = (ops.foldl (step true) b).values.length := by
  suffices hinv : ∀ b, Inv b → Inv (ops.foldl (step true) b) from (count_correct _ (hinv b h)).1
  induction ops with
  | nil => intro b hb; exact hb
  | cons op rest ih => intro b hb; exact ih _ (step_inv b op hb)

/-- Pages holding 5 and 70000 without a count, then setting 9: before the fix the cached count was 1. -/
theorem previous_wrong_count :
    ((([Op.applyPages [5, 70000] (by decide), .set 9]).foldl (step false) ⟨[], some 0, some 0⟩).cached = some 1) ∧
    ((([Op.applyPages [5, 70000] (by decide), .set 9]).foldl (step true) ⟨[], some 0, some 0⟩).cached = some 3) := by
  decide

end Btdb.RoaringCount
