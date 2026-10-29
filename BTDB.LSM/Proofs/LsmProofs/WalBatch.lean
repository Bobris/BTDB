/-!
# WAL upload batches

A model of how `LsmKeyValueDB.UploadWalAsync` (`../LsmKeyValueDB.Wal.cs`) picks the pending records of one upload:

```csharp
while (count < _walPending.Count && (count == 0 || bytes + _walPending[count].Payload.Length <= limit))
    bytes += _walPending[count++].Payload.Length;
```

Records are given by their payload lengths, oldest first.

* `batch_prefix`: the batch is the oldest records, so uploads keep history in order.
* `batch_nonempty`: with a record pending the batch holds at least one, so an upload always makes progress, even for
  a record larger than the limit.
* `batch_within`: the batch fits the limit, or it is that single large record.
* `batch_maximal`: the next record, if any, would not fit, so the batch is as large as the limit allows.
-/

namespace Lsm.WalBatch

/-- Records taken after `bytes` were taken; `first` while none was. -/
def go (limit bytes : Nat) (first : Bool) : List Nat → List Nat
  | [] => []
  | x :: xs => if first || bytes + x ≤ limit then x :: go limit (bytes + x) false xs else []

def batch (limit : Nat) (l : List Nat) : List Nat := go limit 0 true l

theorem go_prefix (limit bytes : Nat) (first : Bool) (l : List Nat) : ∃ rest, go limit bytes first l ++ rest = l := by
  induction l generalizing bytes first with
  | nil => exact ⟨[], rfl⟩
  | cons x xs ih =>
    simp only [go]
    split
    · obtain ⟨rest, h⟩ := ih (bytes + x) false
      exact ⟨rest, by simp [h]⟩
    · exact ⟨x :: xs, rfl⟩

theorem batch_prefix (limit : Nat) (l : List Nat) : ∃ rest, batch limit l ++ rest = l := go_prefix _ _ _ _

theorem batch_nonempty (limit : Nat) (l : List Nat) (h : l ≠ []) : batch limit l ≠ [] := by
  cases l with
  | nil => exact absurd rfl h
  | cons x xs => simp [batch, go]

theorem go_within (limit bytes : Nat) (l : List Nat) (h : bytes ≤ limit) :
    bytes + (go limit bytes false l).sum ≤ limit := by
  induction l generalizing bytes with
  | nil => simpa [go]
  | cons x xs ih =>
    simp only [go, Bool.false_or]
    split
    · next hx =>
      have := ih (bytes + x) (by simpa using hx)
      simp only [List.sum_cons]
      omega
    · simpa

theorem go_over (limit bytes : Nat) (l : List Nat) (h : limit < bytes) : go limit bytes false l = [] := by
  cases l with
  | nil => rfl
  | cons x xs =>
    simp only [go, Bool.false_or]
    split
    · next hx =>
      simp at hx
      omega
    · rfl

theorem batch_within (limit : Nat) (l : List Nat) :
    (batch limit l).sum ≤ limit ∨ (batch limit l).length = 1 := by
  cases l with
  | nil => simp [batch, go]
  | cons x xs =>
    simp only [batch, go, Bool.true_or, ↓reduceIte, Nat.zero_add, List.sum_cons, List.length_cons]
    by_cases hx : x ≤ limit
    · exact Or.inl (go_within limit x xs hx)
    · right
      rw [go_over limit x xs (by omega)]
      rfl

theorem go_maximal (limit bytes : Nat) (l : List Nat) (h : (go limit bytes false l).length < l.length) :
    limit < bytes + (go limit bytes false l).sum + l[(go limit bytes false l).length] := by
  induction l generalizing bytes with
  | nil => simp at h
  | cons x xs ih =>
    by_cases hx : bytes + x ≤ limit
    · simp only [go, hx, decide_true, Bool.or_true, ↓reduceIte, List.length_cons, List.sum_cons,
        List.getElem_cons_succ] at h ⊢
      have := ih (bytes + x) (by omega)
      omega
    · simp only [go, hx, decide_false, Bool.or_false, Bool.false_eq_true, ↓reduceIte, List.length_nil,
        List.sum_nil, List.getElem_cons_zero]
      omega

theorem batch_maximal (limit : Nat) (l : List Nat) (h : (batch limit l).length < l.length) :
    limit < (batch limit l).sum + l[(batch limit l).length] := by
  cases l with
  | nil => simp at h
  | cons x xs =>
    simp only [batch, go, Bool.true_or, ↓reduceIte, Nat.zero_add, List.length_cons, List.sum_cons,
      List.getElem_cons_succ] at h ⊢
    have := go_maximal limit x xs (by omega)
    omega

end Lsm.WalBatch
