/-!
# The contiguous WAL prefix replayed on open

A model of `LsmKeyValueDB.ReplayListedWalAsync` (`../LsmKeyValueDB.Recovery.cs`). The WAL chunk numbers listed at or
after the replay's next chunk `start` are sorted, and only the run of consecutive numbers from `start` is replayed:

```csharp
foreach (var number in chunkNumbers)
{
    if (number != replay.NextChunk + contiguous.Count) break;
    contiguous.Add(number);
}
```

Chunks after a gap belong to commits that were never acknowledged, so they must not be replayed.

* `take_consecutive`: the replayed chunks are `start, start + 1, …`, with no gap.
* `take_listed`: each replayed chunk was listed.
* `take_maximal`: the chunk right after the replayed run was not listed, so the scan stops exactly at the first gap and
  replays every chunk before it.
-/

namespace Lsm.WalContiguous

def take (n : Nat) : List Nat → List Nat
  | [] => []
  | x :: xs => if x = n then x :: take (n + 1) xs else []

theorem take_consecutive (n : Nat) (l : List Nat) : take n l = List.range' n (take n l).length := by
  induction l generalizing n with
  | nil => rfl
  | cons x xs ih =>
    simp only [take]
    split
    · next h =>
      subst h
      simp only [List.length_cons, List.range'_succ]
      exact congrArg (x :: ·) (ih (x + 1))
    · rfl

theorem take_listed (n : Nat) (l : List Nat) : ∀ x ∈ take n l, x ∈ l := by
  induction l generalizing n with
  | nil => simp [take]
  | cons y ys ih =>
    intro x hx
    simp only [take] at hx
    split at hx
    · rcases List.mem_cons.mp hx with h | h
      · exact h ▸ List.mem_cons_self
      · exact List.mem_cons_of_mem _ (ih (n + 1) x h)
    · simp at hx

theorem take_maximal (n : Nat) (l : List Nat) (hs : l.Pairwise (· < ·)) (hge : ∀ x ∈ l, n ≤ x) :
    n + (take n l).length ∉ l := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
    rw [List.pairwise_cons] at hs
    simp only [take]
    split
    · next h =>
      subst h
      simp only [List.length_cons]
      have hrest := ih (x + 1) hs.2 (fun y hy => hs.1 y hy)
      intro hmem
      rcases List.mem_cons.mp hmem with h | h
      · omega
      · exact hrest (by rwa [Nat.add_assoc, Nat.add_comm 1])
    · next h =>
      simp only [List.length_nil, Nat.add_zero]
      intro hmem
      rcases List.mem_cons.mp hmem with h' | h'
      · exact h h'.symm
      · have := hs.1 n h'
        have := hge x List.mem_cons_self
        omega

end Lsm.WalContiguous
