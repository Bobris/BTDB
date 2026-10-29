/-!
# A Bloom filter has no false negatives

A model of `BloomFilter` (`../Format/BloomFilter.cs`). `Build` adds the hash of every key of a table; adding a hash sets
the bits `(h1 + i · h2) % bitCount` for `i < hashCount`, where `h1` and `h2` are its low and high halves.
`MayContain` hashes the key the same way and checks the same bits. The C# computes `h1 + i · h2` in 64 bits from
32-bit halves with `i < 30`, which cannot overflow, so natural numbers model it exactly. Bits are only ever set, so a later key cannot clear
the bits of an earlier one.

* `no_false_negative`: every hash added to the filter passes `MayContain`, whatever else was added.
-/

namespace Lsm.Bloom

/-- The bit positions of a hash, `h1 + i · h2` modulo the bit count, for `i < k`. -/
def positions (k bitCount h1 h2 : Nat) : List Nat := (List.range k).map fun i => (h1 + i * h2) % bitCount

def add (k bitCount : Nat) (bits : Nat → Bool) (h : Nat × Nat) : Nat → Bool :=
  fun b => bits b || (positions k bitCount h.1 h.2).contains b

def build (k bitCount : Nat) (hashes : List (Nat × Nat)) : Nat → Bool :=
  hashes.foldl (add k bitCount) (fun _ => false)

def mayContain (k bitCount : Nat) (bits : Nat → Bool) (h : Nat × Nat) : Bool :=
  (positions k bitCount h.1 h.2).all bits

theorem add_keeps (k bitCount : Nat) (bits : Nat → Bool) (h : Nat × Nat) (b : Nat) (hb : bits b = true) :
    add k bitCount bits h b = true := by
  simp [add, hb]

theorem foldl_keeps (k bitCount : Nat) (hashes : List (Nat × Nat)) (bits : Nat → Bool) (b : Nat)
    (hb : bits b = true) : hashes.foldl (add k bitCount) bits b = true := by
  induction hashes generalizing bits with
  | nil => exact hb
  | cons h hs ih => exact ih _ (add_keeps k bitCount bits h b hb)

theorem add_sets (k bitCount : Nat) (bits : Nat → Bool) (h : Nat × Nat) :
    mayContain k bitCount (add k bitCount bits h) h = true := by
  simp only [mayContain, List.all_eq_true]
  intro b hb
  simp [add, hb]

theorem no_false_negative (k bitCount : Nat) (hashes : List (Nat × Nat)) (h : Nat × Nat) (hh : h ∈ hashes) :
    mayContain k bitCount (build k bitCount hashes) h = true := by
  unfold build
  obtain ⟨pre, post, rfl⟩ := List.append_of_mem hh
  rw [List.foldl_append, List.foldl_cons]
  simp only [mayContain, List.all_eq_true]
  intro b hb
  apply foldl_keeps
  simp [add, hb]

end Lsm.Bloom
