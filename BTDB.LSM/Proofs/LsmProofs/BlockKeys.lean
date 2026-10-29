/-!
# Prefix-compressed keys of a data block

A model of how `DataBlockBuilder.Add` (`../Format/DataBlock.cs`) stores keys and how `RawBlock.DecodeAt` rebuilds
them. Keys are lists of bytes. Every `restartInterval`-th entry is a restart point and stores its key whole; any other
entry stores the length of the prefix it shares with the previous key and the rest of the key. Decoding takes that many
bytes of the previous key and appends the rest, and it reports corruption when the shared length exceeds the previous
key's length.

* `decode_encode`: decoding the entries after a key from that key gives back every key, and the corruption check never
  fires.
* `decode_from_restart`: decoding from any restart point, starting from an empty key as `LowerBound` and `Entry` do,
  gives back the keys from that point on.
* `restart_entry`: the entry at a restart point stores shared length zero and the whole key, which `RestartKey` reads.
-/

namespace Lsm.BlockKeys

/-- Length of the common prefix of the previous key and the key, as the loop in `DataBlockBuilder.Add` counts it. -/
def shared : List Nat → List Nat → Nat
  | a :: as, b :: bs => if a = b then shared as bs + 1 else 0
  | _, _ => 0

/-- Entries `(shared, rest)` stored for keys `ks`, the first at index `i`, after previous key `prev`. -/
def encodeFrom (r : Nat) : Nat → List Nat → List (List Nat) → List (Nat × List Nat)
  | _, _, [] => []
  | i, prev, k :: ks =>
    let s := if i % r = 0 then 0 else shared prev k
    (s, k.drop s) :: encodeFrom r (i + 1) k ks

/-- The entries of a block holding keys `ks` with a restart every `r` entries. -/
def encode (r : Nat) (ks : List (List Nat)) : List (Nat × List Nat) := encodeFrom r 0 [] ks

/-- Keys rebuilt from entries after previous key `prev`; none when a shared length exceeds the previous key. -/
def decode : List Nat → List (Nat × List Nat) → Option (List (List Nat))
  | _, [] => some []
  | prev, (s, rest) :: es =>
    if s ≤ prev.length then (decode (prev.take s ++ rest) es).map ((prev.take s ++ rest) :: ·) else none

theorem shared_le (prev k : List Nat) : shared prev k ≤ prev.length := by
  induction prev generalizing k with
  | nil => simp [shared]
  | cons a as ih =>
    cases k with
    | nil => simp [shared]
    | cons b bs =>
      simp only [shared]
      split
      · have := ih bs
        simp only [List.length_cons]
        omega
      · omega

theorem take_shared (prev k : List Nat) : prev.take (shared prev k) = k.take (shared prev k) := by
  induction prev generalizing k with
  | nil => simp [shared]
  | cons a as ih =>
    cases k with
    | nil => simp [shared]
    | cons b bs =>
      simp only [shared]
      split
      · next h => simp [List.take_succ_cons, h, ih bs]
      · simp

theorem rebuild (prev k : List Nat) (s : Nat) (hs : s = 0 ∨ s = shared prev k) :
    s ≤ prev.length ∧ prev.take s ++ k.drop s = k := by
  rcases hs with rfl | rfl
  · simp
  · exact ⟨shared_le prev k, by rw [take_shared, List.take_append_drop]⟩

theorem decode_encode (r i : Nat) (prev : List Nat) (ks : List (List Nat)) :
    decode prev (encodeFrom r i prev ks) = some ks := by
  induction ks generalizing i prev with
  | nil => simp [encodeFrom, decode]
  | cons k ks ih =>
    have h := rebuild prev k (if i % r = 0 then 0 else shared prev k) (by split <;> simp)
    simp only [encodeFrom, decode, h.1, ↓reduceIte, h.2, ih]
    rfl

theorem decode_from (r : Nat) (j : Nat) :
    ∀ (i : Nat) (prev p : List Nat) (ks : List (List Nat)), (i + j) % r = 0 →
      decode p ((encodeFrom r i prev ks).drop j) = some (ks.drop j) := by
  induction j with
  | zero =>
    intro i prev p ks hi
    cases ks with
    | nil => simp [encodeFrom, decode]
    | cons k ks =>
      simp only [Nat.add_zero] at hi
      simp only [encodeFrom, hi, ↓reduceIte, List.drop_zero, decode, Nat.zero_le, List.take_zero, List.nil_append,
        decode_encode]
      rfl
  | succ j ih =>
    intro i prev p ks hi
    cases ks with
    | nil => simp [encodeFrom, decode]
    | cons k ks =>
      simp only [encodeFrom, List.drop_succ_cons]
      exact ih (i + 1) k p ks (by rw [Nat.add_assoc, Nat.add_comm 1 j]; exact hi)

theorem decode_from_restart (r j : Nat) (ks : List (List Nat)) (hj : j % r = 0) :
    decode [] ((encode r ks).drop j) = some (ks.drop j) :=
  decode_from r j 0 [] [] ks (by simpa using hj)

theorem restart_from (r : Nat) (j : Nat) :
    ∀ (i : Nat) (prev k : List Nat) (ks rest : List (List Nat)), (i + j) % r = 0 → ks.drop j = k :: rest →
      ∃ es, (encodeFrom r i prev ks).drop j = (0, k) :: es := by
  induction j with
  | zero =>
    intro i prev k ks rest hi hk
    simp only [List.drop_zero] at hk
    subst hk
    simp only [Nat.add_zero] at hi
    exact ⟨encodeFrom r (i + 1) k rest, by simp [encodeFrom, hi]⟩
  | succ j ih =>
    intro i prev k ks rest hi hk
    cases ks with
    | nil => simp at hk
    | cons k' ks =>
      simp only [List.drop_succ_cons] at hk
      simp only [encodeFrom, List.drop_succ_cons]
      exact ih (i + 1) k' k ks rest (by rw [Nat.add_assoc, Nat.add_comm 1 j]; exact hi) hk

theorem restart_entry (r j : Nat) (k : List Nat) (ks rest : List (List Nat)) (hj : j % r = 0)
    (hk : ks.drop j = k :: rest) : ∃ es, (encode r ks).drop j = (0, k) :: es :=
  restart_from r j 0 [] k ks rest (by simpa using hj) hk

end Lsm.BlockKeys
