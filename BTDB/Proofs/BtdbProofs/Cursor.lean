import BtdbProofs.KeyRange

/-!
# Finding a key within a prefix

A model of `BTreeKeyValueDBCursor.Find(key, prefixLen)`, which runs the B-tree's `Find(key)` and then restricts its
answer to the keys sharing the first `prefixLen` bytes of `key`. The contract (`IKeyValueDBCursor.Find`): the key
itself, else the greatest smaller key, else the smallest larger key, all among the keys with that prefix, else not
found. The B-tree's `Find(key)` is assumed to meet the same contract over all keys. Keys are byte lists in the order
of `KeyRange.cmp`.

* `block`: the keys with a prefix form a contiguous block.
* `previous_in_block`, `previous_outside_next_in_block`, `previous_outside_next_outside`, `previous_outside_no_next`,
  `next_in_block`, `next_outside`: each branch of the code answers what the contract requires.
-/

namespace Btdb.Cursor

open KeyRange

def lt (a b : List Nat) : Prop := cmp a b = .lt

theorem cmp_eq : ∀ a b, cmp a b = .eq → a = b
  | [], [], _ => rfl
  | [], _ :: _, h => by simp [cmp] at h
  | _ :: _, [], h => by simp [cmp] at h
  | x :: xs, y :: ys, h => by
    simp only [cmp] at h
    by_cases h1 : x < y
    · simp [h1] at h
    · by_cases h2 : y < x
      · simp [h1, h2] at h
      · simp only [h1, h2, ite_false] at h
        rw [show x = y by omega, cmp_eq xs ys h]

theorem lt_irrefl : ∀ a, ¬ lt a a
  | [], h => by simp [lt, cmp] at h
  | x :: xs, h => by simp only [lt, cmp, Nat.lt_irrefl, ite_false] at h; exact lt_irrefl xs h

theorem lt_trans : ∀ a b c, lt a b → lt b c → lt a c
  | [], [], _, h, _ => by simp [lt, cmp] at h
  | [], _ :: _, [], _, h => by simp [lt, cmp] at h
  | [], _ :: _, _ :: _, _, _ => rfl
  | _ :: _, [], _, h, _ => by simp [lt, cmp] at h
  | _ :: _, _ :: _, [], _, h => by simp [lt, cmp] at h
  | x :: xs, y :: ys, z :: zs, h1, h2 => by
    simp only [lt, cmp] at h1 h2 ⊢
    by_cases a1 : x < y
    · by_cases a2 : y < z
      · simp [show x < z by omega]
      · by_cases a3 : z < y
        · simp [a2, a3] at h2
        · simp [show x < z by omega]
    · by_cases a2 : y < x
      · simp [a1, a2] at h1
      · simp only [a1, a2, ite_false] at h1
        have exy : x = y := by omega
        subst exy
        by_cases a3 : x < z
        · simp [a3]
        · by_cases a4 : z < x
          · simp [a3, a4] at h2
          · simp only [a3, a4, ite_false] at h2 ⊢
            exact lt_trans xs ys zs h1 h2

theorem cmp_gt_swap : ∀ a b, cmp a b = .gt → cmp b a = .lt
  | [], [], h => by simp [cmp] at h
  | [], _ :: _, h => by simp [cmp] at h
  | _ :: _, [], _ => rfl
  | x :: xs, y :: ys, h => by
    simp only [cmp] at h ⊢
    by_cases h1 : x < y
    · simp [h1] at h
    · by_cases h2 : y < x
      · simp [h2]
      · simp only [h1, h2, ite_false] at h ⊢
        exact cmp_gt_swap xs ys h

/-- Two keys are equal or one is smaller. -/
theorem trichotomy (a b : List Nat) : lt a b ∨ a = b ∨ lt b a := by
  cases h : cmp a b with
  | lt => exact Or.inl h
  | eq => exact Or.inr (Or.inl (cmp_eq a b h))
  | gt => exact Or.inr (Or.inr (cmp_gt_swap a b h))

/-- The keys with a prefix form a contiguous block. -/
theorem block : ∀ (p a b c : List Nat), p <+: a → p <+: c → lt a b → lt b c → p <+: b
  | [], _, _, _, _, _, _, _ => List.nil_prefix
  | x :: p, a, b, c, ha, hc, h1, h2 => by
    obtain ⟨a', rfl⟩ := ha
    obtain ⟨c', rfl⟩ := hc
    cases b with
    | nil => simp [lt, cmp] at h1
    | cons y b' =>
      simp only [lt, List.cons_append, cmp] at h1 h2
      by_cases a1 : x < y
      · by_cases a2 : y < x
        · omega
        · simp [a1, a2] at h2
      · by_cases a2 : y < x
        · simp [a1, a2] at h1
        · simp only [a1, a2, ite_false] at h1 h2
          have := block p (p ++ a') b' (p ++ c') (List.prefix_append _ _) (List.prefix_append _ _) h1 h2
          rw [show y = x by omega]
          exact List.cons_prefix_cons.2 ⟨rfl, this⟩

/-! ## The answers of `Find`, over a set of keys `S` -/

/-- `k` is the greatest key of `S` below `key`. -/
def IsPrev (S : List Nat → Prop) (key k : List Nat) : Prop :=
  S k ∧ lt k key ∧ ∀ x, S x → lt x key → x = k ∨ lt x k

/-- `k` is the smallest key of `S` above `key`. -/
def IsNext (S : List Nat → Prop) (key k : List Nat) : Prop :=
  S k ∧ lt key k ∧ ∀ x, S x → lt key x → x = k ∨ lt k x

def NoBelow (S : List Nat → Prop) (key : List Nat) : Prop := ∀ x, S x → ¬ lt x key
def NoAbove (S : List Nat → Prop) (key : List Nat) : Prop := ∀ x, S x → ¬ lt key x

/-- The keys of `S` with prefix `p`. -/
def Block (S : List Nat → Prop) (p : List Nat) (x : List Nat) : Prop := S x ∧ p <+: x

variable {S : List Nat → Prop} {p key k n : List Nat}

/-- The B-tree found a smaller key with the prefix: it is the greatest one with the prefix too. -/
theorem previous_in_block (h : IsPrev S key k) (hk : p <+: k) : IsPrev (Block S p) key k :=
  ⟨⟨h.1, hk⟩, h.2.1, fun x hx => h.2.2 x hx.1⟩

/-- A greatest smaller key outside the block means the block has no key below `key`. -/
theorem no_below_of_previous (hp : p <+: key) (h : IsPrev S key k) (hk : ¬ p <+: k) : NoBelow (Block S p) key := by
  intro x hx hlt
  rcases h.2.2 x hx.1 hlt with rfl | hxk
  · exact hk hx.2
  · exact hk (block p x k key hx.2 hp hxk h.2.1)

/-- The B-tree's next key after `k` is above `key` as well, since `key` is not a key. -/
theorem next_above (hs : ¬ S key) (h : IsPrev S key k) (hn : IsNext S k n) : lt key n := by
  rcases trichotomy key n with h' | rfl | h'
  · exact h'
  · exact absurd hn.1 hs
  · rcases h.2.2 n hn.1 h' with rfl | h''
    · exact absurd hn.2.1 (lt_irrefl _)
    · exact absurd (lt_trans _ _ _ hn.2.1 h'') (lt_irrefl _)

/-- Previous outside the block, and the next key is in it: the answer is Next. -/
theorem previous_outside_next_in_block (hp : p <+: key) (hs : ¬ S key) (h : IsPrev S key k) (hk : ¬ p <+: k)
    (hn : IsNext S k n) (hpn : p <+: n) : NoBelow (Block S p) key ∧ IsNext (Block S p) key n := by
  refine ⟨no_below_of_previous hp h hk, ⟨hn.1, hpn⟩, next_above hs h hn, fun x hx hlt => ?_⟩
  exact hn.2.2 x hx.1 (lt_trans _ _ _ h.2.1 hlt)

/-- Previous outside the block, and the next key outside it too: not found. -/
theorem previous_outside_next_outside (hp : p <+: key) (hs : ¬ S key) (h : IsPrev S key k) (hk : ¬ p <+: k)
    (hn : IsNext S k n) (hpn : ¬ p <+: n) : NoBelow (Block S p) key ∧ NoAbove (Block S p) key := by
  refine ⟨no_below_of_previous hp h hk, fun x hx hlt => ?_⟩
  have hkn := next_above hs h hn
  rcases hn.2.2 x hx.1 (lt_trans _ _ _ h.2.1 hlt) with rfl | hnx
  · exact hpn hx.2
  · exact hpn (block p key n x hp hx.2 hkn hnx)

/-- Previous outside the block, and no key after it: not found. -/
theorem previous_outside_no_next (hp : p <+: key) (h : IsPrev S key k) (hk : ¬ p <+: k)
    (hn : ∀ x, S x → ¬ lt k x) : NoBelow (Block S p) key ∧ NoAbove (Block S p) key :=
  ⟨no_below_of_previous hp h hk, fun x hx hlt => hn x hx.1 (lt_trans _ _ _ h.2.1 hlt)⟩

/-- No smaller key at all and the next key is in the block: the answer is Next. -/
theorem next_in_block (hb : NoBelow S key) (hn : IsNext S key n) (hpn : p <+: n) :
    NoBelow (Block S p) key ∧ IsNext (Block S p) key n :=
  ⟨fun x hx => hb x hx.1, ⟨hn.1, hpn⟩, hn.2.1, fun x hx => hn.2.2 x hx.1⟩

/-- No smaller key at all and the next key outside the block: not found. -/
theorem next_outside (hp : p <+: key) (hb : NoBelow S key) (hn : IsNext S key n) (hpn : ¬ p <+: n) :
    NoBelow (Block S p) key ∧ NoAbove (Block S p) key := by
  refine ⟨fun x hx => hb x hx.1, fun x hx hlt => ?_⟩
  rcases hn.2.2 x hx.1 hlt with rfl | hnx
  · exact hpn hx.2
  · exact hpn (block p key n x hp hx.2 hn.2.1 hnx)

end Btdb.Cursor
