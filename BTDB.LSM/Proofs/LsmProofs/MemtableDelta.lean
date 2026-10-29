/-!
# A memtable's delta before a key from one path

A model of `Memtable.PointDeltaBefore` (`../Engine/Memtable.cs`). The memtable is a treap; each node keeps the sum of
the deltas of its subtree (`DeltaSum`, maintained by `Update`). `PointDeltaBefore` walks down from the root: at a node
below the key it adds the left subtree's sum and the node's delta and goes right, otherwise it goes left. Keys are
naturals in key order; balancing priorities do not matter here.

* `delta_before_correct`: for a search tree whose nodes keep their subtree sums, the walk returns the sum of the deltas
  of all keys below the key.
-/

namespace Lsm.MemtableDelta

inductive Tree where
  | leaf
  | node (left : Tree) (key : Nat) (delta : Int) (sum : Int) (right : Tree)

def total : Tree → Int
  | .leaf => 0
  | .node l _ d _ r => total l + d + total r

/-- `Sum(node)`: the stored subtree sum, 0 for no node. -/
def stored : Tree → Int
  | .leaf => 0
  | .node _ _ _ s _ => s

/-- Every node keeps the sum of its subtree. -/
def Sums : Tree → Prop
  | .leaf => True
  | .node l _ d s r => s = total l + d + total r ∧ Sums l ∧ Sums r

def AllKeys (p : Nat → Prop) : Tree → Prop
  | .leaf => True
  | .node l x _ _ r => p x ∧ AllKeys p l ∧ AllKeys p r

def Search : Tree → Prop
  | .leaf => True
  | .node l x _ _ r => AllKeys (· < x) l ∧ AllKeys (x < ·) r ∧ Search l ∧ Search r

/-- The sum of the deltas of keys below `k`. -/
def below (k : Nat) : Tree → Int
  | .leaf => 0
  | .node l x d _ r => below k l + (if x < k then d else 0) + below k r

def walk (k : Nat) : Tree → Int
  | .leaf => 0
  | .node l x d _ r => if x < k then stored l + d + walk k r else walk k l

theorem stored_total (t : Tree) (h : Sums t) : stored t = total t := by
  cases t with
  | leaf => rfl
  | node l x d s r => exact h.1

theorem all_keys_mono (p q : Nat → Prop) (hpq : ∀ x, p x → q x) (t : Tree) (h : AllKeys p t) : AllKeys q t := by
  induction t with
  | leaf => trivial
  | node l x d s r ihl ihr => exact ⟨hpq x h.1, ihl h.2.1, ihr h.2.2⟩

theorem below_all (k : Nat) (t : Tree) (h : AllKeys (· < k) t) : below k t = total t := by
  induction t with
  | leaf => rfl
  | node l x d s r ihl ihr =>
    simp only [below, total, ihl h.2.1, ihr h.2.2, show x < k from h.1, ite_true]

theorem below_none (k : Nat) (t : Tree) (h : AllKeys (k ≤ ·) t) : below k t = 0 := by
  induction t with
  | leaf => rfl
  | node l x d s r ihl ihr =>
    have hx : ¬ x < k := Nat.not_lt.2 h.1
    simp only [below, ihl h.2.1, ihr h.2.2, hx, ite_false]
    omega

theorem delta_before_correct (k : Nat) (t : Tree) (hs : Sums t) (hsearch : Search t) : walk k t = below k t := by
  induction t with
  | leaf => rfl
  | node l x d s r ihl ihr =>
    obtain ⟨_, hsl, hsr⟩ := hs
    obtain ⟨hl, hr, hsearchl, hsearchr⟩ := hsearch
    simp only [walk, below]
    by_cases hx : x < k
    · simp only [hx, ite_true, ihr hsr hsearchr, stored_total l hsl]
      rw [below_all k l (all_keys_mono _ _ (fun y hy => Nat.lt_trans hy hx) l hl)]
    · simp only [hx, ite_false, ihl hsl hsearchl]
      rw [below_none k r (all_keys_mono _ _ (fun y hy => by omega) r hr)]
      omega

end Lsm.MemtableDelta
