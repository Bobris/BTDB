import LsmProofs.MemtableDelta

/-!
# A memtable's tree stays a search tree with exact subtree sums

A model of `Memtable.Split` and `Merge` (`../Engine/Memtable.cs`), which every insert and removal goes through. A node
rebuilt by them gets its sum recomputed (`Update`). Keys are naturals; `Split` puts the keys below the key, or at or
below it when `keyGoesLeft`, on the left. `Merge` of trees whose left keys are all below the right ones picks either
root by priority; `choose` stands for that choice, any one.

* `split_ok`: splitting a search tree with exact sums gives two such trees with the keys on the right sides.
* `merge_ok`: merging two such trees, left keys below right keys, gives such a tree, whatever the priorities.

With `MemtableDelta.delta_before_correct` this keeps `PointDeltaBefore` exact after every change.
-/

namespace Lsm.MemtableTree

open Lsm.MemtableDelta

def mk (l : Tree) (x : Nat) (d : Int) (r : Tree) : Tree := .node l x d (total l + d + total r) r

def goesLeft (k : Nat) (keyGoesLeft : Bool) (x : Nat) : Bool := x < k || (keyGoesLeft && x == k)

def split (k : Nat) (keyGoesLeft : Bool) : Tree → Tree × Tree
  | .leaf => (.leaf, .leaf)
  | .node l x d _ r =>
    if goesLeft k keyGoesLeft x then
      let (rl, rr) := split k keyGoesLeft r
      (mk l x d rl, rr)
    else
      let (ll, lr) := split k keyGoesLeft l
      (ll, mk lr x d r)

theorem goes_left_mono (k : Nat) (kgl : Bool) (x y : Nat) (hx : goesLeft k kgl x = true) (hy : y < x) :
    goesLeft k kgl y = true := by
  unfold goesLeft at *
  cases kgl <;> simp at * <;> omega

theorem goes_right_mono (k : Nat) (kgl : Bool) (x y : Nat) (hx : goesLeft k kgl x = false) (hy : x < y) :
    goesLeft k kgl y = false := by
  unfold goesLeft at *
  cases kgl <;> simp at * <;> omega

def size : Tree → Nat
  | .leaf => 0
  | .node l _ _ _ r => size l + size r + 1

def merge (choose : Tree → Tree → Bool) : Tree → Tree → Tree
  | .leaf, b => b
  | a, .leaf => a
  | .node al ax ad s ar, .node bl bx bd t br =>
    if choose (.node al ax ad s ar) (.node bl bx bd t br) then
      mk al ax ad (merge choose ar (.node bl bx bd t br))
    else
      mk (merge choose (.node al ax ad s ar) bl) bx bd br
termination_by a b => size a + size b
decreasing_by all_goals simp only [size]; omega

def Ok (t : Tree) : Prop := Sums t ∧ Search t

theorem all_keys_and (p q : Nat → Prop) (t : Tree) (hp : AllKeys p t) (hq : AllKeys q t) :
    AllKeys (fun x => p x ∧ q x) t := by
  induction t with
  | leaf => trivial
  | node l x d s r ihl ihr => exact ⟨⟨hp.1, hq.1⟩, ihl hp.2.1 hq.2.1, ihr hp.2.2 hq.2.2⟩

theorem mk_ok (l : Tree) (x : Nat) (d : Int) (r : Tree) (hl : Ok l) (hr : Ok r) (hlx : AllKeys (· < x) l)
    (hrx : AllKeys (x < ·) r) : Ok (mk l x d r) :=
  ⟨⟨rfl, hl.1, hr.1⟩, ⟨hlx, hrx, hl.2, hr.2⟩⟩

theorem total_mk (l : Tree) (x : Nat) (d : Int) (r : Tree) : total (mk l x d r) = total l + d + total r := rfl

/-- What split returns: both parts are fine, hold the keys of the input, and lie on their sides. -/
theorem split_ok (k : Nat) (keyGoesLeft : Bool) (t : Tree) (ht : Ok t) (p : Nat → Prop) (hp : AllKeys p t) :
    Ok (split k keyGoesLeft t).1 ∧ Ok (split k keyGoesLeft t).2 ∧
      AllKeys (fun x => p x ∧ goesLeft k keyGoesLeft x = true) (split k keyGoesLeft t).1 ∧
      AllKeys (fun x => p x ∧ goesLeft k keyGoesLeft x = false) (split k keyGoesLeft t).2 ∧
      total (split k keyGoesLeft t).1 + total (split k keyGoesLeft t).2 = total t := by
  induction t generalizing p with
  | leaf => exact ⟨⟨trivial, trivial⟩, ⟨trivial, trivial⟩, trivial, trivial, rfl⟩
  | node l x d s r ihl ihr =>
    obtain ⟨⟨hs, hsl, hsr⟩, ⟨hl, hr, hsearchl, hsearchr⟩⟩ := ht
    obtain ⟨hpx, hpl, hpr⟩ := hp
    simp only [split]
    split
    · next hx =>
      obtain ⟨o1, o2, o3, o4, o5⟩ := ihr ⟨hsr, hsearchr⟩ (fun y => p y ∧ x < y) (all_keys_and _ _ r hpr hr)
      refine ⟨mk_ok l x d _ ⟨hsl, hsearchl⟩ o1 hl (all_keys_mono _ _ (fun y hy => hy.1.2) _ o3), o2, ?_, ?_, ?_⟩
      · refine ⟨⟨hpx, hx⟩, ?_, all_keys_mono _ _ (fun y hy => ⟨hy.1.1, hy.2⟩) _ o3⟩
        -- Keys of the left subtree are below x, which goes left, so they go left too.
        exact all_keys_mono _ _ (fun y hy => ⟨hy.1, goes_left_mono k keyGoesLeft x y hx hy.2⟩) _
          (all_keys_and _ _ l hpl hl)
      · exact all_keys_mono _ _ (fun y hy => ⟨hy.1.1, hy.2⟩) _ o4
      · rw [total_mk]; simp only [total]; omega
    · next hx =>
      obtain ⟨o1, o2, o3, o4, o5⟩ := ihl ⟨hsl, hsearchl⟩ (fun y => p y ∧ y < x) (all_keys_and _ _ l hpl hl)
      refine ⟨o1, mk_ok _ x d r o2 ⟨hsr, hsearchr⟩ (all_keys_mono _ _ (fun y hy => hy.1.2) _ o4) hr, ?_, ?_, ?_⟩
      · exact all_keys_mono _ _ (fun y hy => ⟨hy.1.1, hy.2⟩) _ o3
      · refine ⟨⟨hpx, by simpa using hx⟩, all_keys_mono _ _ (fun y hy => ⟨hy.1.1, hy.2⟩) _ o4, ?_⟩
        -- Keys of the right subtree are above x, which goes right, so they go right too.
        have hx' : goesLeft k keyGoesLeft x = false := by simpa using hx
        exact all_keys_mono _ _ (fun y hy => ⟨hy.1, goes_right_mono k keyGoesLeft x y hx' hy.2⟩) _
          (all_keys_and _ _ r hpr hr)
      · rw [total_mk]; simp only [total]; omega

/-- Merge keeps the keys of both inputs, a property `p` of all of them, and their total. -/
theorem merge_ok (choose : Tree → Tree → Bool) (a b : Tree) (ha : Ok a) (hb : Ok b) (c : Nat)
    (hac : AllKeys (· < c) a) (hbc : AllKeys (c ≤ ·) b) (p : Nat → Prop) (hpa : AllKeys p a) (hpb : AllKeys p b) :
    Ok (merge choose a b) ∧ AllKeys p (merge choose a b) ∧ total (merge choose a b) = total a + total b := by
  induction a, b using merge.induct choose generalizing p with
  | case1 b => simp only [merge, total]; exact ⟨hb, hpb, by omega⟩
  | case2 a ha' =>
    cases a with
    | leaf => exact absurd rfl ha'
    | node al ax ad s ar =>
      simp only [merge, total]; exact ⟨ha, hpa, by omega⟩
  | case3 al ax ad s ar bl bx bd t br hch ih =>
    obtain ⟨⟨_, hsal, hsar⟩, ⟨hal, har, hsearchal, hsearchar⟩⟩ := ha
    obtain ⟨hpx, hpal, hpar⟩ := hpa
    rw [merge, ite_eq_left hch]
    have hxc : ax < c := hac.1
    obtain ⟨m1, m2, m3⟩ := ih ⟨hsar, hsearchar⟩ hb hac.2.2 hbc (fun y => p y ∧ ax < y)
      (all_keys_and _ _ ar hpar har)
      (all_keys_and _ _ _ hpb (all_keys_mono _ _ (fun y hy => by omega) _ hbc))
    refine ⟨mk_ok al ax ad _ ⟨hsal, hsearchal⟩ m1 hal (all_keys_mono _ _ (fun y hy => hy.2) _ m2),
      ⟨hpx, hpal, all_keys_mono _ _ (fun y hy => hy.1) _ m2⟩, ?_⟩
    rw [total_mk, m3]; simp only [total]; omega
  | case4 al ax ad s ar bl bx bd t br hch ih =>
    obtain ⟨⟨_, hsbl, hsbr⟩, ⟨hbl, hbr, hsearchbl, hsearchbr⟩⟩ := hb
    obtain ⟨hpx, hpbl, hpbr⟩ := hpb
    rw [merge, ite_eq_right hch]
    have hcx : c ≤ bx := hbc.1
    obtain ⟨m1, m2, m3⟩ := ih ha ⟨hsbl, hsearchbl⟩ hac hbc.2.1 (fun y => p y ∧ y < bx)
      (all_keys_and _ _ _ hpa (all_keys_mono _ _ (fun y hy => by omega) _ hac))
      (all_keys_and _ _ bl hpbl hbl)
    refine ⟨mk_ok _ bx bd br m1 ⟨hsbr, hsearchbr⟩ (all_keys_mono _ _ (fun y hy => hy.2) _ m2) hbr,
      ⟨hpx, all_keys_mono _ _ (fun y hy => hy.1) _ m2, hpbr⟩, ?_⟩
    rw [total_mk, m3]; simp only [total]; omega

end Lsm.MemtableTree
