/-!
# A stored object is pending and cached under its oid

A model of how `ObjectDBTransaction` (`../../ODBLayer/ObjectDBTransaction.cs`) tracks the objects of a transaction:
each known object has an oid and a state (`Read`, `Dirty` or `Deleted`); the cache maps an oid to its object, and the
dirty set, written at commit, maps an oid to the object pending under it.

* `store` of a read object makes it dirty and pending; of a deleted one, since the 2026-10-29 fix, also puts it back in
  the cache, taking the oid from an object created under it meanwhile, which becomes deleted
  (`RestoreDeletedToObjCache`).
* `delete` makes an object deleted and removes its oid from the cache and the dirty set.
* `create` makes a new dirty object under a free oid, as `Singleton` does for a singleton deleted before.

Read objects stay cached here; the C# cache of a transaction with many objects holds them weakly, and `Get` reads one it
dropped again, but it holds dirty objects strongly through the dirty set.

* `reachable_inv`: an object pending under an oid is dirty with that oid and cached under it, and a dirty object is
  pending, so what `Get` returns is what the commit writes.
* `old_breaks`: the store before the fix left a deleted object stored again out of the cache
  (`ObjectDBTransactionCoverageTest.ObjectStoredAgainAfterDeleteCanBeGot`).
-/

namespace Btdb.ObjectRestore

inductive St where
  | read
  | dirty
  | deleted
  deriving DecidableEq

structure State where
  /-- An object's oid and state, `none` when unknown. -/
  info : Nat → Option (Nat × St)
  cache : Nat → Option Nat
  pending : Nat → Option Nat

def upd {β} (f : Nat → β) (k : Nat) (v : β) : Nat → β := fun x => if x = k then v else f x

/-- Marks the object cached under `id`, other than `o`, deleted. -/
def displace (s : State) (id o : Nat) : Nat → Option (Nat × St) :=
  match s.cache id with
  | some d => if d = o then s.info else upd s.info d (some (id, .deleted))
  | none => s.info

def store (s : State) (o id : Nat) (st : St) : State :=
  match st with
  | .read => { s with info := upd s.info o (some (id, .dirty)), pending := upd s.pending id (some o) }
  | .dirty => s
  | .deleted =>
    { info := upd (displace s id o) o (some (id, .dirty)), cache := upd s.cache id (some o),
      pending := upd s.pending id (some o) }

inductive Step : State → State → Prop
  | store (s : State) (o id : Nat) (st : St) (hm : s.info o = some (id, st)) : Step s (store s o id st)
  | delete (s : State) (o id : Nat) (st : St) (hm : s.info o = some (id, st)) (hs : st ≠ .deleted) :
      Step s { info := upd s.info o (some (id, .deleted)), cache := upd s.cache id none,
               pending := upd s.pending id none }
  | create (s : State) (o id : Nat) (hm : s.info o = none) (hc : s.cache id = none)
      (hfree : ∀ p q, s.info p = some (id, q) → q = .deleted) :
      Step s { info := upd s.info o (some (id, .dirty)), cache := upd s.cache id (some o),
               pending := upd s.pending id (some o) }

structure Inv (s : State) : Prop where
  pend : ∀ id o, s.pending id = some o → s.info o = some (id, .dirty)
  dirty : ∀ o id, s.info o = some (id, .dirty) → s.pending id = some o
  live : ∀ o id q, s.info o = some (id, q) → q ≠ .deleted → s.cache id = some o
  cacheMeta : ∀ id o, s.cache id = some o → ∃ q, s.info o = some (id, q) ∧ q ≠ .deleted

/-- Objects read from storage are cached and not pending. -/
inductive Reachable : State → Prop
  | init (info : Nat → Option (Nat × St)) (cache : Nat → Option Nat)
      (hc : ∀ id o, cache id = some o → info o = some (id, .read))
      (hm : ∀ o id q, info o = some (id, q) → q = .read ∧ cache id = some o) :
      Reachable ⟨info, cache, fun _ => none⟩
  | step {s s'} : Reachable s → Step s s' → Reachable s'

theorem inv_step {s s' : State} (h : Inv s) (hs : Step s s') : Inv s' := by
  obtain ⟨hp, hd, hl, hcm⟩ := h
  cases hs with
  | store o id st hm =>
    cases st with
    | dirty => exact ⟨hp, hd, hl, hcm⟩
    | read =>
      have hco := hl o id .read hm (by decide)
      refine ⟨fun i x h => ?_, fun x i h => ?_, fun x i q h hq => ?_, fun i x h => ?_⟩ <;>
        simp only [store, upd] at h ⊢
      · by_cases hi : i = id
        · simp [hi] at h; subst h; simp [hi]
        · simp only [hi, ↓reduceIte] at h
          have := hp _ _ h
          by_cases hx : x = o
          · subst hx; simp_all
          · simp [hx, this]
      · by_cases hx : x = o
        · subst hx; simp at h; subst h; simp
        · simp only [hx, ↓reduceIte] at h
          have := hd _ _ h
          by_cases hi : i = id
          · subst hi; have := hl x i .dirty h (by decide); simp_all
          · simp [hi, this]
      · by_cases hx : x = o
        · subst hx; simp at h; obtain ⟨rfl, -⟩ := h; exact hco
        · simp only [hx, ↓reduceIte] at h; exact hl x i q h hq
      · obtain ⟨q, h1, h2⟩ := hcm i x h
        by_cases hx : x = o
        · subst hx; rw [hm] at h1; simp at h1; exact ⟨.dirty, by simp [h1.1], by decide⟩
        · exact ⟨q, by simp [hx, h1], h2⟩
    | deleted =>
      -- The object cached under `id`, if any, is another one, live, and becomes deleted.
      have hother : ∀ d, s.cache id = some d → d ≠ o ∧ ∃ q, s.info d = some (id, q) ∧ q ≠ .deleted := by
        intro d hdc
        obtain ⟨q, h1, h2⟩ := hcm id d hdc
        refine ⟨fun h => ?_, q, h1, h2⟩
        subst h; rw [hm] at h1; simp at h1; exact h2 h1.symm
      have hinfo : ∀ x, (store s o id .deleted).info x =
          if x = o then some (id, .dirty) else if s.cache id = some x then some (id, .deleted) else s.info x := by
        intro x
        simp only [store, upd, displace]
        by_cases hx : x = o
        · simp [hx]
        · simp only [hx, ↓reduceIte]
          cases hc : s.cache id with
          | none => simp
          | some d =>
            have hdo := (hother d hc).1
            by_cases hxd : x = d
            · subst hxd; simp [upd, hdo]
            · simp [upd, hdo, hxd, Ne.symm hxd]
      have hcache : ∀ i, (store s o id .deleted).cache i = if i = id then some o else s.cache i := fun i => rfl
      have hpend : ∀ i, (store s o id .deleted).pending i = if i = id then some o else s.pending i := fun i => rfl
      refine ⟨fun i x h => ?_, fun x i h => ?_, fun x i q h hq => ?_, fun i x h => ?_⟩ <;>
        simp only [hinfo, hcache, hpend] at h ⊢
      · by_cases hi : i = id
        · simp [hi] at h; subst h; simp [hi]
        · simp only [hi, ↓reduceIte] at h
          have hx := hp _ _ h
          have hxo : x ≠ o := fun e => by subst e; rw [hm] at hx; simp at hx
          have hxc : s.cache id ≠ some x := fun e => by
            obtain ⟨-, q, h1, -⟩ := hother x e; rw [hx] at h1; simp at h1; exact hi h1.1
          simp [hxo, hxc, hx]
      · by_cases hx : x = o
        · subst hx; simp at h; subst h; simp
        · simp only [hx, ↓reduceIte] at h
          by_cases hxc : s.cache id = some x
          · simp [hxc] at h
          · simp only [hxc, ↓reduceIte] at h
            have hpi := hd _ _ h
            by_cases hi : i = id
            · subst hi; exact absurd (hl x i .dirty h (by decide)) hxc
            · simp [hi, hpi]
      · by_cases hx : x = o
        · subst hx; simp at h; obtain ⟨rfl, -⟩ := h; simp
        · simp only [hx, ↓reduceIte] at h
          by_cases hxc : s.cache id = some x
          · simp [hxc] at h; exact absurd h.2.symm hq
          · simp only [hxc, ↓reduceIte] at h
            have hci := hl x i q h hq
            by_cases hi : i = id
            · subst hi; exact absurd hci hxc
            · simp [hi, hci]
      · by_cases hi : i = id
        · subst hi; simp at h; subst h; exact ⟨.dirty, by simp, by decide⟩
        · simp only [hi, ↓reduceIte] at h
          obtain ⟨q, h1, h2⟩ := hcm i x h
          have hxo : x ≠ o := fun e => by subst e; rw [hm] at h1; simp at h1; exact h2 h1.2.symm
          have hxc : s.cache id ≠ some x := fun e => by
            obtain ⟨-, q', h1', -⟩ := hother x e; rw [h1] at h1'; simp at h1'; exact hi h1'.1
          exact ⟨q, by simp [hxo, hxc, h1], h2⟩
  | delete o id st hm hs =>
    have hco := hl o id st hm hs
    refine ⟨fun i x h => ?_, fun x i h => ?_, fun x i q h hq => ?_, fun i x h => ?_⟩ <;>
      simp only [upd] at h ⊢
    · by_cases hi : i = id
      · simp [hi] at h
      · simp only [hi, ↓reduceIte] at h
        have hx := hp _ _ h
        have hxo : x ≠ o := fun e => by subst e; rw [hm] at hx; simp at hx; exact hi hx.1.symm
        simp [hxo, hx]
    · by_cases hx : x = o
      · simp [hx] at h
      · simp only [hx, ↓reduceIte] at h
        have hpi := hd _ _ h
        by_cases hi : i = id
        · subst hi; have := hl x i .dirty h (by decide); rw [hco] at this; simp at this; exact absurd this.symm hx
        · simp [hi, hpi]
    · by_cases hx : x = o
      · simp [hx] at h; exact absurd h.2.symm hq
      · simp only [hx, ↓reduceIte] at h
        have hci := hl x i q h hq
        by_cases hi : i = id
        · subst hi; rw [hco] at hci; simp at hci; exact absurd hci.symm hx
        · simp [hi, hci]
    · by_cases hi : i = id
      · simp [hi] at h
      · simp only [hi, ↓reduceIte] at h
        obtain ⟨q, h1, h2⟩ := hcm i x h
        have hxo : x ≠ o := fun e => by subst e; rw [hm] at h1; simp at h1; exact hi h1.1.symm
        exact ⟨q, by simp [hxo, h1], h2⟩
  | create o id hm hc hfree =>
    refine ⟨fun i x h => ?_, fun x i h => ?_, fun x i q h hq => ?_, fun i x h => ?_⟩ <;>
      simp only [upd] at h ⊢
    · by_cases hi : i = id
      · simp [hi] at h; subst h; simp [hi]
      · simp only [hi, ↓reduceIte] at h
        have hx := hp _ _ h
        have hxo : x ≠ o := fun e => by subst e; rw [hm] at hx; simp at hx
        simp [hxo, hx]
    · by_cases hx : x = o
      · subst hx; simp at h; subst h; simp
      · simp only [hx, ↓reduceIte] at h
        by_cases hi : i = id
        · subst hi; exact absurd (hfree x .dirty h) (by decide)
        · simp [hi, hd _ _ h]
    · by_cases hx : x = o
      · subst hx; simp at h; obtain ⟨rfl, -⟩ := h; simp
      · simp only [hx, ↓reduceIte] at h
        have hci := hl x i q h hq
        by_cases hi : i = id
        · subst hi; rw [hc] at hci; simp at hci
        · simp [hi, hci]
    · by_cases hi : i = id
      · subst hi; simp at h; subst h; exact ⟨.dirty, by simp, by decide⟩
      · simp only [hi, ↓reduceIte] at h
        obtain ⟨q, h1, h2⟩ := hcm i x h
        have hxo : x ≠ o := fun e => by subst e; rw [hm] at h1; simp at h1
        exact ⟨q, by simp [hxo, h1], h2⟩

theorem reachable_inv {s : State} (h : Reachable s) : Inv s := by
  induction h with
  | init info cache hc hm =>
    refine ⟨fun _ _ h => by simp at h, fun o id h => ?_, fun o id q h _ => (hm o id q h).2,
      fun id o h => ⟨.read, hc id o h, by decide⟩⟩
    have := (hm o id .dirty h).1
    cases this
  | step _ hs ih => exact inv_step ih hs

/-- The store before the fix: a deleted object becomes dirty and pending, but stays out of the cache. -/
def oldStore (s : State) (o id : Nat) (st : St) : State :=
  match st with
  | .dirty => s
  | _ => { s with info := upd s.info o (some (id, .dirty)), pending := upd s.pending id (some o) }

/-- Object 1 read under oid 5, deleted and stored again. -/
theorem old_breaks :
    let s₀ : State := ⟨fun o => if o = 1 then some (5, .read) else none, fun i => if i = 5 then some 1 else none,
      fun _ => none⟩
    let s₁ : State := ⟨upd s₀.info 1 (some (5, .deleted)), upd s₀.cache 5 none, upd s₀.pending 5 none⟩
    let s₂ := oldStore s₁ 1 5 .deleted
    s₂.pending 5 = some 1 ∧ s₂.cache 5 = none ∧ (store s₁ 1 5 .deleted).cache 5 = some 1 := by
  simp [oldStore, store, upd, displace]

end Btdb.ObjectRestore
