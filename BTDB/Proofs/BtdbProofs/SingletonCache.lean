/-!
# The singleton content cache never serves a stale singleton

A model of `TableInfo.SingletonContent` and `TableInfo.CacheSingletonContent` (`../../ODBLayer/TableInfo.cs`), the
cache of a singleton's stored bytes shared by all transactions of an `ObjectDB`, and of their callers in
`ObjectDBTransaction`. The cache holds a transaction number `n` and the content, or nothing.

* A transaction with number `t` takes the cached content when `n ≤ t`; otherwise, or when nothing is cached, it reads
  its snapshot and offers that to the cache with `t`, which keeps it when `n ≤ t`. A reader's snapshot is a committed
  one, `t ≤ C`, and the content it read is the one committed at `t`; it may offer it at any later time, after other
  steps.
* The writer, whose number is the committed `C`, sets the cache to `C + 1` and nothing before it changes the singleton,
  so its own uncommitted content, offered with `C`, is never kept. Every change must do so: storing an object that may
  be the singleton, deleting one (also by its oid) and `DeleteAllData`. Before the 2026-10-29 fix storing the singleton
  before its oid was loaded, deleting it by its oid and `DeleteAllData` did not, and a stale singleton was served after
  the commit (`ObjectDbSingletonCacheTest`).
* A commit makes the singleton at `C + 1` the new content when the writer changed it, otherwise the one at `C`. A
  rollback changes nothing committed.

The singleton committed at each number is `value`. Natural numbers model transaction numbers, which do not wrap.

* `reachable_inv`: every reachable state keeps the invariant `Inv`: the singleton does not change from the cached number
  to `C`, and cached content is the singleton at its number.
* `lookup_correct`: content a transaction takes from the cache is the singleton of its own snapshot.
* `writer_store_ignored`: content the writer offers after changing the singleton is not kept.
-/

namespace Btdb.SingletonCache

structure State (α : Type) where
  committed : Nat
  value : Nat → α
  cacheN : Nat
  cacheC : Option α
  /-- The writer changed the singleton in the write in progress. -/
  changing : Bool

/-- `CacheSingletonContent`: kept unless older than the cache. -/
def store {α} (s : State α) (t : Nat) (c : Option α) : State α :=
  if s.cacheN ≤ t then { s with cacheN := t, cacheC := c } else s

inductive Step {α} : State α → State α → Prop
  | readerStore (s : State α) (t : Nat) (ht : t ≤ s.committed) :
      Step s (store s t (some (s.value t)))
  | writerStore (s : State α) (c : α) :
      Step s (if s.changing then store s s.committed (some c) else s)
  | change (s : State α) :
      Step s { store s (s.committed + 1) none with changing := true }
  | commitChanged (s : State α) (x : α) (hc : s.changing = true) :
      Step s { s with committed := s.committed + 1,
                      value := fun t => if t = s.committed + 1 then x else s.value t, changing := false }
  | commitUnchanged (s : State α) (hc : s.changing = false) :
      Step s { s with committed := s.committed + 1,
                      value := fun t => if t = s.committed + 1 then s.value s.committed else s.value t }
  | rollback (s : State α) : Step s { s with changing := false }

def init {α} (v : α) : State α := ⟨0, fun _ => v, 0, none, false⟩

inductive Reachable {α} (v : α) : State α → Prop
  | init : Reachable v (init v)
  | step {s s'} : Reachable v s → Step s s' → Reachable v s'

structure Inv {α} (s : State α) : Prop where
  bound : s.cacheN ≤ s.committed + 1
  stable : ∀ t, s.cacheN ≤ t → t ≤ s.committed → s.value t = s.value s.cacheN
  cached : ∀ c, s.cacheC = some c → s.cacheN ≤ s.committed ∧ c = s.value s.cacheN
  changing : s.changing = true → s.cacheN = s.committed + 1 ∧ s.cacheC = none

theorem inv_init {α} (v : α) : Inv (init v) :=
  ⟨by simp [init], fun _ _ _ => rfl, fun _ h => by simp [init] at h, fun h => by simp [init] at h⟩

theorem inv_step {α} {s s' : State α} (h : Inv s) (hs : Step s s') : Inv s' := by
  obtain ⟨hb, hst, hca, hch⟩ := h
  cases hs with
  | readerStore t ht =>
    unfold store
    by_cases hn : s.cacheN ≤ t
    · simp only [hn, ↓reduceIte]
      refine ⟨?_, fun t' h1 h2 => ?_, fun c hc => ?_, fun hc => ?_⟩ <;> dsimp only at *
      · omega
      · rw [hst t' (by omega) h2, hst t hn ht]
      · simp at hc
        exact ⟨ht, hc.symm⟩
      · have := (hch hc).1
        omega
    · simp only [hn, ↓reduceIte]
      exact ⟨hb, hst, hca, hch⟩
  | writerStore c =>
    by_cases hc : s.changing = true
    · have h1 := (hch hc).1
      simp only [hc, ↓reduceIte, store, show ¬s.cacheN ≤ s.committed by omega]
      exact ⟨hb, hst, hca, hch⟩
    · simp only [hc]
      exact ⟨hb, hst, hca, hch⟩
  | change =>
    simp only [store, show s.cacheN ≤ s.committed + 1 from hb, ↓reduceIte]
    exact ⟨Nat.le_refl _, fun t h1 h2 => by dsimp only at h1 h2; omega, fun c hc => by simp at hc, fun _ => ⟨rfl, rfl⟩⟩
  | commitChanged x hc =>
    obtain ⟨hn, hnone⟩ := hch hc
    refine ⟨by simp; omega, fun t h1 h2 => ?_, fun c h => by simp [hnone] at h, fun h => by simp at h⟩
    simp only at h1 h2 ⊢
    have : t = s.committed + 1 := by omega
    subst this
    simp [hn]
  | commitUnchanged hc =>
    refine ⟨by simp; omega, fun t h1 h2 => ?_, fun c h => ?_, fun h => by simp [hc] at h⟩
    · simp only at h1 h2 ⊢
      by_cases hN : s.cacheN = s.committed + 1
      · have : t = s.committed + 1 := by omega
        simp [this, hN]
      · have hnc : s.cacheN ≠ s.committed + 1 := hN
        simp only [hnc, ↓reduceIte]
        by_cases ht : t = s.committed + 1
        · simp only [ht, ↓reduceIte]
          exact hst _ (by omega) (Nat.le_refl _)
        · simp only [ht, ↓reduceIte]
          exact hst t h1 (by omega)
    · simp only at h ⊢
      obtain ⟨h1, h2⟩ := hca c h
      refine ⟨by omega, ?_⟩
      simp [show s.cacheN ≠ s.committed + 1 by omega, h2]
  | rollback =>
    exact ⟨hb, hst, hca, fun h => by simp at h⟩

theorem reachable_inv {α} {v : α} {s : State α} (h : Reachable v s) : Inv s := by
  induction h with
  | init => exact inv_init v
  | step _ hs ih => exact inv_step ih hs

/-- `SingletonContent`: the cached content when the cache is not newer than the transaction. -/
def lookup {α} (s : State α) (t : Nat) : Option α := if s.cacheN ≤ t then s.cacheC else none

theorem lookup_correct {α} {v : α} {s : State α} (h : Reachable v s) (t : Nat) (ht : t ≤ s.committed) (c : α)
    (hl : lookup s t = some c) : c = s.value t := by
  have hi := reachable_inv h
  unfold lookup at hl
  by_cases hn : s.cacheN ≤ t
  · simp only [hn, ↓reduceIte] at hl
    obtain ⟨_, hc⟩ := hi.cached c hl
    rw [hc, hi.stable t hn ht]
  · simp [hn] at hl

theorem writer_store_ignored {α} {v : α} {s : State α} (h : Reachable v s) (hc : s.changing = true) (c : α) :
    store s s.committed (some c) = s := by
  have := ((reachable_inv h).changing hc).1
  simp [store, show ¬s.cacheN ≤ s.committed by omega]

end Btdb.SingletonCache
