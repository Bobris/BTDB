import LsmProofs.Common

/-!
# WAL fencing and replay across writer takeovers

A model of the WAL of `LsmKeyValueDB`: chunks are immutable objects `wal/{n}` created if absent
(`CreateChunkAsync`), each writer session has a fresh epoch, a takeover lists the WAL, replays its contiguous prefix
and fences it with epoch markers (`BecomeWriterAsync`), and readers and recovery replay names in order up to the first
missing one (`WalReplay.Apply`, `ReplayListedWalAsync`, `CatchUpAsync`).

Writers are never stopped: a fenced, crashed or expired writer keeps every chunk it ever planned, and each planned
chunk may land at any later time while its name is free. That covers parallel uploads completing out of order,
delayed requests and lost responses. Nothing relies on the lease; writers may even overlap. A writer acknowledges
the longest prefix of its planned chunks that landed with its content, which is at least what `UploadWalAsync`
acknowledges. A record is an opaque `Nat`; one chunk holds whole records (multi-chunk records, generations and
manifests are not modelled). Epochs are fresh and increasing, as the manifest epoch edit guarantees (`Fence.lean`).

`Rule.current` is the implemented protocol: the takeover fences only the first missing name, and replay ignores a
chunk only when its epoch is lower than an earlier one. `Rule.fixed` fences every missing name below the next chunk,
records the first own chunk in the markers, and stops a writer's history at the first own name that holds a foreign
chunk.

* `current_loses_acknowledged`: an acknowledged commit is lost after a second hole in the WAL.
* `current_replays_a_gap`: replay applies a writer's chunk after its displaced predecessor.
* `fixed_keeps_acknowledged`: every acknowledged history is a prefix of every replay.
* `fixed_replays_one_history`: every replay is a prefix of one writer's history.
-/

namespace Lsm.Wal

structure Chunk where
  epoch : Nat
  marker : Bool
  own : Nat
  recs : List Nat
deriving DecidableEq, Repr

/-- An epoch marker; `own` is the writer's first own chunk name. -/
def markerOf (e own : Nat) : Chunk := ⟨e, true, own, []⟩

def dataOf (e : Nat) (recs : List Nat) : Chunk := ⟨e, false, 0, recs⟩

/-- Replay state: the highest epoch so far, the first own chunk of that epoch, whether a foreign chunk took one of
its own names, and the records applied. -/
structure RS where
  maxE : Nat
  own : Nat
  broken : Bool
  out : List Nat
deriving DecidableEq, Repr

def RS.init : RS := ⟨0, 0, false, []⟩

inductive Rule where
  | current
  | fixed
deriving DecidableEq

/-- `WalReplay.Apply` for chunk `c` at name `n`. -/
def rstep (r : Rule) (s : RS) (n : Nat) (c : Chunk) : RS :=
  if c.epoch < s.maxE then
    (if r = .fixed ∧ s.own ≤ n then { s with broken := true } else s)
  else if s.maxE < c.epoch then
    (if c.marker then ⟨c.epoch, c.own, false, s.out⟩ else ⟨c.epoch, n, false, s.out ++ c.recs⟩)
  else if c.marker ∨ (r = .fixed ∧ s.broken = true) then s
  else { s with out := s.out ++ c.recs }

/-- The replay state after names `0 … k-1`, or `none` when one of them is missing. -/
def runTo (r : Rule) (wal : Nat → Option Chunk) : Nat → Option RS
  | 0 => some RS.init
  | k + 1 => (runTo r wal k).bind fun s => (wal k).map fun c => rstep r s k c

/-- A writer session: its marker name (the first missing name it listed), its first own chunk name, the names it
fences, the history it replayed, whether its fences all landed, its planned chunks (names `own, own + 1, …`) and
how many it acknowledged. -/
structure Writer where
  ff : Nat
  own : Nat
  fences : List Nat
  base : List Nat
  active : Bool
  planned : List (List Nat)
  acked : Nat

/-- Storage, one past the highest existing name, the number of epochs handed out, and writers by epoch. -/
structure State where
  wal : Nat → Option Chunk
  top : Nat
  epochs : Nat
  writers : Nat → Option Writer

def init : State := ⟨fun _ => none, 0, 0, fun _ => none⟩

def chunkAt (W : Writer) (e i : Nat) : Chunk := dataOf e (W.planned[i]?.getD [])

def hist (W : Writer) : List Nat := W.base ++ W.planned.flatten

/-- The names a takeover fences after listing: the first missing one, and under the fixed rule also every other
missing name below the next chunk. -/
def FencesOk : Rule → (Nat → Option Chunk) → Nat → Nat → List Nat → Prop
  | .current, _, _, ff, fences => fences = [ff]
  | .fixed, wal, top, ff, fences => ∀ x, x ∈ fences ↔ (x = ff ∨ (ff ≤ x ∧ x < top ∧ wal x = none))

inductive Step (r : Rule) : State → State → Prop
  /-- A takeover lists and replays the WAL; its first own name is `max nextChunk (firstFree + 1)`. -/
  | begin (σ : State) (ff : Nat) (s : RS) (fences : List Nat)
      (hrun : runTo r σ.wal ff = some s) (hff : σ.wal ff = none) (hf : FencesOk r σ.wal σ.top ff fences) :
      Step r σ { σ with
        epochs := σ.epochs + 1,
        writers := upd σ.writers (σ.epochs + 1) (some ⟨ff, max σ.top (ff + 1), fences, s.out, false, [], 0⟩) }
  | landFence (σ : State) (e x : Nat) (W : Writer) (hW : σ.writers e = some W) (hx : x ∈ W.fences)
      (hfree : σ.wal x = none) :
      Step r σ { σ with wal := upd σ.wal x (some (markerOf e W.own)), top := max σ.top (x + 1) }
  | landData (σ : State) (e i : Nat) (W : Writer) (hW : σ.writers e = some W) (hi : i < W.planned.length)
      (hfree : σ.wal (W.own + i) = none) :
      Step r σ { σ with wal := upd σ.wal (W.own + i) (some (chunkAt W e i)), top := max σ.top (W.own + i + 1) }
  /-- Every fence holds this writer's marker: it starts writing. -/
  | activate (σ : State) (e : Nat) (W : Writer) (hW : σ.writers e = some W)
      (hall : ∀ x ∈ W.fences, σ.wal x = some (markerOf e W.own)) :
      Step r σ { σ with writers := upd σ.writers e (some { W with active := true }) }
  | plan (σ : State) (e : Nat) (W : Writer) (recs : List Nat) (hW : σ.writers e = some W) (ha : W.active = true) :
      Step r σ { σ with writers := upd σ.writers e (some { W with planned := W.planned ++ [recs] }) }
  | ack (σ : State) (e : Nat) (W : Writer) (k : Nat) (hW : σ.writers e = some W) (hk : k ≤ W.planned.length)
      (hl : ∀ i, i < k → σ.wal (W.own + i) = some (chunkAt W e i)) :
      Step r σ { σ with writers := upd σ.writers e (some { W with acked := max W.acked k }) }

inductive Reach (r : Rule) : State → Prop
  | init : Reach r init
  | step {σ τ : State} : Reach r σ → Step r σ τ → Reach r τ

/-! ## Counterexamples for the current rule -/

/-- Writer 1 crashes with chunks 1, 3 and 5 landed and 2 and 4 missing. Writer 2 fences only name 2, writes chunk 6
and acknowledges it. Replay stops at the hole at name 4, so a reader never sees the acknowledged commit, and the next
takeover fences name 4 and discards it. -/
theorem current_loses_acknowledged : ∃ σ, Reach .current σ ∧ ∃ e W f s, σ.writers e = some W ∧
    W.active = true ∧ runTo .current σ.wal f = some s ∧ σ.wal f = none ∧
    ¬ (W.base ++ (W.planned.take W.acked).flatten <+: s.out) := by
  have r1 := Reach.step Reach.init (Step.begin (r := .current) init 0 RS.init [0] rfl rfl rfl)
  have r2 := Reach.step r1 (Step.landFence _ 1 0 _ rfl (by simp) rfl)
  have r3 := Reach.step r2 (Step.activate _ 1 _ rfl (by decide))
  have r4 := Reach.step r3 (Step.plan _ 1 _ [1] rfl rfl)
  have r5 := Reach.step r4 (Step.plan _ 1 _ [2] rfl rfl)
  have r6 := Reach.step r5 (Step.plan _ 1 _ [3] rfl rfl)
  have r7 := Reach.step r6 (Step.plan _ 1 _ [4] rfl rfl)
  have r8 := Reach.step r7 (Step.plan _ 1 _ [5] rfl rfl)
  have r9 := Reach.step r8 (Step.landData _ 1 0 _ rfl (by decide) rfl)
  have r10 := Reach.step r9 (Step.landData _ 1 2 _ rfl (by decide) rfl)
  have r11 := Reach.step r10 (Step.landData _ 1 4 _ rfl (by decide) rfl)
  have r12 := Reach.step r11 (Step.begin _ 2 ⟨1, 1, false, [1]⟩ [2] rfl rfl rfl)
  have r13 := Reach.step r12 (Step.landFence _ 2 2 _ rfl (by simp) rfl)
  have r14 := Reach.step r13 (Step.activate _ 2 _ rfl (by decide))
  have r15 := Reach.step r14 (Step.plan _ 2 _ [7] rfl rfl)
  have r16 := Reach.step r15 (Step.landData _ 2 0 _ rfl (by decide) rfl)
  have r17 := Reach.step r16 (Step.ack _ 2 _ 1 rfl (by decide) (by decide))
  refine ⟨_, r17, 2, _, 4, ⟨2, 6, false, [1]⟩, rfl, rfl, rfl, rfl, ?_⟩
  intro h
  have := h.length_le
  simp at this

/-- Writer 2 takes over after chunk 1 and writes chunks 3 and 4. Writer 1's delayed chunk takes name 3 first, and
replay then applies writer 2's chunk 4 without its chunk 3: the result `[1, 4]` is neither writer's history. -/
theorem current_replays_a_gap : ∃ σ, Reach .current σ ∧ σ.epochs = 2 ∧ ∃ W₁ W₂ s,
    σ.writers 1 = some W₁ ∧ σ.writers 2 = some W₂ ∧ runTo .current σ.wal 5 = some s ∧ σ.wal 5 = none ∧
    s.out = [1, 4] ∧ ¬ (s.out <+: hist W₁) ∧ ¬ (s.out <+: hist W₂) := by
  have r1 := Reach.step Reach.init (Step.begin (r := .current) init 0 RS.init [0] rfl rfl rfl)
  have r2 := Reach.step r1 (Step.landFence _ 1 0 _ rfl (by simp) rfl)
  have r3 := Reach.step r2 (Step.activate _ 1 _ rfl (by decide))
  have r4 := Reach.step r3 (Step.plan _ 1 _ [1] rfl rfl)
  have r5 := Reach.step r4 (Step.plan _ 1 _ [2] rfl rfl)
  have r6 := Reach.step r5 (Step.plan _ 1 _ [5] rfl rfl)
  have r7 := Reach.step r6 (Step.landData _ 1 0 _ rfl (by decide) rfl)
  have r8 := Reach.step r7 (Step.begin _ 2 ⟨1, 1, false, [1]⟩ [2] rfl rfl rfl)
  have r9 := Reach.step r8 (Step.landFence _ 2 2 _ rfl (by simp) rfl)
  have r10 := Reach.step r9 (Step.activate _ 2 _ rfl (by decide))
  have r11 := Reach.step r10 (Step.plan _ 2 _ [3] rfl rfl)
  have r12 := Reach.step r11 (Step.plan _ 2 _ [4] rfl rfl)
  have r13 := Reach.step r12 (Step.landData _ 1 2 _ rfl (by decide) rfl)
  have r14 := Reach.step r13 (Step.landData _ 2 1 _ rfl (by decide) rfl)
  refine ⟨_, r14, rfl, _, _, _, rfl, rfl, rfl, rfl, rfl, ?_, ?_⟩
  · show ¬ ([1, 4] <+: [1, 2, 5])
    intro h; obtain ⟨t, ht⟩ := h; simp at ht
  · show ¬ ([1, 4] <+: [1, 3, 4])
    intro h; obtain ⟨t, ht⟩ := h; simp at ht

/-! ## Replay lemmas -/

theorem runTo_succ {r : Rule} {wal : Nat → Option Chunk} {k : Nat} {s' : RS} :
    runTo r wal (k + 1) = some s' ↔ ∃ s c, runTo r wal k = some s ∧ wal k = some c ∧ rstep r s k c = s' := by
  cases h1 : runTo r wal k <;> cases h2 : wal k <;> simp [runTo, h1, h2]

/-- Every name below a reached point exists. -/
theorem runTo_present {r : Rule} {wal : Nat → Option Chunk} :
    ∀ {k : Nat} {s : RS}, runTo r wal k = some s → ∀ n, n < k → ∃ c, wal n = some c
  | 0, _, _, n, hn => absurd hn (Nat.not_lt_zero n)
  | k + 1, _, h, n, hn => by
    obtain ⟨s, c, hs, hc, _⟩ := runTo_succ.1 h
    rcases Nat.lt_or_ge n k with h' | h'
    · exact runTo_present hs n h'
    · exact ⟨c, by rw [show n = k by omega]; exact hc⟩

def Ext (wal wal' : Nat → Option Chunk) : Prop := ∀ n c, wal n = some c → wal' n = some c

/-- Chunks are immutable: a reached replay state never changes as storage grows. -/
theorem runTo_ext {r : Rule} {wal wal' : Nat → Option Chunk} (hx : Ext wal wal') :
    ∀ {k : Nat} {s : RS}, runTo r wal k = some s → runTo r wal' k = some s
  | 0, _, h => h
  | k + 1, _, h => by
    obtain ⟨s, c, hs, hc, rfl⟩ := runTo_succ.1 h
    exact runTo_succ.2 ⟨s, c, runTo_ext hx hs, hx _ _ hc, rfl⟩

theorem rstep_mono (r : Rule) (s : RS) (n : Nat) (c : Chunk) :
    s.maxE ≤ (rstep r s n c).maxE ∧ s.out <+: (rstep r s n c).out := by
  unfold rstep
  by_cases h1 : c.epoch < s.maxE
  · simp only [h1, ite_true]
    split <;> exact ⟨Nat.le_refl _, List.prefix_refl _⟩
  · simp only [h1, ite_false]
    by_cases h2 : s.maxE < c.epoch
    · simp only [h2, ite_true]
      split
      · exact ⟨Nat.le_of_lt h2, List.prefix_refl _⟩
      · exact ⟨Nat.le_of_lt h2, List.prefix_append _ _⟩
    · simp only [h2, ite_false]
      split
      · exact ⟨Nat.le_refl _, List.prefix_refl _⟩
      · exact ⟨Nat.le_refl _, List.prefix_append _ _⟩

/-- Along one replay the highest epoch only grows and the applied records only grow. -/
theorem runTo_mono {r : Rule} {wal : Nat → Option Chunk} {k : Nat} {s : RS} (hs : runTo r wal k = some s) :
    ∀ (d : Nat) {t : RS}, runTo r wal (k + d) = some t → s.maxE ≤ t.maxE ∧ s.out <+: t.out
  | 0, _, ht => by
    rw [Nat.add_zero, hs] at ht; cases ht; exact ⟨Nat.le_refl _, List.prefix_refl _⟩
  | d + 1, _, ht => by
    obtain ⟨u, c, hu, _, rfl⟩ := runTo_succ.1 (show runTo r wal (k + d + 1) = _ from ht)
    obtain ⟨h1, h2⟩ := runTo_mono hs d hu
    obtain ⟨h3, h4⟩ := rstep_mono r u (k + d) c
    exact ⟨Nat.le_trans h1 h3, h2.trans h4⟩

theorem runTo_mono' {r : Rule} {wal : Nat → Option Chunk} {k k' : Nat} {s t : RS} (hs : runTo r wal k = some s)
    (ht : runTo r wal k' = some t) (hk : k ≤ k') : s.maxE ≤ t.maxE ∧ s.out <+: t.out :=
  runTo_mono hs (k' - k) (by rw [Nat.add_sub_cancel' hk]; exact ht)

/-- Replay never exceeds the highest epoch in storage. -/
theorem runTo_maxE_le {r : Rule} {wal : Nat → Option Chunk} {E : Nat}
    (hE : ∀ n c, wal n = some c → c.epoch ≤ E) : ∀ {k : Nat} {s : RS}, runTo r wal k = some s → s.maxE ≤ E
  | 0, _, h => by simp [runTo, RS.init] at h; subst h; exact Nat.zero_le _
  | k + 1, _, h => by
    obtain ⟨s, c, hs, hc, rfl⟩ := runTo_succ.1 h
    have ih := runTo_maxE_le hE hs
    have hc' := hE k c hc
    unfold rstep
    by_cases h1 : c.epoch < s.maxE
    · simp only [h1, ite_true]; split <;> exact ih
    · simp only [h1, ite_false]
      by_cases h2 : s.maxE < c.epoch
      · simp only [h2, ite_true]; split <;> exact hc'
      · simp only [h2, ite_false]; split <;> exact ih

theorem take_succ_flatten (l : List (List Nat)) (i : Nat) (h : i < l.length) :
    (l.take (i + 1)).flatten = (l.take i).flatten ++ (l[i]?.getD []) := by
  rw [List.take_add_one, List.getElem?_eq_getElem h, Option.toList_some, List.flatten_append,
    List.flatten_singleton, Option.getD_some]

theorem take_flatten_prefix (l : List (List Nat)) (j : Nat) : (l.take j).flatten <+: l.flatten := by
  conv => rhs; rw [← List.take_append_drop j l]
  rw [List.flatten_append]; exact List.prefix_append _ _

theorem rstep_lower (s : RS) (n : Nat) (c : Chunk) (h : c.epoch < s.maxE) :
    rstep .fixed s n c = if s.own ≤ n then { s with broken := true } else s := by
  simp [rstep, h]

theorem rstep_higher_marker (s : RS) (n : Nat) (c : Chunk) (h1 : ¬ c.epoch < s.maxE) (h2 : s.maxE < c.epoch)
    (hm : c.marker = true) : rstep .fixed s n c = ⟨c.epoch, c.own, false, s.out⟩ := by
  simp [rstep, h1, h2, hm]

theorem rstep_equal_marker (s : RS) (n : Nat) (c : Chunk) (he : c.epoch = s.maxE) (hm : c.marker = true) :
    rstep .fixed s n c = s := by
  simp [rstep, he, hm]

theorem rstep_equal_broken (s : RS) (n : Nat) (c : Chunk) (he : c.epoch = s.maxE) (hb : s.broken = true) :
    rstep .fixed s n c = s := by
  simp [rstep, he, hb]

theorem rstep_equal_data (s : RS) (n : Nat) (c : Chunk) (he : c.epoch = s.maxE) (hm : c.marker = false)
    (hb : s.broken = false) : rstep .fixed s n c = { s with out := s.out ++ c.recs } := by
  simp [rstep, he, hm, hb]


/-! ## The invariant of the fixed rule -/

structure WriterOk (wal : Nat → Option Chunk) (e : Nat) (W : Writer) : Prop where
  pin : ∃ s, runTo .fixed wal W.ff = some s ∧ s.out = W.base ∧ s.maxE < e
  ff_lt : W.ff < W.own
  ff_mem : W.ff ∈ W.fences
  fences_range : ∀ x ∈ W.fences, W.ff ≤ x ∧ x < W.own
  others : ∀ x, W.ff < x → x < W.own → x ∉ W.fences → ∃ c, wal x = some c ∧ c.epoch < e
  planned_active : W.planned ≠ [] → W.active = true
  fenced : W.active = true → ∀ x ∈ W.fences, wal x = some (markerOf e W.own)
  acked_le : W.acked ≤ W.planned.length
  acked_landed : ∀ i, i < W.acked → wal (W.own + i) = some (chunkAt W e i)
  /-- `BecomeWriterAsync` grants writes from here: replay reaches the first own name in this writer's epoch. -/
  frame : W.active = true → runTo .fixed wal W.own = some ⟨e, W.own, false, W.base⟩

structure Inv (σ : State) : Prop where
  top : ∀ n, σ.top ≤ n → σ.wal n = none
  epochs : ∀ n c, σ.wal n = some c → 1 ≤ c.epoch ∧ c.epoch ≤ σ.epochs
  range : ∀ e W, σ.writers e = some W → 1 ≤ e ∧ e ≤ σ.epochs
  future : ∀ e, σ.epochs < e → σ.writers e = none
  writers : ∀ e W, σ.writers e = some W → WriterOk σ.wal e W
  /-- Every chunk is a marker on one of its writer's fences or one of its writer's planned chunks. -/
  prov : ∀ n c, σ.wal n = some c → ∃ W, σ.writers c.epoch = some W ∧
    ((c = markerOf c.epoch W.own ∧ n ∈ W.fences) ∨
     (∃ i, i < W.planned.length ∧ n = W.own + i ∧ c = chunkAt W c.epoch i))

theorem WriterOk.ext {wal wal' : Nat → Option Chunk} {e : Nat} {W : Writer} (h : WriterOk wal e W)
    (hx : Ext wal wal') : WriterOk wal' e W where
  pin := let ⟨s, h1, h2, h3⟩ := h.pin; ⟨s, runTo_ext hx h1, h2, h3⟩
  ff_lt := h.ff_lt
  ff_mem := h.ff_mem
  fences_range := h.fences_range
  others x h1 h2 h3 := let ⟨c, hc, he⟩ := h.others x h1 h2 h3; ⟨c, hx _ _ hc, he⟩
  planned_active := h.planned_active
  fenced ha x hx' := hx _ _ (h.fenced ha x hx')
  acked_le := h.acked_le
  acked_landed i hi := hx _ _ (h.acked_landed i hi)
  frame ha := runTo_ext hx (h.frame ha)

theorem ext_upd {wal : Nat → Option Chunk} {x : Nat} {c : Chunk} (hfree : wal x = none) :
    Ext wal (upd wal x (some c)) := by
  intro n c' h
  by_cases hn : n = x
  · subst hn; rw [hfree] at h; cases h
  · rw [upd_ne _ _ hn]; exact h

/-- Once every fence holds the writer's marker, replay reaches its first own name in its epoch. -/
theorem frame_of_fenced {wal : Nat → Option Chunk} {e : Nat} {W : Writer} (h : WriterOk wal e W)
    (hall : ∀ x ∈ W.fences, wal x = some (markerOf e W.own)) :
    runTo .fixed wal W.own = some ⟨e, W.own, false, W.base⟩ := by
  obtain ⟨s, hs, hout, hmax⟩ := h.pin
  have h0 : runTo .fixed wal (W.ff + 1) = some ⟨e, W.own, false, W.base⟩ := by
    refine runTo_succ.2 ⟨s, _, hs, hall _ h.ff_mem, ?_⟩
    have h1 : ¬ e < s.maxE := by omega
    simp [rstep, markerOf, h1, hmax, hout]
  have step : ∀ d, W.ff + 1 + d ≤ W.own → runTo .fixed wal (W.ff + 1 + d) = some ⟨e, W.own, false, W.base⟩ := by
    intro d
    induction d with
    | zero => intro _; exact h0
    | succ d ih =>
      intro hd
      have hprev := ih (by omega)
      refine runTo_succ.2 ⟨_, ?_, hprev, ?_, ?_⟩
      · exact if hm : W.ff + 1 + d ∈ W.fences then markerOf e W.own
          else (h.others (W.ff + 1 + d) (by omega) (by omega) hm).choose
      · by_cases hm : W.ff + 1 + d ∈ W.fences
        · simp only [hm, dite_true]; exact hall _ hm
        · simp only [hm, dite_false]; exact (h.others _ (by omega) (by omega) hm).choose_spec.1
      · by_cases hm : W.ff + 1 + d ∈ W.fences
        · simp [hm, rstep, markerOf]
        · simp only [hm, dite_false]
          have hlt := (h.others _ (by omega) (by omega) hm).choose_spec.2
          have hn : ¬ W.own ≤ W.ff + 1 + d := by omega
          simp [rstep, hlt, hn]
  have := step (W.own - (W.ff + 1)) (by have := h.ff_lt; omega)
  rwa [show W.ff + 1 + (W.own - (W.ff + 1)) = W.own by have := h.ff_lt; omega] at this

theorem inv_init : Inv init where
  top _ _ := rfl
  epochs _ _ h := by simp [init] at h
  range _ _ h := by simp [init] at h
  future _ _ := rfl
  writers _ _ h := by simp [init] at h
  prov _ _ h := by simp [init] at h

theorem inv_step {σ τ : State} (hI : Inv σ) (h : Step .fixed σ τ) : Inv τ := by
  cases h with
  | begin ff s fences hrun hff hf =>
    let W₀ : Writer := ⟨ff, max σ.top (ff + 1), fences, s.out, false, [], 0⟩
    have hW₀ : WriterOk σ.wal (σ.epochs + 1) W₀ := {
      pin := ⟨s, hrun, rfl, by
        have := runTo_maxE_le (E := σ.epochs) (fun n c hc => (hI.epochs n c hc).2) hrun; omega⟩
      ff_lt := by simp [W₀]; omega
      ff_mem := (hf ff).2 (Or.inl rfl)
      fences_range := fun x hx => by
        rcases (hf x).1 hx with rfl | ⟨h1, h2, _⟩
        · simp [W₀]; omega
        · simp [W₀]; omega
      others := fun x h1 h2 h3 => by
        simp only [W₀] at h1 h2 h3
        have hlt : x < σ.top := by omega
        cases hx : σ.wal x with
        | none => exact absurd ((hf x).2 (Or.inr ⟨by omega, hlt, hx⟩)) h3
        | some c => exact ⟨c, rfl, by have := (hI.epochs x c hx).2; omega⟩
      planned_active := fun h => absurd rfl h
      fenced := fun h => by cases h
      acked_le := Nat.le_refl _
      acked_landed := fun i hi => absurd hi (Nat.not_lt_zero i)
      frame := fun h => by cases h }
    exact {
      top := hI.top
      epochs := fun n c hc => by have := hI.epochs n c hc; dsimp only; omega
      range := fun e W hW => by
        rcases upd_some hW with ⟨rfl, _⟩ | ⟨_, hW⟩
        · dsimp only; omega
        · have := hI.range e W hW; dsimp only; omega
      future := fun e he => by
        dsimp only at he ⊢
        rw [upd_ne _ _ (by omega)]; exact hI.future e (by omega)
      writers := fun e W hW => by
        rcases upd_some hW with ⟨rfl, rfl⟩ | ⟨_, hW⟩
        · exact hW₀
        · exact hI.writers e W hW
      prov := fun n c hc => by
        obtain ⟨W, hW, hcase⟩ := hI.prov n c hc
        have := (hI.epochs n c hc).2
        exact ⟨W, by dsimp only; rw [upd_ne _ _ (by omega)]; exact hW, hcase⟩ }
  | landFence e x W hW hx hfree =>
    have hext := ext_upd (c := markerOf e W.own) hfree
    have hr := hI.range e W hW
    exact {
      top := fun n hn => by
        dsimp only at hn ⊢
        rw [upd_ne _ _ (by omega)]; exact hI.top n (by omega)
      epochs := fun n c hc => by
        rcases upd_some hc with ⟨_, rfl⟩ | ⟨_, hc⟩
        · exact hr
        · exact hI.epochs n c hc
      range := hI.range
      future := hI.future
      writers := fun e' W' hW' => (hI.writers e' W' hW').ext hext
      prov := fun n c hc => by
        rcases upd_some hc with ⟨rfl, rfl⟩ | ⟨_, hc⟩
        · exact ⟨W, hW, Or.inl ⟨rfl, hx⟩⟩
        · exact hI.prov n c hc }
  | landData e i W hW hi hfree =>
    have hext := ext_upd (c := chunkAt W e i) hfree
    have hr := hI.range e W hW
    exact {
      top := fun n hn => by
        dsimp only at hn ⊢
        rw [upd_ne _ _ (by omega)]; exact hI.top n (by omega)
      epochs := fun n c hc => by
        rcases upd_some hc with ⟨_, rfl⟩ | ⟨_, hc⟩
        · exact hr
        · exact hI.epochs n c hc
      range := hI.range
      future := hI.future
      writers := fun e' W' hW' => (hI.writers e' W' hW').ext hext
      prov := fun n c hc => by
        rcases upd_some hc with ⟨rfl, rfl⟩ | ⟨_, hc⟩
        · exact ⟨W, hW, Or.inr ⟨i, hi, rfl, rfl⟩⟩
        · exact hI.prov n c hc }
  | activate e W hW hall =>
    have hok := hI.writers e W hW
    let W' : Writer := { W with active := true }
    have hW' : WriterOk σ.wal e W' := {
      pin := hok.pin
      ff_lt := hok.ff_lt
      ff_mem := hok.ff_mem
      fences_range := hok.fences_range
      others := hok.others
      planned_active := fun _ => rfl
      fenced := fun _ => hall
      acked_le := hok.acked_le
      acked_landed := hok.acked_landed
      frame := fun _ => by have := frame_of_fenced hok hall; exact this }
    exact {
      top := hI.top
      epochs := hI.epochs
      range := fun e' W₂ h₂ => by
        rcases upd_some h₂ with ⟨he, _⟩ | ⟨_, h₂⟩
        · rw [he]; exact hI.range e W hW
        · exact hI.range e' W₂ h₂
      future := fun e' he => by
        dsimp only; rw [upd_ne _ _ (by have := hI.range e W hW; arith)]; exact hI.future e' he
      writers := fun e' W₂ h₂ => by
        rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
        · exact hW'
        · exact hI.writers e' W₂ h₂
      prov := fun n c hc => by
        obtain ⟨W₂, h₂, hcase⟩ := hI.prov n c hc
        by_cases he : c.epoch = e
        · rw [he] at h₂ hcase ⊢; rw [hW] at h₂; cases h₂
          exact ⟨W', upd_same _ _ _, hcase⟩
        · exact ⟨W₂, by dsimp only; rw [upd_ne _ _ he]; exact h₂, hcase⟩ }
  | plan e W recs hW ha =>
    have hok := hI.writers e W hW
    let W' : Writer := { W with planned := W.planned ++ [recs] }
    have hchunk : ∀ i, i < W.planned.length → chunkAt W' e i = chunkAt W e i := by
      intro i hi; simp [chunkAt, W', List.getElem?_append_left hi]
    have hW' : WriterOk σ.wal e W' := {
      pin := hok.pin
      ff_lt := hok.ff_lt
      ff_mem := hok.ff_mem
      fences_range := hok.fences_range
      others := hok.others
      planned_active := fun _ => ha
      fenced := hok.fenced
      acked_le := by simp [W']; have := hok.acked_le; omega
      acked_landed := fun i hi => by
        have hi' : i < W.acked := hi
        rw [hchunk i (by have := hok.acked_le; omega)]; exact hok.acked_landed i hi'
      frame := hok.frame }
    exact {
      top := hI.top
      epochs := hI.epochs
      range := fun e' W₂ h₂ => by
        rcases upd_some h₂ with ⟨he, _⟩ | ⟨_, h₂⟩
        · rw [he]; exact hI.range e W hW
        · exact hI.range e' W₂ h₂
      future := fun e' he => by
        dsimp only; rw [upd_ne _ _ (by have := hI.range e W hW; arith)]; exact hI.future e' he
      writers := fun e' W₂ h₂ => by
        rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
        · exact hW'
        · exact hI.writers e' W₂ h₂
      prov := fun n c hc => by
        obtain ⟨W₂, h₂, hcase⟩ := hI.prov n c hc
        by_cases he : c.epoch = e
        · rw [he] at h₂ hcase ⊢; rw [hW] at h₂; cases h₂
          refine ⟨W', upd_same _ _ _, ?_⟩
          rcases hcase with hm | ⟨i, hi, hn, hc'⟩
          · exact Or.inl hm
          · exact Or.inr ⟨i, by simp [W']; omega, hn, by rw [hchunk i hi]; exact hc'⟩
        · exact ⟨W₂, by dsimp only; rw [upd_ne _ _ he]; exact h₂, hcase⟩ }
  | ack e W k hW hk hl =>
    have hok := hI.writers e W hW
    let W' : Writer := { W with acked := max W.acked k }
    have hW' : WriterOk σ.wal e W' := {
      pin := hok.pin
      ff_lt := hok.ff_lt
      ff_mem := hok.ff_mem
      fences_range := hok.fences_range
      others := hok.others
      planned_active := hok.planned_active
      fenced := hok.fenced
      acked_le := Nat.max_le.2 ⟨hok.acked_le, hk⟩
      acked_landed := fun i hi => by
        simp only [W'] at hi
        rcases Nat.lt_or_ge i W.acked with h' | h'
        · exact hok.acked_landed i h'
        · exact hl i (by omega)
      frame := hok.frame }
    exact {
      top := hI.top
      epochs := hI.epochs
      range := fun e' W₂ h₂ => by
        rcases upd_some h₂ with ⟨he, _⟩ | ⟨_, h₂⟩
        · rw [he]; exact hI.range e W hW
        · exact hI.range e' W₂ h₂
      future := fun e' he => by
        dsimp only; rw [upd_ne _ _ (by have := hI.range e W hW; arith)]; exact hI.future e' he
      writers := fun e' W₂ h₂ => by
        rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
        · exact hW'
        · exact hI.writers e' W₂ h₂
      prov := fun n c hc => by
        obtain ⟨W₂, h₂, hcase⟩ := hI.prov n c hc
        by_cases he : c.epoch = e
        · rw [he] at h₂ hcase ⊢; rw [hW] at h₂; cases h₂
          exact ⟨W', upd_same _ _ _, hcase⟩
        · exact ⟨W₂, by dsimp only; rw [upd_ne _ _ he]; exact h₂, hcase⟩ }

theorem fixed_inv {σ : State} (h : Reach .fixed σ) : Inv σ := by
  induction h with
  | init => exact inv_init
  | step _ hs ih => exact inv_step ih hs

/-! ## Durability -/

/-- From its first own name, replay applies a writer's acknowledged chunks in order. -/
theorem frame_acked {wal : Nat → Option Chunk} {e : Nat} {W : Writer} (h : WriterOk wal e W)
    (ha : W.active = true) : ∀ i, i ≤ W.acked →
    runTo .fixed wal (W.own + i) = some ⟨e, W.own, false, W.base ++ (W.planned.take i).flatten⟩
  | 0, _ => by simpa using h.frame ha
  | i + 1, hi => by
    have hlen : i < W.planned.length := by have := h.acked_le; omega
    refine runTo_succ.2 ⟨_, _, frame_acked h ha i (by omega), h.acked_landed i (by omega), ?_⟩
    simp [rstep, chunkAt, dataOf, take_succ_flatten _ _ hlen]

/-- A replay that stops at its first missing name, as recovery and every refresh do, holds every acknowledged
history: no acknowledged commit is lost and no other history precedes it. -/
theorem fixed_keeps_acknowledged {σ : State} (h : Reach .fixed σ) {e : Nat} {W : Writer}
    (hW : σ.writers e = some W) (ha : W.active = true) {f : Nat} {s : RS}
    (hs : runTo .fixed σ.wal f = some s) (hf : σ.wal f = none) :
    W.base ++ (W.planned.take W.acked).flatten <+: s.out := by
  have hok := (fixed_inv h).writers e W hW
  have hfr := frame_acked hok ha W.acked (Nat.le_refl _)
  have hle : W.own + W.acked ≤ f := by
    apply Classical.byContradiction
    intro hlt
    rcases Nat.lt_or_ge f W.own with h1 | h1
    · obtain ⟨c, hc⟩ := runTo_present (hok.frame ha) f h1
      rw [hf] at hc; cases hc
    · have := hok.acked_landed (f - W.own) (by omega)
      rw [show W.own + (f - W.own) = f by omega, hf] at this; cases this
  exact (runTo_mono' hfr hs hle).2

/-! ## One history -/

/-- The replay invariant behind `fixed_replays_one_history`. -/
def Q (σ : State) (k : Nat) (s : RS) : Prop :=
  (s.out = [] ∨ ∃ e W, σ.writers e = some W ∧ W.active = true ∧
    ∃ j, j ≤ W.planned.length ∧ s.out = W.base ++ (W.planned.take j).flatten) ∧
  (∀ W, σ.writers s.maxE = some W → W.active = true → s.broken = false → W.own ≤ k →
    s.own = W.own ∧ k - W.own ≤ W.planned.length ∧ s.out = W.base ++ (W.planned.take (k - W.own)).flatten)

theorem q_run {σ : State} (hI : Inv σ) : ∀ {k : Nat} {s : RS}, runTo .fixed σ.wal k = some s → Q σ k s
  | 0, s, h => by
    simp [runTo, RS.init] at h; subst h
    refine ⟨Or.inl rfl, fun W hW => ?_⟩
    have := (hI.range 0 W hW).1; omega
  | k + 1, s', h => by
    obtain ⟨s, c, hs, hc, rfl⟩ := runTo_succ.1 h
    obtain ⟨hq1, hq2⟩ := q_run hI hs
    obtain ⟨Wc, hWc, hcase⟩ := hI.prov k c hc
    have hokc := hI.writers c.epoch Wc hWc
    -- A data chunk's writer is active, and replay passed its first own name: its epoch is not above the replay's.
    have hdata : ∀ i, i < Wc.planned.length → k = Wc.own + i → c.epoch ≤ s.maxE := by
      intro i hi hk
      have ha := hokc.planned_active (List.ne_nil_of_length_pos (by omega))
      exact (runTo_mono' (hokc.frame ha) hs (by omega)).1
    -- A state reached at a writer's first own name is that writer's frame.
    have hframe : ∀ W, σ.writers (rstep .fixed s k c).maxE = some W → W.active = true → W.own = k + 1 →
        (rstep .fixed s k c).own = W.own ∧ (k + 1) - W.own ≤ W.planned.length ∧
        (rstep .fixed s k c).out = W.base ++ (W.planned.take ((k + 1) - W.own)).flatten := by
      intro W hW ha hown
      have hfr := (hI.writers _ W hW).frame ha
      rw [hown, h] at hfr
      rw [Option.some.inj hfr]
      simp [hown]
    by_cases h1 : c.epoch < s.maxE
    · -- A lower epoch is ignored; in an own name it breaks the replay's epoch.
      have hst := rstep_lower s k c h1
      refine ⟨by rw [hst]; split <;> exact hq1, fun W hW ha hb hown => ?_⟩
      rcases Nat.lt_or_ge k (W.own) with hlt | hge
      · exact hframe W hW ha (by omega)
      · rw [hst] at hW hb
        by_cases ho : s.own ≤ k
        · simp [ho] at hb
        · simp only [ho, ite_false] at hW hb
          exact absurd (by rw [(hq2 W hW ha hb hge).1]; exact hge) ho
    · rcases hcase with ⟨hm, hx⟩ | ⟨i, hi, hk, hci⟩
      · -- A writer's markers lie below its first own name.
        have hmk : c.marker = true := by rw [hm]; rfl
        have hco : c.own = Wc.own := by rw [hm]; rfl
        have hxo := (hokc.fences_range k hx).2
        by_cases h2 : s.maxE < c.epoch
        · have hst := rstep_higher_marker s k c h1 h2 hmk
          refine ⟨by rw [hst]; exact hq1, fun W hW ha _ hown => ?_⟩
          have hW' : W = Wc := by rw [hst] at hW; simp only at hW; rw [hWc] at hW; cases hW; rfl
          subst hW'
          exact hframe W (by rw [hst]; exact hWc) ha (by rw [← hco] at hxo ⊢; simp [hco] at hxo; omega)
        · have heq : c.epoch = s.maxE := by omega
          have hst := rstep_equal_marker s k c heq hmk
          refine ⟨by rw [hst]; exact hq1, fun W hW ha _ hown => ?_⟩
          have hW' : W = Wc := by rw [hst, ← heq, hWc] at hW; cases hW; rfl
          subst hW'
          exact hframe W (by rw [hst, ← heq]; exact hWc) ha (by omega)
      · have hpl : Wc.planned[i]? = some (Wc.planned[i]) := List.getElem?_eq_getElem hi
        have hdm : c.marker = false := by rw [hci]; rfl
        have hdr : c.recs = Wc.planned[i] := by rw [hci]; simp [chunkAt, dataOf, hpl]
        have heq : c.epoch = s.maxE := by have := hdata i hi hk; omega
        by_cases hb : s.broken = true
        · -- A broken epoch applies nothing more.
          have hst := rstep_equal_broken s k c heq hb
          refine ⟨by rw [hst]; exact hq1, fun W hW ha hb' hown => ?_⟩
          rw [hst, hb] at hb'; cases hb'
        · -- The writer's next own chunk extends its history.
          have hb' : s.broken = false := by simpa using hb
          have ha := hokc.planned_active (List.ne_nil_of_length_pos (by omega))
          have hWs : σ.writers s.maxE = some Wc := by rw [← heq]; exact hWc
          obtain ⟨hown, hlen, hout⟩ := hq2 Wc hWs ha hb' (by omega)
          have hst := rstep_equal_data s k c heq hdm hb'
          have hki : k - Wc.own = i := by omega
          have hnew : s.out ++ c.recs = Wc.base ++ (Wc.planned.take (i + 1)).flatten := by
            rw [take_succ_flatten _ _ hi, hout, hki, hpl, hdr]; simp
          refine ⟨Or.inr ⟨c.epoch, Wc, hWc, ha, i + 1, hi, by rw [hst]; exact hnew⟩,
            fun W hW ha' hb'' hown' => ?_⟩
          have hW' : W = Wc := by rw [hst] at hW; simp only at hW; rw [hWs] at hW; cases hW; rfl
          subst hW'
          rw [hst]
          exact ⟨hown, by omega, by rw [show k + 1 - W.own = i + 1 by omega]; exact hnew⟩

/-- Every replay is empty or a prefix of the history of one writer that started writing. -/
theorem fixed_replays_one_history {σ : State} (h : Reach .fixed σ) {k : Nat} {s : RS}
    (hs : runTo .fixed σ.wal k = some s) :
    s.out = [] ∨ ∃ e W, σ.writers e = some W ∧ W.active = true ∧ s.out <+: hist W := by
  rcases (q_run (fixed_inv h) hs).1 with h0 | ⟨e, W, hW, ha, j, _, hj⟩
  · exact Or.inl h0
  · refine Or.inr ⟨e, W, hW, ha, ?_⟩
    rw [hj]
    obtain ⟨t, ht⟩ := take_flatten_prefix W.planned j
    exact ⟨t, by rw [hist, List.append_assoc, ht]⟩

/-! ## Starting at a manifest's WAL start -/

/-- Replay of `d` names from `k`, starting in state `c`, stopping at the first missing name. -/
def runFrom (r : Rule) (wal : Nat → Option Chunk) (k : Nat) (c : RS) : Nat → Option RS
  | 0 => some c
  | d + 1 => (runFrom r wal k c d).bind fun u => (wal (k + d)).map fun ch => rstep r u (k + d) ch

/-- A full replay state `t` and a restarted one `u` at name `pos`: same epoch and break, `u` lacks the records
before the start, and their first own names agree or both are reached. -/
def Agree (pre : List Nat) (pos : Nat) (t u : RS) : Prop :=
  t.maxE = u.maxE ∧ t.broken = u.broken ∧ t.out = pre ++ u.out ∧ (t.own = u.own ∨ (t.own ≤ pos ∧ u.own ≤ pos))

theorem own_next {t u : RS} {pos : Nat} (h : t.own = u.own ∨ (t.own ≤ pos ∧ u.own ≤ pos)) :
    t.own = u.own ∨ (t.own ≤ pos + 1 ∧ u.own ≤ pos + 1) := by
  rcases h with h | ⟨a, b⟩
  · exact Or.inl h
  · exact Or.inr ⟨by omega, by omega⟩

theorem rstep_agree (r : Rule) {pre : List Nat} {pos : Nat} {t u : RS} (h : Agree pre pos t u) (c : Chunk) :
    Agree pre (pos + 1) (rstep r t pos c) (rstep r u pos c) := by
  obtain ⟨hm, hb, ho, hown⟩ := h
  have hown' : (t.own ≤ pos ↔ u.own ≤ pos) := by
    rcases hown with h | ⟨h1, h2⟩
    · rw [h]
    · exact ⟨fun _ => h2, fun _ => h1⟩
  unfold rstep
  rw [hm]
  by_cases h1 : c.epoch < u.maxE
  · simp only [h1, ite_true]
    by_cases h2 : r = .fixed ∧ u.own ≤ pos
    · have h3 : r = .fixed ∧ t.own ≤ pos := ⟨h2.1, hown'.2 h2.2⟩
      simp only [h2, h3, and_self, ite_true]
      exact ⟨rfl, rfl, ho, own_next hown⟩
    · have h3 : ¬ (r = .fixed ∧ t.own ≤ pos) := fun h => h2 ⟨h.1, hown'.1 h.2⟩
      simp only [h2, h3, ite_false]
      exact ⟨hm, hb, ho, own_next hown⟩
  · simp only [h1, ite_false]
    by_cases h2 : u.maxE < c.epoch
    · simp only [h2, ite_true]
      split
      · exact ⟨rfl, rfl, ho, Or.inl rfl⟩
      · exact ⟨rfl, rfl, by simp [ho], Or.inl rfl⟩
    · simp only [h2, ite_false]
      rw [hb]
      split
      · exact ⟨hm, hb, ho, own_next hown⟩
      · exact ⟨rfl, rfl, by simp [ho], own_next hown⟩

/-- Restarting replay at a name the full replay reached unbroken and in its writer's own range, with only its epoch,
changes nothing after it: the records before the start (in tables) followed by the restarted replay are exactly the
full replay. This is why a manifest records its WAL start chunk with the epoch of that chunk's writer
(`LsmManifest.WalStartEpoch`); every start a flush picks is a generation's first data chunk or the next chunk to
write, both in their writer's own range. -/
theorem restart_agrees (r : Rule) (wal : Nat → Option Chunk) {k : Nat} {s : RS} (hs : runTo r wal k = some s)
    (hb : s.broken = false) (hown : s.own ≤ k) : ∀ d,
    (runTo r wal (k + d)).isSome = (runFrom r wal k ⟨s.maxE, k, false, []⟩ d).isSome ∧
    ∀ t u, runTo r wal (k + d) = some t → runFrom r wal k ⟨s.maxE, k, false, []⟩ d = some u →
      Agree s.out (k + d) t u
  | 0 => by
    refine ⟨by simp [hs, runFrom], fun t u ht hu => ?_⟩
    simp only [Nat.add_zero, hs, Option.some.injEq] at ht; subst ht
    simp only [runFrom, Option.some.injEq] at hu; subst hu
    exact ⟨rfl, hb, by simp, Or.inr ⟨by omega, Nat.le_refl _⟩⟩
  | d + 1 => by
    obtain ⟨hsome, hagree⟩ := restart_agrees r wal hs hb hown d
    cases ht : runTo r wal (k + d) with
    | none =>
      have hu : runFrom r wal k ⟨s.maxE, k, false, []⟩ d = none := by
        cases hu : runFrom r wal k ⟨s.maxE, k, false, []⟩ d
        · rfl
        · rw [ht, hu] at hsome; simp at hsome
      refine ⟨by rw [show k + (d + 1) = (k + d) + 1 by omega]; simp [runTo, runFrom, ht, hu], fun t u h _ => ?_⟩
      rw [show k + (d + 1) = (k + d) + 1 by omega, runTo] at h; simp [ht] at h
    | some t₀ =>
      cases hu : runFrom r wal k ⟨s.maxE, k, false, []⟩ d with
      | none => rw [ht, hu] at hsome; simp at hsome
      | some u₀ =>
        have hag := hagree t₀ u₀ ht hu
        have hk : k + (d + 1) = (k + d) + 1 := by omega
        refine ⟨?_, fun t u h1 h2 => ?_⟩
        · rw [hk]; cases hc : wal (k + d) <;> simp [runTo, runFrom, ht, hu, hc]
        · rw [hk] at h1 ⊢
          cases hc : wal (k + d) with
          | none => simp [runTo, ht, hc] at h1
          | some c =>
            simp [runTo, ht, hc] at h1
            simp [runFrom, hu, hc] at h2
            subst h1; subst h2
            exact rstep_agree r hag c

/-- With the fixed rule, a reader starting at a manifest whose WAL start the full replay reaches unbroken sees
the tables followed by exactly what the full replay applies there, so `fixed_keeps_acknowledged` and
`fixed_replays_one_history` hold for it too. -/
theorem fixed_restart_replays {σ : State} (h : Reach .fixed σ) {k : Nat} {s : RS}
    (hs : runTo .fixed σ.wal k = some s) (hb : s.broken = false) (hown : s.own ≤ k) {d : Nat} {u : RS}
    (hu : runFrom .fixed σ.wal k ⟨s.maxE, k, false, []⟩ d = some u) :
    ∃ t, runTo .fixed σ.wal (k + d) = some t ∧ t.out = s.out ++ u.out ∧
      (t.out = [] ∨ ∃ e W, σ.writers e = some W ∧ W.active = true ∧ t.out <+: hist W) := by
  obtain ⟨hsome, hagree⟩ := restart_agrees .fixed σ.wal hs hb hown d
  cases ht : runTo .fixed σ.wal (k + d) with
  | none => rw [ht, hu] at hsome; simp at hsome
  | some t =>
    exact ⟨t, rfl, (hagree t u ht hu).2.2.1, fixed_replays_one_history h ht⟩

/-- The fixed rule still lets a writer take over, write and acknowledge, so the results above are not vacuous. -/
theorem fixed_commits : ∃ σ, Reach .fixed σ ∧ ∃ W s, σ.writers 1 = some W ∧ W.acked = 1 ∧
    runTo .fixed σ.wal 2 = some s ∧ σ.wal 2 = none ∧ s.out = [1] := by
  have r1 := Reach.step Reach.init (Step.begin (r := .fixed) init 0 RS.init [0] rfl rfl (by intro x; simp [init]))
  have r2 := Reach.step r1 (Step.landFence _ 1 0 _ rfl (by simp) rfl)
  have r3 := Reach.step r2 (Step.activate _ 1 _ rfl (by decide))
  have r4 := Reach.step r3 (Step.plan _ 1 _ [1] rfl rfl)
  have r5 := Reach.step r4 (Step.landData _ 1 0 _ rfl (by decide) rfl)
  have r6 := Reach.step r5 (Step.ack _ 1 _ 1 rfl (by decide) (by decide))
  exact ⟨_, r6, _, _, rfl, rfl, rfl, rfl, rfl⟩

end Lsm.Wal
