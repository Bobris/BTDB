import LsmProofs.Wal

/-!
# WAL replay with flushed manifests

`Wal.lean` replays the whole WAL from name 0. Here a flush publishes a manifest that replaces the WAL before its
start: its tables hold the flushing writer's base and first `i` own chunks' records, whether those chunks landed or
not (a flush writes the memtable, not the WAL), and its WAL start is that writer's own chunk `i` with its epoch
(`LsmManifest.WalStartEpoch`). Readers, recovery and takeovers replay from the newest manifest (`LoadAsync`,
`CatchUpAsync`). Only the newest writer flushes: an older one's manifest edit collides with the newer writer's epoch
edit. Writers otherwise behave as in `Wal.lean`, including stale chunks landing at any time.

* `flush_keeps_acknowledged`: every acknowledged history is a prefix of every replay from the newest manifest that
  stops at its first missing name.
* `flush_replays_one_history`: every such replay, tables included, is empty or a prefix of one writer's history.

A flush start in the replayed region before the writer's own chunks preserves the replay (`Wal.restart_agrees`) and
is not modelled here.
-/

namespace Lsm.WalFlush

open Wal (Chunk RS rstep runFrom Writer markerOf dataOf chunkAt hist Ext ext_upd rstep_mono rstep_lower
  rstep_higher_marker rstep_equal_marker rstep_equal_broken rstep_equal_data take_succ_flatten take_flatten_prefix
  Rule)

structure State where
  wal : Nat → Option Chunk
  top : Nat
  epochs : Nat
  writers : Nat → Option Writer
  /-- WAL start of the newest manifest, and the replay state there: its writer's epoch and the tables' records. -/
  start : Nat
  s0 : RS

def init : State := ⟨fun _ => none, 0, 0, fun _ => none, 0, Wal.RS.init⟩

/-- The replay state at name `p` when replaying from the newest manifest. -/
def pos (σ : State) (p : Nat) : Option RS :=
  if σ.start ≤ p then runFrom .fixed σ.wal σ.start σ.s0 (p - σ.start) else none

inductive Step : State → State → Prop
  | begin (σ : State) (ff : Nat) (s : RS) (fences : List Nat) (hrun : pos σ ff = some s) (hff : σ.wal ff = none)
      (hf : ∀ x, x ∈ fences ↔ (x = ff ∨ (ff ≤ x ∧ x < σ.top ∧ σ.wal x = none))) :
      Step σ { σ with
        epochs := σ.epochs + 1,
        writers := upd σ.writers (σ.epochs + 1) (some ⟨ff, max σ.top (ff + 1), fences, s.out, false, [], 0⟩) }
  | landFence (σ : State) (e x : Nat) (W : Writer) (hW : σ.writers e = some W) (hx : x ∈ W.fences)
      (hfree : σ.wal x = none) :
      Step σ { σ with wal := upd σ.wal x (some (markerOf e W.own)), top := max σ.top (x + 1) }
  | landData (σ : State) (e i : Nat) (W : Writer) (hW : σ.writers e = some W) (hi : i < W.planned.length)
      (hfree : σ.wal (W.own + i) = none) :
      Step σ { σ with wal := upd σ.wal (W.own + i) (some (chunkAt W e i)), top := max σ.top (W.own + i + 1) }
  | activate (σ : State) (e : Nat) (W : Writer) (hW : σ.writers e = some W)
      (hall : ∀ x ∈ W.fences, σ.wal x = some (markerOf e W.own)) :
      Step σ { σ with writers := upd σ.writers e (some { W with active := true }) }
  | plan (σ : State) (e : Nat) (W : Writer) (recs : List Nat) (hW : σ.writers e = some W) (ha : W.active = true) :
      Step σ { σ with writers := upd σ.writers e (some { W with planned := W.planned ++ [recs] }) }
  | ack (σ : State) (e : Nat) (W : Writer) (k : Nat) (hW : σ.writers e = some W) (hk : k ≤ W.planned.length)
      (hl : ∀ i, i < k → σ.wal (W.own + i) = some (chunkAt W e i)) :
      Step σ { σ with writers := upd σ.writers e (some { W with acked := max W.acked k }) }
  /-- The newest writer flushes its base and first `i` own chunks to tables; the WAL start becomes its own chunk
  `i`. -/
  | flush (σ : State) (e : Nat) (W : Writer) (i : Nat) (hW : σ.writers e = some W) (ha : W.active = true)
      (hnew : e = σ.epochs) (hi : i ≤ W.planned.length) (hs : σ.start ≤ W.own + i) :
      Step σ { σ with start := W.own + i, s0 := ⟨e, W.own + i, false, W.base ++ (W.planned.take i).flatten⟩ }

inductive Reach : State → Prop
  | init : Reach init
  | step {σ τ : State} : Reach σ → Step σ τ → Reach τ

/-! ## Replay from a start -/

theorem rf_succ {wal : Nat → Option Chunk} {k : Nat} {c : RS} {d : Nat} {s' : RS} :
    runFrom .fixed wal k c (d + 1) = some s' ↔
      ∃ s ch, runFrom .fixed wal k c d = some s ∧ wal (k + d) = some ch ∧ rstep .fixed s (k + d) ch = s' := by
  cases h1 : runFrom .fixed wal k c d <;> cases h2 : wal (k + d) <;> simp [runFrom, h1, h2]

theorem rf_present {wal : Nat → Option Chunk} {k : Nat} {c : RS} :
    ∀ {d : Nat} {s : RS}, runFrom .fixed wal k c d = some s → ∀ j, j < d → ∃ ch, wal (k + j) = some ch
  | 0, _, _, j, hj => absurd hj (Nat.not_lt_zero j)
  | d + 1, _, h, j, hj => by
    obtain ⟨s, ch, hs, hc, _⟩ := rf_succ.1 h
    rcases Nat.lt_or_ge j d with h' | h'
    · exact rf_present hs j h'
    · exact ⟨ch, by rw [show j = d by omega]; exact hc⟩

theorem rf_ext {wal wal' : Nat → Option Chunk} {k : Nat} {c : RS} (hx : Ext wal wal') :
    ∀ {d : Nat} {s : RS}, runFrom .fixed wal k c d = some s → runFrom .fixed wal' k c d = some s
  | 0, _, h => h
  | d + 1, _, h => by
    obtain ⟨s, ch, hs, hc, rfl⟩ := rf_succ.1 h
    exact rf_succ.2 ⟨s, ch, rf_ext hx hs, hx _ _ hc, rfl⟩

theorem rf_mono {wal : Nat → Option Chunk} {k : Nat} {c : RS} {d : Nat} {s : RS}
    (hs : runFrom .fixed wal k c d = some s) :
    ∀ (e : Nat) {t : RS}, runFrom .fixed wal k c (d + e) = some t → s.maxE ≤ t.maxE ∧ s.out <+: t.out
  | 0, _, ht => by
    rw [Nat.add_zero, hs] at ht; cases ht; exact ⟨Nat.le_refl _, List.prefix_refl _⟩
  | e + 1, _, ht => by
    obtain ⟨u, ch, hu, _, rfl⟩ := rf_succ.1 (show runFrom .fixed wal k c (d + e + 1) = _ from ht)
    obtain ⟨h1, h2⟩ := rf_mono hs e hu
    obtain ⟨h3, h4⟩ := rstep_mono .fixed u (k + (d + e)) ch
    exact ⟨Nat.le_trans h1 h3, h2.trans h4⟩

theorem rf_maxE_le {wal : Nat → Option Chunk} {k : Nat} {c : RS} {E : Nat}
    (hE : ∀ n ch, wal n = some ch → ch.epoch ≤ E) (hc : c.maxE ≤ E) :
    ∀ {d : Nat} {s : RS}, runFrom .fixed wal k c d = some s → s.maxE ≤ E
  | 0, _, h => by simp [runFrom] at h; subst h; exact hc
  | d + 1, _, h => by
    obtain ⟨s, ch, hs, hch, rfl⟩ := rf_succ.1 h
    have ih := rf_maxE_le hE hc hs
    have hc' := hE _ ch hch
    unfold rstep
    by_cases h1 : ch.epoch < s.maxE
    · simp only [h1, ite_true]; split <;> exact ih
    · simp only [h1, ite_false]
      by_cases h2 : s.maxE < ch.epoch
      · simp only [h2, ite_true]; split <;> exact hc'
      · simp only [h2, ite_false]; split <;> exact ih

/-! ## Positions -/

theorem pos_start (σ : State) : pos σ σ.start = some σ.s0 := by simp [pos, runFrom]

theorem pos_ge {σ : State} {p : Nat} {s : RS} (h : pos σ p = some s) : σ.start ≤ p := by
  unfold pos at h; split at h
  · assumption
  · cases h

theorem pos_succ {σ : State} {p : Nat} {s' : RS} (hp : σ.start ≤ p) :
    pos σ (p + 1) = some s' ↔ ∃ s c, pos σ p = some s ∧ σ.wal p = some c ∧ rstep .fixed s p c = s' := by
  have h1 : σ.start ≤ p + 1 := by omega
  have h2 : p + 1 - σ.start = (p - σ.start) + 1 := by omega
  have h3 : σ.start + (p - σ.start) = p := by omega
  simp only [pos, h1, hp, ite_true, h2]
  rw [rf_succ, h3]

theorem pos_present {σ : State} {p : Nat} {s : RS} (h : pos σ p = some s) :
    ∀ n, σ.start ≤ n → n < p → ∃ c, σ.wal n = some c := by
  intro n h1 h2
  have hp := pos_ge h
  simp only [pos, hp, ite_true] at h
  obtain ⟨c, hc⟩ := rf_present h (n - σ.start) (by omega)
  exact ⟨c, by rw [show σ.start + (n - σ.start) = n by omega] at hc; exact hc⟩

theorem pos_mono {σ : State} {p q : Nat} {s t : RS} (hs : pos σ p = some s) (ht : pos σ q = some t) (h : p ≤ q) :
    s.maxE ≤ t.maxE ∧ s.out <+: t.out := by
  have hp := pos_ge hs
  simp only [pos, hp, ite_true] at hs
  simp only [pos, Nat.le_trans hp h, ite_true] at ht
  exact rf_mono hs (q - p) (by rw [show p - σ.start + (q - p) = q - σ.start by omega]; exact ht)

theorem pos_ext {σ τ : State} (hx : Ext σ.wal τ.wal) (hs : τ.start = σ.start) (h0 : τ.s0 = σ.s0) {p : Nat}
    {s : RS} (h : pos σ p = some s) : pos τ p = some s := by
  have hp := pos_ge h
  simp only [pos, hp, ite_true] at h
  simp only [pos, hs, h0, hp, ite_true]
  exact rf_ext hx h

theorem pos_maxE_le {σ : State} {E : Nat} (hE : ∀ n ch, σ.wal n = some ch → ch.epoch ≤ E) (hc : σ.s0.maxE ≤ E)
    {p : Nat} {s : RS} (h : pos σ p = some s) : E ≥ s.maxE := by
  have hp := pos_ge h
  simp only [pos, hp, ite_true] at h
  exact rf_maxE_le hE hc h

/-! ## The invariant -/

/-- Writer `W`'s own chunks `0 … j-1` landed with its content. -/
def Landed (wal : Nat → Option Chunk) (e : Nat) (W : Writer) (j : Nat) : Prop :=
  j ≤ W.planned.length ∧ ∀ t, t < j → wal (W.own + t) = some (chunkAt W e t)

structure AllOk (wal : Nat → Option Chunk) (e : Nat) (W : Writer) : Prop where
  ff_lt : W.ff < W.own
  ff_mem : W.ff ∈ W.fences
  fences_range : ∀ x ∈ W.fences, W.ff ≤ x ∧ x < W.own
  planned_active : W.planned ≠ [] → W.active = true
  acked_le : W.acked ≤ W.planned.length
  acked_landed : ∀ i, i < W.acked → wal (W.own + i) = some (chunkAt W e i)

/-- A writer that began after the newest manifest, as in `Wal.lean`, plus where its first missing name lies
relative to every older such writer. -/
structure CurOk (σ : State) (e : Nat) (W : Writer) : Prop where
  pin : ∃ s, pos σ W.ff = some s ∧ s.out = W.base ∧ s.maxE < e
  others : ∀ x, W.ff < x → x < W.own → x ∉ W.fences → ∃ c, σ.wal x = some c ∧ c.epoch < e
  fenced : W.active = true → ∀ x ∈ W.fences, σ.wal x = some (markerOf e W.own)
  frame : W.active = true → pos σ W.own = some ⟨e, W.own, false, W.base⟩
  after : ∀ e₂ W₂, σ.writers e₂ = some W₂ → σ.s0.maxE < e₂ → e₂ < e →
    W₂.ff ≤ W.ff ∧ (W.ff ∈ W₂.fences ∨ W₂.own ≤ W.ff)

/-- An older writer than the newest manifest's: it can never activate, or its landed own chunks are frozen
behind a foreign chunk at `b` and their records are already in the tables. -/
def Frozen (wal : Nat → Option Chunk) (out : List Nat) (e : Nat) (W : Writer) : Prop :=
  (W.active = false ∧ ∃ x ∈ W.fences, ∃ c, wal x = some c ∧ (c.epoch ≠ e ∨ c.marker = false)) ∨
  (∃ k0 b, k0 ≤ W.planned.length ∧ W.own + k0 ≤ b ∧ (∃ c, wal b = some c ∧ (c.epoch ≠ e ∨ c.marker = true)) ∧
    (∀ n, W.own + k0 ≤ n → n < b → wal n ≠ none) ∧
    ∀ j, j ≤ W.planned.length → (∀ t, k0 ≤ t → t < j → wal (W.own + t) = some (chunkAt W e t)) →
      W.base ++ (W.planned.take j).flatten <+: out)

structure AbsOk (wal : Nat → Option Chunk) (s0 : RS) (e : Nat) (W : Writer) : Prop where
  base_le : W.base <+: s0.out
  frozen : e < s0.maxE → Frozen wal s0.out e W

structure Inv (σ : State) : Prop where
  top : ∀ n, σ.top ≤ n → σ.wal n = none
  epochs : ∀ n c, σ.wal n = some c → 1 ≤ c.epoch ∧ c.epoch ≤ σ.epochs
  range : ∀ e W, σ.writers e = some W → 1 ≤ e ∧ e ≤ σ.epochs
  future : ∀ e, σ.epochs < e → σ.writers e = none
  all : ∀ e W, σ.writers e = some W → AllOk σ.wal e W
  prov : ∀ n c, σ.wal n = some c → ∃ W, σ.writers c.epoch = some W ∧
    ((c = markerOf c.epoch W.own ∧ n ∈ W.fences) ∨
     (∃ i, i < W.planned.length ∧ n = W.own + i ∧ c = chunkAt W c.epoch i))
  s0_broken : σ.s0.broken = false
  s0_own : σ.s0.own = σ.start
  s0_le : σ.s0.maxE ≤ σ.epochs
  s0_writer : (σ.s0.maxE = 0 ∧ σ.s0.out = []) ∨ ∃ W, σ.writers σ.s0.maxE = some W ∧ W.active = true ∧
    ∃ i, i ≤ W.planned.length ∧ σ.start = W.own + i ∧ σ.s0.out = W.base ++ (W.planned.take i).flatten
  cur : ∀ e W, σ.writers e = some W → σ.s0.maxE < e → CurOk σ e W
  abs : ∀ e W, σ.writers e = some W → e ≤ σ.s0.maxE → AbsOk σ.wal σ.s0 e W

/-! ## Frames -/

theorem take_flatten_le (l : List (List Nat)) {a b : Nat} (h : a ≤ b) :
    (l.take a).flatten <+: (l.take b).flatten := by
  have : l.take a = (l.take b).take a := by rw [List.take_take, Nat.min_eq_left h]
  rw [this]; exact take_flatten_prefix _ _

theorem prefix_base {base : List Nat} {l : List (List Nat)} {a b : Nat} (h : a ≤ b) :
    base ++ (l.take a).flatten <+: base ++ (l.take b).flatten := by
  obtain ⟨t, ht⟩ := take_flatten_le l h
  exact ⟨t, by rw [List.append_assoc, ht]⟩

/-- Once its fences hold its markers, replay from the manifest reaches the writer's first own name in its epoch. -/
theorem frame_of_fenced' {σ : State} {e : Nat} {W : Writer} (hA : AllOk σ.wal e W) (hc : CurOk σ e W)
    (hall : ∀ x ∈ W.fences, σ.wal x = some (markerOf e W.own)) :
    pos σ W.own = some ⟨e, W.own, false, W.base⟩ := by
  obtain ⟨s, hs, hout, hmax⟩ := hc.pin
  have hge := pos_ge hs
  have h0 : pos σ (W.ff + 1) = some ⟨e, W.own, false, W.base⟩ := by
    refine (pos_succ hge).2 ⟨s, _, hs, hall _ hA.ff_mem, ?_⟩
    have h1 : ¬ e < s.maxE := by omega
    simp [rstep, markerOf, h1, hmax, hout]
  have step : ∀ d, W.ff + 1 + d ≤ W.own → pos σ (W.ff + 1 + d) = some ⟨e, W.own, false, W.base⟩ := by
    intro d
    induction d with
    | zero => intro _; exact h0
    | succ d ih =>
      intro hd
      have hprev := ih (by omega)
      rw [show W.ff + 1 + (d + 1) = (W.ff + 1 + d) + 1 by omega]
      have hp : σ.start ≤ W.ff + 1 + d := by omega
      by_cases hm : W.ff + 1 + d ∈ W.fences
      · refine (pos_succ hp).2 ⟨_, _, hprev, hall _ hm, ?_⟩
        simp [rstep, markerOf]
      · obtain ⟨c, hc', hlt⟩ := hc.others (W.ff + 1 + d) (by omega) (by omega) hm
        refine (pos_succ hp).2 ⟨_, c, hprev, hc', ?_⟩
        have hn : ¬ W.own ≤ W.ff + 1 + d := by omega
        simp [rstep, hlt, hn]
  have := step (W.own - (W.ff + 1)) (by have := hA.ff_lt; omega)
  rwa [show W.ff + 1 + (W.own - (W.ff + 1)) = W.own by have := hA.ff_lt; omega] at this

/-- From its frame, replay applies a current writer's landed own chunks in order. -/
theorem frame_landed' {σ : State} {e : Nat} {W : Writer} (hc : CurOk σ e W) (ha : W.active = true) :
    ∀ j, Landed σ.wal e W j → pos σ (W.own + j) = some ⟨e, W.own, false, W.base ++ (W.planned.take j).flatten⟩
  | 0, _ => by simpa using hc.frame ha
  | j + 1, ⟨hlen, hl⟩ => by
    have hprev := frame_landed' hc ha j ⟨by omega, fun t ht => hl t (by omega)⟩
    rw [show W.own + (j + 1) = (W.own + j) + 1 by omega]
    refine (pos_succ (pos_ge hprev)).2 ⟨_, _, hprev, hl j (by omega), ?_⟩
    simp [rstep, chunkAt, dataOf, take_succ_flatten _ _ (show j < W.planned.length by omega)]

/-- From the manifest, replay applies the start writer's chunks from its WAL start in order. -/
theorem start_landed {σ : State} {e : Nat} {W : Writer} {i : Nat}
    (h0 : σ.s0 = ⟨e, σ.start, false, W.base ++ (W.planned.take i).flatten⟩) :
    ∀ j, i + j ≤ W.planned.length → (∀ t, t < j → σ.wal (σ.start + t) = some (chunkAt W e (i + t))) →
      pos σ (σ.start + j) = some ⟨e, σ.start, false, W.base ++ (W.planned.take (i + j)).flatten⟩
  | 0, _, _ => by simp [pos_start, h0]
  | j + 1, hlen, hl => by
    have hprev := start_landed h0 j (by omega) (fun t ht => hl t (by omega))
    rw [show σ.start + (j + 1) = (σ.start + j) + 1 by omega]
    refine (pos_succ (Nat.le_add_right _ _)).2 ⟨_, _, hprev, hl j (by omega), ?_⟩
    rw [show i + (j + 1) = (i + j) + 1 by omega, take_succ_flatten _ _ (show i + j < W.planned.length by omega)]
    simp [rstep, chunkAt, dataOf]

/-! ## Steps other than a flush -/

theorem inv_init : Inv init where
  top _ _ := rfl
  epochs _ _ h := by simp [init] at h
  range _ _ h := by simp [init] at h
  future _ _ := rfl
  all _ _ h := by simp [init] at h
  prov _ _ h := by simp [init] at h
  s0_broken := rfl
  s0_own := rfl
  s0_le := Nat.le_refl _
  s0_writer := Or.inl ⟨rfl, rfl⟩
  cur _ _ h := by simp [init] at h
  abs _ _ h := by simp [init] at h

theorem inv_begin {σ : State} (hI : Inv σ) (ff : Nat) (s : RS) (fences : List Nat) (hrun : pos σ ff = some s)
    (hff : σ.wal ff = none) (hf : ∀ x, x ∈ fences ↔ (x = ff ∨ (ff ≤ x ∧ x < σ.top ∧ σ.wal x = none))) :
    Inv { σ with
      epochs := σ.epochs + 1,
      writers := upd σ.writers (σ.epochs + 1) (some ⟨ff, max σ.top (ff + 1), fences, s.out, false, [], 0⟩) } := by
  let W₀ : Writer := ⟨ff, max σ.top (ff + 1), fences, s.out, false, [], 0⟩
  have hmax : s.maxE ≤ σ.epochs := pos_maxE_le (fun n c hc => (hI.epochs n c hc).2) hI.s0_le hrun
  have hstart := pos_ge hrun
  have hold : ∀ e, e ≤ σ.epochs → upd σ.writers (σ.epochs + 1) (some W₀) e = σ.writers e :=
    fun e he => upd_ne _ _ (by omega)
  have hA₀ : AllOk σ.wal (σ.epochs + 1) W₀ := {
    ff_lt := by simp [W₀]; omega
    ff_mem := (hf ff).2 (Or.inl rfl)
    fences_range := fun x hx => by
      rcases (hf x).1 hx with rfl | ⟨h1, h2, _⟩
      · simp [W₀]; omega
      · simp [W₀]; omega
    planned_active := fun h => absurd rfl h
    acked_le := Nat.le_refl _
    acked_landed := fun i hi => absurd hi (Nat.not_lt_zero i) }
  exact {
    top := hI.top
    epochs := fun n c hc => by have := hI.epochs n c hc; exact ⟨this.1, by dsimp only; omega⟩
    range := fun e W hW => by
      rcases upd_some hW with ⟨rfl, _⟩ | ⟨_, hW⟩
      · dsimp only; omega
      · have := hI.range e W hW; dsimp only; omega
    future := fun e he => by
      dsimp only at he ⊢
      rw [upd_ne _ _ (by omega)]; exact hI.future e (by omega)
    all := fun e W hW => by
      rcases upd_some hW with ⟨rfl, rfl⟩ | ⟨_, hW⟩
      · exact hA₀
      · exact hI.all e W hW
    prov := fun n c hc => by
      obtain ⟨W, hW, hcase⟩ := hI.prov n c hc
      exact ⟨W, by dsimp only; rw [hold _ (hI.epochs n c hc).2]; exact hW, hcase⟩
    s0_broken := hI.s0_broken
    s0_own := hI.s0_own
    s0_le := by have := hI.s0_le; dsimp only; omega
    s0_writer := by
      rcases hI.s0_writer with h | ⟨W, hW, ha, i, hi, h1, h2⟩
      · exact Or.inl h
      · exact Or.inr ⟨W, by dsimp only; rw [hold _ hI.s0_le]; exact hW, ha, i, hi, h1, h2⟩
    cur := fun e W hW he => by
      rcases upd_some hW with ⟨rfl, rfl⟩ | ⟨hne, hW⟩
      · exact {
          pin := ⟨s, hrun, rfl, by omega⟩
          others := fun x h1 h2 h3 => by
            simp only at h1 h2 h3
            have hlt : x < σ.top := by omega
            cases hx : σ.wal x with
            | none => exact absurd ((hf x).2 (Or.inr ⟨by omega, hlt, hx⟩)) h3
            | some c => exact ⟨c, rfl, by have := (hI.epochs x c hx).2; omega⟩
          fenced := fun h => by cases h
          frame := fun h => by cases h
          after := fun e₂ W₂ hW₂ he₂ hlt => by
            have hW₂' : σ.writers e₂ = some W₂ := by rw [← hold e₂ (by omega)]; exact hW₂
            have hc₂ := hI.cur e₂ W₂ hW₂' he₂
            have hA₂ := hI.all e₂ W₂ hW₂'
            obtain ⟨s₂, hs₂, _, _⟩ := hc₂.pin
            have hff₂ : W₂.ff ≤ ff := by
              apply Classical.byContradiction; intro h
              obtain ⟨c, hc⟩ := pos_present hs₂ ff hstart (by omega)
              rw [hff] at hc; cases hc
            refine ⟨hff₂, ?_⟩
            by_cases hm : ff ∈ W₂.fences
            · exact Or.inl hm
            · by_cases ho : W₂.own ≤ ff
              · exact Or.inr ho
              · have hne : ff ≠ W₂.ff := fun h => hm (h ▸ hA₂.ff_mem)
                obtain ⟨c, hc, _⟩ := hc₂.others ff (by omega) (by omega) hm
                rw [hff] at hc; cases hc }
      · have hc := hI.cur e W hW he
        have hle := (hI.range e W hW).2
        exact {
          pin := hc.pin
          others := hc.others
          fenced := hc.fenced
          frame := hc.frame
          after := fun e₂ W₂ hW₂ he₂ hlt =>
            hc.after e₂ W₂ (by rw [← hold e₂ (by omega)]; exact hW₂) he₂ hlt }
    abs := fun e W hW he => by
      rcases upd_some hW with ⟨rfl, _⟩ | ⟨_, hW⟩
      · have := hI.s0_le; dsimp only at he; omega
      · exact hI.abs e W hW he }

/-- A newly landed chunk that is not one of `W`'s own data chunks leaves `W`'s frozen prefixes alone. -/
theorem frozen_upd {wal : Nat → Option Chunk} {out : List Nat} {e x : Nat} {W : Writer} {c : Chunk}
    (hfree : wal x = none) (hc : c.epoch ≠ e ∨ c.marker = true) (h : Frozen wal out e W) :
    Frozen (upd wal x (some c)) out e W := by
  have hx := ext_upd (c := c) hfree
  rcases h with ⟨ha, y, hy, c', hc', hp⟩ | ⟨k0, b, hk, hb, ⟨cb, hcb, hpb⟩, hocc, hj⟩
  · exact Or.inl ⟨ha, y, hy, c', hx _ _ hc', hp⟩
  · refine Or.inr ⟨k0, b, hk, hb, ⟨cb, hx _ _ hcb, hpb⟩, fun n h1 h2 => ?_, fun j hjl hl => hj j hjl fun t h1 h2 => ?_⟩
    · intro hn; cases hw : wal n with
      | none => exact hocc n h1 h2 hw
      | some c'' => rw [hx _ _ hw] at hn; cases hn
    · have := hl t h1 h2
      by_cases ht : W.own + t = x
      · rw [ht, upd_same] at this; cases this
        rcases hc with h | h <;> simp [chunkAt, dataOf] at h
      · rwa [upd_ne _ _ ht] at this

theorem inv_landFence {σ : State} (hI : Inv σ) (e x : Nat) (W : Writer) (hW : σ.writers e = some W)
    (hx : x ∈ W.fences) (hfree : σ.wal x = none) :
    Inv { σ with wal := upd σ.wal x (some (markerOf e W.own)), top := max σ.top (x + 1) } := by
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
    all := fun e' W' hW' => by
      have h := hI.all e' W' hW'
      exact { h with acked_landed := fun i hi => hext _ _ (h.acked_landed i hi) }
    prov := fun n c hc => by
      rcases upd_some hc with ⟨rfl, rfl⟩ | ⟨_, hc⟩
      · exact ⟨W, hW, Or.inl ⟨rfl, hx⟩⟩
      · exact hI.prov n c hc
    s0_broken := hI.s0_broken
    s0_own := hI.s0_own
    s0_le := hI.s0_le
    s0_writer := hI.s0_writer
    cur := fun e' W' hW' he => by
      have hc := hI.cur e' W' hW' he
      exact {
        pin := let ⟨s, h1, h2, h3⟩ := hc.pin; ⟨s, pos_ext (σ := σ) (by exact hext) (by rfl) (by rfl) h1, h2, h3⟩
        others := fun y h1 h2 h3 => let ⟨c, hc', he'⟩ := hc.others y h1 h2 h3; ⟨c, hext _ _ hc', he'⟩
        fenced := fun ha y hy => hext _ _ (hc.fenced ha y hy)
        frame := fun ha => pos_ext (σ := σ) (by exact hext) (by rfl) (by rfl) (hc.frame ha)
        after := hc.after }
    abs := fun e' W' hW' he => by
      have ha := hI.abs e' W' hW' he
      exact ⟨ha.base_le, fun hlt => frozen_upd hfree (Or.inr rfl) (ha.frozen hlt)⟩ }

theorem inv_landData {σ : State} (hI : Inv σ) (e i : Nat) (W : Writer) (hW : σ.writers e = some W)
    (hi : i < W.planned.length) (hfree : σ.wal (W.own + i) = none) :
    Inv { σ with wal := upd σ.wal (W.own + i) (some (chunkAt W e i)), top := max σ.top (W.own + i + 1) } := by
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
    all := fun e' W' hW' => by
      have h := hI.all e' W' hW'
      exact { h with acked_landed := fun i hi => hext _ _ (h.acked_landed i hi) }
    prov := fun n c hc => by
      rcases upd_some hc with ⟨rfl, rfl⟩ | ⟨_, hc⟩
      · exact ⟨W, hW, Or.inr ⟨i, hi, rfl, rfl⟩⟩
      · exact hI.prov n c hc
    s0_broken := hI.s0_broken
    s0_own := hI.s0_own
    s0_le := hI.s0_le
    s0_writer := hI.s0_writer
    cur := fun e' W' hW' he => by
      have hc := hI.cur e' W' hW' he
      exact {
        pin := let ⟨s, h1, h2, h3⟩ := hc.pin; ⟨s, pos_ext (σ := σ) (by exact hext) (by rfl) (by rfl) h1, h2, h3⟩
        others := fun y h1 h2 h3 => let ⟨c, hc', he'⟩ := hc.others y h1 h2 h3; ⟨c, hext _ _ hc', he'⟩
        fenced := fun ha y hy => hext _ _ (hc.fenced ha y hy)
        frame := fun ha => pos_ext (σ := σ) (by exact hext) (by rfl) (by rfl) (hc.frame ha)
        after := hc.after }
    abs := fun e' W' hW' he => by
      have ha := hI.abs e' W' hW' he
      refine ⟨ha.base_le, fun hlt => ?_⟩
      by_cases hee : e' = e
      · -- The writer's own chunk: its frozen prefixes cannot grow past the foreign chunk at `b`.
        subst hee; rw [hW] at hW'; cases hW'
        rcases ha.frozen hlt with ⟨hin, _⟩ | ⟨k0, b, hk, hb, ⟨cb, hcb, hpb⟩, hocc, hj⟩
        · have := (hI.all e' W hW).planned_active (List.ne_nil_of_length_pos (by omega)); rw [hin] at this; cases this
        · refine Or.inr ⟨k0, b, hk, hb, ⟨cb, hext _ _ hcb, hpb⟩, fun n h1 h2 => ?_, fun j hjl hl => hj j hjl fun t h1 h2 => ?_⟩
          · intro hn; dsimp only at hn; cases hw : σ.wal n with
            | none => exact hocc n h1 h2 hw
            | some c'' => rw [hext _ _ hw] at hn; cases hn
          · have := hl t h1 h2
            dsimp only at this
            by_cases ht : W.own + t = W.own + i
            · exfalso
              rcases Nat.lt_or_ge (W.own + i) b with hlt' | hge
              · exact hocc _ (by omega) hlt' hfree
              · rcases Nat.lt_or_ge b (W.own + i) with hgt | heq
                · have hb' := hl (b - W.own) (by omega) (by omega)
                  dsimp only at hb'
                  rw [show W.own + (b - W.own) = b by omega, upd_ne _ _ (by omega), hcb] at hb'
                  cases hb'; simp [chunkAt, dataOf] at hpb
                · have : W.own + i = b := by omega
                  rw [this, hcb] at hfree; cases hfree
            · rwa [upd_ne _ _ ht] at this
      · exact frozen_upd hfree (Or.inl (by simp [chunkAt, dataOf]; exact hee ∘ Eq.symm)) (ha.frozen hlt) }

theorem inv_activate {σ : State} (hI : Inv σ) (e : Nat) (W : Writer) (hW : σ.writers e = some W)
    (hall : ∀ x ∈ W.fences, σ.wal x = some (markerOf e W.own)) :
    Inv { σ with writers := upd σ.writers e (some { W with active := true }) } := by
  let W' : Writer := { W with active := true }
  have hA := hI.all e W hW
  have hr := hI.range e W hW
  exact {
    top := hI.top
    epochs := hI.epochs
    range := fun e₂ W₂ h₂ => by
      rcases upd_some h₂ with ⟨he, _⟩ | ⟨_, h₂⟩
      · rw [he]; exact hr
      · exact hI.range e₂ W₂ h₂
    future := fun e₂ he => by dsimp only; rw [upd_ne _ _ (by arith)]; exact hI.future e₂ he
    all := fun e₂ W₂ h₂ => by
      rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
      · exact { hA with planned_active := fun _ => rfl }
      · exact hI.all e₂ W₂ h₂
    prov := fun n c hc => by
      obtain ⟨W₂, h₂, hcase⟩ := hI.prov n c hc
      by_cases he : c.epoch = e
      · rw [he] at h₂ hcase ⊢; rw [hW] at h₂; cases h₂
        exact ⟨W', upd_same _ _ _, hcase⟩
      · exact ⟨W₂, by dsimp only; rw [upd_ne _ _ he]; exact h₂, hcase⟩
    s0_broken := hI.s0_broken
    s0_own := hI.s0_own
    s0_le := hI.s0_le
    s0_writer := by
      rcases hI.s0_writer with h | ⟨W₂, h₂, ha, i, hi, h1, h2⟩
      · exact Or.inl h
      · by_cases he : σ.s0.maxE = e
        · rw [he] at h₂; rw [hW] at h₂; cases h₂
          exact Or.inr ⟨W', by dsimp only; rw [he, upd_same], rfl, i, hi, h1, h2⟩
        · exact Or.inr ⟨W₂, by dsimp only; rw [upd_ne _ _ he]; exact h₂, ha, i, hi, h1, h2⟩
    cur := fun e₂ W₂ h₂ he₂ => by
      have hafter : ∀ (W₃ : Writer), CurOk σ e₂ W₃ → (∀ e₃ W₄, upd σ.writers e (some W') e₃ = some W₄ →
          σ.s0.maxE < e₃ → e₃ < e₂ → W₄.ff ≤ W₃.ff ∧ (W₃.ff ∈ W₄.fences ∨ W₄.own ≤ W₃.ff)) := by
        intro W₃ hc e₃ W₄ h₄ h1 h2
        rcases upd_some h₄ with ⟨he₃, rfl⟩ | ⟨_, h₄⟩
        · rw [he₃] at h1 h2; exact hc.after e W hW h1 h2
        · exact hc.after e₃ W₄ h₄ h1 h2
      rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
      · have hc := hI.cur e₂ W hW he₂
        exact {
          pin := hc.pin
          others := hc.others
          fenced := fun _ => hall
          frame := fun _ => by have h := frame_of_fenced' hA hc hall; exact h
          after := hafter W hc }
      · have hc := hI.cur e₂ W₂ h₂ he₂
        exact { hc with after := hafter W₂ hc }
    abs := fun e₂ W₂ h₂ he₂ => by
      rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
      · have ha := hI.abs e₂ W hW he₂
        refine ⟨ha.base_le, fun hlt => ?_⟩
        rcases ha.frozen hlt with ⟨_, x, hx, c, hc, hp⟩ | h
        · rw [hall x hx] at hc; cases hc; simp [markerOf] at hp
        · exact Or.inr h
      · exact hI.abs e₂ W₂ h₂ he₂ }

theorem inv_plan {σ : State} (hI : Inv σ) (e : Nat) (W : Writer) (recs : List Nat) (hW : σ.writers e = some W)
    (ha : W.active = true) :
    Inv { σ with writers := upd σ.writers e (some { W with planned := W.planned ++ [recs] }) } := by
  let W' : Writer := { W with planned := W.planned ++ [recs] }
  have hA := hI.all e W hW
  have hr := hI.range e W hW
  have hchunk : ∀ i, i < W.planned.length → chunkAt W' e i = chunkAt W e i := by
    intro i hi; simp [chunkAt, W', List.getElem?_append_left hi]
  have htake : ∀ j, j ≤ W.planned.length → (W'.planned.take j).flatten = (W.planned.take j).flatten := by
    intro j hj; simp [W', List.take_append_of_le_length hj]
  exact {
    top := hI.top
    epochs := hI.epochs
    range := fun e₂ W₂ h₂ => by
      rcases upd_some h₂ with ⟨he, _⟩ | ⟨_, h₂⟩
      · rw [he]; exact hr
      · exact hI.range e₂ W₂ h₂
    future := fun e₂ he => by dsimp only; rw [upd_ne _ _ (by arith)]; exact hI.future e₂ he
    all := fun e₂ W₂ h₂ => by
      rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
      · exact {
          ff_lt := hA.ff_lt
          ff_mem := hA.ff_mem
          fences_range := hA.fences_range
          planned_active := fun _ => ha
          acked_le := by simp; have := hA.acked_le; omega
          acked_landed := fun i hi => by
            have hi' : i < W.acked := hi
            rw [hchunk i (by have := hA.acked_le; omega)]; exact hA.acked_landed i hi' }
      · exact hI.all e₂ W₂ h₂
    prov := fun n c hc => by
      obtain ⟨W₂, h₂, hcase⟩ := hI.prov n c hc
      by_cases he : c.epoch = e
      · rw [he] at h₂ hcase ⊢; rw [hW] at h₂; cases h₂
        refine ⟨W', upd_same _ _ _, ?_⟩
        rcases hcase with hm | ⟨i, hi, hn, hc'⟩
        · exact Or.inl hm
        · exact Or.inr ⟨i, by simp [W']; omega, hn, by rw [hchunk i hi]; exact hc'⟩
      · exact ⟨W₂, by dsimp only; rw [upd_ne _ _ he]; exact h₂, hcase⟩
    s0_broken := hI.s0_broken
    s0_own := hI.s0_own
    s0_le := hI.s0_le
    s0_writer := by
      rcases hI.s0_writer with h | ⟨W₂, h₂, ha₂, i, hi, h1, h2⟩
      · exact Or.inl h
      · by_cases he : σ.s0.maxE = e
        · rw [he] at h₂; rw [hW] at h₂; cases h₂
          exact Or.inr ⟨W', by dsimp only; rw [he, upd_same], ha, i, by simp [W']; omega, h1,
            by rw [h2, htake i hi]⟩
        · exact Or.inr ⟨W₂, by dsimp only; rw [upd_ne _ _ he]; exact h₂, ha₂, i, hi, h1, h2⟩
    cur := fun e₂ W₂ h₂ he₂ => by
      have hafter : ∀ (W₃ : Writer), CurOk σ e₂ W₃ → (∀ e₃ W₄, upd σ.writers e (some W') e₃ = some W₄ →
          σ.s0.maxE < e₃ → e₃ < e₂ → W₄.ff ≤ W₃.ff ∧ (W₃.ff ∈ W₄.fences ∨ W₄.own ≤ W₃.ff)) := by
        intro W₃ hc e₃ W₄ h₄ h1 h2
        rcases upd_some h₄ with ⟨he₃, rfl⟩ | ⟨_, h₄⟩
        · rw [he₃] at h1 h2; exact hc.after e W hW h1 h2
        · exact hc.after e₃ W₄ h₄ h1 h2
      rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
      · have hc := hI.cur e₂ W hW he₂
        exact {
          pin := hc.pin
          others := hc.others
          fenced := hc.fenced
          frame := hc.frame
          after := hafter W hc }
      · have hc := hI.cur e₂ W₂ h₂ he₂
        exact { hc with after := hafter W₂ hc }
    abs := fun e₂ W₂ h₂ he₂ => by
      rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
      · have hab := hI.abs e₂ W hW he₂
        refine ⟨hab.base_le, fun hlt => ?_⟩
        rcases hab.frozen hlt with ⟨hin, _⟩ | ⟨k0, b, hk, hb, hcb, hocc, hj⟩
        · rw [hin] at ha; cases ha
        · refine Or.inr ⟨k0, b, by simp; omega, hb, hcb, hocc, fun j hjl hl => ?_⟩
          by_cases hjo : j ≤ W.planned.length
          · rw [htake j hjo]
            exact hj j hjo fun t h1 h2 => by rw [← hchunk t (by omega)]; exact hl t h1 h2
          · -- The new last chunk cannot have landed: no chunk of this epoch exists at that name.
            exfalso
            have hlen : W.planned.length < j := by omega
            have hc' := hl W.planned.length (by omega) hlen
            obtain ⟨W₃, h₃, hcase⟩ := hI.prov _ _ hc'
            have he₃ : (chunkAt W' e₂ W.planned.length).epoch = e₂ := rfl
            rw [he₃, hW] at h₃; cases h₃
            rcases hcase with ⟨hm, _⟩ | ⟨i, hi, hn, _⟩
            · simp [chunkAt, dataOf, markerOf] at hm
            · arith
      · exact hI.abs e₂ W₂ h₂ he₂ }

theorem inv_ack {σ : State} (hI : Inv σ) (e : Nat) (W : Writer) (k : Nat) (hW : σ.writers e = some W)
    (hk : k ≤ W.planned.length) (hl : ∀ i, i < k → σ.wal (W.own + i) = some (chunkAt W e i)) :
    Inv { σ with writers := upd σ.writers e (some { W with acked := max W.acked k }) } := by
  let W' : Writer := { W with acked := max W.acked k }
  have hA := hI.all e W hW
  have hr := hI.range e W hW
  exact {
    top := hI.top
    epochs := hI.epochs
    range := fun e₂ W₂ h₂ => by
      rcases upd_some h₂ with ⟨he, _⟩ | ⟨_, h₂⟩
      · rw [he]; exact hr
      · exact hI.range e₂ W₂ h₂
    future := fun e₂ he => by dsimp only; rw [upd_ne _ _ (by arith)]; exact hI.future e₂ he
    all := fun e₂ W₂ h₂ => by
      rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
      · exact {
          ff_lt := hA.ff_lt
          ff_mem := hA.ff_mem
          fences_range := hA.fences_range
          planned_active := hA.planned_active
          acked_le := Nat.max_le.2 ⟨hA.acked_le, hk⟩
          acked_landed := fun i hi => by
            simp only at hi
            rcases Nat.lt_or_ge i W.acked with h' | h'
            · exact hA.acked_landed i h'
            · exact hl i (by omega) }
      · exact hI.all e₂ W₂ h₂
    prov := fun n c hc => by
      obtain ⟨W₂, h₂, hcase⟩ := hI.prov n c hc
      by_cases he : c.epoch = e
      · rw [he] at h₂ hcase ⊢; rw [hW] at h₂; cases h₂
        exact ⟨W', upd_same _ _ _, hcase⟩
      · exact ⟨W₂, by dsimp only; rw [upd_ne _ _ he]; exact h₂, hcase⟩
    s0_broken := hI.s0_broken
    s0_own := hI.s0_own
    s0_le := hI.s0_le
    s0_writer := by
      rcases hI.s0_writer with h | ⟨W₂, h₂, ha, i, hi, h1, h2⟩
      · exact Or.inl h
      · by_cases he : σ.s0.maxE = e
        · rw [he] at h₂; rw [hW] at h₂; cases h₂
          exact Or.inr ⟨W', by dsimp only; rw [he, upd_same], ha, i, hi, h1, h2⟩
        · exact Or.inr ⟨W₂, by dsimp only; rw [upd_ne _ _ he]; exact h₂, ha, i, hi, h1, h2⟩
    cur := fun e₂ W₂ h₂ he₂ => by
      have hafter : ∀ (W₃ : Writer), CurOk σ e₂ W₃ → (∀ e₃ W₄, upd σ.writers e (some W') e₃ = some W₄ →
          σ.s0.maxE < e₃ → e₃ < e₂ → W₄.ff ≤ W₃.ff ∧ (W₃.ff ∈ W₄.fences ∨ W₄.own ≤ W₃.ff)) := by
        intro W₃ hc e₃ W₄ h₄ h1 h2
        rcases upd_some h₄ with ⟨he₃, rfl⟩ | ⟨_, h₄⟩
        · rw [he₃] at h1 h2; exact hc.after e W hW h1 h2
        · exact hc.after e₃ W₄ h₄ h1 h2
      rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
      · have hc := hI.cur e₂ W hW he₂
        exact {
          pin := hc.pin
          others := hc.others
          fenced := hc.fenced
          frame := hc.frame
          after := hafter W hc }
      · have hc := hI.cur e₂ W₂ h₂ he₂
        exact { hc with after := hafter W₂ hc }
    abs := fun e₂ W₂ h₂ he₂ => by
      rcases upd_some h₂ with ⟨rfl, rfl⟩ | ⟨_, h₂⟩
      · have hab := hI.abs e₂ W hW he₂
        exact ⟨hab.base_le, fun h => hab.frozen h⟩
      · exact hI.abs e₂ W₂ h₂ he₂ }

/-! ## A flush -/

theorem frozen_out {wal : Nat → Option Chunk} {out out' : List Nat} {e : Nat} {W : Writer} (h : out <+: out')
    (hf : Frozen wal out e W) : Frozen wal out' e W := by
  rcases hf with hl | ⟨k0, b, hk, hb, hcb, hocc, hj⟩
  · exact Or.inl hl
  · exact Or.inr ⟨k0, b, hk, hb, hcb, hocc, fun j hjl hl => (hj j hjl hl).trans h⟩

theorem s0_eta (σ : State) : σ.s0 = ⟨σ.s0.maxE, σ.s0.own, σ.s0.broken, σ.s0.out⟩ := rfl

theorem inv_flush {σ : State} (hI : Inv σ) (e : Nat) (W : Writer) (i : Nat) (hW : σ.writers e = some W)
    (ha : W.active = true) (hnew : e = σ.epochs) (hi : i ≤ W.planned.length) (hs : σ.start ≤ W.own + i) :
    Inv { σ with start := W.own + i, s0 := ⟨e, W.own + i, false, W.base ++ (W.planned.take i).flatten⟩ } := by
  have hA := hI.all e W hW
  have hr := hI.range e W hW
  -- The flushing writer is newer than the manifest, or it wrote the manifest itself.
  have hcase : (σ.s0.maxE < e ∧ CurOk σ e W) ∨ (σ.s0.maxE = e ∧ ∃ i₀, i₀ ≤ W.planned.length ∧
      σ.start = W.own + i₀ ∧ σ.s0.out = W.base ++ (W.planned.take i₀).flatten) := by
    by_cases hlt : σ.s0.maxE < e
    · exact Or.inl ⟨hlt, hI.cur e W hW hlt⟩
    · have heq : σ.s0.maxE = e := by have := hI.s0_le; omega
      rcases hI.s0_writer with ⟨h0, _⟩ | ⟨W₀, h₀, _, i₀, hi₀, h1, h2⟩
      · omega
      · rw [heq, hW] at h₀; cases h₀
        exact Or.inr ⟨heq, i₀, hi₀, h1, h2⟩
  let out := W.base ++ (W.planned.take i).flatten
  have hold : σ.s0.out <+: out := by
    rcases hcase with ⟨_, hc⟩ | ⟨_, i₀, _, h1, h2⟩
    · obtain ⟨sF, hsF, hout, _⟩ := hc.pin
      have := (pos_mono (pos_start σ) hsF (pos_ge hsF)).2
      rw [hout] at this
      exact this.trans (List.prefix_append _ _)
    · rw [h2]; exact prefix_base (by omega)
  exact {
    top := hI.top
    epochs := hI.epochs
    range := hI.range
    future := hI.future
    all := hI.all
    prov := hI.prov
    s0_broken := rfl
    s0_own := rfl
    s0_le := by dsimp only; omega
    s0_writer := Or.inr ⟨W, hW, ha, i, hi, rfl, rfl⟩
    cur := fun e₂ W₂ h₂ he₂ => by
      have := hI.future e₂ (by dsimp only at he₂; omega); rw [this] at h₂; cases h₂
    abs := fun e₂ W₂ h₂ he₂ => by
      dsimp only at he₂ ⊢
      by_cases hee : e₂ = e
      · subst hee; rw [hW] at h₂; cases h₂
        exact ⟨List.prefix_append _ _, fun h => absurd h (Nat.lt_irrefl _)⟩
      have hlt : e₂ < e := by omega
      have hA₂ := hI.all e₂ W₂ h₂
      -- Older writers that began after the old manifest: the flushing writer listed after them.
      have hcur : σ.s0.maxE < e₂ → ∃ hc : CurOk σ e W, CurOk σ e₂ W₂ ∧ W₂.base <+: out := by
        intro hgt
        rcases hcase with ⟨_, hc⟩ | ⟨heq, _⟩
        · have hc₂ := hI.cur e₂ W₂ h₂ hgt
          obtain ⟨hff, _⟩ := hc.after e₂ W₂ h₂ hgt hlt
          obtain ⟨s₂, hs₂, hout₂, _⟩ := hc₂.pin
          obtain ⟨sF, hsF, houtF, _⟩ := hc.pin
          have := (pos_mono hs₂ hsF hff).2
          rw [hout₂, houtF] at this
          exact ⟨hc, hc₂, this.trans (List.prefix_append _ _)⟩
        · omega
      refine ⟨?_, fun _ => ?_⟩
      · by_cases hle : e₂ ≤ σ.s0.maxE
        · exact (hI.abs e₂ W₂ h₂ hle).base_le.trans hold
        · obtain ⟨_, _, h⟩ := hcur (by omega); exact h
      · by_cases hlt₀ : e₂ < σ.s0.maxE
        · exact frozen_out hold ((hI.abs e₂ W₂ h₂ (by omega)).frozen hlt₀)
        by_cases heq₀ : e₂ = σ.s0.maxE
        · -- The previous manifest's writer: its later chunks stop at the flushing writer's first marker.
          rcases hcase with ⟨_, hc⟩ | ⟨heq, _⟩
          · rcases hI.s0_writer with ⟨h0, _⟩ | ⟨W₀, h₀, ha₀, i₂, hi₂, h1, h2⟩
            · have := (hI.range e₂ W₂ h₂).1; omega
            rw [← heq₀, h₂] at h₀; cases h₀
            obtain ⟨sF, hsF, houtF, _⟩ := hc.pin
            have hF := hc.fenced ha W.ff hA.ff_mem
            have hge := pos_ge hsF
            have h0' : σ.s0 = ⟨e₂, σ.start, false, W₂.base ++ (W₂.planned.take i₂).flatten⟩ := by
              rw [s0_eta σ, ← heq₀, hI.s0_own, hI.s0_broken, h2]
            refine Or.inr ⟨i₂, W.ff, hi₂, by omega, ⟨_, hF, Or.inl (by simp [markerOf]; omega)⟩,
              fun n h3 h4 => ?_, fun j hjl hl => ?_⟩
            · obtain ⟨c, hc'⟩ := pos_present hsF n (by omega) h4
              rw [hc']; simp
            · by_cases hj : j ≤ i₂
              · exact (prefix_base hj).trans (by rw [← h2]; exact hold)
              · have hreach : W₂.own + j ≤ W.ff := by
                  apply Classical.byContradiction; intro hgt
                  have := hl (W.ff - W₂.own) (by omega) (by omega)
                  rw [show W₂.own + (W.ff - W₂.own) = W.ff by omega, hF] at this
                  cases this
                have hland := start_landed h0' (j - i₂) (by omega) (fun t ht => by
                  have := hl (i₂ + t) (by omega) (by omega)
                  rwa [show W₂.own + (i₂ + t) = σ.start + t by omega] at this)
                have := (pos_mono hland hsF (by omega)).2
                rw [show i₂ + (j - i₂) = j by omega, houtF] at this
                exact this.trans (List.prefix_append _ _)
          · omega
        · -- An older writer that began after the old manifest.
          obtain ⟨hc, hc₂, hbase⟩ := hcur (by omega)
          obtain ⟨_, hafter⟩ := hc.after e₂ W₂ h₂ (by omega) hlt
          obtain ⟨sF, hsF, houtF, _⟩ := hc.pin
          obtain ⟨s₂, hs₂, _, _⟩ := hc₂.pin
          have hF := hc.fenced ha W.ff hA.ff_mem
          have hge₂ := pos_ge hs₂
          have hocc : W₂.own ≤ W.ff → ∀ n, W₂.own + 0 ≤ n → n < W.ff → σ.wal n ≠ none := by
            intro hle n h3 h4
            obtain ⟨c, hc'⟩ := pos_present hsF n (by have := hA₂.ff_lt; omega) h4
            rw [hc']; simp
          by_cases ha₂ : W₂.active = true
          · have hown : W₂.own ≤ W.ff := by
              rcases hafter with hm | hle
              · have := hc₂.fenced ha₂ W.ff hm; rw [hF] at this; simp [markerOf] at this; omega
              · exact hle
            refine Or.inr ⟨0, W.ff, Nat.zero_le _, by omega, ⟨_, hF, Or.inl (by simp [markerOf]; omega)⟩,
              hocc hown, fun j hjl hl => ?_⟩
            have hreach : W₂.own + j ≤ W.ff := by
              apply Classical.byContradiction; intro hgt
              have := hl (W.ff - W₂.own) (Nat.zero_le _) (by omega)
              rw [show W₂.own + (W.ff - W₂.own) = W.ff by omega, hF] at this
              cases this
            have hland := frame_landed' hc₂ ha₂ j ⟨hjl, fun t ht => hl t (Nat.zero_le _) ht⟩
            have := (pos_mono hland hsF hreach).2
            rw [houtF] at this
            exact this.trans (List.prefix_append _ _)
          · have hpl : W₂.planned = [] := by
              apply Classical.byContradiction; intro h
              exact ha₂ (hA₂.planned_active h)
            rcases hafter with hm | hle
            · exact Or.inl ⟨by simpa using ha₂, W.ff, hm, _, hF, Or.inl (by simp [markerOf]; omega)⟩
            · refine Or.inr ⟨0, W.ff, Nat.zero_le _, by omega, ⟨_, hF, Or.inl (by simp [markerOf]; omega)⟩,
                hocc hle, fun j hjl _ => ?_⟩
              rw [hpl] at hjl ⊢
              have : j = 0 := by simpa using hjl
              subst this; simpa using hbase }

theorem inv_step {σ τ : State} (hI : Inv σ) (h : Step σ τ) : Inv τ := by
  cases h with
  | begin ff s fences hrun hff hf => exact inv_begin hI ff s fences hrun hff hf
  | landFence e x W hW hx hfree => exact inv_landFence hI e x W hW hx hfree
  | landData e i W hW hi hfree => exact inv_landData hI e i W hW hi hfree
  | activate e W hW hall => exact inv_activate hI e W hW hall
  | plan e W recs hW ha => exact inv_plan hI e W recs hW ha
  | ack e W k hW hk hl => exact inv_ack hI e W k hW hk hl
  | flush e W i hW ha hnew hi hs => exact inv_flush hI e W i hW ha hnew hi hs

theorem reach_inv {σ : State} (h : Reach σ) : Inv σ := by
  induction h with
  | init => exact inv_init
  | step _ hs ih => exact inv_step ih hs

/-! ## Replay from the newest manifest -/

def Q (σ : State) (p : Nat) (s : RS) : Prop :=
  (s.out = [] ∨ ∃ e W, σ.writers e = some W ∧ W.active = true ∧
    ∃ j, j ≤ W.planned.length ∧ s.out = W.base ++ (W.planned.take j).flatten) ∧
  (∀ W, σ.writers s.maxE = some W → W.active = true → s.broken = false → W.own ≤ p →
    s.own ≤ p ∧ p - W.own ≤ W.planned.length ∧ s.out = W.base ++ (W.planned.take (p - W.own)).flatten)

theorem q_run {σ : State} (hI : Inv σ) : ∀ d s, pos σ (σ.start + d) = some s → Q σ (σ.start + d) s
  | 0, s, h => by
    rw [Nat.add_zero, pos_start] at h; cases h
    rcases hI.s0_writer with ⟨h0, hout⟩ | ⟨W, hW, ha, i, hi, h1, h2⟩
    · refine ⟨Or.inl hout, fun W hW => ?_⟩
      rw [h0] at hW; have := (hI.range 0 W hW).1; omega
    · refine ⟨Or.inr ⟨_, W, hW, ha, i, hi, h2⟩, fun W' hW' _ _ _ => ?_⟩
      rw [hW] at hW'; cases hW'
      refine ⟨by rw [hI.s0_own]; omega, by omega, ?_⟩
      rw [h2, show σ.start + 0 - W.own = i by omega]
  | d + 1, s', h => by
    rw [show σ.start + (d + 1) = (σ.start + d) + 1 by omega] at h ⊢
    obtain ⟨s, c, hs, hc, rfl⟩ := (pos_succ (Nat.le_add_right _ _)).1 h
    obtain ⟨hq1, hq2⟩ := q_run hI d s hs
    have hmono : σ.s0.maxE ≤ s.maxE := (pos_mono (pos_start σ) hs (Nat.le_add_right _ _)).1
    obtain ⟨Wc, hWc, hcase⟩ := hI.prov _ c hc
    have hAc := hI.all c.epoch Wc hWc
    -- The writer of the start chunk owns no name from the start on except its own chunks.
    have hstartW : ∀ W, σ.writers σ.s0.maxE = some W → W.own ≤ σ.start := by
      intro W hW
      rcases hI.s0_writer with ⟨h0, _⟩ | ⟨W₀, h₀, _, i₀, _, h1, _⟩
      · rw [h0] at hW; have := (hI.range 0 W hW).1; omega
      · rw [h₀] at hW; cases hW; omega
    have hdata : ∀ i, i < Wc.planned.length → σ.start + d = Wc.own + i → c.epoch ≤ s.maxE := by
      intro i hi hk
      by_cases hle : c.epoch ≤ σ.s0.maxE
      · omega
      · have ha := hAc.planned_active (List.ne_nil_of_length_pos (by omega))
        have hfr := (hI.cur c.epoch Wc hWc (by omega)).frame ha
        exact (pos_mono hfr hs (by omega)).1
    have hframe : ∀ W, σ.writers (rstep .fixed s (σ.start + d) c).maxE = some W → W.active = true →
        W.own = σ.start + d + 1 →
        (rstep .fixed s (σ.start + d) c).own ≤ σ.start + d + 1 ∧ (σ.start + d + 1) - W.own ≤ W.planned.length ∧
        (rstep .fixed s (σ.start + d) c).out = W.base ++ (W.planned.take ((σ.start + d + 1) - W.own)).flatten := by
      intro W hW ha hown
      have hm := (rstep_mono .fixed s (σ.start + d) c).1
      by_cases hle : (rstep .fixed s (σ.start + d) c).maxE ≤ σ.s0.maxE
      · have := hstartW W (by rw [← show (rstep .fixed s (σ.start + d) c).maxE = σ.s0.maxE by omega]; exact hW)
        omega
      · have hfr := (hI.cur _ W hW (by omega)).frame ha
        rw [hown, h] at hfr
        rw [Option.some.inj hfr]
        simp [hown]
    by_cases h1 : c.epoch < s.maxE
    · -- A lower epoch is ignored; in an own name it breaks the replay's epoch.
      have hst := rstep_lower s (σ.start + d) c h1
      refine ⟨by rw [hst]; split <;> exact hq1, fun W hW ha hb hown => ?_⟩
      rcases Nat.lt_or_ge (σ.start + d) W.own with hlt | hge
      · exact hframe W hW ha (by omega)
      · rw [hst] at hW hb
        by_cases ho : s.own ≤ σ.start + d
        · simp [ho] at hb
        · simp only [ho, ite_false] at hW hb
          exact absurd (hq2 W hW ha hb hge).1 ho
    · rcases hcase with ⟨hm, hx⟩ | ⟨i, hi, hk, hci⟩
      · -- A writer's markers lie below its first own name, and the start writer's below the start.
        have hmk : c.marker = true := by rw [hm]; rfl
        have hxo := (hAc.fences_range _ hx).2
        have hcur : σ.s0.maxE < c.epoch := by
          apply Classical.byContradiction; intro hle
          have := hstartW Wc (by rw [show σ.s0.maxE = c.epoch by omega]; exact hWc)
          omega
        by_cases h2 : s.maxE < c.epoch
        · have hst := rstep_higher_marker s (σ.start + d) c h1 h2 hmk
          refine ⟨by rw [hst]; exact hq1, fun W hW ha _ hown => ?_⟩
          have hW' : W = Wc := by rw [hst] at hW; simp only at hW; rw [hWc] at hW; cases hW; rfl
          subst hW'
          have hco : c.own = W.own := by rw [hm]; rfl
          exact hframe W (by rw [hst]; exact hWc) ha (by omega)
        · have heq : c.epoch = s.maxE := by omega
          have hst := rstep_equal_marker s (σ.start + d) c heq hmk
          refine ⟨by rw [hst]; exact hq1, fun W hW ha _ hown => ?_⟩
          have hW' : W = Wc := by rw [hst, ← heq, hWc] at hW; cases hW; rfl
          subst hW'
          exact hframe W (by rw [hst, ← heq]; exact hWc) ha (by omega)
      · have hpl : Wc.planned[i]? = some (Wc.planned[i]) := List.getElem?_eq_getElem hi
        have hdm : c.marker = false := by rw [hci]; rfl
        have hdr : c.recs = Wc.planned[i] := by rw [hci]; simp [chunkAt, dataOf, hpl]
        have heq : c.epoch = s.maxE := by have := hdata i hi hk; omega
        by_cases hb : s.broken = true
        · have hst := rstep_equal_broken s (σ.start + d) c heq hb
          refine ⟨by rw [hst]; exact hq1, fun W hW ha hb' hown => ?_⟩
          rw [hst, hb] at hb'; cases hb'
        · have hb' : s.broken = false := by simpa using hb
          have ha := hAc.planned_active (List.ne_nil_of_length_pos (by omega))
          have hWs : σ.writers s.maxE = some Wc := by rw [← heq]; exact hWc
          obtain ⟨hown, hlen, hout⟩ := hq2 Wc hWs ha hb' (by omega)
          have hst := rstep_equal_data s (σ.start + d) c heq hdm hb'
          have hki : σ.start + d - Wc.own = i := by omega
          have hnew : s.out ++ c.recs = Wc.base ++ (Wc.planned.take (i + 1)).flatten := by
            rw [take_succ_flatten _ _ hi, hout, hki, hpl, hdr]; simp
          refine ⟨Or.inr ⟨c.epoch, Wc, hWc, ha, i + 1, hi, by rw [hst]; exact hnew⟩,
            fun W hW ha' hb'' hown' => ?_⟩
          have hW' : W = Wc := by rw [hst] at hW; simp only at hW; rw [hWs] at hW; cases hW; rfl
          subst hW'
          rw [hst]
          exact ⟨by simp only; omega, by omega, by rw [show σ.start + d + 1 - W.own = i + 1 by omega]; exact hnew⟩

/-- Every replay from the newest manifest, tables included, is empty or a prefix of one writer's history. -/
theorem flush_replays_one_history {σ : State} (h : Reach σ) {p : Nat} {s : RS} (hs : pos σ p = some s) :
    s.out = [] ∨ ∃ e W, σ.writers e = some W ∧ W.active = true ∧ s.out <+: hist W := by
  have hp := pos_ge hs
  rw [show p = σ.start + (p - σ.start) by omega] at hs
  rcases (q_run (reach_inv h) _ s hs).1 with h0 | ⟨e, W, hW, ha, j, _, hj⟩
  · exact Or.inl h0
  · refine Or.inr ⟨e, W, hW, ha, ?_⟩
    rw [hj]
    obtain ⟨t, ht⟩ := take_flatten_prefix W.planned j
    exact ⟨t, by rw [hist, List.append_assoc, ht]⟩

/-- A replay from the newest manifest that stops at its first missing name, as recovery and every refresh do,
holds every acknowledged history, tables included. -/
theorem flush_keeps_acknowledged {σ : State} (h : Reach σ) {e : Nat} {W : Writer} (hW : σ.writers e = some W)
    (ha : W.active = true) {p : Nat} {s : RS} (hs : pos σ p = some s) (hp : σ.wal p = none) :
    W.base ++ (W.planned.take W.acked).flatten <+: s.out := by
  have hI := reach_inv h
  have hA := hI.all e W hW
  have hge := pos_ge hs
  have htables : σ.s0.out <+: s.out := (pos_mono (pos_start σ) hs hge).2
  by_cases hcur : σ.s0.maxE < e
  · have hc := hI.cur e W hW hcur
    have hfr := frame_landed' hc ha W.acked ⟨hA.acked_le, hA.acked_landed⟩
    have hle : W.own + W.acked ≤ p := by
      apply Classical.byContradiction; intro hlt
      rcases Nat.lt_or_ge p W.own with h1 | h1
      · obtain ⟨c, hc'⟩ := pos_present (hc.frame ha) p hge h1
        rw [hp] at hc'; cases hc'
      · have := hA.acked_landed (p - W.own) (by omega)
        rw [show W.own + (p - W.own) = p by omega, hp] at this; cases this
    exact (pos_mono hfr hs hle).2
  · by_cases hlt : e < σ.s0.maxE
    · rcases (hI.abs e W hW (by omega)).frozen hlt with ⟨hin, _⟩ | ⟨k0, b, _, _, _, _, hj⟩
      · rw [hin] at ha; cases ha
      · exact (hj W.acked hA.acked_le fun t _ ht => hA.acked_landed t ht).trans htables
    · rcases hI.s0_writer with ⟨h0, _⟩ | ⟨W₀, h₀, _, i, hi, h1, h2⟩
      · have := (hI.range e W hW).1; omega
      rw [show σ.s0.maxE = e by omega, hW] at h₀; cases h₀
      by_cases hai : W.acked ≤ i
      · exact (prefix_base hai).trans (by rw [← h2]; exact htables)
      · have h0' : σ.s0 = ⟨e, σ.start, false, W.base ++ (W.planned.take i).flatten⟩ := by
          rw [s0_eta σ, show σ.s0.maxE = e by omega, hI.s0_own, hI.s0_broken, h2]
        have hland := start_landed h0' (W.acked - i) (by have := hA.acked_le; omega) (fun t ht => by
          have := hA.acked_landed (i + t) (by omega)
          rwa [show W.own + (i + t) = σ.start + t by omega] at this)
        have hle : σ.start + (W.acked - i) ≤ p := by
          apply Classical.byContradiction; intro hlt'
          have := hA.acked_landed (p - W.own) (by omega)
          rw [show W.own + (p - W.own) = p by omega, hp] at this; cases this
        have := (pos_mono hland hs hle).2
        rwa [show i + (W.acked - i) = W.acked by omega] at this

/-- A flush whose own chunk never landed: its record is in the tables, and a reader from the manifest sees it. This
is the case `Wal.restart_agrees` cannot cover. -/
theorem flush_of_unlanded_chunk : ∃ σ, Reach σ ∧ σ.wal 1 = none ∧ pos σ σ.start = some ⟨1, 2, false, [7]⟩ := by
  have r1 := Reach.step Reach.init (Step.begin init 0 Wal.RS.init [0] rfl rfl (by intro x; simp [init]))
  have r2 := Reach.step r1 (Step.landFence _ 1 0 _ rfl (by simp) rfl)
  have r3 := Reach.step r2 (Step.activate _ 1 _ rfl (by decide))
  have r4 := Reach.step r3 (Step.plan _ 1 _ [7] rfl rfl)
  have r5 := Reach.step r4 (Step.flush _ 1 _ 1 rfl rfl rfl (by decide) (by decide))
  exact ⟨_, r5, rfl, pos_start _⟩

end Lsm.WalFlush
