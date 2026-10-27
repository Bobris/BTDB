import EventLogProofs.Common

/-!
# Exactly-once publication across owner changes

A model of how an owner lane (`EventLogOwnerLane.SubmitAsync` and `ResolveSessionAsync`) and a publisher
(`EventLogTopic.PrepareRound`, `Resolve`) agree on whether a publisher sequence is already committed.

The topic is abstracted to one canonical history `h` of transfer identities `(session, sequence)` guarded by one
version `v`: a lane commits only while its version is current. `Chain.lean` proves that the split chain behaves this
way. Messages are never removed from `net`, so every submission can be delivered late, repeatedly and to any lane.

Proved for `Rule.fixed` (`fixed_*` theorems): no transfer is committed twice, every session's committed sequences are
exactly `1, 2, …, k` in call order (contract 8), and every receipt names the record at its offset.
`previous_duplicates` is a reachable duplicate under `Rule.previous`, the rule implemented before the fix of
2026-10-27. `Rule.fixed` is the implemented rule.
-/

namespace EventLog.ExactlyOnce

/-- A transfer identity: publisher session `s` and sequence `q ≥ 1`. -/
structure Entry where
  s : Nat
  q : Nat
deriving DecidableEq, Repr

/-- The sequences of session `s` in history order. -/
def proj (s : Nat) : List Entry → List Nat
  | [] => []
  | e :: es => if e.s = s then e.q :: proj s es else proj s es

def cnt (s : Nat) (h : List Entry) : Nat := (proj s h).length

def listMax : List Nat → Nat
  | [] => 0
  | x :: xs => max x (listMax xs)

/-- `ResolveSessionAsync`: the highest sequence of `s`, at least `o`, in the frames from offset `rf`, and at least
`o - 1` (`committed = oldestUnresolved - 1`). -/
def scan (s o rf : Nat) (view : List Entry) : Nat :=
  max (o - 1) (listMax ((proj s (view.drop rf)).filter (fun x => decide (o ≤ x))))

/-- One submitted record: session, sequence, `oldestUnresolved`, `previouslyDispatched`, `resumeFrom`. -/
structure Msg where
  s : Nat
  q : Nat
  o : Nat
  prev : Bool
  rf : Nat
deriving DecidableEq, Repr

/-- An owner lane: the history it read or wrote, the version it holds, `SessionState.Accepted` per known session, and
the accepted records not yet committed (pending and in-flight batches, in queue order). -/
structure Lane where
  view : List Entry
  ver : Nat
  known : Nat → Option Nat
  queue : List Entry

/-- A publisher session: which sequences have a receipt, `Outstanding.ResumeFrom` of dispatched sequences, and
`_knownNext`. -/
structure Pub where
  resolved : Nat → Bool
  rf : Nat → Option Nat
  knownNext : Nat

structure State where
  h : List Entry
  v : Nat
  lanes : Nat → Option Lane
  pubs : Nat → Pub
  net : List Msg
  rcpt : List (Entry × Nat)

def receipts (base : Nat) : List Entry → List (Entry × Nat)
  | [] => []
  | e :: es => (e, base) :: receipts (base + 1) es

/-- `previous`: a lane that does not know the session trusts `previouslyDispatched = false` and starts from
`oldestUnresolved - 1`. `fixed`: it always scans from `resumeFrom`; the scan is empty when `resumeFrom` is at or beyond
the lane's next offset. -/
inductive Rule where
  | previous
  | fixed
deriving DecidableEq

def learned : Rule → Msg → List Entry → Nat
  | .previous, m, view => if m.prev then scan m.s m.o m.rf view else m.o - 1
  | .fixed, m, view => scan m.s m.o m.rf view

def init : State := ⟨[], 0, fun _ => none, fun _ => ⟨fun _ => false, fun _ => none, 0⟩, [], []⟩

inductive Step (r : Rule) : State → State → Prop
  /-- `CreateCandidateAsync`: a fresh lane from the current tail. -/
  | newLane (σ : State) (i : Nat) :
      Step r σ { σ with lanes := upd σ.lanes i (some ⟨σ.h, σ.v, fun _ => none, []⟩) }
  /-- A lane stops (fenced, crashed or lost an ambiguous write); its node may later create another. -/
  | stop (σ : State) (i : Nat) :
      Step r σ { σ with lanes := upd σ.lanes i none }
  /-- `InstallAsync`: a content-preserving conditional owner change. -/
  | install (σ : State) (i : Nat) (L : Lane) (hL : σ.lanes i = some L) (hv : L.ver = σ.v) :
      Step r σ { σ with v := σ.v + 1, lanes := upd σ.lanes i (some { L with ver := σ.v + 1 }) }
  /-- `PrepareRound` for a round containing `q`: `o` is the oldest unresolved sequence, `rf` is at most
  `_knownNext` and the `ResumeFrom` of `o` when `o` was dispatched, and `prev` may be false only when no unresolved
  sequence up to `q` was dispatched. Sending marks `q` dispatched at `_knownNext`. -/
  | send (σ : State) (s q o rf : Nat) (prev : Bool)
      (ho : 1 ≤ o) (hoq : o ≤ q)
      (hres : ∀ x, 1 ≤ x → x < o → (σ.pubs s).resolved x = true)
      (hrf : rf ≤ (σ.pubs s).knownNext)
      (hrfo : ∀ r', (σ.pubs s).rf o = some r' → rf ≤ r')
      (hprev : prev = false → ∀ x, o ≤ x → x ≤ q → (σ.pubs s).resolved x = false → (σ.pubs s).rf x = none) :
      Step r σ { σ with
        net := σ.net ++ [⟨s, q, o, prev, rf⟩],
        pubs := upd σ.pubs s { σ.pubs s with
          rf := upd (σ.pubs s).rf q (some (((σ.pubs s).rf q).getD (σ.pubs s).knownNext)) } }
  /-- A lane meets a session for the first time and creates its `SessionState`. -/
  | learn (σ : State) (i : Nat) (L : Lane) (m : Msg) (hL : σ.lanes i = some L) (hm : m ∈ σ.net)
      (hk : L.known m.s = none) :
      Step r σ { σ with lanes := upd σ.lanes i (some { L with
        known := upd L.known m.s (some (learned r m L.view)) }) }
  /-- `AcceptLocked`: the next sequence of a known session joins the queue. -/
  | accept (σ : State) (i : Nat) (L : Lane) (m : Msg) (a : Nat) (hL : σ.lanes i = some L) (hm : m ∈ σ.net)
      (hk : L.known m.s = some a) (hq : m.q = a + 1) :
      Step r σ { σ with lanes := upd σ.lanes i (some { L with
        known := upd L.known m.s (some (a + 1)), queue := L.queue ++ [⟨m.s, m.q⟩] }) }
  /-- `CommitAsync`: a conditional write of the oldest `n` queued records succeeds only on the current version. -/
  | commit (σ : State) (i : Nat) (L : Lane) (n : Nat) (hL : σ.lanes i = some L) (hv : L.ver = σ.v) :
      Step r σ { σ with
        h := σ.h ++ L.queue.take n, v := σ.v + 1,
        lanes := upd σ.lanes i (some { L with
          view := σ.h ++ L.queue.take n, ver := σ.v + 1, queue := L.queue.drop n }),
        rcpt := σ.rcpt ++ receipts σ.h.length (L.queue.take n) }
  /-- A lane answers with an offset it knows (`SessionState.Offsets`, a scan, or committed content it found). -/
  | answer (σ : State) (i : Nat) (L : Lane) (e : Entry) (k : Nat) (hL : σ.lanes i = some L)
      (he : L.view[k]? = some e) :
      Step r σ { σ with rcpt := σ.rcpt ++ [(e, k)] }
  /-- `Resolve`: a publisher receives a receipt. -/
  | resolve (σ : State) (e : Entry) (k : Nat) (he : (e, k) ∈ σ.rcpt) :
      Step r σ { σ with pubs := upd σ.pubs e.s { σ.pubs e.s with
        resolved := upd (σ.pubs e.s).resolved e.q true,
        knownNext := max (σ.pubs e.s).knownNext (k + 1) } }
  /-- `PeerAnswered` or a tail read: a publisher learns a durable next offset. -/
  | hear (σ : State) (s n : Nat) (hn : n ≤ σ.h.length) :
      Step r σ { σ with pubs := upd σ.pubs s { σ.pubs s with knownNext := max (σ.pubs s).knownNext n } }

inductive Reach (r : Rule) : State → Prop
  | init : Reach r init
  | step {σ τ : State} : Reach r σ → Step r σ τ → Reach r τ

/-! ## The counterexample for the previous rule -/

/-- A record committed twice by the previous rule: the retry of sequence 1 commits through lane 0, a later lane 1
receives the delayed first dispatch (`previouslyDispatched = false`) and commits it again. -/
theorem previous_duplicates : ∃ σ, Reach .previous σ ∧ ¬ σ.h.Nodup := by
  have r1 := Reach.step Reach.init (Step.newLane (r := .previous) init 0)
  have r2 := Reach.step r1 (Step.send _ 1 1 1 0 false (by decide) (by decide)
    (fun x h1 h2 => absurd h2 (by omega)) (by decide) (by decide) (fun _ x h1 h2 _ => by simp [init]))
  have r3 := Reach.step r2 (Step.send _ 1 1 1 0 true (by decide) (by decide)
    (fun x h1 h2 => absurd h2 (by omega)) (by decide) (by simp [upd]) (by simp))
  have r4 := Reach.step r3 (Step.learn _ 0 _ ⟨1, 1, 1, true, 0⟩ rfl (by simp) rfl)
  have r5 := Reach.step r4 (Step.accept _ 0 _ ⟨1, 1, 1, true, 0⟩ 0 rfl (by simp) (by simp [upd, learned, scan, listMax, proj, init]) rfl)
  have r6 := Reach.step r5 (Step.commit _ 0 _ 1 rfl rfl)
  have r7 := Reach.step r6 (Step.resolve _ ⟨1, 1⟩ 0 (by simp [receipts, init]))
  have r8 := Reach.step r7 (Step.newLane _ 1)
  have r9 := Reach.step r8 (Step.learn _ 1 _ ⟨1, 1, 1, false, 0⟩ rfl (by simp) rfl)
  have r10 := Reach.step r9 (Step.accept _ 1 _ ⟨1, 1, 1, false, 0⟩ 0 rfl (by simp) (by simp [upd, learned]) rfl)
  have r11 := Reach.step r10 (Step.commit _ 1 _ 1 rfl rfl)
  exact ⟨_, r11, by decide⟩

/-- The fixed rule still publishes: the results below are not vacuous. -/
theorem fixed_commits : ∃ σ, Reach .fixed σ ∧ σ.h = [⟨1, 1⟩] := by
  have r1 := Reach.step Reach.init (Step.newLane (r := .fixed) init 0)
  have r2 := Reach.step r1 (Step.send _ 1 1 1 0 false (by decide) (by decide)
    (fun x h1 h2 => absurd h2 (by omega)) (by decide) (by decide) (fun _ x h1 h2 _ => by simp [init]))
  have r3 := Reach.step r2 (Step.learn _ 0 _ ⟨1, 1, 1, false, 0⟩ rfl (by simp) rfl)
  have r4 := Reach.step r3 (Step.accept _ 0 _ ⟨1, 1, 1, false, 0⟩ 0 rfl (by simp)
    (by simp [upd, learned, scan, listMax, proj, init]) rfl)
  have r5 := Reach.step r4 (Step.commit _ 0 _ 1 rfl rfl)
  exact ⟨_, r5, rfl⟩

/-! ## List lemmas -/

theorem proj_append (s : Nat) (a b : List Entry) : proj s (a ++ b) = proj s a ++ proj s b := by
  induction a with
  | nil => rfl
  | cons e es ih => simp only [List.cons_append, proj]; split <;> simp [ih]

theorem mem_proj {s x : Nat} {l : List Entry} : x ∈ proj s l ↔ (⟨s, x⟩ : Entry) ∈ l := by
  induction l with
  | nil => simp [proj]
  | cons e es ih =>
    cases e with
    | mk a b =>
      simp only [proj, List.mem_cons]
      by_cases h : a = s
      · subst h; simp [ih]
      · simp only [h, ite_false, ih, Entry.mk.injEq]
        constructor
        · exact Or.inr
        · rintro (⟨rfl, _⟩ | hm)
          · exact absurd rfl h
          · exact hm

theorem nodup_of_proj : ∀ l : List Entry, (∀ s, (proj s l).Nodup) → l.Nodup
  | [], _ => List.nodup_nil
  | e :: es, h => by
    rw [List.nodup_cons]
    have he := h e.s
    simp only [proj, ite_true, List.nodup_cons] at he
    refine ⟨fun hm => he.1 (mem_proj.2 hm), nodup_of_proj es fun s => ?_⟩
    have hs := h s
    simp only [proj] at hs
    split at hs
    · exact (List.nodup_cons.1 hs).2
    · exact hs

theorem le_listMax {x : Nat} : ∀ {l : List Nat}, x ∈ l → x ≤ listMax l
  | y :: ys, h => by
    simp only [listMax]
    rcases List.mem_cons.1 h with rfl | h
    · exact Nat.le_max_left _ _
    · exact Nat.le_trans (le_listMax h) (Nat.le_max_right _ _)

theorem listMax_le {c : Nat} : ∀ {l : List Nat}, (∀ x ∈ l, x ≤ c) → listMax l ≤ c
  | [], _ => Nat.zero_le _
  | y :: ys, h => by
    simp only [listMax]
    exact Nat.max_le.2 ⟨h y (by simp), listMax_le fun x hx => h x (by simp [hx])⟩

theorem receipts_mem : ∀ (b : List Entry) (base : Nat) (p : Entry × Nat), p ∈ receipts base b →
    base ≤ p.2 ∧ b[p.2 - base]? = some p.1
  | [], _, _, h => by simp [receipts] at h
  | e :: es, base, p, h => by
    simp only [receipts, List.mem_cons] at h
    rcases h with rfl | h
    · simp
    · obtain ⟨h1, h2⟩ := receipts_mem es (base + 1) p h
      refine ⟨by arith, ?_⟩
      rw [show p.2 - base = (p.2 - (base + 1)) + 1 by arith]
      simpa using h2

theorem proj_take (s n : Nat) (h : List Entry) (hh : proj s h = List.range' 1 (cnt s h)) :
    proj s (h.take n) = List.range' 1 (cnt s (h.take n)) := by
  have e : proj s (h.take n) ++ proj s (h.drop n) = List.range' 1 (cnt s h) := by
    rw [← proj_append, List.take_append_drop]; exact hh
  exact (range'_split _ _ _ _ e).1

/-- The scan finds exactly the committed count when every sequence below `o` is committed and every committed
sequence from `o` on lies at or after `rf`. -/
theorem scan_eq (h : List Entry) (s o rf : Nat)
    (hh : proj s h = List.range' 1 (cnt s h))
    (hbelow : ∀ x, 1 ≤ x → x < o → (⟨s, x⟩ : Entry) ∈ h)
    (hbefore : ∀ x, (⟨s, x⟩ : Entry) ∈ h.take rf → x < o) :
    scan s o rf h = cnt s h := by
  have hle : ∀ x, (⟨s, x⟩ : Entry) ∈ h → 1 ≤ x ∧ x ≤ cnt s h := by
    intro x hx
    have hx' := mem_proj.2 hx
    rw [hh, mem_range'_iff] at hx'
    arith
  unfold scan
  apply Nat.le_antisymm
  · apply Nat.max_le.2
    constructor
    · by_cases ho : 2 ≤ o
      · have := hle (o - 1) (hbelow (o - 1) (by arith) (by arith)); arith
      · arith
    · apply listMax_le
      intro x hx
      rw [List.mem_filter] at hx
      exact (hle x (List.mem_of_mem_drop (mem_proj.1 hx.1))).2
  · by_cases hk : o ≤ cnt s h ∧ 1 ≤ cnt s h
    · have hin : (⟨s, cnt s h⟩ : Entry) ∈ h := by
        apply mem_proj.1
        rw [hh, mem_range'_iff]; arith
      rcases mem_take_or_drop rf hin with ht | hd
      · have := hbefore _ ht; arith
      · have : cnt s h ∈ (proj s (h.drop rf)).filter (fun x => decide (o ≤ x)) := by
          rw [List.mem_filter]; exact ⟨mem_proj.2 hd, by simp [hk.1]⟩
        exact Nat.le_trans (le_listMax this) (Nat.le_max_right _ _)
    · arith

/-! ## The invariant -/

/-- What a lane that holds the current version knows about session `s` is exact. -/
def KnownOk (h : List Entry) (L : Lane) (s : Nat) : Prop :=
  (L.known s = none → proj s L.queue = []) ∧
  (∀ a, L.known s = some a → cnt s h ≤ a ∧ proj s L.queue = List.range' (cnt s h + 1) (a - cnt s h))

structure MsgOk (h : List Entry) (pubs : Nat → Pub) (m : Msg) : Prop where
  o_pos : 1 ≤ m.o
  rf_le : m.rf ≤ h.length
  below : ∀ x, 1 ≤ x → x < m.o → (pubs m.s).resolved x = true
  before : ∀ x, (⟨m.s, x⟩ : Entry) ∈ h.take m.rf → x < m.o
  dispatched : (pubs m.s).rf m.q ≠ none

structure Inv (σ : State) : Prop where
  hist : ∀ s, proj s σ.h = List.range' 1 (cnt s σ.h)
  rcpt : ∀ p ∈ σ.rcpt, σ.h[p.2]? = some p.1
  view : ∀ i L, σ.lanes i = some L → L.view <+: σ.h ∧ L.ver ≤ σ.v ∧ (L.ver = σ.v → L.view = σ.h)
  known : ∀ i L, σ.lanes i = some L → L.ver = σ.v → ∀ s, KnownOk σ.h L s
  resolvedIn : ∀ s x, (σ.pubs s).resolved x = true → (⟨s, x⟩ : Entry) ∈ σ.h
  knownNext : ∀ s, (σ.pubs s).knownNext ≤ σ.h.length
  rfOk : ∀ s x r, (σ.pubs s).rf x = some r → r ≤ σ.h.length ∧ (⟨s, x⟩ : Entry) ∉ σ.h.take r
  undispatched : ∀ s x, (σ.pubs s).rf x = none → (⟨s, x⟩ : Entry) ∉ σ.h ∧
    (∀ i L, σ.lanes i = some L → (⟨s, x⟩ : Entry) ∉ L.queue) ∧ (∀ m ∈ σ.net, m.s = s → m.q ≠ x)
  msg : ∀ m ∈ σ.net, MsgOk σ.h σ.pubs m

theorem inv_init : Inv init where
  hist _ := rfl
  rcpt _ h := by simp [init] at h
  view _ _ h := by simp [init] at h
  known _ _ h := by simp [init] at h
  resolvedIn _ _ h := by simp [init] at h
  knownNext _ := Nat.le_refl _
  rfOk _ _ _ h := by simp [init] at h
  undispatched _ _ _ := ⟨by simp [init], fun _ _ h => by simp [init] at h, fun _ h => by simp [init] at h⟩
  msg _ h := by simp [init] at h

section Steps

variable {σ : State} (hI : Inv σ)
include hI

theorem inv_newLane (i : Nat) :
    Inv { σ with lanes := upd σ.lanes i (some ⟨σ.h, σ.v, fun _ => none, []⟩) } where
  hist := hI.hist
  rcpt := hI.rcpt
  view j L hL := by
    rcases upd_some hL with ⟨_, rfl⟩ | ⟨_, hL⟩
    · exact ⟨List.prefix_refl _, Nat.le_refl _, fun _ => rfl⟩
    · exact hI.view j L hL
  known j L hL hv s := by
    rcases upd_some hL with ⟨_, rfl⟩ | ⟨_, hL⟩
    · exact ⟨fun _ => rfl, fun _ h => by simp at h⟩
    · exact hI.known j L hL hv s
  resolvedIn := hI.resolvedIn
  knownNext := hI.knownNext
  rfOk := hI.rfOk
  undispatched s x hx := by
    obtain ⟨h1, h2, h3⟩ := hI.undispatched s x hx
    refine ⟨h1, fun j L hL => ?_, h3⟩
    rcases upd_some hL with ⟨_, rfl⟩ | ⟨_, hL⟩
    · simp
    · exact h2 j L hL
  msg := hI.msg

theorem inv_stop (i : Nat) : Inv { σ with lanes := upd σ.lanes i none } where
  hist := hI.hist
  rcpt := hI.rcpt
  view j L hL := by
    have ⟨_, hL⟩ := upd_none hL
    exact hI.view j L hL
  known j L hL hv s := by
    have ⟨_, hL⟩ := upd_none hL
    exact hI.known j L hL hv s
  resolvedIn := hI.resolvedIn
  knownNext := hI.knownNext
  rfOk := hI.rfOk
  undispatched s x hx := by
    obtain ⟨h1, h2, h3⟩ := hI.undispatched s x hx
    refine ⟨h1, fun j L hL => ?_, h3⟩
    have ⟨_, hL⟩ := upd_none hL
    exact h2 j L hL
  msg := hI.msg

theorem inv_install (i : Nat) (L : Lane) (hL : σ.lanes i = some L) (hv : L.ver = σ.v) :
    Inv { σ with v := σ.v + 1, lanes := upd σ.lanes i (some { L with ver := σ.v + 1 }) } where
  hist := hI.hist
  rcpt := hI.rcpt
  view j L' hL' := by
    rcases upd_some hL' with ⟨_, rfl⟩ | ⟨_, hL'⟩
    · exact ⟨(hI.view i L hL).1, Nat.le_refl _, fun _ => (hI.view i L hL).2.2 hv⟩
    · obtain ⟨h1, h2, _⟩ := hI.view j L' hL'
      exact ⟨h1, by simp; arith, fun h => by simp at h; arith⟩
  known j L' hL' hv' s := by
    rcases upd_some hL' with ⟨_, rfl⟩ | ⟨_, hL'⟩
    · exact hI.known i L hL hv s
    · have := (hI.view j L' hL').2.1
      simp at hv'; arith
  resolvedIn := hI.resolvedIn
  knownNext := hI.knownNext
  rfOk := hI.rfOk
  undispatched s x hx := by
    obtain ⟨h1, h2, h3⟩ := hI.undispatched s x hx
    refine ⟨h1, fun j L' hL' => ?_, h3⟩
    rcases upd_some hL' with ⟨_, rfl⟩ | ⟨_, hL'⟩
    · exact h2 i L hL
    · exact h2 j L' hL'
  msg := hI.msg

/-- The new message's `resumeFrom` precedes every committed sequence from `oldestUnresolved` on. -/
theorem send_before (s o rf : Nat) (ho : 1 ≤ o)
    (hrfo : ∀ r', (σ.pubs s).rf o = some r' → rf ≤ r') :
    ∀ x, (⟨s, x⟩ : Entry) ∈ σ.h.take rf → x < o := by
  intro x hx
  apply Classical.byContradiction
  intro hxo
  have hp := proj_take s rf σ.h (hI.hist s)
  have hx' := mem_proj.2 hx
  rw [hp, mem_range'_iff] at hx'
  have ho' : (⟨s, o⟩ : Entry) ∈ σ.h.take rf := by
    apply mem_proj.1; rw [hp, mem_range'_iff]; arith
  cases hr : (σ.pubs s).rf o with
  | none => exact (hI.undispatched s o hr).1 (List.mem_of_mem_take ho')
  | some r' =>
    have hle := hrfo r' hr
    apply (hI.rfOk s o r' hr).2
    have : σ.h.take rf = (σ.h.take r').take rf := by rw [List.take_take, Nat.min_eq_left hle]
    rw [this] at ho'
    exact List.mem_of_mem_take ho'

theorem inv_send (s q o rf : Nat) (prev : Bool) (ho : 1 ≤ o)
    (hres : ∀ x, 1 ≤ x → x < o → (σ.pubs s).resolved x = true)
    (hrf : rf ≤ (σ.pubs s).knownNext)
    (hrfo : ∀ r', (σ.pubs s).rf o = some r' → rf ≤ r') :
    Inv { σ with
      net := σ.net ++ [⟨s, q, o, prev, rf⟩],
      pubs := upd σ.pubs s { σ.pubs s with
        rf := upd (σ.pubs s).rf q (some (((σ.pubs s).rf q).getD (σ.pubs s).knownNext)) } } := by
  have hres' : ∀ s', (upd σ.pubs s { σ.pubs s with
      rf := upd (σ.pubs s).rf q (some (((σ.pubs s).rf q).getD (σ.pubs s).knownNext)) } s').resolved =
      (σ.pubs s').resolved := by
    intro s'; by_cases h : s' = s <;> simp [upd, h]
  have hkn' : ∀ s', (upd σ.pubs s { σ.pubs s with
      rf := upd (σ.pubs s).rf q (some (((σ.pubs s).rf q).getD (σ.pubs s).knownNext)) } s').knownNext =
      (σ.pubs s').knownNext := by
    intro s'; by_cases h : s' = s <;> simp [upd, h]
  have hrf' : ∀ s' x, (upd σ.pubs s { σ.pubs s with
      rf := upd (σ.pubs s).rf q (some (((σ.pubs s).rf q).getD (σ.pubs s).knownNext)) } s').rf x =
      if s' = s ∧ x = q then some (((σ.pubs s).rf q).getD (σ.pubs s).knownNext) else (σ.pubs s').rf x := by
    intro s' x
    by_cases h : s' = s
    · subst h; by_cases hx : x = q <;> simp [upd, hx]
    · simp [upd, h]
  exact {
    hist := hI.hist
    rcpt := hI.rcpt
    view := hI.view
    known := hI.known
    resolvedIn := fun s' x h => hI.resolvedIn s' x (by rw [← hres']; exact h)
    knownNext := fun s' => by rw [hkn']; exact hI.knownNext s'
    rfOk := fun s' x r h => by
      rw [hrf'] at h
      split at h
      · rename_i hc
        obtain ⟨rfl, rfl⟩ := hc
        cases hq : (σ.pubs s').rf x with
        | some r0 => simp [hq] at h; subst h; exact hI.rfOk s' x r0 hq
        | none =>
          simp [hq] at h; subst h
          exact ⟨hI.knownNext s', fun hm => (hI.undispatched s' x hq).1 (List.mem_of_mem_take hm)⟩
      · exact hI.rfOk s' x r h
    undispatched := fun s' x h => by
      rw [hrf'] at h
      split at h
      · cases h
      · rename_i hc
        obtain ⟨h1, h2, h3⟩ := hI.undispatched s' x h
        refine ⟨h1, h2, fun m hm hms => ?_⟩
        rcases List.mem_append.1 hm with hm | hm
        · exact h3 m hm hms
        · simp at hm; subst hm; simp at hms ⊢; intro hx; exact hc ⟨hms.symm, hx.symm⟩
    msg := fun m hm => by
      rcases List.mem_append.1 hm with hm | hm
      · have hk := hI.msg m hm
        exact {
          o_pos := hk.o_pos
          rf_le := hk.rf_le
          below := fun x h1 h2 => by rw [hres']; exact hk.below x h1 h2
          before := hk.before
          dispatched := by
            rw [hrf']; split
            · simp
            · exact hk.dispatched }
      · simp at hm; subst hm
        exact {
          o_pos := ho
          rf_le := Nat.le_trans hrf (hI.knownNext s)
          below := fun x h1 h2 => by rw [hres']; exact hres x h1 h2
          before := send_before hI s o rf ho hrfo
          dispatched := by rw [hrf']; simp } }

theorem inv_learn (i : Nat) (L : Lane) (m : Msg) (hL : σ.lanes i = some L) (hm : m ∈ σ.net)
    (hk : L.known m.s = none) :
    Inv { σ with lanes := upd σ.lanes i (some { L with
      known := upd L.known m.s (some (learned .fixed m L.view)) }) } := by
  let L' : Lane := { L with known := upd L.known m.s (some (learned .fixed m L.view)) }
  exact {
    hist := hI.hist
    rcpt := hI.rcpt
    view := fun j L₂ hL₂ => by
      rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
      · exact hI.view i L hL
      · exact hI.view j L₂ hL₂
    known := fun j L₂ hL₂ hv s => by
      rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
      · have hold := hI.known i L hL hv
        by_cases hs : s = m.s
        · subst hs
          have hview := (hI.view i L hL).2.2 hv
          have hmk := hI.msg m hm
          have hscan : learned .fixed m L.view = cnt m.s σ.h := by
            simp only [learned, hview]
            exact scan_eq σ.h m.s m.o m.rf (hI.hist m.s)
              (fun x h1 h2 => hI.resolvedIn m.s x (hmk.below x h1 h2)) hmk.before
          refine ⟨fun h => by simp [upd] at h, fun a ha => ?_⟩
          simp [upd] at ha
          subst ha
          rw [hscan, Nat.sub_self]
          exact ⟨Nat.le_refl _, (hold m.s).1 hk⟩
        · have h1 : L'.known s = L.known s := by simp [L', upd, hs]
          exact ⟨fun h => (hold s).1 (h1 ▸ h), fun a h => (hold s).2 a (h1 ▸ h)⟩
      · exact hI.known j L₂ hL₂ hv s
    resolvedIn := hI.resolvedIn
    knownNext := hI.knownNext
    rfOk := hI.rfOk
    undispatched := fun s x hx => by
      obtain ⟨h1, h2, h3⟩ := hI.undispatched s x hx
      refine ⟨h1, fun j L₂ hL₂ => ?_, h3⟩
      rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
      · exact h2 i L hL
      · exact h2 j L₂ hL₂
    msg := hI.msg }

theorem inv_accept (i : Nat) (L : Lane) (m : Msg) (a : Nat) (hL : σ.lanes i = some L) (hm : m ∈ σ.net)
    (hk : L.known m.s = some a) (hq : m.q = a + 1) :
    Inv { σ with lanes := upd σ.lanes i (some { L with
      known := upd L.known m.s (some (a + 1)), queue := L.queue ++ [⟨m.s, m.q⟩] }) } := by
  let L' : Lane := { L with known := upd L.known m.s (some (a + 1)), queue := L.queue ++ [⟨m.s, m.q⟩] }
  exact {
    hist := hI.hist
    rcpt := hI.rcpt
    view := fun j L₂ hL₂ => by
      rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
      · exact hI.view i L hL
      · exact hI.view j L₂ hL₂
    known := fun j L₂ hL₂ hv s => by
      rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
      · have hold := hI.known i L hL hv
        by_cases hs : s = m.s
        · subst hs
          obtain ⟨hle, hp⟩ := (hold m.s).2 a hk
          refine ⟨fun h => by simp [upd] at h, fun b hb => ?_⟩
          simp [upd] at hb
          subst hb
          refine ⟨by arith, ?_⟩
          simp only [proj_append, hp, proj, ite_true, hq]
          have := range'_app (cnt m.s σ.h + 1) (a - cnt m.s σ.h) 1
          rw [show cnt m.s σ.h + 1 + (a - cnt m.s σ.h) = a + 1 by arith,
            show a - cnt m.s σ.h + 1 = a + 1 - cnt m.s σ.h by arith] at this
          rw [← this]; rfl
        · have h1 : L'.known s = L.known s := by simp [L', upd, hs]
          have h2 : proj s L'.queue = proj s L.queue := by
            simp only [L', proj_append, proj]
            simp [Ne.symm hs]
          exact ⟨fun h => h2 ▸ (hold s).1 (h1 ▸ h), fun b h => h2 ▸ (hold s).2 b (h1 ▸ h)⟩
      · exact hI.known j L₂ hL₂ hv s
    resolvedIn := hI.resolvedIn
    knownNext := hI.knownNext
    rfOk := hI.rfOk
    undispatched := fun s x hx => by
      obtain ⟨h1, h2, h3⟩ := hI.undispatched s x hx
      refine ⟨h1, fun j L₂ hL₂ => ?_, h3⟩
      rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
      · intro hmem
        rcases List.mem_append.1 hmem with hmem | hmem
        · exact h2 i L hL hmem
        · simp at hmem; exact h3 m hm hmem.1.symm hmem.2.symm
      · exact h2 j L₂ hL₂
    msg := hI.msg }

theorem inv_commit (i : Nat) (L : Lane) (n : Nat) (hL : σ.lanes i = some L) (hv : L.ver = σ.v) :
    Inv { σ with
      h := σ.h ++ L.queue.take n, v := σ.v + 1,
      lanes := upd σ.lanes i (some { L with
        view := σ.h ++ L.queue.take n, ver := σ.v + 1, queue := L.queue.drop n }),
      rcpt := σ.rcpt ++ receipts σ.h.length (L.queue.take n) } := by
  let b := L.queue.take n
  let L' : Lane := { L with view := σ.h ++ b, ver := σ.v + 1, queue := L.queue.drop n }
  have hsplit : ∀ s, proj s b ++ proj s (L.queue.drop n) = proj s L.queue := fun s => by
    rw [← proj_append, List.take_append_drop]
  -- What the commit does to one session: its committed run grows by the batch's part of its queued run.
  have hsess : ∀ s, proj s (σ.h ++ b) = List.range' 1 (cnt s (σ.h ++ b)) ∧ KnownOk (σ.h ++ b) L' s := by
    intro s
    have hold := hI.known i L hL hv s
    have hh := hI.hist s
    cases hk : L.known s with
    | none =>
      have hq := (hold).1 hk
      rw [← hsplit] at hq
      obtain ⟨hb, hr⟩ := List.append_eq_nil_iff.1 hq
      have hc : cnt s (σ.h ++ b) = cnt s σ.h := by simp [cnt, proj_append, hb]
      refine ⟨by rw [hc, proj_append, hb, List.append_nil]; exact hh, fun _ => hr, fun a h => ?_⟩
      simp [L', hk] at h
    | some a =>
      obtain ⟨hle, hp⟩ := (hold).2 a hk
      rw [← hsplit] at hp
      obtain ⟨h1, h2⟩ := range'_split _ _ _ _ hp
      have hlen := congrArg List.length hp
      simp only [List.length_append, List.length_range'] at hlen
      have hc : cnt s (σ.h ++ b) = cnt s σ.h + (proj s b).length := by simp [cnt, proj_append]
      refine ⟨?_, fun h => by simp [L', hk] at h, fun a' h => ?_⟩
      · rw [hc, proj_append, hh, h1]
        have := range'_app 1 (cnt s σ.h) (proj s b).length
        rw [Nat.add_comm 1 (cnt s σ.h)] at this
        rw [List.length_range']; exact this
      · simp [L', hk] at h; subst h
        refine ⟨by arith, ?_⟩
        show proj s (L.queue.drop n) = _
        rw [h2, hc]
        congr 1 <;> arith
  exact {
    hist := fun s => (hsess s).1
    rcpt := fun p hp => by
      rcases List.mem_append.1 hp with hp | hp
      · exact getElem?_of_prefix (List.prefix_append _ _) (hI.rcpt p hp)
      · obtain ⟨h1, h2⟩ := receipts_mem _ _ p hp
        rw [List.getElem?_append_right h1]; exact h2
    view := fun j L₂ hL₂ => by
      rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
      · exact ⟨List.prefix_refl _, Nat.le_refl _, fun _ => rfl⟩
      · obtain ⟨h1, h2, _⟩ := hI.view j L₂ hL₂
        exact ⟨h1.trans (List.prefix_append _ _), by arith, fun h => by arith⟩
    known := fun j L₂ hL₂ hv' s => by
      rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
      · exact (hsess s).2
      · have := (hI.view j L₂ hL₂).2.1
        simp at hv'; arith
    resolvedIn := fun s x h => List.mem_append_left _ (hI.resolvedIn s x h)
    knownNext := fun s => by simp; have := hI.knownNext s; arith
    rfOk := fun s x r h => by
      obtain ⟨h1, h2⟩ := hI.rfOk s x r h
      refine ⟨by simp; arith, ?_⟩
      rw [List.take_append_of_le_length h1]; exact h2
    undispatched := fun s x hx => by
      obtain ⟨h1, h2, h3⟩ := hI.undispatched s x hx
      refine ⟨fun hm => ?_, fun j L₂ hL₂ => ?_, h3⟩
      · rcases List.mem_append.1 hm with hm | hm
        · exact h1 hm
        · exact h2 i L hL (List.mem_of_mem_take hm)
      · rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
        · exact fun hm => h2 i L hL (List.mem_of_mem_drop hm)
        · exact h2 j L₂ hL₂
    msg := fun m hm => by
      have hk := hI.msg m hm
      exact {
        o_pos := hk.o_pos
        rf_le := by simp; have := hk.rf_le; arith
        below := hk.below
        before := fun x hx => hk.before x (by rw [List.take_append_of_le_length hk.rf_le] at hx; exact hx)
        dispatched := hk.dispatched } }

theorem inv_answer (i : Nat) (L : Lane) (e : Entry) (k : Nat) (hL : σ.lanes i = some L)
    (he : L.view[k]? = some e) : Inv { σ with rcpt := σ.rcpt ++ [(e, k)] } where
  hist := hI.hist
  rcpt p hp := by
    rcases List.mem_append.1 hp with hp | hp
    · exact hI.rcpt p hp
    · simp at hp; subst hp; exact getElem?_of_prefix (hI.view i L hL).1 he
  view := hI.view
  known := hI.known
  resolvedIn := hI.resolvedIn
  knownNext := hI.knownNext
  rfOk := hI.rfOk
  undispatched := hI.undispatched
  msg := hI.msg

theorem inv_resolve (e : Entry) (k : Nat) (he : (e, k) ∈ σ.rcpt) :
    Inv { σ with pubs := upd σ.pubs e.s { σ.pubs e.s with
      resolved := upd (σ.pubs e.s).resolved e.q true,
      knownNext := max (σ.pubs e.s).knownNext (k + 1) } } := by
  have hk := hI.rcpt _ he
  have hlt : k < σ.h.length := by
    rcases Nat.lt_or_ge k σ.h.length with h | h
    · exact h
    · simp [List.getElem?_eq_none h] at hk
  have hmem : e ∈ σ.h := List.mem_of_getElem? hk
  have hrf' : ∀ s, (upd σ.pubs e.s { σ.pubs e.s with
      resolved := upd (σ.pubs e.s).resolved e.q true,
      knownNext := max (σ.pubs e.s).knownNext (k + 1) } s).rf = (σ.pubs s).rf := by
    intro s; by_cases h : s = e.s <;> simp [upd, h]
  have hres' : ∀ s x, (upd σ.pubs e.s { σ.pubs e.s with
      resolved := upd (σ.pubs e.s).resolved e.q true,
      knownNext := max (σ.pubs e.s).knownNext (k + 1) } s).resolved x = true ↔
      (s = e.s ∧ x = e.q) ∨ (σ.pubs s).resolved x = true := by
    intro s x
    by_cases h : s = e.s
    · subst h; by_cases hx : x = e.q <;> simp [upd, hx]
    · simp [upd, h]
  exact {
    hist := hI.hist
    rcpt := hI.rcpt
    view := hI.view
    known := hI.known
    resolvedIn := fun s x h => by
      rcases (hres' s x).1 h with ⟨rfl, rfl⟩ | h
      · exact hmem
      · exact hI.resolvedIn s x h
    knownNext := fun s => by
      by_cases h : s = e.s
      · subst h; simp [upd]; exact Nat.max_le.2 ⟨hI.knownNext _, hlt⟩
      · simp [upd, h]; exact hI.knownNext s
    rfOk := fun s x r h => hI.rfOk s x r (by rw [← hrf']; exact h)
    undispatched := fun s x h => hI.undispatched s x (by rw [← hrf']; exact h)
    msg := fun m hm => by
      have hk := hI.msg m hm
      exact {
        o_pos := hk.o_pos
        rf_le := hk.rf_le
        below := fun x h1 h2 => (hres' m.s x).2 (Or.inr (hk.below x h1 h2))
        before := hk.before
        dispatched := by rw [hrf']; exact hk.dispatched } }

theorem inv_hear (s n : Nat) (hn : n ≤ σ.h.length) :
    Inv { σ with pubs := upd σ.pubs s { σ.pubs s with knownNext := max (σ.pubs s).knownNext n } } := by
  have hrf' : ∀ s', (upd σ.pubs s { σ.pubs s with knownNext := max (σ.pubs s).knownNext n } s').rf =
      (σ.pubs s').rf := by
    intro s'; by_cases h : s' = s <;> simp [upd, h]
  have hres' : ∀ s', (upd σ.pubs s { σ.pubs s with knownNext := max (σ.pubs s).knownNext n } s').resolved =
      (σ.pubs s').resolved := by
    intro s'; by_cases h : s' = s <;> simp [upd, h]
  exact {
    hist := hI.hist
    rcpt := hI.rcpt
    view := hI.view
    known := hI.known
    resolvedIn := fun s' x h => hI.resolvedIn s' x (by rw [← hres']; exact h)
    knownNext := fun s' => by
      by_cases h : s' = s
      · subst h; simp [upd]; exact Nat.max_le.2 ⟨hI.knownNext _, hn⟩
      · simp [upd, h]; exact hI.knownNext s'
    rfOk := fun s' x r h => hI.rfOk s' x r (by rw [← hrf']; exact h)
    undispatched := fun s' x h => hI.undispatched s' x (by rw [← hrf']; exact h)
    msg := fun m hm => by
      have hk := hI.msg m hm
      exact {
        o_pos := hk.o_pos
        rf_le := hk.rf_le
        below := fun x h1 h2 => by rw [hres']; exact hk.below x h1 h2
        before := hk.before
        dispatched := by rw [hrf']; exact hk.dispatched } }

end Steps

theorem inv_step {σ τ : State} (hI : Inv σ) (h : Step .fixed σ τ) : Inv τ := by
  cases h with
  | newLane i => exact inv_newLane hI i
  | stop i => exact inv_stop hI i
  | install i L hL hv => exact inv_install hI i L hL hv
  | send s q o rf prev ho _ hres hrf hrfo _ => exact inv_send hI s q o rf prev ho hres hrf hrfo
  | learn i L m hL hm hk => exact inv_learn hI i L m hL hm hk
  | accept i L m a hL hm hk hq => exact inv_accept hI i L m a hL hm hk hq
  | commit i L n hL hv => exact inv_commit hI i L n hL hv
  | answer i L e k hL he => exact inv_answer hI i L e k hL he
  | resolve e k he => exact inv_resolve hI e k he
  | hear s n hn => exact inv_hear hI s n hn

theorem fixed_inv {σ : State} (h : Reach .fixed σ) : Inv σ := by
  induction h with
  | init => exact inv_init
  | step _ hs ih => exact inv_step ih hs

/-! ## Results for the fixed rule -/

/-- Contract 7: no transfer, and so no single Publish call, is committed twice. -/
theorem fixed_exactly_once {σ : State} (h : Reach .fixed σ) : σ.h.Nodup :=
  nodup_of_proj σ.h fun s => by rw [(fixed_inv h).hist s]; exact List.nodup_range'

/-- Contract 8: a session's committed sequences are its first calls, in call order. -/
theorem fixed_call_order {σ : State} (h : Reach .fixed σ) (s : Nat) :
    proj s σ.h = List.range' 1 (cnt s σ.h) :=
  (fixed_inv h).hist s

/-- A receipt always carries the exact committed offset. -/
theorem fixed_receipts {σ : State} (h : Reach .fixed σ) : ∀ p ∈ σ.rcpt, σ.h[p.2]? = some p.1 :=
  (fixed_inv h).rcpt

end EventLog.ExactlyOnce
