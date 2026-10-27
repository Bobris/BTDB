import EventLogProofs.Common

/-!
# Split chain, seals and takeover

A model of the storage operations of `EventLogOwnerLane` on the level-0 split chain: conditional appends to the
writable tail (`AppendAsync`, also the combined takeover, seal-only writes and `SetOwnerAsync`), successor creation
with content after the lane's own seal (`CreateSuccessorAsync`), adopting a helper's header-only successor that still
carries the lane's session, header-only successors created by anybody after observing a seal (`CreateCandidateAsync`),
and candidates reading the tail (`FindTailAsync`).

Splits `0, 1, …` stand for split IDs `1, 2, …`. Versions come from a counter, so a version is never reused. A record
is an opaque `Nat`; frame and offset encoding are not modelled. Every operation is atomic and may happen at any time,
so a delayed request is the same request applied later. An ambiguous write either landed or not; the lane then adopts
exactly its own content and session or stops, which `append` followed by `stop`, or `stop` alone, already covers.

Proved for every reachable state and step:

* `chain`: every split except the last is sealed, so the splits form one chain without a fork;
* `step_hist_prefix`: canonical history only grows; a committed prefix is never replaced or truncated;
* `append_exact`, `createSucc_exact`: a write that applies appends its batch exactly after the history the writer
  holds, so its first offset is the length of that history and its receipts are exact. This is what
  `ExactlyOnce.lean` assumes about one versioned history;
* `step_sealed_stable`: a sealed split never changes again.
-/

namespace EventLog.Chain

structure Blob where
  recs : List Nat
  sealed : Bool
  owner : Nat
  ver : Nat

/-- An owner lane: its node session, the split it writes, the version it holds, that split's records and seal as it
last wrote or read them, and (ghost) the records of all earlier splits. -/
structure Lane where
  self : Nat
  sid : Nat
  ver : Nat
  content : List Nat
  sealed : Bool
  pre : List Nat

structure State where
  splits : List Blob
  vc : Nat
  lanes : Nat → Option Lane

/-- Canonical history: the records of all splits in chain order. -/
def hist (splits : List Blob) : List Nat := (splits.map Blob.recs).flatten

def init : State := ⟨[], 0, fun _ => none⟩

inductive Step : State → State → Prop
  /-- The first split of an empty topic, created if absent. -/
  | create0 (σ : State) (self : Nat) (h0 : σ.splits = []) :
      Step σ { σ with splits := [⟨[], false, self, σ.vc⟩], vc := σ.vc + 1 }
  /-- Anybody who observed the sealed last split creates its header-only successor with the inherited owner. A
  create racing with an existing successor is rejected and changes nothing. -/
  | helper (σ : State) (b : Blob) (hb : σ.splits.getLast? = some b) (hs : b.sealed = true) :
      Step σ { σ with splits := σ.splits ++ [⟨[], false, b.owner, σ.vc⟩], vc := σ.vc + 1 }
  /-- A candidate reads the unsealed tail. -/
  | candidate (σ : State) (i self : Nat) (b : Blob) (hb : σ.splits[σ.splits.length - 1]? = some b)
      (hs : b.sealed = false) :
      Step σ { σ with lanes := upd σ.lanes i (some ⟨self, σ.splits.length - 1, b.ver, b.recs, false,
        hist (σ.splits.take (σ.splits.length - 1))⟩) }
  /-- An `If-Match` write of the lane's split: the existing records plus `batch`, sealed when `close`, with the
  lane's session as owner. It applies only while the split still has the lane's version. -/
  | append (σ : State) (i : Nat) (L : Lane) (b : Blob) (batch : List Nat) (close : Bool)
      (hL : σ.lanes i = some L) (hu : L.sealed = false) (hb : σ.splits[L.sid]? = some b) (hv : b.ver = L.ver) :
      Step σ { σ with
        splits := σ.splits.set L.sid ⟨L.content ++ batch, close, L.self, σ.vc⟩, vc := σ.vc + 1,
        lanes := upd σ.lanes i (some { L with ver := σ.vc, content := L.content ++ batch, sealed := close }) }
  /-- After its own seal applied, the lane creates the successor with content, if absent. -/
  | createSucc (σ : State) (i : Nat) (L : Lane) (batch : List Nat) (close : Bool)
      (hL : σ.lanes i = some L) (hs : L.sealed = true) (hlen : σ.splits.length = L.sid + 1) :
      Step σ { σ with
        splits := σ.splits ++ [⟨batch, close, L.self, σ.vc⟩], vc := σ.vc + 1,
        lanes := upd σ.lanes i (some ⟨L.self, L.sid + 1, σ.vc, batch, close, L.pre ++ L.content⟩) }
  /-- The successor exists: the lane continues on it only when it is header-only, unsealed and still carries the
  lane's session. -/
  | adopt (σ : State) (i : Nat) (L : Lane) (b : Blob) (hL : σ.lanes i = some L) (hs : L.sealed = true)
      (hb : σ.splits[L.sid + 1]? = some b) (ho : b.owner = L.self) (hr : b.recs = []) (hu : b.sealed = false) :
      Step σ { σ with lanes := upd σ.lanes i (some ⟨L.self, L.sid + 1, b.ver, [], false, L.pre ++ L.content⟩) }
  | stop (σ : State) (i : Nat) : Step σ { σ with lanes := upd σ.lanes i none }

inductive Reach : State → Prop
  | init : Reach init
  | step {σ τ : State} : Reach σ → Step σ τ → Reach τ

structure LaneOk (splits : List Blob) (vc : Nat) (L : Lane) : Prop where
  sid_lt : L.sid < splits.length
  ver_lt : L.ver < vc
  pre : L.pre = hist (splits.take L.sid)
  sealed : L.sealed = true → ∃ b, splits[L.sid]? = some b ∧ b.sealed = true ∧ b.recs = L.content
  current : ∀ b, splits[L.sid]? = some b → b.ver = L.ver → b.recs = L.content ∧ b.sealed = L.sealed

structure Inv (σ : State) : Prop where
  chain : ∀ j b, σ.splits[j]? = some b → j + 1 < σ.splits.length → b.sealed = true
  ver_lt : ∀ b ∈ σ.splits, b.ver < σ.vc
  lanes : ∀ i L, σ.lanes i = some L → LaneOk σ.splits σ.vc L

/-! ## List lemmas -/

theorem hist_append (a b : List Blob) : hist (a ++ b) = hist a ++ hist b := by
  simp [hist, List.map_append, List.flatten_append]

theorem hist_single (b : Blob) : hist [b] = b.recs := by simp [hist]

theorem take_succ_of (l : List Blob) (j : Nat) (b : Blob) (hb : l[j]? = some b) :
    l.take (j + 1) = l.take j ++ [b] := by
  rw [List.take_add_one, hb]; rfl

theorem eq_take_last (l : List Blob) (j : Nat) (b : Blob) (hb : l[j]? = some b) (hl : l.length = j + 1) :
    l = l.take j ++ [b] := by
  rw [← take_succ_of l j b hb, List.take_of_length_le (by arith)]

theorem take_set_of_le (l : List Blob) (n j : Nat) (x : Blob) (h : n ≤ j) : (l.set j x).take n = l.take n := by
  apply List.ext_getElem?
  intro k
  simp only [List.getElem?_take]
  split
  · rw [List.getElem?_set_ne (by arith)]
  · rfl

theorem getElem?_lt {l : List Blob} {j : Nat} {b : Blob} (hb : l[j]? = some b) : j < l.length := by
  rcases Nat.lt_or_ge j l.length with h | h
  · exact h
  · rw [List.getElem?_eq_none h] at hb; cases hb

/-! ## The invariant -/

theorem inv_init : Inv init where
  chain _ _ h := by simp [init] at h
  ver_lt _ h := by simp [init] at h
  lanes _ _ h := by simp [init] at h

/-- The writable tail: a lane holding the version of its unsealed split writes the last split, and the history is
exactly what the lane holds. -/
theorem current_tail {σ : State} (hI : Inv σ) {L : Lane} (hL : LaneOk σ.splits σ.vc L) (hu : L.sealed = false)
    {b : Blob} (hb : σ.splits[L.sid]? = some b) (hv : b.ver = L.ver) :
    σ.splits.length = L.sid + 1 ∧ b.recs = L.content ∧ hist σ.splits = L.pre ++ L.content := by
  obtain ⟨hr, hs⟩ := hL.current b hb hv
  have hlen : σ.splits.length = L.sid + 1 := by
    rcases Nat.lt_or_ge (L.sid + 1) σ.splits.length with h | h
    · have := hI.chain _ _ hb h; rw [hs, hu] at this; cases this
    · have := hL.sid_lt; arith
  refine ⟨hlen, hr, ?_⟩
  conv => lhs; rw [eq_take_last σ.splits L.sid b hb hlen]
  rw [hist_append, hist_single, hL.pre, hr]

/-- A sealed lane whose successor does not exist yet holds the whole history. -/
theorem sealed_tail {σ : State} {L : Lane} (hL : LaneOk σ.splits σ.vc L) (hs : L.sealed = true)
    (hlen : σ.splits.length = L.sid + 1) : hist σ.splits = L.pre ++ L.content := by
  obtain ⟨b, hb, _, hr⟩ := hL.sealed hs
  conv => lhs; rw [eq_take_last σ.splits L.sid b hb hlen]
  rw [hist_append, hist_single, hL.pre, hr]

/-- Lanes that a step leaves alone stay valid when splits are only added. -/
theorem laneOk_grow {splits : List Blob} {vc : Nat} {L : Lane} (hL : LaneOk splits vc L) (extra : List Blob)
    (vc' : Nat) (hvc : vc ≤ vc') : LaneOk (splits ++ extra) vc' L := by
  have hlt := hL.sid_lt
  have hget : (splits ++ extra)[L.sid]? = splits[L.sid]? := List.getElem?_append_left hlt
  exact {
    sid_lt := by simp; arith
    ver_lt := by have := hL.ver_lt; arith
    pre := by rw [List.take_append_of_le_length (by arith)]; exact hL.pre
    sealed := fun h => by rw [hget]; exact hL.sealed h
    current := fun b hb hv => hL.current b (by rw [← hget]; exact hb) hv }

theorem inv_step {σ τ : State} (hI : Inv σ) (h : Step σ τ) : Inv τ := by
  cases h with
  | create0 self h0 =>
    exact {
      chain := fun j b hb hj => by simp at hj
      ver_lt := fun b hb => by simp at hb; subst hb; simp
      lanes := fun i L hL => by
        have := (hI.lanes i L hL).sid_lt; rw [h0] at this; simp at this }
  | helper b hb hs =>
    exact {
      chain := fun j b' hb' hj => by
        simp only [List.length_append, List.length_singleton] at hj
        rw [List.getElem?_append_left (by arith)] at hb'
        rcases Nat.lt_or_ge (j + 1) σ.splits.length with h | h
        · exact hI.chain j b' hb' h
        · have hj' : j = σ.splits.length - 1 := by arith
          rw [List.getLast?_eq_getElem?, ← hj', hb'] at hb
          cases hb; exact hs
      ver_lt := fun b' hb' => by
        rcases List.mem_append.1 hb' with h | h
        · have := hI.ver_lt b' h; arith
        · simp at h; subst h; simp
      lanes := fun i L hL => laneOk_grow (hI.lanes i L hL) _ _ (by arith) }
  | candidate i self b hb hs =>
    exact {
      chain := hI.chain
      ver_lt := hI.ver_lt
      lanes := fun j L hL => by
        rcases upd_some hL with ⟨_, rfl⟩ | ⟨_, hL⟩
        · have hlt := getElem?_lt hb
          exact {
            sid_lt := by simp; arith
            ver_lt := hI.ver_lt b (List.mem_of_getElem? hb)
            pre := rfl
            sealed := fun h => by cases h
            current := fun b' hb' _ => by
              simp only at hb'; rw [hb] at hb'; cases hb'; exact ⟨rfl, hs⟩ }
        · exact hI.lanes j L hL }
  | append i L b batch close hL hu hb hv =>
    have hLok := hI.lanes i L hL
    obtain ⟨hlen, hr, _⟩ := current_tail hI hLok hu hb hv
    have hbs : b.sealed = false := by rw [(hLok.current b hb hv).2, hu]
    let nb : Blob := ⟨L.content ++ batch, close, L.self, σ.vc⟩
    have hset : ∀ j, (σ.splits.set L.sid nb)[j]? = if j = L.sid then some nb else σ.splits[j]? := by
      intro j
      by_cases hj : j = L.sid
      · subst hj; rw [List.getElem?_set_self (by arith)]; simp
      · rw [List.getElem?_set_ne (Ne.symm hj)]; simp [hj]
    exact {
      chain := fun j b' hb' hj => by
        simp only [List.length_set] at hj
        rw [hset] at hb'
        split at hb'
        · arith
        · exact hI.chain j b' hb' hj
      ver_lt := fun b' hb' => by
        rcases List.mem_or_eq_of_mem_set hb' with h | h
        · have := hI.ver_lt b' h; arith
        · subst h; simp
      lanes := fun j L₂ hL₂ => by
        rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
        · exact {
            sid_lt := by simp; arith
            ver_lt := by simp
            pre := by simp only; rw [take_set_of_le _ _ _ _ (Nat.le_refl _)]; exact hLok.pre
            sealed := fun h => ⟨nb, by rw [hset]; simp, by simpa [nb] using h, rfl⟩
            current := fun b' hb' _ => by
              rw [hset] at hb'; simp at hb'; subst hb'; exact ⟨rfl, rfl⟩ }
        · have h2 := hI.lanes j L₂ hL₂
          have hle : L₂.sid ≤ L.sid := by have := h2.sid_lt; arith
          exact {
            sid_lt := by simp; exact h2.sid_lt
            ver_lt := by have := h2.ver_lt; arith
            pre := by rw [take_set_of_le _ _ _ _ hle]; exact h2.pre
            sealed := fun h => by
              obtain ⟨b₂, hb₂, hs₂, hr₂⟩ := h2.sealed h
              by_cases hj : L₂.sid = L.sid
              · rw [hj, hb] at hb₂; cases hb₂; rw [hbs] at hs₂; cases hs₂
              · exact ⟨b₂, by rw [hset]; simp [hj]; exact hb₂, hs₂, hr₂⟩
            current := fun b' hb' hv' => by
              rw [hset] at hb'
              split at hb'
              · simp at hb'; subst hb'; have := h2.ver_lt; simp [nb] at hv'; arith
              · exact h2.current b' hb' hv' } }
  | createSucc i L batch close hL hs hlen =>
    have hLok := hI.lanes i L hL
    obtain ⟨b₀, hb₀, hs₀, hr₀⟩ := hLok.sealed hs
    exact {
      chain := fun j b' hb' hj => by
        simp only [List.length_append, List.length_singleton] at hj
        rw [List.getElem?_append_left (by arith)] at hb'
        rcases Nat.lt_or_ge (j + 1) σ.splits.length with h | h
        · exact hI.chain j b' hb' h
        · have hj' : j = L.sid := by arith
          rw [hj', hb₀] at hb'; cases hb'; exact hs₀
      ver_lt := fun b' hb' => by
        rcases List.mem_append.1 hb' with h | h
        · have := hI.ver_lt b' h; arith
        · simp at h; subst h; simp
      lanes := fun j L₂ hL₂ => by
        rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
        · have hget : (σ.splits ++ [⟨batch, close, L.self, σ.vc⟩])[L.sid + 1]? =
              some ⟨batch, close, L.self, σ.vc⟩ := by
            rw [List.getElem?_append_right (by arith)]; simp [hlen]
          exact {
            sid_lt := by simp; arith
            ver_lt := by simp
            pre := by
              simp only
              rw [List.take_append_of_le_length (by arith), List.take_of_length_le (by arith),
                sealed_tail hLok hs hlen]
            sealed := fun h => ⟨_, hget, h, rfl⟩
            current := fun b' hb' _ => by
              simp only at hb'; rw [hget] at hb'; cases hb'; exact ⟨rfl, rfl⟩ }
        · exact laneOk_grow (hI.lanes j L₂ hL₂) _ _ (by arith) }
  | adopt i L b hL hs hb ho hr hu =>
    have hLok := hI.lanes i L hL
    obtain ⟨b₀, hb₀, _, hr₀⟩ := hLok.sealed hs
    exact {
      chain := hI.chain
      ver_lt := hI.ver_lt
      lanes := fun j L₂ hL₂ => by
        rcases upd_some hL₂ with ⟨_, rfl⟩ | ⟨_, hL₂⟩
        · exact {
            sid_lt := getElem?_lt hb
            ver_lt := hI.ver_lt b (List.mem_of_getElem? hb)
            pre := by
              simp only
              rw [take_succ_of _ _ _ hb₀, hist_append, hist_single, hLok.pre, hr₀]
            sealed := fun h => by cases h
            current := fun b' hb' _ => by
              simp only at hb'; rw [hb] at hb'; cases hb'; exact ⟨hr, hu⟩ }
        · exact hI.lanes j L₂ hL₂ }
  | stop i =>
    exact {
      chain := hI.chain
      ver_lt := hI.ver_lt
      lanes := fun j L hL => by
        have ⟨_, hL⟩ := upd_none hL
        exact hI.lanes j L hL }

theorem reach_inv {σ : State} (h : Reach σ) : Inv σ := by
  induction h with
  | init => exact inv_init
  | step _ hs ih => exact inv_step ih hs

/-! ## Results -/

/-- No fork: every split except the last is sealed. -/
theorem chain {σ : State} (h : Reach σ) :
    ∀ j b, σ.splits[j]? = some b → j + 1 < σ.splits.length → b.sealed = true :=
  (reach_inv h).chain

/-- A write that applies appends its batch exactly after the history its writer holds. -/
theorem append_exact {σ : State} (hI : Inv σ) {i : Nat} {L : Lane} {b : Blob} {batch : List Nat} {close : Bool}
    (hL : σ.lanes i = some L) (hu : L.sealed = false) (hb : σ.splits[L.sid]? = some b) (hv : b.ver = L.ver) :
    hist σ.splits = L.pre ++ L.content ∧
      hist (σ.splits.set L.sid ⟨L.content ++ batch, close, L.self, σ.vc⟩) = L.pre ++ L.content ++ batch := by
  have hLok := hI.lanes i L hL
  obtain ⟨hlen, _, hh⟩ := current_tail hI hLok hu hb hv
  refine ⟨hh, ?_⟩
  have hb' : (σ.splits.set L.sid ⟨L.content ++ batch, close, L.self, σ.vc⟩)[L.sid]? =
      some ⟨L.content ++ batch, close, L.self, σ.vc⟩ := by
    rw [List.getElem?_set_self (by arith)]
  rw [eq_take_last _ L.sid _ hb' (by simp; arith), hist_append, hist_single,
    take_set_of_le _ _ _ _ (Nat.le_refl _), ← hLok.pre, List.append_assoc]

theorem createSucc_exact {σ : State} (hI : Inv σ) {i : Nat} {L : Lane} {batch : List Nat} {close : Bool}
    (hL : σ.lanes i = some L) (hs : L.sealed = true) (hlen : σ.splits.length = L.sid + 1) :
    hist σ.splits = L.pre ++ L.content ∧
      hist (σ.splits ++ [⟨batch, close, L.self, σ.vc⟩]) = L.pre ++ L.content ++ batch := by
  have hh := sealed_tail (hI.lanes i L hL) hs hlen
  exact ⟨hh, by rw [hist_append, hist_single, hh]⟩

/-- Contract 3: canonical history only grows. -/
theorem step_hist_prefix {σ τ : State} (hI : Inv σ) (h : Step σ τ) : hist σ.splits <+: hist τ.splits := by
  cases h with
  | create0 _ h0 => simp [h0, hist]
  | helper => exact ⟨_, (hist_append _ _).symm⟩
  | candidate => exact List.prefix_refl _
  | append i L b batch close hL hu hb hv =>
    obtain ⟨h1, h2⟩ := append_exact (batch := batch) (close := close) hI hL hu hb hv
    simp only; rw [h1, h2]; exact List.prefix_append _ _
  | createSucc i L batch close hL hs hlen =>
    obtain ⟨h1, h2⟩ := createSucc_exact (batch := batch) (close := close) hI hL hs hlen
    simp only; rw [h1, h2]; exact List.prefix_append _ _
  | adopt => exact List.prefix_refl _
  | stop => exact List.prefix_refl _

/-- A sealed split never changes again. -/
theorem step_sealed_stable {σ τ : State} (hI : Inv σ) (h : Step σ τ) {j : Nat} {b : Blob}
    (hb : σ.splits[j]? = some b) (hs : b.sealed = true) : τ.splits[j]? = some b := by
  cases h with
  | create0 _ h0 => simp [h0] at hb
  | helper => simp only; rw [List.getElem?_append_left (getElem?_lt hb)]; exact hb
  | candidate => exact hb
  | append i L b' batch close hL hu hb' hv =>
    simp only
    by_cases hj : j = L.sid
    · subst hj; rw [hb] at hb'; cases hb'
      rw [(hI.lanes i L hL).current b hb hv |>.2, hu] at hs; cases hs
    · rw [List.getElem?_set_ne (Ne.symm hj)]; exact hb
  | createSucc => simp only; rw [List.getElem?_append_left (getElem?_lt hb)]; exact hb
  | adopt => exact hb
  | stop => exact hb

end EventLog.Chain
