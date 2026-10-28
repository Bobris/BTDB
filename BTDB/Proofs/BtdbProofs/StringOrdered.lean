import BtdbProofs.VUInt

/-!
# Ordinal StartsWith as a key prefix

A model of `MemWriter.WriteStringOrderedPrefix` and `WriteStringOrdered`, the key encoding of the "StringOrderable"
handler. A string is a list of UTF-16 code units. A high surrogate followed by a low one is one token, its code point
plus one; every other unit is a token of its value plus one (ASCII below 0x7F is the one-byte case). Tokens are
written with `PackUnpack.UnsafePackVUInt`, and a whole string ends with a zero byte.

The relation query planner scans the keys starting with the encoded prefix for an ordinal `StartsWith(s)`
(`UsesEncodedStartsWith`), except when `s` ends with a high surrogate.

* `starts_with_correct`: for such `s`, a string's encoding starts with the encoded prefix exactly when the string
  starts with `s`.
* `high_surrogate_needed`: without the exclusion the scan would miss strings that continue the surrogate pair.
* `encOpt_prefix_free`: no key of a string or null is a prefix of another's, so an ordinal equality can be part of a
  key prefix (`UsesEncodedEquals`).
-/

namespace Btdb.StringOrdered

open VUInt

def isHigh (c : Nat) : Bool := 0xD800 ≤ c && c < 0xDC00
def isLow (c : Nat) : Bool := 0xDC00 ≤ c && c < 0xE000

/-- The token values of a string. -/
def toks : List Nat → List Nat
  | [] => []
  | [c] => [c + 1]
  | c :: d :: rest =>
    if isHigh c && isLow d then ((c - 0xD800) * 0x400 + (d - 0xDC00) + 0x10001) :: toks rest
    else (c + 1) :: toks (d :: rest)

/-- `WriteStringOrderedPrefix`. -/
def prefixEnc (s : List Nat) : List Nat := (toks s).flatMap vuint

/-- `WriteStringOrdered` of a string that is not null. -/
def enc (s : List Nat) : List Nat := prefixEnc s ++ [0]

def endsHigh (s : List Nat) : Bool := match s.getLast? with | some c => isHigh c | none => false

/-- Decoding a token back to code units. -/
def untok (t : Nat) : List Nat :=
  if 0x10001 ≤ t then [0xD800 + (t - 0x10001) / 0x400, 0xDC00 + (t - 0x10001) % 0x400] else [t - 1]

def Units (s : List Nat) : Prop := ∀ c ∈ s, c < 0x10000

theorem untok_toks : ∀ s, Units s → (toks s).flatMap untok = s
  | [], _ => rfl
  | [c], h => by
    have := h c (by simp)
    simp [toks, untok]; omega
  | c :: d :: rest, h => by
    have hc := h c (by simp)
    have hd := h d (by simp)
    by_cases hp : (isHigh c && isLow d) = true
    · simp only [isHigh, isLow, Bool.and_eq_true, decide_eq_true_eq] at hp
      have ih := untok_toks rest (fun x hx => h x (by simp [hx]))
      have hu : untok ((c - 0xD800) * 0x400 + (d - 0xDC00) + 0x10001) = [c, d] := by
        have h1 : ((c - 0xD800) * 0x400 + (d - 0xDC00) + 0x10001 - 0x10001) / 0x400 = c - 0xD800 := by omega
        have h2 : ((c - 0xD800) * 0x400 + (d - 0xDC00) + 0x10001 - 0x10001) % 0x400 = d - 0xDC00 := by omega
        simp only [untok, show 0x10001 ≤ (c - 0xD800) * 0x400 + (d - 0xDC00) + 0x10001 by omega, ite_true, h1, h2]
        simp only [List.cons.injEq, and_true]
        omega
      simp only [toks, isHigh, isLow, hp, decide_true, Bool.and_self, ite_true, List.flatMap_cons, ih, hu]
      rfl
    · have ih := untok_toks (d :: rest) (fun x hx => h x (List.mem_cons_of_mem c hx))
      simp only [toks, hp, Bool.false_eq_true, ite_false, List.flatMap_cons, ih]
      simp only [untok, show ¬ (0x10001 ≤ c + 1) by omega, ite_false]
      simp

theorem toks_append : ∀ s t, endsHigh s = false → toks (s ++ t) = toks s ++ toks t
  | [], _, _ => rfl
  | [c], t, h => by
    simp only [endsHigh, List.getLast?_singleton] at h
    cases t with
    | nil => rfl
    | cons d rest => simp [toks, h]
  | c :: d :: rest, t, h => by
    have hrest : endsHigh (d :: rest) = false := by
      simp only [endsHigh] at h ⊢; rwa [List.getLast?_cons_cons] at h
    by_cases hp : (isHigh c && isLow d) = true
    · have hr : endsHigh rest = false := by
        cases rest with
        | nil => rfl
        | cons e rest' => simp only [endsHigh] at hrest ⊢; rwa [List.getLast?_cons_cons] at hrest
      simp only [List.cons_append, toks, hp, ite_true, toks_append rest t hr]
    · simp only [List.cons_append, toks, hp, Bool.false_eq_true, ite_false]
      rw [← List.cons_append, toks_append (d :: rest) t hrest]

theorem toks_bound : ∀ s, Units s → ∀ t ∈ toks s, 1 ≤ t ∧ t < 2 ^ 64
  | [], _, _, h => by simp [toks] at h
  | [c], hs, t, h => by
    have := hs c (by simp); simp [toks] at h; omega
  | c :: d :: rest, hs, t, h => by
    have hc := hs c (by simp)
    have hd := hs d (by simp)
    by_cases hp : (isHigh c && isLow d) = true
    · simp only [toks, hp, ite_true, List.mem_cons] at h
      rcases h with rfl | h
      · omega
      · exact toks_bound rest (fun x hx => hs x (by simp [hx])) t h
    · simp only [toks, hp, Bool.false_eq_true, ite_false, List.mem_cons] at h
      rcases h with rfl | h
      · omega
      · exact toks_bound (d :: rest) (fun x hx => hs x (List.mem_cons_of_mem c hx)) t h

/-- Tokens stay below the null key's token. -/
theorem toks_le : ∀ s, Units s → ∀ t ∈ toks s, t ≤ 0x110000
  | [], _, _, h => by simp [toks] at h
  | [c], hs, t, h => by
    have := hs c (by simp); simp [toks] at h; omega
  | c :: d :: rest, hs, t, h => by
    have hc := hs c (by simp)
    have hd := hs d (by simp)
    by_cases hp : (isHigh c && isLow d) = true
    · simp only [isHigh, isLow, Bool.and_eq_true, decide_eq_true_eq] at hp
      simp only [toks, isHigh, isLow, hp, decide_true, Bool.and_self, ite_true, List.mem_cons] at h
      rcases h with rfl | h
      · omega
      · exact toks_le rest (fun x hx => hs x (by simp [hx])) t h
    · simp only [toks, hp, Bool.false_eq_true, ite_false, List.mem_cons] at h
      rcases h with rfl | h
      · omega
      · exact toks_le (d :: rest) (fun x hx => hs x (List.mem_cons_of_mem c hx)) t h

/-- No token's encoding starts with a zero byte. -/
theorem vuint_head (t : Nat) (h1 : 1 ≤ t) (h2 : t < 2 ^ 64) : ∃ b r, vuint t = b :: r ∧ b ≠ 0 := by
  obtain ⟨f1, -, -, -, -, -, -⟩ := facts t h2
  obtain ⟨r, hr⟩ := head_eq (N := base (len t) + t) f1
  have hb := head_bounds t h2
  refine ⟨_, r, hr, fun h0 => ?_⟩
  rw [h0] at hb
  have hlo : lo (len t) = 0 → len t = 1 := by
    intro h; unfold lo at h; split at h <;> first | omega | simp_all
  have hl := hlo (by omega)
  have : base (len t) + t < 256 ^ (len t - 1) := by
    rcases Nat.eq_zero_or_pos ((base (len t) + t) / 256 ^ (len t - 1)) with h | h
    · exact (Nat.div_eq_zero_iff_lt (Nat.pow_pos (by decide))).1 h
    · omega
  rw [hl] at this
  simp [base] at this
  omega

/-- Token sequences encode prefix free: a byte prefix of an encoded string is a token prefix. -/
theorem tokens_prefix : ∀ (a b : List Nat), (∀ t ∈ a, 1 ≤ t ∧ t < 2 ^ 64) → (∀ t ∈ b, 1 ≤ t ∧ t < 2 ^ 64) →
    a.flatMap vuint <+: b.flatMap vuint ++ [0] → a <+: b
  | [], _, _, _, _ => List.nil_prefix
  | x :: a, [], ha, _, h => by
    obtain ⟨c, r, hr, hc⟩ := vuint_head x (ha x (by simp)).1 (ha x (by simp)).2
    simp only [List.flatMap_cons, hr, List.flatMap_nil, List.nil_append, List.cons_append] at h
    exact absurd (List.cons_prefix_cons.1 h).1 hc
  | x :: a, y :: b, ha, hb, h => by
    simp only [List.flatMap_cons, List.append_assoc] at h
    have hx := ha x (by simp)
    have hy := hb y (by simp)
    have hxy : x = y := by
      rcases List.prefix_or_prefix_of_prefix ((List.prefix_append _ _).trans h) (List.prefix_append _ _)
        with h' | h'
      · exact vuint_prefix_free hx.2 hy.2 h'
      · exact (vuint_prefix_free hy.2 hx.2 h').symm
    subst hxy
    rw [List.prefix_append_right_inj] at h
    exact List.cons_prefix_cons.2 ⟨rfl, tokens_prefix a b (fun t ht => ha t (by simp [ht]))
      (fun t ht => hb t (by simp [ht])) h⟩

theorem starts_with_correct (s str : List Nat) (hs : Units s) (hstr : Units str) (he : endsHigh s = false) :
    prefixEnc s <+: enc str ↔ s <+: str := by
  constructor
  · intro h
    have ht := tokens_prefix _ _ (toks_bound s hs) (toks_bound str hstr) h
    obtain ⟨r, hr⟩ := ht
    refine ⟨r.flatMap untok, ?_⟩
    rw [← untok_toks s hs, ← List.flatMap_append, hr, untok_toks str hstr]
  · rintro ⟨t, rfl⟩
    unfold enc prefixEnc
    rw [toks_append s t he, List.flatMap_append, List.append_assoc]
    exact List.prefix_append _ _

/-- `WriteStringOrdered`, including null. -/
def encOpt : Option (List Nat) → List Nat
  | some s => enc s
  | none => vuint 0x110001

/-- Equal keys are equal strings: no string's encoding is a prefix of another's, null included, so an equality on a
string field can be part of a key prefix. -/
theorem encOpt_prefix_free (a b : Option (List Nat)) (ha : ∀ s, a = some s → Units s) (hb : ∀ s, b = some s → Units s)
    (h : encOpt a <+: encOpt b) : a = b := by
  have tokens_eq : ∀ s t, Units s → Units t → enc s <+: enc t → s = t := by
    intro s t hs ht hst
    have hp := tokens_prefix _ _ (toks_bound s hs) (toks_bound t ht)
      ((List.prefix_append _ _).trans hst)
    obtain ⟨r, hr⟩ := hp
    cases r with
    | nil =>
      rw [← untok_toks s hs, ← untok_toks t ht, ← hr, List.append_nil]
    | cons x r =>
      exfalso
      have hx := toks_bound t ht x (by rw [← hr]; simp)
      obtain ⟨c, r', hc, hc0⟩ := vuint_head x hx.1 hx.2
      unfold enc prefixEnc at hst
      rw [← hr, List.flatMap_append, List.flatMap_cons, hc, List.append_assoc,
        List.prefix_append_right_inj] at hst
      simp at hst
      exact hc0 hst.symm
  have null_vs : ∀ t, Units t → ¬ (encOpt none <+: enc t) ∧ ¬ (enc t <+: encOpt none) := by
    intro t ht
    have hn : (0x110001 : Nat) < 2 ^ 64 := by decide
    constructor
    · intro h
      cases htok : toks t with
      | nil =>
        simp only [encOpt, enc, prefixEnc, htok, List.flatMap_nil, List.nil_append] at h
        have := h.length_le
        simp [vuint, len, be] at this
      | cons x r =>
        have hx := toks_bound t ht x (by rw [htok]; simp)
        have hx' := toks_le t ht x (by rw [htok]; simp)
        simp only [encOpt, enc, prefixEnc, htok, List.flatMap_cons, List.append_assoc] at h
        rcases List.prefix_or_prefix_of_prefix h (List.prefix_append _ _) with h' | h'
        · have := vuint_prefix_free hn hx.2 h'; omega
        · have := vuint_prefix_free hx.2 hn h'; omega
    · intro h
      cases htok : toks t with
      | nil =>
        simp only [encOpt, enc, prefixEnc, htok, List.flatMap_nil, List.nil_append] at h
        simp [vuint, len, be, base] at h
      | cons x r =>
        have hx := toks_bound t ht x (by rw [htok]; simp)
        have hx' := toks_le t ht x (by rw [htok]; simp)
        simp only [encOpt, enc, prefixEnc, htok, List.flatMap_cons, List.append_assoc] at h
        rcases List.prefix_or_prefix_of_prefix ((List.prefix_append _ _).trans h) (List.prefix_refl _) with h' | h'
        · have := vuint_prefix_free hx.2 hn h'; omega
        · have := vuint_prefix_free hn hx.2 h'; omega
  cases a with
  | none =>
    cases b with
    | none => rfl
    | some t => exact absurd h (null_vs t (hb t rfl)).1
  | some s =>
    cases b with
    | none => exact absurd h (null_vs s (ha s rfl)).2
    | some t => rw [tokens_eq s t (ha s rfl) (hb t rfl) h]

/-- The exclusion is needed: a prefix ending with a high surrogate encodes it alone, while a string continuing with
the low surrogate encodes the pair as one token. -/
theorem high_surrogate_needed :
    [0xD800] <+: [0xD800, 0xDC00] ∧ ¬ prefixEnc [0xD800] <+: enc [0xD800, 0xDC00] := by
  refine ⟨⟨[0xDC00], rfl⟩, ?_⟩
  decide

end Btdb.StringOrdered
