/-!
# Relation query key ranges

A model of `RelationQueryRange.Contains` and `IsPastStop` (`BTDB/ODBLayer/RelationQuery.cs`). Keys and encodings are
byte lists compared like `ReadOnlySpan<byte>.SequenceCompareTo`. A key in a range is the range prefix, then the
encoded value of the bounded field, then the rest of the key. The field's encoding is prefix free and orders bytes like
values, as `RelationQueryKeyRanges.IsOrderPreserving` requires of the handlers it admits (checked by
`RelationQueryKeyOrderTest`).

* `lower_correct`: the lower-bound test accepts a key exactly when its value is above an exclusive bound, or at or
  above an inclusive one.
* `upper_correct`: `IsPastStop` rejects a key exactly when its value is above an inclusive bound, or at or above an
  exclusive one.
* `byte_encoding`: the one-byte handlers are such encodings; `VUInt.lean` and `VInt.lean` show it for the
  variable-length integers, which enums use too.
-/

namespace Btdb.KeyRange

/-- `ReadOnlySpan<byte>.SequenceCompareTo`: lexicographic, with a proper prefix first. -/
def cmp : List Nat → List Nat → Ordering
  | [], [] => .eq
  | [], _ :: _ => .lt
  | _ :: _, [] => .gt
  | a :: as, b :: bs => if a < b then .lt else if b < a then .gt else cmp as bs

theorem cmp_swap : ∀ a b, cmp a b = .lt → cmp b a = .gt
  | [], [], h => by simp [cmp] at h
  | [], _ :: _, _ => rfl
  | _ :: _, [], h => by simp [cmp] at h
  | x :: xs, y :: ys, h => by
    simp only [cmp] at h ⊢
    by_cases h1 : x < y
    · have : ¬ y < x := by omega
      simp [h1, this]
    · by_cases h2 : y < x
      · simp [h1, h2] at h
      · simp only [h1, h2, ite_false] at h ⊢
        exact cmp_swap xs ys h

/-- Keys that share a prefix compare like what follows it. -/
theorem cmp_prefix : ∀ (p x y : List Nat), cmp (p ++ x) (p ++ y) = cmp x y
  | [], _, _ => rfl
  | a :: p, x, y => by simp [cmp, cmp_prefix p x y]

/-- After two byte strings that differ before either ends, nothing changes the comparison. -/
theorem cmp_append : ∀ (a b r s : List Nat), ¬ a <+: b → ¬ b <+: a → cmp (a ++ r) (b ++ s) = cmp a b
  | [], b, _, _, h1, _ => absurd (List.nil_prefix) h1
  | _ :: _, [], _, _, _, h2 => absurd (List.nil_prefix) h2
  | x :: xs, y :: ys, r, s, h1, h2 => by
    simp only [List.cons_append, cmp]
    by_cases hxy : x < y
    · simp [hxy]
    · by_cases hyx : y < x
      · simp [hxy, hyx]
      · have he : x = y := by omega
        subst he
        simp only [hxy, ite_false]
        exact cmp_append xs ys r s (fun h => h1 (List.cons_prefix_cons.2 ⟨rfl, h⟩))
          (fun h => h2 (List.cons_prefix_cons.2 ⟨rfl, h⟩))

/-- A prefix free encoding that orders bytes like values, for the values of the field's type. -/
structure Encoding (D : Int → Prop) (enc : Int → List Nat) : Prop where
  prefixFree : ∀ v w, D v → D w → enc v <+: enc w → v = w
  order : ∀ v w, D v → D w → v < w → cmp (enc v) (enc w) = .lt

variable {D : Int → Prop} {enc : Int → List Nat}

theorem starts_iff (E : Encoding D enc) {v w : Int} (dv : D v) (dw : D w) (r : List Nat) :
    enc w <+: enc v ++ r ↔ v = w := by
  constructor
  · intro h
    rcases List.prefix_or_prefix_of_prefix h (List.prefix_append (enc v) r) with h' | h'
    · exact (E.prefixFree w v dw dv h').symm
    · exact E.prefixFree v w dv dw h'
  · intro h; subst h; exact List.prefix_append _ _

theorem cmp_key (E : Encoding D enc) {v w : Int} (dv : D v) (dw : D w) (hne : v ≠ w) (r : List Nat) :
    cmp (enc v ++ r) (enc w) = cmp (enc v) (enc w) := by
  have := cmp_append (enc v) (enc w) r [] (fun h => hne (E.prefixFree v w dv dw h))
    (fun h => hne (E.prefixFree w v dw dv h).symm)
  simpa using this

theorem lt_iff (E : Encoding D enc) {v w : Int} (dv : D v) (dw : D w) (hne : v ≠ w) (r : List Nat) :
    cmp (enc v ++ r) (enc w) = .lt ↔ v < w := by
  rw [cmp_key E dv dw hne]
  constructor
  · intro h
    rcases Int.lt_or_gt_of_ne hne with h' | h'
    · exact h'
    · have := cmp_swap _ _ (E.order w v dw dv h'); rw [this] at h; cases h
  · exact E.order v w dv dw

theorem gt_iff (E : Encoding D enc) {v w : Int} (dv : D v) (dw : D w) (hne : v ≠ w) (r : List Nat) :
    cmp (enc v ++ r) (enc w) = .gt ↔ w < v := by
  rw [cmp_key E dv dw hne]
  constructor
  · intro h
    rcases Int.lt_or_gt_of_ne hne with h' | h'
    · rw [E.order v w dv dw h'] at h; cases h
    · exact h'
  · exact fun h => cmp_swap _ _ (E.order w v dw dv h)

/-- The lower-bound part of `Contains`: `start` is the range prefix and the encoded bound, `key` starts with the
prefix. -/
def lowerOk (start : List Nat) (exclude : Bool) (key : List Nat) : Bool :=
  !(exclude && start.isPrefixOf key) && !(!start.isPrefixOf key && cmp key start == .lt)

/-- `IsPastStop` on the key after the range prefix. -/
def pastStop (stop : List Nat) (inclusive : Bool) (rest : List Nat) : Bool :=
  if inclusive then !stop.isPrefixOf rest && cmp rest stop == .gt
  else stop.isPrefixOf rest || cmp rest stop == .gt

theorem lower_correct (E : Encoding D enc) (p : List Nat) {v lo : Int} (dv : D v) (dlo : D lo) (r : List Nat)
    (exclude : Bool) :
    lowerOk (p ++ enc lo) exclude (p ++ (enc v ++ r)) = true ↔ (if exclude then lo < v else lo ≤ v) := by
  have hs : (p ++ enc lo).isPrefixOf (p ++ (enc v ++ r)) = true ↔ v = lo := by
    rw [List.isPrefixOf_iff_prefix, List.prefix_append_right_inj]; exact starts_iff E dv dlo r
  unfold lowerOk
  rw [cmp_prefix]
  by_cases he : v = lo
  · subst he
    have h1 : (p ++ enc v).isPrefixOf (p ++ (enc v ++ r)) = true := hs.2 rfl
    cases exclude <;> simp [h1] <;> omega
  · have h1 : (p ++ enc lo).isPrefixOf (p ++ (enc v ++ r)) = false := by
      cases h : (p ++ enc lo).isPrefixOf (p ++ (enc v ++ r))
      · rfl
      · exact absurd (hs.1 h) he
    rcases Int.lt_or_gt_of_ne he with hlt | hgt
    · have hc : cmp (enc v ++ r) (enc lo) = .lt := (lt_iff E dv dlo he r).2 hlt
      cases exclude <;> simp [h1, hc] <;> omega
    · have hc : cmp (enc v ++ r) (enc lo) = .gt := (gt_iff E dv dlo he r).2 hgt
      cases exclude <;> simp [h1, hc] <;> omega

theorem upper_correct (E : Encoding D enc) {v hi : Int} (dv : D v) (dhi : D hi) (r : List Nat) (inclusive : Bool) :
    pastStop (enc hi) inclusive (enc v ++ r) = true ↔ (if inclusive then hi < v else hi ≤ v) := by
  have hs : (enc hi).isPrefixOf (enc v ++ r) = true ↔ v = hi := by
    rw [List.isPrefixOf_iff_prefix]; exact starts_iff E dv dhi r
  unfold pastStop
  by_cases he : v = hi
  · subst he
    have h1 : (enc v).isPrefixOf (enc v ++ r) = true := hs.2 rfl
    cases inclusive <;> simp [h1]
  · have h1 : (enc hi).isPrefixOf (enc v ++ r) = false := by
      cases h : (enc hi).isPrefixOf (enc v ++ r)
      · rfl
      · exact absurd (hs.1 h) he
    rcases Int.lt_or_gt_of_ne he with hlt | hgt
    · have hc : cmp (enc v ++ r) (enc hi) = .lt := (lt_iff E dv dhi he r).2 hlt
      cases inclusive <;> simp [h1, hc] <;> omega
    · have hc : cmp (enc v ++ r) (enc hi) = .gt := (gt_iff E dv dhi he r).2 hgt
      cases inclusive <;> simp [h1, hc] <;> omega

/-- A field of one byte that grows with the value ("UInt8", "Bool", "Int8Orderable" with offset 128). -/
theorem byte_encoding (offset : Int) :
    Encoding (fun v => 0 ≤ v + offset ∧ v + offset < 256) (fun v => [(v + offset).toNat]) where
  prefixFree v w _ _ h := by
    have := List.IsPrefix.eq_of_length h (by simp)
    simp at this
    omega
  order v w dv dw h := by
    simp only [cmp]
    have : (v + offset).toNat < (w + offset).toNat := by omega
    simp [this]

end Btdb.KeyRange
