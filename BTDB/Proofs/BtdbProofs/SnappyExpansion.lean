/-!
# Snappy data decompresses to at most 22 times its length

`SnappyDecompress.DecompressRaw` (`../SnappyCompression/SnappyDecompress.cs`) reads elements until the input ends and
succeeds only when they write exactly the declared size. A literal of `len` bytes takes its tag, up to 4 length bytes
and the `len` bytes; a copy takes 2 bytes for 4 to 11 bytes, or 3 bytes for 1 to 64 bytes; 4 byte offsets are
refused. Since the 2026-10-29 fix `Decompress` refuses a declared size above 22 times the input before allocating it.

* `expansion_bound`: the elements of any input write at most 22 times the bytes they take, so the check refuses no
  data that would decompress.
-/

namespace Btdb.SnappyExpansion

inductive Element where
  | literal (len lengthBytes : Nat) (h : lengthBytes ≤ 4)
  | copy1 (len : Nat) (h : 4 ≤ len ∧ len ≤ 11)
  | copy2 (len : Nat) (h : 1 ≤ len ∧ len ≤ 64)

/-- Input bytes an element takes. -/
def taken : Element → Nat
  | .literal len lengthBytes _ => 1 + lengthBytes + len
  | .copy1 .. => 2
  | .copy2 .. => 3

/-- Output bytes an element writes. -/
def written : Element → Nat
  | .literal len .. => len
  | .copy1 len _ => len
  | .copy2 len _ => len

theorem element_bound (e : Element) : written e ≤ 22 * taken e := by
  cases e with
  | literal len lengthBytes h => simp only [written, taken]; omega
  | copy1 len h => simp only [written, taken]; omega
  | copy2 len h => simp only [written, taken]; omega

theorem expansion_bound (elements : List Element) :
    (elements.map written).sum ≤ 22 * (elements.map taken).sum := by
  induction elements with
  | nil => simp
  | cons e rest ih =>
    simp only [List.map_cons, List.sum_cons]
    have := element_bound e
    omega

end Btdb.SnappyExpansion
