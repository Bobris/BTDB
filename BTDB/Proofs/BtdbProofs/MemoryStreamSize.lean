/-!
# A memory stream reads zeros where it was cut off

`MemoryPositionLessStream` (`../StreamLayer/MemoryPositionLessStream.cs`) keeps its bytes in buffers. Since the
2026-10-29 fix, `SetSize` to a smaller size clears the rest of the last buffer it keeps and drops the later ones, and
growing adds zeroed buffers, so every byte from the size on is zero. Here the stream is its bytes by position and its
size; a write past the end grows it.

* `inv_preserved`: after any writes and size changes every byte from the size on is zero.
* `grown_reads_zero`: so a stream shrunk and grown again reads zeros where it was cut off, like a file.
* `previous_reads_old`: before the fix `SetSize` kept the bytes, and growing again read them back.
-/

namespace Btdb.MemoryStreamSize

structure Stream where
  data : Nat → Nat
  size : Nat

def write (s : Stream) (pos : Nat) (bytes : List Nat) : Stream :=
  ⟨fun i => if pos ≤ i ∧ i < pos + bytes.length then bytes.getD (i - pos) 0 else s.data i,
    max s.size (pos + bytes.length)⟩

def setSize (s : Stream) (n : Nat) : Stream := ⟨fun i => if n ≤ i then 0 else s.data i, n⟩

/-- Before the fix: the size changed, the bytes stayed. -/
def previousSetSize (s : Stream) (n : Nat) : Stream := ⟨s.data, n⟩

/-- What reading at `i` returns: the byte when within the size, nothing beyond it. -/
def read (s : Stream) (i : Nat) : Option Nat := if i < s.size then some (s.data i) else none

def Inv (s : Stream) : Prop := ∀ i, s.size ≤ i → s.data i = 0

inductive Op where
  | write (pos : Nat) (bytes : List Nat)
  | setSize (n : Nat)

def apply (s : Stream) : Op → Stream
  | .write pos bytes => write s pos bytes
  | .setSize n => setSize s n

theorem write_inv (s : Stream) (pos : Nat) (bytes : List Nat) (h : Inv s) : Inv (write s pos bytes) := by
  intro i hi
  simp only [write] at *
  have h1 : s.size ≤ i := by omega
  have h2 : ¬(pos ≤ i ∧ i < pos + bytes.length) := by omega
  simp only [h2, ite_false]
  exact h i h1

theorem setSize_inv (s : Stream) (n : Nat) : Inv (setSize s n) := by
  intro i hi
  simp only [setSize] at *
  simp [hi]

theorem inv_preserved (ops : List Op) (s : Stream) (h : Inv s) : Inv (ops.foldl apply s) := by
  induction ops generalizing s with
  | nil => exact h
  | cons op rest ih =>
    apply ih
    cases op with
    | write pos bytes => exact write_inv s pos bytes h
    | setSize n => exact setSize_inv s n

theorem grown_reads_zero (s : Stream) (n m i : Nat) (hn : n ≤ i) (hm : i < m) :
    read (setSize (setSize s n) m) i = some 0 := by
  simp only [read, setSize]
  simp [hm, hn, show ¬ m ≤ i by omega]

theorem previous_reads_old :
    read (previousSetSize (previousSetSize (write ⟨fun _ => 0, 0⟩ 0 [1, 2, 3, 4]) 1) 4) 2 = some 3 := by
  decide

end Btdb.MemoryStreamSize
