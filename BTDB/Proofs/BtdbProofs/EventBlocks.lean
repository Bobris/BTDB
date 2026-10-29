/-!
# An event the store accepts fits the file

A model of how `AppendingEventStore.Store` (`../../EventStoreLayer/AppendingEventStore.cs`) writes an event into a new
file: its bytes go in blocks, each after a header of `hdr` bytes, and `SpaceNeeded` runs the same split, adding a
header per block. A block holds at most `MaxBlockSize - HeaderSize` bytes less what the previous block left in its
last sector, so the sizes vary; the theorem holds for any. `ReadOnlyEventStore.ReadToEnd` reads a block only when it
ends within the maximum file size, and stops at the first that does not, so a block past it loses the event.

* `ends_le_space`: every block ends within `SpaceNeeded` from the start of the event, whatever the block sizes, so an
  event whose `SpaceNeeded` fits the maximum file size, as the store checks since the 2026-10-29 fix, is read whole.
* `old_check_loses`: the check before the fix, of the length with one header only, accepted an event whose last block
  ends past the maximum file size (`EventStoreFileCoverageTest.EventFittingMaxFileSizeOnlyWithoutBlockHeadersIsStoredOrRefused`).
-/

namespace Btdb.EventBlocks

/-- The block sizes of `len` bytes in blocks of `cap`, as in a new file's first blocks. -/
def chunks (len cap : Nat) : List Nat :=
  if cap = 0 ∨ len ≤ cap then [len] else cap :: chunks (len - cap) cap
termination_by len
decreasing_by omega

/-- Where each block ends, the first starting at `start`. -/
def ends (hdr start : Nat) : List Nat → List Nat
  | [] => []
  | s :: r => (start + hdr + s) :: ends hdr (start + hdr + s) r

/-- `SpaceNeeded`: the bytes with a header per block. -/
def space (hdr : Nat) (sizes : List Nat) : Nat := sizes.sum + hdr * sizes.length

theorem ends_le (hdr : Nat) : ∀ (sizes : List Nat) (start e : Nat), e ∈ ends hdr start sizes →
    e ≤ start + space hdr sizes
  | [], _, _, h => by simp [ends] at h
  | s :: r, start, e, h => by
    simp only [ends, List.mem_cons] at h
    simp only [space, List.sum_cons, List.length_cons] at *
    cases h with
    | inl h => subst h; rw [Nat.mul_succ]; omega
    | inr h =>
      have := ends_le hdr r (start + hdr + s) e h
      simp only [space] at this
      rw [Nat.mul_succ]; omega

theorem ends_le_space (hdr : Nat) (sizes : List Nat) (e : Nat) (h : e ∈ ends hdr 0 sizes) :
    e ≤ space hdr sizes := by
  have := ends_le hdr sizes 0 e h
  omega

/-- 4096-byte blocks with 8-byte headers in an 8192-byte file, and an event of 8180 bytes. -/
theorem old_check_loses :
    8180 + 8 ≤ 8192 ∧ chunks 8180 4088 = [4088, 4088, 4] ∧ ends 8 0 [4088, 4088, 4] = [4096, 8192, 8204] ∧
      space 8 [4088, 4088, 4] > 8192 := by
  refine ⟨by decide, ?_, by decide, by decide⟩
  simp [chunks]

end Btdb.EventBlocks
