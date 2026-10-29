/-!
# DiskChunkCache.Put starts at most one new value file

`DiskChunkCache.Put` (`../ChunkCache/DiskChunkCache.cs`) appends content to the current value file and starts a new one
while the content does not fit below the size limit of one file. A new file starts with its header, at most 25 bytes.
Before the 2026-10-29 fix content larger than a file could ever hold made it start new files forever; now content
longer than the limit less the largest header is not cached.

* `fixed_starts_at_most_one`: with the check, at most one new file is started before the content fits.
* `previous_never_fits`: without it, content that does not fit after a header does not fit after any number of new
  files.
-/

namespace Btdb.ChunkCachePut

/-- The writer position after starting `n` new files while the content does not fit. -/
def positionAfter (limit header content : Nat) : Nat → Nat → Nat
  | position, 0 => position
  | position, n + 1 =>
    if position + content > limit then positionAfter limit header content header n else position

/-- The limit of one file is at least 120 bytes, as the capacity is at least 1000. -/
theorem fixed_starts_at_most_one (limit header content position : Nat) (hl : 25 ≤ limit) (hh : header ≤ 25)
    (hc : content ≤ limit - 25) : positionAfter limit header content position 1 + content ≤ limit := by
  unfold positionAfter
  split
  · simp only [positionAfter]; omega
  · omega

theorem previous_never_fits (limit header content : Nat) (h : header + content > limit) :
    ∀ n position, position + content > limit → positionAfter limit header content position n + content > limit
  | 0, _, hp => hp
  | n + 1, position, hp => by
    simp only [positionAfter, hp, ite_true]
    exact previous_never_fits limit header content h n header h

end Btdb.ChunkCachePut
