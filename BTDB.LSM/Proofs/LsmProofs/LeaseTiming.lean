/-!
# Writer lease timing

A model of the writer lease of `LsmKeyValueDB` (`../LsmKeyValueDB.Lease.cs`) over the lease records of a storage
(`FileSystemLsmStorage`, or an object store's leases). Times are true times; each party reads a clock off from true
time by a constant (`k` for the writer, `c` for the storage); clocks running at different rates are not modelled.

* The writer sends an acquire or renew request at true time `r` and sets its deadline, on its own clock, to
  `r + k + duration - margin` (`_leaseDeadline = requested + LeaseDuration - LeaseSafetyMargin`).
* The storage handles the request at true time `s ≥ r` and records the expiry on its clock as `s + c + duration`.
* The writer starts a write only while its clock is before the deadline (`CheckLeaseDeadline`), and a write takes at
  most `margin`.
* Another writer gets the lease only once the storage's clock reaches the expiry, at true time `b`.

* `writes_end_before_takeover`: every write of the old writer completes before the new writer holds the lease,
  whatever the constant clock offsets, because both sides measure the duration on their own clock from instants in
  the right order.
* `margin_needed`: without the margin covering a write's length, a write started just before the deadline can
  complete after the takeover.
-/

namespace Lsm.LeaseTiming

theorem writes_end_before_takeover (r s w len b k c duration margin : Int)
    (handled_after_sent : r ≤ s)
    (started_before_deadline : w + k < r + k + duration - margin)
    (write_within_margin : len ≤ margin)
    (taken_after_expiry : s + c + duration ≤ b + c) :
    w + len < b := by
  omega

theorem margin_needed : ∃ r s w len b duration margin : Int,
    r ≤ s ∧ w < r + duration - margin ∧ s + duration ≤ b ∧ len > margin ∧ ¬(w + len < b) :=
  ⟨0, 0, 9, 2, 10, 10, 0, by decide, by decide, by decide, by decide, by decide⟩

end Lsm.LeaseTiming
