import EventLogProofs
import ReplicationProofs

open EventLog

#print axioms ExactlyOnce.previous_duplicates
#print axioms ExactlyOnce.fixed_exactly_once
#print axioms ExactlyOnce.fixed_call_order
#print axioms ExactlyOnce.fixed_receipts
#print axioms Chain.chain
#print axioms Chain.append_exact
#print axioms Chain.createSucc_exact
#print axioms Chain.step_hist_prefix
#print axioms Chain.step_sealed_stable
#print axioms ExactlyOnce.fixed_commits
#print axioms Bounds.current_bounds_stale
#print axioms Bounds.fixed_bounds_fresh
#print axioms Chain.sealed_tail_validation
#print axioms Merge.coverage
#print axioms Merge.latest_split_exists
#print axioms Merge.old_listing_ends_early
#print axioms Merge.fresh_listing_end
#print axioms Merge.torn_listing_ends_early
#print axioms Merge.merged_has_successor

open Replication

#print axioms Lease.lease_safe
#print axioms Lease.grant_within_lease
#print axioms Lease.drained_grants_expired
#print axioms Lineage.walk_links
#print axioms Lineage.successors_correct
#print axioms Attempts.crash_skipped
#print axioms Attempts.skip_needs_attempts
#print axioms LateCreate.deleted_has_later
#print axioms LateCreate.fixed_not_reused
#print axioms LateCreate.previous_reuses
