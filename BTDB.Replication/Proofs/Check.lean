import EventLogProofs

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
