import LsmProofs

open Lsm

#print axioms Wal.current_loses_acknowledged
#print axioms Wal.current_replays_a_gap
#print axioms Wal.fixed_keeps_acknowledged
#print axioms Wal.fixed_replays_one_history
#print axioms Wal.fixed_commits
#print axioms Counts.total_telescopes
#print axioms Counts.merge_final
#print axioms Counts.merge_consistent
#print axioms Counts.merge_total
#print axioms Wal.restart_agrees
#print axioms Wal.fixed_restart_replays
#print axioms WalFlush.flush_keeps_acknowledged
#print axioms WalFlush.flush_replays_one_history
#print axioms WalFlush.flush_of_unlanded_chunk
#print axioms Fence.current_shares_epoch
#print axioms Fence.fixed_fresh_epochs
#print axioms Fence.late_create_after_deletion
#print axioms Wal.unacknowledged_replayed
