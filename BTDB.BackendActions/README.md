# BTDB.BackendActions

Long-running, resource-limited background actions for applications built on `BTDB.Replication.EventSourcing`.
Application events enqueue and cancel actions in their own transactions, and an ordered result event hands every
outcome back to the application.

Status: in development; the design and milestones are in
[Doc/BackendActionsPlan.md](https://github.com/Bobris/BTDB/blob/master/Doc/BackendActionsPlan.md). This version
contains the action contracts, their replicated state and `BackendActionsHost`, which runs a worker on every node and
the coordinator on the replication leader. Failover is tested in process; qualification with real replication
processes, scheduling, partial results and monitoring are not done yet.
