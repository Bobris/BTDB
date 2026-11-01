# BTDB.BackendActions

Long-running, resource-limited background actions for applications built on `BTDB.Replication.EventSourcing`.
Application events enqueue and cancel actions in their own transactions, and an ordered result event hands every
outcome back to the application.

Usage: [Doc/BackendActions.md](https://github.com/Bobris/BTDB/blob/master/Doc/BackendActions.md). Design and
milestones: [Doc/BackendActionsPlan.md](https://github.com/Bobris/BTDB/blob/master/Doc/BackendActionsPlan.md).
Preview: failover is tested in process and with separate node processes over Azurite, not yet on live Azure.
