# BackendActions subprocess qualification

This non-packable test assembly doubles as a loopback-only node executable. Each node process runs the whole stack
over an isolated Azurite instance: replication with ASP.NET hosting and the HTTP peer transport, an ObjectDB restored
from Blob storage into `OnDiskReplicationFileStorage`, the EventLog service with its Azure storage and HTTP peers, an
`EventPublisher` over the `data` and `metadata` topics, and a `BackendActionsHost` using the node's
`ReplicationLeadership` and `ReplicationLeaderChannel`. The first node leads and publishes the schema; the others
restore it and follow. Test controls live under `/test`.

Run with `azurite-blob` on PATH (or `BTDB_AZURITE_EXECUTABLE` configured):

```sh
dotnet test BTDB.BackendActions.Process.Test/BTDB.BackendActions.Process.Test.csproj
```

The action sleeps for a given time and records its executions per node. Its `OnFinished` counts every handled outcome
in a replicated relation, so each replica shows whether an action finished exactly once.

Scenarios:

- **Healthy cluster.** Twelve actions enqueued on a follower run once across three nodes; every replica handled each
  as one `Completed` outcome of attempt 1.
- **Killed worker.** A follower is killed with its actions running. After the worker timeout (3 s) they run again as
  attempt 2 on the other nodes; both survivors handled every action exactly once.
- **Killed leader.** The leader, with its coordinator, worker and possibly the EventLog topic ownership, is killed
  while actions run on every node. After the 15 s replication lease a follower takes over: the survivors' attempts
  finish as attempt 1, and the killed node's actions run again. Each survivor handled every action exactly once.

Nodes use 6 s attempt leases, a 3 s worker timeout and a 1 s poll timeout, shorter than the defaults.

Limits: loopback sockets and Azurite rather than production TLS and Azure; no partitions, upgrades or restarts on the
same directory yet; a single application database.
