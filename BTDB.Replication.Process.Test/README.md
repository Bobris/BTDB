# Replication subprocess qualification

This non-packable test assembly doubles as a loopback-only node executable. The tests launch independent .NET
processes and exercise the actual coordinator, ASP.NET hosted integration, HTTP peer transport, native BTDB and
Azure lease/TRL adapters against an isolated Azurite instance. Test controls and the injected publication gate exist
only in this project; they are not production endpoints or protocol fields.

Run with `azurite-blob` on PATH (or `BTDB_AZURITE_EXECUTABLE` configured):

```sh
dotnet test BTDB.Replication.Process.Test/BTDB.Replication.Process.Test.csproj
```

The crash scenario publishes event 1, restores a follower, pauses publication, and compares event 2 over HTTP while
Blob still restores only event 1. It kills the leader OS process without releasing or breaking the lease, waits for
its real 15-second lease to expire, and verifies the follower publishes its existing event 2 without handler reexecution.
A third process with no local cache restores that result, then compares event 3 against the new leader. Native values,
application invocation counts, selected leader identity/term and canonical restore cursors are checked.

The divergence scenario applies a different event-2 value on the follower, verifies the actual follower process exits
through the hosted restart path, and checks the leader and canonical history retain their prior state.

The first scenario exposed a core recovery defect: a complete published commit has no local temporary-end marker,
so a cold follower unnecessarily rotated to TRL 3 while the live leader appended to TRL 1. Replication restore now
continues the exact committed physical EOF. Focused tests retain rotation for incomplete, corrupt and explicitly sealed
tails. Standalone recovery is unchanged.

Limits: these tests use local Azurite and loopback sockets, not live Azure or production TLS/proxies. Node caches are
in-memory and process-isolated; killing a process discards its entire cache. The test scheduler uses Stopwatch and
serialized timers for this non-suspended workload; it is not a production clock qualification. Broader partitions,
process suspension, rolling-upgrade handoff and workload/restore performance remain separate acceptance work.
