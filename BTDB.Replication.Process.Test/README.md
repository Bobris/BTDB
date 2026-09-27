# Replication subprocess qualification

This non-packable test assembly doubles as a loopback-only node executable. The tests launch independent .NET
processes that use only the public application, hosting and provider contracts (no friend-assembly access). Each node
runs the actual coordinator, ASP.NET hosting, HTTP peer transport, native BTDB with `OnDiskReplicationFileStorage` and
the Azure adapters against an isolated Azurite instance. Test controls and the injected publication gate exist only in
this project.

Run with `azurite-blob` on PATH (or `BTDB_AZURITE_EXECUTABLE` configured):

```sh
dotnet test BTDB.Replication.Process.Test/BTDB.Replication.Process.Test.csproj
```

Scenarios:

- **Unavailable leader.** The leader publishes event 1, a follower restores it, publication is paused and event 2 is
  compared over HTTP only. The leader process is killed, or suspended with `SIGSTOP` (not on Windows), without
  releasing the lease. After the real 15-second lease expires the follower takes over and publishes its existing
  event 2 without re-executing it. A suspended old leader later resumes and follows the new leader, confirming its own
  unpublished tail without a restore. A third process with no local cache restores the result and follows the new
  leader.
- **Crashed follower.** A follower killed without shutdown restarts from its own disk storage, restores canonical
  state instead of replaying its local tail, and continues comparing.
- **Stalled publication.** With a publication deadline, a leader whose publication stops fences, exits through fatal
  recovery, and a follower takes over and publishes its optimistic tail.
- **Divergence.** A follower applying a different event value exits through the restart path; the leader and
  canonical history keep their state.

`PublicHostingApiTest` guards the external-consumer boundary: internal authority and transition types stay hidden and
public records redact credentials.

Limits: local Azurite and loopback sockets, not live Azure or production TLS/proxies. The test scheduler uses
`Stopwatch` with serialized timers; it is not a production clock qualification. Network partitions, rolling-upgrade
handoff across processes and restore performance remain open.
