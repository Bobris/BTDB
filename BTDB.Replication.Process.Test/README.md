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
- **Planned upgrade handoff.** A restored generation-2 follower reports its prepared upgrade; the generation-1 leader
  drains its grants and transfers the lease without waiting for expiry (about 1.2 s on Azurite and live Azure), and
  then follows the new leader without ever contending again.
- **Isolated leader.** A leader cut off from Blob storage and from its peers, while the process and its local input
  keep running, gives up leadership when its lease authority ends; the follower takes over after expiry, and the healed
  old leader follows and compares the new history.
- **Follower cut off from its leader.** With every replication connection to the leader dropped for longer than the
  lease, the follower keeps applying its input but never contends while the leader renews; after healing it compares
  its tail and continues.
- **Kill during restore.** A node whose Blob reads are slowed is killed in the middle of its restore; restarted on the
  same directory it discards the partial files, restores the published history and follows.
- **Partial multi-database activation.** With two databases, a lease winner whose second database lags behind the
  published history stays `Activating` and adopts and publishes nothing, while local input continues; once the lagging
  input is applied both databases activate and publish.

`PublicHostingApiTest` guards the external-consumer boundary: internal authority and transition types stay hidden and
public records redact credentials.

Set `BTDB_AZURE_BLOB_ENDPOINT=https://<account>.blob.core.windows.net` to run the same scenarios against live Azure;
every node process then authenticates with `DefaultAzureCredential` (Storage Blob Data Contributor). The recorded live
run is in [ObjectStorages.md](../BTDB.Replication/ObjectStorages.md).

Nodes use the production `SystemReplicationScheduler` with a 1000 ppm drift bound and a 250 ms safety margin. Test-only
controls simulate partitions (a Blob pipeline policy that fails every request, middleware that drops replication
connections), slow Blob reads (`BTDB_TEST_READ_DELAY_MILLISECONDS`), a second database (`BTDB_TEST_DATABASES`) and a
higher application generation (`BTDB_TEST_GENERATION`).

Limits: loopback sockets, not production TLS/proxies; partitions are injected per node rather than in the network;
disk-full and device errors during restore are not simulated.
