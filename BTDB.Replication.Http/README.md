# HTTP replication peer adapter

This internal, non-packable adapter connects `IReplicationPeerTransport` to ASP.NET Core/Kestrel and `HttpClient`.
The provider-neutral coordinator continues to own leader authentication, grants, comparison, schema detachment and
handoff. It adds no application execution, Blob fallback, background retries or canonical state.

## Hosting

Register the host-owned `IReplicationNodeHost`, `ILeaderRecordStorage`, `IReplicationLeaseStorage` and
`IReplicationScheduler` as singletons, then call `services.AddBTDBReplication(nodeOptions, maximumClockDriftPpm,
safetyMargin)`. Call `app.MapBTDBReplication()` before starting the ASP.NET host. These extension methods remain
internal alongside the underlying application/storage contracts. They register one HTTP transport, lease controller,
coordinator and hosted service per host; duplicate node/route registration and invalid options fail early. A missing
route prevents hosted-service startup and lease acquisition.

The hosted service waits for `ApplicationStarted`, so Kestrel is listening before restore or lease contention begins.
`ApplicationStopping` synchronously closes the lease controller and cancels coordination. Even a storage call that
ignores cancellation cannot renew the old authority after shutdown begins. Normal shutdown awaits coordinator cleanup;
the host's shutdown deadline still bounds waiting for an uncooperative provider. Application databases remain owned by
the application and must outlive that cleanup. The service never disposes them or cancels application event execution.
A coordinator-requested rebuild or unexpected worker failure requests host shutdown; exceptions remain visible through
the background task even when the host uses the `Ignore` background-failure policy.

The host must still provide a qualified monotonic scheduler (including the specified pause/drift behavior), storage,
application execution and process restart policy. No wall-clock fallback is installed by DI. Public contract promotion,
release packaging, readiness/progress metrics and bounded process termination policy remain future work.

Manual construction remains available: map `transport.Map(app)`, start Kestrel, then run the coordinator with that
transport. Stop and await coordination before disposing the host or transport.

The advertised endpoint is an absolute HTTPS origin without credentials, query, fragment or path. HTTP is accepted
only for loopback tests. Requests go to `POST /_btdb/replication`. Configure certificates and external routing on the
host; redirects are rejected. Request deadlines come from coordinator cancellation. Do not enable logging of the
Authorization header. The adapter does not log identities, credentials, bodies or exception details to responses.

## Wire boundary

Control requests contain the selected cluster ID, term, session ID and advertised endpoint plus one operation:
`connect`, `poll`, `read` or `handoff`. The API key is sent only as a Bearer Authorization header. Every request opens
the exact selected core session again; no additional server session registry, resume token or protocol identity is
required. Identity is revalidated after awaited work before sending the response. A stopped/replaced listener rejects
old requests. Client session disposal cancels requests and body reads.

`poll` returns the echoed challenge, grant result and optional native `(EventId, TrlFileId, TrlPosition)` progress.
A null database requests an authority-only heartbeat. `read` returns raw bytes for the requested database/file/offset;
it delegates to the existing leader reader, which limits access to completed captured history. Missing retained files
return HTTP 410 and become `FileNotFoundException`, preserving the coordinator's restart path. Other rejected requests
become `IOException`; cancellation remains cancellation. `handoff` forwards the existing prepared-upgrade offer.
Successful connect/handoff replies are HTTP 204.

Control bodies are limited to 16 KiB; a read transfers at most 256 KiB. The client verifies progress, challenge and
response lengths and rejects oversized/truncated data. The server admits at most 32 simultaneous requests by default
(configurable in the constructor), returning HTTP 429 immediately for excess requests. It does not queue work or
retry a handoff. Buffer ownership lasts through the response write, without retaining a BTDB root for the peer.

## Evidence and remaining work

`dotnet test BTDB.Replication.Http.Test/BTDB.Replication.Http.Test.csproj` starts real loopback Kestrel listeners.
Coverage includes progress, heartbeat, ranges, prepared handoff, identity/credential changes, stale in-flight replies,
request/session cancellation, unavailable files, overload, redirects, message limits and malformed/truncated replies.
Real native BTDB histories cross the HTTP boundary with TRL rotation, rollback and different virtual batching;
matching histories advance acknowledgement and divergence does not.

Hosted-service tests run the actual coordinator and lease controller inside Kestrel. They cover a live authenticated
heartbeat, restore-before-election ordering, restore retry/cancellation, immediate fencing during an uncooperative
renewal, restart-required shutdown, fatal worker failure, missing routes and invalid/duplicate configuration.

The HTTP suite consists of socket-level component tests in one process. The separate
[subprocess suite](../BTDB.Replication.Process.Test/README.md) now covers leader death, lease-expiry takeover,
optimistic-tail publication, cold restore and divergence against Azurite. Broader fault schedules, TLS/proxy deployment,
public contract promotion, operational metrics, large-cluster load and live-Azure recovery/throughput qualification
remain required before production release. This adapter does not change the remaining exact-skip, backup-reset or
Azure retained-root/TRL pruning work (canonical TRL pruning remains disabled in the Azure adapter).
