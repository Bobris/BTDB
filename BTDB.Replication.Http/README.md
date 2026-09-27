# HTTP replication peer adapter

This non-packable adapter connects `IReplicationPeerTransport` to ASP.NET Core/Kestrel and `HttpClient`, and registers
a node in an ASP.NET host. The coordinator keeps owning authentication decisions, grants, comparison, schema
detachment and handoff; the adapter adds no application execution, Blob fallback, retries or canonical state.

## Hosting

Register the host-owned `IReplicationNodeHost`, `ILeaderRecordStorage`, `IReplicationLeaseStorage` and
`IReplicationScheduler` as singletons, call `services.AddBTDBReplication(nodeOptions, maximumClockDriftPpm,
safetyMargin)` and map `app.MapBTDBReplication()` before starting the host; see the
[hosting guide](../Doc/ReplicationHosting.md). One node, transport and hosted service are registered per host;
duplicate registration, invalid options and a missing route fail before lease acquisition. Readiness is exposed as the
`btdb-replication` health check and the `BTDB.Replication` meter.

The hosted service waits for `ApplicationStarted`, so Kestrel listens before restore or lease contention.
`ApplicationStopping` synchronously fences lease authority and cancels coordination; normal shutdown then awaits
cleanup within the host's shutdown deadline. Application databases stay application-owned and must outlive that
cleanup. A requested rebuild or an unexpected worker failure stops the host, even under the `Ignore` background-failure
policy. No wall-clock scheduler fallback is installed.

The advertised endpoint is an absolute HTTPS origin without credentials, query, fragment or path; HTTP is accepted only
on loopback. Requests go to `POST /_btdb/replication`; redirects are rejected. Certificates and routing belong to the
host. Do not log the Authorization header; the adapter never logs identities, credentials or bodies.

## Wire boundary

Each request carries the selected cluster ID, term, session ID and endpoint plus one operation: `connect`, `poll`,
`read` or `handoff`, in a compact versioned binary encoding. The API key travels only as a Bearer header. Every request
reopens the exact selected leader session, so there is no server session registry or resume token; identity is
revalidated after awaited work and a replaced listener rejects old requests.

- `poll` returns the echoed challenge, the grant, and for each requested database its progress
  `(eventId, trlFileId, trlPosition)`, published cut, latest schema-commit position and inline TRL bytes from the
  requested position (within the requested budget, at most 4 MiB). No databases means an authority-only heartbeat.
- `read` returns at most 256 KiB of completed captured TRL bytes. A range the leader no longer retains returns HTTP 410
  and becomes `FileNotFoundException`, which restarts the follower; other rejections become `IOException`.
- `handoff` forwards a prepared-upgrade offer. `connect` and `handoff` reply with HTTP 204.

Control requests are limited to 16 KiB. The client validates challenge, lengths and inline chunk continuity. The server
admits 32 simultaneous requests by default and answers excess requests with HTTP 429 without queueing.

## Tests

`dotnet test BTDB.Replication.Http.Test/BTDB.Replication.Http.Test.csproj` runs real loopback Kestrel listeners:
progress, heartbeat, ranges, inline bytes, prepared handoff, identity and credential changes, stale in-flight replies,
cancellation, unavailable files, overload, redirects, message limits and malformed replies. Native BTDB histories with
TRL rotation, rollback and different virtual batching cross the HTTP boundary. Hosted-service tests run the actual
coordinator inside Kestrel. The [subprocess suite](../BTDB.Replication.Process.Test/README.md) covers separate
processes. TLS/proxy deployment, large-cluster load and live-Azure qualification remain open.
