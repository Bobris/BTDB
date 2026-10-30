# Replication packages and release preparation

The complete replication family targets .NET 10 and is published on NuGet with the same version as BTDB. Up to
35.13.0 the packages had a `-preview` suffix (the first was `35.11.0-preview`); from 36.0.0 they have none. They are not
production-qualified yet, see [qualification status](#qualification-status).

| Package | Included functionality | Dependencies |
| --- | --- | --- |
| `BTDB.Replication` | Database coordination, publication, restore, checkpoints, cleanup and the optional `BTDB.Replication.EventLog` service and contracts. | `BTDB` at the matching base version. |
| `BTDB.Replication.Http` | ASP.NET Core hosting, peer transport, health checks, metrics, EventLog hosting and transport. | `BTDB.Replication` at the matching version; ASP.NET Core shared framework. |
| `BTDB.Replication.Azure` | Blob leases, leader records, canonical database storage and EventLog storage. | `BTDB.Replication` at the matching version; Azure Storage SDK. |
| `BTDB.Replication.EventSourcing` | Typed validation, separate Layer2 data/metadata topics, ambient ObjectDB consumption, result waits, restart replay, post-commit actions and live consumer discovery. | `BTDB.Replication` at the matching version. |

EventLog is included in this family, with no separate package. `BTDB.AzureStorage` is a separate file-collection
adapter and is not needed by the replication adapters. `BTDB.SourceGenerator` is optional for an application's IOC
usage; it is not required for replication or EventLog hosting.

For the higher-level application wrapper, also install `BTDB.Replication.EventSourcing` at the same version;
see [typed event publishing](ReplicationEventSourcing.md). This new package is prepared for the next release.
Applications using its default BTDB metadata path also reference SourceGenerator at the matching base version.

## Install

For an ASP.NET Core application backed by Azure Blob Storage:

```sh
dotnet add package BTDB.Replication.Http
dotnet add package BTDB.Replication.Azure
```

NuGet restores the core and BTDB transitively. Use the same version for all replication packages. If only the
provider-neutral core is needed, install `BTDB.Replication` alone.

The [hosting guide](ReplicationHosting.md) covers application-owned database restore, input replay, leader storage,
shutdown, and the optional EventLog registration. The [Azure adapter README](../BTDB.Replication.Azure/README.md)
covers client configuration and authentication. The application provisions its Blob container and credentials.

## Prepare the next release

Run from the repository root with a .NET 10 SDK, network access for restore, and Azurite available for adapter and
subprocess tests:

```sh
dotnet test BTDB.SourceGenerator.Test/BTDB.SourceGenerator.Tests.csproj -c Release
dotnet test BTDB.sln -c Release
dotnet run --project Releaser/Releaser/Releaser.csproj -c Release -- prepare patch
```

`prepare` defaults to `patch`; `prepare minor` and `prepare major` select the corresponding increment from the last
released changelog version. It packs all nine projects with the selected version passed as an MSBuild property:
all of them, the four replication packages and the two [LSM packages](LsmKeyValueDB.md) included, use that version
without a suffix.
Packages and symbol packages (where configured) are written to `artifacts/package/prepare/<base-version>/`.
For example, after `35.13.0`, `prepare patch` creates `35.13.1` artifacts.

Preparation does not read publishing tokens, push packages, change tracked project versions or the changelog, create
a commit or tag, or delete existing artifacts. It does not run tests itself. A package consumer check should restore
the two adapters from that local directory, without project references, and exercise the public hosting and EventLog
APIs. Check the generated nuspec dependencies, embedded symbols, README and repository metadata before publication.

## Publish

Commit the reviewed changes and run the existing release command with the same increment:

```sh
dotnet run --project Releaser/Releaser/Releaser.csproj -c Release -- patch
```

This command rebuilds and publishes all nine packages, updates tracked project versions and the changelog, commits and
pushes the release and tag, and creates the GitHub release. It reads the NuGet and GitHub tokens from the existing
user-profile token files. Use it only when ready to publish. NuGet versions cannot be replaced; changes after a
published version require a new version.

## Qualification status

Publication leaves [B3: production-application qualification](../BTDB.Replication/Architecture.md#open-work)
open. EventLog application integration, migration and multi-VM qualification also remain open in its
[implementation plan](../BTDB.Replication/EventLogImplementationPlan.md#11-implementation-milestones).
The preview suffix was dropped on 2026-10-30 as a release decision, to version all BTDB packages alike; it closes none
of these items.
