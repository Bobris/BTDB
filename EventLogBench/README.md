# EventLogBench

A multi-node benchmark of `BTDB.Replication.EventLog` and of event-sourcing consumption through
`BTDB.Replication.EventSourcing`. One container image has two roles:

- **node** (default): hosts one EventLog node over HTTPS peers (port 8443) with `AzureEventLogStorage`, and a plain
  HTTP control API (port 8080). For event-sourcing scenarios it also opens a local on-disk ObjectDB per scenario and
  an `EventPublisher` whose handler upserts one relation row per event.
- **driver** (`dotnet EventLogBench.dll driver ...`): runs a scenario matrix against the nodes and writes JSON.

Every node publishes its share of the load and consumes the whole topic, so a record's end-to-end latency ends when the
last node has it (raw EventLog: the reader yields it; event sourcing: its batch is visible to queries). Before each
scenario the driver estimates each node's clock offset from the minimum-RTT sample of 40 clock reads and passes every
node the common start instant in its own clock. Nodes record milliseconds since that instant, and the driver joins
publications and deliveries of all nodes. The minimum RTT (error bound twice the offset error) and the drift over the
scenario are reported with every result; in AKS they were 0.1–0.5 ms and below 0.03 ms.

## Scenarios

`--suite full` runs, for the given number of nodes:

| Name | Load |
| --- | --- |
| `el-nN-rateR-b1024` | Raw EventLog, 1 KiB records, open loop at R records/s in total (100, 1,000, 5,000, 10,000), spread evenly over the nodes. |
| `el-nN-cC-b1024` | Raw EventLog, closed loop: C publish-and-await loops on every node (1, 16, 128, 1,024). |
| `es-nN-rateR-b256` | `PublishAndWaitAsync` of an event with a 256-byte payload, open loop. |
| `es-nN-cC-b256` | Event sourcing, closed loop. |

Open-loop latency is measured from the scheduled publication time, so queueing behind a saturated owner counts.
Latency statistics exclude publications scheduled in the first second. Throughput counts receipts between the first
and the last second. The first node prepared publishes first and owns the topic. Each result includes:

- `receipt`: publish call to receipt (event sourcing: to the local result after consumption), split by owner and
  other nodes;
- `selfDelivery`, `perNodeDelivery`, `allDelivery`: publication to delivery on the publishing node, each node, and
  the last node;
- `timeline`: receipts and latency per second;
- `storage`: the owner's storage writes (count, latency, mean size, new bytes per write) and, for event sourcing, the
  split of latency into EventLog receipt, reader-to-handler queueing and handler-to-visibility.

## Local smoke run

```bash
dotnet build EventLogBench -c Release
azurite-blob --blobPort 10100 --location /tmp/azurite &   # with AZURITE_ACCOUNTS set as in BlobStorageFixture
POD_NAME=elb-0 PEER_ENDPOINT_FORMAT="http://127.0.0.1:910{0}" PEER_PORT=9100 CONTROL_PORT=9200 STORAGE_PREFIX=smoke \
  API_KEY=k BLOB_CONNECTION_STRING="..." dotnet artifacts/bin/EventLogBench/release/EventLogBench.dll &
# ...start elb-1 and elb-2 with ports 9101/9201 and 9102/9202
dotnet artifacts/bin/EventLogBench/release/EventLogBench.dll driver --nodes 3 --control "http://127.0.0.1:920{0}" \
  --seconds 6 --suite quick
```

Without `TLS_CERT` the peer port is plain HTTP, which the transport accepts only for loopback endpoints.

## AKS

`deploy/bench.yaml` defines the headless service, the StatefulSet (one pod per labelled VM, spread over zones) and a
driver pod. `deploy/run.sh` creates a private CA and peer certificate, builds the image with `az acr build`, and for
each replica count recreates the StatefulSet with fresh pods and a fresh storage prefix, runs the driver and copies its
JSON to `deploy/results`. Node options come from environment variables: `BLOB_ENDPOINT` (workload identity) or
`BLOB_CONNECTION_STRING`, `BLOB_CONTAINER`, `STORAGE_PREFIX`, `API_KEY`, `TLS_CERT`/`TLS_KEY`, `DATA_ROOT` and
`MAX_PUBLISH_ROUNDS`, `COMMIT_LINGER_US` and `ADAPTIVE_LINGER_MAX_US` (override `EventLogOptions.MaxPublishRoundsInFlight`,
`CommitLinger` and `AdaptiveCommitLingerMax`; the adaptive window is on by default, `ADAPTIVE_LINGER_MAX_US=0` turns it
off).
`--suite linger` runs the unsaturated subset (100–5,000/s and 1–128 loops) used to compare commit windows;
`--suite adaptive` adds mixed scenarios (1,000/s open load plus 4 publish-and-await loops per node at once), reports
open and closed receipts separately and samples the owner's commit window.

Results of the run on Standard_E8ads_v5 nodes with Standard_GZRS storage are in
[Measurements.md](../BTDB.Replication/Measurements.md#event-log-on-aks).
