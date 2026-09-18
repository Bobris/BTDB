# M1 executable authority and publication model

2026-09-14. Implemented internal model primitives and focused tests; not a replication runtime or a production
storage adapter. M1 has concrete evidence for finite authority, grant draining and direct TRL CAS. Production
integration and provider clock qualification remain work for the following milestones; the model does not close B1/B5.
KVI restore uses the existing dependency-first publication rule and requires no additional M1 selection mechanism.

## Mechanisms admitted by counterexamples

| Mechanism | Why the simpler design fails | Executable evidence |
| --- | --- | --- |
| Lease deadline anchored before request dispatch | A delayed success otherwise invents a fresh lease period after the remote lease can expire. | `AuthorityTest.DelayedAcquireDoesNotStartANewLeasePeriodAtResponseTime` |
| Irreversible session fencing | Even a successful remote renewal after expiry cannot undo the interval in which another leader could have acted. | `LeaseServiceTest.RemoteRenewAfterExpiryCannotReviveLocallyFencedSession` |
| Request/challenge identity | An old response must not answer a newer pending request or extend a new window. IDs are local to the already validated session. | `AuthorityTest.OlderResponseCannotOverrideTheCurrentRequest`, `DelayedChallengeRepliesAndClosedConnectionsCannotConfirm` |
| One maximum grant deadline | Early lease transfer can leave valid predecessor grants. Stop issuing and wait out their maximum possible expiry; no individual revocation messages or acknowledgement set is required. | `AuthorityTest.OneDrainDeadlineWaitsOutEveryGrantAndStopsNewGrants` |
| Authority term in the TRL CAS object | A lease on leader.json does not protect data blobs; even same-byte adoption must invalidate the old tail token. | `LeaseServiceTest.LeaderLeaseDoesNotFenceAnotherBlobAndChangingLeaseDoesNotChangeEtag`, `PublicationTest.AdoptionAndOldAppendHaveExactlyOneCasWinner` |
| Selected successor key plus native file ID | Prepared files must not select themselves. A never-reused branch key locates the object; its native ID supplies the restore address (the native header contains the previous ID, not its own). | `PublicationTest.PreparedSuccessorDoesNotSelectItselfAndLinkSealsPredecessor`, `NativePublicationTest.SelectedMultiFileTransactionReopensWithoutChangingNativeBytes` |
| Explicit pending outcome | A read of the old version after timeout is compatible with a later successful effect. | `PublicationTest.TimeoutAndOldReadRemainPendingUntilTheRequestLands` |

No operation ID, append hash, metadata format version, per-transaction HTTP envelope, pushed bytes, peer durability
classification or per-follower revocation protocol was added. For this model, consistent content + metadata + ETag is
sufficient to recognize the exact desired result. `Changed` never means the old operation did not execute.
No production allocation/performance claim follows from the model's full-buffer copies.

## Concrete candidate TRL representation

The object body is unchanged native BTDB TRL. `TrlMetadata` encodes ASCII string metadata:

- `btdb_term`: nonzero unsigned decimal selected term.
- `btdb_next`, `btdb_next_id`: either both absent or a namespace-relative, never-reused object key and positive native
  file ID. The object namespace supplies database/stream scope. These fields change atomically with content.

`TrlSnapshot` represents one consistent content/metadata/token read. The adapter must bind multipart reads to one
version. The native codec is not changed, and no envelope is prepended to a TRL or KVI. A known genesis key is the
model's discovery root; native checkpoint restore and remote file allocation belong to M3/M6.

| Mutation | Precondition | Desired result | Ambiguity handling |
| --- | --- | --- | --- |
| Genesis | Root key absent; complete prepared recovery bytes | First selected content and term | Missing means pending; exact result recognizes publication; another result means recover winner. |
| Append/link | Expected tail ETag, matching term, no existing successor | Existing prefix plus complete transaction closure, optional prepared successor link | Old ETag means pending; exact result is observed; a different result requires history reconciliation. |
| Adoption | Expected unlinked tail ETag and newer selected term | Same bytes, newer term, changed ETag | If old append won, preserve its complete bytes and adopt that new end. If link won, follow it before adopting. |

Only one CAS using the same predecessor token wins. After a successful link, the predecessor is immutable and the
successor is the next candidate tail. Preparation must finish before linking; a lost predecessor CAS leaves unselected
objects, never permission to publish from them. Unique names are supplied by the test; durable name allocation and
orphan collection are not implemented. The intent helper does not decode transaction cuts or enforce preparation.
Those caller preconditions need the M2 decoder/capture and M3 publisher.

The native replay test creates a transaction spanning multiple files, stages its successor chain, restores the old
commit before linking, then restores the selected commit/rollback after linking. It checks actual values and
CommitUlong through a fresh BTreeKeyValueDB. This is stronger than the separate synthetic M0 history oracle, but it
is not a production incremental decoder or a general restore implementation.

## Supported mathematical clock model

Let each local clock rate relative to service time lie in `[1-r, 1+r]`, with `0 <= r < 1`. Clocks never step backwards
and continue advancing while the process or OS is paused. `L` is the service's guaranteed minimum lease duration.
With local request-start time `s` and positive operational margin `m`, the local authority deadline is:

`D = s + floor(L * (1-r)) - m`.

Authority must be checked at the action's serialized decision point and expire at `now >= D`. Acquisition begins
without authority. Ambiguous renewal leaves only the previously proven deadline. Once that deadline passes, a new
session/selection is needed; a late response cannot revive the old session. Overlapping responses are conservatively
accepted only for the newest request.

For a follower challenge duration `W`, the leader waits at most the following bound in its own clock units:

`B = ceil(W * (1+r) / (1-r))`.

A challenge starts at the follower before transmission. Issuance occurs later, so waiting `B` from issuance is
conservative even for delayed responses and disconnected followers. Issue only while `now + B < D`. Track the maximum
`now + B`, stop issuing during drain, and transfer only after that time. These inequalities are tested at both rate
extremes with integer rounding. No synchronized wall clocks are required.

The deterministic scheduler models continued elapsed time during process pauses. No production clock implementation,
OS suspend qualification, service-rate bound, operational margin or atomic role/commit integration is qualified here.
The finite lease simulator covers acquire/renew/change/release, including renewal after expiry without intervening
mutation. It does not model break, the provider's possible acquire delay after expiry, or all Azure error variants.
GET observations never renew local authority. Out-of-band administrative early lease release/break is outside the
cooperative grant-transfer proof and must not be used as an automatic failover shortcut.

## KVI publication and restore: existing ordering is sufficient

KVI is selected only when opening/restoring a database from Blob Storage into local files. It does not participate
in live follower synchronization. All PVL and TRL files referenced by a KVI must be successfully published before
publishing that KVI, including before starting its upload. Only after replacement KVI publication succeeds may
obsolete remote files become eligible for deletion. Actual deletion is planned with a configurable operational delay,
provisionally about one day from obsolescence. Restore selects the newest KVI, copies its dependencies and replays the tail.
If concurrent cleanup removes a needed file, restore restarts from the newest KVI.

`NativeCheckpointStillNeedsItsReferencedValueFiles` generates a real native KVI, reopens it with its files, then
manually deletes a value-bearing TRL. Native open subsequently returns an empty database at event 0. This is only
a fixture demonstrating that incomplete local copies must not be accepted as successful restoration. It does not
exercise the publication/cleanup ordering, delayed uploads or checkpoint selection, and does not establish a need
for extra ancestry metadata, a selection protocol or an M1 blocker. The earlier conclusion claiming such a
counterexample was incorrect.

Implement the existing ordering and restore retry in M3, and remote file-ID/offset mapping in M6. No extra KVI
protocol is required by the current evidence. Remote allocation/key non-reuse and authority/term selection are
ordinary publisher/role integration work; the M1 helpers assume selected authority and validated session context.
