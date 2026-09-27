# M1 authority and publication evidence

Status 2026-09-27. This records the finite-authority model, the grant drain bound and the direct TRL CAS
representation, with the tests that motivate each mechanism. The mechanisms are used by the implemented runtime
(see [ImplementationPlan.md](ImplementationPlan.md)); production clock and live-provider qualification remain open,
so this evidence does not close B1 or B5 in [Architecture.md](Architecture.md). All tests are in
`BTDB.Replication.Test` unless noted.

## Mechanisms admitted by counterexamples

| Mechanism | Why the simpler design fails | Evidence |
| --- | --- | --- |
| Lease deadline anchored at request dispatch | A delayed success would otherwise start a fresh lease period after the remote lease may have expired. | `AuthorityTest.DelayedAcquireDoesNotStartANewLeasePeriodAtResponseTime` |
| Ambiguous renewal keeps only the proven deadline | A renewal whose reply arrives after a pause cannot extend authority. | `AuthorityTest.AmbiguousRenewalAndProcessPauseCannotExtendAuthority` |
| Irreversible session fencing | A successful remote renewal after local expiry cannot undo the interval in which another leader could act. | `LeaseServiceTest.RemoteRenewAfterExpiryCannotReviveLocallyFencedSession` |
| Request and challenge identity | An old response must not answer a newer request or extend a newer window. Lease requests are numbered per session; each poll carries one challenge, and a follower counts a grant only from its own dispatch time and only if it arrives within the window (`ReplicationNodeCoordinator.PollLeaderAsync`). | `AuthorityTest.OlderResponseCannotOverrideTheCurrentRequest`, `ReplicationPeerPollTest.StaleIncompleteReorderedOrInvalidAnswersAreRejected`; no dedicated test yet for a grant arriving after its window |
| One maximum grant deadline | Early lease transfer can leave valid predecessor grants. Stop issuing and wait out their maximum possible expiry; no per-follower revocation or acknowledgement is needed. | `AuthorityTest.OneDrainDeadlineWaitsOutEveryGrantAndStopsNewGrants` |
| Authority term in the TRL CAS object | A lease on `leader.json` does not protect data blobs; even same-byte adoption must invalidate the old tail token. | `LeaseServiceTest.LeaderLeaseDoesNotFenceAnotherBlobAndChangingLeaseDoesNotChangeEtag`, `CanonicalTrlPublisherTest.AdoptionChangesOnlyMetadataAndFencesDelayedOldTermAppend`, `CanonicalTrlPublisherTest.OldAppendWinningFirstForcesTheAdopterToRediscoverInsteadOfDroppingHistory` |
| Selected successor key plus native file ID | Prepared files must not select themselves. A never-reused key locates the object; its native ID is the restore address, because a native header stores only the previous ID. | `CanonicalTrlPublisherTest.MultiFileGenesisSelectsItsRootOnlyAfterSuccessors`, `CanonicalTrlPublisherTest.AuthorityLossAfterPreparingASuccessorNeverSelectsIt`, `NativePublicationTest.SelectedMultiFileTransactionReopensWithoutChangingNativeBytes` |
| Explicit ambiguous outcome | Reading the old version after a timeout is compatible with a later successful effect; a retry must not duplicate it. | `StorageFaultTest.TimeoutAndOldReadDoNotPreventLaterRemoteEffect`, `CanonicalTrlPublisherTest.ExactRetryCannotDuplicateBytesWhenOriginalRequestLandsLate` |

No operation ID, append hash, metadata format version, per-transaction envelope or per-follower revocation protocol
was needed. Consistent content, metadata and ETag recognize the exact intended result; a changed result never means
that the old operation did not execute.

## TRL representation

The object body is unchanged native BTDB TRL; no envelope is prepended to a TRL or KVI. `TrlMetadata` is committed
atomically with the content:

- `btdb_term`: nonzero unsigned decimal selected term.
- `btdb_next`, `btdb_next_id`: both absent, or a namespace-relative never-reused successor key and its positive native
  file ID. The namespace supplies database scope.

`TrlObjectState` is one consistent token/length/metadata read; adapters bind ranged reads to that version. Discovery
starts at the known genesis key or, once a checkpoint exists, at the retained-root hint stored on the newest KVI.

| Mutation | Precondition | Result | Ambiguity handling |
| --- | --- | --- | --- |
| Genesis | Root key absent; complete prepared recovery bytes | First selected content and term | Absent means still pending; the exact result means published; any other result means recover the winner. |
| Append/link | Expected tail token, same term, no successor | Prefix plus complete transactions, optionally linking a prepared successor | Old token means pending; exact result means applied; a different result requires rediscovery. |
| Adoption | Expected unlinked tail token, newer term | Same bytes, newer term, new token | If an old append won, rediscover and adopt its end; if a link won, follow it first. |

Only one CAS against a given predecessor token wins. After a successful link the predecessor is immutable and the
successor becomes the tail candidate. Successors are prepared before the predecessor CAS; losing that CAS leaves
unselected objects, never permission to publish from them.

## Clock model

Each local monotonic clock runs at a rate within `[1-r, 1+r]` of service time, `0 <= r < 1`, never steps backwards
and keeps advancing while the process or OS is paused. `L` is the service's guaranteed minimum lease duration and `m`
a positive safety margin. For local request-start time `s`, the local authority deadline is

`D = s + floor(L * (1-r)) - m`.

Authority is checked at each serialized decision point and expires at `now >= D`. Acquisition starts without
authority, ambiguous renewal keeps only the previously proven deadline, and once `D` passes a new session and
selection are required; a late response cannot revive the old session.

For a follower grant window `W`, measured by the follower from before it sends the poll, the leader bounds the
window in its own clock units by

`B = ceil(W * (1+r) / (1-r))`.

The leader issues a grant only while `now + B < D`, tracks the maximum `now + B`, stops issuing during drain and
transfers the lease only after that time. `AuthorityTest.WorstClockRatesKeepPeerExpiryInsideServiceLease` checks
these inequalities at both rate extremes with integer rounding. No synchronized wall clocks are required.

Not qualified here: the production clock implementation, OS-suspend behavior, the service rate bound, the operational
margin and all Azure lease error variants (break, delayed acquire after expiry). GET observations never renew local
authority. Administrative early lease release or break is outside the cooperative transfer argument and must not be
used as an automatic failover shortcut.

## KVI ordering needs no extra mechanism

KVI is used only when opening or restoring a database from Blob Storage, never in live following. Every PVL and TRL a
KVI references is published before its upload starts, and obsolete files become deletable only after the replacement
KVI is published. If cleanup removes a needed file during restore, restore starts over from the newest KVI.
`NativePublicationTest.NativeCheckpointStillNeedsItsReferencedValueFiles` only shows that an incomplete local copy
must not count as a successful restore (native open then returns an empty database); it does not justify ancestry
metadata or a selection protocol.
