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
| Unchanged-content tail adoption | A lease on `leader.json` does not protect data blobs; even same-byte adoption must invalidate the old tail token. | `LeaseServiceTest.LeaderLeaseDoesNotFenceAnotherBlobAndChangingLeaseDoesNotChangeEtag`, `CanonicalTrlPublisherTest.AdoptionChangesOnlyVersionAndFencesDelayedOldTermAppend`, `CanonicalTrlPublisherTest.OldAppendWinningFirstForcesTheAdopterToRediscoverInsteadOfDroppingHistory` |
| One shared key per native ID | Competing leaders must collide on the same object. Compare bytes on failed creates; incomplete multi-file transactions replay only after their complete native end arrives. | `CanonicalTrlPublisherTest.CreateCollisionComparesExistingNativeBytesBeforeContinuing`, `CanonicalTrlPublisherTest.RestartDuringMultiFilePublicationRegeneratesExactlyTheSameTrls` |
| Explicit ambiguous outcome | Reading the old version after a timeout is compatible with a later successful effect; a retry must not duplicate it. | `StorageFaultTest.TimeoutAndOldReadDoNotPreventLaterRemoteEffect`, `CanonicalTrlPublisherTest.ExactRetryCannotDuplicateBytesWhenOriginalRequestLandsLate` |

No operation ID, append hash, metadata format version, per-transaction envelope or per-follower revocation protocol
was needed. Consistent content, length and ETag recognize the exact intended result; a changed result never means
that the old operation did not execute.

## TRL representation

The object body is unchanged native BTDB TRL; no envelope is prepended to TRL or KVI. All terms use the same
`{fileId}.trl` key. TRL objects carry no protocol metadata. The only Blob metadata fields are `btdb_sha256`
for immutable PVL/KVI content and `btdb_delete_after` for delayed deletion.

`TrlObjectState` is one consistent token/length read; ranges bind to that version. Discovery lists native files,
and native KVI references and TRL predecessor headers determine the retained recovery closure.

| Mutation | Precondition | Result | Ambiguity handling |
| --- | --- | --- | --- |
| Create | Shared native key absent | One published native prefix | Compare existing bytes on collision; accept equality, CAS-extend a matching shorter prefix, restart on divergence. |
| Append | Expected tail token and length | Old bytes plus the intended suffix | Old token remains pending; exact result is accepted; incompatible content requires restart. |
| Adoption | Verified tail and expected token | Same bytes, new token | Rediscover if an old append wins. No term metadata is needed to invalidate the old token. |

Files are published oldest first. A crash may expose only the beginning of a transaction. Recovery exposes only
complete commits/rollbacks, rewinds the unfinished disposable local suffix and regenerates it from application input
with identical IDs, offsets and bytes. Activation compares every published prefix before permitting new publication.

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
