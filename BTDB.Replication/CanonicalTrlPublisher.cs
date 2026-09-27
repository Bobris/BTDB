using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

public sealed record TrlObjectState(string Token, uint Length, TrlMetadata Metadata);
public sealed record TrlHead(uint FileId, string Key, TrlObjectState State);
public enum TrlWriteOutcome { Applied, Rejected, Ambiguous }
public sealed record TrlWriteResult(TrlWriteOutcome Outcome, TrlObjectState? State = null);
internal enum TrlPublishResult { Idle, Adopted, Published, Pending, AuthorityLost, Conflict }

/// <summary>A fixed native prefix retained by the acknowledgement position, not a copied TRL buffer. A null token means create-if-absent.</summary>
public sealed record TrlWrite(uint FileId, string Key, string? ExpectedToken, uint ExpectedLength, uint Length,
    TrlMetadata Metadata, IFileCollectionFile Source)
{
    public uint AppendLength => Length - ExpectedLength;
    public void ReadAppend(uint offset, Span<byte> destination)
    {
        if ((ulong)offset + (uint)destination.Length > AppendLength) throw new ArgumentOutOfRangeException(nameof(offset));
        Source.RandomRead(destination, (ulong)ExpectedLength + offset, false);
    }
}

/// <summary>
/// One database/selected term, one capture consumer. Input tail must be restored and verified against local bytes.
/// Caller owns canonical ID/key allocation and authority acquisition. This lane never acquires authority itself.
/// Hosts only receive a coordinator-owned instance to pass to ReplicationMaintenance; they never publish or dispose it.
/// </summary>
public sealed class CanonicalTrlPublisher : IDisposable
{
    readonly BTreeKeyValueDB database;
    readonly TransactionLogCapture capture;
    readonly IReplicationStorage storage;
    readonly LeaseAuthority authority;
    readonly ulong term;
    readonly Func<uint, string> keyForFile;

    internal CanonicalTrlPublisher(BTreeKeyValueDB database, TransactionLogCapture capture, IReplicationStorage storage,
        LeaseAuthority authority, ulong term, Func<uint, string> keyForFile, TrlHead? restoredTail = null)
    {
        this.database = database;
        this.capture = capture;
        this.storage = storage;
        this.authority = authority;
        this.term = term;
        this.keyForFile = keyForFile;
        _tail = restoredTail;
    }

    sealed class Plan(TransactionLogPosition? position, TrlWrite[] writes)
    {
        public readonly TransactionLogPosition? Position = position;
        public readonly TrlWrite[] Writes = writes;
        public TrlObjectState? TailState;
        public int Index = writes.Length - 1; // Prepare the final successor first; select the predecessor last.
        public bool Dispatched;
    }

    readonly SemaphoreSlim _lane = new(1);
    byte[]? _localBuffer;
    byte[]? _remoteBuffer;
    TrlHead? _tail;
    Plan? _plan;
    bool _conflict;
    bool _disposed;

    IFileCollectionFile Source(uint id) => database.FileCollection.GetFile(id)
        ?? throw new InvalidDataException("Missing retained TRL source.");

    public void Dispose()
    {
        _lane.Wait();
        try
        {
            if (_disposed) return;
            _disposed = true;
        }
        finally { _lane.Release(); }
    }

    internal bool HasAuthority => authority.IsValid;
    internal void Fence() => authority.Fence();
    internal TrlHead? Tail => _tail;
    internal TransactionLogPosition PublishedPosition => _tail is { } tail ? new(tail.FileId, tail.State.Length) : default;

    /// <summary>Fence the verified predecessor without publishing any optimistic local suffix.</summary>
    internal async ValueTask<TrlPublishResult> AdoptAsync(CancellationToken cancellation = default)
    {
        await _lane.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!authority.IsValid) return TrlPublishResult.AuthorityLost;
            if (_conflict) return TrlPublishResult.Conflict;
            if (_plan?.Position != null) throw new InvalidOperationException("Publication has already started.");
            if (_plan == null && (_tail == null || _tail.State.Metadata.Term == term)) return TrlPublishResult.Idle;
            return await PublishCoreAsync(null, true, cancellation).ConfigureAwait(false);
        }
        finally { _lane.Release(); }
    }

    /// <summary>
    /// Publish the latest complete local prefix, coalescing transactions. An ambiguous operation is reconciled before any later mutation.
    /// retryPending resends only the exact unresolved CAS, with unchanged token/content, while authority is live.
    /// After authority loss this method may reconcile reads but never dispatch another write. The remote token must
    /// be independent of local compaction/execution cancellation.
    /// </summary>
    internal async ValueTask<TrlPublishResult> PublishNextAsync(bool retryPending = false,
        CancellationToken remoteCancellation = default)
    {
        await _lane.WaitAsync(remoteCancellation).ConfigureAwait(false);
        try
        {
            return await PublishCoreAsync(null, retryPending, remoteCancellation).ConfigureAwait(false);
        }
        finally { _lane.Release(); }
    }

    /// <summary>
    /// Publish through a complete position captured from this database (for example a pinned KVI snapshot's cut).
    /// The caller must retain that cut and establish its transaction boundary; arbitrary byte offsets are not valid.
    /// Later local commits are excluded from a new plan. An already dispatched plan must finish unchanged, even if
    /// it extends past the requested cut. Pending, authority loss and conflict never establish the barrier.
    /// </summary>
    internal async ValueTask<TrlPublishResult> PublishThroughAsync(TransactionLogPosition position,
        bool retryPending = false, CancellationToken remoteCancellation = default)
    {
        await _lane.WaitAsync(remoteCancellation).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            // Restore supplies a verified canonical tail before this session has made any local commit.
            if (position.FileId == 0 ||
                (Order(position) > Order(capture.Completed) && Order(position) > Order(PublishedPosition)))
                throw new ArgumentOutOfRangeException(nameof(position));
            if (_conflict) return TrlPublishResult.Conflict;
            var progressed = false;
            while (Order(PublishedPosition) < Order(position))
            {
                var result = await PublishCoreAsync(position, retryPending, remoteCancellation).ConfigureAwait(false);
                if (result is not (TrlPublishResult.Published or TrlPublishResult.Adopted)) return result;
                progressed = true;
            }
            return progressed ? TrlPublishResult.Published : TrlPublishResult.Idle;
        }
        finally { _lane.Release(); }
    }

    static ulong Order(TransactionLogPosition position) => ((ulong)position.FileId << 32) | position.Offset;

    async ValueTask<TrlPublishResult> PublishCoreAsync(TransactionLogPosition? requestedEnd, bool retryPending,
        CancellationToken remoteCancellation)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_conflict) return TrlPublishResult.Conflict;
        if (_plan == null)
        {
            if (!authority.IsValid) return TrlPublishResult.AuthorityLost;
            if (term == 0) throw new InvalidOperationException("A selected term is required.");
            if (_tail is { } tail && tail.State.Metadata.Term != term)
            {
                if (tail.State.Metadata.Term > term || tail.State.Metadata.Next != null)
                    return Conflict(); // Rediscover/follow the selected chain; never overwrite it.
                var source = Source(tail.FileId);
                _plan = new(null, [new(tail.FileId, tail.Key, tail.State.Token, tail.State.Length,
                    tail.State.Length, new(term), source)]);
            }
            else
            {
                var end = requestedEnd ?? capture.Completed;
                if (end.FileId == 0 || end == PublishedPosition) return TrlPublishResult.Idle;
                _plan = BuildPlan(end, remoteCancellation);
            }
        }
        while (_plan != null)
        {
            var plan = _plan;
            var write = plan.Writes[plan.Index];
            if (plan.Dispatched)
            {
                var resolution = await ReconcileAsync(write, remoteCancellation).ConfigureAwait(false);
                if (resolution.Result == TrlPublishResult.Conflict) return Conflict();
                if (resolution.State != null)
                {
                    if (Accept(plan, resolution.State)) return TrlPublishResult.Published;
                    continue;
                }
                if (!authority.IsValid) return TrlPublishResult.AuthorityLost;
                if (!retryPending) return TrlPublishResult.Pending;
            }
            if (!authority.IsValid) return TrlPublishResult.AuthorityLost;
            remoteCancellation.ThrowIfCancellationRequested();
            var hadUnresolvedRequest = plan.Dispatched;
            plan.Dispatched = true; // Set BEFORE the await: even cancellation/IOException may follow a landed effect.
            var result = await storage.WriteAsync(write, remoteCancellation).ConfigureAwait(false);
            if (result.Outcome == TrlWriteOutcome.Applied)
            {
                if (result.State == null || result.State.Token == write.ExpectedToken ||
                    result.State.Length != write.Length || result.State.Metadata != write.Metadata)
                    throw new InvalidDataException("Invalid TRL write receipt; reconcile before continuing.");
                if (Accept(plan, result.State)) return TrlPublishResult.Published;
                continue;
            }
            var observed = await ReconcileAsync(write, remoteCancellation).ConfigureAwait(false);
            if (observed.State != null)
            {
                if (Accept(plan, observed.State)) return TrlPublishResult.Published;
                continue;
            }
            if (observed.Result == TrlPublishResult.Conflict ||
                (result.Outcome == TrlWriteOutcome.Rejected && !hadUnresolvedRequest)) return Conflict();
            // An old read is not proof that an earlier request will never land, including after a retry rejection.
            return authority.IsValid ? TrlPublishResult.Pending : TrlPublishResult.AuthorityLost;
        }
        return TrlPublishResult.Adopted; // A completed adoption does not advance the local acknowledgement.
    }

    TrlPublishResult Conflict()
    {
        _conflict = true;
        return TrlPublishResult.Conflict;
    }

    bool Accept(Plan plan, TrlObjectState state)
    {
        // Successors are confirmed backwards; only the final tail receipt survives the plan.
        if (plan.Index == plan.Writes.Length - 1) plan.TailState = state;
        plan.Dispatched = false;
        if (--plan.Index >= 0) return false;
        var tailWrite = plan.Writes[^1];
        _tail = new(tailWrite.FileId, tailWrite.Key, plan.TailState!);
        _plan = null;
        if (plan.Position is not { } position) return false;
        capture.Acknowledge(position);
        return true;
    }

    async ValueTask<(TrlPublishResult Result, TrlObjectState? State)> ReconcileAsync(TrlWrite write, CancellationToken cancellation)
    {
        var observed = await storage.ReadAsync(write.Key, cancellation).ConfigureAwait(false);
        if (observed == null || observed.Token == write.ExpectedToken) return (TrlPublishResult.Pending, null);
        if (observed.Length != write.Length || observed.Metadata != write.Metadata)
            return (TrlPublishResult.Conflict, null);
        // Ambiguity is exceptional: compare the exact intended native prefix in bounded chunks, with no wire hashes.
        var localBuffer = _localBuffer ??= new byte[64 * 1024];
        var remoteBuffer = _remoteBuffer ??= new byte[64 * 1024];
        for (uint offset = 0; offset < write.Length;)
        {
            var size = (int)Math.Min((uint)localBuffer.Length, write.Length - offset);
            write.Source.RandomRead(localBuffer.AsSpan(0, size), offset, false);
            await storage.ReadRangeAsync(write.Key, observed.Token, offset, remoteBuffer.AsMemory(0, size), cancellation)
                .ConfigureAwait(false);
            if (!localBuffer.AsSpan(0, size).SequenceEqual(remoteBuffer.AsSpan(0, size)))
                return (TrlPublishResult.Conflict, null);
            offset += (uint)size;
        }
        return (TrlPublishResult.Published, observed);
    }

    Plan BuildPlan(TransactionLogPosition end, CancellationToken cancellation)
    {
        var parts = new List<(uint Id, uint End)>();
        var id = end.FileId;
        var length = end.Offset;
        while (true)
        {
            cancellation.ThrowIfCancellationRequested();
            var source = Source(id);
            if (length > source.GetSize()) throw new InvalidDataException("Incomplete local TRL prefix.");
            parts.Add((id, length));
            if (_tail?.FileId == id) break;
            var info = database.FileCollection.FileInfoByIdx(id) as IFileTransactionLog
                ?? throw new InvalidDataException("Missing TRL continuation header.");
            var previous = info.PreviousFileId;
            if (previous == 0)
            {
                if (_tail != null) throw new InvalidDataException("Local TRL does not continue selected history.");
                break;
            }
            if (previous >= id) throw new InvalidDataException("Invalid TRL predecessor.");
            id = previous;
            length = checked((uint)Source(id).GetSize());
        }
        parts.Reverse();
        var writes = new TrlWrite[parts.Count];
        var keys = parts.Select(f => keyForFile(f.Id)).ToArray();
        if (_tail != null) keys[0] = _tail.Key;
        if (keys.Distinct(StringComparer.Ordinal).Count() != keys.Length) throw new InvalidDataException("TRL keys collide.");
        for (var i = 0; i < writes.Length; i++)
        {
            var (partId, partEnd) = parts[i];
            var expected = i == 0 ? _tail?.State : null;
            if (partEnd < (expected?.Length ?? 0)) throw new InvalidDataException("Cannot shrink canonical TRL.");
            var next = i + 1 == writes.Length ? null : new TrlSuccessor(keys[i + 1], parts[i + 1].Id);
            var metadata = new TrlMetadata(term, next);
            _ = metadata.Encode();
            writes[i] = new(partId, keys[i], expected?.Token, expected?.Length ?? 0, partEnd, metadata,
                Source(partId));
        }
        return new(end, writes);
    }
}
