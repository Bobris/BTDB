using System;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

internal readonly record struct LeaderTrlProgress(ulong EventId, uint TrlFileId, uint TrlPosition)
{
    public TransactionLogPosition Position => new(TrlFileId, TrlPosition);
}

/// <summary>
/// One authenticated leader/database connection. The owner validates term and database before delivering messages,
/// calls CompareLatestAsync after progress or local completion, and closes this object before replacing the session.
/// There is no application execution, Blob access or background retry loop here.
/// </summary>
internal sealed class FollowerComparisonSession(
    Func<uint, IFileCollectionFile?> getFile, TransactionLogCapture capture, ILeaderTrlReader leader,
    IReplicationScheduler clock, Action requestRestart, TransactionLogPosition? compareFrom = null)
{
    public FollowerComparisonSession(IFileCollection local, TransactionLogCapture capture, ILeaderTrlReader leader,
        IReplicationScheduler clock, Action requestRestart, TransactionLogPosition? compareFrom = null)
        : this(local.GetFile, capture, leader, clock, requestRestart, compareFrom) { }

    readonly object _lock = new();
    readonly SemaphoreSlim _lane = new(1);
    readonly CancellationTokenSource _closedCancellation = new();
    readonly TrlPrefixComparer _comparer = new(getFile, capture, compareFrom);
    readonly ConfirmationWindow _window = new(clock);
    LeaderTrlProgress? _latest, _compared;
    bool _closed;

    public LeaderTrlProgress? Compared { get { lock (_lock) return _compared; } }

    // This is metadata, not a retained read root. Expiry immediately removes live confirmation.
    public LeaderTrlProgress? Confirmed { get { lock (_lock) return _window.IsValid ? _compared : null; } }

    public long BeginChallenge(TimeSpan duration)
    {
        lock (_lock) return _window.BeginChallenge(duration);
    }

    public bool AcceptChallenge(long challenge)
    {
        lock (_lock) return _window.Accept(challenge);
    }

    public void NotifyProgress(LeaderTrlProgress progress)
    {
        if (progress.TrlFileId == 0 || progress.TrlPosition == 0)
            throw new ArgumentOutOfRangeException(nameof(progress));
        lock (_lock)
        {
            if (_closed) return;
            if (_latest is { } latest)
            {
                var order = Order(progress.Position).CompareTo(Order(latest.Position));
                if (order < 0) return; // Delayed notification on this same connection.
                if (order == 0 && progress.EventId != latest.EventId)
                    throw new ArgumentException("The same native cut cannot have different event IDs.", nameof(progress));
            }
            _latest = progress;
        }
    }

    public async ValueTask<TrlCompareResult?> CompareLatestAsync(CancellationToken cancellation = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, _closedCancellation.Token);
        await _lane.WaitAsync(linked.Token).ConfigureAwait(false);
        var restart = false;
        try
        {
            LeaderTrlProgress progress;
            lock (_lock)
            {
                if (_closed) throw new OperationCanceledException("Leader session is closed.", linked.Token);
                linked.Token.ThrowIfCancellationRequested();
                if (_latest is not { } latest) return null;
                progress = latest;
            }
            var result = await _comparer.CompareAsync(leader, progress.Position, linked.Token).ConfigureAwait(false);
            lock (_lock)
            {
                if (_closed) throw new OperationCanceledException("Leader session is closed.", linked.Token);
                linked.Token.ThrowIfCancellationRequested();
                if (result == TrlCompareResult.Matched) _compared = progress;
                if (result == TrlCompareResult.Diverged)
                {
                    _closed = true;
                    _window.Close();
                    restart = true;
                }
            }
            return result;
        }
        finally
        {
            _lane.Release();
            if (restart)
            {
                try { _closedCancellation.Cancel(); }
                finally { requestRestart(); }
            }
        }
    }

    public void Close()
    {
        lock (_lock)
        {
            if (_closed) return;
            _closed = true;
            _window.Close();
        }
        _closedCancellation.Cancel();
    }

    static ulong Order(TransactionLogPosition position) => ((ulong)position.FileId << 32) | position.Offset;
}
