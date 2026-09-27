using System;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;

namespace BTDB.Replication;

public readonly record struct LeaderTrlProgress(ulong EventId, uint TrlFileId, uint TrlPosition)
{
    public TransactionLogPosition Position => new(TrlFileId, TrlPosition);
}

/// <summary>
/// One authenticated leader/database connection. The owner validates term and database before delivering messages,
/// calls CompareLatestAsync after progress or local completion, and closes this object before replacing the session.
/// There is no application execution, Blob access or background retry loop here. A byte match is historical equality,
/// not a live confirmation grant; leader liveness is tracked by the owner's poll grants.
/// LocalProgress (the host's completed local cut) names a lagging partial match that stops at the local end.
/// </summary>
internal sealed class FollowerComparisonSession(
    Func<uint, IFileCollectionFile?> getFile, TransactionLogCapture capture, ILeaderTrlReader leader,
    Action requestRestart, TransactionLogPosition compareFrom, Func<LeaderTrlProgress?>? localProgress = null)
{
    readonly object _lock = new();
    // Serializes comparisons; the comparer itself is not thread-safe.
    readonly SemaphoreSlim _lane = new(1);
    readonly CancellationTokenSource _closedCancellation = new();
    readonly TrlPrefixComparer _comparer = new(getFile, capture, compareFrom);
    LeaderTrlProgress? _latest, _compared;
    TransactionLogPosition? _comparedPosition;
    bool _closed;

    /// <summary>The latest matched cut with a known event ID: the leader's progress, or a lagging local cut.</summary>
    public LeaderTrlProgress? Compared { get { lock (_lock) return _compared; } }

    /// <summary>The latest matched complete transaction cut, including a lagging local prefix.</summary>
    public TransactionLogPosition? ComparedPosition { get { lock (_lock) return _comparedPosition; } }

    // Owner reads this after Close to resume comparison with the same leader session; the lane has stopped by then.
    internal TransactionLogPosition ResumePosition => _comparer.Position;

    public void NotifyProgress(LeaderTrlProgress progress)
    {
        if (progress.TrlFileId == 0 || progress.TrlPosition == 0)
            throw new ArgumentOutOfRangeException(nameof(progress));
        lock (_lock)
        {
            if (_closed) return;
            if (_latest is { } latest)
            {
                var order = progress.Position.CompareTo(latest.Position);
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
            // Host callback outside the lock: it only names a partial match that ends exactly at the local cut.
            var local = result == TrlCompareResult.LocalBehind ? localProgress?.Invoke() : null;
            lock (_lock)
            {
                if (_closed) throw new OperationCanceledException("Leader session is closed.", linked.Token);
                linked.Token.ThrowIfCancellationRequested();
                if (result == TrlCompareResult.Matched)
                {
                    _compared = progress;
                    if (_comparedPosition is not { } previous || progress.Position > previous) _comparedPosition = progress.Position;
                }
                else if (result == TrlCompareResult.LocalBehind && _comparer.MatchedThrough is { } matched &&
                         (_comparedPosition is not { } previous || matched > previous))
                {
                    _comparedPosition = matched;
                    if (local is { } cut && cut.Position == matched) _compared = cut;
                }
                if (result == TrlCompareResult.Diverged)
                {
                    _closed = true;
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
        }
        _closedCancellation.Cancel();
    }
}
