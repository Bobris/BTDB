using System;
using System.Collections.Generic;
using System.IO;
using BTDB.KVDBLayer;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>One newly acquired lease's selection/activation lane. Keep this object across transient retries;
/// create a new one for a fresh lease. Lease maintenance must run independently. No publisher is exposed until
/// every supplied required database has been validated and adopted.</summary>
internal sealed class LeadershipSession(LeaderSelection selection, IReadOnlyList<ActivationDatabase> databases,
    Func<ActivationDatabase, LeaseAuthority, CancellationToken, ValueTask>? prepare = null, Action? progress = null) : IDisposable
{
    // Local progress only: selection (1), discovery/validation/adoption (2), schema preparation/publication (3).
    // Lexicographic order keeps retrying an earlier database or range from extending the deadline.
    (int Phase, int Database, int Step, uint File, ulong Offset) _progress;
    readonly SemaphoreSlim _lane = new(1);
    SelectedLeadership? _selected;
    IReadOnlyList<CanonicalTrlPublisher>? _publishers;
    readonly Dictionary<string, TransactionLogPosition> _prepared = new(StringComparer.Ordinal);
    // Canonical prefixes verified under this lease; a lagging candidate resumes here instead of revalidating.
    readonly Dictionary<string, TransactionLogPosition> _validated = new(StringComparer.Ordinal);

    internal SelectedLeadership? Selected => _selected;

    // The owner stops the transition lane before disposal, including cancellation after activation.
    public void Dispose()
    {
        if (_publishers == null) return;
        foreach (var publisher in _publishers) publisher.Dispose();
        _publishers = null;
    }

    void ReportProgress(int phase, int database, int step, uint file, ulong offset)
    {
        if (progress == null) return;
        var next = (phase, database, step, file, offset);
        // Repeated discovery/validation after an I/O failure is not forward activation progress.
        if (next.CompareTo(_progress) <= 0) return;
        _progress = next;
        progress?.Invoke();
    }

    /// <summary>Null means selection is unresolved or local execution has not reached canonical history yet; retry
    /// later under the same lease while the host keeps executing inputs.</summary>
    public async ValueTask<IReadOnlyList<CanonicalTrlPublisher>?> ActivateAsync(CancellationToken cancellation = default)
    {
        await _lane.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            _selected ??= await selection.SelectAsync(cancellation).ConfigureAwait(false);
            if (_selected == null) return null;
            ReportProgress(1, 0, 0, 0, 0);
            if (!_selected.Authority.IsValid) throw new InvalidOperationException("Leadership session expired.");
            _publishers ??= await LeadershipActivation.ActivateAsync(_selected, databases, cancellation,
                progress == null ? null : (database, step, file, offset) => ReportProgress(2, database, step, file, offset),
                _validated).ConfigureAwait(false);
            if (_publishers == null) return null;
            if (prepare != null)
                for (var i = 0; i < databases.Count; i++)
                {
                    var database = databases[i];
                    if (!_prepared.TryGetValue(database.Name, out var cut))
                    {
                        if (!_selected.Authority.IsValid) throw new InvalidOperationException("Preparation requires live authority.");
                        await prepare(database, _selected.Authority, cancellation).ConfigureAwait(false);
                        cut = database.Capture.Completed;
                        _prepared.Add(database.Name, cut);
                        ReportProgress(3, i, 0, 0, 0);
                    }
                    if (cut.FileId == 0) continue;
                    var result = await _publishers[i].PublishThroughAsync(cut, true, cancellation).ConfigureAwait(false);
                    if (result is not (TrlPublishResult.Idle or TrlPublishResult.Published or TrlPublishResult.Adopted))
                        throw new IOException("Initialization/schema publication is not yet confirmed.");
                    ReportProgress(3, i, 1, cut.FileId, cut.Offset);
                }
            return _publishers;
        }
        finally { _lane.Release(); }
    }
}
