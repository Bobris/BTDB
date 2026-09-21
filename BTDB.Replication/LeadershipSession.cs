using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication;

/// <summary>One newly acquired lease's selection/activation lane. Keep this object across transient retries;
/// create a new one for a fresh lease. Lease maintenance must run independently. No publisher is exposed until
/// every supplied required database has been validated and adopted.</summary>
internal sealed class LeadershipSession(LeaderSelection selection, IReadOnlyList<ActivationDatabase> databases) : IDisposable
{
    readonly SemaphoreSlim _lane = new(1);
    SelectedLeadership? _selected;
    IReadOnlyList<CanonicalTrlPublisher>? _publishers;

    internal SelectedLeadership? Selected => _selected;

    // The owner stops the transition lane before disposal, including cancellation after activation.
    public void Dispose()
    {
        if (_publishers == null) return;
        foreach (var publisher in _publishers) publisher.Dispose();
        _publishers = null;
    }

    public async ValueTask<IReadOnlyList<CanonicalTrlPublisher>?> ActivateAsync(CancellationToken cancellation = default)
    {
        await _lane.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            _selected ??= await selection.SelectAsync(cancellation).ConfigureAwait(false);
            if (_selected == null) return null;
            if (!_selected.Authority.IsValid) throw new InvalidOperationException("Leadership session expired.");
            _publishers ??= await LeadershipActivation.ActivateAsync(_selected, databases, cancellation).ConfigureAwait(false);
            return _publishers;
        }
        finally { _lane.Release(); }
    }
}
