using System;

namespace BTDB.Replication.EventLog;

/// <summary>
/// Chooses the owner's commit window from when records arrive relative to the moment a write would start without one:
/// the commit when records are already queued, otherwise the first record arriving after it, which would start the
/// write at once (that record itself gains nothing from waiting). Waiting L lets the records arriving within L after
/// that moment join the write, saving them about one write duration W, while every record of the cycle waits L longer.
/// Per record the gain is f(L) - L/W, where f(L) is the fraction of all arrivals that land within L after the moment. Publishers that answer a commit (publish-and-await loops, often on other nodes)
/// concentrate arrivals right after it and make the gain positive; independent arrivals are uniform over the cycle,
/// where f(L) = L/(W + L) &lt; L/W, so the window stays zero. Sparse arrivals make f(L) noisy, so the arrivals in a
/// window must also exceed their independent-traffic expectation by a clear Poisson margin. Statistics decay per
/// commit; a new window replaces the current one only when its gain is clearly higher. Not thread safe: the owner lane
/// calls it under its lock.
/// </summary>
internal sealed class CommitLingerEstimator
{
    internal const int Bins = 16;
    const double Decay = 1.0 / 32;
    const double MinimumGain = 0.05;
    const double SwitchMargin = 0.02;
    const double MinimumArrivals = 8; // the decayed count settles near 32 per arrival per commit
    static readonly TimeSpan MinimumWrite = TimeSpan.FromMilliseconds(1);

    readonly TimeSpan _max;
    readonly long _binTicks;
    readonly double[] _arrivals = new double[Bins];
    double _total;
    double _writeTicks;
    TimeSpan? _origin;
    bool _awaitingFirst;

    public CommitLingerEstimator(TimeSpan max)
    {
        _max = max;
        _binTicks = Math.Max(1, max.Ticks / Bins);
    }

    public TimeSpan Current { get; private set; }

    public void Arrival(TimeSpan now)
    {
        _total += 1;
        if (_awaitingFirst)
        {
            _awaitingFirst = false;
            _origin = now;
            return;
        }
        if (_origin is not { } origin) return;
        var since = (now - origin).Ticks;
        if (since >= 0 && since < _binTicks * Bins) _arrivals[since / _binTicks] += 1;
    }

    /// <param name="writeStarted">When the storage write of this commit began, after any window.</param>
    /// <param name="queued">Records are already waiting for the next write.</param>
    public void Committed(TimeSpan writeStarted, TimeSpan now, bool queued)
    {
        var write = Math.Max(0, (now - writeStarted).Ticks);
        _writeTicks = _writeTicks == 0 ? write : _writeTicks + (write - _writeTicks) / 16;
        for (var i = 0; i < Bins; i++) _arrivals[i] *= 1 - Decay;
        _total *= 1 - Decay;
        _origin = queued ? now : null;
        _awaitingFirst = !queued;
        Recompute();
    }

    void Recompute()
    {
        if (_total < MinimumArrivals)
        {
            Current = TimeSpan.Zero;
            return;
        }
        var write = Math.Max(_writeTicks, MinimumWrite.Ticks);
        var best = TimeSpan.Zero;
        var bestGain = MinimumGain;
        var currentGain = 0.0;
        double cumulative = 0;
        for (var i = 0; i < Bins; i++)
        {
            cumulative += _arrivals[i];
            var window = TimeSpan.FromTicks(_binTicks * (i + 1));
            if (window > _max) break;
            var share = Math.Min(1, window.Ticks / write);
            var gain = cumulative / _total - share;
            if (window == Current) currentGain = gain;
            // Arrivals in the window must exceed what independent traffic puts there by a clear Poisson margin.
            var expected = share * _total;
            var significant = cumulative >= expected + 3 * Math.Sqrt(Math.Max(expected, 1)) + 1;
            if (gain > bestGain && significant)
            {
                best = window;
                bestGain = gain;
            }
        }
        if (best == Current) return;
        // Hysteresis: keep a still profitable window unless the best one is clearly better.
        if (Current > TimeSpan.Zero && currentGain >= MinimumGain && bestGain < currentGain + SwitchMargin) return;
        Current = best;
    }
}
