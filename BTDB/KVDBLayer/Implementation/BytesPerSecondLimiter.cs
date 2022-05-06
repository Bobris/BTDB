using System;
using System.Diagnostics;
using System.Threading;

namespace BTDB.KVDBLayer.Implementation;

public struct BytesPerSecondLimiter
{
    readonly ulong _bytesPerSecond;
    readonly Stopwatch _stopwatch;
    ulong _newCorrectionAfter;
    readonly ulong _delta;

    // 0 means no limit
    public BytesPerSecondLimiter(ulong bytesPerSecond)
    {
        _stopwatch = Stopwatch.StartNew();
        _bytesPerSecond = bytesPerSecond;
        _newCorrectionAfter = ulong.MaxValue;
        _delta = 0;
        if (bytesPerSecond > 0)
        {
            _delta = bytesPerSecond >> 4; // correction should be done approximately 16 times per second
            if (_delta == 0) _delta = 1;
            _newCorrectionAfter = _delta;
        }
    }

    public void Limit(ulong bytesTillNow)
    {
        if (bytesTillNow < _newCorrectionAfter)
            return;
        var msTillNow = _stopwatch.ElapsedMilliseconds;
        var shouldTakeMs = (long)(bytesTillNow * 1000.0 / _bytesPerSecond);
        if (shouldTakeMs > msTillNow)
        {
            // wait max 2 minutes to not slow it down too much (Limit function was not called often enough)
            Thread.Sleep((int)Math.Min(shouldTakeMs - msTillNow, 120000));
        }

        _newCorrectionAfter = bytesTillNow + _delta;
    }

    public ulong TotalTimeInMs => (ulong)_stopwatch.ElapsedMilliseconds;
}
