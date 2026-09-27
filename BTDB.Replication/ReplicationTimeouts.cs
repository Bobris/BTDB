using System;
using System.Threading;

namespace BTDB.Replication;

internal static class ReplicationTimeouts
{
    /// <summary>Timeout callback body. Disposing scheduled work does not wait for a callback that already started,
    /// so the request may have finished and disposed its source; late cancellation is then a no-op.</summary>
    public static void Cancel(CancellationTokenSource request)
    {
        try { request.Cancel(); }
        catch (ObjectDisposedException) { }
    }
}
