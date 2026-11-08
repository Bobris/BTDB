using System;
using System.Net.Http;

namespace BTDB.Replication.Http;

static class PeerHttpHandler
{
    /// <summary>A peer that went away can take its address with it, and connection attempts to that address are then
    /// dropped silently. Without this limit one attempt hangs for the operating system's timeout, about two minutes on
    /// Linux, and the caller's deadline does not help an event log request, which has none.</summary>
    internal static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(3);

    /// <summary>Never forwards a credential-bearing POST to a redirect target.</summary>
    internal static SocketsHttpHandler Create() => new()
    {
        AllowAutoRedirect = false, UseCookies = false, ConnectTimeout = ConnectTimeout
    };
}
