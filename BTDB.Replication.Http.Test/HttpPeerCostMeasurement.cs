using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.Replication.Http.Test;

// Opt-in (BTDB_REPLICATION_MEASURE=1): what one follower step costs over real HTTP on loopback Kestrel, for an
// empty poll and for polls carrying the full 4 MiB inline budget. Results are recorded in Measurements.md.
public partial class HttpReplicationPeerTransportTest
{
    [Fact]
    public async Task MeasurePollLatencyAndInlineThroughput()
    {
        if (Environment.GetEnvironmentVariable("BTDB_REPLICATION_MEASURE") == null) return;
        await using var server = await Server.Start();
        server.Backend.Trl = new byte[64 * 1024 * 1024];
        new Random(1).NextBytes(server.Backend.Trl);
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        for (var i = 0; i < 200; i++) await session.PollAsync([new("main")], i, TimeSpan.FromSeconds(1), 0, default);
        const int polls = 2000;
        var clock = Stopwatch.StartNew();
        for (var i = 0; i < polls; i++) await session.PollAsync([new("main")], i, TimeSpan.FromSeconds(1), 0, default);
        var empty = clock.Elapsed / polls;
        var budget = ReplicationPeerPoll.MaximumInlineBytes;
        long bytes = 0;
        clock.Restart();
        for (var round = 0; round < 8; round++)
            for (uint from = 0; from < server.Backend.Trl.Length; from += (uint)budget)
            {
                var poll = await session.PollAsync([new("main", new(3, from))], round, TimeSpan.FromSeconds(1), budget, default);
                foreach (var chunk in poll.Databases[0].Chunks!) bytes += chunk.Bytes.Length;
            }
        var full = clock.Elapsed;
        output.WriteLine($"empty poll {empty.TotalMicroseconds:0} µs; 4 MiB inline polls {bytes / full.TotalSeconds / 1048576:0} MiB/s " +
                         $"({full.TotalMilliseconds / (bytes / (double)budget):0.0} ms per poll)");
    }
}
