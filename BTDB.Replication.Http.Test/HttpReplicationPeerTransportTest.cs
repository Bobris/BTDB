using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.Test;
using BTDB.StreamLayer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace BTDB.Replication.Http.Test;

public class HttpReplicationPeerTransportTest
{
    sealed class Server : IAsyncDisposable
    {
        public readonly HttpReplicationPeerTransport Transport = new(1);
        public readonly HttpReplicationPeerTransport Client = new();
        public readonly Backend Backend = new();
        public ReplicationPeerIdentity Identity = null!;
        public IDisposable Registration = null!;
        public Func<HttpContext, Task>? Override;
        WebApplication _app = null!;

        public static async Task<Server> Start()
        {
            var server = new Server();
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseKestrel().UseUrls("http://127.0.0.1:0");
            server._app = builder.Build();
            server._app.Use((context, next) => server.Override is { } handler ? handler(context) : next(context));
            server.Transport.Map(server._app);
            await server._app.StartAsync();
            var endpoint = server._app.Services.GetRequiredService<IServer>().Features
                .Get<IServerAddressesFeature>()!.Addresses.Single();
            server.Identity = new("cluster", 7, "session", endpoint, "secret");
            server.Registration = server.Transport.Listen(endpoint, identity =>
            {
                if (identity != server.Identity) throw new IOException("Rejected.");
                return new Connection(server.Backend);
            });
            return server;
        }

        public async ValueTask DisposeAsync()
        {
            Backend.Release.TrySetResult();
            Registration.Dispose();
            Client.Dispose();
            Transport.Dispose();
            await _app.DisposeAsync();
        }
    }

    sealed class Backend
    {
        public readonly byte[] Bytes = Enumerable.Range(0, HttpReplicationPeerTransport.MaximumRange + 19).Select(i => (byte)i).ToArray();
        public readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool Block, IgnoreCancellation;
        public PreparedHandoff? Offer;
        public int Reads;
        public ILeaderTrlReader? NativeReader;
    }

    sealed class Connection(Backend backend) : IReplicationPeerSession, ILeaderTrlReader
    {
        public void Dispose() { }
        public async ValueTask<ReplicationPeerPoll> PollAsync(IReadOnlyList<ReplicationPeerPollRequest> databases, long challenge,
            TimeSpan duration, int inlineBudget, CancellationToken cancellation)
        {
            foreach (var database in databases)
                if (database.Database != "main") throw new IOException("Unknown database.");
            if (backend.Block)
            {
                backend.Entered.TrySetResult();
                try { await backend.Release.Task.WaitAsync(backend.IgnoreCancellation ? CancellationToken.None : cancellation); }
                catch (OperationCanceledException) { backend.Cancelled.TrySetResult(); throw; }
            }
            // Inline bytes of file 3 from the requested position, through the advertised progress (456).
            return new(challenge, true, databases.Select(d => new ReplicationPeerDatabaseProgress(d.Database, new(123, 3, 456), new(3, 400),
                d.From.FileId == 3 && d.From.Offset < 456 && inlineBudget > 0
                    ? [new(3, d.From.Offset, backend.Bytes.AsMemory((int)d.From.Offset, Math.Min(inlineBudget, 456 - (int)d.From.Offset)))]
                    : null)).ToArray());
        }
        public ILeaderTrlReader Reader(string database)
        {
            if (database != "main") throw new IOException("Unknown database.");
            return backend.NativeReader ?? this;
        }
        public ValueTask OfferHandoffAsync(PreparedHandoff offer, CancellationToken cancellation)
        { backend.Offer = offer; return ValueTask.CompletedTask; }
        public ValueTask<int> ReadAsync(uint fileId, ulong offset, Memory<byte> destination, CancellationToken cancellation)
        {
            backend.Reads++;
            if (fileId != 3) throw new FileNotFoundException();
            if (offset > (ulong)backend.Bytes.Length) throw new IOException();
            var count = Math.Min(destination.Length, backend.Bytes.Length - (int)offset);
            backend.Bytes.AsMemory((int)offset, count).CopyTo(destination);
            return ValueTask.FromResult(count);
        }
    }

    [Fact]
    public async Task PollRangeAndPreparedHandoffCrossRealHttp()
    {
        await using var server = await Server.Start();
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        var poll = await session.PollAsync([new("main")], 11, TimeSpan.FromSeconds(1), 0, default);
        Assert.Equal(11, poll.Challenge);
        Assert.True(poll.Granted);
        Assert.Equal(new ReplicationPeerDatabaseProgress("main", new(123, 3, 456), new(3, 400)), Assert.Single(poll.Databases));
        var inline = Assert.Single((await session.PollAsync([new("main", new(3, 100))], 13, TimeSpan.FromSeconds(1), 1000, default))
            .Databases);
        var chunk = Assert.Single(inline.Chunks!);
        Assert.Equal((3u, 100u), (chunk.FileId, chunk.Offset));
        Assert.Equal(server.Backend.Bytes.AsSpan(100, 356).ToArray(), chunk.Bytes.ToArray());
        Assert.Null(Assert.Single((await session.PollAsync([new("main", new(3, 100))], 14, TimeSpan.FromSeconds(1), 0, default))
            .Databases).Chunks); // A zero budget asks for progress only.
        Assert.Empty((await session.PollAsync([], 12, TimeSpan.FromSeconds(1), 0, default)).Databases);
        var bytes = new byte[server.Backend.Bytes.Length];
        var reader = session.Reader("main");
        var first = await reader.ReadAsync(3, 0, bytes, default);
        Assert.Equal(HttpReplicationPeerTransport.MaximumRange, first);
        Assert.Equal(19, await reader.ReadAsync(3, (ulong)first, bytes.AsMemory(first), default));
        Assert.Equal(server.Backend.Bytes, bytes);
        Assert.Equal(0, await reader.ReadAsync(3, (ulong)bytes.Length, bytes, default));
        var offer = new PreparedHandoff(8, Guid.NewGuid().ToString());
        await session.OfferHandoffAsync(offer, default);
        Assert.Equal(offer, server.Backend.Offer);
    }

    [Fact]
    public async Task EveryRequestReauthenticatesTheExactLeaderIdentity()
    {
        await using var server = await Server.Start();
        foreach (var invalid in new[]
                 {
                     server.Identity with { ClusterId = "other" }, server.Identity with { Term = 8 },
                     server.Identity with { SessionId = "other" }, server.Identity with { ApiKey = "wrong" }
                 })
        {
            var error = await Assert.ThrowsAsync<IOException>(() => server.Client.ConnectAsync(invalid, default).AsTask());
            Assert.DoesNotContain(invalid.ApiKey, error.Message);
        }
        using var old = await server.Client.ConnectAsync(server.Identity, default);
        server.Identity = server.Identity with { Term = 8, SessionId = "replacement", ApiKey = "rotated" };
        await Assert.ThrowsAsync<IOException>(() => old.PollAsync([new("main")], 1, TimeSpan.FromSeconds(1), 0, default).AsTask());
        await Assert.ThrowsAsync<IOException>(() => old.Reader("main").ReadAsync(3, 0, new byte[1], default).AsTask());
        using var current = await server.Client.ConnectAsync(server.Identity, default);
        Assert.True((await current.PollAsync([new("main")], 1, TimeSpan.FromSeconds(1), 0, default)).Granted);
    }

    [Fact]
    public async Task MissingRetainedBytesRemainDistinguishableFromTransportFailure()
    {
        await using var server = await Server.Start();
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        await Assert.ThrowsAsync<FileNotFoundException>(() => session.Reader("main").ReadAsync(1, 0, new byte[5], default).AsTask());
        await Assert.ThrowsAsync<IOException>(() => session.Reader("unknown").ReadAsync(3, 0, new byte[5], default).AsTask());
        server.Registration.Dispose();
        await Assert.ThrowsAsync<IOException>(() => session.PollAsync([], 1, TimeSpan.FromSeconds(1), 0, default).AsTask());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationAndSessionCloseAbortPendingHttp(bool close)
    {
        await using var server = await Server.Start();
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        using var cancellation = new CancellationTokenSource();
        server.Backend.Block = true;
        var poll = session.PollAsync([new("main")], 1, TimeSpan.FromSeconds(1), 0, cancellation.Token).AsTask();
        await server.Backend.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        if (close) session.Dispose(); else cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => poll.WaitAsync(TimeSpan.FromSeconds(10)));
        await server.Backend.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task ReplacedLeaderCannotReturnAnInflightResponse()
    {
        await using var server = await Server.Start();
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        server.Backend.Block = server.Backend.IgnoreCancellation = true;
        var poll = session.PollAsync([new("main")], 1, TimeSpan.FromSeconds(1), 0, default).AsTask();
        await server.Backend.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        server.Identity = server.Identity with { Term = 8 };
        server.Backend.Release.SetResult();
        await Assert.ThrowsAsync<IOException>(() => poll.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task BusyServerRejectsInsteadOfQueuingAnotherRequest()
    {
        await using var server = await Server.Start();
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        server.Backend.Block = true;
        var poll = session.PollAsync([new("main")], 1, TimeSpan.FromSeconds(1), 0, default).AsTask();
        await server.Backend.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var error = await Assert.ThrowsAsync<IOException>(() => session.PollAsync([], 2, TimeSpan.FromSeconds(1), 0, default).AsTask());
        Assert.Contains("429", error.Message);
        server.Backend.Release.SetResult();
        await poll;
    }

    [Fact]
    public async Task InvalidAndOversizedRequestsCannotReachTheRangeReader()
    {
        await using var server = await Server.Start();
        using var client = new HttpClient();
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", server.Identity.ApiKey);
        var request = new PeerRequest("cluster", 7, "session", server.Identity.Endpoint,
            PeerOperation.Read, "main", FileId: 3, Count: HttpReplicationPeerTransport.MaximumRange + 1);
        using var response = await client.PostAsync(server.Identity.Endpoint + HttpReplicationPeerTransport.Path,
            new ByteArrayContent(ReplicationPeerWire.EncodeRequest(request)));
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var encoded = ReplicationPeerWire.EncodeRequest(request with { Count = 5 });
        foreach (var malformed in new[] { encoded[..^1], [.. encoded, 0], [2, .. encoded[1..]] })
        {
            using var rejected = await client.PostAsync(server.Identity.Endpoint + HttpReplicationPeerTransport.Path,
                new ByteArrayContent(malformed)); // Truncated, trailing bytes, unknown protocol version.
            Assert.Equal(HttpStatusCode.BadRequest, rejected.StatusCode);
        }
        Assert.Equal(0, server.Backend.Reads);
        using var content = new StringContent(new string('x', HttpReplicationPeerTransport.MaximumControlBytes + 1));
        using var oversized = await client.PostAsync(server.Identity.Endpoint + HttpReplicationPeerTransport.Path, content);
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, oversized.StatusCode);
    }

    [Fact]
    public async Task RedirectsDoNotReplayAuthenticatedRequests()
    {
        await using var server = await Server.Start();
        var requests = 0;
        server.Override = context =>
        {
            Interlocked.Increment(ref requests);
            context.Response.StatusCode = 307;
            context.Response.Headers.Location = server.Identity.Endpoint + "/redirect";
            return Task.CompletedTask;
        };
        await Assert.ThrowsAsync<IOException>(() => server.Client.ConnectAsync(server.Identity, default).AsTask());
        Assert.Equal(1, requests);
    }

    [Theory]
    [InlineData("oversized")]
    [InlineData("malformed")]
    [InlineData("stale")]
    [InlineData("incomplete")]
    [InlineData("unrequested")]
    public async Task InvalidProgressResponsesAreTransportFailures(string kind)
    {
        await using var server = await Server.Start();
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        var body = kind switch
        {
            "oversized" => new byte[HttpReplicationPeerTransport.MaximumControlBytes + 1],
            "stale" => ReplicationPeerWire.EncodePoll(new(99, true, [new("main", null)])),
            "incomplete" => ReplicationPeerWire.EncodePoll(new(1, true, [])),
            "unrequested" => ReplicationPeerWire.EncodePoll(new(1, true, [new("main", new(1, 3, 456), null, [new(3, 0, new byte[10])])])),
            _ => "not binary"u8.ToArray()
        };
        server.Override = context => context.Response.Body.WriteAsync(body).AsTask();
        await Assert.ThrowsAsync<IOException>(() => session.PollAsync([new("main")], 1, TimeSpan.FromSeconds(1), 0, default).AsTask());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InvalidRangeResponsesDoNotAdvanceComparison(bool truncated)
    {
        await using var server = await Server.Start();
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        server.Override = async context =>
        {
            context.Response.ContentLength = truncated ? 5 : 100;
            await context.Response.StartAsync();
            if (truncated) context.Abort();
            else await context.Response.Body.WriteAsync(new byte[100]);
        };
        await Assert.ThrowsAnyAsync<IOException>(() => session.Reader("main").ReadAsync(3, 0, new byte[5], default).AsTask());
    }

    sealed class FixedClock : IReplicationScheduler
    {
        public TimeSpan Elapsed => TimeSpan.Zero;
        public IDisposable Schedule(TimeSpan delay, Action callback, string description) => throw new NotSupportedException();
    }

    sealed class TinyLogs : ITransactionLogSizeStrategy
    {
        public TransactionLogSizeLimits GetLimits(uint fileId) => new(1024, 1536);
    }

    static void Seed(InMemoryReplicationFileStorage files)
    {
        var writer = new MemWriter(files.AddFile("trl", FileIdParity.Odd).GetAppenderWriter());
        writer.WriteBlock("BTDB3"u8);
        writer.WriteGuid(new Guid("5d076258-e492-4931-a5b8-a19dc9fe6c76"));
        writer.WriteUInt8((byte)KVFileType.TransactionLog);
        writer.WriteVInt64(1);
        writer.WriteVInt32(0);
        writer.Flush();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NativeHistoryAcrossRotatedFilesMatchesOrDivergesOverHttp(bool diverge)
    {
        await using var server = await Server.Start();
        using var leaderFiles = new InMemoryReplicationFileStorage();
        using var followerFiles = new InMemoryReplicationFileStorage();
        Seed(leaderFiles);
        Seed(followerFiles);
        var leaderCapture = new TransactionLogCapture();
        var followerCapture = new TransactionLogCapture();
        using var leader = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new LocalReplicatedCollection(leaderFiles), TransactionLogCapture = leaderCapture,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null, TransactionLogSizeStrategy = new TinyLogs()
        });
        using var follower = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new LocalReplicatedCollection(followerFiles), TransactionLogCapture = followerCapture,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null, TransactionLogSizeStrategy = new TinyLogs()
        });
        for (ulong id = 1; id <= 8; id++)
        {
            await Write(leader, id, (byte)id, false);
            await Write(follower, id, diverge && id == 6 ? (byte)99 : (byte)id, true);
        }
        var authority = new LeaseAuthority(new FixedClock(), 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromMinutes(1));
        server.Backend.NativeReader = new LeaderTrlReader(leader, leaderCapture, authority);
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        Assert.True(leaderFiles.GetCount() > 1);
        var acknowledged = followerCapture.Acknowledged;
        var comparer = new TrlPrefixComparer(followerFiles, followerCapture);
        Assert.Equal(diverge ? TrlCompareResult.Diverged : TrlCompareResult.Matched,
            await comparer.CompareAsync(session.Reader("main"), leaderCapture.Completed));
        Assert.Equal(diverge ? acknowledged : leaderCapture.Completed, followerCapture.Acknowledged);
        authority.Fence();
        await Assert.ThrowsAsync<IOException>(() => session.Reader("main").ReadAsync(1, 0, new byte[1], default).AsTask());

        static async Task Write(BTreeKeyValueDB db, ulong id, byte value, bool batch)
        {
            using var transaction = await db.StartWritingTransaction(id, batch);
            using var cursor = transaction.CreateCursor();
            cursor.CreateOrUpdateKeyValue([(byte)id], Enumerable.Repeat(value, 700).ToArray());
            if (id != 3) transaction.Commit(); // Preserve the same rollback evidence across different batching.
        }
    }

    [Theory]
    [InlineData("http://example.com")]
    [InlineData("https://user:secret@example.com")]
    [InlineData("https://example.com/path")]
    public async Task CredentialsRequireHttpsOutsideLoopback(string endpoint)
    {
        using var client = new HttpReplicationPeerTransport();
        await Assert.ThrowsAsync<ArgumentException>(() => client.ConnectAsync(new("c", 1, "s", endpoint, "secret"), default).AsTask());
    }
}
