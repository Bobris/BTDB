using System;
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
        public async ValueTask<ReplicationPeerProgress> PollAsync(string? database, long challenge, TimeSpan duration, CancellationToken cancellation)
        {
            if (database != null && database != "main") throw new IOException("Unknown database.");
            if (backend.Block)
            {
                backend.Entered.TrySetResult();
                try { await backend.Release.Task.WaitAsync(backend.IgnoreCancellation ? CancellationToken.None : cancellation); }
                catch (OperationCanceledException) { backend.Cancelled.TrySetResult(); throw; }
            }
            return new(challenge, true, database == null ? null : new(123, 3, 456));
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
        Assert.Equal(new ReplicationPeerProgress(11, true, new(123, 3, 456)),
            await session.PollAsync("main", 11, TimeSpan.FromSeconds(1), default));
        Assert.Null((await session.PollAsync(null, 12, TimeSpan.FromSeconds(1), default)).Progress);
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
        await Assert.ThrowsAsync<IOException>(() => old.PollAsync("main", 1, TimeSpan.FromSeconds(1), default).AsTask());
        await Assert.ThrowsAsync<IOException>(() => old.Reader("main").ReadAsync(3, 0, new byte[1], default).AsTask());
        using var current = await server.Client.ConnectAsync(server.Identity, default);
        Assert.True((await current.PollAsync("main", 1, TimeSpan.FromSeconds(1), default)).Granted);
    }

    [Fact]
    public async Task MissingRetainedBytesRemainDistinguishableFromTransportFailure()
    {
        await using var server = await Server.Start();
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        await Assert.ThrowsAsync<FileNotFoundException>(() => session.Reader("main").ReadAsync(1, 0, new byte[5], default).AsTask());
        await Assert.ThrowsAsync<IOException>(() => session.Reader("unknown").ReadAsync(3, 0, new byte[5], default).AsTask());
        server.Registration.Dispose();
        await Assert.ThrowsAsync<IOException>(() => session.PollAsync(null, 1, TimeSpan.FromSeconds(1), default).AsTask());
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
        var poll = session.PollAsync("main", 1, TimeSpan.FromSeconds(1), cancellation.Token).AsTask();
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
        var poll = session.PollAsync("main", 1, TimeSpan.FromSeconds(1), default).AsTask();
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
        var poll = session.PollAsync("main", 1, TimeSpan.FromSeconds(1), default).AsTask();
        await server.Backend.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var error = await Assert.ThrowsAsync<IOException>(() => session.PollAsync(null, 2, TimeSpan.FromSeconds(1), default).AsTask());
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
        var request = new HttpReplicationPeerTransport.Request("cluster", 7, "session", server.Identity.Endpoint,
            "read", "main", FileId: 3, Count: HttpReplicationPeerTransport.MaximumRange + 1);
        using var response = await client.PostAsJsonAsync(server.Identity.Endpoint + HttpReplicationPeerTransport.Path,
            request, new System.Text.Json.JsonSerializerOptions());
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
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
    public async Task InvalidProgressResponsesAreTransportFailures(string kind)
    {
        await using var server = await Server.Start();
        using var session = await server.Client.ConnectAsync(server.Identity, default);
        server.Override = context => context.Response.WriteAsync(kind switch
        {
            "oversized" => new string(' ', HttpReplicationPeerTransport.MaximumControlBytes + 1),
            "stale" => "{\"Challenge\":99,\"Granted\":true}",
            _ => "not json"
        });
        await Assert.ThrowsAsync<IOException>(() => session.PollAsync("main", 1, TimeSpan.FromSeconds(1), default).AsTask());
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
