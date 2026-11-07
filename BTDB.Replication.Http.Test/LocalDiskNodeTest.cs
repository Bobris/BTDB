using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.Replication.EventLog;
using BTDB.Replication.Test;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace BTDB.Replication.Http.Test;

/// <summary>A single development node with the shared storage in local directories instead of Azure Blob Storage: either
/// hosted exactly like production, where only the three storage registrations differ, or with the single-node
/// registrations in a generic host without any HTTP server.</summary>
[Collection(ReplicationHostCollection.Name)]
public class LocalDiskNodeTest : IDisposable
{
    const string Database = "main";
    const string Initial = """{"format":1,"clusterId":"cluster","term":0,"revision":0,"applicationGeneration":0,"databaseNames":[]} """;
    static readonly TrlSuccessor Genesis = new("1.trl", 1);
    readonly string _root = Path.Combine(Path.GetTempPath(), "btdb-local-node-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, true);
    }

    static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    /// <summary>The application: restores its database from the canonical storage and applies the "events" topic in
    /// order, event offset N as commit N + 1.</summary>
    sealed class Application(string endpoint, IBindableReplicationStorage canonical, string cacheDirectory)
        : IReplicationNodeHost, IAsyncDisposable
    {
        readonly OnDiskReplicationFileStorage _cache = new(cacheDirectory);
        readonly TransactionLogCapture _capture = new();
        readonly object _progressLock = new();
        LeaderTrlProgress? _progress;
        ReplicationFileSet? _files;
        public BTreeKeyValueDB? Db;
        public bool Restored;

        public async ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation)
        {
            Db?.Dispose();
            Db = null;
            if (_files != null)
            {
                await _files.DisposeAsync();
                _files = null;
            }
            IFileCollection files;
            Restored = await canonical.ResolveRecoveryRootAsync(Genesis, cancellation) != Genesis ||
                       await canonical.ReadAsync(Genesis.Key, cancellation) != null;
            if (Restored)
            {
                var inventory = await CanonicalTrlInventory.DiscoverAsync(canonical, Genesis, cancellation);
                _files = new(_cache, canonical.Bind(inventory));
                await _files.InitializeAsync(cancellation);
                files = _files;
            }
            else files = new LocalReplicatedCollection(_cache); // Nothing published yet: a new database.
            Db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
            {
                FileCollection = files, TransactionLogCapture = _capture, CompactorScheduler = null,
                Compression = new NoCompressionStrategy()
            }, cancellation);
            if (Restored)
            {
                var position = Db.ReplicationRestoredPosition;
                using var read = Db.StartReadOnlyTransaction();
                lock (_progressLock) _progress = new(read.GetCommitUlong(), position.FileId, position.Offset);
            }
            return [new(Database, Db, _capture, canonical, Genesis,
                Restored ? Db.ReplicationRestoredPosition : default, id => $"{id}.trl")];
        }

        public async Task RunAsync(IEventTopic topic, CancellationToken cancellation)
        {
            var db = Db!;
            ulong next;
            using (var read = db.StartReadOnlyTransaction()) next = read.GetCommitUlong();
            await foreach (var record in topic.ReadAsync(next, null, cancellation))
            {
                using (var transaction = await db.StartWritingTransaction(record.Offset + 1))
                {
                    using var cursor = transaction.CreateCursor();
                    cursor.CreateOrUpdateKeyValue(BitConverter.GetBytes(record.Offset), record.Payload.Span);
                    transaction.Commit();
                }
                var end = _capture.Completed;
                lock (_progressLock) _progress = new(record.Offset + 1, end.FileId, end.Offset);
            }
        }

        public LeaderTrlProgress? Progress
        {
            get { lock (_progressLock) return _progress; }
        }

        public LeaderCandidate CreateCandidate() => new("cluster", endpoint, Guid.NewGuid().ToString("N"), 1,
            [Database], endpoint, Guid.NewGuid().ToString("N"));
        public LeaderTrlProgress? GetProgress(string database) => Progress;
        public void RequestRestart(string reason) => throw new InvalidOperationException(reason);
        public void RequestFatalRestart(string reason) => Environment.FailFast(reason);
        public void ReportStatus(ReplicationNodeRole role) { }
        public void DatabaseRemoved(string database) { }
        public ValueTask<ulong> CaptureInitializationCursorAsync(string database, CancellationToken cancellation) =>
            ValueTask.FromResult(0ul);

        public async ValueTask DisposeAsync()
        {
            Db?.Dispose();
            if (_files != null) await _files.DisposeAsync();
            _cache.Dispose();
        }
    }

    sealed class Node : IAsyncDisposable
    {
        public required IHost App;
        public required Application Application;
        public required IDisposable[] Storages;
        public required Task Running;
        public required CancellationTokenSource Stopping;
        public IEventTopic Events => App.Services.GetRequiredService<IEventLog>().GetTopic("events");
        public ReplicationStatus Status => App.Services.GetRequiredService<ReplicationStatus>();

        public async ValueTask DisposeAsync()
        {
            await Stopping.CancelAsync();
            await Running.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            await App.StopAsync();
            if (App is IAsyncDisposable app) await app.DisposeAsync();
            else App.Dispose();
            await Application.DisposeAsync();
            foreach (var storage in Storages) storage.Dispose();
            Stopping.Dispose();
        }
    }

    static ReplicationNodeOptions Options(string endpoint) => new("cluster", endpoint, TimeSpan.FromMilliseconds(50),
        TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), 1);

    /// <param name="endpoint">An HTTP origin hosts the node in Kestrel with peer endpoints, as production does; null
    /// uses the single-node registrations in a generic host without any server.</param>
    async Task<Node> StartAsync(string? endpoint, CancellationToken cancellation)
    {
        // Production registers AzureLeaderStorage, AzureReplicationStorage and AzureEventLogStorage instead.
        var leader = new FileSystemLeaderStorage(Path.Combine(_root, "shared", "leader"), TimeSpan.FromSeconds(15), Initial);
        var canonical = new FileSystemReplicationStorage(Path.Combine(_root, "shared", Database));
        var eventLog = new FileSystemEventLogStorage(Path.Combine(_root, "shared", "eventlog"));
        var application = new Application(endpoint ?? "single-node", canonical, Path.Combine(_root, "node", Database));
        IHost app;
        if (endpoint != null)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseKestrel().UseUrls(endpoint);
            Register(builder.Services);
            builder.Services.AddBTDBReplication(Options(endpoint), 1000, TimeSpan.FromMilliseconds(250));
            builder.Services.AddBTDBEventLog(endpoint, "local-development-key");
            var web = builder.Build();
            web.MapBTDBReplication();
            web.MapBTDBEventLog();
            app = web;
        }
        else
        {
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            Register(builder.Services);
            builder.Services.AddBTDBReplicationSingleNode(Options("single-node"), 1000, TimeSpan.FromMilliseconds(250));
            builder.Services.AddBTDBEventLogSingleNode();
            app = builder.Build();
        }
        await app.StartAsync(cancellation);
        var status = app.Services.GetRequiredService<ReplicationStatus>();
        while (status.Current is not { Ready: true, Role: ReplicationNodeRole.Leader })
            await Task.Delay(20, cancellation);
        var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        var topic = app.Services.GetRequiredService<IEventLog>().GetTopic("events");
        return new()
        {
            App = app, Application = application, Storages = [leader, canonical, eventLog],
            Running = application.RunAsync(topic, stopping.Token), Stopping = stopping
        };

        void Register(IServiceCollection services)
        {
            services.AddSingleton<IReplicationNodeHost>(application);
            services.AddSingleton<IReplicationLeaderStorage>(leader);
            services.AddSingleton<IEventLogStorage>(eventLog);
        }
    }

    static async Task WaitForAppliedAndPublishedAsync(Node node, ulong events, CancellationToken cancellation)
    {
        while (node.Application.Progress is not { } progress || progress.EventId < events)
        {
            if (node.Running.IsFaulted) await node.Running;
            await Task.Delay(10, cancellation);
        }
        var cut = node.Application.Progress!.Value;
        await node.Status.WaitForCanonicalAsync(Database, new(cut.TrlFileId, cut.TrlPosition), cancellation);
    }

    static void AssertContent(BTreeKeyValueDB db, ulong events)
    {
        using var read = db.StartReadOnlyTransaction();
        Assert.Equal(events, read.GetCommitUlong());
        using var cursor = read.CreateCursor();
        Assert.Equal((long)events, cursor.GetKeyValueCount([]));
        for (ulong offset = 0; offset < events; offset++)
        {
            Assert.True(cursor.FindExactKey(BitConverter.GetBytes(offset)));
            Span<byte> buffer = stackalloc byte[8];
            Assert.Equal(BitConverter.GetBytes(offset * 7), cursor.GetValueSpan(ref buffer).ToArray());
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SingleNodeRestartsFromLocalDirectories(bool http)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var cancellation = timeout.Token;
        var endpoint = http ? $"http://127.0.0.1:{FreePort()}" : null;
        await using (var node = await StartAsync(endpoint, cancellation))
        {
            Assert.False(node.Application.Restored);
            for (ulong offset = 0; offset < 50; offset++)
                Assert.Equal(offset, await node.Events.PublishAsync(BitConverter.GetBytes(offset * 7), cancellation));
            await WaitForAppliedAndPublishedAsync(node, 50, cancellation);
        }

        // A disposable node: its local cache is gone, the shared directories hold the history and the event log.
        Directory.Delete(Path.Combine(_root, "node"), true);
        await using (var node = await StartAsync(endpoint, cancellation))
        {
            Assert.True(node.Application.Restored);
            AssertContent(node.Application.Db!, 50);
            Assert.Equal(new EventLogBounds(0, 50), await node.Events.GetBoundsAsync(cancellation));
            for (ulong offset = 50; offset < 60; offset++)
                Assert.Equal(offset, await node.Events.PublishAsync(BitConverter.GetBytes(offset * 7), cancellation));
            await WaitForAppliedAndPublishedAsync(node, 60, cancellation);
            AssertContent(node.Application.Db!, 60);
        }

        // Restarting on the kept cache validates it instead of downloading everything again.
        await using (var node = await StartAsync(endpoint, cancellation))
        {
            Assert.True(node.Application.Restored);
            AssertContent(node.Application.Db!, 60);
        }
    }

    [Fact]
    public void SingleNodeRegistrationsHaveNoPeerEndpointAndCountAsTheHostsOnlyNode()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddBTDBReplicationSingleNode(Options("single-node"), 1000, TimeSpan.FromMilliseconds(250));
        builder.Services.AddBTDBEventLogSingleNode();
        Assert.Throws<InvalidOperationException>(() =>
            builder.Services.AddBTDBReplication(Options("http://127.0.0.1:5080"), 1000, TimeSpan.Zero));
        Assert.Throws<InvalidOperationException>(() =>
            builder.Services.AddBTDBEventLog("http://127.0.0.1:5080", "key"));
        Assert.Throws<ArgumentException>(() => new ServiceCollection()
            .AddBTDBReplicationSingleNode(Options(" "), 1000, TimeSpan.Zero));
        using var app = builder.Build();
        Assert.Throws<InvalidOperationException>(() => app.MapBTDBReplication());
        Assert.Throws<InvalidOperationException>(() => app.MapBTDBEventLog());
    }
}
