using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Core.Pipeline;
using BTDB.KVDBLayer;
using BTDB.Replication.Test;
using Xunit;
using Xunit.Abstractions;

namespace BTDB.Replication.Azure.Test;

public class CheckpointRequestTest(AzuriteFixture fixture, ITestOutputHelper output) : IClassFixture<AzuriteFixture>
{
    sealed class Clock : IReplicationScheduler
    {
        readonly System.Diagnostics.Stopwatch _clock = System.Diagnostics.Stopwatch.StartNew();
        public TimeSpan Elapsed => _clock.Elapsed;
        public IDisposable Schedule(TimeSpan delay, Action callback, string description) => throw new NotSupportedException();
    }

    sealed class Requests : HttpPipelineSynchronousPolicy
    {
        public readonly Dictionary<string, int> Counts = new();
        public override void OnSendingRequest(HttpMessage message)
        {
            var query = message.Request.Uri.ToUri().Query;
            var comp = query.Split('&', '?').FirstOrDefault(p => p.StartsWith("comp=", StringComparison.Ordinal)) ?? "";
            var kind = $"{message.Request.Method} {comp}";
            lock (Counts) Counts[kind] = Counts.GetValueOrDefault(kind) + 1;
        }
    }

    static async Task Change(BTreeKeyValueDB db, ulong eventId, int keys)
    {
        using var transaction = await db.StartWritingTransaction(eventId);
        using var cursor = transaction.CreateCursor();
        for (var i = 0; i < keys; i++)
            cursor.CreateOrUpdateKeyValue(BitConverter.GetBytes((int)(eventId * 1000 + (ulong)i)), new byte[2000]);
        transaction.Commit();
    }

    [Fact]
    public async Task SteadyStateCheckpointNeedsNoRequestPerRetainedFile()
    {
        var requests = new Requests();
        var container = await fixture.ContainerAsync(requests);
        var raw = new AzureReplicationStorage(container, "db");
        var clock = new Clock();
        var authority = new LeaseAuthority(clock, 0, TimeSpan.Zero);
        authority.AcceptSuccess(authority.BeginRequest(), TimeSpan.FromHours(1));
        using var local = new InMemoryReplicationFileStorage();
        var capture = new TransactionLogCapture();
        using var db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new LocalReplicatedCollection(local), TransactionLogCapture = capture,
            Compression = new NoCompressionStrategy(), CompactorScheduler = null, FileSplitSize = 8096
        });
        ulong eventId = 0;
        // Values in sealed TRLs with enough waste; compaction moves live ones into many small PVLs.
        for (var i = 0; i < 400; i++)
        {
            using var transaction = await db.StartWritingTransaction(++eventId);
            using var cursor = transaction.CreateCursor();
            cursor.CreateOrUpdateKeyValue(BitConverter.GetBytes(i), Enumerable.Repeat((byte)i, 2000).ToArray());
            transaction.Commit();
        }
        using (var transaction = await db.StartWritingTransaction(++eventId))
        {
            using var cursor = transaction.CreateCursor();
            for (var i = 0; i < 400; i += 2) cursor.CreateOrUpdateKeyValue(BitConverter.GetBytes(i), new byte[2000]);
            transaction.Commit();
        }
        Assert.True(await db.Compact(CancellationToken.None));
        using var publisher = new CanonicalTrlPublisher(db, capture, raw, authority, 1, id => $"trl/{id}");
        Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
        var inventory = await CanonicalTrlInventory.DiscoverAsync(raw, new("trl/1", 1));
        var storage = raw.Bind(inventory, authority);
        await using var files = new ReplicationFileSet(local, storage);
        using var maintenance = new ReplicationMaintenance(db, files, publisher, storage, authority, clock,
            TimeSpan.FromTicks(1), TimeSpan.FromDays(1));
        await maintenance.RunDueAsync(default);
        for (var round = 0; round < 3; round++)
        {
            for (var i = 0; i < 10; i++) await Change(db, ++eventId, 1);
            Assert.Equal(TrlPublishResult.Published, await publisher.PublishNextAsync());
            lock (requests.Counts) requests.Counts.Clear();
            await maintenance.RunDueAsync(default);
            var types = db.FileCollection.FileTypes.Select(t => t.Value).ToArray();
            var pvls = types.Count(t => t == KVFileType.PureValues);
            var trls = types.Count(t => t == KVFileType.TransactionLog);
            output.WriteLine($"round {round}: {pvls} local PVLs, {trls} local TRLs, requests: " +
                string.Join(", ", requests.Counts.OrderBy(p => p.Key).Select(p => $"{p.Key}={p.Value}")));
            // Before: 4 listings and a HEAD per reused PVL and per retained TRL (173 HEADs at 20 PVLs/153 TRLs).
            Assert.True(pvls > 10 && trls > 100, $"{pvls} PVLs, {trls} TRLs");
            Assert.Equal(3, requests.Counts.GetValueOrDefault("GET comp=list"));
            Assert.True(requests.Counts.GetValueOrDefault("HEAD ") <= 2, $"{requests.Counts.GetValueOrDefault("HEAD ")} HEADs");
        }
    }
}
