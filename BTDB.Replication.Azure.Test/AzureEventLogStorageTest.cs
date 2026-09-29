using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Core.Pipeline;
using BTDB.Replication.EventLog;
using BTDB.Replication.Test.EventLog;
using Xunit;
using static BTDB.Replication.Test.EventLog.EventLogTestExtensions;

namespace BTDB.Replication.Azure.Test;

public class AzureEventLogStorageTest(BlobStorageFixture fixture) : IClassFixture<BlobStorageFixture>
{
    const string Topic = "t";
    static readonly EventLogOwner A = new("a", "http://node-a:5000/x?y=1");
    static readonly EventLogOwner B = new("b", "http://node-b");

    /// <summary>Loses the response of the next successful conditional upload after Azure applied it.</summary>
    sealed class LoseNextUpload : HttpPipelineSynchronousPolicy
    {
        public bool Armed;
        public int Uploads;
        public override void OnReceivedResponse(HttpMessage message)
        {
            if (message.Request.Method != RequestMethod.Put || message.Response.Status is < 200 or >= 300) return;
            Uploads++;
            if (!Armed || message.Request.Uri.ToUri().Query.Contains("comp=metadata", StringComparison.Ordinal)) return;
            Armed = false;
            throw new IOException("Lost upload response after Azure applied it.");
        }
    }

    [Fact]
    public async Task ConditionalWritesOwnersRangesAndListingFollowTheContract()
    {
        var storage = new AzureEventLogStorage(await fixture.ContainerAsync(), "log");
        var created = await storage.WriteAsync("t/1", null, new byte[] { 1, 2, 3 }, A, null, default);
        Assert.Equal(EventLogWriteOutcome.Applied, created.Outcome);
        Assert.Equal(EventLogWriteOutcome.Rejected, (await storage.WriteAsync("t/1", null, new byte[] { 9 }, B, null, default)).Outcome);
        var blob = await storage.ReadAsync("t/1", default);
        Assert.Equal(created.Version, blob!.Version);
        Assert.Equal(A, blob.Owner);
        Assert.Equal(new byte[] { 1, 2, 3 }, blob.Content.ToArray());
        var replaced = await storage.WriteAsync("t/1", created.Version, new byte[] { 1, 2, 3, 4 }, A, null, default);
        Assert.Equal(EventLogWriteOutcome.Applied, replaced.Outcome);
        Assert.Equal(EventLogWriteOutcome.Rejected, (await storage.WriteAsync("t/1", created.Version, new byte[] { 7 }, B, null, default)).Outcome);
        var owned = await storage.SetOwnerAsync("t/1", replaced.Version!, B, default);
        Assert.Equal(EventLogWriteOutcome.Applied, owned.Outcome);
        Assert.Equal(EventLogWriteOutcome.Rejected, (await storage.SetOwnerAsync("t/1", replaced.Version!, A, default)).Outcome);
        var properties = await storage.GetPropertiesAsync("t/1", default);
        Assert.Equal((owned.Version, 4L, B), (properties!.Version, properties.Length, properties.Owner));
        var range = new byte[2];
        await storage.ReadRangeAsync("t/1", owned.Version!, 1, range, default);
        Assert.Equal(new byte[] { 2, 3 }, range);
        await Assert.ThrowsAsync<EventLogVersionChangedException>(async () =>
            await storage.ReadRangeAsync("t/1", created.Version!, 1, range, default));
        await storage.WriteAsync("t/2", null, new byte[] { 5 }, null, "abc", default);
        await storage.WriteAsync("u/1", null, new byte[] { 5 }, null, null, default);
        var listed = new List<EventLogObjectInfo>();
        await foreach (var info in storage.ListAsync("t/", default)) listed.Add(info);
        Assert.Equal(["t/1", "t/2"], listed.Select(i => i.Key));
        Assert.Null(await storage.ReadAsync("t/3", default));
        Assert.Null(await storage.GetPropertiesAsync("t/3", default));
    }

    [Fact]
    public async Task DeletionNeedsAnElapsedDeadlineAndTheMarkedVersion()
    {
        var time = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        var storage = new AzureEventLogStorage(await fixture.ContainerAsync(), "log", time);
        var created = await storage.WriteAsync("t/1", null, new byte[] { 1 }, null, "sha", default);
        Assert.False(await storage.DeleteAsync("t/1", created.Version!, default)); // unmarked
        Assert.Null(await storage.ScheduleDeletionAsync("t/1", "\"0x0\"", TimeSpan.FromHours(1), default));
        var marked = await storage.ScheduleDeletionAsync("t/1", created.Version!, TimeSpan.FromHours(1), default);
        Assert.NotNull(marked);
        Assert.Equal(marked, await storage.ScheduleDeletionAsync("t/1", marked!, TimeSpan.FromHours(5), default));
        Assert.False(await storage.DeleteAsync("t/1", marked!, default)); // not due
        time.Advance(TimeSpan.FromHours(2));
        Assert.False(await storage.DeleteAsync("t/1", created.Version!, default)); // stale version
        Assert.True(await storage.DeleteAsync("t/1", marked!, default));
        Assert.Null(await storage.ReadAsync("t/1", default));
    }

    [Fact]
    public async Task LargeContentIsStagedAndCommittedConditionally()
    {
        var storage = new AzureEventLogStorage(await fixture.ContainerAsync(), "log", null, 1024);
        var content = Enumerable.Range(0, 5000).Select(i => (byte)i).ToArray();
        var created = await storage.WriteAsync("t/1", null, content, A, null, default);
        Assert.Equal(EventLogWriteOutcome.Applied, created.Outcome);
        Assert.Equal(EventLogWriteOutcome.Rejected, (await storage.WriteAsync("t/1", null, content, A, null, default)).Outcome);
        Assert.Equal(content, (await storage.ReadAsync("t/1", default))!.Content.ToArray());
        Assert.Equal(A, (await storage.GetPropertiesAsync("t/1", default))!.Owner);
    }

    [Fact]
    public async Task OwnerLaneOnAzureReconcilesALostResponseWithoutDuplicates()
    {
        var faults = new LoseNextUpload();
        var storage = new AzureEventLogStorage(await fixture.ContainerAsync(faults), "log");
        var options = new EventLogOptions { SplitCap = 512, SealFreeSpace = 64, AmbiguousRetryDelay = TimeSpan.Zero };
        var lane = await EventLogOwnerLane.CreateCandidateAsync(storage, new(storage, Topic), Topic, options,
            new TestScheduler(), A, default);
        for (var i = 0; i < 30; i++)
        {
            faults.Armed = i % 3 == 1;
            Assert.Equal((ulong)i, await lane.SubmitAsync(1, (ulong)i + 1, Bytes(i, 40), 1, false, 0, default));
        }
        var records = await new EventLogStorageReader(storage, Topic).ReadAllAsync();
        Assert.Equal(Enumerable.Range(0, 30).Select(i => Bytes(i, 40)), records.Select(r => r.Payload.ToArray()));
        var b = await EventLogOwnerLane.CreateCandidateAsync(storage, new(storage, Topic), Topic, options,
            new TestScheduler(), B, default);
        Assert.Equal(30ul, await b.SubmitAsync(2, 1, Bytes(30, 40), 1, false, 0, default));
        await Assert.ThrowsAsync<EventLogOwnerLostException>(async () =>
            await lane.SubmitAsync(1, 31, Bytes(99, 40), 1, false, 0, default));
    }

    [Fact]
    public async Task TwoNodesPublishAndFollowThroughAzure()
    {
        var container = await fixture.ContainerAsync();
        var transport = new InProcessEventLogTransport();
        var scheduler = new SystemReplicationScheduler();
        var options = new EventLogOptions
        {
            SplitCap = 2048, SealFreeSpace = 256, HeartbeatInterval = TimeSpan.FromMilliseconds(100),
            OwnerTimeout = TimeSpan.FromSeconds(1), ContentionBackoff = TimeSpan.FromMilliseconds(20)
        };
        await using var a = new EventLogService(new AzureEventLogStorage(container, "log"), transport.From("a"), "a",
            scheduler, options);
        await using var b = new EventLogService(new AzureEventLogStorage(fixture.Connect(container.Name), "log"),
            transport.From("b"), "b", scheduler, options);
        transport.Register("a", a);
        transport.Register("b", b);
        Assert.Equal(0ul, await a.GetTopic(Topic).PublishAsync(Bytes(0)));
        using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var received = new List<ulong>();
        var reading = Task.Run(async () =>
        {
            await foreach (var record in b.GetTopic(Topic).ReadAsync(0, 40, cancel.Token)) received.Add(record.Offset);
        });
        var offsets = await Task.WhenAll(Enumerable.Range(1, 39)
            .Select(i => (i % 2 == 0 ? a : b).GetTopic(Topic).PublishAsync(Bytes(i)).AsTask()));
        Assert.Equal(Enumerable.Range(1, 39).Select(i => (ulong)i), offsets.OrderBy(o => o));
        await reading.WaitAsync(cancel.Token);
        Assert.Equal(Enumerable.Range(0, 40).Select(i => (ulong)i), received);
    }
}
