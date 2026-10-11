using BTDB.KVDBLayer;
using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.EventSourcing.Test;

public class EventPublisherTest
{
    static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!predicate()) await Task.Delay(1, timeout.Token);
    }

    [Fact]
    public async Task ValidationRunsSynchronouslyBeforeEitherTopicAndMayReject()
    {
        using var db = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var handler = new Handler();
        await using var publisher = db.Publisher(data, metadata, handler);
        await publisher.StartAsync();
        var callerThread = Environment.CurrentManagedThreadId;
        handler.Validating = ev =>
        {
            Assert.Equal(callerThread, Environment.CurrentManagedThreadId);
            Assert.True(Ambient.IsQuery);
            Assert.False(Ambient.IsReplay);
            Assert.Equal(0ul, ev.EventId);
            Assert.Equal(0, data.Count);
            Assert.Equal(0, metadata.Count);
            Assert.Empty(Ambient.Relation<IEntries>());
        };
        var rejected = publisher.PublishAndWaitAsync(new Add { Amount = 0 });
        Assert.True(rejected.IsCompletedSuccessfully);
        Assert.False((await rejected).Success);
        handler.Validating = _ => throw new ArgumentException("Invalid event");
        Assert.Throws<ArgumentException>(() => { _ = publisher.PublishAndWaitAsync(new Add { Amount = 1 }); });
        Assert.Equal(0, data.Count);
        Assert.Equal(0, metadata.Count);
    }

    [Fact]
    public async Task EventLargerThanTheMaximumFailsBeforeItsDataPublication()
    {
        using var db = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        await using var publisher = new EventPublisher(db.Objects, data, metadata, new Handler().Dispatcher(),
            new() { TimeProvider = new FixedTime(), MaximumEventSize = 10 }, db.Capture);
        await publisher.StartAsync();
        var error = await Assert.ThrowsAsync<EventTooLargeException>(() =>
            publisher.PublishAndWaitAsync(new Add { Amount = 1 }));
        Assert.Equal(typeof(Add), error.EventType);
        Assert.True(error.Size > 10);
        Assert.Equal(0, data.Count);
        Assert.False(publisher.Completion.IsCompleted);
    }

    [Fact]
    public async Task BufferedBytesBoundTheReadAhead()
    {
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        using (var source = await TestDatabase.OpenAsync())
        await using (var publisher = source.Publisher(data, metadata, new Handler()))
        {
            await publisher.StartAsync();
            for (var i = 1; i <= 10; i++) await publisher.PublishAsync(new AddPure { Amount = i });
        }
        var sizes = new List<int>();
        await foreach (var record in data.ReadAsync(0, 10)) sizes.Add(record.Payload.Length);
        Assert.True(sizes.Min() * 3 > sizes.Max() * 5 / 2);
        using var cold = await TestDatabase.OpenAsync();
        var handler = new Handler();
        using var gate = new ManualResetEventSlim();
        handler.Consuming = ev => { if (ev.EventId == 1) Assert.True(gate.Wait(TimeSpan.FromSeconds(10))); };
        var requested = -1L;
        data.BeforeRead = offset =>
        {
            Interlocked.Exchange(ref requested, (long)offset);
            return Task.CompletedTask;
        };
        // Room for two events: the one being consumed and one read ahead.
        await using var coldPublisher = new EventPublisher(cold.Objects, data, metadata, handler.Dispatcher(),
            new() { TimeProvider = new FixedTime(), BufferedBytes = sizes.Max() * 5 / 2 }, cold.Capture);
        var starting = coldPublisher.StartAsync();
        await UntilAsync(() => Interlocked.Read(ref requested) >= 2);
        await Task.Delay(100);
        Assert.Equal(2, Interlocked.Read(ref requested)); // the third record waits for room before deserialization
        gate.Set();
        await starting;
        Assert.Equal(Enumerable.Range(1, 10).Select(i => ((ulong)i, true)), handler.Seen);
    }

    [Fact]
    public async Task SeparateTopicsReuseMetadataAndReturnVisibleLocalResults()
    {
        using var db = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var handler = new Handler();
        await using var publisher = db.Publisher(data, metadata, handler);
        await publisher.StartAsync();
        var ev = new Add { Amount = 2 };
        var first = await publisher.PublishAndWaitAsync(ev);
        var metadataCount = metadata.Count;
        var second = await publisher.PublishAndWaitAsync(new Add { Amount = 3 });
        Assert.True(first.Success);
        Assert.Equal(1ul, first.EventId);
        Assert.Equal(first.EventId, ev.EventId);
        Assert.Equal(2ul, second.EventId);
        Assert.True(metadataCount > 0);
        Assert.Equal(metadataCount, metadata.Count);
        Assert.Equal(2, data.Count);
        Assert.Equal(new[] { 2, 3 }, db.Read().Select(e => e.Amount));
        Assert.All(handler.Seen, s => Assert.False(s.Replay));
        using var read = db.Objects.StartReadOnlyTransaction();
        Assert.Equal(2ul, read.GetCommitUlong());
        Assert.Equal(2ul, publisher.Progress!.Value.EventId);
    }

    [Fact]
    public async Task TwoIndependentPublishersShareMetadataAndApplyIdenticalTransactions()
    {
        using var a = await TestDatabase.OpenAsync();
        using var canonical = a.CopyCanonicalBase();
        using var b = await TestDatabase.OpenAsync(canonical);
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var ah = new Handler();
        var bh = new Handler();
        await using var ap = a.Publisher(data, metadata, ah, batch: 1);
        await using var bp = b.Publisher(data, metadata, bh, batch: 64);
        await Task.WhenAll(ap.StartAsync(), bp.StartAsync());
        var calls = new List<Task>();
        for (var i = 1; i <= 40; i++)
            calls.Add(i % 2 == 0 ? ap.PublishAsync(new AddPure { Amount = i }).AsTask()
                : bp.PublishAndWaitAsync(new Add { Amount = i }));
        await Task.WhenAll(calls).WaitAsync(TimeSpan.FromSeconds(20));
        await UntilAsync(() => ap.LastAppliedEventId == 40 && bp.LastAppliedEventId == 40);
        Assert.Equal(40, data.Count);
        Assert.True(metadata.Count < data.Count);
        Assert.Equal(a.Read().Select(e => (e.Id, e.Amount, e.When, e.AllocatedId)),
            b.Read().Select(e => (e.Id, e.Amount, e.When, e.AllocatedId)));
        Assert.Equal(NativeTrl(a), NativeTrl(b));
        Assert.All(ah.Seen.Concat(bh.Seen), s => Assert.False(s.Replay));
    }

    [Fact]
    public async Task RestartReplaysOnlyTheMissingTailAndColdNodeReplaysHistory()
    {
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        using var files = new InMemoryReplicationFileStorage();
        using (var db = await TestDatabase.OpenAsync(files))
        await using (var publisher = db.Publisher(data, metadata, new Handler()))
        {
            await publisher.StartAsync();
            await publisher.PublishAsync(new AddPure { Amount = 1 });
        }
        using (var source = await TestDatabase.OpenAsync())
        await using (var publisher = source.Publisher(data, metadata, new Handler()))
        {
            await publisher.StartAsync();
            await publisher.PublishAsync(new AddPure { Amount = 2 });
        }
        using var resumed = await TestDatabase.OpenAsync(files);
        var resumedHandler = new Handler();
        await using var resumedPublisher = resumed.Publisher(data, metadata, resumedHandler);
        await resumedPublisher.StartAsync();
        Assert.Equal(new[] { (2ul, true) }, resumedHandler.Seen);
        Assert.Equal(2, resumed.Read().Length);
        using var cold = await TestDatabase.OpenAsync();
        var coldHandler = new Handler();
        await using var coldPublisher = cold.Publisher(data, metadata, coldHandler);
        await coldPublisher.StartAsync();
        Assert.Equal(new[] { (1ul, true), (2ul, true) }, coldHandler.Seen);
        await resumedPublisher.PublishAsync(new AddPure { Amount = 3 });
        await UntilAsync(() => coldPublisher.LastAppliedEventId == 3);
        Assert.Equal((3ul, false), coldHandler.Seen.Last());
        Assert.Equal((3ul, false), resumedHandler.Seen.Last());
    }

    [Fact]
    public async Task MetadataMustBeAppliedBeforeDataPublicationAndBeforeDelayedConsumption()
    {
        using var db = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var appliedGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        metadata.BeforeRead = _ => appliedGate.Task;
        await using var publisher = db.Publisher(data, metadata, new Handler());
        await publisher.StartAsync();
        var pending = publisher.PublishAndWaitAsync(new Add { Amount = 1 });
        await UntilAsync(() => metadata.Count > 0);
        Assert.Equal(0, data.Count);
        Assert.False(pending.IsCompleted);
        appliedGate.SetResult();
        Assert.True((await pending.WaitAsync(TimeSpan.FromSeconds(10))).Success);
    }

    [Fact]
    public async Task ConsumerCanFinishBeforeTheDataPublicationReceipt()
    {
        using var db = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var receiptGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        data.BeforeReceipt = _ => receiptGate.Task;
        await using var publisher = db.Publisher(data, metadata, new Handler());
        await publisher.StartAsync();
        var publishing = publisher.PublishAndWaitAsync(new Add { Amount = 7 });
        await UntilAsync(() => publisher.LastAppliedEventId == 1);
        Assert.Single(db.Read());
        Assert.False(publishing.IsCompleted);
        receiptGate.SetResult();
        Assert.Equal(1ul, (await publishing.WaitAsync(TimeSpan.FromSeconds(10))).EventId);
    }

    [Fact]
    public async Task FailureRollsBackOnlyItsEventAndRestartRetriesTheUnconsumedRecord()
    {
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        using var files = new InMemoryReplicationFileStorage();
        using (var db = await TestDatabase.OpenAsync(files))
        {
            var publisher = db.Publisher(data, metadata, new Handler { Fail = true });
            await publisher.StartAsync();
            await publisher.PublishAsync(new AddPure { Amount = 1 });
            var publishing = publisher.PublishAsync(new AddPure { Amount = -2 }).AsTask();
            await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.Completion);
            await Assert.ThrowsAnyAsync<Exception>(() => publishing);
            Assert.Equal(1ul, publisher.LastAppliedEventId);
            Assert.Single(db.Read());
            await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.DisposeAsync().AsTask());
        }
        using var reopened = await TestDatabase.OpenAsync(files);
        var handler = new Handler();
        await using var recovered = reopened.Publisher(data, metadata, handler);
        await recovered.StartAsync();
        Assert.Equal(new[] { (2ul, true) }, handler.Seen);
        Assert.Equal(new[] { 1, -2 }, reopened.Read().Select(e => e.Amount));
        Assert.Equal(new[] { 1ul, 2ul }, reopened.Read().Select(e => e.AllocatedId));
    }

    [Fact]
    public async Task AmbientQueriesStayIsolatedAndConsumptionCannotPublish()
    {
        using var a = await TestDatabase.OpenAsync();
        using var b = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var ah = new Handler();
        await using var ap = a.Publisher(data, metadata, ah);
        await using var bp = b.Publisher(new MemoryTopic("other-data"), new MemoryTopic("other-meta"), new Handler());
        await Task.WhenAll(ap.StartAsync(), bp.StartAsync());
        ah.Consuming = _ =>
        {
            Assert.False(Ambient.IsQuery);
            Assert.Equal(Ambient.CurrentEvent!.EventId, Ambient.EventId);
            Assert.Throws<InvalidOperationException>(() => Ambient.PublishEventAsync(new AddPure()));
        };
        await ap.PublishAsync(new AddPure { Amount = 1 });
        await Task.WhenAll(Task.Run(() => { using var scope = ap.StartQuery(); Assert.Single(Ambient.Relation<IEntries>()); }),
            Task.Run(() => { using var scope = bp.StartQuery(); Assert.Empty(Ambient.Relation<IEntries>()); }));
        Assert.Throws<InvalidOperationException>(() => Ambient.Relation<IEntries>());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MissingRetainedInputFailsStartup(bool pruneMetadata)
    {
        using var db = await TestDatabase.OpenAsync();
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        if (pruneMetadata) metadata.First = 1;
        else data.First = 1;
        var publisher = db.Publisher(data, metadata, new Handler());
        await Assert.ThrowsAsync<BTDB.Replication.EventLog.EventLogOffsetNotAvailableException>(() => publisher.StartAsync());
        await Assert.ThrowsAsync<BTDB.Replication.EventLog.EventLogOffsetNotAvailableException>(() => publisher.DisposeAsync().AsTask());
    }

    static byte[] NativeTrl(TestDatabase database)
    {
        var result = new List<byte>();
        foreach (var file in database.Files.Enumerate().Where(f => database.Files.GetFileType(f.Index) == KVFileType.TransactionLog)
                     .OrderBy(f => f.Index))
        {
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, false);
            result.AddRange(bytes);
        }
        return result.ToArray();
    }

    [Fact]
    public async Task LaggingMetadataConsumerWaitsWithoutApplyingTheDependentData()
    {
        using var a = await TestDatabase.OpenAsync();
        using var canonical = a.CopyCanonicalBase();
        using var b = await TestDatabase.OpenAsync(canonical);
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var delayed = new DelayedTopic(metadata);
        await using var ap = a.Publisher(data, metadata, new Handler());
        await using var bp = b.Publisher(data, delayed, new Handler());
        await Task.WhenAll(ap.StartAsync(), bp.StartAsync());
        await ap.PublishAsync(new AddPure { Amount = 5 });
        await delayed.Waiting.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(0ul, bp.LastAppliedEventId);
        Assert.Empty(b.Read());
        delayed.Release.SetResult();
        await UntilAsync(() => bp.LastAppliedEventId == 1);
        Assert.Equal(NativeTrl(a), NativeTrl(b));
    }

    [Fact]
    public async Task FailedEventInTheSameReplayBatchPreservesTheCommittedPrefix()
    {
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        using (var source = await TestDatabase.OpenAsync())
        await using (var publisher = source.Publisher(data, metadata, new Handler()))
        {
            await publisher.StartAsync();
            await publisher.PublishAsync(new AddPure { Amount = 1 });
            await publisher.PublishAsync(new AddPure { Amount = -2 });
        }
        using var db = await TestDatabase.OpenAsync();
        var failing = db.Publisher(data, metadata, new Handler { Fail = true });
        await Assert.ThrowsAsync<InvalidOperationException>(() => failing.StartAsync());
        Assert.Equal(1ul, failing.LastAppliedEventId);
        Assert.Single(db.Read());
        await Assert.ThrowsAsync<InvalidOperationException>(() => failing.DisposeAsync().AsTask());
        await using var recovered = db.Publisher(data, metadata, new Handler());
        await recovered.StartAsync();
        Assert.Equal(new[] { 1, -2 }, db.Read().Select(e => e.Amount));
        Assert.Equal(new[] { 1ul, 2ul }, db.Read().Select(e => e.AllocatedId));
    }

    [Fact]
    public async Task ConcurrentMetadataProposalsAreAppliedInTopicOrderBeforeEitherDataWrite()
    {
        using var a = await TestDatabase.OpenAsync();
        using var canonical = a.CopyCanonicalBase();
        using var b = await TestDatabase.OpenAsync(canonical);
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        metadata.BeforeRead = _ => gate.Task;
        await using var ap = a.Publisher(data, metadata, new Handler());
        await using var bp = b.Publisher(data, metadata, new Handler());
        await Task.WhenAll(ap.StartAsync(), bp.StartAsync());
        var first = ap.PublishAsync(new AddPure { Amount = 4 }).AsTask();
        var second = bp.PublishAndWaitAsync(new Add { Amount = 8 });
        await UntilAsync(() => metadata.Count == 2);
        Assert.Equal(0, data.Count);
        gate.SetResult();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(10));
        await UntilAsync(() => ap.LastAppliedEventId == 2 && bp.LastAppliedEventId == 2);
        Assert.Equal(new[] { 4, 8 }, a.Read().Select(e => e.Amount).Order());
        Assert.Equal(NativeTrl(a), NativeTrl(b));
    }
}
