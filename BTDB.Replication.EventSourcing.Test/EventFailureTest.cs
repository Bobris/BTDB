using System.Text.Json.Nodes;
using BTDB.KVDBLayer;
using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.EventSourcing.Test;

public class EventFailureTest
{
    static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!predicate()) await Task.Delay(1, timeout.Token);
    }

    /// <summary>leader.json applicationData: versioned, writable only while this node leads.</summary>
    internal sealed class LeaderData : IEventSkipDecisions
    {
        readonly object _lock = new();
        int _version;
        JsonNode? _value;
        public volatile bool Leader = true;
        public int Writes;

        public JsonNode? Value
        {
            get { lock (_lock) return _value?.DeepClone(); }
            set { lock (_lock) { _value = value; _version++; } }
        }

        public ValueTask<EventSkipSnapshot> ReadAsync(CancellationToken cancellation)
        {
            lock (_lock) return new(new EventSkipSnapshot(_version, _value?.DeepClone()));
        }

        public ValueTask<LeaderWriteOutcome> TryWriteAsync(EventSkipSnapshot expected, JsonNode? value,
            CancellationToken cancellation)
        {
            lock (_lock)
            {
                if (!Leader || (int)expected.Version != _version) return new(LeaderWriteOutcome.Rejected);
                _value = value?.DeepClone();
                _version++;
                Writes++;
                return new(LeaderWriteOutcome.Applied);
            }
        }
    }

    sealed class PoisonFixture
    {
        internal readonly MemoryTopic Data = new("data");
        internal readonly MemoryTopic Metadata = new("metadata");
        internal readonly LeaderData Decisions = new();
        internal readonly InMemoryEventLogStorage Attempts = new();
        internal readonly List<(ulong EventId, string Reason)> Skipped = new();
        internal Func<LeaderTrlProgress, CancellationToken, ValueTask> WaitForCanonical = (_, _) => ValueTask.CompletedTask;
        internal TimeSpan DecisionTimeout = TimeSpan.FromMinutes(1);

        internal EventPublisher Publisher(TestDatabase db, Handler handler, string node = "node", int batch = 32) =>
            new(db.Objects, Data, Metadata, handler.Dispatcher(), new()
            {
                TimeProvider = new FixedTime(), MaximumBatchSize = batch,
                OnEventSkipped = (id, reason) => { lock (Skipped) Skipped.Add((id, reason)); },
                Poison = new(Decisions, WaitForCanonical, Attempts, node)
                {
                    DecisionRetryInterval = TimeSpan.FromMilliseconds(5), DecisionTimeout = DecisionTimeout
                }
            }, db.Capture);

        internal Dictionary<ulong, string> Skips => EventSkips.Parse(Decisions.Value);

        /// <summary>Two runs fail fatally on event 2 and leave its attempts exhausted on node "node".</summary>
        internal async Task ExhaustAttemptsAsync(InMemoryReplicationFileStorage files)
        {
            using (var db = await TestDatabase.OpenAsync(files))
            {
                var first = Publisher(db, new Handler { Fail = true });
                await first.StartAsync();
                await first.PublishAsync(new AddPure { Amount = 1 });
                var publishing = first.PublishAsync(new AddPure { Amount = -2 }).AsTask();
                await Assert.ThrowsAsync<IOException>(() => first.Completion);
                await Assert.ThrowsAnyAsync<Exception>(() => publishing);
                await Assert.ThrowsAsync<IOException>(() => first.DisposeAsync().AsTask());
            }
            using (var db = await TestDatabase.OpenAsync(files))
            {
                var second = Publisher(db, new Handler { Fail = true });
                await Assert.ThrowsAsync<IOException>(() => second.StartAsync()); // second attempt
                await Assert.ThrowsAsync<IOException>(() => second.DisposeAsync().AsTask());
            }
            Assert.Empty(Skips);
        }
    }

    [Fact]
    public async Task DeterministicFailureRollsBackOnlyItsEventAndConsumptionContinues()
    {
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        using var db = await TestDatabase.OpenAsync();
        var failed = new List<(ulong, Type)>();
        var handler = new Handler { FailDeterministically = true };
        handler.Consuming = _ => Ambient.RegisterPostCommit(() => { });
        await using (var publisher = new EventPublisher(db.Objects, data, metadata, handler.Dispatcher(),
                         new() { TimeProvider = new FixedTime(), OnConsumeFailed = (ev, e) => failed.Add((ev.EventId, e.GetType())) },
                         db.Capture))
        {
            await publisher.StartAsync();
            await publisher.PublishAsync(new AddPure { Amount = 1 });
            var rejected = new AddPure { Amount = -2 };
            await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(rejected).AsTask());
            Assert.Equal(2ul, rejected.EventId);
            await publisher.PublishAsync(new AddPure { Amount = 3 });
            Assert.False(publisher.Completion.IsCompleted);
            Assert.Equal(3ul, publisher.LastAppliedEventId);
        }
        Assert.Equal(new[] { 1, 3 }, db.Read().Select(e => e.Amount));
        Assert.Equal(new[] { 1ul, 2ul }, db.Read().Select(e => e.AllocatedId)); // the rolled-back allocation is reused
        Assert.Equal(new[] { (2ul, typeof(InvalidOperationException)) }, failed);
        using (var read = db.Objects.StartReadOnlyTransaction()) Assert.Equal(3ul, read.GetCommitUlong());

        // Replay reaches the same outcome without stopping.
        using var cold = await TestDatabase.OpenAsync();
        await using var coldPublisher = cold.Publisher(data, metadata, new Handler { FailDeterministically = true });
        await coldPublisher.StartAsync();
        Assert.Equal(new[] { 1, 3 }, cold.Read().Select(e => e.Amount));
        Assert.Equal(new[] { 1ul, 2ul }, cold.Read().Select(e => e.AllocatedId));
    }

    [Fact]
    public async Task DefaultClassificationTreatsEnvironmentFailuresAsFatal()
    {
        Assert.True(EventPublisherOptions.DefaultIsFatalConsumeException(new OutOfMemoryException()));
        Assert.True(EventPublisherOptions.DefaultIsFatalConsumeException(new IOException()));
        Assert.True(EventPublisherOptions.DefaultIsFatalConsumeException(new OperationCanceledException()));
        Assert.True(EventPublisherOptions.DefaultIsFatalConsumeException(
            new InvalidOperationException("wrapped", new InsufficientExecutionStackException())));
        Assert.True(EventPublisherOptions.DefaultIsFatalConsumeException(
            new AggregateException(new ArgumentException(), new TimeoutException())));
        Assert.False(EventPublisherOptions.DefaultIsFatalConsumeException(new NullReferenceException()));
        Assert.False(EventPublisherOptions.DefaultIsFatalConsumeException(new ArgumentException()));

        // A custom policy can make any exception fatal.
        var data = new MemoryTopic("data");
        using var db = await TestDatabase.OpenAsync();
        var publisher = new EventPublisher(db.Objects, data, new MemoryTopic("metadata"),
            new Handler { FailDeterministically = true }.Dispatcher(),
            new() { TimeProvider = new FixedTime(), IsFatalConsumeException = _ => true }, db.Capture);
        await publisher.StartAsync();
        _ = publisher.PublishAsync(new AddPure { Amount = -1 }).AsTask();
        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.Completion);
        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.DisposeAsync().AsTask());
    }

    [Fact]
    public async Task EventFailingFatallyOnEveryAttemptIsSkippedOnAllNodes()
    {
        var fixture = new PoisonFixture();
        fixture.Decisions.Value = new JsonObject { ["mySetting"] = "kept" };
        using var files = new InMemoryReplicationFileStorage();
        await fixture.ExhaustAttemptsAsync(files);
        using (var db = await TestDatabase.OpenAsync(files))
        {
            var handler = new Handler { Fail = true };
            await using var third = fixture.Publisher(db, handler);
            await third.StartAsync(); // both attempts exhausted: skipped instead of a third one
            Assert.Empty(handler.Seen);
            Assert.Equal(2ul, third.LastAppliedEventId);
            Assert.Equal(new[] { 1 }, db.Read().Select(e => e.Amount));
            await third.PublishAsync(new AddPure { Amount = 3 });
            Assert.Equal(new[] { 1, 3 }, db.Read().Select(e => e.Amount));
        }
        Assert.Equal(new[] { 2ul }, fixture.Skips.Keys);
        Assert.Equal("kept", fixture.Decisions.Value!["mySetting"]!.GetValue<string>()); // other application data stays
        Assert.Equal(2ul, Assert.Single(fixture.Skipped).EventId);

        // Another node never consumes the skipped event.
        using var cold = await TestDatabase.OpenAsync();
        var coldHandler = new Handler { Fail = true };
        await using var coldPublisher = fixture.Publisher(cold, coldHandler, "other");
        await coldPublisher.StartAsync();
        Assert.Equal(new[] { 1ul, 3ul }, coldHandler.Seen.Select(s => s.Id));
        Assert.Equal(new[] { 1, 3 }, cold.Read().Select(e => e.Amount));
        Assert.Equal(new[] { 2ul, 2ul }, fixture.Skipped.Select(s => s.EventId));
    }

    [Fact]
    public async Task FollowerWaitsUntilALeaderRecordsTheDecision()
    {
        var fixture = new PoisonFixture();
        using var files = new InMemoryReplicationFileStorage();
        await fixture.ExhaustAttemptsAsync(files);
        fixture.Decisions.Leader = false;
        using var db = await TestDatabase.OpenAsync(files);
        await using var follower = fixture.Publisher(db, new Handler { Fail = true });
        var starting = follower.StartAsync();
        await Task.Delay(100);
        Assert.False(starting.IsCompleted); // a follower cannot record the decision
        Assert.Empty(fixture.Skips);
        fixture.Decisions.Leader = true; // it became the leader (or another leader decided)
        await starting.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { 2ul }, fixture.Skips.Keys);
        Assert.Equal(2ul, follower.LastAppliedEventId);
    }

    [Fact]
    public async Task NodeWithoutADecisionStopsAfterTheTimeout()
    {
        var fixture = new PoisonFixture { DecisionTimeout = TimeSpan.FromMilliseconds(50) };
        using var files = new InMemoryReplicationFileStorage();
        await fixture.ExhaustAttemptsAsync(files);
        fixture.Decisions.Leader = false;
        using var db = await TestDatabase.OpenAsync(files);
        var follower = fixture.Publisher(db, new Handler { Fail = true });
        await Assert.ThrowsAsync<TimeoutException>(() => follower.StartAsync().WaitAsync(TimeSpan.FromSeconds(10)));
        await Assert.ThrowsAsync<TimeoutException>(() => follower.DisposeAsync().AsTask());
        Assert.Equal(1ul, follower.LastAppliedEventId);
    }

    [Fact]
    public async Task OperatorSkipAppliesAtStartWithoutDeserializingTheEvent()
    {
        var fixture = new PoisonFixture();
        using (var source = await TestDatabase.OpenAsync())
        await using (var publisher = source.Publisher(fixture.Data, fixture.Metadata, new Handler()))
        {
            await publisher.StartAsync();
            for (var i = 1; i <= 3; i++) await publisher.PublishAsync(new AddPure { Amount = i });
        }
        fixture.Decisions.Leader = false;
        Assert.False(await EventPublisher.SkipEventAsync(fixture.Decisions, 2, "Corrupted by a bad release", default));
        fixture.Decisions.Leader = true;
        Assert.True(await EventPublisher.SkipEventAsync(fixture.Decisions, 2, "Corrupted by a bad release", default));
        using var cold = await TestDatabase.OpenAsync();
        var handler = new Handler();
        await using var coldPublisher = fixture.Publisher(cold, handler);
        await coldPublisher.StartAsync();
        Assert.Equal(new[] { 1ul, 3ul }, handler.Seen.Select(s => s.Id));
        Assert.Equal((2ul, "Corrupted by a bad release"), Assert.Single(fixture.Skipped));
        Assert.Equal(3ul, coldPublisher.LastAppliedEventId);
    }

    [Fact]
    public async Task ConsumptionWaitsForTheCanonicalWindowThenPrunesCoveredDecisions()
    {
        var fixture = new PoisonFixture();
        using (var source = await TestDatabase.OpenAsync())
        await using (var publisher = source.Publisher(fixture.Data, fixture.Metadata, new Handler()))
        {
            await publisher.StartAsync();
            for (var i = 1; i <= 3; i++) await publisher.PublishAsync(new AddPure { Amount = i });
        }
        fixture.Decisions.Value = new JsonObject
        {
            ["mySetting"] = "kept", [EventSkips.Key] = new JsonObject { ["2"] = "old", ["9"] = "future" }
        };
        var canonical = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var waited = new List<LeaderTrlProgress>();
        fixture.WaitForCanonical = async (progress, c) =>
        {
            lock (waited) waited.Add(progress);
            await canonical.Task.WaitAsync(c);
        };
        using var cold = await TestDatabase.OpenAsync();
        var handler = new Handler();
        await using var coldPublisher = fixture.Publisher(cold, handler, batch: 2);
        var starting = coldPublisher.StartAsync();
        await UntilAsync(() => { lock (waited) return waited.Count == 1; });
        Assert.Equal(2ul, waited[0].EventId); // the window holds events 1 and 2 (2 is skipped)
        Assert.Equal(cold.Capture.Completed, waited[0].Position);
        await Task.Delay(50);
        Assert.Equal(new[] { 1ul }, handler.Seen.Select(s => s.Id));
        Assert.False(starting.IsCompleted);
        Assert.Equal(new ulong[] { 2, 9 }, fixture.Skips.Keys.Order());
        canonical.SetResult();
        await starting;
        Assert.Equal(new[] { 1ul, 3ul }, handler.Seen.Select(s => s.Id));
        Assert.Single(waited);
        // Event 2 is canonical, so no restore needs its decision any more.
        Assert.Equal(new[] { 9ul }, fixture.Skips.Keys);
        Assert.Equal("kept", fixture.Decisions.Value!["mySetting"]!.GetValue<string>());
        // Each attempt in the window was recorded before its consumption.
        var attempts = await fixture.Attempts.ReadAsync("event-attempts/data/node", CancellationToken.None);
        Assert.Contains("\"Offset\":0", System.Text.Encoding.UTF8.GetString(attempts!.Content.Span));
    }
}
