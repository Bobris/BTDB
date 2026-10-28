using System.Runtime.CompilerServices;
using BTDB.KVDBLayer;
using BTDB.Replication.EventLog;
using Xunit;

namespace BTDB.Replication.EventSourcing.Test;

public class NotePart
{
    public int X { get; set; }
    public Dictionary<string, int>? Map { get; set; }
}

public class Note : PureEvent
{
    public string? Text { get; set; }
    public NotePart? Part { get; set; }
    public List<NotePart>? Parts { get; set; }
}

public class TransferLeg
{
    public ulong From { get; set; }
    public List<string>? Tags { get; set; }
}

public class Transfer : EventWithResult<Added>
{
    public int Amount { get; set; }
    public TransferLeg? Leg { get; set; }
}

/// <summary>Nodes publish random events of several types, some first seen at once on several nodes and some failing
/// deterministically, through views of shared topics that delay receipts and reads, while publishers restart, also
/// under their publications and with the database reopened. Every
/// node ends with the same entries and the same native transaction log, and every result names its own entry.</summary>
public class EventPublisherRandomTest
{
    sealed class JitterTopic(IEventTopic inner, int seed) : IEventTopic
    {
        readonly Random _random = new(seed);

        int Delay()
        {
            lock (_random) return _random.Next(4) == 0 ? _random.Next(0, 3) : -1;
        }

        public string Name => inner.Name;

        // Publish synchronously, so the inner topic keeps this node's call order; only the receipt is late.
        public ValueTask<ulong> PublishAsync(ReadOnlyMemory<byte> record, CancellationToken cancellation = default)
        {
            var publishing = inner.PublishAsync(record, cancellation);
            var delay = Delay();
            return delay < 0 ? publishing : Late(publishing, delay, cancellation);
        }

        static async ValueTask<ulong> Late(ValueTask<ulong> publishing, int delay, CancellationToken cancellation)
        {
            var offset = await publishing;
            await Task.Delay(delay, cancellation);
            return offset;
        }

        public ValueTask<EventLogBounds> GetBoundsAsync(CancellationToken cancellation = default) =>
            inner.GetBoundsAsync(cancellation);

        public async IAsyncEnumerable<EventLogRecord> ReadAsync(ulong start, ulong? end = null,
            [EnumeratorCancellation] CancellationToken cancellation = default)
        {
            await foreach (var record in inner.ReadAsync(start, end, cancellation))
            {
                var delay = Delay();
                if (delay >= 0) await Task.Delay(delay, cancellation);
                yield return record;
            }
        }
    }

    sealed class RandomHandler : IEventHandler<Add, Added>, IEventHandler<AddPure>, IEventHandler<Note>,
        IEventHandler<Transfer, Added>
    {
        public Added? Validate(Add ev) => null;
        public Added Consume(Add ev) => Apply(ev, ev.Amount);
        public void Validate(AddPure ev) { }
        public void Consume(AddPure ev) => Apply(ev, ev.Amount);
        public void Validate(Note ev) { }

        public void Consume(Note ev) => Apply(ev, (ev.Text?.Length ?? -1) + (ev.Part?.X ?? 0) +
                                                  (ev.Part?.Map?.Values.Sum() ?? 0) + (ev.Parts?.Sum(p => p.X) ?? 0));

        public Added? Validate(Transfer ev) => ev.Amount == 0 ? new Added { Success = false } : null;

        public Added Consume(Transfer ev) =>
            Apply(ev, ev.Amount + (int)(ev.Leg?.From ?? 0) + (ev.Leg?.Tags?.Count ?? 0));

        static Added Apply(Event ev, int amount)
        {
            Ambient.Relation<IEntries>().Upsert(new()
            {
                Id = ev.EventId, Amount = amount, When = ev.When, AllocatedId = Ambient.AllocateId()
            });
            if (amount < 0 && amount % 3 == 0) throw new InvalidOperationException("Rejected after writing.");
            return new() { Success = true, EventId = ev.EventId };
        }

        internal EventDispatcher Dispatcher() => new EventDispatcher().Register<Add, Added>(this)
            .Register<AddPure>(this).Register<Note>(this).Register<Transfer, Added>(this);
    }

    static Event RandomEvent(Random random)
    {
        var amount = random.Next(-9, 10);
        return random.Next(4) switch
        {
            0 => new AddPure { Amount = amount },
            1 => new Add { Amount = amount == 0 ? 1 : amount },
            2 => new Note
            {
                Text = random.Next(3) == 0 ? null : new string('n', random.Next(3)),
                Part = random.Next(2) == 0 ? null : new() { X = amount, Map = random.Next(2) == 0 ? null : new() { ["k"] = 1 } },
                Parts = random.Next(2) == 0 ? null : [new() { X = -1 }, new() { X = amount }]
            },
            _ => new Transfer
            {
                Amount = amount,
                Leg = random.Next(2) == 0 ? null : new() { From = (ulong)random.Next(3), Tags = random.Next(2) == 0 ? null : ["t"] }
            }
        };
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 30).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task NodesApplyIdenticalHistoriesUnderRestartsAndConcurrentNewTypes(int seed)
    {
        var random = new Random(seed);
        var data = new MemoryTopic("data");
        var metadata = new MemoryTopic("metadata");
        var first = await TestDatabase.OpenAsync();
        var databases = new List<TestDatabase> { first };
        try
        {
            for (var i = 1; i < 3; i++) databases.Add(await TestDatabase.OpenAsync(first.CopyCanonicalBase()));
            var handler = new RandomHandler();
            var results = new System.Collections.Concurrent.ConcurrentBag<Added>();
            EventPublisher NewPublisher(int node) => new(databases[node].Objects,
                new JitterTopic(data, seed * 10 + node), new JitterTopic(metadata, seed * 20 + node),
                handler.Dispatcher(), new() { TimeProvider = new FixedTime(), MaximumBatchSize = 1 + random.Next(8) },
                databases[node].Capture);

            var publishers = new EventPublisher[databases.Count];
            for (var node = 0; node < publishers.Length; node++) publishers[node] = NewPublisher(node);
            await Task.WhenAll(publishers.Select(p => p.StartAsync())).WaitAsync(TimeSpan.FromSeconds(20));
            var plans = Enumerable.Range(0, publishers.Length).Select(_ =>
                Enumerable.Range(0, 40).Select(_ => (Event: RandomEvent(random),
                    Restart: random.Next(12) == 0 ? random.Next(1, 4) : 0, Wait: random.Next(3) == 0)).ToList()).ToList();
            await Task.WhenAll(plans.Select((plan, node) => Task.Run(async () =>
            {
                var calls = new List<Task>();
                foreach (var (ev, restart, wait) in plan)
                {
                    if (restart != 0)
                    {
                        // 1 waits for the node's publications; 2 and 3 stop the publisher under them, 3 also reopens
                        // the database from its files.
                        if (restart == 1) await Task.WhenAll(calls.Select(c => c.ContinueWith(_ => { })));
                        await publishers[node].DisposeAsync();
                        await Task.WhenAll(calls.Select(c => c.ContinueWith(_ => { })));
                        calls.Clear();
                        if (restart == 3)
                        {
                            var files = databases[node].Files;
                            databases[node].Dispose();
                            databases[node] = await TestDatabase.OpenAsync(files);
                        }

                        publishers[node] = NewPublisher(node);
                        await publishers[node].StartAsync();
                    }

                    var publisher = publishers[node];
                    var call = ev switch
                    {
                        PureEvent pure => publisher.PublishAsync(pure).AsTask(),
                        Add add => Record(publisher.PublishAndWaitAsync(add)),
                        _ => Record(publisher.PublishAndWaitAsync((Transfer)ev))
                    };
                    // Rejected after writing, or cut short by a restart: an unknown outcome.
                    calls.Add(call.ContinueWith(t => t.Exception is { } e && e.InnerException is not
                        (InvalidOperationException or OperationCanceledException)
                        ? throw e.InnerException! : Task.CompletedTask).Unwrap());
                    if (wait) await calls[^1];
                }

                await Task.WhenAll(calls);
            }))).WaitAsync(TimeSpan.FromSeconds(60));

            async Task Record(Task<Added> call) => results.Add(await call);

            using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
                while (publishers.Any(p => p.LastAppliedEventId != (ulong)data.Count))
                    await Task.Delay(1, timeout.Token);
            var expected = databases[0].Read().Select(e => (e.Id, e.Amount, e.When, e.AllocatedId)).ToList();
            foreach (var database in databases.Skip(1))
                Assert.Equal(expected, database.Read().Select(e => (e.Id, e.Amount, e.When, e.AllocatedId)));
            foreach (var database in databases.Skip(1)) Assert.Equal(NativeTrl(databases[0]), NativeTrl(database));
            Assert.All(expected, e => Assert.InRange(e.Id, 1ul, (ulong)data.Count));
            Assert.All(expected, e => Assert.False(e.Amount < 0 && e.Amount % 3 == 0));
            Assert.Equal(Enumerable.Range(1, expected.Count).Select(i => (ulong)i), expected.Select(e => e.AllocatedId));
            var ids = expected.Select(e => e.Id).ToHashSet();
            Assert.All(results, r => Assert.True(r.Success ? ids.Contains(r.EventId) : r.EventId == 0, $"Seed {seed}: {r.EventId}"));
            foreach (var publisher in publishers) await publisher.DisposeAsync();
        }
        finally
        {
            foreach (var database in databases) database.Dispose();
        }
    }

    static byte[] NativeTrl(TestDatabase database)
    {
        var result = new List<byte>();
        foreach (var file in database.Files.Enumerate()
                     .Where(f => database.Files.GetFileType(f.Index) == KVFileType.TransactionLog).OrderBy(f => f.Index))
        {
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, false);
            result.AddRange(bytes);
        }

        return result.ToArray();
    }
}
