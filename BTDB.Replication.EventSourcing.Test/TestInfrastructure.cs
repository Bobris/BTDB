using System.Runtime.CompilerServices;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.Replication.EventLog;
using BTDB.Replication.Test;
using BTDB.StreamLayer;

namespace BTDB.Replication.EventSourcing.Test;

// Controlled input boundary: appends become readable before their receipt, and each node can delay metadata delivery.
internal sealed class MemoryTopic(string name) : IEventTopic
{
    readonly object _lock = new();
    readonly List<byte[]> _records = new();
    TaskCompletionSource _changed = Signal();
    internal Func<ulong, Task>? BeforeReceipt;
    internal Func<ulong, Task>? BeforeRead;
    internal Action<ulong>? AfterRead;
    internal ulong First;
    internal int Count { get { lock (_lock) return _records.Count; } }
    public string Name => name;
    static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask<ulong> PublishAsync(ReadOnlyMemory<byte> record, CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        TaskCompletionSource changed;
        ulong offset;
        lock (_lock)
        {
            offset = (ulong)_records.Count;
            _records.Add(record.ToArray());
            changed = _changed;
            _changed = Signal();
        }
        changed.TrySetResult();
        return BeforeReceipt == null ? ValueTask.FromResult(offset) : WaitReceiptAsync(offset, cancellation);
    }

    async ValueTask<ulong> WaitReceiptAsync(ulong offset, CancellationToken cancellation)
    {
        await BeforeReceipt!(offset).WaitAsync(cancellation);
        return offset;
    }

    public ValueTask<EventLogBounds> GetBoundsAsync(CancellationToken cancellation = default)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock) return ValueTask.FromResult(new EventLogBounds(First, (ulong)_records.Count));
    }

    public async IAsyncEnumerable<EventLogRecord> ReadAsync(ulong start, ulong? end = null,
        [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        while (end == null || start < end)
        {
            byte[]? bytes;
            Task changed;
            lock (_lock)
            {
                if (start < First) throw new EventLogOffsetNotAvailableException("Record was pruned.");
                bytes = start < (ulong)_records.Count ? _records[(int)start] : null;
                changed = _changed.Task;
            }
            if (bytes == null) { await changed.WaitAsync(cancellation); continue; }
            if (BeforeRead != null) await BeforeRead(start).WaitAsync(cancellation);
            cancellation.ThrowIfCancellationRequested();
            yield return new(start++, bytes);
            AfterRead?.Invoke(start - 1);
        }
    }
}

internal sealed class TestDatabase : IDisposable
{
    internal readonly InMemoryReplicationFileStorage Files;
    internal readonly TransactionLogCapture Capture = new();
    internal readonly BTreeKeyValueDB KeyValues;
    internal readonly ObjectDB Objects = new();

    TestDatabase(InMemoryReplicationFileStorage files, BTreeKeyValueDB database, TransactionLogCapture capture)
    {
        Files = files;
        KeyValues = database;
        Capture = capture;
        Objects.Open(database, false, new DBOptions().WithoutAutoRegistrationOfRelations());
    }

    internal static async Task<TestDatabase> OpenAsync(InMemoryReplicationFileStorage? files = null)
    {
        files ??= new();
        var capture = new TransactionLogCapture();
        var database = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new LocalReplicatedCollection(files), TransactionLogCapture = capture,
            RequireExplicitTransactions = true, CompactorScheduler = null, Compression = new NoCompressionStrategy()
        });
        var result = new TestDatabase(files, database, capture);
        await result.Objects.InitializeRelations([typeof(IEntries)]);
        return result;
    }

    internal Entry[] Read()
    {
        using var transaction = Objects.StartReadOnlyTransaction();
        return transaction.GetRelation<IEntries>().ToArray();
    }

    internal EventPublisher Publisher(IEventTopic data, IEventTopic metadata, Handler handler, EventCluster? cluster = null,
        int batch = 32) => new(Objects, data, metadata, handler.Dispatcher(),
        new() { TimeProvider = new FixedTime(), MaximumBatchSize = batch, Cluster = cluster }, Capture);

    // Replica fixtures share the same restored native genesis (including the DB GUID), with separate local files.
    internal InMemoryReplicationFileStorage CopyCanonicalBase()
    {
        var copy = new InMemoryReplicationFileStorage();
        foreach (var file in Files.Enumerate())
        {
            var hint = Files.GetFileType(file.Index) switch
            {
                KVFileType.TransactionLog => "trl",
                KVFileType.KeyIndex => "kvi",
                KVFileType.PureValues => "pvl",
                _ => throw new InvalidOperationException("Unexpected native file.")
            };
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, false);
            var target = copy.ImportFile(file.Index, hint);
            var writer = new MemWriter(target.GetAppenderWriter());
            writer.WriteBlock(bytes);
            writer.Flush();
        }
        return copy;
    }

    public void Dispose() { Objects.Dispose(); KeyValues.Dispose(); }
}

internal sealed class FixedTime : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => new(2026, 10, 4, 12, 0, 0, TimeSpan.Zero);
}

internal sealed class DelayedTopic(IEventTopic inner) : IEventTopic
{
    internal readonly TaskCompletionSource Waiting = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public string Name => inner.Name;
    public ValueTask<ulong> PublishAsync(ReadOnlyMemory<byte> record, CancellationToken cancellation = default) =>
        inner.PublishAsync(record, cancellation);
    public ValueTask<EventLogBounds> GetBoundsAsync(CancellationToken cancellation = default) => inner.GetBoundsAsync(cancellation);
    public async IAsyncEnumerable<EventLogRecord> ReadAsync(ulong start, ulong? end = null,
        [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        await foreach (var record in inner.ReadAsync(start, end, cancellation))
        {
            Waiting.TrySetResult();
            await Release.Task.WaitAsync(cancellation);
            yield return record;
        }
    }
}

public class Entry
{
    [PrimaryKey] public ulong Id { get; set; }
    public int Amount { get; set; }
    public DateTime When { get; set; }
    public ulong AllocatedId { get; set; }
}

public interface IEntries : IRelation<Entry>;

public class Add : EventWithResult<Added>
{
    public int Amount { get; set; }
}

public class Added
{
    public bool Success { get; set; }
    public ulong EventId { get; set; }
}

public class AddPure : PureEvent
{
    public int Amount { get; set; }
}

internal sealed class Handler : IEventHandler<Add, Added>, IEventHandler<AddPure>
{
    internal Action<Event>? Validating;
    internal Action<Event>? Consuming;
    internal readonly System.Collections.Concurrent.ConcurrentQueue<(ulong Id, bool Replay)> Seen = new();
    internal bool Fail;

    public Added? Validate(Add ev)
    {
        Validating?.Invoke(ev);
        return ev.Amount == 0 ? new Added { Success = false } : null;
    }

    public Added Consume(Add ev) { Apply(ev, ev.Amount); return new() { Success = true, EventId = ev.EventId }; }
    public void Validate(AddPure ev) => Validating?.Invoke(ev);
    public void Consume(AddPure ev) => Apply(ev, ev.Amount);

    void Apply(Event ev, int amount)
    {
        Seen.Enqueue((ev.EventId, Ambient.IsReplay));
        Ambient.Relation<IEntries>().Upsert(new()
        {
            Id = ev.EventId, Amount = amount, When = ev.When, AllocatedId = Ambient.AllocateId()
        });
        Consuming?.Invoke(ev);
        if (Fail && amount < 0) throw new InvalidOperationException("Handler failed after writing.");
    }

    internal EventDispatcher Dispatcher() => new EventDispatcher().Register<Add, Added>(this).Register<AddPure>(this);
}
