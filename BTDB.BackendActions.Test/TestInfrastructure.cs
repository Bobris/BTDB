using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.Replication;
using BTDB.Replication.EventLog;
using BTDB.Replication.EventSourcing;
using BTDB.Replication.Test;
using BTDB.StreamLayer;

namespace BTDB.BackendActions.Test;

internal sealed class MemoryTopic(string name) : IEventTopic
{
    readonly object _lock = new();
    readonly List<byte[]> _records = new();
    TaskCompletionSource _changed = Signal();
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
        return ValueTask.FromResult(offset);
    }

    public ValueTask<EventLogBounds> GetBoundsAsync(CancellationToken cancellation = default)
    {
        lock (_lock) return ValueTask.FromResult(new EventLogBounds(0, (ulong)_records.Count));
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
                bytes = start < (ulong)_records.Count ? _records[(int)start] : null;
                changed = _changed.Task;
            }
            if (bytes == null) { await changed.WaitAsync(cancellation); continue; }
            yield return new(start++, bytes);
        }
    }
}

internal sealed class TestTime : TimeProvider
{
    public DateTimeOffset Now = new(2026, 10, 4, 12, 0, 0, TimeSpan.Zero);
    public override DateTimeOffset GetUtcNow() => Now;
}

/// <summary>One replica: a replicated ObjectDB, the actions of this test assembly and a publisher over shared topics.</summary>
internal sealed class Node : IAsyncDisposable
{
    internal readonly InMemoryReplicationFileStorage Files;
    internal readonly BTreeKeyValueDB KeyValues;
    internal readonly ObjectDB Objects = new();
    internal readonly BackendActions Actions = new();
    internal readonly Recorder Recorder = new();
    internal EventPublisher Publisher = null!;

    Node(InMemoryReplicationFileStorage files, BTreeKeyValueDB keyValues)
    {
        Files = files;
        KeyValues = keyValues;
    }

    internal static async Task<Node> StartAsync(MemoryTopic data, MemoryTopic metadata, TimeProvider time,
        int batch = 32, InMemoryReplicationFileStorage? files = null)
    {
        files ??= new();
        var capture = new TransactionLogCapture();
        var keyValues = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = new LocalReplicatedCollection(files), TransactionLogCapture = capture,
            RequireExplicitTransactions = true, CompactorScheduler = null, Compression = new NoCompressionStrategy()
        });
        var node = new Node(files, keyValues);
        node.Objects.Open(keyValues, false, new DBOptions().WithoutAutoRegistrationOfRelations());
        node.Recorder.Register(node.Actions);
        node.Actions.RegisterTypes(node.Objects);
        await node.Objects.InitializeRelations([.. BackendActions.RelationTypes]);
        var dispatcher = new EventDispatcher();
        dispatcher.Register<PlaceOrder>(new AppHandler(node));
        node.Actions.RegisterEvents(dispatcher);
        node.Publisher = new(node.Objects, data, metadata, dispatcher,
            new() { TimeProvider = time, MaximumBatchSize = batch }, capture);
        await node.Publisher.StartAsync();
        return node;
    }

    internal T[] Read<TRelation, T>() where TRelation : class, IRelation<T> where T : class
    {
        using var transaction = Objects.StartReadOnlyTransaction();
        return transaction.GetRelation<TRelation>().ToArray();
    }

    internal QueuedAction[] Queued => Read<IQueuedActions, QueuedAction>();
    internal DelayedAction[] Delayed => Read<IDelayedActions, DelayedAction>();
    internal ActionInputIndex[] Indexes => Read<IActionInputIndexes, ActionInputIndex>();

    /// <summary>Replica fixtures share the native genesis, including the DB GUID, with separate local files.</summary>
    internal InMemoryReplicationFileStorage CopyFiles()
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
            var writer = new MemWriter(copy.ImportFile(file.Index, hint).GetAppenderWriter());
            writer.WriteBlock(bytes);
            writer.Flush();
        }
        return copy;
    }

    internal byte[] NativeTrl()
    {
        var result = new List<byte>();
        foreach (var file in Files.Enumerate().Where(f => Files.GetFileType(f.Index) == KVFileType.TransactionLog)
                     .OrderBy(f => f.Index))
        {
            var bytes = new byte[checked((int)file.GetSize())];
            file.RandomRead(bytes, 0, false);
            result.AddRange(bytes);
        }
        return result.ToArray();
    }

    public async ValueTask DisposeAsync()
    {
        await Publisher.DisposeAsync();
        Objects.Dispose();
        KeyValues.Dispose();
    }
}

/// <summary>Enqueues or cancels from inside a consume, as an application handler does.</summary>
public class PlaceOrder : PureEvent
{
    public object? Input { get; set; }
    public ActionPriority Priority { get; set; } = ActionPriority.Normal;
    public DateTime? StartAt { get; set; }
    public ulong CancelTenantId { get; set; }
    public ulong CancelActionId { get; set; }
}

internal sealed class AppHandler(Node node) : IEventHandler<PlaceOrder>
{
    public void Validate(PlaceOrder ev) { }

    public void Consume(PlaceOrder ev)
    {
        if (ev.Input is IActionInput input)
            node.Recorder.Enqueued.Enqueue(node.Actions.Enqueue(input,
                new() { Priority = ev.Priority, StartAt = ev.StartAt }));
        if (ev.CancelActionId != 0)
            node.Recorder.Cancelled.Enqueue(node.Actions.Cancel(new(ev.CancelTenantId, ev.CancelActionId)));
    }
}

[Generate]
public class OrderResult
{
    public string Text { get; set; } = "";
}

[BackendAction(Cpu = 50, MaxConcurrent = 2)]
public class ProcessOrder : ActionInput<OrderResult>
{
    public int Amount { get; set; }
}

[BackendAction(Name = "refresh")]
public class RefreshCache : ActionInput<OrderResult>, IActionInputUnificator
{
    public string Key { get; set; } = "";
    public string GetUniqueKey() => Key;
}

public class CollectItems : ActionInput<OrderResult>, IActionInputAggregator
{
    public string Key { get; set; } = "";
    public List<int> Items { get; set; } = new();
    public string GetUniqueKey() => Key;

    public bool TryAggregate(IActionInputAggregator other)
    {
        var items = ((CollectItems)other).Items;
        if (Items.Count + items.Count > 3) return false;
        Items.AddRange(items);
        return true;
    }
}

public class ValidatedOrder : ActionInput<OrderResult>
{
    public bool Reject { get; set; }
}

internal sealed record Outcome(string Name, QualifiedActionId Id, ActionOutcomeKind Kind, ulong Attempt, string? Result,
    string? Error, string Input);

/// <summary>Handlers that record every callback on this replica.</summary>
internal sealed class Recorder
{
    internal readonly ConcurrentQueue<QualifiedActionId> Enqueued = new();
    internal readonly ConcurrentQueue<bool> Cancelled = new();
    internal readonly ConcurrentQueue<Outcome> Outcomes = new();
    internal readonly ConcurrentQueue<QualifiedActionId> BeforeStarts = new();

    internal void Register(BackendActions actions) => actions
        .Register(new Handler<ProcessOrder>(this, i => i.Amount.ToString()))
        .Register(new Handler<RefreshCache>(this, i => i.Key))
        .Register(new Handler<CollectItems>(this, i => string.Join(",", i.Items)))
        .Register(new ValidatingHandler(this));

    internal void Record<TInput>(TInput input, ActionOutcome<OrderResult> outcome, string text) where TInput : IActionInput =>
        Outcomes.Enqueue(new(typeof(TInput).Name, new(input.TenantId, input.ActionId), outcome.Kind, outcome.Attempt,
            outcome.Result?.Text, outcome.Error?.Code, text));

    internal class Handler<TInput>(Recorder recorder, Func<TInput, string> describe) : IActionHandler<TInput, OrderResult>
        where TInput : ActionInput<OrderResult>
    {
        protected Recorder Owner => recorder;

        public Task<OrderResult> ExecuteAsync(TInput input, IActionContext context) =>
            Task.FromResult(new OrderResult { Text = describe(input) });

        public void OnFinished(TInput input, ActionOutcome<OrderResult> outcome) =>
            recorder.Record(input, outcome, describe(input));
    }

    sealed class ValidatingHandler(Recorder recorder) : Handler<ValidatedOrder>(recorder, i => i.Reject.ToString()),
        IActionWithBeforeStart<ValidatedOrder>
    {
        public void OnBeforeStart(ValidatedOrder input)
        {
            Owner.BeforeStarts.Enqueue(new(input.TenantId, input.ActionId));
            if (input.Reject) throw new InvalidOperationException("Rejected before start.");
        }
    }
}
