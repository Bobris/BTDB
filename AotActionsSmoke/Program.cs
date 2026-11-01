using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using BTDB;
using BTDB.BackendActions;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.Replication.EventLog;
using BTDB.Replication.EventSourcing;

namespace AotActionsSmoke;

[BackendAction(Cpu = 50, MaxConcurrent = 2)]
public class Square : ActionInput<Number>
{
    public long Value { get; set; }
}

[Generate]
public class Number
{
    public long Value { get; set; }
}

public class SquareHandler : IActionHandler<Square, Number>
{
    public Task<Number> ExecuteAsync(Square input, IActionContext context) =>
        Task.FromResult(new Number { Value = input.Value * input.Value });

    public void OnFinished(Square input, ActionOutcome<Number> outcome) =>
        Program.Outcomes.Enqueue($"square {input.ActionId} {input.Value} {outcome.Kind} {outcome.Attempt} {outcome.Result?.Value}");
}

/// <summary>Returns its result in parts.</summary>
public class Chunked : ActionInput<Number>
{
    public int Parts { get; set; }
}

public class ChunkedHandler : IActionHandler<Chunked, Number>
{
    public async Task<Number> ExecuteAsync(Chunked input, IActionContext context)
    {
        for (var part = 1; part <= input.Parts; part++) await context.SubmitPartialAsync(new Number { Value = part });
        return new() { Value = 0 };
    }

    public void OnFinished(Chunked input, ActionOutcome<Number> outcome) =>
        Program.Outcomes.Enqueue($"chunked {outcome.Kind} {outcome.Result?.Value}");
}

public class Failing : ActionInput<Number>;

public class FailingHandler : IActionHandler<Failing, Number>
{
    public Task<Number> ExecuteAsync(Failing input, IActionContext context) =>
        throw new BackendActionException("Smoke", "Failed on purpose.");

    public void OnFinished(Failing input, ActionOutcome<Number> outcome) =>
        Program.Outcomes.Enqueue($"failing {outcome.Kind} {outcome.Error?.Code}");
}

/// <summary>Inputs with one key merge into one queued action.</summary>
[BackendAction(SingleRunningPerTenant = true)]
public class Sum : ActionInput<Number>, IActionInputAggregator
{
    public List<long> Values { get; set; } = new();
    public byte[] GetUniqueKey() => "sum"u8.ToArray();

    public bool TryAggregate(IActionInputAggregator other)
    {
        Values.AddRange(((Sum)other).Values);
        return true;
    }
}

public class SumHandler : IActionHandler<Sum, Number>
{
    public Task<Number> ExecuteAsync(Sum input, IActionContext context) =>
        Task.FromResult(new Number { Value = input.Values.Sum() });

    public void OnFinished(Sum input, ActionOutcome<Number> outcome) =>
        Program.Outcomes.Enqueue($"sum {outcome.Kind} {outcome.Result?.Value}");
}

/// <summary>Registered by hand: the generator skips generic handlers, so its metadata comes from the attributes.</summary>
[BackendAction(Name = "smoke-by-hand"), RepeatedlyScheduled(60)]
public class ByHand : ActionInput<Number>;

public class ByHandHandler<T> : IActionHandler<ByHand, Number>
{
    public Task<Number> ExecuteAsync(ByHand input, IActionContext context) => Task.FromResult(new Number());
    public void OnFinished(ByHand input, ActionOutcome<Number> outcome) => Program.Outcomes.Enqueue("by-hand " + outcome.Kind);
}

/// <summary>An application event that enqueues inside its consume.</summary>
public class AddToSum : PureEvent
{
    public List<long> Values { get; set; } = new();
}

sealed class AddToSumHandler(BackendActions actions) : IEventHandler<AddToSum>
{
    public void Validate(AddToSum ev) { }

    public void Consume(AddToSum ev)
    {
        foreach (var value in ev.Values) actions.Enqueue(new Sum { Values = [value] });
    }
}

static class Program
{
    internal static readonly ConcurrentQueue<string> Outcomes = new();
    static int _failures;

    static async Task<int> Main()
    {
        Storage.Run();
        using var stopping = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        using var files = new InMemoryFileCollection();
        using var keyValues = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = files, RequireExplicitTransactions = true, CompactorScheduler = null,
            Compression = new NoCompressionStrategy()
        });
        using var objects = new ObjectDB();
        objects.Open(keyValues, false, new DBOptions().WithoutAutoRegistrationOfRelations());

        var actions = new BackendActions().RegisterGenerated().Register(new ByHandHandler<int>());
        actions.RegisterTypes(objects);
        await objects.InitializeRelations([.. BackendActions.RelationTypes]);
        var dispatcher = new EventDispatcher();
        dispatcher.Register<AddToSum>(new AddToSumHandler(actions));
        actions.RegisterEvents(dispatcher);
        await using var publisher = new EventPublisher(objects, new MemoryTopic("data"), new MemoryTopic("metadata"),
            dispatcher);
        await publisher.StartAsync(stopping.Token);

        // Cutover first: an action of the old system that ran twice before.
        await actions.ImportAsync(publisher, [
            new ImportedAction
            {
                ActionId = 1000, Input = new Square { Value = 9 }, Order = 1, QueuedAt = DateTime.UtcNow, RunAttempts = 2
            }
        ], cancellation: stopping.Token);

        var host = new BackendActionsHost(actions, publisher, new LoopbackCluster(), new BackendActionsOptions
        {
            NodeId = "smoke", ApplicationGeneration = 1, PollTimeout = TimeSpan.FromMilliseconds(200),
            RetryDelay = TimeSpan.FromMilliseconds(50), PlanInterval = TimeSpan.FromMilliseconds(20)
        });
        var running = host.RunAsync(stopping.Token);

        try
        {
            var square = await actions.EnqueueAsync(publisher, new Square { Value = 3 }, cancellation: stopping.Token);
            Check("new IDs follow the imported ones", true, square.ActionId > 1000);
            await actions.EnqueueAsync(publisher, new Chunked { Parts = 2 }, cancellation: stopping.Token);
            await actions.EnqueueAsync(publisher, new Failing(), cancellation: stopping.Token);
            await actions.EnqueueAsync(publisher, new Echo { Payload = Shapes.Sample() }, cancellation: stopping.Token);
            await publisher.PublishAsync(new AddToSum { Values = [1, 2, 3] }, stopping.Token);
            var later = new ActionEnqueueOptions { StartAt = DateTime.UtcNow.AddDays(1) };
            var byHand = await actions.EnqueueAsync(publisher, new ByHand(), later, stopping.Token);
            using (publisher.StartQuery())
                Check("hand registration reads the attributes", "smoke-by-hand", actions.FindAction(byHand)?.Name);
            Check("a delayed action cancels", true, await actions.CancelAsync(publisher, byHand, stopping.Token));

            string[] expected =
            [
                "chunked Completed 0", "chunked Partial 1", "chunked Partial 2", "echo Completed same", "failing Failed Smoke",
                $"square 1000 9 Completed 3 81", $"square {square.ActionId} 3 Completed 1 9", "sum Completed 6"
            ];
            var watch = Stopwatch.StartNew();
            while (Outcomes.Count < expected.Length && watch.Elapsed < TimeSpan.FromSeconds(30))
                await Task.Delay(20, stopping.Token);
            Check("outcomes", string.Join(", ", expected), string.Join(", ", Outcomes.Order(StringComparer.Ordinal)));
            var overview = await host.GetOverviewAsync(stopping.Token);
            Check("overview workers", "smoke", string.Join(",", overview.Workers.Select(w => w.NodeId)));
            using (publisher.StartQuery())
                Check("nothing left", 0, actions.ListActions().Count());
        }
        catch (Exception error)
        {
            // A NativeAOT gap surfaces as an exception; print it before disposal can fail too.
            _failures++;
            Console.WriteLine("FAIL " + error);
        }
        finally
        {
            await stopping.CancelAsync();
            try { await running; }
            catch (OperationCanceledException) { }
        }
        Console.WriteLine(_failures == 0 ? "BackendActions NativeAOT smoke passed." : $"{_failures} checks failed.");
        return _failures == 0 ? 0 : 1;
    }

    internal static void Step(string name, Action check)
    {
        try
        {
            check();
            Console.WriteLine("ok   " + name);
        }
        catch (Exception error)
        {
            _failures++;
            Console.WriteLine($"FAIL {name}: {error}");
        }
    }

    static void Check<TValue>(string name, TValue expected, TValue actual)
    {
        if (Equals(expected, actual))
        {
            Console.WriteLine($"ok   {name}: {actual}");
            return;
        }
        _failures++;
        Console.WriteLine($"FAIL {name}: expected {expected}, actual {actual}");
    }
}

/// <summary>A single node that is always the leader; requests to the leader are served in process.</summary>
sealed class LoopbackCluster : IBackendActionsCluster
{
    readonly ConcurrentDictionary<string, Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>>> _handlers = new();

    public async Task RunWhileLeaderAsync(Func<CancellationToken, Task> action, CancellationToken cancellation)
    {
        try { await Task.Run(() => action(cancellation), cancellation); }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
        catch (Exception error) { Console.WriteLine("Coordinator failed: " + error); throw; }
        await Task.Delay(Timeout.Infinite, cancellation);
    }

    public IDisposable Serve(string channel,
        Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> handler)
    {
        _handlers.TryAdd(channel, handler);
        return new Registration(() => _handlers.TryRemove(channel, out _));
    }

    public async ValueTask<ReadOnlyMemory<byte>> SendAsync(string channel, ReadOnlyMemory<byte> request,
        CancellationToken cancellation)
    {
        if (!_handlers.TryGetValue(channel, out var handler)) throw new IOException("No leader serves " + channel);
        try
        {
            return await Task.Run(async () => await handler(request, cancellation), cancellation);
        }
        catch (Exception error) when (error is not (IOException or OperationCanceledException))
        {
            // As the replication leader channel does; printed, because a NativeAOT gap surfaces here.
            Console.WriteLine($"Leader handler of {channel} failed: {error}");
            throw new IOException($"The leader handler of channel '{channel}' failed.", error);
        }
    }

    sealed class Registration(Action dispose) : IDisposable
    {
        public void Dispose() => dispose();
    }
}

sealed class MemoryTopic(string name) : IEventTopic
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
