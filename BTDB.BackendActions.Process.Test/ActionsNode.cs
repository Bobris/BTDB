using System.Collections.Concurrent;
using Azure.Storage.Blobs;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.Replication;
using BTDB.Replication.Azure;
using BTDB.Replication.EventLog;
using BTDB.Replication.EventSourcing;
using BTDB.Replication.Test;
using Microsoft.Extensions.DependencyInjection;

namespace BTDB.BackendActions.ProcessTests;

/// <summary>An action that sleeps; every node records the attempts it executed.</summary>
public class SleepAction : ActionInput<SleepResult>
{
    public int Milliseconds { get; set; }
}

[Generate]
public class SleepResult
{
    public string Node { get; set; } = "";
}

/// <summary>Every outcome OnFinished handled for an action, replicated like any application row.</summary>
public class Finish
{
    [PrimaryKey(1)] public ulong ActionId { get; set; }
    public ulong Attempt { get; set; }
    public ActionOutcomeKind Kind { get; set; }
    public string Node { get; set; } = "";
    public int Count { get; set; }
}

public interface IFinishes : IRelation<Finish>
{
    void Insert(Finish item);
    void Update(Finish item);
    Finish? FindByIdOrDefault(ulong actionId);
}

internal sealed record NodeState(string Role, bool Running, ulong Applied, bool Published);

internal sealed record Execution(ulong ActionId, ulong Attempt);

internal sealed class SleepHandler(string node, ConcurrentQueue<Execution> executions) : IActionHandler<SleepAction, SleepResult>
{
    public async Task<SleepResult> ExecuteAsync(SleepAction input, IActionContext context)
    {
        executions.Enqueue(new(context.Id.ActionId, context.Attempt));
        await Task.Delay(input.Milliseconds, context.Cancellation);
        return new() { Node = node };
    }

    public void OnFinished(SleepAction input, ActionOutcome<SleepResult> outcome)
    {
        var finishes = Ambient.Relation<IFinishes>();
        if (finishes.FindByIdOrDefault(input.ActionId) is { } finish)
        {
            finish.Count++;
            finish.Attempt = outcome.Attempt;
            finish.Kind = outcome.Kind;
            finish.Node = outcome.Result?.Node ?? "";
            finishes.Update(finish);
        }
        else
            finishes.Insert(new()
            {
                ActionId = input.ActionId, Attempt = outcome.Attempt, Kind = outcome.Kind,
                Node = outcome.Result?.Node ?? "", Count = 1
            });
    }
}

/// <summary>
/// One node of the qualification cluster: a replicated ObjectDB restored from Azurite, an EventPublisher over the
/// cluster's EventLog topics and a BackendActionsHost. A node with published history starts its application right
/// after restore; the genesis leader starts it after preparing the schema.
/// </summary>
internal sealed class ActionsNode(string nodeId, string endpoint, BlobContainerClient container, string directory,
    BackendActionsOptions actionOptions) : IReplicationNodeHost, IAsyncDisposable
{
    static readonly TrlSuccessor Genesis = new("1.trl", 1);
    static readonly Type[] Relations = [typeof(IFinishes), .. BackendActions.RelationTypes];

    readonly OnDiskReplicationFileStorage _files = new(directory);
    readonly TransactionLogCapture _capture = new();
    readonly AzureReplicationStorage _canonical = new(container, "main");
    readonly SemaphoreSlim _schema = new(1);
    readonly object _progressLock = new();
    readonly CancellationTokenSource _stopping = new();
    readonly TaskCompletionSource<IServiceProvider> _services = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal readonly ConcurrentQueue<Execution> Executions = new();
    readonly BackendActions _actions = new();
    ReplicationFileSet? _collection;
    BTreeKeyValueDB? _db;
    ObjectDB? _objects;
    LeaderTrlProgress? _progress;
    EventPublisher? _publisher;
    Task? _application;
    int _started;

    internal ReplicationNodeRole Role { get; private set; } = ReplicationNodeRole.Restoring;
    internal EventPublisher? Publisher => Volatile.Read(ref _publisher);
    internal BackendActions Actions => _actions;

    /// <summary>Registers the action types; call once before the replication host starts.</summary>
    internal void Attach(IServiceProvider services)
    {
        _services.TrySetResult(services);
    }

    // Source-generated registration; the handler needs this node's state, so it comes from a factory.
    internal void RegisterActions() => _actions.RegisterGenerated(_ => new SleepHandler(nodeId, Executions));

    public async ValueTask<IReadOnlyList<ActivationDatabase>> RestoreAsync(CancellationToken cancellation)
    {
        _objects?.Dispose();
        _objects = null;
        _db?.Dispose();
        _db = null;
        if (_collection != null) { await _collection.DisposeAsync(); _collection = null; }
        var restored = false;
        IFileCollection files;
        if (await _canonical.ResolveRecoveryRootAsync(Genesis, cancellation) == Genesis &&
            await _canonical.ReadAsync(Genesis.Key, cancellation) == null)
            files = new LocalReplicatedCollection(_files);
        else
        {
            var inventory = await CanonicalTrlInventory.DiscoverAsync(_canonical, Genesis, cancellation);
            _collection = new(_files, _canonical.Bind(inventory));
            await _collection.InitializeAsync(cancellation);
            files = _collection;
            restored = true;
        }
        _db = await BTreeKeyValueDB.OpenAsync(new KeyValueDBOptions
        {
            FileCollection = files, TransactionLogCapture = _capture, CompactorScheduler = null,
            Compression = new NoCompressionStrategy(), RequireExplicitTransactions = true
        }, cancellation);
        _objects = new ObjectDB();
        _objects.Open(_db, false, new DBOptions().WithoutAutoRegistrationOfRelations());
        _actions.RegisterTypes(_objects);
        if (restored)
        {
            var position = _db.ReplicationRestoredPosition;
            using (var read = _db.StartReadOnlyTransaction())
                lock (_progressLock) _progress = new(read.GetCommitUlong(), position.FileId, position.Offset);
            StartApplication(); // the restored history contains the schema
        }
        return [new("main", _db, _capture, _canonical, Genesis, restored ? _db.ReplicationRestoredPosition : default,
            id => $"{id}.trl")];
    }

    public async ValueTask PrepareSchemaAsync(ActivationDatabase database, LeaseAuthority authority,
        CancellationToken cancellation)
    {
        if (!authority.IsValid) throw new IOException("Leader authority ended before schema preparation.");
        await InitializeRelationsAsync(cancellation);
        var end = _capture.Completed;
        if (end.FileId != 0 && Publisher == null)
            using (var read = _db!.StartReadOnlyTransaction())
                lock (_progressLock) _progress = new(read.GetCommitUlong(), end.FileId, end.Offset);
        StartApplication();
    }

    async Task InitializeRelationsAsync(CancellationToken cancellation)
    {
        await _schema.WaitAsync(cancellation);
        try
        {
            await _objects!.InitializeRelations(Relations);
        }
        finally { _schema.Release(); }
    }

    void StartApplication()
    {
        if (Interlocked.Exchange(ref _started, 1) == 0) _application = RunApplicationAsync();
    }

    async Task RunApplicationAsync()
    {
        try
        {
            var services = await _services.Task;
            await InitializeRelationsAsync(_stopping.Token); // read-only: the schema is already in the database
            var dispatcher = new EventDispatcher();
            _actions.RegisterEvents(dispatcher);
            var log = services.GetRequiredService<IEventLog>();
            var publisher = new EventPublisher(_objects!, log.GetTopic("data"), log.GetTopic("metadata"), dispatcher,
                new(), _capture);
            await publisher.StartAsync(_stopping.Token);
            Volatile.Write(ref _publisher, publisher);
            Console.WriteLine("APPLICATION " + nodeId);
            var host = new BackendActionsHost(_actions, publisher, services.GetRequiredService<ReplicationLeadership>(),
                services.GetRequiredService<ReplicationLeaderChannel>(), actionOptions);
            await host.RunAsync(_stopping.Token);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested) { }
        catch (Exception error)
        {
            Console.Error.WriteLine("APPLICATION FAILED " + error);
        }
    }

    public LeaderCandidate CreateCandidate() =>
        new("cluster", nodeId, Guid.NewGuid().ToString("N"), 1, ["main"], endpoint, Guid.NewGuid().ToString("N"));

    public LeaderTrlProgress? GetProgress(string database)
    {
        if (Publisher?.Progress is { } progress) return progress;
        lock (_progressLock) return _progress;
    }

    public void RequestRestart(string reason) => Console.Error.WriteLine("RESTART " + reason);

    public void RequestFatalRestart(string reason)
    {
        Console.Error.WriteLine("FATAL " + reason);
        Environment.Exit(75);
    }

    public void ReportStatus(ReplicationNodeRole role) => Role = role;
    public void DatabaseRemoved(string database) { }
    public ValueTask<ulong> CaptureInitializationCursorAsync(string database, CancellationToken cancellation) =>
        ValueTask.FromResult(0ul);

    internal NodeState State(ReplicationStatus status) => new(Role.ToString(), Publisher != null,
        Publisher?.LastAppliedEventId ?? 0, status.Current.Databases.FirstOrDefault()?.Published != null);

    internal async Task<List<QualifiedActionId>> EnqueueAsync(int count, int milliseconds)
    {
        var publisher = Publisher ?? throw new IOException("The application is not running.");
        var ids = new List<QualifiedActionId>();
        for (var i = 0; i < count; i++)
            ids.Add(await _actions.EnqueueAsync(publisher, new SleepAction { Milliseconds = milliseconds }));
        return ids;
    }

    internal Finish[] Finishes()
    {
        var publisher = Publisher ?? throw new IOException("The application is not running.");
        using var query = publisher.StartQuery();
        return Ambient.Relation<IFinishes>().ToArray();
    }

    public async ValueTask DisposeAsync()
    {
        _stopping.Cancel();
        if (_application != null) await _application;
        if (_publisher != null) await _publisher.DisposeAsync();
        _objects?.Dispose();
        _db?.Dispose();
        if (_collection != null) await _collection.DisposeAsync();
        _files.Dispose();
    }
}
