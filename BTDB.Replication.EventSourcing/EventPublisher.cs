using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using BTDB.EventStoreLayer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.Replication.EventLog;

namespace BTDB.Replication.EventSourcing;

public sealed record EventPublisherOptions
{
    public int MaximumBatchSize { get; init; } = 32;
    public int BufferedEvents { get; init; } = 256;
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
    public Func<Guid> NewPublicationId { get; init; } = Guid.NewGuid;
    public ITypeNameMapper? TypeNameMapper { get; init; }
    public EventCluster? Cluster { get; init; }
}

/// <summary>Replays ordered data and metadata EventLog topics into an already restored and schema-initialized ObjectDB.
/// Does not own the database or topic; stop this publisher before disposing either.</summary>
public sealed class EventPublisher : IEventPublisher, IAsyncDisposable
{
    sealed class Pending(Event original)
    {
        internal readonly Event Original = original;
        internal readonly TaskCompletionSource<object?> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    readonly IEventTopic _topic;
    readonly IEventTopic _metadataTopic;
    readonly Layer2EventCodec _codec;
    readonly object _metadataLock = new();
    readonly SemaphoreSlim _publishing = new(1);
    TaskCompletionSource _metadataChanged = NewMetadataSignal();
    ulong _metadataNext;
    readonly IEventDispatcher _dispatcher;
    readonly EventPublisherOptions _options;
    readonly TransactionLogCapture? _capture;
    readonly ConcurrentDictionary<Guid, Pending> _pending = new();
    readonly CancellationTokenSource _stop = new();
    readonly CancellationToken _lifetime;
    readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly object _progressLock = new();
    LeaderTrlProgress? _progress;
    Task _run = Task.CompletedTask;
    Exception? _fault;
    ulong _lastApplied;
    ulong _replayEnd;
    int _started, _disposed;

    public EventPublisher(IObjectDB database, IEventTopic dataTopic, IEventTopic metadataTopic, IEventDispatcher dispatcher,
        EventPublisherOptions? options = null, TransactionLogCapture? capture = null)
    {
        Database = database ?? throw new ArgumentNullException(nameof(database));
        _topic = dataTopic ?? throw new ArgumentNullException(nameof(dataTopic));
        _metadataTopic = metadataTopic ?? throw new ArgumentNullException(nameof(metadataTopic));
        if (dataTopic.Name == metadataTopic.Name)
            throw new ArgumentException("Data and metadata must use different topics.", nameof(metadataTopic));
        _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        _options = options ?? new();
        if (_options.MaximumBatchSize <= 0 || _options.BufferedEvents <= 0)
            throw new ArgumentOutOfRangeException(nameof(options));
        ArgumentNullException.ThrowIfNull(_options.TimeProvider);
        ArgumentNullException.ThrowIfNull(_options.NewPublicationId);
        if (dispatcher is EventDispatcher typed) typed.Freeze();
        _codec = new(_options.TypeNameMapper);
        _capture = capture;
        _lifetime = _stop.Token;
        _options.Cluster?.Attach();
    }

    internal IObjectDB Database { get; }
    public ulong LastAppliedEventId => Volatile.Read(ref _lastApplied);
    /// <summary>Atomic complete batch cut for IReplicationNodeHost.GetProgress, when a capture was supplied.</summary>
    public LeaderTrlProgress? Progress { get { lock (_progressLock) return _progress; } }
    /// <summary>Faults when replay, consumption or input fails. Hosts must observe it and arrange restart.</summary>
    public Task Completion => _run;
    public Ambient.Scope StartQuery() => Ambient.StartQuery(this);
    public EventClusterSnapshot Cluster => (_options.Cluster ??
        throw new InvalidOperationException("Configure EventPublisherOptions.Cluster for node discovery.")).Current;

    /// <summary>Captures the durable input end, replays to it and then follows live events. May be called once.</summary>
    public Task StartAsync(CancellationToken cancellation = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Interlocked.CompareExchange(ref _started, 1, 0) != 0)
            throw new InvalidOperationException("The publisher is already started.");
        using (ExecutionContext.SuppressFlow()) _run = Task.Run(RunAsync);
        return _ready.Task.WaitAsync(cancellation);
    }

    // These entry points deliberately are not async: Validate finishes on the caller's stack.
    public ValueTask PublishAsync(PureEvent ev, CancellationToken cancellation = default)
    {
        var (rejection, task) = Publish(ev, cancellation);
        return rejection != null ? ValueTask.CompletedTask : new(task!);
    }

    public Task<TResult> PublishAndWaitAsync<TResult>(EventWithResult<TResult> ev,
        CancellationToken cancellation = default) where TResult : class
    {
        var (rejection, task) = Publish(ev, cancellation);
        return rejection != null ? Task.FromResult((TResult)rejection) : ResultAsync<TResult>(task!);
    }

    (object? Rejection, Task<object?>? Task) Publish(Event ev, CancellationToken cancellation)
    {
        ArgumentNullException.ThrowIfNull(ev);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _fault) is { } fault) throw new InvalidOperationException("Event consumption failed.", fault);
        if (!_ready.Task.IsCompletedSuccessfully) throw new InvalidOperationException("Await StartAsync before publishing.");
        cancellation.ThrowIfCancellationRequested();
        ev.EventId = 0;
        ev.When = _options.TimeProvider.GetUtcNow().UtcDateTime;
        using var scope = StartQuery();
        var rejection = _dispatcher.Validate(ev);
        if (rejection != null) return (rejection, null);
        var stored = new StoredEvent { PublicationId = _options.NewPublicationId(), Event = ev };
        var pending = new Pending(ev);
        if (stored.PublicationId == Guid.Empty || !_pending.TryAdd(stored.PublicationId, pending))
            throw new InvalidOperationException("Publication IDs must be nonempty and unique.");
        return (null, PublishStoredAsync(stored, pending, cancellation));
    }

    async Task<object?> PublishStoredAsync(StoredEvent stored, Pending pending, CancellationToken cancellation)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellation, _lifetime);
        try
        {
            ValueTask<ulong> writing;
            await _publishing.WaitAsync(linked.Token).ConfigureAwait(false);
            try
            {
                while (true)
                {
                    byte[] bytes;
                    bool metadata;
                    lock (_metadataLock) bytes = _codec.Serialize(stored, out metadata);
                    if (!metadata)
                    {
                        writing = _topic.PublishAsync(bytes, linked.Token);
                        break;
                    }
                    var metadataOffset = await _metadataTopic.PublishAsync(bytes, linked.Token).ConfigureAwait(false);
                    await WaitForMetadataAsync(checked(metadataOffset + 1), linked.Token).ConfigureAwait(false);
                }
            }
            finally { _publishing.Release(); }
            var offset = await writing.ConfigureAwait(false);
            pending.Original.EventId = checked(offset + 1);
            // RunAsync resolves every pending result. Linking this wait to _lifetime can cancel
            // an already committed prefix before its asynchronous result continuation runs after a later failure.
            return await pending.Completion.Task.WaitAsync(cancellation).ConfigureAwait(false);
        }
        finally { _pending.TryRemove(stored.PublicationId, out _); }
    }

    static async Task<TResult> ResultAsync<TResult>(Task<object?> task) where TResult : class =>
        (TResult)(await task.ConfigureAwait(false) ?? throw new InvalidOperationException("The handler returned no result."));

    async Task RunAsync()
    {
        Task? reading = null, metadataReading = null, clusterWatching = null;
        try
        {
            using (var read = Database.StartReadOnlyTransaction()) _lastApplied = read.GetCommitUlong();
            UpdateProgress();
            if (_options.Cluster is { } cluster) await cluster.RefreshAsync(_lifetime).ConfigureAwait(false);
            var metadataBounds = await _metadataTopic.GetBoundsAsync(_lifetime).ConfigureAwait(false);
            if (metadataBounds.First != 0)
                throw new EventLogOffsetNotAvailableException("Retain the complete Layer2 metadata topic.");
            metadataReading = ReadMetadataAsync();
            await WaitForMetadataAsync(metadataBounds.Next, _lifetime).ConfigureAwait(false);
            var bounds = await _topic.GetBoundsAsync(_lifetime).ConfigureAwait(false);
            if (_lastApplied < bounds.First || _lastApplied > bounds.Next)
                throw new EventLogOffsetNotAvailableException("The database cursor is outside retained EventLog history.");
            _replayEnd = bounds.Next;
            if (_lastApplied == _replayEnd) await ReadyAsync().ConfigureAwait(false);
            var channel = Channel.CreateBounded<(ulong Offset, StoredEvent Stored)>(new BoundedChannelOptions(_options.BufferedEvents)
                { SingleReader = true, SingleWriter = true });
            reading = ReadAsync(channel.Writer, _lastApplied);
            while (await channel.Reader.WaitToReadAsync(_lifetime).ConfigureAwait(false))
            {
                var completed = new List<(Pending Pending, object? Result)>();
                var postCommit = new List<Action>();
                var cursor = _lastApplied;
                Exception? failure = null;
                try
                {
                    for (var count = 0; count < _options.MaximumBatchSize && channel.Reader.TryRead(out var record); count++)
                    {
                        _lifetime.ThrowIfCancellationRequested();
                        var (offset, stored) = record;
                        if (offset != cursor) throw new InvalidDataException("Nonconsecutive event input.");
                        var ev = stored.Event;
                        ev.EventId = checked(offset + 1);
                        using (var transaction = await Database.StartWritingTransaction(ev.EventId, inBatch: true).ConfigureAwait(false))
                        {
                            using var scope = Ambient.StartConsume(this, transaction, ev, offset < _replayEnd);
                            var result = _dispatcher.Consume(ev);
                            transaction.Commit();
                            postCommit.AddRange(scope.PostCommit);
                            if (_pending.TryGetValue(stored.PublicationId, out var pending)) completed.Add((pending, result));
                        }
                        cursor = ev.EventId;
                    }
                }
                catch (Exception error) { failure = error; }
                Database.FinishTransactionBatchAfterCurrentTransaction();
                Volatile.Write(ref _lastApplied, cursor);
                UpdateProgress();
                if (failure == null && cursor >= _replayEnd && !_ready.Task.IsCompleted)
                    await ReadyAsync().ConfigureAwait(false);
                foreach (var action in postCommit)
                {
                    try
                    {
                        using var scope = StartQuery();
                        action();
                    }
                    catch (Exception error)
                    {
                        failure = failure == null ? error : new AggregateException(failure, error);
                    }
                }
                foreach (var (pending, result) in completed) pending.Completion.TrySetResult(result);
                if (failure != null) ExceptionDispatchInfo.Throw(failure);
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested && Volatile.Read(ref _fault) == null)
        {
            _ready.TrySetCanceled(_lifetime);
        }
        catch (Exception error)
        {
            // Cancellation caused by an earlier fault (input reader, cluster) reports that root cause.
            Fail(error);
            ExceptionDispatchInfo.Throw(Volatile.Read(ref _fault)!);
        }
        finally
        {
            _stop.Cancel();
            foreach (var pending in _pending.Values)
                pending.Completion.TrySetException(_fault ?? new OperationCanceledException("Event publisher stopped."));
            if (metadataReading != null) await metadataReading.ConfigureAwait(false);
            if (reading != null)
                try { await reading.ConfigureAwait(false); }
                catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
            if (clusterWatching != null) await clusterWatching.ConfigureAwait(false);
            if (_options.Cluster is { } cluster) await cluster.StopAsync().ConfigureAwait(false);
        }

        async Task ReadyAsync()
        {
            if (_options.Cluster is { } cluster)
            {
                await cluster.BeatAsync(_lifetime).ConfigureAwait(false);
                clusterWatching = WatchClusterAsync(cluster);
            }
            _ready.TrySetResult();
        }
    }

    // The first fault wins; later failures are usually consequences of the cancellation it triggers.
    void Fail(Exception error)
    {
        Interlocked.CompareExchange(ref _fault, error, null);
        _ready.TrySetException(error);
        _stop.Cancel();
    }

    async Task WatchClusterAsync(EventCluster cluster)
    {
        try { await cluster.RunAsync(_lifetime).ConfigureAwait(false); }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception error) { Fail(error); }
    }

    static TaskCompletionSource NewMetadataSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    async Task ReadMetadataAsync()
    {
        try
        {
            await foreach (var record in _metadataTopic.ReadAsync(0, null, _lifetime).ConfigureAwait(false))
            {
                TaskCompletionSource applied;
                lock (_metadataLock)
                {
                    if (record.Offset != _metadataNext) throw new InvalidDataException("Nonconsecutive metadata input.");
                    _codec.ApplyMetadata(record.Payload);
                    _metadataNext = checked(record.Offset + 1);
                    applied = _metadataChanged;
                    _metadataChanged = NewMetadataSignal();
                }
                applied.TrySetResult();
            }
            throw new EndOfStreamException("The metadata topic stopped following live records.");
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (Exception error) { Fail(error); }
    }

    async Task WaitForMetadataAsync(ulong next, CancellationToken cancellation)
    {
        while (true)
        {
            Task changed;
            lock (_metadataLock)
            {
                if (_metadataNext >= next) return;
                changed = _metadataChanged.Task;
            }
            await changed.WaitAsync(cancellation).ConfigureAwait(false);
        }
    }

    async Task<StoredEvent> DeserializeAsync(ReadOnlyMemory<byte> bytes)
    {
        lock (_metadataLock)
            if (_codec.TryDeserialize(bytes, out var available)) return available!;
        // Every data publication follows its durable metadata publication. Catch up to the metadata end observed
        // after this data record; still missing descriptors then mean corruption, not an indefinite live wait.
        var bounds = await _metadataTopic.GetBoundsAsync(_lifetime).ConfigureAwait(false);
        await WaitForMetadataAsync(bounds.Next, _lifetime).ConfigureAwait(false);
        lock (_metadataLock)
            return _codec.TryDeserialize(bytes, out var stored) ? stored!
                : throw new InvalidDataException("Event metadata is missing from the metadata topic.");
    }

    // Deserializes ahead of consumption so it overlaps with the consumer's transactions. EventDeserializer copies
    // all event data (including ReadOnlyMemory<byte> fields), so the payload need not outlive the enumerator step.
    async Task ReadAsync(ChannelWriter<(ulong Offset, StoredEvent Stored)> writer, ulong start)
    {
        try
        {
            await foreach (var record in _topic.ReadAsync(start, null, _lifetime).ConfigureAwait(false))
            {
                var stored = await DeserializeAsync(record.Payload).ConfigureAwait(false);
                await writer.WriteAsync((record.Offset, stored), _lifetime).ConfigureAwait(false);
            }
            writer.TryComplete(new EndOfStreamException("The data topic stopped following live records."));
        }
        catch (Exception error) { writer.TryComplete(error); }
    }

    void UpdateProgress()
    {
        if (_capture == null) return;
        var position = _capture.Completed;
        lock (_progressLock) _progress = new(_lastApplied, position.FileId, position.Offset);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _stop.Cancel();
        try { await _run.ConfigureAwait(false); }
        finally { _stop.Dispose(); }
    }
}
