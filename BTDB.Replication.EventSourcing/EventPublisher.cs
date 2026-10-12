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
    /// <summary>Serialized bytes of events read ahead of consumption, including those of the batch being consumed.
    /// One event always fits, however large.</summary>
    public long BufferedBytes { get; init; } = 64 * 1024 * 1024;
    /// <summary>Largest serialized event; a larger one fails with <see cref="EventTooLargeException"/> before its data
    /// publication. Keep it at most the data topic's EventLog <see cref="EventLogOptions.MaxRecordSize"/>.</summary>
    public int MaximumEventSize { get; init; } = new EventLogOptions().MaxRecordSize;
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
    public Func<Guid> NewPublicationId { get; init; } = Guid.NewGuid;
    public ITypeNameMapper? TypeNameMapper { get; init; }
    public EventCluster? Cluster { get; init; }

    /// <summary>Classifies an exception from <c>Consume</c>. A fatal one is probably not deterministic (out of memory,
    /// I/O, cancellation): the publisher stops and retries the event after restart. Any other exception counts as
    /// deterministic, so every node gets it: the event's transaction rolls back, only the cursor advances, and its
    /// local publication fails with the exception.</summary>
    public Func<Exception, bool> IsFatalConsumeException { get; init; } = DefaultIsFatalConsumeException;

    /// <summary>Called on every node for an event whose <c>Consume</c> failed deterministically, before its batch is
    /// visible. An exception from the callback stops the publisher.</summary>
    public Action<Event, Exception>? OnConsumeFailed { get; init; }

    /// <summary>Called with the event ID and reason for every event skipped by a leader.json decision.</summary>
    public Action<ulong, string>? OnEventSkipped { get; init; }

    /// <summary>Detects events whose consumption never completes and skips them on every node.</summary>
    public EventPoisonOptions? Poison { get; init; }

    public static bool DefaultIsFatalConsumeException(Exception error) => error switch
    {
        OutOfMemoryException or InsufficientExecutionStackException or OperationCanceledException or IOException
            or TimeoutException or ObjectDisposedException or ThreadInterruptedException => true,
        AggregateException aggregate => aggregate.InnerExceptions.Any(DefaultIsFatalConsumeException),
        _ => error.InnerException is { } inner && DefaultIsFatalConsumeException(inner)
    };
}

/// <summary>An event whose serialized form exceeds <see cref="EventPublisherOptions.MaximumEventSize"/>; nothing of it was
/// published to the data topic.</summary>
public sealed class EventTooLargeException(Type eventType, int size, int maximumSize)
    : Exception($"Event {eventType.Name} serializes to {size} bytes, more than the maximum event size {maximumSize}.")
{
    public Type EventType { get; } = eventType;
    public int Size { get; } = size;
    public int MaximumSize { get; } = maximumSize;
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
    TaskCompletionSource _metadataChanged = NewSignal();
    ulong _metadataNext;
    readonly object _bufferLock = new();
    long _bufferedBytes;
    TaskCompletionSource _bufferReleased = NewSignal();
    readonly IEventDispatcher _dispatcher;
    readonly EventPublisherOptions _options;
    readonly TransactionLogCapture? _capture;
    readonly ConcurrentDictionary<Guid, Pending> _pending = new();
    readonly EventAttemptJournal? _attempts;
    Dictionary<ulong, string> _skips = new();
    ulong _windowEnd;
    bool _windowDurable;
    ulong _canonicalEventId;
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
        if (_options.MaximumBatchSize <= 0 || _options.BufferedEvents <= 0 || _options.BufferedBytes <= 0 ||
            _options.MaximumEventSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(options));
        ArgumentNullException.ThrowIfNull(_options.TimeProvider);
        ArgumentNullException.ThrowIfNull(_options.NewPublicationId);
        ArgumentNullException.ThrowIfNull(_options.IsFatalConsumeException);
        if (_options.Poison is { } poison)
        {
            if (poison.Attempts <= 0 || poison.DecisionRetryInterval <= TimeSpan.Zero || poison.DecisionTimeout <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(options));
            if (capture == null)
                throw new ArgumentException("Skipping poison events needs the database's TransactionLogCapture.", nameof(capture));
            _attempts = new(poison.AttemptStorage,
                $"event-attempts/{Uri.EscapeDataString(dataTopic.Name)}/{Uri.EscapeDataString(poison.NodeId)}",
                poison.Attempts);
        }
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
                        if (bytes.Length > _options.MaximumEventSize)
                            throw new EventTooLargeException(stored.Event.GetType(), bytes.Length, _options.MaximumEventSize);
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

    /// <summary>Records an operator decision in leader.json to skip an event, for example one that stops every
    /// publisher. Call it on the active leader: false means the write was rejected (not the leader, or a concurrent
    /// change), so retry. Publishers apply decisions when they start: restart them afterwards. Never skip an event some
    /// node already consumed.</summary>
    public static ValueTask<bool> SkipEventAsync(ReplicationApplicationData applicationData, ulong eventId, string reason,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(applicationData);
        return SkipEventAsync(new ApplicationDataSkipDecisions(applicationData), eventId, reason, cancellation);
    }

    internal static ValueTask<bool> SkipEventAsync(IEventSkipDecisions decisions, ulong eventId, string reason,
        CancellationToken cancellation)
    {
        ArgumentOutOfRangeException.ThrowIfZero(eventId);
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return EventSkips.TryUpdateAsync(decisions, (eventId, reason), 0, TimeSpan.FromSeconds(1), TimeProvider.System,
            cancellation);
    }

    static async Task<TResult> ResultAsync<TResult>(Task<object?> task) where TResult : class =>
        (TResult)(await task.ConfigureAwait(false) ?? throw new InvalidOperationException("The handler returned no result."));

    async Task RunAsync()
    {
        Task? reading = null, metadataReading = null;
        try
        {
            using (var read = Database.StartReadOnlyTransaction()) _lastApplied = read.GetCommitUlong();
            UpdateProgress();
            var metadataBounds = await _metadataTopic.GetBoundsAsync(_lifetime).ConfigureAwait(false);
            if (metadataBounds.First != 0)
                throw new EventLogOffsetNotAvailableException("Retain the complete Layer2 metadata topic.");
            metadataReading = ReadMetadataAsync();
            await WaitForMetadataAsync(metadataBounds.Next, _lifetime).ConfigureAwait(false);
            var bounds = await _topic.GetBoundsAsync(_lifetime).ConfigureAwait(false);
            if (_lastApplied < bounds.First || _lastApplied > bounds.Next)
                throw new EventLogOffsetNotAvailableException("The database cursor is outside retained EventLog history.");
            _replayEnd = bounds.Next;
            if (_options.Poison is { } poison)
            {
                _skips = EventSkips.Parse((await poison.Decisions.ReadAsync(_lifetime).ConfigureAwait(false)).Value);
                await _attempts!.LoadAsync(_lifetime).ConfigureAwait(false);
                _windowEnd = checked(_lastApplied + (ulong)_options.MaximumBatchSize);
            }
            if (_lastApplied == _replayEnd) await ReadyAsync().ConfigureAwait(false);
            var channel = Channel.CreateBounded<(ulong Offset, StoredEvent? Stored, int Size)>(new BoundedChannelOptions(_options.BufferedEvents)
                { SingleReader = true, SingleWriter = true });
            reading = ReadAsync(channel.Writer, _lastApplied);
            while (await channel.Reader.WaitToReadAsync(_lifetime).ConfigureAwait(false))
            {
                var completed = new List<(Pending Pending, object? Result, Exception? Error)>();
                var postCommit = new List<Action>();
                var cursor = _lastApplied;
                Exception? failure = null;
                try
                {
                    for (var count = 0; count < _options.MaximumBatchSize && !AtWindowEnd(cursor) &&
                                        channel.Reader.TryRead(out var record); count++)
                    {
                        _lifetime.ThrowIfCancellationRequested();
                        var (offset, stored, size) = record;
                        if (offset != cursor) throw new InvalidDataException("Nonconsecutive event input.");
                        var eventId = checked(offset + 1);
                        var skip = stored == null ? _skips[eventId] : null;
                        if (skip == null && _attempts != null && offset < _windowEnd &&
                            await _attempts.BeforeAttemptAsync(offset, _lifetime).ConfigureAwait(false))
                        {
                            var options = _options.Poison!;
                            skip = await DecideSkipAsync(eventId,
                                $"Consumption on node '{options.NodeId}' did not complete in {options.Attempts} attempts.")
                                .ConfigureAwait(false);
                        }
                        if (skip != null)
                        {
                            await CommitCursorAsync(eventId).ConfigureAwait(false);
                            if (stored != null && _pending.TryGetValue(stored.PublicationId, out var skipped))
                                completed.Add((skipped, null, new EventSkippedException(eventId, skip)));
                            _options.OnEventSkipped?.Invoke(eventId, skip);
                        }
                        else
                        {
                            var ev = stored!.Event;
                            ev.EventId = eventId;
                            object? result = null;
                            Exception? rejected = null;
                            using (var transaction = await Database.StartWritingTransaction(eventId, inBatch: true).ConfigureAwait(false))
                            {
                                using var scope = Ambient.StartConsume(this, transaction, ev, offset < _replayEnd);
                                try { result = _dispatcher.Consume(ev); }
                                catch (Exception error) when (!_options.IsFatalConsumeException(error)) { rejected = error; }
                                if (rejected == null)
                                {
                                    transaction.Commit();
                                    postCommit.AddRange(scope.PostCommit);
                                }
                            }
                            if (rejected != null)
                            {
                                // Every node gets the same exception: discard the event's writes, keep its position.
                                await CommitCursorAsync(eventId).ConfigureAwait(false);
                                _options.OnConsumeFailed?.Invoke(ev, rejected);
                            }
                            if (_pending.TryGetValue(stored.PublicationId, out var pending))
                                completed.Add((pending, result, rejected));
                        }
                        cursor = eventId;
                        ReleaseBuffer(size);
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
                foreach (var (pending, result, error) in completed)
                    if (error != null) pending.Completion.TrySetException(error);
                    else pending.Completion.TrySetResult(result);
                if (failure != null) ExceptionDispatchInfo.Throw(failure);
                if (AtWindowEnd(cursor))
                {
                    // Leave the attempt window only once a restart would restore beyond it.
                    var window = _options.Poison!;
                    await window.WaitForCanonicalAsync(Progress!.Value, _lifetime).ConfigureAwait(false);
                    _windowDurable = true;
                    _canonicalEventId = cursor;
                    await PruneSkipsAsync(window).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested && Volatile.Read(ref _fault) == null)
        {
            _ready.TrySetCanceled(_lifetime);
        }
        catch (Exception error)
        {
            // Cancellation caused by an earlier fault (input reader) reports that root cause.
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
            if (_options.Cluster is { } cluster) await cluster.StopAsync().ConfigureAwait(false);
        }

        bool AtWindowEnd(ulong cursor) => _attempts != null && !_windowDurable && cursor == _windowEnd;

        async Task ReadyAsync()
        {
            if (_options.Cluster is { } cluster) await cluster.RegisterAsync(_lifetime).ConfigureAwait(false);
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

    static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

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
                    _metadataChanged = NewSignal();
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
    async Task ReadAsync(ChannelWriter<(ulong Offset, StoredEvent? Stored, int Size)> writer, ulong start)
    {
        try
        {
            await foreach (var record in _topic.ReadAsync(start, null, _lifetime).ConfigureAwait(false))
            {
                var size = record.Payload.Length;
                await ReserveBufferAsync(size).ConfigureAwait(false);
                // A skipped event is never deserialized: its record may be what crashes the process.
                var stored = _skips.ContainsKey(record.Offset + 1) ? null
                    : await DeserializeAsync(record.Payload).ConfigureAwait(false);
                await writer.WriteAsync((record.Offset, stored, size), _lifetime).ConfigureAwait(false);
            }
            writer.TryComplete(new EndOfStreamException("The data topic stopped following live records."));
        }
        catch (Exception error) { writer.TryComplete(error); }
    }

    // Only the active leader records the decision. A follower retries until a leader recorded one, or stops after
    // DecisionTimeout: when the leader consumed the event, the restart restores canonical history beyond it.
    async Task<string> DecideSkipAsync(ulong eventId, string reason)
    {
        var poison = _options.Poison!;
        using var timeout = new CancellationTokenSource(poison.DecisionTimeout, _options.TimeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_lifetime, timeout.Token);
        try
        {
            while (true)
            {
                var snapshot = await poison.Decisions.ReadAsync(linked.Token).ConfigureAwait(false);
                if (EventSkips.Parse(snapshot.Value).TryGetValue(eventId, out var decided)) return decided;
                if (await EventSkips.TryUpdateAsync(poison.Decisions, (eventId, reason), _canonicalEventId,
                        poison.DecisionRetryInterval, _options.TimeProvider, linked.Token).ConfigureAwait(false))
                    return reason;
                await Task.Delay(poison.DecisionRetryInterval, _options.TimeProvider, linked.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested && !_lifetime.IsCancellationRequested)
        {
            throw new TimeoutException($"No active leader recorded a skip decision for event {eventId}.");
        }
    }

    // Decisions at or below the canonical cut are never needed again. Only the leader writes; others leave them.
    async Task PruneSkipsAsync(EventPoisonOptions poison)
    {
        try
        {
            await EventSkips.TryUpdateAsync(poison.Decisions, null, _canonicalEventId, poison.DecisionRetryInterval,
                _options.TimeProvider, _lifetime).ConfigureAwait(false);
        }
        catch (IOException) { } // Best effort: a later start prunes again.
    }

    async Task CommitCursorAsync(ulong eventId)
    {
        using var transaction = await Database.StartWritingTransaction(eventId, inBatch: true).ConfigureAwait(false);
        transaction.Commit();
    }

    // An empty buffer admits any event, so one larger than BufferedBytes still progresses.
    async Task ReserveBufferAsync(int size)
    {
        while (true)
        {
            Task released;
            lock (_bufferLock)
            {
                if (_bufferedBytes == 0 || _bufferedBytes + size <= _options.BufferedBytes)
                {
                    _bufferedBytes += size;
                    return;
                }
                released = _bufferReleased.Task;
            }
            await released.WaitAsync(_lifetime).ConfigureAwait(false);
        }
    }

    void ReleaseBuffer(int size)
    {
        TaskCompletionSource released;
        lock (_bufferLock)
        {
            _bufferedBytes -= size;
            released = _bufferReleased;
            _bufferReleased = NewSignal();
        }
        released.TrySetResult();
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
