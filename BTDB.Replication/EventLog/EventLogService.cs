using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

/// <summary>
/// One node's event log: every topic publishes through its current owner, reached in process or through
/// <see cref="IEventLogPeerTransport"/>, and this node serves the topics it owns to its peers. A node takes over a
/// topic when the topic has no owner, when the recorded owner is this endpoint's previous session, or when the owner
/// stayed unreachable for <see cref="EventLogOptions.OwnerTimeout"/>. Followers read storage only to catch up and while
/// the owner is unreachable; live records and heartbeats come from the owner.
/// </summary>
public sealed class EventLogService : IEventLog, IEventLogPeerHandler, IAsyncDisposable
{
    readonly ConcurrentDictionary<string, EventLogTopic> _topics = new(StringComparer.Ordinal);
    readonly CancellationTokenSource _disposal = new();

    public EventLogService(IEventLogStorage storage, IEventLogPeerTransport transport, string endpoint,
        IReplicationScheduler scheduler, EventLogOptions? options = null)
    {
        Storage = storage;
        Transport = transport;
        Endpoint = endpoint;
        Scheduler = scheduler;
        Options = options ?? new EventLogOptions();
    }

    internal IEventLogStorage Storage { get; }
    internal IEventLogPeerTransport Transport { get; }
    public string Endpoint { get; }
    internal IReplicationScheduler Scheduler { get; }
    internal EventLogOptions Options { get; }
    internal CancellationToken Disposal => _disposal.Token;

    public IEventTopic GetTopic(string name) => Topic(name);

    internal EventLogTopic Topic(string name)
    {
        ObjectDisposedException.ThrowIf(_disposal.IsCancellationRequested, this);
        if (_topics.TryGetValue(name, out var topic)) return topic;
        EventLogFormat.ValidateTopic(name);
        Options.Validate(name);
        return _topics.GetOrAdd(name, n => new(this, n));
    }

    ValueTask<EventLogSubmitResponse> IEventLogPeerHandler.SubmitAsync(EventLogSubmitRequest request,
        CancellationToken cancellation) => Topic(request.Topic).ServeSubmitAsync(request, cancellation);

    ValueTask<EventLogBoundsResponse> IEventLogPeerHandler.GetBoundsAsync(string topic, CancellationToken cancellation) =>
        Topic(topic).ServeBoundsAsync(cancellation);

    IAsyncEnumerable<EventLogLiveMessage> IEventLogPeerHandler.SubscribeAsync(string topic, ulong from,
        CancellationToken cancellation) => Topic(topic).ServeSubscribeAsync(from, cancellation);

    /// <summary>The owner session this node currently holds for a topic, if any; for diagnostics and tests.</summary>
    public bool IsOwner(string topic) => _topics.TryGetValue(topic, out var t) && t.IsOwner;

    public async ValueTask DisposeAsync()
    {
        if (_disposal.IsCancellationRequested) return;
        _disposal.Cancel();
        foreach (var topic in _topics.Values) await topic.DisposeAsync().ConfigureAwait(false);
        _disposal.Dispose();
    }
}

internal sealed class EventLogTopic : IEventTopic, IAsyncDisposable
{
    sealed class Outstanding(ulong sequence, byte[] record)
    {
        public readonly ulong Sequence = sequence;
        public readonly byte[] Record = record;
        public readonly TaskCompletionSource<ulong> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool Dispatched;
        public ulong ResumeFrom;
    }

    sealed class Subscriber
    {
        public readonly System.Threading.Channels.Channel<EventLogLiveMessage> Channel =
            System.Threading.Channels.Channel.CreateBounded<EventLogLiveMessage>(
                new System.Threading.Channels.BoundedChannelOptions(1024) { SingleReader = true });

        public void Offer(EventLogLiveMessage message)
        {
            if (!Channel.Writer.TryWrite(message)) Channel.Writer.TryComplete(); // overflow: catch up from storage
        }
    }

    readonly EventLogService _service;
    readonly EventLogStorageReader _reader;
    readonly EventLogOptions _options;
    readonly object _lock = new();
    readonly SemaphoreSlim _resolve = new(1);
    readonly ulong _session;
    readonly Queue<Outstanding> _outstanding = new();
    readonly LinkedList<EventLogCommittedBatch> _cache = new();
    readonly List<Subscriber> _subscribers = [];
    ulong _nextSequence = 1;
    bool _publishing;
    ulong _knownNext;
    EventLogOwnerLane? _lane;
    string? _ownerEndpoint;
    string? _suspectEndpoint;
    TimeSpan? _unreachableSince;
    long _cacheBytes;
    ulong _cacheStart;
    bool _committedSinceHeartbeat;
    IDisposable? _heartbeat;
    IDisposable? _mergeTimer;
    CancellationTokenSource? _mergeCancellation;
    readonly EventLogMerger _merger;

    public EventLogTopic(EventLogService service, string name)
    {
        _service = service;
        Name = name;
        _options = service.Options;
        _session = _options.NewSessionId();
        _reader = new(service.Storage, name, _options.MergeLevels);
        _merger = new(service.Storage, name, _options, service.Scheduler);
    }

    public string Name { get; }
    internal EventLogStorageReader Reader => _reader;

    public bool IsOwner
    {
        get { lock (_lock) return _lane is { IsStopped: false, IsInstalled: true }; }
    }

    TimeSpan Now => _service.Scheduler.Elapsed;

    // ----- Publishing -----

    public ValueTask<ulong> PublishAsync(ReadOnlyMemory<byte> record, CancellationToken cancellation = default)
    {
        if (record.Length > _options.MaxRecordSize)
            throw new EventLogRecordTooLargeException($"Record of {record.Length} bytes exceeds {_options.MaxRecordSize}.");
        var outstanding = new Outstanding(0, record.ToArray());
        bool start;
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_service.Disposal.IsCancellationRequested, this);
            outstanding = new(_nextSequence++, outstanding.Record);
            _outstanding.Enqueue(outstanding);
            start = !_publishing;
            _publishing = true;
        }
        if (start) _ = PublishLoopAsync();
        return new(outstanding.Completion.Task.WaitAsync(cancellation));
    }

    async Task PublishLoopAsync()
    {
        var disposal = _service.Disposal;
        while (true)
        {
            List<Outstanding> round;
            lock (_lock)
            {
                if (_outstanding.Count == 0 || disposal.IsCancellationRequested)
                {
                    _publishing = false;
                    break;
                }
                round = [];
                foreach (var item in _outstanding)
                {
                    if (round.Count == _options.MaxBatchRecords) break;
                    round.Add(item);
                }
            }
            try
            {
                var (lane, endpoint) = await ResolveOwnerAsync(true, disposal).ConfigureAwait(false);
                if (lane != null) await SubmitLocalAsync(lane, round, disposal).ConfigureAwait(false);
                else if (endpoint != null) await SubmitRemoteAsync(endpoint, round, disposal).ConfigureAwait(false);
                else await _service.Scheduler.DelayAsync(_options.ContentionBackoff, "event log owner wait", disposal)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (disposal.IsCancellationRequested) { }
            catch (Exception)
            {
                // Storage or takeover trouble: keep the records outstanding and try again.
                try
                {
                    await _service.Scheduler.DelayAsync(_options.ContentionBackoff, "event log publish retry", disposal)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) { }
            }
        }
        if (disposal.IsCancellationRequested) FailOutstanding(new ObjectDisposedException(nameof(EventLogService)));
    }

    void FailOutstanding(Exception reason)
    {
        List<Outstanding> failed;
        lock (_lock)
        {
            failed = [.. _outstanding];
            _outstanding.Clear();
        }
        foreach (var item in failed) item.Completion.TrySetException(reason);
    }

    (bool PreviouslyDispatched, ulong ResumeFrom) PrepareRound(List<Outstanding> round)
    {
        lock (_lock)
        {
            var previously = false;
            var resumeFrom = _knownNext;
            foreach (var item in round)
            {
                if (item.Dispatched)
                {
                    previously = true;
                    resumeFrom = Math.Min(resumeFrom, item.ResumeFrom);
                }
                else
                {
                    item.Dispatched = true;
                    item.ResumeFrom = _knownNext;
                }
            }
            return (previously, resumeFrom);
        }
    }

    void Resolve(Outstanding item, ulong offset)
    {
        lock (_lock)
        {
            if (_outstanding.TryPeek(out var head) && head == item) _outstanding.Dequeue();
            _knownNext = Math.Max(_knownNext, offset + 1);
        }
        item.Completion.TrySetResult(offset);
    }

    async Task SubmitLocalAsync(EventLogOwnerLane lane, List<Outstanding> round, CancellationToken cancellation)
    {
        var (previously, resumeFrom) = PrepareRound(round);
        var oldest = round[0].Sequence;
        var tasks = new Task<ulong>[round.Count];
        for (var i = 0; i < round.Count; i++)
            tasks[i] = lane.SubmitAsync(_session, round[i].Sequence, round[i].Record, oldest, previously, resumeFrom,
                cancellation).AsTask();
        for (var i = 0; i < round.Count; i++)
        {
            try
            {
                Resolve(round[i], await tasks[i].ConfigureAwait(false));
            }
            catch (EventLogOwnerLostException)
            {
                OwnerLost(lane);
                return;
            }
            catch (EventLogSequenceGapException)
            {
                return;
            }
        }
    }

    async Task SubmitRemoteAsync(string endpoint, List<Outstanding> round, CancellationToken cancellation)
    {
        var (previously, resumeFrom) = PrepareRound(round);
        var records = new ReadOnlyMemory<byte>[round.Count];
        for (var i = 0; i < round.Count; i++) records[i] = round[i].Record;
        EventLogSubmitResponse response;
        try
        {
            response = await _service.Transport.SubmitAsync(endpoint,
                new(Name, _session, round[0].Sequence, round[0].Sequence, previously, resumeFrom, records),
                cancellation).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellation.IsCancellationRequested)
        {
            PeerFailed(endpoint);
            await _service.Scheduler.DelayAsync(_options.ContentionBackoff, "event log peer retry", cancellation)
                .ConfigureAwait(false);
            return;
        }
        PeerAnswered(endpoint, response.DurableNext);
        switch (response.Status)
        {
            case EventLogSubmitStatus.Committed:
                if (response.Offsets.Count != round.Count) throw new InvalidOperationException("Wrong number of offsets.");
                for (var i = 0; i < round.Count; i++) Resolve(round[i], response.Offsets[i]);
                break;
            case EventLogSubmitStatus.NotOwner:
                lock (_lock)
                    if (_ownerEndpoint == endpoint)
                        _ownerEndpoint = response.OwnerEndpoint == _service.Endpoint ? null : response.OwnerEndpoint;
                break;
            case EventLogSubmitStatus.SequenceGap:
                await _service.Scheduler.DelayAsync(_options.ContentionBackoff, "event log gap retry", cancellation)
                    .ConfigureAwait(false);
                break;
        }
    }

    void PeerFailed(string endpoint)
    {
        lock (_lock)
        {
            if (_ownerEndpoint != endpoint) return;
            _unreachableSince ??= Now;
            if (Now - _unreachableSince.Value < _options.OwnerTimeout) return;
            _suspectEndpoint = endpoint;
            _ownerEndpoint = null;
            _unreachableSince = null;
        }
    }

    void PeerAnswered(string endpoint, ulong durableNext)
    {
        lock (_lock)
        {
            if (_ownerEndpoint == endpoint) _unreachableSince = null;
            _knownNext = Math.Max(_knownNext, durableNext);
        }
    }

    // ----- Ownership -----

    /// <summary>The local lane when this node owns (or is taking over) the topic, else the remote owner endpoint, or
    /// neither when there is no reachable owner and this call may not take over.</summary>
    async ValueTask<(EventLogOwnerLane? Lane, string? Endpoint)> ResolveOwnerAsync(bool mayTakeOver,
        CancellationToken cancellation)
    {
        await _resolve.WaitAsync(cancellation).ConfigureAwait(false);
        try
        {
            lock (_lock)
            {
                if (_lane is { IsStopped: false } lane) return (lane, null);
                if (_ownerEndpoint != null) return (null, _ownerEndpoint);
            }
            var tail = await _reader.FindTailAsync(cancellation).ConfigureAwait(false);
            string? suspect;
            lock (_lock)
            {
                _knownNext = Math.Max(_knownNext, tail?.NextOffset ?? 0);
                suspect = _suspectEndpoint;
                if (tail?.Owner is { } owner && owner.Endpoint != _service.Endpoint && owner.Endpoint != suspect)
                {
                    _ownerEndpoint = owner.Endpoint;
                    return (null, owner.Endpoint);
                }
            }
            if (!mayTakeOver) return (null, null);
            var self = new EventLogOwner(_options.NewSessionId().ToString("x16"), _service.Endpoint);
            var candidate = await EventLogOwnerLane.CreateCandidateAsync(_service.Storage, _reader, Name, _options,
                _service.Scheduler, self, cancellation).ConfigureAwait(false);
            Attach(candidate);
            return (candidate, null);
        }
        finally
        {
            _resolve.Release();
        }
    }

    void Attach(EventLogOwnerLane lane)
    {
        lane.Committed += batch => OnCommitted(lane, batch);
        lane.Stopped += _ => OwnerLost(lane);
        lock (_lock)
        {
            _lane = lane;
            _ownerEndpoint = null;
            _suspectEndpoint = null;
            _unreachableSince = null;
            _cache.Clear();
            _cacheBytes = 0;
            _cacheStart = lane.NextOffset;
            _knownNext = Math.Max(_knownNext, lane.NextOffset);
            _mergeCancellation?.Cancel();
            _mergeCancellation = CancellationTokenSource.CreateLinkedTokenSource(_service.Disposal);
        }
        ScheduleHeartbeat(lane);
        ScheduleMerge(lane);
    }

    void ScheduleMerge(EventLogOwnerLane lane)
    {
        if (_options.MergeFanOut == 0 || _options.MergeLevels == 0) return;
        lock (_lock)
        {
            if (_lane != lane || _mergeCancellation is not { IsCancellationRequested: false } cancellation) return;
            _mergeTimer?.Dispose();
            _mergeTimer = _service.Scheduler.Schedule(_options.MergeInterval, () => _ = MergeAsync(lane, cancellation.Token),
                "event log merge");
        }
    }

    /// <summary>One background merge and cleanup pass while this node owns the topic; appends keep priority because
    /// the pass runs separately from the lane and losing ownership cancels it.</summary>
    internal async Task<EventLogMerger.PassResult?> MergeAsync(EventLogOwnerLane lane, CancellationToken cancellation)
    {
        EventLogMerger.PassResult? result = null;
        try
        {
            if (lane.IsInstalled && !lane.IsStopped) result = await _merger.RunPassAsync(cancellation).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { return null; }
        catch (Exception) { } // storage trouble: the next pass retries
        ScheduleMerge(lane);
        return result;
    }

    internal async Task<EventLogMerger.PassResult?> MergeNowAsync()
    {
        EventLogOwnerLane? lane;
        CancellationToken token;
        lock (_lock)
        {
            lane = _lane;
            token = _mergeCancellation?.Token ?? CancellationToken.None;
        }
        return lane == null ? null : await _merger.RunPassAsync(token).ConfigureAwait(false);
    }

    void OwnerLost(EventLogOwnerLane lane)
    {
        Subscriber[] subscribers;
        lock (_lock)
        {
            if (_lane != lane) return;
            _lane = null;
            _heartbeat?.Dispose();
            _heartbeat = null;
            _mergeTimer?.Dispose();
            _mergeCancellation?.Cancel();
            subscribers = [.. _subscribers];
            _subscribers.Clear();
            _cache.Clear();
            _cacheBytes = 0;
        }
        lane.Stop();
        foreach (var subscriber in subscribers)
        {
            subscriber.Offer(EventLogLiveMessage.NotOwner(null));
            subscriber.Channel.Writer.TryComplete();
        }
    }

    void OnCommitted(EventLogOwnerLane lane, EventLogCommittedBatch batch)
    {
        Subscriber[] subscribers;
        lock (_lock)
        {
            if (_lane != lane) return;
            _committedSinceHeartbeat = true;
            _knownNext = Math.Max(_knownNext, batch.FirstOffset + (ulong)batch.Records.Count);
            _cache.AddLast(batch);
            foreach (var record in batch.Records) _cacheBytes += record.Length + 16;
            while (_cacheBytes > _options.RecentCacheBytes && _cache.First is { } first)
            {
                foreach (var record in first.Value.Records) _cacheBytes -= record.Length + 16;
                _cacheStart = first.Value.FirstOffset + (ulong)first.Value.Records.Count;
                _cache.RemoveFirst();
            }
            subscribers = [.. _subscribers];
        }
        var message = new EventLogLiveMessage(EventLogLiveKind.Batch, batch.FirstOffset, batch.Records,
            batch.TailVersion, null);
        foreach (var subscriber in subscribers) subscriber.Offer(message);
    }

    void ScheduleHeartbeat(EventLogOwnerLane lane)
    {
        lock (_lock)
        {
            if (_lane != lane || _service.Disposal.IsCancellationRequested) return;
            _heartbeat?.Dispose();
            _heartbeat = _service.Scheduler.Schedule(_options.HeartbeatInterval, () => _ = HeartbeatAsync(lane),
                "event log heartbeat");
        }
    }

    async Task HeartbeatAsync(EventLogOwnerLane lane)
    {
        bool idle;
        lock (_lock)
        {
            if (_lane != lane) return;
            idle = !_committedSinceHeartbeat;
            _committedSinceHeartbeat = false;
        }
        if (idle && lane.IsInstalled)
        {
            try
            {
                var next = await lane.ValidateAsync(_service.Disposal).ConfigureAwait(false);
                Subscriber[] subscribers;
                lock (_lock) subscribers = [.. _subscribers];
                var message = new EventLogLiveMessage(EventLogLiveKind.Heartbeat, next, [], lane.TailVersion, null);
                foreach (var subscriber in subscribers) subscriber.Offer(message);
            }
            catch (EventLogOwnerLostException)
            {
                OwnerLost(lane);
                return;
            }
            catch (Exception) when (!_service.Disposal.IsCancellationRequested)
            {
                // Storage unreachable: no validated heartbeat, so subscribers time out if this persists.
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
        ScheduleHeartbeat(lane);
    }

    // ----- Serving peers -----

    internal async ValueTask<EventLogSubmitResponse> ServeSubmitAsync(EventLogSubmitRequest request,
        CancellationToken cancellation)
    {
        EventLogOwnerLane? lane;
        string? hint;
        lock (_lock)
        {
            lane = _lane is { IsStopped: false } l ? l : null;
            hint = _ownerEndpoint;
        }
        if (lane == null) return new(EventLogSubmitStatus.NotOwner, [], _knownNext, hint);
        var tasks = new Task<ulong>[request.Records.Count];
        for (var i = 0; i < tasks.Length; i++)
            tasks[i] = lane.SubmitAsync(request.Session, request.FirstSequence + (ulong)i, request.Records[i],
                request.OldestUnresolved, request.PreviouslyDispatched, request.ResumeFrom, cancellation).AsTask();
        var offsets = new ulong[tasks.Length];
        try
        {
            for (var i = 0; i < tasks.Length; i++) offsets[i] = await tasks[i].ConfigureAwait(false);
        }
        catch (EventLogOwnerLostException)
        {
            return new(EventLogSubmitStatus.NotOwner, [], lane.NextOffset, null);
        }
        catch (EventLogSequenceGapException)
        {
            return new(EventLogSubmitStatus.SequenceGap, [], lane.NextOffset, null);
        }
        return new(EventLogSubmitStatus.Committed, offsets, lane.NextOffset, null);
    }

    internal async ValueTask<EventLogBoundsResponse> ServeBoundsAsync(CancellationToken cancellation)
    {
        EventLogOwnerLane? lane;
        string? hint;
        lock (_lock)
        {
            lane = _lane is { IsStopped: false } l ? l : null;
            hint = _ownerEndpoint;
        }
        if (lane == null) return new(false, default, hint);
        try
        {
            await lane.InstallAsync().WaitAsync(cancellation).ConfigureAwait(false);
            return new(true, new(0, await lane.ValidateAsync(cancellation).ConfigureAwait(false)), null);
        }
        catch (EventLogOwnerLostException)
        {
            return new(false, default, null);
        }
    }

    internal async IAsyncEnumerable<EventLogLiveMessage> ServeSubscribeAsync(ulong from,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        Subscriber? subscriber = null;
        EventLogLiveMessage? refusal = null;
        lock (_lock)
        {
            if (_lane is not { IsStopped: false, IsInstalled: true } lane) refusal = EventLogLiveMessage.NotOwner(_ownerEndpoint);
            else if (from > lane.NextOffset) refusal = EventLogLiveMessage.NotOwner(null);
            else if (from < _cacheStart && from < lane.NextOffset)
                refusal = new(EventLogLiveKind.CatchUp, lane.NextOffset, [], lane.TailVersion, null);
            else
            {
                subscriber = new();
                foreach (var batch in _cache)
                    if (batch.FirstOffset + (ulong)batch.Records.Count > from)
                        subscriber.Offer(new(EventLogLiveKind.Batch, batch.FirstOffset, batch.Records, batch.TailVersion,
                            null));
                _subscribers.Add(subscriber);
            }
        }
        if (refusal != null)
        {
            yield return refusal;
            yield break;
        }
        try
        {
            await foreach (var message in subscriber!.Channel.Reader.ReadAllAsync(cancellation).ConfigureAwait(false))
                yield return message;
        }
        finally
        {
            lock (_lock) _subscribers.Remove(subscriber!);
        }
    }

    // ----- Reading -----

    public async ValueTask<EventLogBounds> GetBoundsAsync(CancellationToken cancellation = default)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var (lane, endpoint) = await ResolveOwnerAsync(false, cancellation).ConfigureAwait(false);
            if (lane != null)
            {
                var response = await ServeBoundsAsync(cancellation).ConfigureAwait(false);
                if (response.IsOwner) return response.Bounds;
                continue;
            }
            if (endpoint == null) break;
            try
            {
                var response = await _service.Transport.GetBoundsAsync(endpoint, Name, cancellation).ConfigureAwait(false);
                PeerAnswered(endpoint, response.Bounds.Next);
                if (response.IsOwner) return response.Bounds;
                lock (_lock)
                    if (_ownerEndpoint == endpoint) _ownerEndpoint = response.OwnerEndpoint;
            }
            catch (Exception) when (!cancellation.IsCancellationRequested)
            {
                PeerFailed(endpoint);
                break;
            }
        }
        // No reachable owner: the storage tail covers every completed receipt.
        var tail = await _reader.FindTailAsync(cancellation).ConfigureAwait(false);
        return new(0, tail?.NextOffset ?? 0);
    }

    public async IAsyncEnumerable<EventLogRecord> ReadAsync(ulong start, ulong? end = null,
        [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        if (end < start) throw new ArgumentOutOfRangeException(nameof(end));
        var bounds = await GetBoundsAsync(cancellation).ConfigureAwait(false);
        if (start < bounds.First || start > bounds.Next)
            throw new EventLogOffsetNotAvailableException(
                $"Offset {start} of topic '{Name}' is outside [{bounds.First}, {bounds.Next}].");
        var offset = start;
        var fromStorage = false;
        while (end == null || offset < end)
        {
            if (fromStorage)
            {
                // Catch up from storage only when the owner cannot serve the offset or is unreachable.
                await foreach (var record in _reader.ReadRecordsAsync(offset, end, cancellation).ConfigureAwait(false))
                {
                    if (record.Offset != offset) continue;
                    yield return record;
                    offset++;
                }
                if (offset == end) yield break;
            }
            await using var live = new LiveSubscription(this, offset, cancellation);
            var gap = false;
            while (await live.NextAsync().ConfigureAwait(false) is { } message)
            {
                if (message.Kind != EventLogLiveKind.Batch) continue;
                if (message.Offset > offset)
                {
                    gap = true;
                    break;
                }
                for (var i = 0; i < message.Records.Count; i++)
                {
                    var recordOffset = message.Offset + (ulong)i;
                    if (recordOffset < offset) continue;
                    yield return new(recordOffset, message.Records[i]);
                    offset++;
                    if (offset == end) yield break;
                }
            }
            fromStorage = gap || live.End != LiveEnd.NotOwner;
            if (live.End is LiveEnd.Unreachable or LiveEnd.NotOwner)
                await _service.Scheduler.DelayAsync(
                    live.End == LiveEnd.Unreachable ? _options.HeartbeatInterval : _options.ContentionBackoff,
                    "event log reader retry", cancellation).ConfigureAwait(false);
        }
    }

    enum LiveEnd { None, Ended, CatchUp, NotOwner, Unreachable }

    /// <summary>A live stream from the owner that ends on a gap, catch-up request, owner change, stream end, or when
    /// neither a batch nor a heartbeat arrives within <see cref="EventLogOptions.OwnerTimeout"/>.</summary>
    sealed class LiveSubscription : IAsyncDisposable
    {
        readonly EventLogTopic _topic;
        readonly ulong _from;
        readonly CancellationToken _outer;
        CancellationTokenSource? _cts;
        IAsyncEnumerator<EventLogLiveMessage>? _enumerator;
        IDisposable? _timer;
        string? _endpoint;
        bool _done;

        public LiveSubscription(EventLogTopic topic, ulong from, CancellationToken outer)
        {
            _topic = topic;
            _from = from;
            _outer = outer;
        }

        public LiveEnd End { get; private set; }

        void Arm()
        {
            _timer?.Dispose();
            var cts = _cts!;
            _timer = _topic._service.Scheduler.Schedule(_topic._options.OwnerTimeout, () =>
            {
                try { cts.Cancel(); }
                catch (ObjectDisposedException) { }
            }, "event log owner timeout");
        }

        public async ValueTask<EventLogLiveMessage?> NextAsync()
        {
            if (_done) return null;
            try
            {
                if (_enumerator == null)
                {
                    var (lane, endpoint) = await _topic.ResolveOwnerAsync(false, _outer).ConfigureAwait(false);
                    _cts = CancellationTokenSource.CreateLinkedTokenSource(_outer);
                    if (lane != null)
                        _enumerator = _topic.ServeSubscribeAsync(_from, _cts.Token).GetAsyncEnumerator(_cts.Token);
                    else if (endpoint != null)
                    {
                        _endpoint = endpoint;
                        _enumerator = _topic._service.Transport.SubscribeAsync(endpoint, _topic.Name, _from, _cts.Token)
                            .GetAsyncEnumerator(_cts.Token);
                    }
                    else
                    {
                        End = LiveEnd.Unreachable;
                        _done = true;
                        return null;
                    }
                }
                Arm();
                if (!await _enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    End = LiveEnd.Ended;
                    _done = true;
                    return null;
                }
                var message = _enumerator.Current;
                if (_endpoint != null) _topic.PeerAnswered(_endpoint, 0);
                switch (message.Kind)
                {
                    case EventLogLiveKind.NotOwner:
                        lock (_topic._lock)
                            if (_endpoint != null && _topic._ownerEndpoint == _endpoint)
                                _topic._ownerEndpoint = message.OwnerEndpoint == _topic._service.Endpoint
                                    ? null : message.OwnerEndpoint;
                        End = LiveEnd.NotOwner;
                        _done = true;
                        return null;
                    case EventLogLiveKind.CatchUp:
                        End = LiveEnd.CatchUp;
                        _done = true;
                        return null;
                }
                return message;
            }
            catch (Exception) when (!_outer.IsCancellationRequested)
            {
                if (_endpoint != null) _topic.PeerFailed(_endpoint);
                End = LiveEnd.Unreachable;
                _done = true;
                return null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            _timer?.Dispose();
            if (_enumerator != null)
            {
                _cts?.Cancel();
                try { await _enumerator.DisposeAsync().ConfigureAwait(false); }
                catch (Exception) { }
            }
            _cts?.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        EventLogOwnerLane? lane;
        Subscriber[] subscribers;
        lock (_lock)
        {
            lane = _lane;
            _lane = null;
            _heartbeat?.Dispose();
            _mergeTimer?.Dispose();
            _mergeCancellation?.Cancel();
            subscribers = [.. _subscribers];
            _subscribers.Clear();
        }
        lane?.Stop();
        foreach (var subscriber in subscribers) subscriber.Channel.Writer.TryComplete();
        FailOutstanding(new ObjectDisposedException(nameof(EventLogService)));
        await Task.CompletedTask;
    }
}
