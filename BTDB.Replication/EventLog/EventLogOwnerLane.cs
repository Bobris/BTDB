using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

/// <summary>This node does not own the topic (any more); the caller must route to the current owner.</summary>
public sealed class EventLogOwnerLostException(string message) : Exception(message);

/// <summary>A publisher sequence arrived after a gap that was not filled in time; resend from the oldest outstanding.</summary>
public sealed class EventLogSequenceGapException(string message) : Exception(message);

internal sealed record EventLogCommittedBatch(ulong FirstOffset, IReadOnlyList<ReadOnlyMemory<byte>> Records,
    string TailVersion);

/// <summary>
/// The writer of one topic while this node owns it: batches submitted records into frames, appends them to the tail with
/// one ETag-conditional write per commit, rotates splits with proactive seals and resolves ambiguous writes by repeating
/// the exact write. Every write installs this lane's owner session, so the first write of a candidate is its takeover.
/// A rejected write stops the lane for good; a successor lane on another node reconciles publishers by their sessions.
/// </summary>
internal sealed class EventLogOwnerLane
{
    sealed class Pending(ulong session, ulong sequence, ReadOnlyMemory<byte> record)
    {
        public readonly ulong Session = session;
        public readonly ulong Sequence = sequence;
        public readonly ReadOnlyMemory<byte> Record = record;
        public readonly TaskCompletionSource<ulong> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public IDisposable? GapTimer;
    }

    sealed class SessionState(ulong committed)
    {
        public ulong Committed = committed;
        public ulong Accepted = committed;
        public readonly Dictionary<ulong, ulong> Offsets = new();
        public readonly Queue<ulong> OffsetOrder = new();
        public readonly Dictionary<ulong, Pending> Pending = new();
        public readonly SortedDictionary<ulong, Pending> Held = new();

        public void Commit(ulong sequence, ulong offset)
        {
            Committed = Math.Max(Committed, sequence);
            Accepted = Math.Max(Accepted, sequence);
            if (Offsets.TryAdd(sequence, offset))
            {
                OffsetOrder.Enqueue(sequence);
                if (OffsetOrder.Count > RecentOffsets) Offsets.Remove(OffsetOrder.Dequeue());
            }
        }
    }

    sealed class FencedException(bool committed) : Exception("The topic tail changed under this owner.")
    {
        public readonly bool Committed = committed;
    }

    const int RecentOffsets = 4096;

    readonly IEventLogStorage _storage;
    readonly EventLogStorageReader _reader;
    readonly string _topic;
    readonly EventLogOptions _options;
    readonly IReplicationScheduler _scheduler;
    readonly int _headerLength;
    readonly object _lock = new();
    readonly Queue<Pending> _queue = new();
    readonly Dictionary<ulong, SessionState> _sessions = new();
    readonly TaskCompletionSource _stoppedCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly List<TaskCompletionSource<bool>> _commitWaiters = [];

    ulong _splitId;
    string _version;
    byte[] _content;
    ulong _next;
    int _frames;
    bool _sealed;
    bool _installed;
    bool _running;
    bool _installRequested;
    Exception? _stopReason;

    EventLogOwnerLane(IEventLogStorage storage, EventLogStorageReader reader, string topic, EventLogOptions options,
        IReplicationScheduler scheduler, EventLogOwner self, EventLogTail tail, bool installed)
    {
        _storage = storage;
        _reader = reader;
        _topic = topic;
        _options = options;
        _scheduler = scheduler;
        Self = self;
        _headerLength = EventLogFormat.SplitHeaderLength(topic);
        _splitId = tail.SplitId;
        _version = tail.Version;
        _content = tail.Content;
        _next = tail.NextOffset;
        _frames = tail.Object.Frames.Count;
        _sealed = tail.Sealed;
        _installed = installed;
    }

    public EventLogOwner Self { get; }

    /// <summary>Raised after every commit, in commit order, outside the lane's lock.</summary>
    public event Action<EventLogCommittedBatch>? Committed;

    public event Action<Exception>? Stopped;

    public Task Completion => _stoppedCompletion.Task;

    public bool IsStopped
    {
        get { lock (_lock) return _stopReason != null; }
    }

    public bool IsInstalled
    {
        get { lock (_lock) return _installed; }
    }

    public ulong NextOffset
    {
        get { lock (_lock) return _next; }
    }

    public string TailVersion
    {
        get { lock (_lock) return _version; }
    }

    /// <summary>
    /// Prepare to own the topic from its current tail. An empty topic is created and a sealed idle tail gets its
    /// header-only successor with the inherited owner, so the returned candidate always holds a writable split. The
    /// candidate is not installed: its first write, a commit or <see cref="InstallAsync"/>, takes over.
    /// </summary>
    public static async ValueTask<EventLogOwnerLane> CreateCandidateAsync(IEventLogStorage storage,
        EventLogStorageReader reader, string topic, EventLogOptions options, IReplicationScheduler scheduler,
        EventLogOwner self, CancellationToken cancellation)
    {
        for (var attempt = 0; ; attempt++)
        {
            if (attempt == options.TakeoverAttempts * 2)
                throw new EventLogOwnerLostException($"Topic '{topic}' tail keeps changing.");
            var tail = await reader.FindTailAsync(cancellation).ConfigureAwait(false);
            if (tail == null)
            {
                var content = EventLogFormat.CreateSplit(topic, 1, 0);
                var result = await CreateOrReconcileAsync(storage, scheduler, options, EventLogFormat.SplitKey(topic, 1),
                    content, self, cancellation).ConfigureAwait(false);
                if (result != null)
                    return new(storage, reader, topic, options, scheduler, self,
                        new(1, result, content, EventLogFormat.Parse(content, topic), self), true);
                continue;
            }
            if (tail.Sealed)
            {
                var content = EventLogFormat.CreateSplit(topic, tail.SplitId + 1, tail.NextOffset);
                await CreateOrReconcileAsync(storage, scheduler, options, EventLogFormat.SplitKey(topic, tail.SplitId + 1),
                    content, tail.Owner, cancellation).ConfigureAwait(false);
                continue;
            }
            return new(storage, reader, topic, options, scheduler, self, tail, tail.Owner == self);
        }
    }

    /// <summary>Header-only create-if-absent; returns the version when this exact content exists, else null.</summary>
    static async ValueTask<string?> CreateOrReconcileAsync(IEventLogStorage storage, IReplicationScheduler scheduler,
        EventLogOptions options, string key, byte[] content, EventLogOwner? owner, CancellationToken cancellation)
    {
        while (true)
        {
            var result = await storage.WriteAsync(key, null, content, owner, null, cancellation).ConfigureAwait(false);
            if (result.Outcome == EventLogWriteOutcome.Applied) return result.Version;
            if (result.Outcome == EventLogWriteOutcome.Rejected)
            {
                var blob = await storage.ReadAsync(key, cancellation).ConfigureAwait(false);
                return blob != null && blob.Owner == owner && blob.Content.Span.SequenceEqual(content) ? blob.Version : null;
            }
            await scheduler.DelayAsync(options.AmbiguousRetryDelay, "event log create retry", cancellation)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Take over now with a content-preserving owner change unless a commit already installed this lane.</summary>
    public Task InstallAsync()
    {
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lock)
        {
            if (_stopReason != null) return Task.FromException(new EventLogOwnerLostException(_stopReason.Message));
            if (_installed) return Task.CompletedTask;
            _installRequested = true;
            _commitWaiters.Add(completion);
        }
        Kick();
        return completion.Task;
    }

    /// <summary>
    /// Accept one record of a publisher session. Sequences of a session commit in order; a repeated sequence joins its
    /// pending commit or returns its committed offset. <paramref name="oldestUnresolved"/> is the publisher's lowest
    /// sequence without a receipt; lower sequences are resolved. When the lane does not know the session and the records
    /// may have been dispatched to a predecessor, it first installs itself and scans committed history from
    /// <paramref name="resumeFrom"/>, so a record is never committed twice.
    /// </summary>
    public async ValueTask<ulong> SubmitAsync(ulong session, ulong sequence, ReadOnlyMemory<byte> record,
        ulong oldestUnresolved, bool previouslyDispatched, ulong resumeFrom, CancellationToken cancellation)
    {
        if (record.Length > _options.MaxRecordSize)
            throw new EventLogRecordTooLargeException($"Record of {record.Length} bytes exceeds {_options.MaxRecordSize}.");
        Task<ulong> task;
        while (true)
        {
            SessionState? state;
            lock (_lock)
            {
                ThrowIfStopped();
                _sessions.TryGetValue(session, out state);
                if (sequence < oldestUnresolved || oldestUnresolved == 0)
                    throw new ArgumentOutOfRangeException(nameof(oldestUnresolved));
                if (state == null && !previouslyDispatched)
                {
                    state = new(oldestUnresolved - 1);
                    _sessions.Add(session, state);
                }
                if (state != null && (sequence > state.Committed || state.Offsets.ContainsKey(sequence)))
                {
                    task = AcceptLocked(state, session, sequence, record);
                    break;
                }
            }
            await ResolveSessionAsync(session, oldestUnresolved, resumeFrom, cancellation).ConfigureAwait(false);
            lock (_lock)
                if (_sessions.TryGetValue(session, out state) && sequence <= state.Committed &&
                    !state.Offsets.ContainsKey(sequence))
                    throw new EventLogSequenceGapException($"Sequence {sequence} was committed before {resumeFrom}.");
        }
        Kick();
        return await task.WaitAsync(cancellation).ConfigureAwait(false);
    }

    Task<ulong> AcceptLocked(SessionState state, ulong session, ulong sequence, ReadOnlyMemory<byte> record)
    {
        if (state.Offsets.TryGetValue(sequence, out var offset)) return Task.FromResult(offset);
        if (state.Pending.TryGetValue(sequence, out var pending)) return pending.Completion.Task;
        if (state.Held.TryGetValue(sequence, out pending)) return pending.Completion.Task;
        pending = new(session, sequence, record);
        if (sequence == state.Accepted + 1)
        {
            Enqueue(state, pending);
            while (state.Held.Remove(state.Accepted + 1, out var held))
            {
                held.GapTimer?.Dispose();
                Enqueue(state, held);
            }
        }
        else
        {
            state.Held.Add(sequence, pending);
            pending.GapTimer = _scheduler.Schedule(_options.OwnerTimeout, () =>
            {
                lock (_lock)
                    if (!state.Held.Remove(sequence)) return;
                pending.Completion.TrySetException(new EventLogSequenceGapException(
                    $"Sequence {sequence} of session {session:x} waited for a missing predecessor."));
            }, "event log sequence gap");
        }
        return pending.Completion.Task;
    }

    void Enqueue(SessionState state, Pending pending)
    {
        state.Accepted = pending.Sequence;
        state.Pending.Add(pending.Sequence, pending);
        _queue.Enqueue(pending);
    }

    async ValueTask ResolveSessionAsync(ulong session, ulong oldestUnresolved, ulong resumeFrom,
        CancellationToken cancellation)
    {
        await InstallAsync().WaitAsync(cancellation).ConfigureAwait(false);
        ulong end;
        lock (_lock) end = _next;
        var committed = oldestUnresolved - 1;
        var offsets = new List<(ulong Sequence, ulong Offset)>();
        if (resumeFrom < end)
            await foreach (var (_, frame) in _reader.ReadFramesAsync(resumeFrom, end, cancellation).ConfigureAwait(false))
            {
                var offset = frame.FirstOffset;
                foreach (var (s, q) in EventLogFormat.Transfers(frame))
                {
                    if (s == session && offset >= resumeFrom && q >= oldestUnresolved)
                    {
                        offsets.Add((q, offset));
                        committed = Math.Max(committed, q);
                    }
                    offset++;
                }
            }
        lock (_lock)
        {
            if (!_sessions.TryGetValue(session, out var state))
            {
                state = new(committed);
                _sessions.Add(session, state);
            }
            foreach (var (q, offset) in offsets) state.Commit(q, offset);
        }
    }

    void ThrowIfStopped()
    {
        if (_stopReason != null) throw new EventLogOwnerLostException(_stopReason.Message);
    }

    void Kick()
    {
        lock (_lock)
        {
            if (_running || _stopReason != null || _queue.Count == 0 && !_installRequested) return;
            _running = true;
        }
        _ = RunAsync();
    }

    async Task RunAsync()
    {
        while (true)
        {
            List<Pending>? batch;
            bool install;
            lock (_lock)
            {
                batch = TakeBatchLocked();
                install = batch == null && _installRequested && !_installed;
                if (batch == null && !install)
                {
                    _installRequested = false;
                    CompleteWaitersLocked(true);
                    _running = false;
                    return;
                }
            }
            try
            {
                if (batch != null) await CommitAsync(batch).ConfigureAwait(false);
                else await SetOwnerAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Stop(exception is FencedException ? new EventLogOwnerLostException(exception.Message) : exception,
                    exception is FencedException { Committed: true } ? null : batch, batch);
                return;
            }
        }
    }

    List<Pending>? TakeBatchLocked()
    {
        if (_queue.Count == 0) return null;
        var batch = new List<Pending>();
        var room = _options.SplitCap - _headerLength - EventLogFormat.SealLength - EventLogFormat.FrameLength(0, 0, 0);
        long bytes = 0;
        while (_queue.Count > 0 && batch.Count < _options.MaxBatchRecords)
        {
            var next = _queue.Peek();
            var cost = next.Record.Length + 4 + 20;
            if (batch.Count > 0 && bytes + cost > room) break;
            batch.Add(_queue.Dequeue());
            bytes += cost;
        }
        return batch;
    }

    static List<EventLogTransferRun> Runs(List<Pending> batch)
    {
        var runs = new List<EventLogTransferRun>();
        foreach (var pending in batch)
            if (runs.Count > 0 && runs[^1] is var last && last.Session == pending.Session &&
                last.FirstSequence + last.Count == pending.Sequence)
                runs[^1] = last with { Count = last.Count + 1 };
            else runs.Add(new(pending.Session, pending.Sequence, 1));
        return runs;
    }

    async Task CommitAsync(List<Pending> batch)
    {
        var records = batch.Select(p => p.Record).ToArray();
        var runs = Runs(batch);
        var frameLength = EventLogFormat.FrameLength(runs.Count, records.Length, records.Sum(r => (long)r.Length));
        var large = _headerLength + frameLength + EventLogFormat.SealLength > _options.SplitCap;
        ulong first;
        lock (_lock) first = _next;
        byte[] frame = new byte[frameLength];
        EventLogFormat.WriteFrame(frame, first, records, runs);
        var count = (ulong)batch.Count;
        if (!_sealed)
        {
            if (!large && _content.Length + frameLength + EventLogFormat.SealLength <= _options.SplitCap)
            {
                await AppendAsync(frame, count, ShouldSeal(_content.Length + frameLength)).ConfigureAwait(false);
                Complete(batch, first);
                return;
            }
            if (_frames == 0)
            {
                await AppendAsync(frame, count, true).ConfigureAwait(false);
                Complete(batch, first);
                return;
            }
            try
            {
                await AppendAsync(null, 0, true).ConfigureAwait(false);
            }
            catch (FencedException)
            {
                throw new FencedException(false); // the seal-only write carried none of this batch
            }
        }
        await CreateSuccessorAsync(frame, count, large || ShouldSeal(_headerLength + frameLength)).ConfigureAwait(false);
        Complete(batch, first);
    }

    bool ShouldSeal(int length) => _options.SplitCap - length - EventLogFormat.SealLength < _options.SealFreeSpace;

    async Task AppendAsync(byte[]? frame, ulong count, bool seal)
    {
        var length = _content.Length + (frame?.Length ?? 0) + (seal ? EventLogFormat.SealLength : 0);
        var content = new byte[length];
        _content.CopyTo(content, 0);
        var position = _content.Length;
        var next = _next + count;
        if (frame != null)
        {
            frame.CopyTo(content, position);
            position += frame.Length;
        }
        if (seal) EventLogFormat.WriteSeal(content.AsSpan(position), next, _splitId + 1);
        var version = await WriteAsync(EventLogFormat.SplitKey(_topic, _splitId), _version, content)
            .ConfigureAwait(false);
        lock (_lock)
        {
            _content = content;
            _version = version;
            _next = next;
            if (frame != null) _frames++;
            _sealed = seal;
            _installed = true;
        }
    }

    async Task CreateSuccessorAsync(byte[] frame, ulong count, bool seal)
    {
        var id = _splitId + 1;
        var content = new byte[_headerLength + frame.Length + (seal ? EventLogFormat.SealLength : 0)];
        EventLogFormat.WriteSplitHeader(content, _topic, id, _next);
        frame.CopyTo(content, _headerLength);
        var next = _next + count;
        if (seal) EventLogFormat.WriteSeal(content.AsSpan(_headerLength + frame.Length), next, id + 1);
        var key = EventLogFormat.SplitKey(_topic, id);
        string version;
        try
        {
            version = await WriteAsync(key, null, content).ConfigureAwait(false);
        }
        catch (FencedException fenced) when (!fenced.Committed)
        {
            // A helper or contender may have created the header-only successor first. It still carries our session
            // when nobody has taken it over, so append to it; otherwise this owner is fenced.
            var blob = await _storage.ReadAsync(key, CancellationToken.None).ConfigureAwait(false);
            var header = EventLogFormat.CreateSplit(_topic, id, _next);
            if (blob == null || blob.Owner != Self || !blob.Content.Span.SequenceEqual(header))
                throw;
            lock (_lock)
            {
                _splitId = id;
                _version = blob.Version;
                _content = header;
                _frames = 0;
                _sealed = false;
            }
            await AppendAsync(frame, count, seal).ConfigureAwait(false);
            return;
        }
        lock (_lock)
        {
            _splitId = id;
            _version = version;
            _content = content;
            _next = next;
            _frames = 1;
            _sealed = seal;
            _installed = true;
        }
    }

    async Task SetOwnerAsync()
    {
        var key = EventLogFormat.SplitKey(_topic, _splitId);
        var ambiguous = false;
        while (true)
        {
            var result = await _storage.SetOwnerAsync(key, _version, Self, CancellationToken.None).ConfigureAwait(false);
            if (result.Outcome == EventLogWriteOutcome.Applied)
            {
                lock (_lock)
                {
                    _version = result.Version!;
                    _installed = true;
                }
                return;
            }
            if (result.Outcome == EventLogWriteOutcome.Rejected)
            {
                if (ambiguous)
                {
                    var blob = await _storage.ReadAsync(key, CancellationToken.None).ConfigureAwait(false);
                    if (blob != null && blob.Owner == Self && blob.Content.Span.SequenceEqual(_content))
                    {
                        lock (_lock)
                        {
                            _version = blob.Version;
                            _installed = true;
                        }
                        return;
                    }
                }
                throw new FencedException(false);
            }
            ambiguous = true;
            await _scheduler.DelayAsync(_options.AmbiguousRetryDelay, "event log owner retry", CancellationToken.None)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Repeat the exact write until resolved. A rejection after an ambiguous attempt may be our own landed
    /// write: adopt it when the object is exactly our content and session, report committed frames when our content is
    /// a prefix of a contender's version, and otherwise report that the write did not land.</summary>
    async Task<string> WriteAsync(string key, string? expectedVersion, byte[] content)
    {
        var ambiguous = false;
        while (true)
        {
            var result = await _storage.WriteAsync(key, expectedVersion, content, Self, null, CancellationToken.None)
                .ConfigureAwait(false);
            if (result.Outcome == EventLogWriteOutcome.Applied) return result.Version!;
            if (result.Outcome == EventLogWriteOutcome.Rejected)
            {
                if (!ambiguous) throw new FencedException(false);
                var blob = await _storage.ReadAsync(key, CancellationToken.None).ConfigureAwait(false);
                if (blob != null && blob.Owner == Self && blob.Content.Span.SequenceEqual(content)) return blob.Version;
                throw new FencedException(blob != null && blob.Content.Span.StartsWith(content));
            }
            ambiguous = true;
            await _scheduler.DelayAsync(_options.AmbiguousRetryDelay, "event log write retry", CancellationToken.None)
                .ConfigureAwait(false);
        }
    }

    void Complete(List<Pending> batch, ulong first)
    {
        var records = new ReadOnlyMemory<byte>[batch.Count];
        string version;
        lock (_lock)
        {
            for (var i = 0; i < batch.Count; i++)
            {
                var pending = batch[i];
                records[i] = pending.Record;
                var state = _sessions[pending.Session];
                state.Pending.Remove(pending.Sequence);
                state.Commit(pending.Sequence, first + (ulong)i);
            }
            version = _version;
            CompleteWaitersLocked(true);
        }
        for (var i = 0; i < batch.Count; i++) batch[i].Completion.TrySetResult(first + (ulong)i);
        Committed?.Invoke(new(first, records, version));
    }

    void CompleteWaitersLocked(bool result)
    {
        foreach (var waiter in _commitWaiters) waiter.TrySetResult(result);
        _commitWaiters.Clear();
    }

    /// <summary>
    /// Validate that this lane still owns the tail: the tail version is unchanged and no successor was created by
    /// anybody else. A write in flight is awaited instead, because its success proves ownership. Returns the durable
    /// next offset, covering every receipt completed before the call; a fenced lane stops and throws.
    /// </summary>
    public async ValueTask<ulong> ValidateAsync(CancellationToken cancellation)
    {
        TaskCompletionSource<bool>? waiter = null;
        string version;
        ulong splitId;
        bool sealedTail;
        lock (_lock)
        {
            ThrowIfStopped();
            if (_running)
            {
                waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _commitWaiters.Add(waiter);
            }
            version = _version;
            splitId = _splitId;
            sealedTail = _sealed;
        }
        if (waiter != null)
        {
            await waiter.Task.WaitAsync(cancellation).ConfigureAwait(false);
            lock (_lock)
            {
                ThrowIfStopped();
                return _next;
            }
        }
        var properties = await _storage.GetPropertiesAsync(EventLogFormat.SplitKey(_topic, splitId), cancellation)
            .ConfigureAwait(false);
        var fenced = properties == null || properties.Version != version;
        if (!fenced && sealedTail)
            fenced = await _storage.GetPropertiesAsync(EventLogFormat.SplitKey(_topic, splitId + 1), cancellation)
                .ConfigureAwait(false) != null;
        lock (_lock)
        {
            ThrowIfStopped();
            if (_version != version || _splitId != splitId || _running) return _next; // a newer commit proves it
            if (!fenced) return _next;
        }
        Stop(new EventLogOwnerLostException($"Topic '{_topic}' was taken over."), null, null);
        throw new EventLogOwnerLostException($"Topic '{_topic}' was taken over.");
    }

    /// <summary>Stop the lane: pending and held records fail with <see cref="EventLogOwnerLostException"/> so publishers
    /// resubmit them to the next owner, which finds any that did commit.</summary>
    public void Stop(Exception? reason = null) =>
        Stop(reason ?? new EventLogOwnerLostException($"Owner of topic '{_topic}' stopped."), null, null);

    void Stop(Exception reason, List<Pending>? failedBatch, List<Pending>? batch)
    {
        List<Pending> failed;
        lock (_lock)
        {
            if (_stopReason != null) return;
            _stopReason = reason;
            failed = [.. _queue];
            _queue.Clear();
            foreach (var state in _sessions.Values)
            {
                foreach (var held in state.Held.Values)
                {
                    held.GapTimer?.Dispose();
                    failed.Add(held);
                }
                state.Held.Clear();
                state.Pending.Clear();
            }
            if (failedBatch != null) failed.AddRange(failedBatch);
            CompleteWaitersLocked(false);
            _running = false;
        }
        var lost = reason as EventLogOwnerLostException ?? new EventLogOwnerLostException(reason.Message);
        if (batch != null && failedBatch == null)
        {
            // The rejected write's content was found in canonical history: its records are committed.
            ulong first;
            lock (_lock) first = _next;
            for (var i = 0; i < batch.Count; i++) batch[i].Completion.TrySetResult(first + (ulong)i);
        }
        foreach (var pending in failed) pending.Completion.TrySetException(lost);
        _stoppedCompletion.TrySetResult();
        Stopped?.Invoke(reason);
    }
}
