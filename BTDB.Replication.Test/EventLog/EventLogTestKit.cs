using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Replication.EventLog;

namespace BTDB.Replication.Test.EventLog;

/// <summary>Virtual time: a delay elapses immediately on the thread pool unless the scheduler is manual, in which case
/// callbacks run when <see cref="Advance"/> passes their due time.</summary>
internal sealed class TestScheduler(bool manual = false) : IReplicationScheduler
{
    sealed class Work(long due, Action callback) : IDisposable
    {
        public readonly long Due = due;
        public readonly Action Callback = callback;
        public bool Cancelled;
        public void Dispose() => Cancelled = true;
    }

    readonly object _lock = new();
    readonly List<Work> _work = [];
    long _elapsed;

    public TimeSpan Elapsed
    {
        get { lock (_lock) return TimeSpan.FromTicks(_elapsed); }
    }

    public IDisposable Schedule(TimeSpan delay, Action callback, string description)
    {
        lock (_lock)
        {
            var work = new Work(_elapsed + delay.Ticks, callback);
            if (manual) _work.Add(work);
            else
                ThreadPool.QueueUserWorkItem(_ =>
                {
                    if (work.Cancelled) return;
                    lock (_lock) _elapsed = Math.Max(_elapsed, work.Due);
                    callback();
                });
            return work;
        }
    }

    public void Advance(TimeSpan duration)
    {
        long until;
        lock (_lock) until = _elapsed + duration.Ticks;
        while (true)
        {
            Work? next;
            lock (_lock)
            {
                _work.RemoveAll(w => w.Cancelled);
                next = _work.Where(w => w.Due <= until).MinBy(w => w.Due);
                if (next == null)
                {
                    _elapsed = until;
                    return;
                }
                _work.Remove(next);
                _elapsed = Math.Max(_elapsed, next.Due);
            }
            next.Callback();
        }
    }
}

public enum StorageFault
{
    None,
    /// <summary>The write takes effect but the response is lost.</summary>
    LoseResponse,
    /// <summary>The request times out without taking effect.</summary>
    TimeoutWithoutEffect,
}

internal sealed record StorageOperation(string Kind, string Key, string? ExpectedVersion);

/// <summary>Fault-injecting wrapper around <see cref="InMemoryEventLogStorage"/>.</summary>
internal sealed class FaultyEventLogStorage(TimeProvider? time = null) : IEventLogStorage
{
    public readonly InMemoryEventLogStorage Inner = new(time);
    readonly List<StorageOperation> _operations = [];

    /// <summary>Decide a fault for each mutation; called before the mutation.</summary>
    public Func<StorageOperation, StorageFault>? Fault { get; set; }

    /// <summary>Runs before a mutation reaches storage, e.g. to let a competitor write first.</summary>
    public Func<StorageOperation, ValueTask>? BeforeMutation { get; set; }

    /// <summary>Runs after a mutation took effect and before its response is returned.</summary>
    public Func<StorageOperation, ValueTask>? AfterMutation { get; set; }

    public IReadOnlyList<StorageOperation> Operations
    {
        get { lock (_operations) return _operations.ToArray(); }
    }

    public int Mutations(string kind) => Operations.Count(o => o.Kind == kind);

    async ValueTask<EventLogWriteResult> MutateAsync(StorageOperation operation,
        Func<ValueTask<EventLogWriteResult>> apply)
    {
        lock (_operations) _operations.Add(operation);
        if (BeforeMutation is { } before) await before(operation);
        var fault = Fault?.Invoke(operation) ?? StorageFault.None;
        if (fault == StorageFault.TimeoutWithoutEffect) return EventLogWriteResult.Ambiguous;
        var result = await apply();
        if (AfterMutation is { } after) await after(operation);
        return fault == StorageFault.LoseResponse ? EventLogWriteResult.Ambiguous : result;
    }

    public ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken cancellation) =>
        Inner.ReadAsync(key, cancellation);

    public ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken cancellation) =>
        Inner.GetPropertiesAsync(key, cancellation);

    public ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination,
        CancellationToken cancellation) => Inner.ReadRangeAsync(key, version, offset, destination, cancellation);

    public ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion, ReadOnlyMemory<byte> content,
        EventLogOwner? owner, string? sha256, CancellationToken cancellation) =>
        MutateAsync(new(expectedVersion == null ? "create" : "write", key, expectedVersion),
            () => Inner.WriteAsync(key, expectedVersion, content, owner, sha256, cancellation));

    public ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner,
        CancellationToken cancellation) =>
        MutateAsync(new("owner", key, expectedVersion), () => Inner.SetOwnerAsync(key, expectedVersion, owner, cancellation));

    public IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix, CancellationToken cancellation) =>
        Inner.ListAsync(prefix, cancellation);

    public ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay,
        CancellationToken cancellation) => Inner.ScheduleDeletionAsync(key, version, delay, cancellation);

    public ValueTask<bool> DeleteAsync(string key, string version, CancellationToken cancellation) =>
        Inner.DeleteAsync(key, version, cancellation);
}

internal sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
{
    DateTimeOffset _now = start;
    public override DateTimeOffset GetUtcNow() => _now;
    public void Advance(TimeSpan duration) => _now += duration;
}

internal static class EventLogTestExtensions
{
    public static async Task<List<EventLogRecord>> ReadAllAsync(this EventLogStorageReader reader, ulong from = 0)
    {
        var result = new List<EventLogRecord>();
        await foreach (var record in reader.ReadRecordsAsync(from, null, CancellationToken.None))
            result.Add(new(record.Offset, record.Payload.ToArray()));
        return result;
    }

    public static byte[] Bytes(int value, int length = 4)
    {
        var bytes = new byte[length];
        BitConverter.TryWriteBytes(bytes, value);
        return bytes;
    }
}
