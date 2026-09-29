using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

/// <summary>Deterministic reference storage for tests and single-process use. Versions never repeat, even after a
/// delete. All operations complete synchronously.</summary>
internal sealed class InMemoryEventLogStorage(TimeProvider? timeProvider = null) : IEventLogStorage
{
    sealed record Entry(string Version, byte[] Content, EventLogOwner? Owner, string? Sha256, DateTimeOffset? DeleteAfter);

    readonly object _lock = new();
    readonly SortedDictionary<string, Entry> _objects = new(StringComparer.Ordinal);
    readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    long _version;

    public int Writes { get; private set; }
    public int Reads { get; private set; }

    string NextVersion() => (++_version).ToString("x", System.Globalization.CultureInfo.InvariantCulture);

    public IReadOnlyCollection<string> Keys
    {
        get { lock (_lock) return _objects.Keys.ToArray(); }
    }

    public string? Sha256Of(string key)
    {
        lock (_lock) return _objects.GetValueOrDefault(key)?.Sha256;
    }

    public ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            Reads++;
            return ValueTask.FromResult(_objects.TryGetValue(key, out var entry)
                ? new EventLogBlob(entry.Version, entry.Content.ToArray(), entry.Owner) : null);
        }
    }

    public ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            Reads++;
            return ValueTask.FromResult(_objects.TryGetValue(key, out var entry)
                ? new EventLogBlobProperties(entry.Version, entry.Content.Length, entry.Owner) : null);
        }
    }

    public ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            Reads++;
            if (!_objects.TryGetValue(key, out var entry) || entry.Version != version)
                throw new EventLogVersionChangedException(key);
            if (offset < 0 || offset + destination.Length > entry.Content.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));
            entry.Content.AsSpan((int)offset, destination.Length).CopyTo(destination.Span);
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion, ReadOnlyMemory<byte> content,
        EventLogOwner? owner, string? sha256, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            Writes++;
            var exists = _objects.TryGetValue(key, out var entry);
            if (expectedVersion == null ? exists : !exists || entry!.Version != expectedVersion)
                return ValueTask.FromResult(EventLogWriteResult.Rejected);
            var version = NextVersion();
            _objects[key] = new(version, content.ToArray(), owner, sha256, null);
            return ValueTask.FromResult(new EventLogWriteResult(EventLogWriteOutcome.Applied, version));
        }
    }

    public ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            Writes++;
            if (!_objects.TryGetValue(key, out var entry) || entry.Version != expectedVersion)
                return ValueTask.FromResult(EventLogWriteResult.Rejected);
            var version = NextVersion();
            _objects[key] = entry with { Version = version, Owner = owner };
            return ValueTask.FromResult(new EventLogWriteResult(EventLogWriteOutcome.Applied, version));
        }
    }

    public async IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        EventLogObjectInfo[] result;
        lock (_lock)
        {
            Reads++;
            result = _objects.Where(o => o.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(o => new EventLogObjectInfo(o.Key, o.Value.Version, o.Value.Content.Length, o.Value.DeleteAfter))
                .ToArray();
        }
        foreach (var info in result)
        {
            cancellation.ThrowIfCancellationRequested();
            yield return info;
        }
        await Task.CompletedTask;
    }

    public ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            Writes++;
            if (!_objects.TryGetValue(key, out var entry) || entry.Version != version)
                return ValueTask.FromResult<string?>(null);
            if (entry.DeleteAfter != null) return ValueTask.FromResult<string?>(entry.Version);
            var next = NextVersion();
            _objects[key] = entry with { Version = next, DeleteAfter = _time.GetUtcNow() + delay };
            return ValueTask.FromResult<string?>(next);
        }
    }

    public ValueTask<bool> DeleteAsync(string key, string version, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            Writes++;
            if (!_objects.TryGetValue(key, out var entry) || entry.Version != version ||
                entry.DeleteAfter is not { } due || _time.GetUtcNow() < due) return ValueTask.FromResult(false);
            _objects.Remove(key);
            return ValueTask.FromResult(true);
        }
    }
}
