using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.Replication.EventLog;

/// <summary>
/// Event-log topics in a local directory, for development and tests of a single node; the drop-in counterpart of
/// <c>AzureEventLogStorage</c>. Content and owner metadata change atomically, every write is flushed to disk and no
/// write is ever ambiguous. One process owns the directory while any instance on it is undisposed. Topics keep their
/// keys as relative paths, for example <c>events/00000000000000000000.elog</c>.
/// </summary>
/// <param name="directory">The event log's own directory; created when missing.</param>
/// <param name="timeProvider">UTC source of deletion deadlines.</param>
public sealed class FileSystemEventLogStorage(string directory, TimeProvider? timeProvider = null)
    : IEventLogStorage, IDisposable
{
    readonly FileSystemObjectStore _store = new(directory);
    readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    static EventLogOwner? Owner(FileSystemObjectStore.Metadata metadata) =>
        metadata is { OwnerSession: { } session, OwnerEndpoint: { } endpoint } ? new(session, endpoint) : null;

    public ValueTask<EventLogBlob?> ReadAsync(string key, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            return ValueTask.FromResult(current == null ? null
                : new EventLogBlob(current.Version, _store.ReadAll(current), Owner(current)));
        }
    }

    public ValueTask<EventLogBlobProperties?> GetPropertiesAsync(string key, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            return ValueTask.FromResult(current == null ? null
                : new EventLogBlobProperties(current.Version, current.Length, Owner(current)));
        }
    }

    public ValueTask ReadRangeAsync(string key, string version, long offset, Memory<byte> destination,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            if (current == null || current.Version != version || offset < 0 ||
                offset + destination.Length > current.Length) throw new EventLogVersionChangedException(key);
            _store.Read(current, offset, destination.Span);
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<EventLogWriteResult> WriteAsync(string key, string? expectedVersion, ReadOnlyMemory<byte> content,
        EventLogOwner? owner, string? sha256, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            if (expectedVersion == null ? current != null : current == null || current.Version != expectedVersion)
                return ValueTask.FromResult(EventLogWriteResult.Rejected);
            // A write replaces all metadata, including a deletion mark, like a conditional Put Blob.
            var next = new FileSystemObjectStore.Metadata(FileSystemObjectStore.NewVersion(),
                _store.WriteData(content.Span), content.Length, sha256, null, owner?.Session, owner?.Endpoint);
            _store.Put(key, next);
            if (current != null) _store.DeleteData(current.DataFile);
            return ValueTask.FromResult(new EventLogWriteResult(EventLogWriteOutcome.Applied, next.Version));
        }
    }

    public ValueTask<EventLogWriteResult> SetOwnerAsync(string key, string expectedVersion, EventLogOwner owner,
        CancellationToken cancellation)
    {
        ArgumentNullException.ThrowIfNull(owner);
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            if (current == null || current.Version != expectedVersion)
                return ValueTask.FromResult(EventLogWriteResult.Rejected);
            // Writable splits carry only owner metadata, so replacing all metadata preserves everything else.
            var next = current with
            {
                Version = FileSystemObjectStore.NewVersion(), Sha256 = null, DeleteAfter = null,
                OwnerSession = owner.Session, OwnerEndpoint = owner.Endpoint
            };
            _store.Put(key, next);
            return ValueTask.FromResult(new EventLogWriteResult(EventLogWriteOutcome.Applied, next.Version));
        }
    }

    public async IAsyncEnumerable<EventLogObjectInfo> ListAsync(string prefix,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        cancellation.ThrowIfCancellationRequested();
        List<(string Key, FileSystemObjectStore.Metadata Metadata)> objects;
        lock (_store.Lock) objects = _store.List(prefix);
        foreach (var (key, metadata) in objects)
        {
            cancellation.ThrowIfCancellationRequested();
            yield return new(key, metadata.Version, metadata.Length, metadata.DeleteAfter);
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    public ValueTask<string?> ScheduleDeletionAsync(string key, string version, TimeSpan delay,
        CancellationToken cancellation)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(delay.Ticks);
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            if (current == null || current.Version != version) return ValueTask.FromResult<string?>(null);
            if (current.DeleteAfter != null) return ValueTask.FromResult<string?>(version);
            var next = current with
            {
                Version = FileSystemObjectStore.NewVersion(), DeleteAfter = _time.GetUtcNow() + delay
            };
            _store.Put(key, next);
            return ValueTask.FromResult<string?>(next.Version);
        }
    }

    public ValueTask<bool> DeleteAsync(string key, string version, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_store.Lock)
        {
            var current = _store.Get(key);
            if (current == null || current.Version != version || current.DeleteAfter is not { } deadline ||
                _time.GetUtcNow() < deadline) return ValueTask.FromResult(false);
            _store.Remove(key, current);
            return ValueTask.FromResult(true);
        }
    }

    /// <summary>Releases the directory once every instance on it is disposed.</summary>
    public void Dispose() => _store.Dispose();
}
