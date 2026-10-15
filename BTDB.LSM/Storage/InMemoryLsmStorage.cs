using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB.LSM.Storage;

/// Thread-safe in-memory object store for tests and development. Objects are kept as immutable byte arrays.
public sealed class InMemoryLsmStorage : ILsmStorage
{
    readonly SortedDictionary<string, byte[]> _objects = new(StringComparer.Ordinal);
    readonly Lock _lock = new();
    long _readRequests;
    long _readBytes;

    public long ReadRequests => Interlocked.Read(ref _readRequests);
    public long ReadBytes => Interlocked.Read(ref _readBytes);

    public int ObjectCount
    {
        get
        {
            lock (_lock) return _objects.Count;
        }
    }

    public ValueTask<LsmWriteOutcome> CreateAsync(string key, ReadOnlyMemory<byte> content,
        CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (_objects.ContainsKey(key)) return ValueTask.FromResult(LsmWriteOutcome.Rejected);
            _objects.Add(key, content.ToArray());
            return ValueTask.FromResult(LsmWriteOutcome.Applied);
        }
    }

    public ValueTask<byte[]?> TryReadAsync(string key, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock)
        {
            if (!_objects.TryGetValue(key, out var content)) return ValueTask.FromResult<byte[]?>(null);
            Interlocked.Increment(ref _readRequests);
            Interlocked.Add(ref _readBytes, content.Length);
            return ValueTask.FromResult<byte[]?>(content.ToArray());
        }
    }

    public ValueTask ReadRangeAsync(string key, long offset, Memory<byte> destination, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        ReadRange(key, offset, destination.Span);
        return ValueTask.CompletedTask;
    }

    public void ReadRange(string key, long offset, Span<byte> destination)
    {
        byte[] content;
        lock (_lock)
        {
            if (!_objects.TryGetValue(key, out content!))
                throw new FileNotFoundException("Object does not exist: " + key);
        }

        if (offset < 0 || offset + destination.Length > content.Length)
            throw new ArgumentOutOfRangeException(nameof(offset), $"Range beyond the end of {key}");
        Interlocked.Increment(ref _readRequests);
        Interlocked.Add(ref _readBytes, destination.Length);
        content.AsSpan((int)offset, destination.Length).CopyTo(destination);
    }

    public async IAsyncEnumerable<LsmObjectInfo> ListAsync(string prefix,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        LsmObjectInfo[] result;
        lock (_lock)
        {
            result = _objects.Where(o => o.Key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(o => new LsmObjectInfo(o.Key, o.Value.Length)).ToArray();
        }

        foreach (var info in result)
        {
            cancellation.ThrowIfCancellationRequested();
            yield return info;
        }

        await Task.CompletedTask;
    }

    public ValueTask DeleteAsync(string key, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (_lock) _objects.Remove(key);
        return ValueTask.CompletedTask;
    }
}
