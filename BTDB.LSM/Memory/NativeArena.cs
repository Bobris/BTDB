using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;

namespace BTDB.LSM.Memory;

/// Bump allocator over native memory chunks. Only the writer allocates; memory never moves, so readers may keep
/// pointers until the whole arena is released. Nothing is freed individually: a memtable's arena is released at once
/// when the memtable is flushed and no snapshot uses it any more (ImplementationPlan.md, section 7.1).
sealed unsafe class NativeArena : IDisposable
{
    readonly int _chunkSize;
    readonly List<nint> _chunks = [];
    byte* _current;
    int _remaining;
    long _allocatedBytes;
    long _usedBytes;
    int _disposed;

    public NativeArena(int chunkSize = 1024 * 1024)
    {
        _chunkSize = chunkSize;
    }

    /// Native bytes reserved by this arena.
    public long AllocatedBytes => Interlocked.Read(ref _allocatedBytes);

    /// Bytes handed out by Allocate, including copies made obsolete by later versions.
    public long UsedBytes => Interlocked.Read(ref _usedBytes);

    /// Allocates size bytes aligned to 8.
    public byte* Allocate(int size)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        size = (size + 7) & ~7;
        Interlocked.Add(ref _usedBytes, size);
        if (size > _chunkSize / 4) return NewChunk(size);
        if (size > _remaining)
        {
            _current = NewChunk(_chunkSize);
            _remaining = _chunkSize;
        }

        var result = _current;
        _current += size;
        _remaining -= size;
        return result;
    }

    public byte* Copy(ReadOnlySpan<byte> data)
    {
        if (data.Length == 0) return null;
        var target = Allocate(data.Length);
        data.CopyTo(new Span<byte>(target, data.Length));
        return target;
    }

    byte* NewChunk(int size)
    {
        var chunk = (byte*)NativeMemory.Alloc((nuint)size);
        _chunks.Add((nint)chunk);
        Interlocked.Add(ref _allocatedBytes, size);
        return chunk;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        foreach (var chunk in _chunks) NativeMemory.Free((void*)chunk);
        _chunks.Clear();
        _current = null;
        _remaining = 0;
        Interlocked.Exchange(ref _allocatedBytes, 0);
        GC.SuppressFinalize(this);
    }

    ~NativeArena()
    {
        Dispose();
    }
}
