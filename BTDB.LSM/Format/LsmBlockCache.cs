using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;

namespace BTDB.LSM.Format;

interface ICachedBlock
{
    long MemorySize { get; }
}

/// Cache shared by the tables of one or more databases (ImplementationPlan.md, section 12.1). Data blocks and
/// separated values are kept raw (decompressed) in native memory in a sharded CLOCK cache, so cached data creates no
/// managed objects; readers pin an entry only for the duration of one call. Decoded index and filter partitions, which
/// are small and accessed on every lookup, live in a managed LRU that gets one eighth of the budget.
public sealed class LsmBlockCache : IDisposable
{
    readonly NativeClockCache _data;
    readonly ManagedLruCache _metadata;

    public LsmBlockCache(long capacityBytes, int shardCount = 16)
    {
        _metadata = new(Math.Max(64 * 1024, capacityBytes / 8));
        _data = new(Math.Max(64 * 1024, capacityBytes - capacityBytes / 8), shardCount);
    }

    public long SizeBytes => _data.SizeBytes + _metadata.SizeBytes;
    public long NativeBytes => _data.SizeBytes;
    public long Hits => _data.Hits + _metadata.Hits;
    public long Misses => _data.Misses + _metadata.Misses;

    /// Pins the raw bytes cached for (file, offset), loading them on a miss; dispose the handle to unpin.
    internal NativeClockCache.Handle Pin(ulong file, long offset, Func<byte[]> load) => _data.Pin(file, offset, load);

    /// Index and filter partitions.
    internal T GetOrLoadMetadata<T>(ulong file, long offset, Func<T> load) where T : class, ICachedBlock =>
        _metadata.GetOrLoad(file, offset, load);

    internal void Evict(ulong file)
    {
        _data.Evict(file);
        _metadata.Evict(file);
    }

    public void Dispose() => _data.Dispose();
}

/// Sharded CLOCK cache of raw byte blocks in native memory. Pinned entries are never freed; an entry evicted while
/// pinned is freed when its last pin is released.
sealed unsafe class NativeClockCache : IDisposable
{
    internal sealed class Slot
    {
        public ulong File;
        public long Offset;
        public byte* Pointer;
        public int Length;
        public int Pins;
        public bool Referenced;
        public bool Dead;
        public Shard Shard = null!;
    }

    internal sealed class Shard
    {
        public readonly Lock Lock = new();
        public readonly Dictionary<(ulong File, long Offset), Slot> Slots = new();
        public readonly List<Slot> Ring = [];
        public int Hand;
        public long Size;
    }

    /// A pinned cache entry; Span stays valid until Dispose.
    internal readonly struct Handle(Slot slot) : IDisposable
    {
        public ReadOnlySpan<byte> Span => new(slot.Pointer, slot.Length);

        public void Dispose() => Release(slot);
    }

    readonly Shard[] _shards;
    readonly long _shardCapacity;
    long _hits;
    long _misses;
    int _disposed;

    public NativeClockCache(long capacityBytes, int shardCount)
    {
        _shards = new Shard[shardCount];
        for (var i = 0; i < shardCount; i++) _shards[i] = new();
        _shardCapacity = Math.Max(1, capacityBytes / shardCount);
    }

    public long Hits => Interlocked.Read(ref _hits);
    public long Misses => Interlocked.Read(ref _misses);

    public long SizeBytes
    {
        get
        {
            long size = 0;
            foreach (var shard in _shards) size += Interlocked.Read(ref shard.Size);
            return size;
        }
    }

    Shard ShardFor(ulong file, long offset) =>
        _shards[(int)((uint)HashCode.Combine(file, offset) % (uint)_shards.Length)];

    public Handle Pin(ulong file, long offset, Func<byte[]> load)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        var shard = ShardFor(file, offset);
        lock (shard.Lock)
        {
            if (shard.Slots.TryGetValue((file, offset), out var slot))
            {
                slot.Pins++;
                slot.Referenced = true;
                Interlocked.Increment(ref _hits);
                return new(slot);
            }
        }

        Interlocked.Increment(ref _misses);
        var data = load();
        lock (shard.Lock)
        {
            if (shard.Slots.TryGetValue((file, offset), out var existing))
            {
                existing.Pins++;
                existing.Referenced = true;
                return new(existing);
            }

            var slot = new Slot
            {
                File = file, Offset = offset, Length = data.Length, Pins = 1, Referenced = true, Shard = shard,
                Pointer = (byte*)NativeMemory.Alloc((nuint)Math.Max(1, data.Length))
            };
            data.CopyTo(new Span<byte>(slot.Pointer, data.Length));
            shard.Slots.Add((file, offset), slot);
            shard.Ring.Add(slot);
            shard.Size += data.Length;
            EvictIfNeeded(shard);
            return new(slot);
        }
    }

    static void Release(Slot slot)
    {
        lock (slot.Shard.Lock)
        {
            slot.Pins--;
            if (slot.Dead && slot.Pins == 0) Free(slot);
        }
    }

    static void Free(Slot slot)
    {
        NativeMemory.Free(slot.Pointer);
        slot.Pointer = null;
    }

    void EvictIfNeeded(Shard shard)
    {
        var steps = 2 * shard.Ring.Count;
        while (shard.Size > _shardCapacity && shard.Ring.Count > 0 && steps-- > 0)
        {
            if (shard.Hand >= shard.Ring.Count) shard.Hand = 0;
            var slot = shard.Ring[shard.Hand];
            if (slot.Pins > 0)
            {
                shard.Hand++;
                continue;
            }

            if (slot.Referenced)
            {
                slot.Referenced = false;
                shard.Hand++;
                continue;
            }

            Remove(shard, shard.Hand);
            Free(slot);
        }
    }

    static void Remove(Shard shard, int ringIndex)
    {
        var slot = shard.Ring[ringIndex];
        shard.Ring[ringIndex] = shard.Ring[^1];
        shard.Ring.RemoveAt(shard.Ring.Count - 1);
        shard.Slots.Remove((slot.File, slot.Offset));
        shard.Size -= slot.Length;
    }

    public void Evict(ulong file)
    {
        foreach (var shard in _shards)
        {
            lock (shard.Lock)
            {
                for (var i = shard.Ring.Count - 1; i >= 0; i--)
                {
                    var slot = shard.Ring[i];
                    if (slot.File != file) continue;
                    Remove(shard, i);
                    if (slot.Pins == 0) Free(slot);
                    else slot.Dead = true;
                }
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        foreach (var shard in _shards)
        {
            lock (shard.Lock)
            {
                foreach (var slot in shard.Ring)
                {
                    if (slot.Pins == 0) Free(slot);
                    else slot.Dead = true;
                }

                shard.Ring.Clear();
                shard.Slots.Clear();
                shard.Size = 0;
            }
        }

        GC.SuppressFinalize(this);
    }

    ~NativeClockCache()
    {
        Dispose();
    }
}

/// Byte-budgeted LRU of decoded managed objects keyed by (file number, offset).
sealed class ManagedLruCache(long capacityBytes)
{
    readonly Dictionary<(ulong File, long Offset), LinkedListNode<Item>> _items = new();
    readonly LinkedList<Item> _lru = new();
    readonly Lock _lock = new();
    long _sizeBytes;
    long _hits;
    long _misses;

    public long SizeBytes => Interlocked.Read(ref _sizeBytes);
    public long Hits => Interlocked.Read(ref _hits);
    public long Misses => Interlocked.Read(ref _misses);

    sealed record Item((ulong File, long Offset) Key, ICachedBlock Block);

    public T GetOrLoad<T>(ulong file, long offset, Func<T> load) where T : class, ICachedBlock
    {
        var key = (file, offset);
        lock (_lock)
        {
            if (_items.TryGetValue(key, out var node))
            {
                _lru.Remove(node);
                _lru.AddFirst(node);
                _hits++;
                return (T)node.Value.Block;
            }

            _misses++;
        }

        var block = load();
        lock (_lock)
        {
            if (_items.TryGetValue(key, out var existing)) return (T)existing.Value.Block;
            var node = _lru.AddFirst(new Item(key, block));
            _items.Add(key, node);
            _sizeBytes += block.MemorySize;
            while (_sizeBytes > capacityBytes && _lru.Count > 1)
            {
                var last = _lru.Last!;
                _lru.RemoveLast();
                _items.Remove(last.Value.Key);
                _sizeBytes -= last.Value.Block.MemorySize;
            }
        }

        return block;
    }

    public void Evict(ulong file)
    {
        lock (_lock)
        {
            var node = _lru.First;
            while (node != null)
            {
                var next = node.Next;
                if (node.Value.Key.File == file)
                {
                    _lru.Remove(node);
                    _items.Remove(node.Value.Key);
                    _sizeBytes -= node.Value.Block.MemorySize;
                }

                node = next;
            }
        }
    }
}
