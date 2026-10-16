using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using BTDB.LSM.Storage;

namespace BTDB.LSM.Format;

/// A file of one database in a cache: file numbers restart in every database, so a cache shared by several databases
/// keys entries by the database's scope too.
readonly record struct CacheFile(ulong Scope, ulong FileNumber);

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
    readonly ManagedClockCache _metadata;

    public LsmBlockCache(long capacityBytes, int shardCount = 16)
    {
        _metadata = new(Math.Max(64 * 1024, capacityBytes / 8));
        _data = new(Math.Max(64 * 1024, capacityBytes - capacityBytes / 8), shardCount);
    }

    readonly ConditionalWeakTable<ILsmStorage, Dictionary<string, ulong>> _scopes = new();
    long _lastScope;

    /// The scope of the database stored under prefix in storage. Instances on the same storage object and prefix see
    /// the same immutable objects, so they share a scope and its cached blocks.
    internal ulong Scope(ILsmStorage storage, string prefix)
    {
        var byPrefix = _scopes.GetValue(storage, _ => new());
        lock (byPrefix)
        {
            if (!byPrefix.TryGetValue(prefix, out var scope))
                byPrefix.Add(prefix, scope = (ulong)Interlocked.Increment(ref _lastScope));
            return scope;
        }
    }

    public long SizeBytes => _data.SizeBytes + _metadata.SizeBytes;
    public long NativeBytes => _data.SizeBytes;
    public long Hits => _data.Hits + _metadata.Hits;
    public long Misses => _data.Misses + _metadata.Misses;

    /// Pins the raw bytes cached for (file, offset), loading them on a miss; dispose the handle to unpin.
    internal NativeClockCache.Handle Pin(CacheFile file, long offset, Func<byte[]> load) => _data.Pin(file, offset, load);

    internal bool Contains(CacheFile file, long offset) => _data.Contains(file, offset);

    /// Adds prefetched raw bytes unpinned and not yet referenced, so unused readahead is evicted first.
    internal void Insert(CacheFile file, long offset, byte[] data) => _data.Insert(file, offset, data);

    /// Index and filter partitions.
    internal T GetOrLoadMetadata<T>(CacheFile file, long offset, Func<T> load) where T : class, ICachedBlock =>
        _metadata.GetOrLoad(file, offset, load);

    internal void Evict(CacheFile file)
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
        public CacheFile File;
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
        public readonly Dictionary<(CacheFile File, long Offset), Slot> Slots = new();
        public readonly List<Slot> Ring = [];
        public int Hand;
        public long Size;

        // Counted under the shard's lock: shared counters would be written by every reader thread.
        public long Hits;
        public long Misses;
    }

    /// A pinned cache entry; Span stays valid until Dispose.
    internal readonly struct Handle(Slot slot) : IDisposable
    {
        public ReadOnlySpan<byte> Span => new(slot.Pointer, slot.Length);

        public void Dispose() => Release(slot);
    }

    readonly Shard[] _shards;
    readonly long _shardCapacity;
    int _disposed;

    public NativeClockCache(long capacityBytes, int shardCount)
    {
        _shards = new Shard[shardCount];
        for (var i = 0; i < shardCount; i++) _shards[i] = new();
        _shardCapacity = Math.Max(1, capacityBytes / shardCount);
    }

    public long Hits => Sum(static shard => shard.Hits);
    public long Misses => Sum(static shard => shard.Misses);

    long Sum(Func<Shard, long> counter)
    {
        long sum = 0;
        foreach (var shard in _shards)
            lock (shard.Lock)
                sum += counter(shard);
        return sum;
    }

    public long SizeBytes
    {
        get
        {
            long size = 0;
            foreach (var shard in _shards) size += Interlocked.Read(ref shard.Size);
            return size;
        }
    }

    Shard ShardFor(CacheFile file, long offset) =>
        _shards[(int)((uint)HashCode.Combine(file, offset) % (uint)_shards.Length)];

    public Handle Pin(CacheFile file, long offset, Func<byte[]> load)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        var shard = ShardFor(file, offset);
        lock (shard.Lock)
        {
            if (shard.Slots.TryGetValue((file, offset), out var slot))
            {
                slot.Pins++;
                slot.Referenced = true;
                shard.Hits++;
                return new(slot);
            }

            shard.Misses++;
        }

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

    public bool Contains(CacheFile file, long offset)
    {
        var shard = ShardFor(file, offset);
        lock (shard.Lock) return shard.Slots.ContainsKey((file, offset));
    }

    public void Insert(CacheFile file, long offset, byte[] data)
    {
        var shard = ShardFor(file, offset);
        lock (shard.Lock)
        {
            // Checked under the lock: Dispose sets the flag before it frees each shard under the same lock.
            if (_disposed != 0 || shard.Slots.ContainsKey((file, offset))) return;
            var slot = new Slot
            {
                File = file, Offset = offset, Length = data.Length, Shard = shard,
                Pointer = (byte*)NativeMemory.Alloc((nuint)Math.Max(1, data.Length))
            };
            data.CopyTo(new Span<byte>(slot.Pointer, data.Length));
            shard.Slots.Add((file, offset), slot);
            shard.Ring.Add(slot);
            shard.Size += data.Length;
            EvictIfNeeded(shard);
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

    public void Evict(CacheFile file)
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

/// Byte-budgeted CLOCK cache of decoded managed objects keyed by (file, offset). Index and filter partitions are read
/// on every lookup, so a hit takes no lock: it finds the entry in a concurrent dictionary and sets its reference bit.
/// Inserts and eviction share one lock and one budget, so a few large filter partitions cannot overflow a shard.
/// Concurrent misses of one key load it once.
sealed class ManagedClockCache(long capacityBytes)
{
    sealed class Entry((CacheFile File, long Offset) key, Lazy<ICachedBlock> value)
    {
        public readonly (CacheFile File, long Offset) Key = key;
        public readonly Lazy<ICachedBlock> Value = value;
        public volatile bool Referenced = true;
        public long Size;
    }

    readonly ConcurrentDictionary<(CacheFile File, long Offset), Entry> _entries = new();
    readonly Lock _ringLock = new();
    readonly List<Entry> _ring = [];
    int _hand;
    long _size;
    long _misses;

    public long SizeBytes
    {
        get
        {
            lock (_ringLock) return _size;
        }
    }

    /// Hits are not counted, which would make every lookup write a shared counter; misses are rare.
    public long Hits => 0;

    public long Misses => Interlocked.Read(ref _misses);

    public T GetOrLoad<T>(CacheFile file, long offset, Func<T> load) where T : class, ICachedBlock
    {
        var key = (file, offset);
        if (!_entries.TryGetValue(key, out var entry))
        {
            var created = new Entry(key, new(() => load(), LazyThreadSafetyMode.ExecutionAndPublication));
            entry = _entries.GetOrAdd(key, created);
            if (entry == created) return (T)Load(entry);
        }

        entry.Referenced = true;
        return (T)Wait(entry);
    }

    ICachedBlock Wait(Entry entry)
    {
        try
        {
            return entry.Value.Value;
        }
        catch
        {
            // A failed load is not cached; the next caller loads again.
            _entries.TryRemove(new KeyValuePair<(CacheFile, long), Entry>(entry.Key, entry));
            throw;
        }
    }

    ICachedBlock Load(Entry entry)
    {
        Interlocked.Increment(ref _misses);
        var value = Wait(entry);
        lock (_ringLock)
        {
            if (_entries.TryGetValue(entry.Key, out var current) && current == entry)
            {
                entry.Size = value.MemorySize;
                _ring.Add(entry);
                _size += entry.Size;
                EvictIfNeeded(entry);
            }
        }

        return value;
    }

    void EvictIfNeeded(Entry keep)
    {
        var steps = 2 * _ring.Count;
        while (_size > capacityBytes && _ring.Count > 1 && steps-- > 0)
        {
            if (_hand >= _ring.Count) _hand = 0;
            var entry = _ring[_hand];
            if (entry.Referenced || entry == keep)
            {
                entry.Referenced = false;
                _hand++;
                continue;
            }

            RemoveAt(_hand);
        }
    }

    void RemoveAt(int ringIndex)
    {
        var entry = _ring[ringIndex];
        _ring[ringIndex] = _ring[^1];
        _ring.RemoveAt(_ring.Count - 1);
        _entries.TryRemove(new KeyValuePair<(CacheFile, long), Entry>(entry.Key, entry));
        _size -= entry.Size;
    }

    public void Evict(CacheFile file)
    {
        lock (_ringLock)
        {
            for (var i = _ring.Count - 1; i >= 0; i--)
                if (_ring[i].Key.File == file)
                    RemoveAt(i);
            foreach (var (key, entry) in _entries)
                if (key.File == file)
                    _entries.TryRemove(new KeyValuePair<(CacheFile, long), Entry>(key, entry));
        }
    }
}
