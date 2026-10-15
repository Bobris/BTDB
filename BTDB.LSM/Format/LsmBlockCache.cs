using System;
using System.Collections.Generic;
using System.Threading;

namespace BTDB.LSM.Format;

/// Byte-budgeted LRU cache of decoded data blocks keyed by (file number, block offset). It can be shared by several
/// databases. Concurrent misses of the same block may both load it; the first inserted copy wins. A sharded CLOCK
/// cache in native memory replaces it in phase L2 (ImplementationPlan.md, section 12.1).
public sealed class LsmBlockCache
{
    readonly long _capacityBytes;
    readonly Dictionary<(ulong File, long Offset), LinkedListNode<Item>> _items = new();
    readonly LinkedList<Item> _lru = new();
    readonly Lock _lock = new();
    long _sizeBytes;
    long _hits;
    long _misses;

    public LsmBlockCache(long capacityBytes)
    {
        _capacityBytes = capacityBytes;
    }

    public long SizeBytes => Interlocked.Read(ref _sizeBytes);
    public long Hits => Interlocked.Read(ref _hits);
    public long Misses => Interlocked.Read(ref _misses);

    sealed record Item((ulong File, long Offset) Key, DataBlock Block);

    internal DataBlock GetOrLoad(ulong file, long offset, Func<DataBlock> load)
    {
        var key = (file, offset);
        lock (_lock)
        {
            if (_items.TryGetValue(key, out var node))
            {
                _lru.Remove(node);
                _lru.AddFirst(node);
                _hits++;
                return node.Value.Block;
            }

            _misses++;
        }

        var block = load();
        lock (_lock)
        {
            if (_items.TryGetValue(key, out var existing)) return existing.Value.Block;
            var node = _lru.AddFirst(new Item(key, block));
            _items.Add(key, node);
            _sizeBytes += block.MemorySize;
            while (_sizeBytes > _capacityBytes && _lru.Count > 1)
            {
                var last = _lru.Last!;
                _lru.RemoveLast();
                _items.Remove(last.Value.Key);
                _sizeBytes -= last.Value.Block.MemorySize;
            }
        }

        return block;
    }

    internal void Evict(ulong file)
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
