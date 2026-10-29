using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Collections;
using Xunit;

namespace BTDBTest;

// Compares LruCache and SpanByteLruCache with a reference model under random gets, sets, removes, clears and capacity
// increases: a get or set makes a key the most recently used, and a new key beyond the capacity evicts the least
// recently used one.
public class LruCacheDifferentialTest
{
    sealed class Model(int capacity)
    {
        readonly LinkedList<(string Key, int Value)> _usage = new();

        public int Capacity { get; set; } = capacity;

        public bool TryGet(string key, out int value)
        {
            var node = Find(key);
            if (node == null)
            {
                value = 0;
                return false;
            }

            _usage.Remove(node);
            _usage.AddFirst(node);
            value = node.Value.Value;
            return true;
        }

        public void Set(string key, int value)
        {
            var node = Find(key);
            if (node != null) _usage.Remove(node);
            else if (_usage.Count == Capacity) _usage.RemoveLast();
            _usage.AddFirst((key, value));
        }

        public bool Remove(string key)
        {
            var node = Find(key);
            if (node == null) return false;
            _usage.Remove(node);
            return true;
        }

        public void Clear() => _usage.Clear();

        public int Count => _usage.Count;

        public IEnumerable<(string Key, int Value)> Items => _usage;

        LinkedListNode<(string Key, int Value)>? Find(string key)
        {
            for (var node = _usage.First; node != null; node = node.Next)
                if (node.Value.Key == key)
                    return node;
            return null;
        }
    }

    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 60).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void LruCachesMatchModel(int seed)
    {
        var random = new Random(seed);
        var capacity = 1 << random.Next(1, 5);
        var startingCapacity = random.Next(1, capacity + 1);
        var cache = new LruCache<string, int>(capacity, startingCapacity);
        var spanCache = new SpanByteLruCache<int>(capacity, startingCapacity);
        Assert.Equal(capacity, cache.MaxCapacity);
        var model = new Model(capacity);
        var keyCount = capacity * 3;
        for (var step = 0; step < 3000; step++)
        {
            // Keys of varied length, the empty one included, so SpanByteLruCache stores and compacts its bytes.
            var key = new string('k', random.Next(keyCount) % 7) + random.Next(keyCount);
            if (random.Next(10) == 0) key = "";
            var bytes = System.Text.Encoding.UTF8.GetBytes(key);
            switch (random.Next(20))
            {
                case < 8:
                {
                    var expected = model.TryGet(key, out var expectedValue);
                    Assert.Equal(expected, cache.TryGetValue(key, out var value));
                    Assert.Equal(expected, spanCache.TryGetValue(bytes, out var spanValue));
                    Assert.Equal(expectedValue, value);
                    Assert.Equal(expectedValue, spanValue);
                    break;
                }
                case < 16:
                    model.Set(key, step);
                    cache[key] = step;
                    spanCache[bytes] = step;
                    break;
                case < 19:
                    var removed = model.Remove(key);
                    Assert.Equal(removed, cache.Remove(key));
                    Assert.Equal(removed, spanCache.Remove(bytes));
                    break;
                default:
                    if (random.Next(4) == 0)
                    {
                        model.Clear();
                        cache.Clear();
                        spanCache.Clear();
                    }
                    else
                    {
                        // Before the fix growing beyond the maximum capacity let the caches hold more items.
                        var newCapacity = random.Next(1, capacity * 2);
                        cache.IncreaseCapacity(newCapacity);
                        spanCache.IncreaseCapacity(newCapacity);
                    }

                    break;
            }

            Assert.Equal(model.Count, cache.Count);
            Assert.Equal(model.Count, spanCache.Count);
            var expectedItems = model.Items.OrderBy(i => i.Key, StringComparer.Ordinal).ToList();
            Assert.Equal(expectedItems,
                cache.Select(p => (p.Key, p.Value)).OrderBy(i => i.Key, StringComparer.Ordinal).ToList());
            Assert.Equal(expectedItems,
                spanCache.Select(p => (Key: System.Text.Encoding.UTF8.GetString(p.Key), p.Value))
                    .OrderBy(i => i.Key, StringComparer.Ordinal).ToList());
        }
    }
}
