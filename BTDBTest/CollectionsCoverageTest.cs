using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BTDB.Collections;
using BTDB.Locks;
using Xunit;

namespace BTDBTest;

public class CollectionsCoverageTest
{
    /// Reference LRU model
    sealed class LruModel<TKey, TValue> where TKey : notnull
    {
        readonly int _capacity;
        readonly Dictionary<TKey, TValue> _values = new();
        readonly LinkedList<TKey> _usage = new(); // First is most recently used

        public LruModel(int capacity) => _capacity = capacity;

        public int Count => _values.Count;

        void Touch(TKey key)
        {
            _usage.Remove(key);
            _usage.AddFirst(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            if (!_values.TryGetValue(key, out value!)) return false;
            Touch(key);
            return true;
        }

        public void Set(TKey key, TValue value, out bool added)
        {
            added = !_values.ContainsKey(key);
            if (added && _values.Count == _capacity)
            {
                var victim = _usage.Last!.Value;
                _usage.RemoveLast();
                _values.Remove(victim);
            }

            _values[key] = value;
            Touch(key);
        }

        public bool Remove(TKey key)
        {
            if (!_values.Remove(key)) return false;
            _usage.Remove(key);
            return true;
        }

        public void Clear()
        {
            _values.Clear();
            _usage.Clear();
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> Items => _values;
    }

    [Theory]
    [InlineData(1, 1, 2)]
    [InlineData(2, 4, 8)]
    [InlineData(3, 8, 2)]
    [InlineData(4, 16, 16)]
    [InlineData(5, 64, 4)]
    public void LruCacheBehavesLikeReferenceModel(int seed, int capacity, int startingCapacity)
    {
        var rnd = new Random(seed);
        var cache = new LruCache<int, int>(capacity, startingCapacity);
        var model = new LruModel<int, int>(cache.MaxCapacity);
        var keyRange = capacity * 3;
        for (var step = 0; step < 5000; step++)
        {
            var key = rnd.Next(keyRange);
            switch (rnd.Next(10))
            {
                case 0:
                case 1:
                case 2:
                {
                    var expected = model.TryGetValue(key, out var expectedValue);
                    Assert.Equal(expected, cache.TryGetValue(key, out var value));
                    if (expected) Assert.Equal(expectedValue, value);
                    break;
                }
                case 3:
                case 4:
                case 5:
                {
                    model.Set(key, step, out var expectedAdded);
                    cache.GetOrAddValueRef(key, out var added) = step;
                    Assert.Equal(expectedAdded, added);
                    break;
                }
                case 6:
                    model.Set(key, -step, out _);
                    cache[key] = -step;
                    break;
                case 7:
                case 8:
                    Assert.Equal(model.Remove(key), cache.Remove(key));
                    break;
                default:
                    if (rnd.Next(30) == 0)
                    {
                        model.Clear();
                        cache.Clear();
                    }
                    else
                    {
                        cache.IncreaseCapacity(rnd.Next(1, cache.MaxCapacity + 1));
                    }

                    break;
            }

            Assert.Equal(model.Count, cache.Count);
            if (step % 97 == 0)
            {
                Assert.Equal(model.Items.OrderBy(p => p.Key), cache.OrderBy(p => p.Key));
            }
        }
    }

    [Fact]
    public void LruCacheIncreaseCapacityAfterRemoveKeepsAllItems()
    {
        var cache = new LruCache<int, string>(8, 4);
        cache[1] = "a";
        cache[2] = "b";
        cache[3] = "c";
        Assert.True(cache.Remove(1));
        cache.IncreaseCapacity(8);
        Assert.Equal(8, cache.Capacity);
        Assert.True(cache.TryGetValue(3, out var v3));
        Assert.Equal("c", v3);
        Assert.True(cache.TryGetValue(2, out var v2));
        Assert.Equal("b", v2);
        Assert.Equal(new[] { 2, 3 }, cache.Select(p => p.Key).OrderBy(k => k));
        cache[4] = "d";
        cache[5] = "e";
        Assert.Equal(new[] { 2, 3, 4, 5 }, cache.Select(p => p.Key).OrderBy(k => k));
    }

    [Theory]
    [InlineData(1, 2, 2)]
    [InlineData(2, 4, 4)]
    [InlineData(3, 8, 2)]
    [InlineData(4, 16, 16)]
    [InlineData(5, 64, 8)]
    public void SpanByteLruCacheBehavesLikeReferenceModel(int seed, int capacity, int startingCapacity)
    {
        var rnd = new Random(seed);
        var cache = new SpanByteLruCache<int>(capacity, startingCapacity);
        var model = new LruModel<string, int>(cache.MaxCapacity);
        var keyRange = capacity * 3;
        for (var step = 0; step < 5000; step++)
        {
            var keyString = new string((char)('a' + rnd.Next(26)), rnd.Next(0, 4)) + rnd.Next(keyRange);
            var key = Encoding.UTF8.GetBytes(keyString);
            switch (rnd.Next(10))
            {
                case 0:
                case 1:
                case 2:
                {
                    var expected = model.TryGetValue(keyString, out var expectedValue);
                    Assert.Equal(expected, cache.TryGetValue(key, out var value));
                    if (expected) Assert.Equal(expectedValue, value);
                    break;
                }
                case 3:
                case 4:
                case 5:
                {
                    model.Set(keyString, step, out var expectedAdded);
                    cache.GetOrAddValueRef(key, out var added) = step;
                    Assert.Equal(expectedAdded, added);
                    break;
                }
                case 6:
                    model.Set(keyString, -step, out _);
                    cache[key] = -step;
                    break;
                case 7:
                case 8:
                    Assert.Equal(model.Remove(keyString), cache.Remove(key));
                    break;
                default:
                    if (rnd.Next(30) == 0)
                    {
                        model.Clear();
                        cache.Clear();
                    }
                    else
                    {
                        cache.IncreaseCapacity(rnd.Next(1, cache.MaxCapacity + 1));
                    }

                    break;
            }

            Assert.Equal(model.Count, cache.Count);
            if (step % 97 == 0)
            {
                Assert.Equal(model.Items.OrderBy(p => p.Key, StringComparer.Ordinal),
                    cache.Select(p => new KeyValuePair<string, int>(Encoding.UTF8.GetString(p.Key), p.Value))
                        .OrderBy(p => p.Key, StringComparer.Ordinal));
            }
        }
    }

    [Fact]
    public void SpanByteLruCacheCompactionAfterRemoveKeepsAllKeys()
    {
        var cache = new SpanByteLruCache<int>(16, 16);
        cache["aaaa"u8] = 1;
        cache["bbbb"u8] = 2;
        cache["cccc"u8] = 3;
        cache["dddd"u8] = 4;
        Assert.True(cache.Remove("aaaa"u8));
        Assert.True(cache.Remove("bbbb"u8));
        // byte storage is full, adding forces compaction of key bytes
        cache["eeee"u8] = 5;
        Assert.True(cache.TryGetValue("cccc"u8, out var c));
        Assert.Equal(3, c);
        Assert.True(cache.TryGetValue("dddd"u8, out var d));
        Assert.Equal(4, d);
        Assert.True(cache.TryGetValue("eeee"u8, out var e));
        Assert.Equal(5, e);
        Assert.Equal(new[] { "cccc", "dddd", "eeee" },
            cache.Select(p => Encoding.UTF8.GetString(p.Key)).OrderBy(k => k));
    }

    [Fact]
    public void SpanByteLruCacheIncreaseCapacityAfterRemoveKeepsAllItems()
    {
        var cache = new SpanByteLruCache<int>(8, 4);
        cache["1"u8] = 1;
        cache["2"u8] = 2;
        cache["3"u8] = 3;
        Assert.True(cache.Remove("1"u8));
        cache.IncreaseCapacity(8);
        Assert.True(cache.TryGetValue("3"u8, out var v3));
        Assert.Equal(3, v3);
        Assert.True(cache.TryGetValue("2"u8, out var v2));
        Assert.Equal(2, v2);
        Assert.Equal(2, cache.Count());
    }

    [Fact]
    public void LruCachesValidateArgumentsAndExposeDebugViews()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new LruCache<int, int>(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpanByteLruCache<int>(0));
        var cache = new LruCache<string, int>(1);
        Assert.Equal(2, cache.MaxCapacity);
        Assert.Throws<ArgumentNullException>(() => cache.TryGetValue(null!, out _));
        Assert.Throws<ArgumentNullException>(() => cache.Remove(null!));
        Assert.Throws<ArgumentNullException>(() => cache.GetOrAddValueRef(null!, out _));
        cache["a"] = 1;
        var view = new LruCacheDebugView<string, int>(cache);
        Assert.Equal(new[] { new KeyValuePair<string, int>("a", 1) }, view.Items);
        IEnumerator enumerator = ((IEnumerable)cache).GetEnumerator();
        Assert.True(enumerator.MoveNext());
        Assert.Equal(new KeyValuePair<string, int>("a", 1), enumerator.Current);
        Assert.False(enumerator.MoveNext());
        enumerator.Reset();
        Assert.True(enumerator.MoveNext());
        ((IDisposable)enumerator).Dispose();

        var spanCache = new SpanByteLruCache<int>(4);
        spanCache["k"u8] = 7;
        var spanView = new SpanByteLruCacheDebugView<int>(spanCache);
        Assert.Equal(new[] { new KeyValuePair<string, int>("k", 7) }, spanView.Items);
        IEnumerator spanEnumerator = ((IEnumerable)spanCache).GetEnumerator();
        Assert.True(spanEnumerator.MoveNext());
        Assert.IsType<KeyValuePair<byte[], int>>(spanEnumerator.Current);
        Assert.False(spanEnumerator.MoveNext());
        spanEnumerator.Reset();
        Assert.True(spanEnumerator.MoveNext());
    }

    sealed class BadHash(int value) : IEquatable<BadHash>
    {
        public readonly int Value = value;
        public bool Equals(BadHash? other) => other != null && other.Value == Value;
        public override bool Equals(object? obj) => Equals(obj as BadHash);
        public override int GetHashCode() => Value % 3;
        public override string ToString() => Value.ToString();
    }

    [Theory]
    [InlineData(1, -1)]
    [InlineData(2, 0)]
    [InlineData(3, 5)]
    [InlineData(4, 100)]
    public void RefDictionaryBehavesLikeDictionary(int seed, int capacity)
    {
        var rnd = new Random(seed);
        var dict = capacity < 0 ? new RefDictionary<BadHash, int>() : new RefDictionary<BadHash, int>(capacity);
        var model = new Dictionary<int, int>();
        var seqLock = new SeqLock();
        for (var step = 0; step < 4000; step++)
        {
            var k = rnd.Next(60);
            var key = new BadHash(k);
            switch (rnd.Next(12))
            {
                case 0:
                    Assert.Equal(model.ContainsKey(k), dict.ContainsKey(key));
                    break;
                case 1:
                {
                    Assert.Equal(model.TryGetValue(k, out var e), dict.TryGetValue(key, out var v));
                    Assert.Equal(e, v);
                    break;
                }
                case 2:
                {
                    Assert.Equal(model.TryGetValue(k, out var e), dict.TryGetValueSeqLock(key, out var v, ref seqLock));
                    Assert.Equal(e, v);
                    break;
                }
                case 3:
                    Assert.Equal(model.TryAdd(k, step), dict.TryAdd(key, step));
                    break;
                case 4:
                case 5:
                    model[k] = step;
                    dict.GetOrAddValueRef(key) = step;
                    break;
                case 6:
                case 7:
                    Assert.Equal(model.Remove(k), dict.Remove(key));
                    break;
                case 8:
                {
                    ref var r = ref dict.GetOrFakeValueRef(key);
                    Assert.Equal(model.TryGetValue(k, out var e) ? e : 0, r);
                    break;
                }
                case 9:
                {
                    ref var r = ref dict.GetOrFakeValueRef(key, out var found);
                    Assert.Equal(model.TryGetValue(k, out var e), found);
                    if (found)
                    {
                        Assert.Equal(e, r);
                        r = step;
                        model[k] = step;
                    }

                    break;
                }
                case 10:
                {
                    var arr = new KeyValuePair<BadHash, int>[dict.Count + 1];
                    dict.CopyTo(arr, 1);
                    Assert.Equal(model.OrderBy(p => p.Key), arr.Skip(1).Select(p => new KeyValuePair<int, int>(p.Key.Value, p.Value)).OrderBy(p => p.Key));
                    var viaIndex = new List<KeyValuePair<int, int>>();
                    foreach (var idx in dict.Index)
                    {
                        Assert.Equal(dict.KeyRef(idx), dict.DangerousKeyRef(idx));
                        viaIndex.Add(new(dict.KeyRef(idx).Value, dict.ValueRef(idx)));
                    }

                    Assert.Equal(model.OrderBy(p => p.Key), viaIndex.OrderBy(p => p.Key));
                    break;
                }
                default:
                    if (rnd.Next(40) == 0)
                    {
                        model.Clear();
                        dict.Clear();
                    }

                    break;
            }

            Assert.Equal(model.Count, dict.Count);
        }

        Assert.Equal(model.OrderBy(p => p.Key), dict.Select(p => new KeyValuePair<int, int>(p.Key.Value, p.Value)).OrderBy(p => p.Key));
    }

    [Fact]
    public void RefDictionaryEdgeCases()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RefDictionary<int, int>(-1));
        var dict = new RefDictionary<string, int>();
        Assert.Throws<ArgumentNullException>(() => dict.ContainsKey(null!));
        Assert.Throws<ArgumentNullException>(() => dict.TryGetValue(null!, out _));
        Assert.Throws<ArgumentNullException>(() => dict.Remove(null!));
        Assert.Throws<ArgumentNullException>(() => dict.GetOrFakeValueRef(null!));
        Assert.Throws<ArgumentNullException>(() => dict.GetOrFakeValueRef(null!, out _));
        Assert.Throws<ArgumentNullException>(() => dict.GetOrAddValueRef(null!));
        Assert.Throws<ArgumentNullException>(() => dict.TryAdd(null!, 1));
        var seqLock = new SeqLock();
        Assert.Throws<ArgumentNullException>(() => dict.TryGetValueSeqLock(null!, out _, ref seqLock));
        Assert.Throws<ArgumentNullException>(() => dict.CopyTo(null!, 0));
        Assert.False(dict.Remove("x"));
        dict.Clear();
        Assert.Equal(1, dict.Capacity);
        dict.GetOrAddValueRef("a") = 1;
        dict.GetOrAddValueRef("b") = 2;
        var view = new RefDictionaryDebugView<string, int>(dict);
        Assert.Equal(2, view.Items.Length);
        Assert.Throws<ArgumentNullException>(() => new RefDictionaryDebugView<string, int>(null!));
        IEnumerator enumerator = ((IEnumerable)dict).GetEnumerator();
        Assert.True(enumerator.MoveNext());
        Assert.NotNull(enumerator.Current);
        enumerator.Reset();
        var count = 0;
        while (enumerator.MoveNext()) count++;
        Assert.Equal(2, count);
        Assert.False(enumerator.MoveNext());
        IEnumerator<uint> indexEnumerator = ((IEnumerable<uint>)dict.Index).GetEnumerator();
        Assert.True(indexEnumerator.MoveNext());
        Assert.True(indexEnumerator.MoveNext());
        Assert.False(indexEnumerator.MoveNext());
        indexEnumerator.Reset();
        Assert.True(indexEnumerator.MoveNext());
        Assert.Equal(0u, ((IEnumerator)indexEnumerator).Current);
        indexEnumerator.Dispose();
        Assert.Equal(2, ((IEnumerable)dict.Index).Cast<uint>().Count());
    }

    [Theory]
    [InlineData(1, -1)]
    [InlineData(2, 0)]
    [InlineData(3, 7)]
    public void SpanByteNoRemoveDictionaryBehavesLikeDictionary(int seed, int capacity)
    {
        var rnd = new Random(seed);
        var dict = capacity < 0 ? new SpanByteNoRemoveDictionary<int>() : new SpanByteNoRemoveDictionary<int>(capacity);
        var model = new Dictionary<string, int>();
        var seqLock = new SeqLock();
        for (var step = 0; step < 3000; step++)
        {
            var keyString = rnd.Next(5) == 0 ? "" : new string('k', rnd.Next(1, 5)) + rnd.Next(200);
            var key = Encoding.UTF8.GetBytes(keyString);
            switch (rnd.Next(6))
            {
                case 0:
                    Assert.Equal(model.ContainsKey(keyString), dict.ContainsKey(key));
                    break;
                case 1:
                {
                    Assert.Equal(model.TryGetValue(keyString, out var e), dict.TryGetValue(key, out var v));
                    Assert.Equal(e, v);
                    break;
                }
                case 2:
                {
                    Assert.Equal(model.TryGetValue(keyString, out var e), dict.TryGetValueSeqLock(key, out var v, ref seqLock));
                    Assert.Equal(e, v);
                    break;
                }
                case 3:
                    model[keyString] = step;
                    dict.GetOrAddValueRef(key) = step;
                    break;
                case 4:
                    model[keyString] = -step;
                    dict[key] = -step;
                    break;
                default:
                    if (model.TryGetValue(keyString, out var expected)) Assert.Equal(expected, dict[key]);
                    else Assert.Throws<ArgumentException>(() => dict[key]);
                    break;
            }

            Assert.Equal(model.Count, dict.Count);
        }

        var arr = new KeyValuePair<byte[], int>[dict.Count];
        dict.CopyTo(arr, 0);
        Assert.Equal(model.OrderBy(p => p.Key, StringComparer.Ordinal),
            arr.Select(p => new KeyValuePair<string, int>(Encoding.UTF8.GetString(p.Key), p.Value)).OrderBy(p => p.Key, StringComparer.Ordinal));
        Assert.Equal(model.OrderBy(p => p.Key, StringComparer.Ordinal),
            dict.Select(p => new KeyValuePair<string, int>(Encoding.UTF8.GetString(p.Key), p.Value)).OrderBy(p => p.Key, StringComparer.Ordinal));
        Assert.True(dict.Capacity >= dict.Count);
    }

    [Fact]
    public void SpanByteNoRemoveDictionaryEdgeCases()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SpanByteNoRemoveDictionary<int>(-1));
        Assert.Throws<ArgumentNullException>(() => new SpanByteNoRemoveDictionary<int>().CopyTo(null!, 0));
        var dict = new SpanByteNoRemoveDictionary<int>();
        dict["a"u8] = 1;
        var view = new SpanByteNoRemoveDictionaryDebugView<int>(dict);
        Assert.Equal(new[] { new KeyValuePair<string, int>("a", 1) }, view.Items);
        Assert.Throws<ArgumentNullException>(() => new SpanByteNoRemoveDictionaryDebugView<int>(null!));
        IEnumerator enumerator = ((IEnumerable)dict).GetEnumerator();
        Assert.True(enumerator.MoveNext());
        Assert.IsType<KeyValuePair<byte[], int>>(enumerator.Current);
        Assert.False(enumerator.MoveNext());
        enumerator.Reset();
        Assert.True(enumerator.MoveNext());
        ((IDisposable)enumerator).Dispose();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void StructListBehavesLikeList(int seed)
    {
        var rnd = new Random(seed);
        var list = new StructList<int>();
        var model = new List<int>();
        for (var step = 0; step < 3000; step++)
        {
            var v = rnd.Next(50);
            switch (rnd.Next(20))
            {
                case 0:
                    list.Add(v);
                    model.Add(v);
                    break;
                case 1:
                    list.AddRef() = v;
                    model.Add(v);
                    break;
                case 2:
                    Assert.Equal(!model.Contains(v), list.AddUnique(v));
                    if (!model.Contains(v)) model.Add(v);
                    break;
                case 3:
                {
                    var idx = rnd.Next(model.Count + 1);
                    list.Insert(idx) = v;
                    model.Insert(idx, v);
                    break;
                }
                case 4:
                {
                    var idx = rnd.Next(model.Count + 1);
                    list.Insert((uint)idx) = v;
                    model.Insert(idx, v);
                    break;
                }
                case 5:
                    list.Insert(^0) = v;
                    model.Add(v);
                    break;
                case 6:
                    if (model.Count > 0)
                    {
                        var idx = rnd.Next(model.Count);
                        list.RemoveAt(idx);
                        model.RemoveAt(idx);
                    }

                    break;
                case 7:
                    if (model.Count > 0)
                    {
                        list.RemoveAt(^1);
                        model.RemoveAt(model.Count - 1);
                    }

                    break;
                case 8:
                    if (model.Contains(v))
                    {
                        list.RemoveItem(v);
                        model.Remove(v);
                    }

                    break;
                case 9:
                {
                    var count = (uint)rnd.Next(4);
                    list.RepeatAdd(v, count);
                    model.AddRange(Enumerable.Repeat(v, (int)count));
                    break;
                }
                case 10:
                {
                    var items = Enumerable.Range(0, rnd.Next(4)).Select(_ => rnd.Next(50)).ToArray();
                    list.AddRange(items);
                    model.AddRange(items);
                    break;
                }
                case 11:
                {
                    var items = Enumerable.Range(0, rnd.Next(4)).Select(_ => rnd.Next(50)).ToArray();
                    var idx = rnd.Next(model.Count + 1);
                    list.InsertRange(idx, items);
                    model.InsertRange(idx, items);
                    break;
                }
                case 12:
                    if (model.Contains(v))
                    {
                        var items = Enumerable.Range(0, rnd.Next(4)).Select(_ => rnd.Next(50)).ToArray();
                        list.ReplaceItem(v, items);
                        var idx = model.IndexOf(v);
                        model.RemoveAt(idx);
                        model.InsertRange(idx, items);
                    }

                    break;
                case 13:
                    if (model.Contains(v))
                    {
                        list.ReplaceItem(v, v + 100);
                        model[model.IndexOf(v)] = v + 100;
                    }

                    break;
                case 14:
                {
                    var count = (uint)rnd.Next(model.Count + 3);
                    list.SetCount(count);
                    if (count < model.Count) model.RemoveRange((int)count, model.Count - (int)count);
                    else model.AddRange(Enumerable.Repeat(0, (int)count - model.Count));
                    break;
                }
                case 15:
                    if (model.Count > 0)
                    {
                        list.Pop();
                        model.RemoveAt(model.Count - 1);
                    }

                    break;
                case 16:
                    list.Reserve((uint)rnd.Next(100));
                    break;
                case 17:
                    list.Truncate();
                    break;
                case 18:
                    if (rnd.Next(10) == 0)
                    {
                        list.Sort(Comparer<int>.Default);
                        model.Sort();
                    }

                    break;
                default:
                    if (rnd.Next(20) == 0)
                    {
                        if (rnd.Next(2) == 0) list.Clear();
                        else list.ClearAndTruncate();
                        model.Clear();
                    }

                    break;
            }

            Assert.Equal(model.Count, (int)list.Count);
            Assert.Equal(model.IndexOf(v), list.IndexOf(v));
            if (model.Count > 0)
            {
                Assert.Equal(model[^1], list.Last);
                Assert.Equal(model[0], list[0]);
                Assert.Equal(model[0], list[0u]);
                Assert.Equal(model[^1], list[^1]);
            }
        }

        Assert.Equal(model, list.ToArray());
        Assert.Equal(model, list);
        Assert.Equal(model, list.AsReadOnlySpan().ToArray());
        Assert.Equal(model, ((ReadOnlySpan<int>)list).ToArray());
        Assert.Equal(model, ((ReadOnlyMemory<int>)list).ToArray());
        Assert.Equal(model, list.AsReadOnlyMemory().ToArray());
        Assert.Equal(model.All(x => x < 1000), list.All(x => x < 1000));
        Assert.True(list.All(_ => true));
    }

    [Fact]
    public void StructListAddUniqueAcceptsNull()
    {
        var list = new StructList<string?>();
        Assert.True(list.AddUnique("a"));
        Assert.True(list.AddUnique(null));
        Assert.False(list.AddUnique(null));
        Assert.False(list.AddUnique("a"));
        Assert.Equal(new[] { "a", null }, list.ToArray());
    }

    [Fact]
    public void StructListMiscellaneous()
    {
        var list = new StructList<int>();
        Assert.Throws<InvalidOperationException>(() => list.Pop());
        Assert.Throws<InvalidOperationException>(() => GetLast(list));
        Assert.Throws<ArgumentOutOfRangeException>(() => GetAt(list, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => GetAt(list, 0u));
        Assert.Throws<ArgumentOutOfRangeException>(() => InsertAt(list, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => RemoveAtIndex(list, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => list.InsertRange(1, new[] { 1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => list.Reserve(uint.MaxValue));
        Assert.Throws<ArgumentOutOfRangeException>(() => list.RemoveItem(5));
        Assert.Throws<ArgumentOutOfRangeException>(() => list.ReplaceItem(5, new[] { 1, 2 }));
        list.RepeatAdd(1, 0);
        list.AddRange(ReadOnlySpan<int>.Empty);
        list.InsertRange(0, ReadOnlySpan<int>.Empty);
        list.Truncate();
        Assert.Null(list.UnsafeBackingArray);
        list.AddRange(new[] { 3, 1, 2 });
        list.Sort(Comparer<int>.Default);
        Assert.Equal(new[] { 1, 2, 3 }, list.ToArray());
        Assert.Equal(new[] { 2, 3 }, list[1..].ToArray());
        Assert.Equal(new[] { 2, 3 }, list.AsSpan(1).ToArray());
        Assert.Equal(new[] { 2 }, list.AsSpan(1, 1).ToArray());
        Assert.Equal(new[] { 2, 3 }, list.AsReadOnlySpan(1).ToArray());
        Assert.Equal(new[] { 2 }, list.AsReadOnlySpan(1, 1).ToArray());
        var copy = new StructList<int>(list);
        Assert.True(copy.Equals(list));
        Assert.True(copy.Equals((object)list));
        Assert.False(copy.Equals("x"));
        Assert.Equal(list.GetHashCode(), copy.GetHashCode());
        copy[0] = 10;
        Assert.False(copy.Equals(list));
        var empty = new StructList<int>(new StructList<int>());
        Assert.Equal(0u, empty.Count);
        var fromArray = new StructList<int>(new[] { 7, 8, 9 }, 2);
        Assert.Equal(new[] { 7, 8 }, fromArray.ToArray());
        var target = new StructList<int>();
        target.TransferFrom(ref fromArray);
        Assert.Equal(0u, fromArray.Count);
        Assert.Equal(new[] { 7, 8 }, target.ToArray());
        IEnumerator enumerator = ((IEnumerable)target).GetEnumerator();
        Assert.True(enumerator.MoveNext());
        Assert.Equal(7, enumerator.Current);
        enumerator.Reset();
        Assert.True(enumerator.MoveNext());
        Assert.True(enumerator.MoveNext());
        Assert.False(enumerator.MoveNext());
        ((IDisposable)enumerator).Dispose();
        Assert.Equal(new[] { 7, 8 }, ((IEnumerable<int>)target).ToArray());
        target.Reserve(1);
        target.Reserve(10);
        Assert.Equal(10, target.UnsafeBackingArray!.Length);
        Assert.Equal(new[] { 7, 8 }, target.ToArray());
    }

    static int GetLast(StructList<int> l) => l.Last;
    static int GetAt(StructList<int> l, int i) => l[i];
    static int GetAt(StructList<int> l, uint i) => l[i];
    static void InsertAt(StructList<int> l, int i) => l.Insert(i);
    static void RemoveAtIndex(StructList<int> l, int i) => l.RemoveAt(i);

    // Before the fix the default array equalled an empty one but hashed differently.
    [Fact]
    public void DefaultAndEmptyEquatableArraysHashAlike()
    {
        var empty = new EquatableArray<int>([]);
        Assert.True(empty.Equals(default));
        Assert.Equal(default(EquatableArray<int>).GetHashCode(), empty.GetHashCode());
    }

    [Fact]
    public void EquatableArrayHasValueSemantics()
    {
        EquatableArray<int> a = new[] { 1, 2, 3 };
        var b = new EquatableArray<int>([1, 2, 3]);
        EquatableArray<int> c = [1, 2];
        var defaultArray = default(EquatableArray<int>);
        Assert.True(a == b);
        Assert.False(a != b);
        Assert.True(a != c);
        Assert.True(a.Equals((object)b));
        Assert.False(a.Equals("x"));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.Equal(0, defaultArray.GetHashCode());
        Assert.True(defaultArray.IsEmpty);
        Assert.True(EquatableArray<int>.Empty.IsEmpty);
        Assert.True(defaultArray == EquatableArray<int>.Empty);
        Assert.Equal(0, defaultArray.Count);
        Assert.Empty(defaultArray);
        Assert.Empty(((IEnumerable)defaultArray).Cast<int>());
        Assert.Equal(3, a.Count);
        Assert.False(a.IsEmpty);
        Assert.Equal(2, a[1]);
        Assert.Equal(new[] { 1, 2, 3 }, a);
        Assert.Equal(new[] { 1, 2, 3 }, ((IEnumerable)a).Cast<int>());
        Assert.Equal(new[] { 1, 2, 3 }, a.GetArray());
        Assert.Equal(new[] { 1, 2 }, EquatableArray.Create<int>([1, 2]).AsSpan().ToArray());
    }

    [Fact]
    public void ReadOnlyListIndexOfExtension()
    {
        IReadOnlyList<string?> list = new List<string?> { "a", null, "b", "a" };
        Assert.Equal(0, list.IndexOf("a"));
        Assert.Equal(1, list.IndexOf(null));
        Assert.Equal(2, list.IndexOf("b"));
        Assert.Equal(-1, list.IndexOf("c"));
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(5, 8)]
    [InlineData(1000, 1024)]
    [InlineData(1024, 1024)]
    [InlineData(1025, 2048)]
    public void PowerOf2RoundsUp(int input, int expected)
    {
        Assert.Equal(expected, HashHelpers.PowerOf2(input));
    }

    [Fact]
    public void HashHelpersThrowers()
    {
        Assert.Throws<InvalidOperationException>(HashHelpers.ThrowInvalidOperationException_ConcurrentOperationsNotSupported);
        Assert.Throws<ArgumentNullException>(HashHelpers.ThrowKeyArgumentNullException);
        Assert.Throws<ArgumentOutOfRangeException>(HashHelpers.ThrowCapacityArgumentOutOfRangeException);
        Assert.Throws<NotSupportedException>(() => HashHelpers.ThrowNotSupportedException());
    }

    [Fact]
    public void EmptyUtf16Utf8DictionaryLookupsReturnNotFound()
    {
        var dict = new Utf16Utf8Dictionary<int>(0, []);
        Assert.Equal(0, dict.Count);
        Assert.False(dict.ContainsKey("a".AsSpan()));
        Assert.False(dict.ContainsKey("a"u8));
        Assert.Equal(-1, dict.GetIndex("a".AsSpan()));
        Assert.Equal(-1, dict.GetIndex("a"u8));
        Assert.False(dict.TryGetValue("a".AsSpan(), out _));
        Assert.False(dict.TryGetValue("a"u8, out _));
        dict.GetValueRef("a".AsSpan(), out var found);
        Assert.False(found);
        Assert.Empty(dict.Index);
    }

    [Fact]
    public void Utf16Utf8DictionaryValidatesCount()
    {
        Assert.Throws<ArgumentException>(() => new Utf16Utf8Dictionary<int>(3, ["a", "b"]));
        // Before the fix more keys than the count ran past the entries with IndexOutOfRangeException.
        Assert.Throws<ArgumentException>(() => new Utf16Utf8Dictionary<int>(1, ["a", "b"]));
    }

    [Fact]
    public void DerivedUtf16Utf8DictionaryMapsByIndexOfParent()
    {
        var parent = new Utf16Utf8Dictionary<int>(3, ["alpha", "beta", "žluť"]);
        var derived = new DerivedUtf16Utf8Dictionary<string, int>(parent);
        Assert.Same(parent, derived.Parent);
        foreach (var idx in parent.Index)
        {
            derived.ValueRef(idx) = parent.KeyUtf16(idx).ToString().ToUpperInvariant();
        }

        Assert.True(derived.TryGetValue("beta".AsSpan(), out var beta));
        Assert.Equal("BETA", beta);
        Assert.True(derived.TryGetValue("žluť"u8, out var zlut));
        Assert.Equal("ŽLUŤ", zlut);
        Assert.False(derived.TryGetValue("gamma".AsSpan(), out var missing));
        Assert.Null(missing);
        Assert.False(derived.TryGetValue("gamma"u8, out _));
        ref var alpha = ref derived.GetValueRef("alpha".AsSpan(), out var found);
        Assert.True(found);
        alpha = "changed";
        ref var alpha8 = ref derived.GetValueRef("alpha"u8, out found);
        Assert.True(found);
        Assert.Equal("changed", alpha8);
        derived.GetValueRef("x".AsSpan(), out found);
        Assert.False(found);
        derived.GetValueRef("x"u8, out found);
        Assert.False(found);
    }

    [Fact]
    public void Utf16Utf8DictionaryIndexEnumerators()
    {
        var dict = new Utf16Utf8Dictionary<int>(3, ["a", "b", "c"]);
        IEnumerator<uint> e = ((IEnumerable<uint>)dict.Index).GetEnumerator();
        Assert.True(e.MoveNext());
        Assert.Equal(0u, ((IEnumerator)e).Current);
        e.Reset();
        Assert.Equal(3, ((IEnumerable)dict.Index).Cast<uint>().Count());
        e.Dispose();
        dict.ValueRef(1) = 42;
        ref var b = ref dict.GetValueRef("b"u8, out var found);
        Assert.True(found);
        Assert.Equal(42, b);
        Assert.Equal("b"u8.ToArray(), dict.KeyUtf8(1).ToArray());
    }
}
