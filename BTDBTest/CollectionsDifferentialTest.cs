using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.Collections;
using Xunit;

namespace BTDBTest;

// Compares RefDictionary with Dictionary and StructList with List under random operations.
public class CollectionsDifferentialTest
{
    public static IEnumerable<object[]> Seeds() => Enumerable.Range(1, 40).Select(seed => new object[] { seed });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void RefDictionaryMatchesDictionary(int seed)
    {
        var random = new Random(seed);
        var dictionary = new RefDictionary<int, int>(random.Next(0, 5));
        var reference = new Dictionary<int, int>();
        var keyCount = random.Next(4, 200);
        for (var step = 0; step < 4000; step++)
        {
            var key = random.Next(keyCount) - keyCount / 4;
            switch (random.Next(10))
            {
                case 0:
                case 1:
                    Assert.Equal(reference.TryAdd(key, step), dictionary.TryAdd(key, step));
                    break;
                case 2:
                case 3:
                    dictionary.GetOrAddValueRef(key) = step;
                    reference[key] = step;
                    break;
                case 4:
                case 5:
                    Assert.Equal(reference.Remove(key), dictionary.Remove(key));
                    break;
                case 6:
                {
                    Assert.Equal(reference.TryGetValue(key, out var expected), dictionary.TryGetValue(key, out var value));
                    Assert.Equal(expected, value);
                    Assert.Equal(reference.ContainsKey(key), dictionary.ContainsKey(key));
                    break;
                }
                case 7:
                {
                    ref var value = ref dictionary.GetOrFakeValueRef(key, out var found);
                    Assert.Equal(reference.ContainsKey(key), found);
                    if (found) Assert.Equal(reference[key], value);
                    break;
                }
                case 8:
                    if (random.Next(50) == 0)
                    {
                        dictionary.Clear();
                        reference.Clear();
                    }

                    break;
                default:
                {
                    var byIndex = new List<KeyValuePair<int, int>>();
                    foreach (var index in dictionary.Index)
                        byIndex.Add(new(dictionary.KeyRef(index), dictionary.ValueRef(index)));
                    Assert.Equal(reference.OrderBy(p => p.Key), byIndex.OrderBy(p => p.Key));
                    break;
                }
            }

            Assert.Equal(reference.Count, dictionary.Count);
        }

        Assert.Equal(reference.OrderBy(p => p.Key), dictionary.OrderBy(p => p.Key));
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void StructListMatchesList(int seed)
    {
        var random = new Random(seed);
        var list = new StructList<int>();
        var reference = new List<int>();
        for (var step = 0; step < 3000; step++)
        {
            switch (random.Next(12))
            {
                case 0:
                case 1:
                    list.Add(step);
                    reference.Add(step);
                    break;
                case 2:
                {
                    var index = random.Next(reference.Count + 1);
                    list.Insert(index) = step;
                    reference.Insert(index, step);
                    break;
                }
                case 3:
                {
                    var index = random.Next(reference.Count + 1);
                    var values = Enumerable.Range(step, random.Next(4)).ToArray();
                    list.InsertRange(index, values);
                    reference.InsertRange(index, values);
                    break;
                }
                case 4:
                    if (reference.Count == 0) break;
                    var removeAt = random.Next(reference.Count);
                    list.RemoveAt(removeAt);
                    reference.RemoveAt(removeAt);
                    break;
                case 5:
                    if (reference.Count == 0) break;
                    list.Pop();
                    reference.RemoveAt(reference.Count - 1);
                    break;
                case 6:
                {
                    if (reference.Count == 0) break;
                    var item = reference[random.Next(reference.Count)];
                    var values = Enumerable.Range(-step - 10, random.Next(4)).ToArray();
                    list.ReplaceItem(item, values);
                    var at = reference.IndexOf(item);
                    reference.RemoveAt(at);
                    reference.InsertRange(at, values);
                    break;
                }
                case 7:
                {
                    var values = Enumerable.Range(step, random.Next(5)).ToArray();
                    list.AddRange(values);
                    reference.AddRange(values);
                    break;
                }
                case 8:
                    if (random.Next(4) == 0) list.Truncate();
                    else list.Reserve((uint)random.Next(0, reference.Count * 2 + 2));
                    break;
                case 9:
                    if (reference.Count == 0) break;
                    var removed = reference[random.Next(reference.Count)];
                    list.RemoveItem(removed);
                    reference.Remove(removed);
                    break;
                case 10:
                {
                    var probe = random.Next(-20, step + 1);
                    Assert.Equal(reference.IndexOf(probe), list.IndexOf(probe));
                    break;
                }
                default:
                    if (random.Next(40) == 0)
                    {
                        list.Clear();
                        reference.Clear();
                    }

                    break;
            }

            Assert.Equal((uint)reference.Count, list.Count);
            Assert.True(System.Runtime.InteropServices.CollectionsMarshal.AsSpan(reference).SequenceEqual(list.AsReadOnlySpan()), $"step {step}");
        }
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public void SpanByteNoRemoveDictionaryMatchesDictionary(int seed)
    {
        var random = new Random(seed);
        var dictionary = new SpanByteNoRemoveDictionary<int>(random.Next(0, 5));
        var reference = new Dictionary<string, int>();
        var keyCount = random.Next(4, 300);
        for (var step = 0; step < 3000; step++)
        {
            // Keys of varied length, the empty one included.
            var key = random.Next(10) == 0 ? "" : new string('k', random.Next(keyCount) % 9) + random.Next(keyCount);
            var bytes = System.Text.Encoding.UTF8.GetBytes(key);
            switch (random.Next(4))
            {
                case 0:
                case 1:
                    dictionary.GetOrAddValueRef(bytes) = step;
                    reference[key] = step;
                    break;
                case 2:
                {
                    Assert.Equal(reference.TryGetValue(key, out var expected), dictionary.TryGetValue(bytes, out var value));
                    Assert.Equal(expected, value);
                    Assert.Equal(reference.ContainsKey(key), dictionary.ContainsKey(bytes));
                    break;
                }
                default:
                    if (reference.TryGetValue(key, out var existing)) Assert.Equal(existing, dictionary[bytes]);
                    break;
            }

            Assert.Equal(reference.Count, dictionary.Count);
        }

        Assert.Equal(reference.OrderBy(p => p.Key, StringComparer.Ordinal),
            dictionary.Select(p => new KeyValuePair<string, int>(System.Text.Encoding.UTF8.GetString(p.Key), p.Value))
                .OrderBy(p => p.Key, StringComparer.Ordinal));
        var copy = new KeyValuePair<byte[], int>[dictionary.Count + 1];
        dictionary.CopyTo(copy, 1);
        Assert.Equal(reference.OrderBy(p => p.Key, StringComparer.Ordinal),
            copy.Skip(1).Select(p => new KeyValuePair<string, int>(System.Text.Encoding.UTF8.GetString(p.Key), p.Value))
                .OrderBy(p => p.Key, StringComparer.Ordinal));
    }

    // Before the fix Pop, Clear and shrinking SetCount kept the removed items reachable from the backing array.
    [Fact]
    public void StructListDoesNotKeepRemovedReferences()
    {
        var list = new StructList<object>();
        for (var i = 0; i < 4; i++) list.Add(new object());
        list.Pop();
        Assert.Null(list.UnsafeBackingArray![3]);
        list.SetCount(1);
        Assert.Null(list.UnsafeBackingArray![1]);
        Assert.Null(list.UnsafeBackingArray![2]);
        Assert.NotNull(list.UnsafeBackingArray![0]);
        list.Clear();
        Assert.Null(list.UnsafeBackingArray![0]);
    }

    // Before the fix reserving less than the capacity, but more than the count, shrank the array to it.
    [Fact]
    public void StructListReserveNeverShrinksCapacity()
    {
        var list = new StructList<int>();
        list.Reserve(100);
        list.Add(1);
        list.Reserve(10);
        Assert.Equal(100, list.UnsafeBackingArray!.Length);
        Assert.Equal(new[] { 1 }, list.ToArray());
    }

    // Before the fix InsertRange resized the array on every call, so it shrank the capacity Reserve made.
    [Fact]
    public void StructListInsertRangeKeepsReservedCapacity()
    {
        var list = new StructList<int>();
        list.Reserve(100);
        list.Add(1);
        list.Add(2);
        list.InsertRange(1, [5, 6]);
        Assert.Equal(new[] { 1, 5, 6, 2 }, list.ToArray());
        Assert.Equal(100, list.UnsafeBackingArray!.Length);
        // A missing item throws like ReplaceItem with a span and RemoveItem do.
        Assert.Throws<ArgumentOutOfRangeException>(() => list.ReplaceItem(99, 7));
    }
}
