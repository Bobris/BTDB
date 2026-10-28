using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ODBDictionaryCoverageTest : FieldHandlerCoverageTestBase
{
    [Generate]
    public class DictRoot
    {
        public IOrderedDictionary<int, string> A { get; set; } = null!;
        public IOrderedDictionary<int, string> B { get; set; } = null!;
        public IDictionary<ulong, IDictionary<int, int>> Nested { get; set; } = null!;
        public IOrderedDictionary<int, List<IDictionary<int, int>>> NestedList { get; set; } = null!;
    }

    [Fact]
    public void KeyValuePairCollectionApi()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<DictRoot>();
        ICollection<KeyValuePair<int, string>> coll = root.A;
        coll.Add(new(2, "b"));
        coll.Add(new(1, "a"));
        Assert.Throws<ArgumentException>(() => coll.Add(new(1, "x")));
        Assert.False(coll.IsReadOnly);
        Assert.True(coll.Contains(new(1, "a")));
        Assert.False(coll.Contains(new(1, "b")));
        Assert.False(coll.Contains(new(3, "a")));
        Assert.False(coll.Remove(new(1, "b")));
        Assert.Equal(2, coll.Count);
        Assert.True(coll.Remove(new(1, "a")));
        Assert.Equal(new[] { new KeyValuePair<int, string>(2, "b") }, coll);
        Assert.Throws<ArgumentNullException>(() => coll.CopyTo(null!, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => coll.CopyTo(new KeyValuePair<int, string>[1], -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => coll.CopyTo(new KeyValuePair<int, string>[1], 2));
        Assert.Throws<ArgumentException>(() => coll.CopyTo(new KeyValuePair<int, string>[1], 1));
        var array = new KeyValuePair<int, string>[2];
        coll.CopyTo(array, 1);
        Assert.Equal(new KeyValuePair<int, string>(2, "b"), array[1]);
        var nonGeneric = ((IEnumerable)root.A).Cast<KeyValuePair<int, string>>().ToList();
        Assert.Single(nonGeneric);
        Assert.Throws<ArgumentException>(() => root.A[42]);
        Assert.False(root.A.TryGetValue(42, out var missing));
        Assert.Null(missing);
    }

    [Fact]
    public void KeysCollectionApi()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<DictRoot>();
        root.B[100] = "other";
        var keys = root.A.Keys;
        Assert.Same(keys, root.A.Keys);
        keys.Add(3);
        keys.Add(1);
        Assert.Null(root.A[3]);
        Assert.False(keys.IsReadOnly);
        Assert.True(keys.Contains(1));
        Assert.False(keys.Contains(2));
        Assert.Equal(2, keys.Count);
        Assert.Equal(new[] { 1, 3 }, ((IEnumerable)keys).Cast<int>());
        Assert.Throws<ArgumentNullException>(() => keys.CopyTo(null!, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => keys.CopyTo(new int[3], -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => keys.CopyTo(new int[3], 4));
        Assert.Throws<ArgumentException>(() => keys.CopyTo(new int[3], 2));
        var array = new int[3];
        keys.CopyTo(array, 1);
        Assert.Equal(new[] { 0, 1, 3 }, array);
        Assert.True(keys.Remove(1));
        Assert.False(keys.Remove(1));
        Assert.Equal(new[] { 3 }, keys);
        keys.Clear();
        Assert.Empty(root.A);
        Assert.Equal("other", root.B[100]);
    }

    [Fact]
    public void ValuesCollectionApi()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<DictRoot>();
        root.A[2] = "two";
        root.A[1] = "one";
        var values = root.A.Values;
        Assert.Same(values, root.A.Values);
        Assert.True(values.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => values.Add("x"));
        Assert.Throws<NotSupportedException>(() => values.Remove("one"));
        Assert.True(values.Contains("two"));
        Assert.False(values.Contains("three"));
        Assert.Equal(2, values.Count);
        Assert.Equal(new[] { "one", "two" }, ((IEnumerable)values).Cast<string>());
        Assert.Throws<ArgumentNullException>(() => values.CopyTo(null!, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => values.CopyTo(new string[3], -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => values.CopyTo(new string[3], 4));
        Assert.Throws<ArgumentException>(() => values.CopyTo(new string[3], 2));
        var array = new string[3];
        values.CopyTo(array, 1);
        Assert.Equal(new[] { null, "one", "two" }, array);
        values.Clear();
        Assert.Empty(root.A);
    }

    static List<int> ReferenceRange(SortedDictionary<int, string> dict, AdvancedEnumeratorParam<int> param)
    {
        IEnumerable<int> res = dict.Keys;
        if (param.StartProposition == KeyProposition.Included) res = res.Where(v => v >= param.Start);
        if (param.StartProposition == KeyProposition.Excluded) res = res.Where(v => v > param.Start);
        if (param.EndProposition == KeyProposition.Included) res = res.Where(v => v <= param.End);
        if (param.EndProposition == KeyProposition.Excluded) res = res.Where(v => v < param.End);
        if (param.Order == EnumerationOrder.Descending) res = res.Reverse();
        return res.ToList();
    }

    static IEnumerable<AdvancedEnumeratorParam<int>> AllParams()
    {
        var propositions = new[] { KeyProposition.Ignored, KeyProposition.Included, KeyProposition.Excluded };
        foreach (var order in new[] { EnumerationOrder.Ascending, EnumerationOrder.Descending })
        foreach (var sp in propositions)
        foreach (var ep in propositions)
        foreach (var start in new[] { 0, 3, 4, 9, 10 })
        foreach (var end in new[] { 0, 1, 4, 5, 10 })
            yield return new(order, start, sp, end, ep);
    }

    [Fact]
    public void AdvancedEnumeratorMatchesReference()
    {
        var reference = new SortedDictionary<int, string>();
        foreach (var i in new[] { 1, 3, 5, 7, 9 }) reference[i] = "v" + i;
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<DictRoot>();
        root.B[-1] = "x";
        root.B[11] = "y";
        foreach (var p in reference) root.A[p.Key] = p.Value;
        foreach (var param in AllParams())
        {
            var expected = ReferenceRange(reference, param);
            using var enumerator = root.A.GetAdvancedEnumerator(param);
            Assert.Equal((uint)expected.Count, enumerator.Count);
            var actual = new List<int>();
            while (enumerator.NextKey(out var key))
            {
                Assert.Equal(reference[key], enumerator.CurrentValue);
                actual.Add(key);
            }

            Assert.False(enumerator.NextKey(out _));
            Assert.Equal(0u, enumerator.Position);
            Assert.True(expected.SequenceEqual(actual),
                $"{param.Order} {param.StartProposition} {param.Start} {param.EndProposition} {param.End}: expected {string.Join(",", expected)} actual {string.Join(",", actual)}");
            if (expected.Count > 1)
            {
                using var enumerator2 = root.A.GetAdvancedEnumerator(param);
                enumerator2.Position = 1;
                Assert.Throws<BTDBException>(() => enumerator2.CurrentValue);
                Assert.Throws<BTDBException>(() => enumerator2.CurrentValue = "z");
                Assert.True(enumerator2.NextKey(out var second));
                Assert.Equal(expected[1], second);
                Assert.Equal(1u, enumerator2.Position);
                enumerator2.Position = 1000;
                Assert.False(enumerator2.NextKey(out _));
                Assert.Throws<IndexOutOfRangeException>(() => enumerator2.CurrentValue);
                Assert.Throws<IndexOutOfRangeException>(() => enumerator2.CurrentValue = "z");
            }
        }
    }

    [Fact]
    public void AdvancedEnumeratorReleasesRangeAfterReachingEnd()
    {
        // Documents current (intentional, see ObjectDbTest.AdvancedIterationBasics) behavior: after enumeration
        // reaches end Count and Position drop to 0 and the enumerator cannot be rewound.
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<DictRoot>();
        for (var i = 0; i < 3; i++) root.A[i] = "v" + i;
        using var enumerator = root.A.GetAdvancedEnumerator(new(EnumerationOrder.Ascending));
        Assert.Equal(3u, enumerator.Count);
        enumerator.Position = 2;
        Assert.True(enumerator.NextKey(out var key));
        Assert.Equal(2, key);
        Assert.False(enumerator.NextKey(out _));
        Assert.Equal(0u, enumerator.Count);
        Assert.Equal(0u, enumerator.Position);
        enumerator.Position = 0;
        Assert.False(enumerator.NextKey(out _));
    }

    [Fact]
    public void AdvancedEnumeratorCanUpdateValues()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<DictRoot>();
        for (var i = 0; i < 5; i++) root.A[i] = "v" + i;
        using (var enumerator = root.A.GetAdvancedEnumerator(new(EnumerationOrder.Descending)))
        {
            while (enumerator.NextKey(out var key))
            {
                enumerator.CurrentValue = "u" + key;
            }
        }

        Assert.Equal(new[] { "u0", "u1", "u2", "u3", "u4" }, root.A.Values);
    }

    [Fact]
    public void RemoveRangeMatchesReference()
    {
        foreach (var param in AllParams())
        {
            if (param.StartProposition == KeyProposition.Ignored || param.EndProposition == KeyProposition.Ignored)
                continue;
            var reference = new SortedDictionary<int, string>();
            foreach (var i in new[] { 1, 3, 5, 7, 9 }) reference[i] = "v" + i;
            using var tr = _db.StartTransaction();
            var root = tr.Singleton<DictRoot>();
            root.B.Clear();
            root.B[0] = "b0";
            root.B[4] = "b4";
            root.B[10] = "b10";
            root.A.Clear();
            foreach (var p in reference) root.A.Add(p.Key, p.Value);
            var toRemove = ReferenceRange(reference, param);
            var removed = root.A.RemoveRange(param.Start, param.StartProposition == KeyProposition.Included,
                param.End, param.EndProposition == KeyProposition.Included);
            foreach (var r in toRemove) reference.Remove(r);
            Assert.Equal(toRemove.Count, removed);
            Assert.Equal(reference, root.A);
            Assert.Equal(reference.Count, root.A.Count);
            Assert.Equal(new[] { 0, 4, 10 }, root.B.Keys);
        }
    }

    [Fact]
    public void IncreasingAndDecreasingEnumeratorsMatchReference()
    {
        var reference = new SortedDictionary<int, string>();
        foreach (var i in new[] { 2, 4, 6 }) reference[i] = "v" + i;
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<DictRoot>();
        root.B[-5] = "x";
        root.B[50] = "x";
        Assert.Empty(root.A.GetIncreasingEnumerator(0));
        Assert.Empty(root.A.GetDecreasingEnumerator(0));
        foreach (var p in reference) root.A.Add(p);
        for (var start = 0; start < 8; start++)
        {
            Assert.Equal(reference.Where(p => p.Key >= start), root.A.GetIncreasingEnumerator(start));
            Assert.Equal(reference.Where(p => p.Key <= start).Reverse(), root.A.GetDecreasingEnumerator(start));
        }
    }

    [Fact]
    public void UpsertSupportsNonGenericDictionary()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<DictRoot>();
        var internalDict = (IInternalODBDictionary)root.A;
        internalDict.Upsert(null);
        Assert.Empty(root.A);
        internalDict.Upsert(new Hashtable { { 1, "a" }, { 2, "b" } });
        internalDict.Upsert(new Dictionary<int, string> { { 2, "c" } });
        Assert.Equal(new[] { "a", "c" }, root.A.Values);
        Assert.Equal(2, internalDict.Count);
    }

    [Fact]
    public void ReplacingNestedDictionariesFreesOnlyUnusedOnes()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<DictRoot>();
            var list = new List<IDictionary<int, int>>();
            for (var i = 0; i < 20; i++) list.Add(new Dictionary<int, int> { { i, i } });
            root.NestedList[1] = list;
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<DictRoot>();
            var list = root.NestedList[1];
            Assert.Equal(20, list.Count);
            Assert.Equal(19, list[19][19]);
            // keep 18 of original nested dictionaries (>= 16 => hash set path) and add a new one
            var newList = list.Take(18).ToList();
            newList.Add(new Dictionary<int, int> { { 100, 100 } });
            root.NestedList[1] = newList;
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<DictRoot>();
            var list = root.NestedList[1];
            Assert.Equal(19, list.Count);
            for (var i = 0; i < 18; i++) Assert.Equal(i, list[i][i]);
            Assert.Equal(100, list[18][100]);
        }

        AssertNoLeaks();
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<DictRoot>();
            root.NestedList.Clear();
            tr.Commit();
        }

        AssertNoLeaks();
    }

    void AssertNoLeaks()
    {
        using var visitor = new FindUnusedKeysVisitor();
        using var tr = _db.StartReadOnlyTransaction();
        visitor.ImportAllKeys(tr);
        visitor.Iterate(tr);
        Assert.Empty(visitor.UnseenKeys());
    }

    /// Removing each listed key while enumerating a range stays within the range, also when the removed key is the
    /// bound the enumeration stops at.
    [Theory]
    [InlineData(EnumerationOrder.Ascending)]
    [InlineData(EnumerationOrder.Descending)]
    public void RemovingListedKeysKeepsAdvancedEnumerationInRange(EnumerationOrder order)
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<DictRoot>();
        for (var i = 1; i <= 5; i++) root.A[i] = "v" + i;
        var listed = new List<int>();
        using var enumerator = root.A.GetAdvancedEnumerator(
            new(order, 2, KeyProposition.Included, 4, KeyProposition.Included));
        while (enumerator.NextKey(out var key))
        {
            listed.Add(key);
            Assert.True(root.A.Remove(key));
        }

        Assert.Equal(order == EnumerationOrder.Ascending ? new[] { 2, 3, 4 } : new[] { 4, 3, 2 }, listed);
        Assert.Equal(new[] { 1, 5 }, root.A.Keys.Order());
    }
}
