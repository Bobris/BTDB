using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ODBSetCoverageTest : FieldHandlerCoverageTestBase
{
    [Generate]
    public class SetsRoot
    {
        public IOrderedSet<int> A { get; set; } = null!;
        public IOrderedSet<int> B { get; set; } = null!;
        public IOrderedSet<string> Strings { get; set; } = null!;
        public IOrderedSet<byte[]> Bytes { get; set; } = null!;
    }

    [Fact]
    public void BasicOperationsMatchSortedSet()
    {
        var reference = new SortedSet<int>();
        var rnd = new Random(42);
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SetsRoot>();
            root.B.Add(1000);
            for (var i = 0; i < 300; i++)
            {
                var v = rnd.Next(-50, 50);
                switch (rnd.Next(3))
                {
                    case 0:
                    case 1:
                        Assert.Equal(reference.Add(v), root.A.Add(v));
                        break;
                    default:
                        Assert.Equal(reference.Remove(v), root.A.Remove(v));
                        break;
                }

                Assert.Equal(reference.Count, root.A.Count);
            }

            Assert.Equal(reference, root.A);
            for (var v = -51; v <= 51; v++)
                Assert.Equal(reference.Contains(v), root.A.Contains(v));
            Assert.Equal(new[] { 1000 }, root.B);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var root = tr.Singleton<SetsRoot>();
            Assert.Equal(reference, root.A);
            Assert.Equal(reference.Count, root.A.Count);
            Assert.Equal(reference.Reverse(), root.A.GetReverseEnumerator());
            Assert.False(root.A.IsReadOnly);
            var nonGeneric = new List<int>();
            foreach (var o in (IEnumerable)root.A) nonGeneric.Add((int)o!);
            Assert.Equal(reference, nonGeneric);
            var ids = new List<int>();
            ((IInternalODBSet)root.A).Iterate((ref byte item) =>
                ids.Add(System.Runtime.CompilerServices.Unsafe.As<byte, int>(ref item)));
            Assert.Equal(reference, ids);
            Assert.Equal(reference.Count, ((IInternalODBSet)root.A).Count);
            Assert.NotEqual(((IInternalODBSet)root.A).DictId, ((IInternalODBSet)root.B).DictId);
        }
    }

    [Fact]
    public void IncreasingAndDecreasingEnumeratorsMatchReference()
    {
        var reference = new SortedSet<int> { 2, 4, 6, 8 };
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        root.B.Add(-100);
        root.B.Add(100);
        foreach (var i in reference) root.A.Add(i);
        for (var start = 0; start <= 10; start++)
        {
            Assert.Equal(reference.Where(v => v >= start), root.A.GetIncreasingEnumerator(start));
            Assert.Equal(reference.Where(v => v <= start).Reverse(), root.A.GetDecreasingEnumerator(start));
        }

        root.A.Clear();
        Assert.Empty(root.A.GetIncreasingEnumerator(0));
        Assert.Empty(root.A.GetDecreasingEnumerator(0));
        Assert.Empty(root.A.GetReverseEnumerator());
        Assert.Equal(new[] { -100, 100 }, root.B);
    }

    static IEnumerable<int> ReferenceRange(SortedSet<int> set, AdvancedEnumeratorParam<int> param)
    {
        IEnumerable<int> res = set;
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
        foreach (var start in new[] { 0, 3, 4, 5, 9, 10 })
        foreach (var end in new[] { 0, 1, 4, 5, 7, 8, 10 })
            yield return new(order, start, sp, end, ep);
    }

    [Fact]
    public void AdvancedEnumeratorMatchesReference()
    {
        var reference = new SortedSet<int> { 1, 3, 5, 7, 9 };
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        // neighbour set makes sure prefix boundaries are respected
        root.B.Add(-1);
        root.B.Add(int.MaxValue);
        foreach (var i in reference) root.A.Add(i);
        foreach (var param in AllParams())
        {
            var expected = ReferenceRange(reference, param);
            var actual = root.A.GetAdvancedEnumerator(param).ToList();
            Assert.True(expected.SequenceEqual(actual),
                $"{param.Order} {param.StartProposition} {param.Start} {param.EndProposition} {param.End}: expected {string.Join(",", expected)} actual {string.Join(",", actual)}");
        }

        var enumerator = root.A.GetAdvancedEnumerator(new()).GetEnumerator();
        Assert.Throws<NotSupportedException>(() => enumerator.Reset());
        Assert.True(enumerator.MoveNext());
        Assert.Equal(1, ((IEnumerator)enumerator).Current);
        enumerator.Dispose();
        var nonGeneric = ((IEnumerable)root.A.GetAdvancedEnumerator(new(EnumerationOrder.Descending)))
            .GetEnumerator();
        Assert.True(nonGeneric.MoveNext());
        Assert.Equal(9, nonGeneric.Current);
        ((IDisposable)nonGeneric).Dispose();
    }

    [Theory]
    [InlineData(EnumerationOrder.Ascending)]
    [InlineData(EnumerationOrder.Descending)]
    public void AdvancedEnumeratorKeepsReturningFalseAfterEnd(EnumerationOrder order)
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        root.A.Add(1);
        root.A.Add(2);
        using var enumerator = root.A.GetAdvancedEnumerator(new(order)).GetEnumerator();
        Assert.True(enumerator.MoveNext());
        Assert.True(enumerator.MoveNext());
        Assert.False(enumerator.MoveNext());
        Assert.False(enumerator.MoveNext());
        Assert.False(enumerator.MoveNext());
    }

    [Fact]
    public void AdvancedEnumeratorOnEmptySetReturnsNothing()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        root.B.Add(5);
        foreach (var param in AllParams())
        {
            Assert.Empty(root.A.GetAdvancedEnumerator(param));
        }
    }

    [Fact]
    public void RemoveRangeMatchesReference()
    {
        foreach (var param in AllParams())
        {
            var reference = new SortedSet<int> { 1, 3, 5, 7, 9 };
            using var tr = _db.StartTransaction();
            var root = tr.Singleton<SetsRoot>();
            root.B.Clear();
            root.B.Add(0);
            root.B.Add(4);
            root.B.Add(10);
            root.A.Clear();
            foreach (var i in reference) root.A.Add(i);
            var toRemove = ReferenceRange(reference, param).ToList();
            var removed = root.A.RemoveRange(param);
            foreach (var r in toRemove) reference.Remove(r);
            Assert.True(toRemove.Count == removed,
                $"{param.StartProposition} {param.Start} {param.EndProposition} {param.End}: expected {toRemove.Count} actual {removed}");
            Assert.Equal(reference, root.A);
            Assert.Equal(reference.Count, root.A.Count);
            Assert.Equal(new[] { 0, 4, 10 }, root.B);
        }
    }

    [Fact]
    public void RemoveRangeOnEmptySetReturnsZero()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        root.B.Add(1);
        Assert.Equal(0, root.A.RemoveRange(new()));
        Assert.Equal(0, root.A.RemoveRange(new(EnumerationOrder.Ascending, 1, KeyProposition.Included, 2,
            KeyProposition.Included)));
        Assert.Equal(new[] { 1 }, root.B);
    }

    [Fact]
    public void SetOperationsWorkOrThrowNotSupported()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        var set = root.A;
        set.UnionWith(new[] { 5, 1, 3, 1 });
        Assert.Equal(new[] { 1, 3, 5 }, set);
        set.ExceptWith(new[] { 3, 4 });
        Assert.Equal(new[] { 1, 5 }, set);
        ((ICollection<int>)set).Add(2);
        Assert.Equal(new[] { 1, 2, 5 }, set);
        Assert.Throws<NotSupportedException>(() => set.IntersectWith(new[] { 1 }));
        Assert.Throws<NotSupportedException>(() => set.IsProperSubsetOf(new[] { 1 }));
        Assert.Throws<NotSupportedException>(() => set.IsProperSupersetOf(new[] { 1 }));
        Assert.Throws<NotSupportedException>(() => set.IsSubsetOf(new[] { 1 }));
        Assert.Throws<NotSupportedException>(() => set.IsSupersetOf(new[] { 1 }));
        Assert.Throws<NotSupportedException>(() => set.Overlaps(new[] { 1 }));
        Assert.Throws<NotSupportedException>(() => set.SetEquals(new[] { 1 }));
        Assert.Throws<NotSupportedException>(() => set.SymmetricExceptWith(new[] { 1 }));
    }

    [Fact]
    public void CopyToValidatesArguments()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        root.A.Add(2);
        root.A.Add(1);
        Assert.Throws<ArgumentNullException>(() => root.A.CopyTo(null!, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => root.A.CopyTo(new int[2], -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => root.A.CopyTo(new int[2], 3));
        Assert.Throws<ArgumentException>(() => root.A.CopyTo(new int[2], 1));
        var array = new int[4];
        root.A.CopyTo(array, 1);
        Assert.Equal(new[] { 0, 1, 2, 0 }, array);
    }

    [Fact]
    public void QuerySizeWorks()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        root.Strings.Add("a");
        root.Strings.Add(new string('x', 100));
        var sizes = ((IQuerySizeDictionary<string>)root.Strings).QuerySizeEnumerator().ToList();
        Assert.Equal(2, sizes.Count);
        Assert.All(sizes, s => Assert.Equal(0u, s.Value));
        Assert.True(sizes[1].Key > sizes[0].Key + 90);
        var bigSize = ((IQuerySizeDictionary<string>)root.Strings).QuerySizeByKey(new string('x', 100));
        Assert.Equal(sizes[1], bigSize);
        Assert.Throws<ArgumentException>(() => ((IQuerySizeDictionary<string>)root.Strings).QuerySizeByKey("b"));
    }

    [Fact]
    public void ByteArrayKeysAreOrderedAndRoundTrip()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SetsRoot>();
            Assert.True(root.Bytes.Add(new byte[] { 2 }));
            Assert.True(root.Bytes.Add(new byte[] { 1, 255 }));
            Assert.True(root.Bytes.Add(new byte[] { 1 }));
            Assert.True(root.Bytes.Add(Array.Empty<byte>()));
            Assert.False(root.Bytes.Add(new byte[] { 1 }));
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SetsRoot>();
            Assert.Equal(new[] { "", "01", "01FF", "02" }, root.Bytes.Select(Convert.ToHexString));
            Assert.True(root.Bytes.Contains(new byte[] { 1, 255 }));
            Assert.False(root.Bytes.Contains(new byte[] { 1, 254 }));
            Assert.True(root.Bytes.Remove(new byte[] { 1 }));
            Assert.Equal(new[] { "02", "01FF", "" }, root.Bytes.GetReverseEnumerator().Select(Convert.ToHexString));
        }
    }

    [Fact]
    public void AssigningForeignSetCopiesContent()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SetsRoot>();
            var orderedSet = new OrderedSet<int> { 3, 1, 2 };
            root.A = orderedSet;
            root.B = null!;
            tr.Store(root);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SetsRoot>();
            Assert.Equal(new[] { 1, 2, 3 }, root.A);
            Assert.Empty(root.B);
            Assert.IsType<ODBSet<int>>(root.B);
        }
    }

    [Fact]
    public void OrderedSetHelperClassThrowsForOrderedOperations()
    {
        var set = new OrderedSet<int> { 1 };
        Assert.Throws<NotSupportedException>(() => set.GetReverseEnumerator());
        Assert.Throws<NotSupportedException>(() => set.GetIncreasingEnumerator(0));
        Assert.Throws<NotSupportedException>(() => set.GetDecreasingEnumerator(0));
        Assert.Throws<NotSupportedException>(() => set.RemoveRange(new()));
        Assert.Throws<NotSupportedException>(() => set.GetAdvancedEnumerator(new()));
    }

    [Fact]
    public void UpsertAddsAllItems()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        var internalSet = (IInternalODBSet)root.A;
        internalSet.Upsert(null);
        Assert.Empty(root.A);
        internalSet.Upsert(new List<int> { 3, 1 });
        internalSet.Upsert(new ArrayList { 2, 3 });
        Assert.Equal(new[] { 1, 2, 3 }, root.A);
    }

    static int CountOf<T>(ICollection<T> collection) => collection.Count;

    [Fact]
    public void ModificationsAreVisibleInCountAfterClear()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        root.A.Add(1);
        root.A.Add(2);
        Assert.Equal(2, root.A.Count);
        root.A.Clear();
        Assert.Equal(0, CountOf(root.A));
        root.A.Add(7);
        Assert.Equal(1, CountOf(root.A));
        Assert.False(root.A.Remove(8));
        Assert.True(root.A.Remove(7));
        Assert.Equal(0, CountOf(root.A));
    }

    /// Removing each listed item while enumerating a range stays within the range, also when the removed item is the
    /// bound the enumeration stops at.
    [Theory]
    [InlineData(EnumerationOrder.Ascending)]
    [InlineData(EnumerationOrder.Descending)]
    public void RemovingListedItemsKeepsAdvancedEnumerationInRange(EnumerationOrder order)
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        for (var i = 1; i <= 5; i++) root.A.Add(i);
        var listed = new List<int>();
        foreach (var item in root.A.GetAdvancedEnumerator(
                     new(order, 2, KeyProposition.Included, 4, KeyProposition.Included)))
        {
            listed.Add(item);
            Assert.True(root.A.Remove(item));
        }

        Assert.Equal(order == EnumerationOrder.Ascending ? new[] { 2, 3, 4 } : new[] { 4, 3, 2 }, listed);
        Assert.Equal(new[] { 1, 5 }, root.A.Order());
    }

    [Theory]
    [InlineData("forward")]
    [InlineData("reverse")]
    [InlineData("increasing")]
    [InlineData("decreasing")]
    [InlineData("advanced descending")]
    public void RemovingEachListedItemListsEveryItemOnce(string scan)
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        for (var i = 1; i <= 9; i++) root.A.Add(i);
        var (items, expected) = scan switch
        {
            "forward" => ((IEnumerable<int>)root.A, Enumerable.Range(1, 9).ToArray()),
            "reverse" => (root.A.GetReverseEnumerator(), Enumerable.Range(1, 9).Reverse().ToArray()),
            "increasing" => (root.A.GetIncreasingEnumerator(4), Enumerable.Range(4, 6).ToArray()),
            "decreasing" => (root.A.GetDecreasingEnumerator(6), Enumerable.Range(1, 6).Reverse().ToArray()),
            _ => (root.A.GetAdvancedEnumerator(new(EnumerationOrder.Descending, 0, KeyProposition.Ignored, 0,
                KeyProposition.Ignored)), Enumerable.Range(1, 9).Reverse().ToArray())
        };
        var listed = new List<int>();
        foreach (var item in items)
        {
            listed.Add(item);
            Assert.True(root.A.Remove(item));
        }

        Assert.Equal(expected, listed);
    }

    /// Adding items next to the listed ones while enumerating does not repeat or skip the items listed before.
    [Theory]
    [InlineData("forward")]
    [InlineData("reverse")]
    public void AddingItemsWhileEnumeratingKeepsListedOnesOnce(string scan)
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<SetsRoot>();
        for (var i = 10; i <= 50; i += 10) root.A.Add(i);
        var listed = new List<int>();
        foreach (var item in scan == "forward" ? (IEnumerable<int>)root.A : root.A.GetReverseEnumerator())
        {
            listed.Add(item);
            if (item % 10 == 0)
            {
                root.A.Add(item - 1);
                root.A.Add(item + 1);
            }
        }

        Assert.Equal(listed.Distinct().Count(), listed.Count);
        Assert.Subset(listed.ToHashSet(), new HashSet<int> { 10, 20, 30, 40, 50 });
        Assert.True(scan == "forward" ? listed.SequenceEqual(listed.Order()) : listed.SequenceEqual(listed.OrderDescending()));
    }
}
