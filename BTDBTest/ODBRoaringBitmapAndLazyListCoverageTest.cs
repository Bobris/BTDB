using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ODBRoaringBitmapAndLazyListCoverageTest : FieldHandlerCoverageTestBase
{
    [Generate]
    public class BitsRoot
    {
        public IRoaringBitmap Bits { get; set; } = null!;
        public IRoaringBitmap Other { get; set; } = null!;
        public ILazyUlongList List { get; set; } = null!;
    }

    [Generate]
    public class BitsHolder
    {
        public string Name { get; set; } = "";
        public IRoaringBitmap? Bits { get; set; }
        public ILazyUlongList? List { get; set; }
    }

    [Generate]
    public class BitsHolderWithoutCollections
    {
        public string Name { get; set; } = "";
    }

    [Fact]
    public void RandomSetOperationsMatchReferenceAcrossTransactions()
    {
        var reference = new SortedSet<ulong>();
        var rnd = new Random(1234);
        for (var round = 0; round < 6; round++)
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<BitsRoot>();
                root.Other.Set(7, true);
                for (var i = 0; i < 300; i++)
                {
                    var page = (ulong)rnd.Next(4);
                    var value = (page << 16) + (ulong)rnd.Next(64);
                    var enable = rnd.Next(3) != 0;
                    root.Bits.Set(value, enable);
                    if (enable) reference.Add(value);
                    else reference.Remove(value);
                    Assert.Equal((ulong)reference.Count, root.Bits.Count);
                    var probe = ((ulong)rnd.Next(4) << 16) + (ulong)rnd.Next(64);
                    Assert.Equal(reference.Contains(probe), root.Bits.Get(probe));
                }

                root.Bits.Flush();
                root.Other.Flush();
                tr.Commit();
            }

            ReopenDb();
            using (var tr = _db.StartReadOnlyTransaction())
            {
                var root = tr.Singleton<BitsRoot>();
                Assert.Equal(reference, root.Bits);
                Assert.Equal((ulong)reference.Count, root.Bits.Count);
                Assert.True(root.Bits.IsComplete());
                Assert.Equal(new ulong[] { 7 }, root.Other);
            }
        }
    }

    [Fact]
    public void UnsettingAllBitsRemovesPagesAndCount()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<BitsRoot>();
            root.Bits.Set(1, true);
            root.Bits.Set(70000, true);
            root.Bits.Set(1, true); // no change
            root.Bits.Flush();
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<BitsRoot>();
            Assert.Equal(2ul, root.Bits.Count);
            root.Bits.Set(70000, false);
            root.Bits.Set(70000, false); // no change
            root.Bits.Set(1, false);
            root.Bits.Flush();
            Assert.Equal(0ul, root.Bits.Count);
            Assert.Empty(root.Bits);
            tr.Commit();
        }

        using (var tr = _db.StartReadOnlyTransaction())
        {
            var root = tr.Singleton<BitsRoot>();
            Assert.Equal(0ul, root.Bits.Count);
            Assert.False(root.Bits.Get(1));
            Assert.False(root.Bits.Get(70000));
            Assert.Empty(root.Bits);
        }

        AssertNoLeaks();
    }

    [Fact]
    public void BitmapEnumeratorKeepsReturningFalseAfterEnd()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<BitsRoot>();
        root.Bits.Set(3, true);
        root.Bits.Set(100000, true);
        using var enumerator = root.Bits.GetEnumerator();
        Assert.True(enumerator.MoveNext());
        Assert.Equal(3ul, enumerator.Current);
        Assert.True(enumerator.MoveNext());
        Assert.Equal(100000ul, ((IEnumerator)enumerator).Current);
        Assert.False(enumerator.MoveNext());
        Assert.False(enumerator.MoveNext());
        Assert.Throws<NotSupportedException>(() => enumerator.Reset());
        var nonGeneric = ((IEnumerable)root.Bits).Cast<ulong>().ToList();
        Assert.Equal(new ulong[] { 3, 100000 }, nonGeneric);
    }

    // RoaringBitmap.BuildAsync applies its pages in several transactions and the count last. Before the fix a bitmap
    // with pages but no count yet counted 0, so setting a bit stored a count of 1.
    [Fact]
    public void BitmapWithPagesButNoCountYetCountsItsPages()
    {
        // Clear, then the array page 0 holding 5 and the array page 1 holding 70000 (offset 4464), without a count.
        // A page's length is aligned to 2 bytes from the start of the commands.
        byte[] pages = [0, 1, 0, 0, 2, 0, 5, 0, 1, 1, 2, 0, 0x70, 0x11];
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<BitsRoot>();
            root.Bits.ApplyCommands(pages);
            Assert.False(root.Bits.IsComplete());
            Assert.Equal(2ul, root.Bits.Count);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<BitsRoot>();
            Assert.Equal(new ulong[] { 5, 70000 }, root.Bits);
            Assert.Equal(2ul, root.Bits.Count);
            root.Bits.Set(9, true);
            Assert.Equal(3ul, root.Bits.Count);
            root.Bits.Flush();
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var root = tr.Singleton<BitsRoot>();
            Assert.True(root.Bits.IsComplete());
            Assert.Equal(3ul, root.Bits.Count);
            Assert.Equal(new ulong[] { 5, 9, 70000 }, root.Bits);
        }
    }

    [Fact]
    public void BitmapClearAndApplyCommandValidation()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<BitsRoot>();
        root.Bits.Set(5, true);
        Assert.Throws<InvalidOperationException>(() => root.Bits.ApplyCommands(new byte[] { 0 }));
        root.Bits.Flush();
        Assert.Throws<ArgumentException>(() => root.Bits.ApplyCommands(new byte[] { 42 }));
        root.Bits.Clear();
        Assert.Equal(0ul, root.Bits.Count);
        Assert.Empty(root.Bits);
        root.Bits.Set(6, true);
        Assert.Equal(new ulong[] { 6 }, root.Bits);
    }

    [Fact]
    public void LazyListBasicOperations()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<BitsRoot>();
            for (var i = 0ul; i < 10; i++) root.List.Add(i * 3);
            root.List.Flush();
            Assert.True(root.List.IsComplete());
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<BitsRoot>();
            Assert.Equal(10ul, root.List.Count);
            Assert.Equal(new ulong[] { 21, 24, 27 }, root.List.EnumerateFromIndex(7));
            Assert.Empty(root.List.EnumerateFromIndex(100000));
            Assert.Equal(10, ((IEnumerable)root.List).Cast<ulong>().Count());
            root.List.Add(100);
            // enumeration flushes pending record
            Assert.Equal(100ul, root.List.Last());
            Assert.Throws<ArgumentException>(() => root.List.ApplyCommands(new byte[] { 42 }));
            root.List.Clear();
            root.List.Flush();
            Assert.Equal(0ul, root.List.Count);
            Assert.Empty(root.List);
            tr.Commit();
        }

        AssertNoLeaks();
    }

    [Fact]
    public void LazyListApplyCommandsFailsWithUnflushedChanges()
    {
        using var tr = _db.StartTransaction();
        var root = tr.Singleton<BitsRoot>();
        root.List.Add(1);
        Assert.Throws<InvalidOperationException>(() => root.List.ApplyCommands(Array.Empty<byte>()));
    }

    [Fact]
    public void NullCollectionsAreAllocatedOnSaveAndForeignImplementationsAreRejected()
    {
        _db.RegisterType(typeof(BitsHolder));
        using (var tr = _db.StartTransaction())
        {
            tr.Store(new BitsHolder { Name = "null" });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var holder = tr.Enumerate<BitsHolder>().Single();
            Assert.NotNull(holder.Bits);
            Assert.NotNull(holder.List);
            Assert.Empty(holder.Bits!);
            Assert.Empty(holder.List!);
        }

        using (var tr = _db.StartTransaction())
        {
            tr.Store(new BitsHolder { Name = "foreign", Bits = new ForeignBitmap() });
            Assert.Throws<BTDBException>(() => tr.Commit());
        }

        using (var tr = _db.StartTransaction())
        {
            tr.Store(new BitsHolder { Name = "foreign", List = new ForeignList() });
            Assert.Throws<BTDBException>(() => tr.Commit());
        }
    }

    [Fact]
    public void RemovedBitmapAndListFieldsAreSkippedOnLoad()
    {
        _db.RegisterType(typeof(BitsHolder), "Holder");
        using (var tr = _db.StartTransaction())
        {
            var holder = tr.New<BitsHolder>();
            holder.Name = "n";
            holder.Bits!.Set(1, true);
            holder.Bits.Flush();
            holder.List!.Add(5);
            holder.List.Flush();
            tr.Store(holder);
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(BitsHolderWithoutCollections), "Holder");
        using (var tr = _db.StartTransaction())
        {
            var holder = tr.Enumerate<BitsHolderWithoutCollections>().Single();
            Assert.Equal("n", holder.Name);
        }

        ReopenDb();
        _db.RegisterType(typeof(BitsHolder), "Holder");
        using (var tr = _db.StartTransaction())
        {
            var holder = tr.Enumerate<BitsHolder>().Single();
            Assert.Equal(new ulong[] { 1 }, holder.Bits!);
            Assert.Equal(new ulong[] { 5 }, holder.List!);
        }
    }

    void AssertNoLeaks()
    {
        using var visitor = new FindUnusedKeysVisitor();
        using var tr = _db.StartReadOnlyTransaction();
        visitor.ImportAllKeys(tr);
        visitor.Iterate(tr);
        Assert.Empty(visitor.UnseenKeys());
    }

    class ForeignBitmap : IRoaringBitmap
    {
        public IEnumerator<ulong> GetEnumerator() => Enumerable.Empty<ulong>().GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public bool Get(ulong value) => false;

        public void Set(ulong value, bool enabled)
        {
        }

        public void Clear()
        {
        }

        public ulong Count => 0;
        public bool IsComplete() => true;

        public void Flush()
        {
        }

        public void ApplyCommands(ReadOnlyMemory<byte> commands)
        {
        }
    }

    class ForeignList : ILazyUlongList
    {
        public IEnumerator<ulong> GetEnumerator() => Enumerable.Empty<ulong>().GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Add(ulong value)
        {
        }

        public void Clear()
        {
        }

        public IEnumerable<ulong> EnumerateFromIndex(ulong index) => Enumerable.Empty<ulong>();
        public ulong Count => 0;
        public bool IsComplete() => true;

        public void Flush()
        {
        }

        public void ApplyCommands(ReadOnlyMemory<byte> commands)
        {
        }
    }
}
