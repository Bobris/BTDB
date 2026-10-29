using System;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDBTransactionCoverageTest : FieldHandlerCoverageTestBase
{
    [Generate]
    public class Node
    {
        public int Value { get; set; }
        public string? Name { get; set; }
    }

    [Generate]
    public class OtherNode
    {
        public int Value { get; set; }
    }

    [Generate]
    public class Holder
    {
        public IIndirect<Node>? Ind { get; set; }
        public Node? Inline { get; set; }
    }

    // more than 30 objects switch transaction into "big" object cache with weak references
    const int ManyObjects = 100;

    [Fact]
    public void ManyNewObjectsAreStoredAndCanBeModifiedInBigCacheMode()
    {
        var oids = new List<ulong>();
        using (var tr = _db.StartTransaction())
        {
            var nodes = new List<Node>();
            for (var i = 0; i < ManyObjects; i++)
            {
                var node = tr.New<Node>();
                node.Value = i;
                nodes.Add(node);
            }

            for (var i = 0; i < ManyObjects; i++)
            {
                oids.Add(i % 2 == 0 ? tr.Store(nodes[i]) : tr.StoreAndFlush(nodes[i]));
                Assert.Equal(oids[i], tr.GetOid(nodes[i]));
            }

            // second store is no-op returning same oid
            Assert.Equal(oids[0], tr.Store(nodes[0]));
            Assert.Equal(oids[1], tr.StoreAndFlush(nodes[1]));
            Assert.Equal(0ul, tr.GetOid(new Node()));
            Assert.Equal(0ul, tr.GetOid(null));
            Assert.Same(nodes[5], tr.Get(oids[5]));
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var all = tr.Enumerate<Node>().ToList();
            Assert.Equal(Enumerable.Range(0, ManyObjects), all.Select(n => n.Value).OrderBy(v => v));
            foreach (var node in all)
            {
                node.Name = "n" + node.Value;
                tr.Store(node);
            }

            Assert.NotEqual(0u, tr.GetStorageSize(oids[3]).Key);
            Assert.Equal(new KeyValuePair<uint, uint>(0, 0), tr.GetStorageSize(ulong.MaxValue));
            tr.Delete(all.First(n => n.Value == 7));
            tr.Delete(oids[8]);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var all = tr.Enumerate<Node>().ToList();
            Assert.Equal(ManyObjects - 2, all.Count);
            Assert.All(all, n => Assert.Equal("n" + n.Value, n.Name));
            Assert.Null(tr.Get(oids[7]));
            Assert.Null(tr.Get(oids[8]));
            tr.DeleteAll<Node>();
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            Assert.Empty(tr.Enumerate<Node>());
        }
    }

    // Before the fix new objects not flushed yet were enumerated only when newer than the last stored object, so one
    // stored before an object flushed with StoreAndFlush was missing.
    [Fact]
    public void EnumerateIncludesDirtyObjectsBeforeFlushedOnes()
    {
        using var tr = _db.StartTransaction();
        tr.Store(new Node { Value = 1 });
        tr.StoreAndFlush(new Node { Value = 2 });
        Assert.Equal([1, 2], tr.Enumerate<Node>().Select(n => n.Value).OrderBy(v => v));
    }

    // Before the fix an object stored again after its deletion was missing from the transaction's cache, so Get
    // returned null until the commit, and StoreAndFlush did not store it at all.
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void ObjectStoredAgainAfterDeleteCanBeGot(bool bigCache, bool flush)
    {
        ulong oid;
        using (var tr = _db.StartTransaction())
        {
            oid = tr.Store(new Node { Value = 1 });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            if (bigCache)
                for (var i = 0; i < ManyObjects; i++) tr.Store(new Node { Value = 100 + i });
            var node = (Node)tr.Get(oid)!;
            tr.Delete(node);
            Assert.Null(tr.Get(oid));
            node.Value = 2;
            Assert.Equal(oid, flush ? tr.StoreAndFlush(node) : tr.Store(node));
            Assert.Same(node, tr.Get(oid));
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
            Assert.Equal(2, ((Node)tr.Get(oid)!).Value);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ObjectDeletedBeforeStoringIsStoredWhenStoredExplicitly(bool flush)
    {
        ulong oid;
        using (var tr = _db.StartTransaction())
        {
            var node = new Node { Value = 5 };
            tr.Delete(node);
            oid = flush ? tr.StoreAndFlush(node) : tr.Store(node);
            Assert.Same(node, tr.Get(oid));
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
            Assert.Equal(5, ((Node)tr.Get(oid)!).Value);
    }

    [Fact]
    public void DeletingUnknownObjectsInBigCacheModeIsIgnored()
    {
        using var tr = _db.StartTransaction();
        for (var i = 0; i < ManyObjects; i++) tr.Store(new Node { Value = i });
        var notStored = new Node { Value = -1 };
        tr.Delete(notStored);
        // deleted before store means it is never stored
        Assert.Equal(0ul, ((IInternalObjectDBTransaction)tr).StoreIfNotInlined(notStored, true, false));
        tr.Delete(123456789ul);
        tr.Commit();
        using var tr2 = _db.StartTransaction();
        Assert.Equal(ManyObjects, tr2.Enumerate<Node>().Count());
    }

    [Fact]
    public void StoreIfNotInlinedInBigCacheMode()
    {
        using var tr = _db.StartTransaction();
        var nodes = new List<Node>();
        for (var i = 0; i < ManyObjects; i++)
        {
            var n = new Node { Value = i };
            nodes.Add(n);
            tr.Store(n);
        }

        var oid = tr.GetOid(nodes[10]);
        Assert.Equal(oid, ((IInternalObjectDBTransaction)tr).StoreIfNotInlined(nodes[10], false, false));
        Assert.Equal(ulong.MaxValue, ((IInternalObjectDBTransaction)tr).StoreIfNotInlined(nodes[11], false, true));
        Assert.Equal(ulong.MaxValue, ((IInternalObjectDBTransaction)tr).StoreIfNotInlined(new OtherNode(), false, false));
        var fresh = new Node { Value = 1000 };
        Assert.Equal(ulong.MaxValue, ((IInternalObjectDBTransaction)tr).StoreIfNotInlined(fresh, true, true));
        var freshOid = ((IInternalObjectDBTransaction)tr).StoreIfNotInlined(fresh, true, false);
        Assert.NotEqual(0ul, freshOid);
        Assert.NotEqual(ulong.MaxValue, freshOid);
        tr.Commit();
        using var tr2 = _db.StartTransaction();
        Assert.Equal(ManyObjects, tr2.Enumerate<Node>().Count());
    }

    [Fact]
    public void StoringIndirectNotMaterializedReturnsItsOid()
    {
        ulong oid;
        using (var tr = _db.StartTransaction())
        {
            var holder = new Holder { Ind = new DBIndirect<Node>(new Node { Value = 5 }) };
            tr.Store(holder);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var holder = tr.Enumerate<Holder>().Single();
            oid = holder.Ind!.Oid;
            Assert.Equal(oid, tr.Store(holder.Ind));
            Assert.Equal(oid, tr.StoreAndFlush(holder.Ind));
            Assert.Equal(5, holder.Ind.Value!.Value);
            Assert.Equal(oid, tr.Store(holder.Ind));
            tr.Delete(holder.Ind);
            tr.Delete(new DBIndirect<Node>(new Node()));
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            Assert.Null(tr.Get(oid));
        }
    }

    [Fact]
    public void StoringNonClassThrows()
    {
        using var tr = _db.StartTransaction();
        Assert.Throws<InvalidOperationException>(() => tr.Store(42));
        Assert.Throws<ArgumentNullException>(() => tr.Delete(null!));
    }

    [Fact]
    public void NextCommitTemporaryCloseTransactionLogCanBeCalled()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.Store(new Node { Value = 1 });
            tr.NextCommitTemporaryCloseTransactionLog();
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(1, tr.Enumerate<Node>().Single().Value);
        }
    }

    [Fact]
    public void CreateOrUpdateKeyValueWritesRawData()
    {
        using var tr = _db.StartWritingTransaction().Result;
        var internalTr = (IInternalObjectDBTransaction)tr;
        Assert.True(internalTr.CreateOrUpdateKeyValue(new byte[] { 255, 1 }, new byte[] { 2 }));
        Assert.False(internalTr.CreateOrUpdateKeyValue(new byte[] { 255, 1 }, new byte[] { 3 }));
        using var cursor = tr.KeyValueDBTransaction.CreateCursor();
        Assert.True(cursor.FindExactKey(new byte[] { 255, 1 }));
        Assert.Equal(new byte[] { 3 }, cursor.SlowGetValue());
    }

    [Fact]
    public async System.Threading.Tasks.Task StartWritingTransactionWithEventIdSetsCommitUlong()
    {
        using (var tr = await _db.StartWritingTransaction(42ul))
        {
            Assert.Equal(42ul, tr.GetCommitUlong());
            tr.Commit();
        }

        using (var tr = _db.StartReadOnlyTransaction())
        {
            Assert.Equal(42ul, tr.GetCommitUlong());
        }

        using (var tr = await _db.StartWritingTransaction(43ul))
        {
            // rollback discards the change
        }

        using (var tr = _db.StartReadOnlyTransaction())
        {
            Assert.Equal(42ul, tr.GetCommitUlong());
        }
    }
}
