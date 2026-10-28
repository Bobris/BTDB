using System;
using System.Threading;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// Before the fix inline objects nested deeper than the stack holds overflowed it and ended the process.
public class ObjectDbDeepInlineTest : ObjectDbTestBase
{
    public ObjectDbDeepInlineTest(ITestOutputHelper output) : base(output)
    {
    }

    public class DeepNode
    {
        public int Value { get; set; }
        public DeepNode? Next { get; set; }
    }

    public class DeepRow
    {
        [PrimaryKey]
        public ulong Id { get; set; }

        public DeepNode? Node { get; set; }
    }

    public interface IDeepRowTable : IRelation<DeepRow>
    {
        DeepRow FindById(ulong id);
        bool RemoveById(ulong id);
    }

    static DeepNode? Chain(int depth)
    {
        DeepNode? head = null;
        for (var i = 0; i < depth; i++) head = new DeepNode { Value = i, Next = head };
        return head;
    }

    static Exception? RunWithStack(int stackSize, Action action)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                error = e;
            }
        }, stackSize);
        thread.Start();
        thread.Join();
        return error;
    }

    [Fact]
    public void TooDeepToWriteFailsWithException()
    {
        var error = RunWithStack(1024 * 1024, () =>
        {
            using var tr = _db.StartTransaction();
            tr.GetRelation<IDeepRowTable>().Upsert(new DeepRow { Id = 1, Node = Chain(100_000) });
        });
        Assert.IsType<BTDBException>(error);
    }

    [Fact]
    public void TooDeepToReadFailsWithException()
    {
        Assert.Null(RunWithStack(256 * 1024 * 1024, () =>
        {
            using var tr = _db.StartTransaction();
            tr.GetRelation<IDeepRowTable>().Upsert(new DeepRow { Id = 1, Node = Chain(20_000) });
            tr.Commit();
        }));
        ReopenDb();
        Assert.IsType<BTDBException>(RunWithStack(512 * 1024, () =>
        {
            using var tr = _db.StartReadOnlyTransaction();
            tr.GetRelation<IDeepRowTable>().FindById(1);
        }));
        // Nothing in the row needs freeing, so removing it does not read it and the row can still go.
        Assert.Null(RunWithStack(512 * 1024, () =>
        {
            using var tr = _db.StartTransaction();
            Assert.True(tr.GetRelation<IDeepRowTable>().RemoveById(1));
            tr.Commit();
        }));
    }
}
