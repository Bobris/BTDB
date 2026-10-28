using System.Collections.Generic;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// Before the fix a reference cycle through inline objects overflowed the stack and ended the process.
public class ObjectDbInlineCycleTest : ObjectDbTestBase
{
    public ObjectDbInlineCycleTest(ITestOutputHelper output) : base(output)
    {
    }

    public class CycleNode
    {
        public string? Name { get; set; }
        public CycleNode? Next { get; set; }
        public List<CycleNode>? Children { get; set; }
    }

    public class CycleRow
    {
        [PrimaryKey]
        public ulong Id { get; set; }

        public CycleNode? Node { get; set; }
        public CycleNode? Other { get; set; }
    }

    public interface ICycleRowTable : IRelation<CycleRow>
    {
        CycleRow FindById(ulong id);
    }

    [Generate]
    public class CycleRoot
    {
        public CycleNode? Node { get; set; }
    }

    [Fact]
    public void RelationRowWithCycleIsRefusedAndTransactionGoesOn()
    {
        var self = new CycleNode { Name = "self" };
        self.Next = self;
        var throughList = new CycleNode { Name = "list" };
        throughList.Children = [new CycleNode { Next = throughList }];
        var shared = new CycleNode { Name = "shared" };
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<ICycleRowTable>();
            Assert.Throws<BTDBException>(() => table.Upsert(new CycleRow { Id = 1, Node = self }));
            Assert.Throws<BTDBException>(() => table.Upsert(new CycleRow { Id = 2, Node = throughList }));
            // The same object in two places is no cycle.
            table.Upsert(new CycleRow { Id = 3, Node = new CycleNode { Next = shared }, Other = shared });
            tr.Commit();
        }

        ReopenDb();
        using var tr2 = _db.StartReadOnlyTransaction();
        var row = tr2.GetRelation<ICycleRowTable>().FindById(3);
        Assert.Equal("shared", row.Node!.Next!.Name);
        Assert.Equal("shared", row.Other!.Name);
    }

    [Fact]
    public void SingletonWithCycleFailsCommit()
    {
        var self = new CycleNode { Name = "self" };
        self.Next = self;
        using var tr = _db.StartTransaction();
        tr.Singleton<CycleRoot>().Node = self;
        Assert.Throws<BTDBException>(() => tr.Commit());
    }
}
