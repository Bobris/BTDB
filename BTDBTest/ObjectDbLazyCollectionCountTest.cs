using System.Collections.Generic;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// Before the fix each instance of a lazy dictionary or set cached its count, so after one instance changed the
// collection another instance of it, as loading its owner twice in a transaction gives, reported the old count.
public class ObjectDbLazyCollectionCountTest : ObjectDbTestBase
{
    public ObjectDbLazyCollectionCountTest(ITestOutputHelper output) : base(output)
    {
    }

    public class CountedRow
    {
        [PrimaryKey]
        public ulong Id { get; set; }

        public IDictionary<int, int> Map { get; set; } = null!;
        public IOrderedSet<int> Set { get; set; } = null!;
    }

    public interface ICountedRowTable : IRelation<CountedRow>
    {
        CountedRow FindById(ulong id);
    }

    [Fact]
    public void CountSeesChangesThroughAnotherInstance()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<ICountedRowTable>();
        table.Upsert(new CountedRow
        {
            Id = 1, Map = new Dictionary<int, int> { [1] = 1 }, Set = new OrderedSet<int> { 1 }
        });
        var first = table.FindById(1);
        var second = table.FindById(1);
        Assert.Equal(1, first.Map.Count);
        Assert.Equal(1, second.Map.Count);
        Assert.Equal(1, first.Set.Count);
        Assert.Equal(1, second.Set.Count);

        first.Map[2] = 2;
        first.Set.Add(2);
        Assert.Equal(2, second.Map.Count);
        Assert.Equal(2, second.Set.Count);

        second.Map.Clear();
        second.Set.Clear();
        Assert.Empty(first.Map);
        Assert.Equal(0, first.Map.Count);
        Assert.Equal(0, first.Set.Count);
        var keys = new int[first.Map.Keys.Count];
        first.Map.Keys.CopyTo(keys, 0);
    }
}
