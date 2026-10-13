using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbAdvancedEnumeratorBoundaryTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [BTDB.Generate]
    public class Item
    {
        [PrimaryKey(1), SecondaryKey("Group", Order = 1)]
        public ulong CompanyId { get; set; }

        [PrimaryKey(2), SecondaryKey("Group", Order = 2)]
        public ulong GroupId { get; set; }

        [PrimaryKey(3), SecondaryKey("Group", Order = 3)]
        public ulong Id { get; set; }
    }

    [BTDB.Generate]
    public interface IItemTable : IRelation<Item>
    {
        bool RemoveById(ulong companyId, ulong groupId, ulong id);
        IEnumerable<Item> ListById(ulong companyId, ulong groupId, AdvancedEnumeratorParam<ulong> id);
        IEnumerable<Item> ListByGroup(ulong companyId, ulong groupId, AdvancedEnumeratorParam<ulong> id);
        IEnumerable<Item> ListById(ulong companyId, AdvancedEnumeratorParam<ulong> groupId);
        IEnumerable<Item> ListByGroup(ulong companyId, AdvancedEnumeratorParam<ulong> groupId);
    }

    public static IEnumerable<object[]> Boundaries()
    {
        foreach (var order in new[] { EnumerationOrder.Ascending, EnumerationOrder.Descending })
        foreach (var proposition in new[] { KeyProposition.Included, KeyProposition.Excluded })
        foreach (var deleteBoundary in new[] { false, true })
        foreach (var secondaryKey in new[] { false, true })
            yield return [order, proposition, deleteBoundary, secondaryKey];
    }

    [Theory]
    [MemberData(nameof(Boundaries))]
    public void CompleteBoundaryKeepsSuccessorAfterDeletion(EnumerationOrder order, KeyProposition proposition,
        bool deleteBoundary, bool secondaryKey)
    {
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IItemTable>();
            for (ulong id = 1; id <= 3; id++)
                table.Upsert(new Item { CompanyId = 1, GroupId = 1, Id = id });
            table.Upsert(new Item { CompanyId = 1, GroupId = 2, Id = 4 });
            tr.Commit();
        }

        if (deleteBoundary)
        {
            using var tr = _db.StartTransaction();
            Assert.True(tr.GetRelation<IItemTable>().RemoveById(1, 1, 2));
            tr.Commit();
        }

        using var read = _db.StartReadOnlyTransaction();
        var items = read.GetRelation<IItemTable>();
        var bounds = new AdvancedEnumeratorParam<ulong>(order, 2, proposition, 0, KeyProposition.Ignored);
        var result = secondaryKey ? items.ListByGroup(1, 1, bounds) : items.ListById(1, 1, bounds);
        var expected = proposition == KeyProposition.Included && !deleteBoundary ? new ulong[] { 2, 3 } : [3];
        if (order == EnumerationOrder.Descending)
            expected = expected.Reverse().ToArray();

        Assert.Equal(expected, result.Select(item => item.Id));
        Assert.Equal(expected.Length, result.Count());
    }

    [Theory]
    [MemberData(nameof(Boundaries))]
    public void PartialBoundaryExcludesTheWholePrefix(EnumerationOrder order, KeyProposition proposition,
        bool deleteBoundary, bool secondaryKey)
    {
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IItemTable>();
            for (ulong group = 1; group <= 3; group++)
            for (ulong id = 1; id <= 2; id++)
                table.Upsert(new Item { CompanyId = 1, GroupId = group, Id = id });
            table.Upsert(new Item { CompanyId = 2, GroupId = 3, Id = 3 });
            tr.Commit();
        }

        if (deleteBoundary)
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IItemTable>();
            Assert.True(table.RemoveById(1, 2, 1));
            Assert.True(table.RemoveById(1, 2, 2));
            tr.Commit();
        }

        using var read = _db.StartReadOnlyTransaction();
        var items = read.GetRelation<IItemTable>();
        var bounds = new AdvancedEnumeratorParam<ulong>(order, 2, proposition, 0, KeyProposition.Ignored);
        var result = secondaryKey ? items.ListByGroup(1, bounds) : items.ListById(1, bounds);
        var expected = proposition == KeyProposition.Included && !deleteBoundary
            ? new[] { (2UL, 1UL), (2UL, 2UL), (3UL, 1UL), (3UL, 2UL) }
            : [(3UL, 1UL), (3UL, 2UL)];
        if (order == EnumerationOrder.Descending)
            expected = expected.Reverse().ToArray();

        Assert.Equal(expected, result.Select(item => (item.GroupId, item.Id)));
        Assert.Equal(expected.Length, result.Count());
    }
}
