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
        int CountById(ulong companyId, AdvancedEnumeratorParam<ulong> groupId);
        int CountByGroup(ulong companyId, AdvancedEnumeratorParam<ulong> groupId);
        bool AnyById(ulong companyId, AdvancedEnumeratorParam<ulong> groupId);
        bool AnyByGroup(ulong companyId, AdvancedEnumeratorParam<ulong> groupId);
    }

    [BTDB.Generate]
    public interface IItemOrderedTable : IRelation<Item>
    {
        bool RemoveById(ulong companyId, ulong groupId, ulong id);
        IOrderedDictionaryEnumerator<ulong, Item> ListById(ulong companyId, ulong groupId, AdvancedEnumeratorParam<ulong> id);
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

    /// Counting by key indices (`CountWithProposition`, modelled in BTDB/Proofs/BtdbProofs/Count.lean) agrees with
    /// enumeration and with the bounds for every pair of propositions, on a partial key whose groups have several items.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CountMatchesEnumerationForAllPropositions(bool secondaryKey)
    {
        var groups = new (ulong Company, ulong Group, ulong Ids)[] { (0, 5, 1), (1, 1, 2), (1, 2, 1), (1, 4, 3), (2, 0, 1) };
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IItemTable>();
            foreach (var (company, group, ids) in groups)
                for (ulong id = 1; id <= ids; id++)
                    table.Upsert(new Item { CompanyId = company, GroupId = group, Id = id });
            tr.Commit();
        }

        using var read = _db.StartReadOnlyTransaction();
        var items = read.GetRelation<IItemTable>();
        var propositions = new[] { KeyProposition.Ignored, KeyProposition.Included, KeyProposition.Excluded };
        foreach (var startProposition in propositions)
        foreach (var endProposition in propositions)
            for (ulong start = 0; start <= 5; start++)
            for (ulong end = 0; end <= 5; end++)
            {
                var bounds = new AdvancedEnumeratorParam<ulong>(EnumerationOrder.Ascending, start, startProposition,
                    end, endProposition);
                var expected = groups.Where(g => g.Company == 1 &&
                        startProposition switch
                        {
                            KeyProposition.Included => g.Group >= start,
                            KeyProposition.Excluded => g.Group > start,
                            _ => true
                        } &&
                        endProposition switch
                        {
                            KeyProposition.Included => g.Group <= end,
                            KeyProposition.Excluded => g.Group < end,
                            _ => true
                        })
                    .Sum(g => (int)g.Ids);
                var listed = (secondaryKey ? items.ListByGroup(1, bounds) : items.ListById(1, bounds)).Count();
                var counted = secondaryKey ? items.CountByGroup(1, bounds) : items.CountById(1, bounds);
                var any = secondaryKey ? items.AnyByGroup(1, bounds) : items.AnyById(1, bounds);
                var description = $"{startProposition} {start}, {endProposition} {end}";
                Assert.True(expected == listed, $"listed {listed} of {expected} for {description}");
                Assert.True(expected == counted, $"counted {counted} of {expected} for {description}");
                Assert.True((expected > 0) == any, $"any {any} for {expected} for {description}");
            }
    }

    /// Removing each listed item while enumerating stays within the range, also when the removed item is the bound
    /// the enumeration stops at.
    [Theory]
    [InlineData(EnumerationOrder.Ascending, false)]
    [InlineData(EnumerationOrder.Descending, false)]
    [InlineData(EnumerationOrder.Ascending, true)]
    [InlineData(EnumerationOrder.Descending, true)]
    public void RemovingListedItemsKeepsEnumerationInRange(EnumerationOrder order, bool secondaryKey)
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IItemTable>();
        for (ulong id = 1; id <= 5; id++) table.Upsert(new Item { CompanyId = 1, GroupId = 1, Id = id });
        var bounds = new AdvancedEnumeratorParam<ulong>(order, 2, KeyProposition.Included, 4, KeyProposition.Included);
        var listed = new List<ulong>();
        foreach (var item in secondaryKey ? table.ListByGroup(1, 1, bounds) : table.ListById(1, 1, bounds))
        {
            listed.Add(item.Id);
            Assert.True(table.RemoveById(1, 1, item.Id));
        }

        var expected = new ulong[] { 2, 3, 4 };
        Assert.Equal(order == EnumerationOrder.Ascending ? expected : expected.Reverse().ToArray(), listed);
    }

    [Theory]
    [InlineData(EnumerationOrder.Ascending)]
    [InlineData(EnumerationOrder.Descending)]
    public void RemovingListedKeysKeepsOrderedEnumerationInRange(EnumerationOrder order)
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IItemOrderedTable>();
        for (ulong id = 1; id <= 5; id++) table.Upsert(new Item { CompanyId = 1, GroupId = 1, Id = id });
        var listed = new List<ulong>();
        using var enumerator = table.ListById(1, 1,
            new AdvancedEnumeratorParam<ulong>(order, 2, KeyProposition.Included, 4, KeyProposition.Included));
        while (enumerator.NextKey(out var id))
        {
            listed.Add(id);
            Assert.True(table.RemoveById(1, 1, id));
        }

        var expected = new ulong[] { 2, 3, 4 };
        Assert.Equal(order == EnumerationOrder.Ascending ? expected : expected.Reverse().ToArray(), listed);
    }
}
