using System.Collections.Generic;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class AdvancedEnumeratorEnumKeyTest : ObjectDbTestBase
{
    public AdvancedEnumeratorEnumKeyTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum ItemCategory
    {
        Standard,
        Premium,
        Enterprise
    }

    public class CategorizedItem
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }

        [PrimaryKey(2)] public ulong Id { get; set; }

        [SecondaryKey("Category", IncludePrimaryKeyOrder = 1)]
        public ItemCategory Category { get; set; }

        public string Name { get; set; } = "";
    }

    public interface ICategorizedItemTable : IRelation<CategorizedItem>
    {
        void Insert(CategorizedItem item);
        bool AnyByCategory(ulong tenantId, AdvancedEnumeratorParam<ItemCategory> param);
        IEnumerable<CategorizedItem> ListByCategory(ulong tenantId, AdvancedEnumeratorParam<ItemCategory> param);
    }

    [Fact]
    public void AdvancedEnumeratorWithEnumSecondaryKeyWorks()
    {
        using var tr = _db.StartTransaction();
        var creator = tr.InitRelation<ICategorizedItemTable>("CategorizedItem");
        var table = creator(tr);

        table.Insert(new CategorizedItem { TenantId = 1, Id = 1, Category = ItemCategory.Standard, Name = "A" });
        table.Insert(new CategorizedItem { TenantId = 1, Id = 2, Category = ItemCategory.Premium, Name = "B" });
        table.Insert(new CategorizedItem { TenantId = 1, Id = 3, Category = ItemCategory.Enterprise, Name = "C" });

        // This should not throw NotSupportedException: "Key does not support type '...ItemCategory'"
        var hasPremium = table.AnyByCategory(1, new AdvancedEnumeratorParam<ItemCategory>(
            EnumerationOrder.Ascending,
            ItemCategory.Premium, KeyProposition.Included,
            ItemCategory.Premium, KeyProposition.Included));

        Assert.True(hasPremium);

        var premiumItems = new List<CategorizedItem>(table.ListByCategory(1,
            new AdvancedEnumeratorParam<ItemCategory>(
                EnumerationOrder.Ascending,
                ItemCategory.Premium, KeyProposition.Included,
                ItemCategory.Enterprise, KeyProposition.Included)));

        Assert.Equal(2, premiumItems.Count);

        tr.Commit();
    }
}
