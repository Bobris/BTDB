using System;
using BTDB.Bon;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbTableWithSizesTest : ObjectDbTestBase
{
    public ObjectDbTableWithSizesTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Document
    {
        [PrimaryKey(1)] public ulong TenantId { get; set; }

        [PrimaryKey(2)] public string Key { get; set; }

        public ReadOnlyMemory<byte> Value { get; set; }
    }

    public interface IDocumentTable : IRelation<Document>
    {
        (bool Inserted, uint KeySize, uint OldValueSize, uint NewValueSize) ShallowUpsertWithSizes(Document obj,
            bool allowInsert, bool allowUpdate);

        (ulong Count, ulong KeySizes, ulong ValueSizes) RemoveWithSizesById(Constraint<ulong> tenantId,
            Constraint<string> key);
    }

    [Fact]
    public void DoubleUpsertChangeValueSize()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IDocumentTable>();
        Assert.Equal((true, 9u, 0u, 239666u),
            table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First", Value = CreateSampleBonData(10000) },
                true, true));
        Assert.Equal((false, 9u, 239666u, 171u),
            table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First", Value = CreateSampleBonData(10) },
                true, true));
    }

    [Fact]
    public void UpdateWithoutDataIgnored()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IDocumentTable>();
        Assert.Equal((false, 0u, 0u, 0u),
            table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First", Value = CreateSampleBonData(10000) },
                false, true));
    }

    [Fact]
    public void DoubleInsertIgnored()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IDocumentTable>();
        Assert.Equal((true, 9u, 0u, 239666u),
            table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First", Value = CreateSampleBonData(10000) },
                true, false));
        Assert.Equal((false, 9u, 239666u, 239666u),
            table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First", Value = CreateSampleBonData(10000) },
                true, false));
    }

    [Fact]
    public void CouldBeUsedForQuerySize()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IDocumentTable>();
        Assert.Equal((false, 0u, 0u, 0u),
            table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First" },
                false, false));
        Assert.Equal((true, 9u, 0u, 239666u),
            table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First", Value = CreateSampleBonData(10000) },
                true, false));
        Assert.Equal((false, 9u, 239666u, 239666u),
            table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First" },
                false, false));
    }

    [Fact]
    public void RemoveWithSizesWorks()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IDocumentTable>();
        CreateData(table);
        Assert.Equal((2ul,19ul,239666ul+171ul), table.RemoveWithSizesById(Constraint<ulong>.Any, Constraint<string>.Any));
        Assert.Equal(0, table.Count);
        CreateData(table);
        Assert.Equal((2ul,19ul,239666ul+171ul), table.RemoveWithSizesById(Constraint.Unsigned.Exact(1ul), Constraint<string>.Any));
        Assert.Equal(0, table.Count);
        CreateData(table);
        Assert.Equal((2ul,19ul,239666ul+171ul), table.RemoveWithSizesById(Constraint.Unsigned.Exact(1ul), Constraint.String.StartsWith("Fi")));
        Assert.Equal(0, table.Count);
    }

    static void CreateData(IDocumentTable table)
    {
        table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First", Value = CreateSampleBonData(10000) },
            true, true);
        table.ShallowUpsertWithSizes(new() { TenantId = 1, Key = "First2", Value = CreateSampleBonData(10) },
            true, true);
    }

    static ReadOnlyMemory<byte> CreateSampleBonData(int size)
    {
        var bonBuilder = new BonBuilder();
        bonBuilder.StartArray();
        for (var id = 0; id < size; id++)
        {
            bonBuilder.StartObject();
            bonBuilder.WriteKey("Id");
            bonBuilder.Write(id);
            bonBuilder.WriteKey("Name");
            bonBuilder.Write("Bobris " + id);
            bonBuilder.FinishObject();
        }

        bonBuilder.FinishArray();
        return bonBuilder.FinishAsMemory();
    }
}
