using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class NullableKeyFindByTest : ObjectDbTestBase
{
    public NullableKeyFindByTest(ITestOutputHelper output) : base(output)
    {
    }

    public enum Small : byte { A = 1, B = 200 }

    [Generate]
    public class Row
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("U8")] public byte? U8 { get; set; }
        [SecondaryKey("I16")] public short? I16 { get; set; }
        [SecondaryKey("U16")] public ushort? U16 { get; set; }
        [SecondaryKey("I32")] public int? I32 { get; set; }
        [SecondaryKey("B")] public bool? B { get; set; }
        [SecondaryKey("E")] public Small? E { get; set; }
        [SecondaryKey("I64")] public long? I64 { get; set; }
        [SecondaryKey("I8")] public sbyte? I8 { get; set; }
    }

    [Generate]
    public interface IRowTable : IRelation<Row>
    {
        void Insert(Row row);
        IEnumerable<Row> FindByU8(byte? u8);
        IEnumerable<Row> FindByI16(short? i16);
        IEnumerable<Row> FindByU16(ushort? u16);
        IEnumerable<Row> FindByI32(int? i32);
        IEnumerable<Row> FindByB(bool? b);
        IEnumerable<Row> FindByE(Small? e);
        IEnumerable<Row> FindByI64(long? i64);
        IEnumerable<Row> FindByI8(sbyte? i8);
    }

    IRowTable Fill(IObjectDBTransaction tr)
    {
        var table = tr.GetRelation<IRowTable>();
        table.Insert(new() { Id = 1, U8 = 200, I16 = -5, U16 = 60000, I32 = -7, B = true, E = Small.B, I64 = -9, I8 = -3 });
        table.Insert(new() { Id = 2 });
        return table;
    }

    // FindBy writes the key it looks up itself, so it must encode a nullable key like the rows store it.
    [Fact]
    public void FindByNullableKeysFindsValuesAndNulls()
    {
        using var tr = _db.StartTransaction();
        var table = Fill(tr);
        Assert.Equal(1UL, Assert.Single(table.FindByU8(200)).Id);
        Assert.Equal(1UL, Assert.Single(table.FindByI16(-5)).Id);
        Assert.Equal(1UL, Assert.Single(table.FindByU16(60000)).Id);
        Assert.Equal(1UL, Assert.Single(table.FindByI32(-7)).Id);
        Assert.Equal(1UL, Assert.Single(table.FindByB(true)).Id);
        Assert.Equal(1UL, Assert.Single(table.FindByE(Small.B)).Id);
        Assert.Equal(1UL, Assert.Single(table.FindByI64(-9)).Id);
        Assert.Equal(2UL, Assert.Single(table.FindByU8(null)).Id);
        Assert.Equal(2UL, Assert.Single(table.FindByI16(null)).Id);
        Assert.Equal(2UL, Assert.Single(table.FindByE(null)).Id);
        Assert.Equal(2UL, Assert.Single(table.FindByI8(null)).Id);
        Assert.Empty(table.FindByI32(-8));
    }

    [Fact]
    public void FindByNullableSByteKeyFindsValue()
    {
        using var tr = _db.StartTransaction();
        var table = Fill(tr);
        Assert.Equal(1UL, Assert.Single(table.FindByI8(-3)).Id);
        Assert.Empty(table.FindByI8(3));
    }

    [Generate]
    public class SByteKeyed
    {
        [PrimaryKey(1)] public sbyte? Key { get; set; }
        public string? Name { get; set; }
    }

    [Generate]
    public interface ISByteKeyedTable : IRelation<SByteKeyed>
    {
        void Insert(SByteKeyed row);
        SByteKeyed? FindByIdOrDefault(sbyte? key);
    }

    /// A nullable sbyte key stores its value byte raw, so rows sort null, 0 to 127, then -128 to -1.
    [Fact]
    public void NullableSBytePrimaryKeyIsFoundAndSortsRaw()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<ISByteKeyedTable>();
        foreach (var key in new sbyte?[] { 1, -1, null, 5, 127, -128, 0 })
            table.Insert(new() { Key = key, Name = key?.ToString() ?? "null" });
        Assert.Equal("5", table.FindByIdOrDefault(5)!.Name);
        Assert.Equal("-1", table.FindByIdOrDefault(-1)!.Name);
        Assert.Equal("null", table.FindByIdOrDefault(null)!.Name);
        Assert.Null(table.FindByIdOrDefault(2));
        Assert.Equal("null 0 1 5 127 -128 -1", string.Join(" ", table.Select(r => r.Name)));
    }
}
