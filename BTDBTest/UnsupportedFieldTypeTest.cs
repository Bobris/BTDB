using System;
using System.Collections.Generic;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// char is not a supported field type. Before the fix a nullable, tuple, list or dictionary of it failed with
// NullReferenceException while building its field handler, instead of reporting the unsupported type as char does.
public class UnsupportedFieldTypeTest : ObjectDbTestBase
{
    public UnsupportedFieldTypeTest(ITestOutputHelper output) : base(output)
    {
    }

    [Generate]
    public class WithNullableChar
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public char? Letter { get; set; }
    }

    public interface IWithNullableCharTable : IRelation<WithNullableChar>
    {
        void Insert(WithNullableChar row);
    }

    [Generate]
    public class WithCharTuple
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public (char, int) Pair { get; set; }
    }

    public interface IWithCharTupleTable : IRelation<WithCharTuple>
    {
        void Insert(WithCharTuple row);
    }

    [Generate]
    public class WithCharList
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public List<char>? Letters { get; set; }
    }

    public interface IWithCharListTable : IRelation<WithCharList>
    {
        void Insert(WithCharList row);
    }

    [Fact]
    public void NullableOfUnsupportedTypeIsReported()
    {
        using var tr = _db.StartTransaction();
        var e = Assert.Throws<BTDBException>(() => tr.GetRelation<IWithNullableCharTable>());
        Assert.Contains("property Letter", e.Message);
    }

    [Fact]
    public void TupleOfUnsupportedTypeIsReported()
    {
        using var tr = _db.StartTransaction();
        var e = Assert.Throws<BTDBException>(() => tr.GetRelation<IWithCharTupleTable>());
        Assert.Contains("property Pair", e.Message);
    }

    [Fact]
    public void ListOfUnsupportedTypeIsReported()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IWithCharListTable>();
        Assert.Throws<InvalidOperationException>(() => table.Insert(new() { Id = 1, Letters = ['a'] }));
    }
}
