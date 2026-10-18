using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class RelationBugRegressionTest : ObjectDbTestBase
{
    public RelationBugRegressionTest(ITestOutputHelper output) : base(output)
    {
    }

    public class RRow
    {
        [PrimaryKey(1)] public ulong T { get; set; }
        [PrimaryKey(2)] public long K { get; set; }
        [PrimaryKey(3)] public ulong V { get; set; }
        [SecondaryKey("Name")] public string Name { get; set; } = "";
    }

    public interface IRRowTable : IRelation<RRow>
    {
        IOrderedDictionaryEnumerator<long, RRow> ListById(ulong t, AdvancedEnumeratorParam<long> k);
        long CountById(ulong t, AdvancedEnumeratorParam<long> k);
        bool AnyById(ulong t, AdvancedEnumeratorParam<long> k);
        long CountByName(AdvancedEnumeratorParam<string> name);
        int RemoveById(ulong t, AdvancedEnumeratorParam<long> k);
        RRow? FirstByIdOrDefault(Constraint<ulong> t, Constraint<long> k);
        RRow? LastByIdOrDefault(Constraint<ulong> t, Constraint<long> k);
    }

    public class ByteKeys
    {
        [PrimaryKey(1)] public byte U8 { get; set; }
        [PrimaryKey(2)] public sbyte I8 { get; set; }
        [SecondaryKey("N", Order = 1)] public byte? NU8 { get; set; }
        [SecondaryKey("N", Order = 2)] public sbyte? NI8 { get; set; }
    }

    public interface IByteKeysTable : IRelation<ByteKeys>
    {
        IEnumerable<ByteKeys> ScanById(Constraint<byte> u8, Constraint<sbyte> i8);
        IEnumerable<ByteKeys> ScanByN(Constraint<byte?> nu8, Constraint<sbyte?> ni8);
    }

    static void RunInMode(bool noEmit, Action action)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            action();
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    void Fill(IRRowTable table)
    {
        for (ulong t = 1; t <= 3; t++)
        for (long k = -2; k <= 2; k++)
        for (ulong v = 0; v < 2; v++)
            table.Upsert(new() { T = t, K = k, V = v, Name = "n" + (k + 2) });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void FirstAndLastByIdWithConstraintsWorkInBothModes(bool noEmit)
    {
        // IL emitted FirstById/LastById did not pass `target` argument and failed with InvalidProgramException
        RunInMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IRRowTable>();
            Fill(table);
            var first = table.FirstByIdOrDefault(Constraint.Unsigned.Exact(2), Constraint.Signed.Any)!;
            Assert.Equal((2ul, -2L, 0ul), (first.T, first.K, first.V));
            var last = table.LastByIdOrDefault(Constraint.Unsigned.Exact(2), Constraint.Signed.Any)!;
            Assert.Equal((2ul, 2L, 1ul), (last.T, last.K, last.V));
            Assert.Null(table.FirstByIdOrDefault(Constraint.Unsigned.Exact(4), Constraint.Signed.Any));
            Assert.Null(table.LastByIdOrDefault(Constraint.Unsigned.Exact(4), Constraint.Signed.Any));
        });
    }

    [Theory]
    [InlineData(EnumerationOrder.Ascending)]
    [InlineData(EnumerationOrder.Descending)]
    public void OrderedDictionaryEnumeratorStaysAtEnd(EnumerationOrder order)
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IRRowTable>();
        Fill(table);
        using var enumerator = table.ListById(2, new(order));
        Assert.Equal(10u, enumerator.Count);
        var keys = new List<long>();
        while (enumerator.NextKey(out var key)) keys.Add(key);
        Assert.Equal(10, keys.Count);
        // after End it must keep returning false, previously it restarted enumeration from beginning
        Assert.False(enumerator.NextKey(out _));
        Assert.False(enumerator.NextKey(out _));
        Assert.Equal(10u, enumerator.Count);
        Assert.Equal(10u, enumerator.Position);
        enumerator.Position = 3;
        Assert.Equal(3u, enumerator.Position);
        Assert.True(enumerator.NextKey(out var keyAt3));
        Assert.Equal(keys[3], keyAt3);
    }

    [Fact]
    public void OrderedDictionaryEnumeratorPositionCanReturnFromEnd()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IRRowTable>();
        Fill(table);
        using var enumerator = table.ListById(1, new(EnumerationOrder.Ascending));
        enumerator.Position = 10;
        Assert.Equal(10u, enumerator.Count);
        Assert.Equal(10u, enumerator.Position);
        Assert.False(enumerator.NextKey(out _));
        // previously NullReferenceException, because setting Position to End disposed cursor and Count became 0
        enumerator.Position = 0;
        Assert.True(enumerator.NextKey(out var key));
        Assert.Equal(-2, key);
        Assert.Equal(-2, enumerator.CurrentValue.K);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void CountWithExcludedStartSkipsWholeNonUniquePrefix(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IRRowTable>();
            Fill(table);
            var param = new AdvancedEnumeratorParam<long>(EnumerationOrder.Ascending, 0, KeyProposition.Excluded, 0,
                KeyProposition.Ignored);
            // K=0 has two rows (V=0,1), both must be excluded
            Assert.Equal(4, table.CountById(2, param));
            Assert.Equal(4, CountEnumerated(table.ListById(2, param)));
            var nameParam = new AdvancedEnumeratorParam<string>(EnumerationOrder.Ascending, "n3",
                KeyProposition.Excluded, "", KeyProposition.Ignored);
            Assert.Equal(6, table.CountByName(nameParam));
            var lastExcluded = new AdvancedEnumeratorParam<long>(EnumerationOrder.Ascending, 2,
                KeyProposition.Excluded, 0, KeyProposition.Ignored);
            Assert.Equal(0, table.CountById(2, lastExcluded));
            Assert.False(table.AnyById(2, lastExcluded));
        });
    }

    static int CountEnumerated(IOrderedDictionaryEnumerator<long, RRow> enumerator)
    {
        using var e = enumerator;
        var count = 0;
        while (e.NextKey(out _)) count++;
        return count;
    }

    [Theory]
    [InlineData(true, EnumerationOrder.Descending)]
    [InlineData(false, EnumerationOrder.Descending)]
    [InlineData(true, EnumerationOrder.Ascending)]
    [InlineData(false, EnumerationOrder.Ascending)]
    public void RemoveByIdWithAdvancedParamDoesNotRemoveOutsideOfRange(bool noEmit, EnumerationOrder order)
    {
        RunInMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IRRowTable>();
            Fill(table);
            // descending removal previously continued below start bound and removed rows with K=-2 too
            Assert.Equal(8, table.RemoveById(2, new(order, -1, KeyProposition.Included, 0, KeyProposition.Ignored)));
            Assert.Equal(new[] { -2L, -2L }, table.Where(r => r.T == 2).Select(r => r.K));
            Assert.Equal(22, table.Count);
            Assert.Equal(2, table.RemoveById(2, new(order, 0, KeyProposition.Ignored, -2, KeyProposition.Included)));
            Assert.Equal(20, table.Count);
            Assert.Equal(20, table.CountByName(new AdvancedEnumeratorParam<string>()));
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ByteAndSByteKeysWorkWithGenericConstraints(bool noEmit)
    {
        // byte/sbyte keys are stored in single byte, but constraints used variable length integer encoding, so values
        // >= 128 (or negative) were not found and Any constraint read past end of key
        RunInMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IByteKeysTable>();
            table.Upsert(new() { U8 = 200, I8 = -100, NU8 = 200, NI8 = -100 });
            table.Upsert(new() { U8 = 200, I8 = 5, NU8 = null, NI8 = 5 });
            table.Upsert(new() { U8 = 1, I8 = sbyte.MinValue, NU8 = 1, NI8 = null });
            Assert.Equal(3, table.ScanById(Constraint<byte>.Any, Constraint<sbyte>.Any).Count());
            Assert.Equal(new sbyte[] { -100, 5 },
                table.ScanById(Constraint.Exact<byte>(200), Constraint<sbyte>.Any).Select(r => r.I8));
            Assert.Single(table.ScanById(Constraint<byte>.Any, Constraint.Exact<sbyte>(-100)));
            Assert.Single(table.ScanById(Constraint<byte>.Any, Constraint.Exact(sbyte.MinValue)));
            Assert.Equal(3, table.ScanByN(Constraint<byte?>.Any, Constraint<sbyte?>.Any).Count());
            Assert.Single(table.ScanByN(Constraint.Exact<byte?>(200), Constraint<sbyte?>.Any));
            Assert.Single(table.ScanByN(Constraint.Exact<byte?>(null), Constraint.Exact<sbyte?>(5)));
            Assert.Single(table.ScanByN(Constraint<byte?>.Any, Constraint.Exact<sbyte?>(-100)));
            Assert.Single(table.ScanByN(Constraint<byte?>.Any, Constraint.Exact<sbyte?>(null)));
        });
    }
}
