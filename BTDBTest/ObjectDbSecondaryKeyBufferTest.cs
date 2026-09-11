using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbSecondaryKeyBufferTest(ITestOutputHelper output) : ObjectDbTestBase(output)
{
    [Generate]
    public class Record
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong BatchId { get; set; }
        [PrimaryKey(3)] public ulong MessageId { get; set; }
        [InKeyValue(4)] public string Recipient { get; set; } = "recipient";
        [SecondaryKey("CustomField", IncludePrimaryKeyOrder = 1)]
        public string CustomField { get; set; } = "";
        [SecondaryKey("Other", IncludePrimaryKeyOrder = 1)]
        public string Other { get; set; } = "";
    }

    public interface ITable : IRelation<Record>
    {
        void Insert(Record record);
        void Update(Record record);
        void UpdateById(ulong companyId, ulong batchId, ulong messageId, string recipient, string customField, string other);
        int RemoveById(ulong companyId, ulong batchId);
        int RemoveByIdPartial(ulong companyId, ulong batchId, int maxCount);
        int RemoveById(ulong companyId, ulong batchId, AdvancedEnumeratorParam<ulong> messageId);
        IEnumerable<Record> FindByCustomField(ulong companyId, string customField);
        IEnumerable<Record> FindByOther(ulong companyId, string other);
    }

    [Theory]
    [InlineData(24, false)]
    [InlineData(8192, false)]
    [InlineData(24, true)]
    [InlineData(8192, true)]
    public void MultipleIndexesSurviveUpdatesAndBatchRemoval(int length, bool updateById)
    {
        var original = new string('a', length);
        var replacement = new string('b', length + 17);
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<ITable>();
            for (ulong batch = 1; batch <= 2; batch++)
            for (ulong id = 1; id <= 3; id++)
                table.Insert(new Record
                {
                    CompanyId = 1, BatchId = batch, MessageId = id,
                    CustomField = original, Other = "short"
                });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<ITable>();
            // Grow and shrink alternating index buffers; repeat unchanged updates too.
            foreach (var value in new[] { replacement, replacement, "tiny" })
            {
                if (updateById) table.UpdateById(1, 1, 1, "recipient", value, value);
                else table.Update(new Record
                {
                    CompanyId = 1, BatchId = 1, MessageId = 1, CustomField = value, Other = value
                });
                Assert.Single(table.FindByCustomField(1, value));
                Assert.Single(table.FindByOther(1, value));
                Assert.Equal(5, table.FindByCustomField(1, original).Count());
                Assert.Equal(5, table.FindByOther(1, "short").Count());
            }
            Assert.Empty(table.FindByCustomField(1, replacement));
            Assert.Empty(table.FindByOther(1, replacement));
            Assert.Equal(3, table.RemoveById(1, 1));
            Assert.Equal(0, table.RemoveById(1, 1));
            Assert.Empty(table.FindByCustomField(1, "tiny"));
            Assert.Empty(table.FindByOther(1, "tiny"));
            tr.Commit();
        }
        ReopenDb();
        using var read = _db.StartTransaction();
        var remaining = read.GetRelation<ITable>();
        Assert.Equal(3, remaining.FindByCustomField(1, original).Count());
        Assert.Equal(3, remaining.FindByOther(1, "short").Count());
        Assert.All(remaining, record => Assert.Equal(2ul, record.BatchId));
    }

    [Generate]
    public class ComputedRecord
    {
        [PrimaryKey(1)] public ulong BatchId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        public string Value { get; set; } = "";
        [SecondaryKey("Computed")] public string Computed => Value + "!";
        [OnBeforeRemove] public bool KeepSecondRecord() => Id == 2;
    }

    public interface IComputedTable : IRelation<ComputedRecord>
    {
        void Insert(ComputedRecord record);
        void Update(ComputedRecord record);
        void UpdateById(ulong batchId, ulong id, string value);
        int RemoveById(ulong batchId);
        int RemoveByIdPartial(ulong batchId, int maxCount);
        int RemoveById(ulong batchId, AdvancedEnumeratorParam<ulong> id);
        IEnumerable<ComputedRecord> FindByComputed(string computed);
    }

    [Theory]
    [InlineData(24)]
    [InlineData(8192)]
    public void ComputedIndexesRemainCorrect(int length)
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IComputedTable>();
        var value = new string('x', length);
        table.Insert(new ComputedRecord { BatchId = 1, Id = 1, Value = value });
        Assert.Single(table.FindByComputed(value + "!"));
        table.UpdateById(1, 1, "small");
        Assert.Empty(table.FindByComputed(value + "!"));
        Assert.Single(table.FindByComputed("small!"));
        table.Update(new ComputedRecord { BatchId = 1, Id = 1, Value = value });
        Assert.Empty(table.FindByComputed("small!"));
        Assert.Single(table.FindByComputed(value + "!"));
        Assert.Equal(1, table.RemoveById(1));
        Assert.Empty(table.FindByComputed(value + "!"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void BatchWriterReusesGrowingBuffersAcrossRows(int mode)
    {
        var values = new[] { new string('a', 8192), "tiny", new string('b', 20000), "last" };
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<ITable>();
        for (ulong batch = 1; batch <= 2; batch++)
        for (var i = 0; i < values.Length; i++)
            table.Insert(new Record
            {
                CompanyId = 1, BatchId = batch, MessageId = (ulong)i,
                CustomField = values[i], Other = values[values.Length - 1 - i]
            });
        var removed = mode switch
        {
            0 => table.RemoveById(1, 1),
            1 => table.RemoveByIdPartial(1, 1, 4),
            _ => table.RemoveById(1, 1,
                new(EnumerationOrder.Descending, 0, KeyProposition.Included, 3, KeyProposition.Included))
        };
        Assert.Equal(4, removed);
        Assert.Equal(4, table.Count);
        foreach (var value in values)
        {
            Assert.Equal(2ul, Assert.Single(table.FindByCustomField(1, value)).BatchId);
            Assert.Equal(2ul, Assert.Single(table.FindByOther(1, value)).BatchId);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void BatchWriterPreservesComputedKeysAndVetoes(int mode)
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IComputedTable>();
        var values = new[] { new string('a', 8192), "keep", new string('b', 20000), "last" };
        for (var i = 0; i < values.Length; i++)
            table.Insert(new ComputedRecord { BatchId = 1, Id = (ulong)i + 1, Value = values[i] });
        var removed = mode switch
        {
            0 => table.RemoveById(1),
            1 => table.RemoveByIdPartial(1, 4),
            _ => table.RemoveById(1,
                new(EnumerationOrder.Ascending, 1, KeyProposition.Included, 4, KeyProposition.Included))
        };
        Assert.Equal(3, removed);
        Assert.Equal(2ul, Assert.Single(table).Id);
        foreach (var value in values)
            Assert.Equal(value == "keep" ? 1 : 0, table.FindByComputed(value + "!").Count());
    }


    [Generate]
    public class StringConversionRecord
    {
        [PrimaryKey(1)] public ulong BatchId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [SecondaryKey("A")]
        [SecondaryKey("Reverse", Order = 2)] public string? A { get; set; }
        [SecondaryKey("Reverse", Order = 1)] public string? B { get; set; }
    }

    public interface IStringConversionTable : IRelation<StringConversionRecord>
    {
        void Insert(StringConversionRecord value);
        void Update(StringConversionRecord value);
        void UpdateById(ulong batchId, ulong id, string? a, string? b);
        int RemoveById(ulong batchId);
        IEnumerable<StringConversionRecord> FindByA(string? a);
        IEnumerable<StringConversionRecord> FindByReverse(string? b, string? a);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void StringConversionWorksWithBothMergerImplementations(bool noEmit)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            var values = new[] { null, "", "ASCII", "\0\u007f\u0080", "😀\ud800!\udfff", new string('z', 8192) };
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IStringConversionTable>();
            for (var i = 0; i < values.Length; i++)
                table.Insert(new StringConversionRecord
                {
                    BatchId = 1, Id = (ulong)i, A = values[i], B = values[values.Length - i - 1]
                });
            for (var i = 0; i < values.Length; i++)
            {
                var a = values[i];
                var b = values[values.Length - i - 1];
                Assert.Single(table.FindByReverse(b, a));
                table.UpdateById(1, (ulong)i, "changed" + a, b);
                Assert.Empty(table.FindByA(a));
                Assert.Empty(table.FindByReverse(b, a));
                Assert.Single(table.FindByReverse(b, "changed" + a));
                table.Update(new StringConversionRecord { BatchId = 1, Id = (ulong)i, A = a, B = b });
                Assert.Empty(table.FindByReverse(b, "changed" + a));
                Assert.Single(table.FindByA(a));
                Assert.Single(table.FindByReverse(b, a));
            }
            Assert.Equal(values.Length, table.RemoveById(1));
            foreach (var value in values) Assert.Empty(table.FindByA(value));
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }


    [Generate]
    public class IntegerConversionRecord
    {
        [PrimaryKey(1)] public ulong BatchId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        [SecondaryKey("Signed")]
        [SecondaryKey("Reverse", Order = 2)] public long Signed { get; set; }
        [SecondaryKey("Unsigned")]
        [SecondaryKey("Reverse", Order = 1)] public ulong Unsigned { get; set; }
    }

    public interface IIntegerConversionTable : IRelation<IntegerConversionRecord>
    {
        void Insert(IntegerConversionRecord value);
        void UpdateById(ulong batchId, ulong id, long signed, ulong unsigned);
        int RemoveById(ulong batchId);
        IEnumerable<IntegerConversionRecord> FindBySigned(long signed);
        IEnumerable<IntegerConversionRecord> FindByUnsigned(ulong unsigned);
        IEnumerable<IntegerConversionRecord> FindByReverse(ulong unsigned, long signed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IntegerCopiesWorkWithBothMergerImplementations(bool noEmit)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            using var tr = _db.StartTransaction();
            var table = tr.GetRelation<IIntegerConversionTable>();
            var values = new[] { long.MinValue, -8193, -65, -64, -1, 0, 63, 64, 8192, long.MaxValue };
            for (var i = 0; i < values.Length; i++)
                table.Insert(new IntegerConversionRecord
                {
                    BatchId = 1, Id = (ulong)i, Signed = values[i], Unsigned = unchecked((ulong)values[i])
                });
            for (var i = 0; i < values.Length; i++)
            {
                var signed = values[i];
                var unsigned = unchecked((ulong)signed);
                Assert.Single(table.FindByReverse(unsigned, signed));
                table.UpdateById(1, (ulong)i, signed ^ 42, unsigned ^ 42);
                Assert.Empty(table.FindBySigned(signed));
                Assert.Empty(table.FindByUnsigned(unsigned));
                Assert.Empty(table.FindByReverse(unsigned, signed));
                Assert.Single(table.FindByReverse(unsigned ^ 42, signed ^ 42));
                table.UpdateById(1, (ulong)i, signed, unsigned);
            }
            Assert.Equal(values.Length, table.RemoveById(1));
            foreach (var value in values)
            {
                Assert.Empty(table.FindBySigned(value));
                Assert.Empty(table.FindByUnsigned(unchecked((ulong)value)));
            }
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

}
