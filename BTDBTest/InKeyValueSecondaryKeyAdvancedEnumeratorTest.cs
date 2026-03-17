using System.Collections.Generic;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class InKeyValueSecondaryKeyAdvancedEnumeratorTest : ObjectDbTestBase
{
    public InKeyValueSecondaryKeyAdvancedEnumeratorTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Record
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong BatchId { get; set; }
        [PrimaryKey(3)] public ulong MessageId { get; set; }
        [InKeyValue(4)] public string Recipient { get; set; } = "";
        [SecondaryKey("CustomField", IncludePrimaryKeyOrder = 1)]
        public string CustomField { get; set; } = "";
        public string Name { get; set; } = "";
    }

    public interface IRecordTable : IRelation<Record>
    {
        void Insert(Record record);
        IOrderedDictionaryEnumerator<ulong, Record> ListByCustomField(ulong companyId, string customField, AdvancedEnumeratorParam<ulong> param);
        IOrderedDictionaryEnumerator<ulong, Record> ListByCustomField(ulong companyId, string customField, ulong batchId, AdvancedEnumeratorParam<ulong> param);
    }

    [Fact]
    public void ListBySecondaryKeyWithExtraPkPrefixAndAdvancedEnumeratorParamWorks()
    {
        using var tr = _db.StartTransaction();
        var creator = tr.InitRelation<IRecordTable>("Record");
        var table = creator(tr);

        table.Insert(new Record { CompanyId = 1, BatchId = 10, MessageId = 100, CustomField = "cf1", Recipient = "a@b.c", Name = "A" });
        table.Insert(new Record { CompanyId = 1, BatchId = 10, MessageId = 101, CustomField = "cf1", Recipient = "d@e.f", Name = "B" });
        table.Insert(new Record { CompanyId = 1, BatchId = 20, MessageId = 200, CustomField = "cf1", Recipient = "g@h.i", Name = "C" });

        // 2-param overload (companyId + customField + AEP on batchId) should work
        var enumerator = table.ListByCustomField(1, "cf1", AdvancedEnumeratorParam<ulong>.Instance);
        Assert.Equal(3u, enumerator.Count);
        enumerator.Dispose();

        // 3-param overload (companyId + customField + batchId + AEP on messageId) - triggers BTDB0016 build error
        var enumerator2 = table.ListByCustomField(1, "cf1", 10, AdvancedEnumeratorParam<ulong>.Instance);
        Assert.Equal(2u, enumerator2.Count);
        enumerator2.Dispose();

        tr.Commit();
    }
}
