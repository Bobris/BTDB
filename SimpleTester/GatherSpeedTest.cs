using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace SimpleTester;

public class Record
{
    [PrimaryKey(1)] public ulong CompanyId { get; set; }

    [PrimaryKey(2)] public ulong BatchId { get; set; }

    [PrimaryKey(3)] public ulong MessageId { get; set; }

    [InKeyValue(4)] public string Recipient { get; set; }
}

public interface IRecordTable : IRelation<Record>
{
    ulong GatherById(ICollection<Record> target, long skip, long take, Constraint<ulong> companyId,
        Constraint<ulong> batchId, Constraint<ulong> messageId, Constraint<string> recipient);
}

public class GatherSpeedTest
{
    readonly BTreeKeyValueDB _lowDb;
    readonly ObjectDB _odb;

    public GatherSpeedTest()
    {
        _lowDb = new BTreeKeyValueDB(new KeyValueDBOptions()
        {
            CompactorScheduler = null,
            Compression = new NoCompressionStrategy(),
            FileCollection = new OnDiskFileCollection("./data")
        });
        _odb = new ObjectDB();
        _odb.Open(_lowDb, false, new DBOptions());
    }

    public void CreateData()
    {
        using var tr = _odb.StartTransaction();
        var t = tr.GetRelation<IRecordTable>();
        var r = new Random(1);
        for (var i = 0u; i < 100u; i++)
        {
            var batches = r.Next(1000, 1000);
            for (var j = 0u; j < batches; j++)
            {
                var messages = r.Next(1000, 1000);
                for (var k = 0u; k < messages; k++)
                {
                    var recipient = "abcdef" + r.Next(1000, 2000) + "@uvw" + r.Next(1000, 2000) + ".com";
                    t.Upsert(new()
                    {
                        CompanyId = i + 1000000, BatchId = j + 1000000, MessageId = k + 1000000, Recipient = recipient
                    });
                }
            }
        }

        tr.Commit();
        _lowDb.CreateKvi(CancellationToken.None);
    }

    public void Run()
    {
        using var tr = _odb.StartTransaction();
        var t = tr.GetRelation<IRecordTable>();
        ICollection<Record> coll = new List<Record>();
        for (int i = 0; i < 2; i++)
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            t.GatherById(coll, 0, 100, Constraint<ulong>.Any, Constraint<ulong>.Any, Constraint<ulong>.Any,
                Constraint.String.Exact("nonexistent"));
            sw.Stop();
            Console.WriteLine("GatherById took " + sw.ElapsedMilliseconds + "ms");
        }
    }
}
