using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDbSelfHealingTest : IDisposable
{
    readonly IKeyValueDB _lowDb;
    IObjectDB _db;
    LoggerMock _logger;

    class LoggerMock : IObjectDBLogger
    {
        public readonly IList<Tuple<string, string>> ReportedIncompatibleKeys = new List<Tuple<string, string>>();

        public void ReportIncompatiblePrimaryKey(string relationName, string field)
        {
            ReportedIncompatibleKeys.Add(Tuple.Create(relationName, field));
        }
    }

    public ObjectDbSelfHealingTest()
    {
        _lowDb = new InMemoryKeyValueDB();
        OpenDb();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
    }

    void ReopenDb()
    {
        _db.Dispose();
        OpenDb();
    }

    void OpenDb()
    {
        _logger = new LoggerMock();
        _db = new ObjectDB { Logger = _logger };
        _db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration().WithSelfHealing());
    }

    public class JobV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("Name")] public string Name { get; set; }
    }

    public interface IJobTable1 : IRelation<JobV1>
    {
        void Insert(JobV1 job);
    }

    public class JobIncompatible
    {
        [PrimaryKey(1)] public Guid Id { get; set; }
        [SecondaryKey("Name")] public string Name { get; set; }
    }

    public interface IJobTableIncompatible : IRelation<JobIncompatible>
    {
        void Insert(JobIncompatible job);
        IEnumerable<JobIncompatible> ListByName(AdvancedEnumeratorParam<string> name);
    }

    [Fact]
    public void ChangeOfPrimaryKeyDeleteAllData()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTable1>("Job");
            var jobTable = creator(tr);
            var job = new JobV1 { Id = 11, Name = "Code" };
            jobTable.Insert(job);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTableIncompatible>("Job");
            Assert.NotEmpty(_logger.ReportedIncompatibleKeys);
            Assert.Equal("Job", _logger.ReportedIncompatibleKeys[0].Item1);
            Assert.Equal("Id", _logger.ReportedIncompatibleKeys[0].Item2);

            var jobTable = creator(tr);
            Assert.Empty(jobTable);

            var job = new JobIncompatible() { Id = Guid.NewGuid(), Name = "Code" };
            jobTable.Insert(job);

            var en = jobTable.ListByName(new(EnumerationOrder.Ascending)).First();
            Assert.Equal("Code", en.Name);

            AssertNoLeaksInDb();
        }
    }

    void AssertNoLeaksInDb()
    {
        using (var visitor = new FindUnusedKeysVisitor())
        {
            using (var tr = _db.StartReadOnlyTransaction())
            {
                visitor.ImportAllKeys(tr);
                visitor.Iterate(tr);
                Assert.Empty(visitor.UnseenKeys());
            }
        }
    }
}
