using Assent;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using System;
using System.Collections.Generic;
using System.Text;
using BTDB;
using Xunit;

namespace BTDBTest;

public class FindUnusedKeysTest : IDisposable
{
    readonly IKeyValueDB _lowDb;
    IObjectDB _db;
    readonly Type2NameRegistry _registry;

    public FindUnusedKeysTest()
    {
        _lowDb = new InMemoryKeyValueDB();
        _registry = new Type2NameRegistry();
        OpenDb();
    }

    void OpenDb()
    {
        _db = new ObjectDB();
        _db.Open(_lowDb, false, new DBOptions().WithoutAutoRegistration().WithCustomType2NameRegistry(_registry));
    }

    void ReopenDb()
    {
        _db.Dispose();
        OpenDb();
    }

    void StoreJob(ulong id, string name)
    {
        using (var tr = _db.StartTransaction())
        {
            var jobs = tr.Singleton<ODBIteratorTest.JobMap>();
            jobs.Jobs[id] = new ODBIteratorTest.Job { Duty = new ODBIteratorTest.Duty { Name = name } };
            tr.Commit();
        }
    }

    [Fact]
    public void DoesNotReportFalsePositive()
    {
        StoreJob(1, "Not create leak");
        AssertNoLeaksInDb();
    }

    void AssertNoLeaksInDb()
    {
        Assert.Equal("", FindLeaks());
    }

    string FindLeaks()
    {
        using (var visitor = new FindUnusedKeysVisitor())
        {
            using (var tr = _db.StartReadOnlyTransaction())
            {
                visitor.ImportAllKeys(tr);
                visitor.Iterate(tr);
                return DumpUnseenKeys(visitor, " ");
            }
        }
    }

    void AssertLeaksInDb()
    {
        Assert.False(string.IsNullOrEmpty(FindLeaks()));
    }

    [Generate]
    public class IndirectDuty
    {
        public IIndirect<ODBIteratorTest.Duty> Duty { get; set; }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void HandlesCorrectlyIIndirect(bool deleteCorrectly)
    {
        using (var tr = _db.StartTransaction())
        {
            var duty = tr.Singleton<IndirectDuty>();
            duty.Duty.Value = new ODBIteratorTest.Duty { Name = "Read" };
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var duty = tr.Singleton<IndirectDuty>();
            if (deleteCorrectly)
                tr.Delete(duty.Duty.Value);
            duty.Duty.Value = new ODBIteratorTest.Duty { Name = "Write" };
            tr.Store(duty);
            tr.Commit();
        }

        if (deleteCorrectly)
            AssertNoLeaksInDb();
        else
            AssertLeaksInDb();
    }

    [Generate]
    public class Directory
    {
        public IDictionary<string, ODBIteratorTest.JobMap> Dir { get; set; }
    }

    [Fact]
    public void FindAndRemovesUnusedKeys()
    {
        StoreJobInDictionary("programming", "code");
        StoreJobInDictionary("chess", "mate");
        using (var tr = _db.StartTransaction())
        {
            var sports = tr.Singleton<Directory>();
            sports.Dir["programming"] = new ODBIteratorTest.JobMap();
            tr.Commit();
        }

        using (var visitor = new FindUnusedKeysVisitor())
        {
            using (var tr = _db.StartReadOnlyTransaction())
            {
                visitor.ImportAllKeys(tr);
                visitor.Iterate(tr);
            }

            var report = DumpUnseenKeys(visitor, "\r\n");
            this.Assent(report);

            using (var tr = _db.StartTransaction())
            {
                visitor.DeleteUnused(tr);
                tr.Commit();
            }
        }

        ReopenDb();

        AssertNoLeaksInDb();
        using (var tr = _db.StartReadOnlyTransaction())
        {
            //check that db has is not broken after removing unused keys
            var sports = tr.Singleton<Directory>();
            Assert.Equal("mate", sports.Dir["chess"].Jobs[0].Duty.Name);
        }
    }

    void StoreJobInDictionary(string sport, string activity)
    {
        using (var tr = _db.StartTransaction())
        {
            var sports = tr.Singleton<Directory>();
            sports.Dir[sport] = new ODBIteratorTest.JobMap
            {
                Jobs = new Dictionary<ulong, ODBIteratorTest.Job>
                    { [0] = new ODBIteratorTest.Job { Duty = new ODBIteratorTest.Duty { Name = activity } } }
            };
            tr.Commit();
        }
    }

    [Fact]
    public void TablesWithNoInstancesAreNotReported()
    {
        StoreJob(1, "Sleep");
        using (var tr = _db.StartTransaction())
        {
            var jobs = tr.Singleton<ODBIteratorTest.JobMap>();
            tr.Delete(jobs.Jobs[1].Duty);
            tr.Delete(jobs.Jobs[1]);
            jobs.Jobs.Remove(1);
            tr.Commit();
        }

        AssertNoLeaksInDb();
    }

    static string DumpUnseenKeys(FindUnusedKeysVisitor visitor, string concat)
    {
        var builder = new StringBuilder();
        foreach (var unseenKey in visitor.UnseenKeys())
        {
            if (builder.Length > 0)
                builder.Append(concat);
            foreach (var b in unseenKey.Key)
                builder.Append(b.ToString("X2"));
            builder.Append(" Value len:");
            builder.Append(unseenKey.ValueSize);
        }

        return builder.ToString();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
    }
}
