using System;
using System.Collections.Generic;
using System.Text;
using ApprovalTests;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using ApprovalTests.Reporters;

namespace BTDBTest
{
    public class FindUnusedKeysTest : IDisposable
    {
        IKeyValueDB _lowDb;
        IObjectDB _db;

        public FindUnusedKeysTest()
        {
            _lowDb = new InMemoryKeyValueDB();
            OpenDb();
        }

        void OpenDb()
        {
            _db = new ObjectDB();
            _db.Open(_lowDb, false);
        }

        void ReopenDb()
        {
            _db.Dispose();
            OpenDb();
        }

        public class Duty
        {
            public string Name { get; set; }
        }

        public class Job
        {
            public Duty Duty { get; set; }
        }

        public class JobMap
        {
            public IDictionary<ulong, Job> Jobs { get; set; }
        }

        void StoreJob(ulong id, string name)
        {
            using (var tr = _db.StartTransaction())
            {
                var jobs = tr.Singleton<JobMap>();
                jobs.Jobs[id] = new Job { Duty = new Duty { Name = name } };
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
            using (var visitor = new FindUnusedKeysVisitor())
            {
                using (var tr = _db.StartReadOnlyTransaction())
                {
                    visitor.ImportAllKeys(tr);
                    visitor.Iterate(tr);
                    var report = DumpUnseenKeys(visitor, " ");
                    Assert.Equal(string.Empty, report);
                }
            }
        }

        [Fact]
        [UseReporter(typeof(DiffReporter))]
        public void FindAndRemovesUnusedKeys()
        {
            StoreJob(1, "Create leak");
            StoreJob(1, "Code");

            using (var visitor = new FindUnusedKeysVisitor())
            {
                using (var tr = _db.StartReadOnlyTransaction())
                {
                    visitor.ImportAllKeys(tr);
                    visitor.Iterate(tr);
                }
                var report = DumpUnseenKeys(visitor, "\r\n");
                Approvals.Verify(report);

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
                var jobs = tr.Singleton<JobMap>();
                //check that db has is not broken after removing unused keys
                Assert.Equal(jobs.Jobs[1].Duty.Name, "Code");
            }
        }

        [Fact]
        public void TablesWithNoInstancesAreNotReported()
        {
            StoreJob(1, "Sleep");
            using (var tr = _db.StartTransaction())
            {
                var jobs = tr.Singleton<JobMap>();
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
}
