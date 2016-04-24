using ApprovalTests;
using ApprovalTests.Reporters;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

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

        public class Link
        {
            [PrimaryKey]
            public ulong Id { get; set; }
            public IDictionary<ulong, ulong> Edges { get; set; }
            public string Name { get; set; }
        }

        public interface ILinks
        {
            void Insert(Link link);
            bool RemoveById(ulong id);
        }

        [Fact]
        public void DoesNotReportFalsePositiveInRelations()
        {
            Func<IObjectDBTransaction, ILinks> creator;
            using (var tr = _db.StartTransaction())
            {
                creator = tr.InitRelation<ILinks>("LinksRelation");
                var links = creator(tr);
                var link = new Link { Id = 1, Edges = new Dictionary<ulong, ulong> { [0] = 1, [1] = 2, [2] = 3 } };
                links.Insert(link);
                tr.Commit();
            }
            AssertNoLeaksInDb();
            using (var tr = _db.StartTransaction())
            {
                var links = creator(tr);
                Assert.True(links.RemoveById(1));
                tr.Commit();
            }
            AssertNoLeaksInDb();
        }

        void AssertNoLeaksInDb()
        {
            Assert.Equal(string.Empty, FindLeaks());
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

        public class IndirectDuty
        {
            public IIndirect<Duty> Duty { get; set; }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void HandlesCorrectlyIIndirect(bool deleteCorrectly)
        {
            using (var tr = _db.StartTransaction())
            {
                var duty = tr.Singleton<IndirectDuty>();
                duty.Duty.Value = new Duty { Name = "Read" };
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var duty = tr.Singleton<IndirectDuty>();
                if (deleteCorrectly)
                    tr.Delete(duty.Duty.Value);
                duty.Duty.Value = new Duty { Name = "Write" };
                tr.Store(duty);
                tr.Commit();
            }
            if (deleteCorrectly)
                AssertNoLeaksInDb();
            else
                AssertLeaksInDb();
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
            Console.WriteLine(builder.ToString());
            return builder.ToString();
        }

        public void Dispose()
        {
            _db.Dispose();
            _lowDb.Dispose();
        }

    }
}
