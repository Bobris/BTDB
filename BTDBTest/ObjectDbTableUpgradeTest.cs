using System;
using System.Collections.Generic;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest
{
    public class ObjectDbTableUpgradeTest : IDisposable
    {
        readonly IKeyValueDB _lowDb;
        IObjectDB _db;

        public ObjectDbTableUpgradeTest()
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
            _db = new ObjectDB();
            _db.Open(_lowDb, false);
        }

        public class JobV1
        {
            [PrimaryKey(1)]
            public ulong Id { get; set; }

            public string Name { get; set; }
        }

        public interface IJobTable1
        {
            void Insert(JobV1 job);
        }

        public class JobV2
        {
            [PrimaryKey(1)]
            public ulong Id { get; set; }

            [SecondaryKey("Name")]
            public string Name { get; set; }

            [SecondaryKey("Cost")] //todo [SecondaryKey("Cost", IncludePrimaryKeyOrder = 1)]
            public int Cost { get; set; }
        }

        public interface IJobTable2
        {
            void Insert(JobV2 job);
            JobV2 FindByNameOrDefault(string name);
            JobV2 FindByCostOrDefault(int cost);
            IEnumerator<JobV2> ListByCost(AdvancedEnumeratorParam<int> param);
        }

        public class JobV3
        {
            [PrimaryKey(1)]
            public ulong Id { get; set; }

            [SecondaryKey("Cost")]
            public double Cost { get; set; }
        }

        public class JobIncompatible
        {
            [PrimaryKey(1)]
            public Guid Id { get; set; }
        }

        public interface IJobTableIncompatible
        {
            void Insert(JobIncompatible job);
        }

        [Fact]
        public void ChangeOfPrimaryKeyIsNotSupported()
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
                Assert.Throws<BTDBException>(() => tr.InitRelation<IJobTableIncompatible>("Job"));
            }
        }

        [Fact]
        public void NewIndexesAreAutomaticallyGenerated()
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
                var creator = tr.InitRelation<IJobTable2>("Job");
                var jobTable = creator(tr);
                var job = new JobV2 { Id = 21, Name = "Build", Cost = 42 };
                jobTable.Insert(job);
                var j = jobTable.FindByNameOrDefault("Code");
                Assert.Equal("Code", j.Name);
                j = jobTable.FindByCostOrDefault(42);
                Assert.Equal("Build", j.Name);

                var en = jobTable.ListByCost(new AdvancedEnumeratorParam<int>(EnumerationOrder.Ascending));
                Assert.True(en.MoveNext());
                Assert.Equal(0, en.Current.Cost);
                Assert.True(en.MoveNext());
                Assert.Equal(42, en.Current.Cost);
                tr.Commit();
            }
        }

    }
}