using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.ODBLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class MidLevelDBTest
    {
        ILowLevelDB _lowDB;
        IMidLevelDB _db;

        public interface IPerson
        {
            string Name { get; set; }
            uint Age { get; set; }
        }

        [SetUp]
        public void Setup()
        {
            _lowDB = new LowLevelDB();
            _lowDB.Open(new ManagedMemoryStream(), true);
            _db = new MidLevelDB();
            _db.Open(_lowDB, true);
        }

        [TearDown]
        public void TearDown()
        {
            _db.Dispose();
        }

        [Test]
        public void NewDatabaseIsEmpty()
        {
            using (var tr = _db.StartTransaction())
            {
                Assert.AreEqual(tr.Query<IPerson>().Count(), 0);
            }
        }

        [Test]
        public void InsertPerson()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                p.Name = "Bobris";
                p.Age = 35;
                var p2 = tr.Query<IPerson>().First();
                Assert.AreSame(p, p2);
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
                tr.Commit();
            }
        }

        [Test]
        public void InsertPersonAndQueryInNextTransaction()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                p.Name = "Bobris";
                p.Age = 35;
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Query<IPerson>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
            }
        }

    }
}
