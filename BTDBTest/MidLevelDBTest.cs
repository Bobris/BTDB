using System;
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
        IStream _dbstream;
        ILowLevelDB _lowDB;
        IMidLevelDB _db;

        public interface IPerson
        {
            string Name { get; set; }
            uint Age { get; set; }
        }

        public interface IPersonNew
        {
            string Name { get; set; }
            string Comment { get; set; }
            ulong Age { get; set; }
        }

        [SetUp]
        public void Setup()
        {
            _dbstream = new ManagedMemoryStream();
            OpenDB();
        }

        [TearDown]
        public void TearDown()
        {
            _db.Dispose();
            _dbstream.Dispose();
        }

        void ReopenDB()
        {
            _db.Dispose();
            OpenDB();
        }

        void OpenDB()
        {
            _lowDB = new LowLevelDB();
            _lowDB.Open(_dbstream, false);
            _db = new MidLevelDB();
            _db.Open(_lowDB, true);
        }

        [Test]
        public void NewDatabaseIsEmpty()
        {
            using (var tr = _db.StartTransaction())
            {
                Assert.AreEqual(tr.Enumerate<IPerson>().Count(), 0);
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
                var p2 = tr.Enumerate<IPerson>().First();
                Assert.AreSame(p, p2);
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
                tr.Commit();
            }
        }

        [Test]
        public void InsertPersonAndEnumerateInNextTransaction()
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
                var p = tr.Enumerate<IPerson>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
            }
        }

        [Test]
        public void InsertPersonAndEnumerateAfterReopen()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                p.Name = "Bobris";
                p.Age = 35;
                tr.Commit();
            }
            ReopenDB();
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<IPerson>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
            }
        }

        [Test]
        public void ModifyPerson()
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
                var p = tr.Enumerate<IPerson>().First();
                p.Age++;
                Assert.AreEqual(36, p.Age);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<IPerson>().First();
                Assert.AreEqual(36, p.Age);
            }
        }

        [Test]
        public void PersonUpgrade()
        {
            var personObjDBName = _db.RegisterType(typeof(IPerson));
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                p.Name = "Bobris";
                p.Age = 35;
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(IPersonNew), personObjDBName);
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<IPersonNew>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
                Assert.AreEqual(null, p.Comment);
            }
        }

        [Test]
        public void ALotsOfPeople()
        {
            using (var tr = _db.StartTransaction())
            {
                for (uint i = 0; i < 1000; i++)
                {
                    var p = tr.Insert<IPerson>();
                    p.Name = string.Format("Person {0}", i);
                    p.Age = i;
                }
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var q = tr.Enumerate<IPerson>().OrderByDescending(p => p.Age);
                uint i = 1000;
                foreach (var p in q)
                {
                    i--;
                    Assert.AreEqual(i, p.Age);
                    Assert.AreEqual(string.Format("Person {0}", i), p.Name);
                }
            }
        }

        [Test]
        public void DeleteObject()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                p.Name = "Bobris";
                p.Age = 35;
                p = tr.Insert<IPerson>();
                p.Name = "DeadMan";
                p.Age = 105;
                Assert.AreEqual(2, tr.Enumerate<IPerson>().Count());
                tr.Delete(p);
                Assert.AreEqual(1, tr.Enumerate<IPerson>().Count());
                tr.Commit();
            }
        }

        [Test]
        public void OIdsAreInOrder()
        {
            ulong firstOid;
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                firstOid = ((IMidLevelObject)p).Oid;
                p = tr.Insert<IPerson>();
                Assert.AreEqual(firstOid + 1, ((IMidLevelObject)p).Oid);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                Assert.AreEqual(firstOid + 2, ((IMidLevelObject)p).Oid);
                tr.Commit();
            }
            ReopenDB();
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                Assert.AreEqual(firstOid + 3, ((IMidLevelObject)p).Oid);
                tr.Commit();
            }
        }

        [Test]
        public void EnumReturnsOidsInOrderAndNewObjIsSkipped()
        {
            using (var tr = _db.StartTransaction())
            {
                var p1 = tr.Insert<IPerson>();
                var p2 = tr.Insert<IPerson>();
                int i = 0;
                foreach (var p in tr.Enumerate<IPerson>())
                {
                    if (i == 0)
                    {
                        Assert.AreSame(p1, p);
                        tr.Insert<IPerson>();
                    }
                    else
                    {
                        Assert.AreSame(p2, p);
                    }
                    i++;
                }
                Assert.AreEqual(2, i);
            }
        }
    }
}
