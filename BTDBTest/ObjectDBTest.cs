using System;
using System.Linq;
using BTDB.KVDBLayer.Implementation;
using BTDB.KVDBLayer.Interface;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class ObjectDBTest
    {
        IPositionLessStream _dbstream;
        IKeyValueDB _lowDB;
        IObjectDB _db;

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

        public interface ITree
        {
            ITree Left { get; set; }
            ITree Right { get; set; }
            string Content { get; set; }
        }

        [SetUp]
        public void Setup()
        {
            _dbstream = new MemoryPositionLessStream();
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
            _lowDB = new KeyValueDB();
            _lowDB.Open(_dbstream, false);
            _db = new ObjectDB();
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
        public void PersonDegrade()
        {
            var personObjDBName = _db.RegisterType(typeof(IPersonNew));
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPersonNew>();
                p.Name = "Bobris";
                p.Age = 35;
                p.Comment = "Will be lost";
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(IPerson), personObjDBName);
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<IPerson>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
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
                firstOid = ((IDBObject)p).Oid;
                p = tr.Insert<IPerson>();
                Assert.AreEqual(firstOid + 1, ((IDBObject)p).Oid);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                Assert.AreEqual(firstOid + 2, ((IDBObject)p).Oid);
                tr.Commit();
            }
            ReopenDB();
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                Assert.AreEqual(firstOid + 3, ((IDBObject)p).Oid);
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

        [Test]
        public void GetByOid()
        {
            ulong firstOid;
            using (var tr = _db.StartTransaction())
            {
                var p1 = tr.Insert<IPerson>();
                firstOid = ((IDBObject)p1).Oid;
                p1.Name = "Bobris";
                p1.Age = 35;
                var p2 = tr.Insert<IPerson>();
                p2.Name = "DeadMan";
                p2.Age = 105;
                Assert.AreSame(p1, tr.Get(firstOid));
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Get(firstOid);
                Assert.IsInstanceOf<IPerson>(p);
                Assert.AreEqual("Bobris", ((IPerson)p).Name);
            }
        }

        [Test]
        public void SingletonBasic()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<IPerson>();
                p.Name = "Bobris";
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<IPerson>();
                Assert.AreEqual("Bobris", p.Name);
            }
        }

        [Test]
        public void SingletonComplex()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<IPerson>();
                p.Name = "Garbage";
                // No commit here
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<IPerson>();
                Assert.Null(p.Name);
                p.Name = "Bobris";
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<IPerson>();
                Assert.AreEqual("Bobris", p.Name);
                tr.Delete(p);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<IPerson>();
                Assert.Null(p.Name);
            }
        }

        [Test]
        public void NestedIfaceObject()
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<ITree>();
                t.Content = "Root";
                Assert.Null(t.Left);
                var l = tr.Insert<ITree>();
                l.Content = "Left";
                t.Left = l;
                Assert.AreSame(l, t.Left);
                t.Right = tr.Insert<ITree>();
                t.Right.Content = "Right";
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<ITree>();
                Assert.AreEqual("Root", t.Content);
                Assert.AreEqual("Left", t.Left.Content);
                Assert.AreEqual("Right", t.Right.Content);
            }
        }

        [Test]
        public void NestedIfaceObjectDelete()
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<ITree>();
                t.Left = tr.Insert<ITree>();
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<ITree>();
                Assert.NotNull(t.Left);
                tr.Delete(t.Left);
                Assert.Null(t.Left);
            }
        }

        [Test]
        public void NestedIfaceObjectModification()
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<ITree>();
                t.Left = tr.Insert<ITree>();
                t.Left.Content = "Before";
                tr.Insert<ITree>().Content = "After";
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<ITree>();
                t.Left = tr.Enumerate<ITree>().First(i => i.Content == "After");
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<ITree>();
                Assert.AreEqual("After", t.Left.Content);
            }
        }

        public interface IVariousFieldTypes : IDBObject
        {
            string StringField { get; set; }
            sbyte SByteField { get; set; }
            byte ByteField { get; set; }
            short ShortField { get; set; }
            ushort UShortField { get; set; }
            int IntField { get; set; }
            uint UIntField { get; set; }
            long LongField { get; set; }
            ulong ULongField { get; set; }
            IDBObject DBObjectField { get; set; }
            IVariousFieldTypes VariousFieldTypesField { get; set; }
            bool BoolField { get; set; }
            double DoubleField { get; set; }
            float FloatField { get; set; }
            decimal DecimalField { get; set; }
            Guid GuidField { get; set; }
            DateTime DateTimeField { get; set; }
        }

        [Test]
        public void FieldsOfVariousTypes()
        {
            using (var tr = _db.StartTransaction())
            {
                var o = tr.Singleton<IVariousFieldTypes>();
                Assert.Null(o.StringField);
                Assert.AreEqual(0, o.SByteField);
                Assert.AreEqual(0, o.ByteField);
                Assert.AreEqual(0, o.ShortField);
                Assert.AreEqual(0, o.UShortField);
                Assert.AreEqual(0, o.IntField);
                Assert.AreEqual(0, o.UIntField);
                Assert.AreEqual(0, o.LongField);
                Assert.AreEqual(0, o.ULongField);
                Assert.Null(o.DBObjectField);
                Assert.Null(o.VariousFieldTypesField);
                Assert.False(o.BoolField);
                Assert.AreEqual(0d, o.DoubleField);
                Assert.AreEqual(0f, o.FloatField);
                Assert.AreEqual(0m, o.DecimalField);
                Assert.AreEqual(new DateTime(), o.DateTimeField);
                Assert.AreEqual(new Guid(), o.GuidField);

                o.StringField = "Text";
                o.SByteField = -10;
                o.ByteField = 10;
                o.ShortField = -1000;
                o.UShortField = 1000;
                o.IntField = -100000;
                o.UIntField = 100000;
                o.LongField = -1000000000000;
                o.ULongField = 1000000000000;
                o.DBObjectField = o;
                o.VariousFieldTypesField = o;
                o.BoolField = true;
                o.DoubleField = 12.34;
                o.FloatField = -12.34f;
                o.DecimalField = 123456.789m;
                o.DateTimeField = new DateTime(2000, 1, 1, 12, 34, 56, DateTimeKind.Local);
                o.GuidField = new Guid("39aabab2-9971-4113-9998-a30fc7d5606a");

                AssertContent(o);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var o = tr.Singleton<IVariousFieldTypes>();
                AssertContent(o);
            }
        }

        static void AssertContent(IVariousFieldTypes o)
        {
            Assert.AreEqual("Text", o.StringField);
            Assert.AreEqual(-10, o.SByteField);
            Assert.AreEqual(10, o.ByteField);
            Assert.AreEqual(-1000, o.ShortField);
            Assert.AreEqual(1000, o.UShortField);
            Assert.AreEqual(-100000, o.IntField);
            Assert.AreEqual(100000, o.UIntField);
            Assert.AreEqual(-1000000000000, o.LongField);
            Assert.AreEqual(1000000000000, o.ULongField);
            Assert.AreSame(o, o.DBObjectField);
            Assert.AreSame(o, o.VariousFieldTypesField);
            Assert.True(o.BoolField);
            Assert.AreEqual(12.34, o.DoubleField, 1e-10);
            Assert.AreEqual(-12.34, o.FloatField, 1e-6);
            Assert.AreEqual(123456.789m, o.DecimalField);
            Assert.AreEqual(new DateTime(2000, 1, 1, 12, 34, 56, DateTimeKind.Local), o.DateTimeField);
            Assert.AreEqual(new Guid("39aabab2-9971-4113-9998-a30fc7d5606a"), o.GuidField);
        }

        [Test]
        public void AccessingObjectOutsideOfTransactionIsForbidden()
        {
            IPerson p;
            using (var tr = _db.StartTransaction())
            {
                p = tr.Insert<IPerson>();
                tr.Commit();
            }
            Assert.Throws<BTDBException>(() => { if (p.Name != null) { } });
            Assert.Throws<BTDBException>(() => { p.Name = "does not matter"; });
        }

        [Test]
        public void AccessingDeletedObjectIsForbidden()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Insert<IPerson>();
                tr.Delete(p);
                Assert.Throws<BTDBException>(() => { if (p.Name != null) { } });
                Assert.Throws<BTDBException>(() => { p.Name = "does not matter"; });
            }
        }
    }
}
