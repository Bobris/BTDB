using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.KVDBLayer;
using BTDB.KV2DBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class ObjectDBTest
    {
        IPositionLessStream _dbstream;
        IFileCollection _fileCollection;
        IKeyValueDB _lowDB;
        IObjectDB _db;

        public class Person
        {
            public string Name { get; set; }
            public uint Age { get; set; }
        }

        public class PersonWithNonStoredProperty
        {
            public string Name { get; set; }
            [NotStored]
            public uint Age { get; set; }
        }

        public class PersonNew
        {
            public string Name { get; set; }
            public string Comment { get; set; }
            public ulong Age { get; set; }
        }

        public class Tree
        {
            public Tree Left { get; set; }
            public Tree Right { get; set; }
            public string Content { get; set; }
        }

        public class Empty
        {
        }

        [SetUp]
        public void Setup()
        {
            _dbstream = new MemoryPositionLessStream();
            _fileCollection = new InMemoryFileCollection();
            OpenDB();
        }

        [TearDown]
        public void TearDown()
        {
            _db.Dispose();
            _dbstream.Dispose();
            _fileCollection.Dispose();
        }

        void ReopenDB()
        {
            _db.Dispose();
            OpenDB();
        }

        void OpenDB()
        {
            //_lowDB = new KeyValueDB(_dbstream);
            _lowDB = new KeyValue2DB(_fileCollection);
            _db = new ObjectDB();
            _db.Open(_lowDB, true);
        }

        [Test]
        public void NewDatabaseIsEmpty()
        {
            using (var tr = _db.StartTransaction())
            {
                Assert.AreEqual(tr.Enumerate<Person>().Count(), 0);
            }
        }

        [Test]
        public void InsertPerson()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = new Person { Name = "Bobris", Age = 35 };
                tr.Store(p);
                var p2 = tr.Enumerate<Person>().First();
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
                tr.Store(new Person { Name = "Bobris", Age = 35 });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<Person>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
            }
        }

        [Test]
        public void InsertPersonAndEnumerateAfterReopen()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new Person { Name = "Bobris", Age = 35 });
                tr.Commit();
            }
            ReopenDB();
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<Person>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
            }
        }

        [Test]
        public void ModifyPerson()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new Person { Name = "Bobris", Age = 35 });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<Person>().First();
                p.Age++;
                tr.Store(p);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<Person>().First();
                Assert.AreEqual(36, p.Age);
            }
        }

        [Test]
        public void PersonUpgrade()
        {
            var personObjDBName = _db.RegisterType(typeof(Person));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new Person { Name = "Bobris", Age = 35 });
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(PersonNew), personObjDBName);
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<PersonNew>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.Age);
                Assert.AreEqual(null, p.Comment);
            }
        }

        [Test]
        public void PersonDegrade()
        {
            var personObjDBName = _db.RegisterType(typeof(PersonNew));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new PersonNew { Name = "Bobris", Age = 35, Comment = "Will be lost" });
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(Person), personObjDBName);
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<Person>().First();
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
                    tr.Store(new Person { Name = string.Format("Person {0}", i), Age = i });
                }
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var q = tr.Enumerate<Person>().OrderByDescending(p => p.Age);
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
                var p = new Person { Name = "Bobris", Age = 35 };
                tr.Store(p);
                p = new Person { Name = "DeadMan", Age = 105 };
                tr.Store(p);
                Assert.AreEqual(2, tr.Enumerate<Person>().Count());
                tr.Delete(p);
                Assert.AreEqual(1, tr.Enumerate<Person>().Count());
                tr.Commit();
            }
        }

        [Test]
        public void OIdsAreInOrder()
        {
            ulong firstOid;
            using (var tr = _db.StartTransaction())
            {
                firstOid = tr.Store(new Person());
                Assert.AreEqual(firstOid + 1, tr.Store(new Person()));
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                Assert.AreEqual(firstOid + 2, tr.Store(new Person()));
                tr.Commit();
            }
            ReopenDB();
            using (var tr = _db.StartTransaction())
            {
                Assert.AreEqual(firstOid + 3, tr.Store(new Person()));
                tr.Commit();
            }
        }

        [Test]
        public void EnumReturnsOidsInOrderAndNewObjIsSkipped()
        {
            using (var tr = _db.StartTransaction())
            {
                var p1 = new Person();
                var p2 = new Person();
                tr.Store(p1);
                tr.Store(p2);
                int i = 0;
                foreach (var p in tr.Enumerate<Person>())
                {
                    if (i == 0)
                    {
                        Assert.AreSame(p1, p);
                        tr.Store(new Person());
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
                var p1 = new Person();
                firstOid = tr.Store(p1);
                p1.Name = "Bobris";
                p1.Age = 35;
                tr.Store(new Person { Name = "DeadMan", Age = 105 });
                Assert.AreSame(p1, tr.Get(firstOid));
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Get(firstOid);
                Assert.IsInstanceOf<Person>(p);
                Assert.AreEqual("Bobris", ((Person)p).Name);
            }
        }

        [Test]
        public void SingletonBasic()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<Person>();
                p.Name = "Bobris";
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<Person>();
                Assert.AreEqual("Bobris", p.Name);
            }
        }

        [Test]
        public void SingletonComplex()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<Person>();
                p.Name = "Garbage";
                // No commit here
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<Person>();
                Assert.Null(p.Name);
                p.Name = "Bobris";
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<Person>();
                Assert.AreEqual("Bobris", p.Name);
                tr.Delete(p);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<Person>();
                Assert.Null(p.Name);
            }
        }

        [Test]
        public void NestedIfaceObject()
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<Tree>();
                t.Content = "Root";
                Assert.Null(t.Left);
                t.Left = new Tree { Content = "Left" };
                t.Right = new Tree { Content = "Right" };
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<Tree>();
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
                var t = tr.Singleton<Tree>();
                t.Left = new Tree();
                tr.Delete(t.Left);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<Tree>();
                Assert.Null(t.Left);
            }
        }

        [Test]
        public void NestedIfaceObjectModification()
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<Tree>();
                t.Left = new Tree { Content = "Before" };
                tr.Store(new Tree { Content = "After" });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<Tree>();
                t.Left = tr.Enumerate<Tree>().First(i => i.Content == "After");
                tr.Store(t);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<Tree>();
                Assert.AreEqual("After", t.Left.Content);
            }
        }

        public enum TestEnum
        {
            Item1,
            Item2
        }

        public class VariousFieldTypes
        {
            public string StringField { get; set; }
            public sbyte SByteField { get; set; }
            public byte ByteField { get; set; }
            public short ShortField { get; set; }
            public ushort UShortField { get; set; }
            public int IntField { get; set; }
            public uint UIntField { get; set; }
            public long LongField { get; set; }
            public ulong ULongField { get; set; }
            public object DBObjectField { get; set; }
            public VariousFieldTypes VariousFieldTypesField { get; set; }
            public bool BoolField { get; set; }
            public double DoubleField { get; set; }
            public float FloatField { get; set; }
            public decimal DecimalField { get; set; }
            public Guid GuidField { get; set; }
            public DateTime DateTimeField { get; set; }
            public TimeSpan TimeSpanField { get; set; }
            public TestEnum EnumField { get; set; }
            public byte[] ByteArrayField { get; set; }
        }

        [Test]
        public void FieldsOfVariousTypes()
        {
            using (var tr = _db.StartTransaction())
            {
                var o = tr.Singleton<VariousFieldTypes>();
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
                Assert.AreEqual(new TimeSpan(), o.TimeSpanField);
                Assert.AreEqual(new Guid(), o.GuidField);
                Assert.AreEqual(TestEnum.Item1, o.EnumField);
                Assert.AreEqual(null, o.ByteArrayField);

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
                o.TimeSpanField = new TimeSpan(1, 2, 3, 4);
                o.GuidField = new Guid("39aabab2-9971-4113-9998-a30fc7d5606a");
                o.EnumField = TestEnum.Item2;
                o.ByteArrayField = new byte[] { 0, 1, 2 };

                AssertContent(o);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var o = tr.Singleton<VariousFieldTypes>();
                AssertContent(o);
            }
        }

        static void AssertContent(VariousFieldTypes o)
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
            Assert.AreEqual(new TimeSpan(1, 2, 3, 4), o.TimeSpanField);
            Assert.AreEqual(new Guid("39aabab2-9971-4113-9998-a30fc7d5606a"), o.GuidField);
            Assert.AreEqual(TestEnum.Item2, o.EnumField);
            Assert.AreEqual(new byte[] { 0, 1, 2 }, o.ByteArrayField);
        }

        public class Root
        {
            public IList<Person> Persons { get; set; }
        }

        [Test]
        public void ListOfDBObjectsSimple()
        {
            _db.RegisterType(typeof(Person));
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<Root>();
                root.Persons = new List<Person> { new Person { Name = "P1" }, new Person { Name = "P2" } };
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<Root>();
                Assert.AreEqual(2, root.Persons.Count);
                var p1 = root.Persons[0];
                var p2 = root.Persons[1];
                Assert.AreEqual("P1", p1.Name);
                Assert.AreEqual("P2", p2.Name);
            }
        }

        public class VariousLists
        {
            public IList<int> IntList { get; set; }
            public IList<string> StringList { get; set; }
            public IList<byte> ByteList { get; set; }
        }

        [Test]
        public void ListOfSimpleValues()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<VariousLists>();
                root.IntList = new List<int> { 5, 10, 2000 };
                root.StringList = new List<string> { "A", null, "AB!" };
                root.ByteList = new List<byte> { 0, 255 };
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<VariousLists>();
                Assert.AreEqual(new List<int> { 5, 10, 2000 }, root.IntList);
                Assert.AreEqual(new List<string> { "A", null, "AB!" }, root.StringList);
                Assert.AreEqual(new List<byte> { 0, 255 }, root.ByteList);
                root.IntList = null;
                root.StringList = null;
                root.ByteList = null;
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<VariousLists>();
                Assert.AreEqual(null, root.IntList);
                Assert.AreEqual(null, root.StringList);
                Assert.AreEqual(null, root.ByteList);
                root.IntList = new List<int>();
                root.StringList = new List<string>();
                root.ByteList = new List<byte>();
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<VariousLists>();
                Assert.AreEqual(new List<int>(), root.IntList);
                Assert.AreEqual(new List<string>(), root.StringList);
                Assert.AreEqual(new List<byte>(), root.ByteList);
            }
        }

        [Test]
        public void ListOfSimpleValuesSkip()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<VariousLists>();
                root.IntList = new List<int> { 5, 10, 2000 };
                root.StringList = new List<string>();
                root.ByteList = null;
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(Empty), "VariousLists");
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<Empty>();
            }
        }

        public class InlineDictionary
        {
            public Dictionary<int, string> Int2String { get; set; }
        }

        [Test]
        public void InlineDictionariesOfSimpleValues()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineDictionary>();
                root.Int2String = new Dictionary<int, string> { { 1, "one" }, { 0, null } };
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineDictionary>();
                Assert.AreEqual(2, root.Int2String.Count);
                Assert.AreEqual("one", root.Int2String[1]);
                Assert.AreEqual(null, root.Int2String[0]);
                root.Int2String.Clear();
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineDictionary>();
                Assert.AreEqual(0, root.Int2String.Count);
                root.Int2String = null;
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineDictionary>();
                Assert.IsNull(root.Int2String);
            }
        }

        [Test]
        public void InlineDictionariesOfSimpleValuesSkip()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineDictionary>();
                root.Int2String = new Dictionary<int, string> { { 1, "one" }, { 0, null } };
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(Empty), "InlineDictionary");
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<Empty>();
            }
        }

        public class InlineList
        {
            public List<int> IntList { get; set; }
        }

        [Test]
        public void InlineListsOfSimpleValues()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineList>();
                root.IntList = new List<int> { 1, 2, 3 };
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineList>();
                Assert.AreEqual(3, root.IntList.Count);
                Assert.AreEqual(1, root.IntList[0]);
                Assert.AreEqual(2, root.IntList[1]);
                Assert.AreEqual(3, root.IntList[2]);
                root.IntList.Clear();
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineList>();
                Assert.AreEqual(0, root.IntList.Count);
                root.IntList = null;
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineList>();
                Assert.IsNull(root.IntList);
            }
        }

        [Test]
        public void InlineListsOfSimpleValuesSkip()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineList>();
                root.IntList = new List<int> { 1, 2 };
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(Empty), "InlineList");
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<Empty>();
            }
        }

        public class SimpleDictionary
        {
            public IDictionary<int, string> Int2String { get; set; }
        }

        [Test]
        public void DictionariesOfSimpleValues()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<SimpleDictionary>();
                root.Int2String = new Dictionary<int, string> { { 1, "one" }, { 0, null } };
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<SimpleDictionary>();
                Assert.AreEqual(2, root.Int2String.Count);
                Assert.AreEqual("one", root.Int2String[1]);
                Assert.AreEqual(null, root.Int2String[0]);
                root.Int2String.Clear();
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<SimpleDictionary>();
                Assert.AreEqual(0, root.Int2String.Count);
            }
        }

        [Test]
        public void DictionariesOfSimpleValuesSkip()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<SimpleDictionary>();
                root.Int2String = new Dictionary<int, string> { { 1, "one" }, { 0, null } };
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(Empty), "SimpleDictionary");
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<Empty>();
            }
        }

        public class ComplexDictionary
        {
            public IDictionary<string, Person> String2Person { get; set; }
        }

        [Test]
        public void DictionariesOfComplexValues()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                Assert.NotNull(root.String2Person);
                root.String2Person = new Dictionary<string, Person> { { "Boris", new Person { Name = "Boris", Age = 35 } }, { "null", null } };
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                Assert.AreEqual(2, root.String2Person.Count);
                Assert.IsTrue(root.String2Person.ContainsKey("Boris"));
                Assert.IsTrue(root.String2Person.ContainsKey("null"));
                var p = root.String2Person["Boris"];
                Assert.NotNull(p);
                Assert.AreEqual("Boris", p.Name);
                Assert.AreEqual(35, root.String2Person["Boris"].Age);
                Assert.AreEqual(null, root.String2Person["null"]);
                Assert.AreEqual(new[] { "Boris", "null" }, root.String2Person.Keys);
                Assert.AreEqual(p, root.String2Person.Values.First());
                root.String2Person.Clear();
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                Assert.AreEqual(0, root.String2Person.Count);
            }
            ReopenDB();
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                Assert.AreEqual(0, root.String2Person.Count);
            }
        }

        [Test]
        public void DictionariesOfComplexValuesSkip()
        {
            _db.RegisterType(typeof(Person));
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                root.String2Person = new Dictionary<string, Person> { { "Boris", new Person { Name = "Boris", Age = 35 } }, { "null", null } };
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(Empty), "ComplexDictionary");
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<Empty>();
            }
        }

        public class ByteArrayDictionary
        {
            public IDictionary<byte[], byte[]> Bytes2Bytes { get; set; }
        }

        [Test]
        public void DictionaryOfByteArrayKeysAndValues()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<ByteArrayDictionary>();
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ByteArrayDictionary>();
                root.Bytes2Bytes.Add(new byte[] { 2 }, new byte[] { 2, 2 });
                root.Bytes2Bytes.Add(new byte[] { 1, 0xFF }, new byte[] { 1 });
                root.Bytes2Bytes.Add(new byte[0], new byte[0]);

                AssertEqual(new KeyValuePair<byte[], byte[]>(new byte[0], new byte[0]), root.Bytes2Bytes.First());
                AssertEqual(new KeyValuePair<byte[], byte[]>(new byte[] { 1, 0xFF }, new byte[] { 1 }), root.Bytes2Bytes.Skip(1).First());
                AssertEqual(new KeyValuePair<byte[], byte[]>(new byte[] { 2 }, new byte[] { 2, 2 }), root.Bytes2Bytes.Skip(2).First());
            }
        }

        void AssertEqual(KeyValuePair<byte[], byte[]> expected, KeyValuePair<byte[], byte[]> actual)
        {
            Assert.True(expected.Key.SequenceEqual(actual.Key));
            Assert.True(expected.Value.SequenceEqual(actual.Value));
        }

        public enum TestEnumUlong : ulong
        {
            Item1,
            Item2
        }

        public enum TestEnumEx
        {
            Item1,
            Item2,
            Item3
        }

        public class CTestEnum
        {
            public TestEnum E { get; set; }
        }

        public class CTestEnumUlong
        {
            public TestEnumUlong E { get; set; }
        }

        public class CTestEnumEx
        {
            public TestEnumEx E { get; set; }
        }

        [Test]
        public void EnumUpgrade()
        {
            TestEnum2TestEnumUlong(TestEnum.Item1, TestEnumUlong.Item1);
            TestEnum2TestEnumUlong(TestEnum.Item2, TestEnumUlong.Item2);
        }

        void TestEnum2TestEnumUlong(TestEnum from, TestEnumUlong to)
        {
            ReopenDB();
            var testEnumObjDBName = _db.RegisterType(typeof(CTestEnum));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new CTestEnum { E = from });
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(CTestEnumUlong), testEnumObjDBName);
            using (var tr = _db.StartTransaction())
            {
                var e = tr.Enumerate<CTestEnumUlong>().First();
                Assert.AreEqual(to, e.E);
                tr.Delete(e);
                tr.Commit();
            }
        }

        [Test]
        public void EnumUpgradeAddItem()
        {
            TestEnum2TestEnumEx(TestEnum.Item1, TestEnumEx.Item1);
            TestEnum2TestEnumEx(TestEnum.Item2, TestEnumEx.Item2);
        }

        void TestEnum2TestEnumEx(TestEnum from, TestEnumEx to)
        {
            ReopenDB();
            var testEnumObjDBName = _db.RegisterType(typeof(CTestEnum));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new CTestEnum { E = from });
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(CTestEnumEx), testEnumObjDBName);
            using (var tr = _db.StartTransaction())
            {
                var e = tr.Enumerate<CTestEnumEx>().First();
                Assert.AreEqual(to, e.E);
                tr.Delete(e);
                tr.Commit();
            }
        }

        public class Manager : Person
        {
            public List<Person> Managing { get; set; }
        }

        [Test]
        public void InheritanceSupportedWithAutoRegistration()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                var sl1 = new Person { Name = "Poor Slave", Age = 18 };
                var sl2 = new Person { Name = "Poor Poor Slave", Age = 17 };
                root.String2Person.Add("slave", sl1);
                root.String2Person.Add("slave2", sl2);
                root.String2Person.Add("master", new Manager { Name = "Chief", Age = 19, Managing = new List<Person> { sl1, sl2 } });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                var dict = root.String2Person;
                Assert.IsInstanceOf<Person>(dict["slave"]);
                Assert.IsInstanceOf<Manager>(dict["master"]);
                Assert.AreEqual(3, dict.Count);
                Assert.AreEqual("Chief", dict["master"].Name);
                Assert.AreSame(dict["slave2"], ((Manager)dict["master"]).Managing[1]);
            }
        }

        [Test]
        public void NotStoredProperties()
        {
            var personObjDBName = _db.RegisterType(typeof(PersonWithNonStoredProperty));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new PersonWithNonStoredProperty { Name = "Bobris", Age = 35 });
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(Person), personObjDBName);
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<Person>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(0, p.Age);
            }
        }

        [StoredInline]
        public class InlinePerson : Person
        {
        }

        public class ListOfInlinePersons
        {
            public List<InlinePerson> InlinePersons { get; set; }
        }

        [Test]
        public void SupportOfInlineObjects()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ListOfInlinePersons>();
                root.InlinePersons = new List<InlinePerson> { new InlinePerson { Name = "Me" } };
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ListOfInlinePersons>();
                root.InlinePersons[0].Age = 1;
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ListOfInlinePersons>();
                Assert.AreEqual(1, root.InlinePersons[0].Age);
            }
        }

        public class PersonWithPrivateAge
        {
            public string Name { get; set; }
            private uint Age { get; set; }
            [NotStored]
            public uint PublicAge
            {
                get { return Age; }
                set { Age = value; }
            }
        }

        [Test]
        public void SupportForStorageOfPrivateProperties()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new PersonWithPrivateAge { Name = "Bobris", PublicAge = 35 });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<PersonWithPrivateAge>().First();
                Assert.AreEqual("Bobris", p.Name);
                Assert.AreEqual(35, p.PublicAge);
            }
        }

        [Test]
        public void SingletonShouldNotNeedWritableTransactionIfNotCommited()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<ComplexDictionary>();
                Assert.False(((IInternalObjectDBTransaction)tr).KeyValueDBTransaction.IsWritting());
            }
        }

        public class TwoComplexDictionary
        {
            public IDictionary<string, Person> String2Person { get; set; }
            public IDictionary<string, PersonNew> String2PersonNew { get; set; }
        }

        [Test]
        public void UpgradingWithNewComplexDictionary()
        {
            var typeName = _db.RegisterType(typeof(ComplexDictionary));
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<ComplexDictionary>();
                d.String2Person.Add("A", new Person { Name = "A" });
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(TwoComplexDictionary), typeName);
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<TwoComplexDictionary>();
                tr.Store(d);
                Assert.AreEqual(1, d.String2Person.Count);
                Assert.AreEqual("A", d.String2Person["A"].Name);
                d.String2PersonNew.Add("B", new PersonNew { Name = "B" });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<TwoComplexDictionary>();
                Assert.AreEqual("A", d.String2Person["A"].Name);
                Assert.AreEqual("B", d.String2PersonNew["B"].Name);
            }
        }

        public class ObjectWithDictWithInlineKey
        {
            public IDictionary<InlinePerson, int> Dict { get; set; }
        }

        [StoredInline]
        public class InlinePersonNew : PersonNew
        {
        }

        public class ObjectWithDictWithInlineKeyNew
        {
            public IDictionary<InlinePersonNew, int> Dict { get; set; }
        }

        [Test, Ignore]
        public void UpgradingKeyInDictionary()
        {
            var singName = _db.RegisterType(typeof(ObjectWithDictWithInlineKey));
            var persName = _db.RegisterType(typeof(InlinePerson));
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<ObjectWithDictWithInlineKey>();
                d.Dict.Add(new InlinePerson { Name = "A" }, 1);
                tr.Commit();
            }
            ReopenDB();
            _db.RegisterType(typeof(ObjectWithDictWithInlineKeyNew), singName);
            _db.RegisterType(typeof(InlinePersonNew), persName);
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<ObjectWithDictWithInlineKeyNew>();
                var p = d.Dict.Keys.First();
                Assert.AreEqual("A", p.Name);
                Assert.AreEqual(1, d.Dict[p]);
            }
        }
    }
}
