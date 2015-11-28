using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest
{
    public class ObjectDbTest : IDisposable
    {
        IKeyValueDB _lowDb;
        IObjectDB _db;

        public class Person : IEquatable<Person>
        {
            public string Name { get; set; }
            public uint Age { get; set; }

            public bool Equals(Person other)
            {
                return Name == other.Name && Age == other.Age;
            }
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

        public ObjectDbTest()
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

        [Fact]
        public void NewDatabaseIsEmpty()
        {
            using (var tr = _db.StartTransaction())
            {
                Assert.Equal(tr.Enumerate<Person>().Count(), 0);
            }
        }

        [Fact]
        public void ReadOnlyTransactionThrowsOnWriteAccess()
        {
            using (var tr = _db.StartReadOnlyTransaction())
            {
                Assert.Throws<BTDBTransactionRetryException>(() => tr.StoreAndFlush(new Person()));
            }
        }


        [Fact]
        public void InsertPerson()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = new Person { Name = "Bobris", Age = 35 };
                tr.Store(p);
                var p2 = tr.Enumerate<Person>().First();
                Assert.Same(p, p2);
                Assert.Equal("Bobris", p.Name);
                Assert.Equal(35u, p.Age);
                tr.Commit();
            }
        }

        [Fact]
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
                Assert.Equal("Bobris", p.Name);
                Assert.Equal(35u, p.Age);
            }
        }

        [Fact]
        public void InsertPersonAndEnumerateAfterReopen()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new Person { Name = "Bobris", Age = 35 });
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<Person>().First();
                Assert.Equal("Bobris", p.Name);
                Assert.Equal(35u, p.Age);
            }
        }

        [Fact]
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
                Assert.Equal(36u, p.Age);
            }
        }

        [Fact]
        public void PersonUpgrade()
        {
            var personObjDbName = _db.RegisterType(typeof(Person));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new Person { Name = "Bobris", Age = 35 });
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(PersonNew), personObjDbName);
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<PersonNew>().First();
                Assert.Equal("Bobris", p.Name);
                Assert.Equal(35u, p.Age);
                Assert.Equal(null, p.Comment);
            }
        }

        [Fact]
        public void PersonDegrade()
        {
            var personObjDbName = _db.RegisterType(typeof(PersonNew));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new PersonNew { Name = "Bobris", Age = 35, Comment = "Will be lost" });
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(Person), personObjDbName);
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<Person>().First();
                Assert.Equal("Bobris", p.Name);
                Assert.Equal(35u, p.Age);
            }
        }

        [Fact]
        public void ALotsOfPeople()
        {
            using (var tr = _db.StartTransaction())
            {
                for (uint i = 0; i < 1000; i++)
                {
                    tr.Store(new Person { Name = $"Person {i}", Age = i });
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
                    Assert.Equal(i, p.Age);
                    Assert.Equal($"Person {i}", p.Name);
                }
            }
        }

        [Fact]
        public void DeleteObject()
        {
            using (var tr = _db.StartTransaction())
            {
                var p = new Person { Name = "Bobris", Age = 35 };
                tr.Store(p);
                p = new Person { Name = "DeadMan", Age = 105 };
                tr.Store(p);
                Assert.Equal(2, tr.Enumerate<Person>().Count());
                tr.Delete(p);
                Assert.Equal(1, tr.Enumerate<Person>().Count());
                tr.Commit();
            }
        }

        [Fact]
        public void OIdsAreInOrder()
        {
            ulong firstOid;
            using (var tr = _db.StartTransaction())
            {
                firstOid = tr.Store(new Person());
                Assert.Equal(firstOid + 1, tr.Store(new Person()));
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                Assert.Equal(firstOid + 2, tr.Store(new Person()));
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                Assert.Equal(firstOid + 3, tr.Store(new Person()));
                tr.Commit();
            }
        }

        [Fact]
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
                        Assert.Same(p1, p);
                        tr.Store(new Person());
                    }
                    else
                    {
                        Assert.Same(p2, p);
                    }
                    i++;
                }
                Assert.Equal(2, i);
            }
        }

        [Fact]
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
                Assert.Same(p1, tr.Get(firstOid));
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Get(firstOid);
                Assert.IsType<Person>(p);
                Assert.Equal("Bobris", ((Person)p).Name);
            }
        }

        [Fact]
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
                Assert.Equal("Bobris", p.Name);
            }
        }

        [Fact]
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
                Assert.Equal("Bobris", p.Name);
                tr.Delete(p);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Singleton<Person>();
                Assert.Null(p.Name);
            }
        }

        [Fact]
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
                Assert.Equal("Root", t.Content);
                Assert.Equal("Left", t.Left.Content);
                Assert.Equal("Right", t.Right.Content);
            }
        }

        [Fact]
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

        [Fact]
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
                Assert.Equal("After", t.Left.Content);
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
            public object DbObjectField { get; set; }
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
            public ByteBuffer ByteBufferField { get; set; }
        }

        [Fact]
        public void FieldsOfVariousTypes()
        {
            using (var tr = _db.StartTransaction())
            {
                var o = tr.Singleton<VariousFieldTypes>();
                Assert.Null(o.StringField);
                Assert.Equal(0, o.SByteField);
                Assert.Equal(0, o.ByteField);
                Assert.Equal(0, o.ShortField);
                Assert.Equal(0, o.UShortField);
                Assert.Equal(0, o.IntField);
                Assert.Equal(0u, o.UIntField);
                Assert.Equal(0, o.LongField);
                Assert.Equal(0u, o.ULongField);
                Assert.Null(o.DbObjectField);
                Assert.Null(o.VariousFieldTypesField);
                Assert.False(o.BoolField);
                Assert.Equal(0d, o.DoubleField);
                Assert.Equal(0f, o.FloatField);
                Assert.Equal(0m, o.DecimalField);
                Assert.Equal(new DateTime(), o.DateTimeField);
                Assert.Equal(new TimeSpan(), o.TimeSpanField);
                Assert.Equal(new Guid(), o.GuidField);
                Assert.Equal(TestEnum.Item1, o.EnumField);
                Assert.Equal(null, o.ByteArrayField);
                Assert.Equal(ByteBuffer.NewEmpty().ToByteArray(), o.ByteBufferField.ToByteArray());

                o.StringField = "Text";
                o.SByteField = -10;
                o.ByteField = 10;
                o.ShortField = -1000;
                o.UShortField = 1000;
                o.IntField = -100000;
                o.UIntField = 100000;
                o.LongField = -1000000000000;
                o.ULongField = 1000000000000;
                o.DbObjectField = o;
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
                o.ByteBufferField = ByteBuffer.NewAsync(new byte[] { 0, 1, 2 }, 1, 1);

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
            Assert.Equal("Text", o.StringField);
            Assert.Equal(-10, o.SByteField);
            Assert.Equal(10, o.ByteField);
            Assert.Equal(-1000, o.ShortField);
            Assert.Equal(1000, o.UShortField);
            Assert.Equal(-100000, o.IntField);
            Assert.Equal(100000u, o.UIntField);
            Assert.Equal(-1000000000000, o.LongField);
            Assert.Equal(1000000000000u, o.ULongField);
            Assert.Same(o, o.DbObjectField);
            Assert.Same(o, o.VariousFieldTypesField);
            Assert.True(o.BoolField);
            Assert.InRange(12.34 - o.DoubleField, -1e-10, 1e10);
            Assert.InRange(-12.34 - o.FloatField, -1e-6, 1e6);
            Assert.Equal(123456.789m, o.DecimalField);
            Assert.Equal(new DateTime(2000, 1, 1, 12, 34, 56, DateTimeKind.Local), o.DateTimeField);
            Assert.Equal(new TimeSpan(1, 2, 3, 4), o.TimeSpanField);
            Assert.Equal(new Guid("39aabab2-9971-4113-9998-a30fc7d5606a"), o.GuidField);
            Assert.Equal(TestEnum.Item2, o.EnumField);
            Assert.Equal(new byte[] { 0, 1, 2 }, o.ByteArrayField);
            Assert.Equal(new byte[] { 1 }, o.ByteBufferField.ToByteArray());
        }

        public class Root
        {
            public IList<Person> Persons { get; set; }
        }

        [Fact]
        public void ListOfDbObjectsSimple()
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
                Assert.Equal(2, root.Persons.Count);
                var p1 = root.Persons[0];
                var p2 = root.Persons[1];
                Assert.Equal("P1", p1.Name);
                Assert.Equal("P2", p2.Name);
            }
        }

        public class VariousLists
        {
            public IList<int> IntList { get; set; }
            public IList<string> StringList { get; set; }
            public IList<byte> ByteList { get; set; }
        }

        [Fact]
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
                Assert.Equal(new List<int> { 5, 10, 2000 }, root.IntList);
                Assert.Equal(new List<string> { "A", null, "AB!" }, root.StringList);
                Assert.Equal(new List<byte> { 0, 255 }, root.ByteList);
                root.IntList = null;
                root.StringList = null;
                root.ByteList = null;
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<VariousLists>();
                Assert.Equal(null, root.IntList);
                Assert.Equal(null, root.StringList);
                Assert.Equal(null, root.ByteList);
                root.IntList = new List<int>();
                root.StringList = new List<string>();
                root.ByteList = new List<byte>();
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<VariousLists>();
                Assert.Equal(new List<int>(), root.IntList);
                Assert.Equal(new List<string>(), root.StringList);
                Assert.Equal(new List<byte>(), root.ByteList);
            }
        }

        [Fact]
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
            ReopenDb();
            _db.RegisterType(typeof(Empty), "VariousLists");
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<Empty>();
            }
        }

        public class InlineDictionary
        {
            public Dictionary<int, string> Int2String { get; set; }
        }

        [Fact]
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
                Assert.Equal(2, root.Int2String.Count);
                Assert.Equal("one", root.Int2String[1]);
                Assert.Equal(null, root.Int2String[0]);
                root.Int2String.Clear();
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineDictionary>();
                Assert.Equal(0, root.Int2String.Count);
                root.Int2String = null;
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineDictionary>();
                Assert.Null(root.Int2String);
            }
        }

        [Fact]
        public void InlineDictionariesOfSimpleValuesSkip()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineDictionary>();
                root.Int2String = new Dictionary<int, string> { { 1, "one" }, { 0, null } };
                tr.Commit();
            }
            ReopenDb();
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

        [Fact]
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
                Assert.Equal(3, root.IntList.Count);
                Assert.Equal(1, root.IntList[0]);
                Assert.Equal(2, root.IntList[1]);
                Assert.Equal(3, root.IntList[2]);
                root.IntList.Clear();
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineList>();
                Assert.Equal(0, root.IntList.Count);
                root.IntList = null;
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineList>();
                Assert.Null(root.IntList);
            }
        }

        [Fact]
        public void InlineListsOfSimpleValuesSkip()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<InlineList>();
                root.IntList = new List<int> { 1, 2 };
                tr.Commit();
            }
            ReopenDb();
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

        [Fact]
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
                Assert.Equal(2, root.Int2String.Count);
                Assert.Equal("one", root.Int2String[1]);
                Assert.Equal(null, root.Int2String[0]);
                root.Int2String.Clear();
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<SimpleDictionary>();
                Assert.Equal(0, root.Int2String.Count);
            }
        }

        [Fact]
        public void DictionariesOfSimpleValuesSkip()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<SimpleDictionary>();
                root.Int2String = new Dictionary<int, string> { { 1, "one" }, { 0, null } };
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(Empty), "SimpleDictionary");
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<Empty>();
            }
        }

        public class SimpleOrderedDictionary
        {
            public IOrderedDictionary<int, string> Int2String { get; set; }
        }

        [Fact]
        public void OrderedDictionaryEnumeration()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<SimpleOrderedDictionary>();
                root.Int2String.Add(3, "C");
                root.Int2String.Add(1, "A");
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<SimpleOrderedDictionary>();
                Assert.Equal("AC", root.Int2String.Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("CA", root.Int2String.GetReverseEnumerator().Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("AC", root.Int2String.GetIncreasingEnumerator(0).Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("AC", root.Int2String.GetIncreasingEnumerator(1).Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("C", root.Int2String.GetIncreasingEnumerator(2).Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("C", root.Int2String.GetIncreasingEnumerator(3).Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("", root.Int2String.GetIncreasingEnumerator(4).Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("", root.Int2String.GetDecreasingEnumerator(0).Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("A", root.Int2String.GetDecreasingEnumerator(1).Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("A", root.Int2String.GetDecreasingEnumerator(2).Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("CA", root.Int2String.GetDecreasingEnumerator(3).Aggregate("", (current, p) => current + p.Value));
                Assert.Equal("CA", root.Int2String.GetDecreasingEnumerator(4).Aggregate("", (current, p) => current + p.Value));
            }
        }

        [Fact]
        public void CanQuerySizeOfKeyInOrderedDictionary()
        {
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<SimpleOrderedDictionary>();
                root.Int2String.Add(3, "C");
                var qs = (IQuerySizeDictionary<int>)root.Int2String;
                var size = qs.QuerySizeByKey(3);
                Assert.Equal(3u, size.Key);
                Assert.Equal(2u, size.Value);
                root.Int2String.Add(1, "A");
                size = qs.QuerySizeEnumerator().First();
                Assert.Equal(3u, size.Key);
                Assert.Equal(2u, size.Value);
            }
        }

        public class ComplexDictionary
        {
            public IDictionary<string, Person> String2Person { get; set; }
        }

        [Fact]
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
                Assert.Equal(2, root.String2Person.Count);
                Assert.True(root.String2Person.ContainsKey("Boris"));
                Assert.True(root.String2Person.ContainsKey("null"));
                var p = root.String2Person["Boris"];
                Assert.NotNull(p);
                Assert.Equal("Boris", p.Name);
                Assert.Equal(35u, root.String2Person["Boris"].Age);
                Assert.Equal(null, root.String2Person["null"]);
                Assert.Equal(new[] { "Boris", "null" }, root.String2Person.Keys.ToList());
                Assert.Equal(p, root.String2Person.Values.First());
                root.String2Person.Clear();
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                Assert.Equal(0, root.String2Person.Count);
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                Assert.Equal(0, root.String2Person.Count);
            }
        }

        [Fact]
        public void DictionariesOfComplexValuesSkip()
        {
            _db.RegisterType(typeof(Person));
            using (var tr = _db.StartTransaction())
            {
                var root = tr.Singleton<ComplexDictionary>();
                root.String2Person = new Dictionary<string, Person> { { "Boris", new Person { Name = "Boris", Age = 35 } }, { "null", null } };
                tr.Commit();
            }
            ReopenDb();
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

        [Fact]
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

        [Fact]
        public void EnumUpgrade()
        {
            TestEnum2TestEnumUlong(TestEnum.Item1, TestEnumUlong.Item1);
            TestEnum2TestEnumUlong(TestEnum.Item2, TestEnumUlong.Item2);
        }

        void TestEnum2TestEnumUlong(TestEnum from, TestEnumUlong to)
        {
            ReopenDb();
            var testEnumObjDbName = _db.RegisterType(typeof(CTestEnum));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new CTestEnum { E = from });
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(CTestEnumUlong), testEnumObjDbName);
            using (var tr = _db.StartTransaction())
            {
                var e = tr.Enumerate<CTestEnumUlong>().First();
                Assert.Equal(to, e.E);
                tr.Delete(e);
                tr.Commit();
            }
        }

        [Fact]
        public void EnumUpgradeAddItem()
        {
            TestEnum2TestEnumEx(TestEnum.Item1, TestEnumEx.Item1);
            TestEnum2TestEnumEx(TestEnum.Item2, TestEnumEx.Item2);
        }

        void TestEnum2TestEnumEx(TestEnum from, TestEnumEx to)
        {
            ReopenDb();
            var testEnumObjDbName = _db.RegisterType(typeof(CTestEnum));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new CTestEnum { E = from });
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(CTestEnumEx), testEnumObjDbName);
            using (var tr = _db.StartTransaction())
            {
                var e = tr.Enumerate<CTestEnumEx>().First();
                Assert.Equal(to, e.E);
                tr.Delete(e);
                tr.Commit();
            }
        }

        public class Manager : Person
        {
            public List<Person> Managing { get; set; }
        }

        [Fact]
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
                Assert.IsType<Person>(dict["slave"]);
                Assert.IsType<Manager>(dict["master"]);
                Assert.Equal(3, dict.Count);
                Assert.Equal("Chief", dict["master"].Name);
                Assert.Same(dict["slave2"], ((Manager)dict["master"]).Managing[1]);
            }
        }

        [Fact]
        public void NotStoredProperties()
        {
            var personObjDbName = _db.RegisterType(typeof(PersonWithNonStoredProperty));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new PersonWithNonStoredProperty { Name = "Bobris", Age = 35 });
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(Person), personObjDbName);
            using (var tr = _db.StartTransaction())
            {
                var p = tr.Enumerate<Person>().First();
                Assert.Equal("Bobris", p.Name);
                Assert.Equal(0u, p.Age);
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

        [Fact]
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
                Assert.Equal(1u, root.InlinePersons[0].Age);
            }
        }

        [Fact]
        public void ForbidToStoreInlinedObjectDirectly()
        {
            using (var tr = _db.StartTransaction())
            {
                Assert.Throws<BTDBException>(() => tr.Store(new InlinePerson()));
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

        [Fact]
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
                Assert.Equal("Bobris", p.Name);
                Assert.Equal(35u, p.PublicAge);
            }
        }

        [Fact]
        public void CanGetSizeOfObject()
        {
            using (var tr = _db.StartTransaction())
            {
                var oid = tr.StoreAndFlush(new Person { Name = "Bobris", Age = 35 });
                var s = tr.GetStorageSize(oid);
                Assert.Equal(2u, s.Key);
                Assert.Equal(10u, s.Value);
            }
        }

        [Fact]
        public void SingletonShouldNotNeedWritableTransactionIfNotCommited()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<ComplexDictionary>();
                Assert.False(((IInternalObjectDBTransaction)tr).KeyValueDBTransaction.IsWritting());
            }
        }

        public class IndirectTree
        {
            public IIndirect<IndirectTree> Parent { get; set; }
            public IIndirect<IndirectTree> Left { get; set; }
            public IIndirect<IndirectTree> Right { get; set; }
            public string Content { get; set; }
        }

        [Fact]
        public void IndirectLazyReferencesWorks()
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<IndirectTree>();
                Assert.NotNull(t.Parent);
                t.Left.Value = new IndirectTree();
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<IndirectTree>();
                Assert.Null(t.Parent.Value);
                Assert.NotNull(t.Left.Value);
            }
        }

        public class TwoComplexDictionary
        {
            public IDictionary<string, Person> String2Person { get; set; }
            public IDictionary<string, PersonNew> String2PersonNew { get; set; }
        }

        [Fact]
        public void UpgradingWithNewComplexDictionary()
        {
            var typeName = _db.RegisterType(typeof(ComplexDictionary));
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<ComplexDictionary>();
                d.String2Person.Add("A", new Person { Name = "A" });
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(TwoComplexDictionary), typeName);
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<TwoComplexDictionary>();
                tr.Store(d);
                Assert.Equal(1, d.String2Person.Count);
                Assert.Equal("A", d.String2Person["A"].Name);
                d.String2PersonNew.Add("B", new PersonNew { Name = "B" });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<TwoComplexDictionary>();
                Assert.Equal("A", d.String2Person["A"].Name);
                Assert.Equal("B", d.String2PersonNew["B"].Name);
            }
        }

        public class ObjectWithDictString2ListOfUInt64
        {
            public IDictionary<string, List<ulong>> Dict { get; set; }
        }

        public class Object2WithDictString2ListOfUInt64
        {
            public IDictionary<string, List<ulong>> Dict { get; set; }
            public String Added { get; set; }
        }

        [Fact]
        public void UpgradeWithDictString2ListOfUInt64Works()
        {
            var typeName = _db.RegisterType(typeof(ObjectWithDictString2ListOfUInt64));
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<ObjectWithDictString2ListOfUInt64>();
                d.Dict.Add("A", new List<ulong> { 1, 2 });
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(Object2WithDictString2ListOfUInt64), typeName);
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<Object2WithDictString2ListOfUInt64>();
                Assert.NotNull(d.Dict);
            }
        }

        public class ObjectWithDictInt2String
        {
            public IDictionary<int, string> Dict { get; set; }
        }

        public class Object2WithDictInt2String
        {
            public IDictionary<int, string> Dict { get; set; }
            public String Added { get; set; }
        }

        [Fact]
        public void UpgradeWithDictInt2StringWorks()
        {
            var typeName = _db.RegisterType(typeof(ObjectWithDictInt2String));
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<ObjectWithDictInt2String>();
                d.Dict.Add(10, "A");
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(Object2WithDictInt2String), typeName);
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<Object2WithDictInt2String>();
                Assert.NotNull(d.Dict);
            }
        }

        [Fact]
        public void PossibleToAllocateNewPreinitializedObject()
        {
            using (var tr = _db.StartTransaction())
            {
                var d = tr.New<Object2WithDictString2ListOfUInt64>();
                Assert.NotNull(d.Dict);
                Assert.Null(d.Added);
                d.Added = "Stored";
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Enumerate<Object2WithDictString2ListOfUInt64>().First();
                Assert.NotNull(d.Dict);
                Assert.Equal("Stored", d.Added);
            }
        }

        [Fact]
        public void BiggerTransactionWithGC()
        {
            using (var tr = _db.StartTransaction())
            {
                for (uint i = 0; i < 100; i++)
                {
                    tr.StoreAndFlush(new Person { Name = "Bobris", Age = i });
                    if (i == 90)
                    {
                        GC.Collect();
                    }
                }
                tr.Commit();
            }
        }

        public class ObjectWithDictWithInlineKey
        {
            public IDictionary<InlinePerson, int> Dict { get; set; }
        }

        [Fact]
        public void DictionaryContainsMustWorkInReadOnlyTransaction()
        {
            using (var tr = _db.StartReadOnlyTransaction())
            {
                tr.Singleton<ObjectWithDictWithInlineKey>().Dict.ContainsKey(new InlinePerson());
            }
        }

        [StoredInline]
        public class InlinePersonNew : PersonNew
        {
        }

        public class ObjectWithDictWithInlineKeyNew
        {
            public IDictionary<InlinePersonNew, int> Dict { get; set; }
        }

        [Fact(Skip="This is very difficult to do")]
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
            ReopenDb();
            _db.RegisterType(typeof(ObjectWithDictWithInlineKeyNew), singName);
            _db.RegisterType(typeof(InlinePersonNew), persName);
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<ObjectWithDictWithInlineKeyNew>();
                var p = d.Dict.Keys.First();
                Assert.Equal("A", p.Name);
                Assert.Equal(1, d.Dict[p]);
            }
        }

        public class ObjectWithDictWithDateTimeKey
        {
            public IDictionary<DateTime, DateTime> Dist { get; set; }
        }

        [Fact]
        public void ForbidToStoreDateTimeUnknownKindInKey()
        {
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<ObjectWithDictWithDateTimeKey>();
                var unknownKind = new DateTime(2013, 1, 25, 22, 05, 00);
                var utcKind = unknownKind.ToUniversalTime();
                Assert.Throws<ArgumentOutOfRangeException>(() => d.Dist.Add(unknownKind, utcKind));
                d.Dist.Add(utcKind, unknownKind);
            }
        }

        [Fact]
        public void PitOfSuccessForbidStoreDictionary()
        {
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<ObjectWithDictWithDateTimeKey>();
                Assert.Throws<InvalidOperationException>(() => tr.Store(d.Dist));
            }
        }

        public enum CC1V1T3
        {
            Item1 = 0
        }

        public class CC1V1T2
        {
            public IDictionary<CC1V1T3, long> Dict2 { get; set; }
        }

        public class CC1V1T1
        {
            public IDictionary<ulong, CC1V1T2> Dict { get; set; }
        }

        public enum CC1V2T3
        {
            Item1 = 0,
            Item2 = 1,
            Item3 = 2
        }

        public class CC1V2T2
        {
            public IDictionary<CC1V2T3, long> Dict2 { get; set; }
        }

        public class CC1V2T1
        {
            public IDictionary<ulong, CC1V2T2> Dict { get; set; }
        }

        [Fact]
        public void CC1Upgrade()
        {
            var t1Name = _db.RegisterType(typeof(CC1V1T1));
            var t2Name = _db.RegisterType(typeof(CC1V1T2));
            var t3Name = _db.RegisterType(typeof(CC1V1T3));
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<CC1V1T1>();
                var d2 = tr.New<CC1V1T2>();
                d2.Dict2[CC1V1T3.Item1] = 10;
                d.Dict[1] = d2;
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(CC1V2T1), t1Name);
            _db.RegisterType(typeof(CC1V2T2), t2Name);
            _db.RegisterType(typeof(CC1V2T3), t3Name);
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<CC1V2T1>();
                var d2 = d.Dict[1];
                Assert.Equal(10, d2.Dict2[CC1V2T3.Item1]);
            }
        }

        public class IndirectValueDict
        {
            public IDictionary<int, IIndirect<Person>> Dict { get; set; }
        }

        [Fact]
        public void BasicIndirectInDict()
        {
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<IndirectValueDict>();
                d.Dict[1] = new DBIndirect<Person>(new Person { Name = "A", Age = 10 });
                d.Dict[2] = new DBIndirect<Person>(new Person { Name = "B", Age = 20 });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                Assert.Equal(2, tr.Enumerate<Person>().Count());
                var d = tr.Singleton<IndirectValueDict>();
                Assert.Equal(10u, d.Dict[1].Value.Age);
                Assert.Equal("B", d.Dict[2].Value.Name);
                tr.Delete(d.Dict[2].Value);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                Assert.Equal(1, tr.Enumerate<Person>().Count());
                var d = tr.Singleton<IndirectValueDict>();
                Assert.Equal(10u, d.Dict[1].Value.Age);
                Assert.Null(d.Dict[2].Value);
            }
        }

        [Fact]
        public void DeleteIndirectWithoutMaterialization()
        {
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<IndirectValueDict>();
                d.Dict[1] = new DBIndirect<Person>(new Person { Name = "A", Age = 10 });
                d.Dict[2] = new DBIndirect<Person>(new Person { Name = "B", Age = 20 });
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<IndirectValueDict>();
                tr.Delete(d.Dict[2]);
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                Assert.Equal(1, tr.Enumerate<Person>().Count());
                var d = tr.Singleton<IndirectValueDict>();
                Assert.Equal(10u, d.Dict[1].Value.Age);
                Assert.Null(d.Dict[2].Value);
            }
        }

        [Theory]
        [InlineData(2, false, 4, false, "1245", 1)]
        [InlineData(2, true, 4, false, "145", 2)]
        [InlineData(2, false, 4, true, "125", 2)]
        [InlineData(2, true, 4, true, "15", 3)]
        [InlineData(3, false, 3, false, "12345", 0)]
        [InlineData(6, true, 5, true, "12345", 0)]
        [InlineData(0, true, 6, true, "", 5)]
        [InlineData(0, false, 6, false, "", 5)]
        public void RemoveRange(int start, bool includeStart, int end, bool includeEnd, string result, int removedCount)
        {
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<SimpleOrderedDictionary>().Int2String;
                for (var i = 1; i <= 5; i++)
                {
                    d[i] = i.ToString(CultureInfo.InvariantCulture);
                }
                Assert.Equal(removedCount, d.RemoveRange(start, includeStart, end, includeEnd));
                Assert.Equal(result, d.Aggregate("", (s, p) => s + p.Value));
            }
        }

        [Fact]
        public void PossibleToEnumerateSingletons()
        {
            using (var tr = _db.StartTransaction())
            {
                tr.Singleton<Root>();
                tr.Singleton<Tree>();
                CheckSingletonTypes(tr.EnumerateSingletonTypes());
                tr.Commit();
            }
            using (var tr = _db.StartTransaction())
            {
                CheckSingletonTypes(tr.EnumerateSingletonTypes());
            }
            ReopenDb();
            _db.RegisterType(typeof(Root));
            _db.RegisterType(typeof(Tree));
            using (var tr = _db.StartTransaction())
            {
                CheckSingletonTypes(tr.EnumerateSingletonTypes());
            }
        }

        void CheckSingletonTypes(IEnumerable<Type> types)
        {
            var a = types.ToArray();
            Assert.Equal(2, a.Length);
            if (a[0] == typeof(Root))
            {
                Assert.Equal(typeof(Root), a[0]);
                Assert.Equal(typeof(Tree), a[1]);
            }
            else
            {
                Assert.Equal(typeof(Tree), a[0]);
                Assert.Equal(typeof(Root), a[1]);
            }
        }

        [StoredInline]
        public class Rule1
        {
            public string Name { get; set; }
        }

        [StoredInline]
        public class Rule2
        {
            public string Name { get; set; }
            public int Type { get; set; }
        }
        public class ObjectWfd1
        {
            public Rule1 A { get; set; }
            public Rule1 B { get; set; }
            public Rule1 C { get; set; }
        }

        public class ObjectWfd2
        {
            public Rule2 A { get; set; }
            //public Rule2 B { get; set; }
            public Rule2 C { get; set; }
        }

        [Fact]
        public void UpgradeDeletedInlineObjectWorks()
        {
            var typeNameWfd = _db.RegisterType(typeof(ObjectWfd1));
            var typeNameRule = _db.RegisterType(typeof(Rule1));

            using (var tr = _db.StartTransaction())
            {
                var wfd = tr.Singleton<ObjectWfd1>();
                wfd.A = new Rule1 { Name = "A" };
                wfd.B = new Rule1 { Name = "B" };
                wfd.C = new Rule1 { Name = "C" };
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(ObjectWfd2), typeNameWfd);
            _db.RegisterType(typeof(Rule2), typeNameRule);

            using (var tr = _db.StartTransaction())
            {
                var wfd = tr.Singleton<ObjectWfd2>();
                Assert.Equal("C", wfd.C.Name);
            }
        }

        public enum MapEnum
        {
            A
        };

        public enum MapEnumEx
        {
            A,
            B
        }

        public class ComplexMap
        {
            public IOrderedDictionary<ulong, IDictionary<MapEnum, int>> Items { get; set; }
        }

        public class ComplexMapEx
        {
            public IOrderedDictionary<ulong, IDictionary<MapEnumEx, int>> Items { get; set; }
        }

        [Fact]
        public void UpgradeNestedMapWithEnumWorks()
        {
            var typeComplexMap = _db.RegisterType(typeof(ComplexMap));
            var typeEnum = _db.RegisterType(typeof(MapEnum));

            using (var tr = _db.StartTransaction())
            {
                var wfd = tr.Singleton<ComplexMap>();
                wfd.Items[0] = new Dictionary<MapEnum, int> { { MapEnum.A, 11 } };
                tr.Commit();
            }

            ReopenDb();
            _db.RegisterType(typeof(ComplexMapEx), typeComplexMap);
            _db.RegisterType(typeof(MapEnumEx), typeEnum);

            using (var tr = _db.StartTransaction())
            {
                var wfd = tr.Singleton<ComplexMapEx>();
                Assert.Equal(11, wfd.Items[0][MapEnumEx.A]);
            }
        }

        public class SimpleWithIndexer
        {
            public string OddName { get; set; }
            public string EvenName { get; set; }

            public string this[int i] => i % 2 == 0 ? EvenName : OddName;
        }

        [Fact]
        public void CanStoreObjectWithIndexer()
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<SimpleWithIndexer>();
                t.OddName = "oddy";
                t.EvenName = "evvy";
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var t = tr.Singleton<SimpleWithIndexer>();
                Assert.Equal("oddy", t.OddName);
                Assert.Equal("evvy", t[12]);
            }
        }

        [Fact]
        public void LoopsThroughAllItemsEvenIfDeleteIsPerformed()
        {
            using (var tr = _db.StartTransaction())
            {
                var sd = tr.Singleton<SimpleDictionary>().Int2String;
                sd[0] = "a";
                sd[1] = "b";
                sd[2] = "c";

                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var sd = tr.Singleton<SimpleDictionary>().Int2String;
                Assert.Equal(sd.Count, 3);

                Assert.Throws<InvalidOperationException>(() =>
                {
                    foreach (var kvp in sd)
                    {
                        sd.Remove(kvp.Key);
                    }
                });
            }
        }

        public class DictWithComplexCompoundKey
        {
            public IOrderedDictionary<LogId, string> Items { get; set; }
        }

        [StoredInline]
        public class LogId
        {
            public string Key { get; set; }
            public DateTime DateTime { get; set; }
            public ulong CollisionId { get; set; }
        }

        [Fact]
        public void CanRemoveRangeOfCompoundKeys()
        {
            using (var tr = _db.StartTransaction())
            {
                var sd = tr.Singleton<DictWithComplexCompoundKey>().Items;
                sd[new LogId() { Key = "key", DateTime = DateTime.UtcNow, CollisionId = 0 }] = "a";
                sd[new LogId() { Key = "key", DateTime = DateTime.UtcNow.AddSeconds(1), CollisionId = 0 }] = "b";
                sd[new LogId() { Key = "key", DateTime = DateTime.UtcNow.AddSeconds(2), CollisionId = 0 }] = "c";

                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var sd = tr.Singleton<DictWithComplexCompoundKey>().Items;
                var deleted = sd.RemoveRange(new LogId { Key = "key", DateTime = DateTime.MinValue.ToUniversalTime(), CollisionId = ushort.MinValue },
                    true, new LogId { Key = "key", DateTime = DateTime.MaxValue.ToUniversalTime(), CollisionId = ushort.MaxValue }, true);

                Assert.Equal(3, deleted);
                Assert.Equal(0, sd.Count);
            }
        }

        public enum TestRenamedEnum
        {
            [PersistedName("Item1")]
            ItemA,
            [PersistedName("Item2")]
            ItemB
        }

        public class CTestRenamedEnum
        {
            [PersistedName("E")]
            public TestRenamedEnum EE { get; set; }
        }

        [Fact]
        public void UnderstandPersistedNameInEnum()
        {
            TestEnum2TestRenamedEnum(TestEnum.Item1, TestRenamedEnum.ItemA);
            TestEnum2TestRenamedEnum(TestEnum.Item2, TestRenamedEnum.ItemB);
        }

        void TestEnum2TestRenamedEnum(TestEnum from, TestRenamedEnum to)
        {
            ReopenDb();
            var testEnumObjDbName = _db.RegisterType(typeof(CTestEnum));
            using (var tr = _db.StartTransaction())
            {
                tr.Store(new CTestEnum { E = from });
                tr.Commit();
            }
            ReopenDb();
            _db.RegisterType(typeof(CTestRenamedEnum), testEnumObjDbName);
            using (var tr = _db.StartTransaction())
            {
                var e = tr.Enumerate<CTestRenamedEnum>().First();
                Assert.Equal(to, e.EE);
                tr.Delete(e);
                tr.Commit();
            }
        }

        [StoredInline]
        public class UlongGuidKey
        {
            public ulong Ulong { get; set; }
            public Guid Guid { get; set; }
        }

        public class UlongGuidMap
        {
            public IOrderedDictionary<UlongGuidKey, string> Items { get; set; }
        }

        public class GuidMap
        {
            public IOrderedDictionary<Guid, string> Items { get; set; }
        }

        [Fact]
        public void GuidInKey()
        {
            var guid = Guid.NewGuid();
            using (var tr = _db.StartTransaction())
            {
                var items = tr.Singleton<GuidMap>().Items;
                items[guid] = "a";
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var items = tr.Singleton<GuidMap>().Items;
                string value;
                Assert.Equal(true, items.TryGetValue(guid, out value));

                Assert.Equal("a", value);
            }
        }

        [Fact]
        public void GuidInComplexKey()
        {
            var guid = Guid.NewGuid();
            using (var tr = _db.StartTransaction())
            {
                var items = tr.Singleton<UlongGuidMap>().Items;
                items[new UlongGuidKey { Ulong = 1, Guid = guid }] = "a";
                tr.Commit();
            }
            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var items = tr.Singleton<UlongGuidMap>().Items;
                string value;
                Assert.Equal(true, items.TryGetValue(new UlongGuidKey {Ulong = 1, Guid = guid}, out value));

                Assert.Equal("a", value);
            }
        }

        public class TimeIndex
        {
            public IOrderedDictionary<TimeIndexKey, ulong> Items { get; set; }

            [StoredInline]
            public class TimeIndexKey
            {
                public DateTime Time { get; set; }
            }
        }


        [Fact(Skip="Very difficult without breaking backward compatibility of database. And what is worse problem string inside object is not ordered correctly!")]
        public void CannotStoreDateTimeKindUnspecified()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                using (var tr = _db.StartTransaction())
                {
                    var t = tr.Singleton<TimeIndex>().Items;
                    var unspecifiedKindDate = new DateTime(2015, 1, 1, 1, 1, 1, DateTimeKind.Unspecified);
                    t[new TimeIndex.TimeIndexKey { Time = unspecifiedKindDate }] = 15;
                    tr.Commit();
                }
            });
        }

        [Test]
        [TestCase(-1, false, -1, false, "13579")]
        [TestCase(2, true, -1, false, "3579")]
        [TestCase(2, false, -1, false, "3579")]
        [TestCase(3, true, -1, false, "3579")]
        [TestCase(3, false, -1, false, "579")]
        [TestCase(-1, false, 8, true, "1357")]
        [TestCase(-1, false, 8, false, "1357")]
        [TestCase(-1, false, 7, true, "1357")]
        [TestCase(-1, false, 7, false, "135")]
        [TestCase(3, true, 7, true, "357")]
        [TestCase(3, true, 7, false, "35")]
        [TestCase(3, false, 7, true, "57")]
        [TestCase(3, false, 7, false, "5")]
        [TestCase(0, true, 10, true, "13579")]
        [TestCase(0, true, 10, false, "13579")]
        [TestCase(0, false, 10, true, "13579")]
        [TestCase(0, false, 10, false, "13579")]
        [TestCase(10, false, 0, false, "")]
        [TestCase(5, false, 5, false, "")]
        [TestCase(5, false, 5, true, "")]
        [TestCase(5, true, 5, false, "")]
        [TestCase(5, true, 5, true, "5")]
        public void AdvancedIterationBasics(int start, bool includeStart, int end, bool includeEnd, string result)
        {
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<SimpleOrderedDictionary>().Int2String;
                // 1 3 5 7 9
                for (var i = 1; i <= 9; i += 2)
                {
                    d[i] = i.ToString(CultureInfo.InvariantCulture);
                }
                var param = new AdvancedEnumeratorParam<int>(EnumerationOrder.Ascending, start,
                    start == -1
                        ? KeyProposition.Ignored
                        : includeStart ? KeyProposition.Included : KeyProposition.Excluded,
                    end,
                    end == -1 ? KeyProposition.Ignored : includeEnd ? KeyProposition.Included : KeyProposition.Excluded);
                var e = d.GetAdvancedEnumerator(param);
                var res = "";
                int key;
                Assert.AreEqual(result.Length, e.Count);
                while (e.NextKey(out key))
                {
                    Assert.AreEqual(res.Length, e.Position);
                    var val = e.CurrentValue;
                    Assert.AreEqual(key.ToString(CultureInfo.InvariantCulture), val);
                    res += val;
                }
                Assert.AreEqual(result, res);
                Assert.AreEqual(res.Length, e.Position);
                param = new AdvancedEnumeratorParam<int>(EnumerationOrder.Descending, start,
                    start == -1
                        ? KeyProposition.Ignored
                        : includeStart ? KeyProposition.Included : KeyProposition.Excluded,
                    end,
                    end == -1 ? KeyProposition.Ignored : includeEnd ? KeyProposition.Included : KeyProposition.Excluded);
                e = d.GetAdvancedEnumerator(param);
                res = "";
                Assert.AreEqual(result.Length, e.Count);
                while (e.NextKey(out key))
                {
                    Assert.AreEqual(res.Length, e.Position);
                    var val = e.CurrentValue;
                    Assert.AreEqual(key.ToString(CultureInfo.InvariantCulture), val);
                    res = val + res;
                }
                Assert.AreEqual(result, res);
                Assert.AreEqual(res.Length, e.Position);
            }
        }

        [Test]
        [TestCase(-1, false, -1, false, "13579")]
        [TestCase(2, true, -1, false, "3579")]
        [TestCase(2, false, -1, false, "3579")]
        [TestCase(3, true, -1, false, "3579")]
        [TestCase(3, false, -1, false, "579")]
        [TestCase(-1, false, 8, true, "1357")]
        [TestCase(-1, false, 8, false, "1357")]
        [TestCase(-1, false, 7, true, "1357")]
        [TestCase(-1, false, 7, false, "135")]
        [TestCase(3, true, 7, true, "357")]
        [TestCase(3, true, 7, false, "35")]
        [TestCase(3, false, 7, true, "57")]
        [TestCase(3, false, 7, false, "5")]
        [TestCase(0, true, 10, true, "13579")]
        [TestCase(0, true, 10, false, "13579")]
        [TestCase(0, false, 10, true, "13579")]
        [TestCase(0, false, 10, false, "13579")]
        [TestCase(10, false, 0, false, "")]
        [TestCase(5, false, 5, false, "")]
        [TestCase(5, false, 5, true, "")]
        [TestCase(5, true, 5, false, "")]
        [TestCase(5, true, 5, true, "5")]
        public void AdvancedIterationSeeks(int start, bool includeStart, int end, bool includeEnd, string result)
        {
            using (var tr = _db.StartTransaction())
            {
                var d = tr.Singleton<SimpleOrderedDictionary>().Int2String;
                // 1 3 5 7 9
                for (var i = 1; i <= 9; i += 2)
                {
                    d[i] = i.ToString(CultureInfo.InvariantCulture);
                }
                var param = new AdvancedEnumeratorParam<int>(EnumerationOrder.Ascending, start,
                    start == -1
                        ? KeyProposition.Ignored
                        : includeStart ? KeyProposition.Included : KeyProposition.Excluded,
                    end,
                    end == -1 ? KeyProposition.Ignored : includeEnd ? KeyProposition.Included : KeyProposition.Excluded);
                var e = d.GetAdvancedEnumerator(param);
                var res = "";
                int key;
                e.Position = 2;
                while (e.NextKey(out key))
                {
                    Assert.AreEqual(res.Length, e.Position - 2);
                    var val = e.CurrentValue;
                    Assert.AreEqual(key.ToString(CultureInfo.InvariantCulture), val);
                    res += val;
                }
                Assert.AreEqual(result.Substring(Math.Min(result.Length, 2)), res);
                param = new AdvancedEnumeratorParam<int>(EnumerationOrder.Descending, start,
                    start == -1
                        ? KeyProposition.Ignored
                        : includeStart ? KeyProposition.Included : KeyProposition.Excluded,
                    end,
                    end == -1 ? KeyProposition.Ignored : includeEnd ? KeyProposition.Included : KeyProposition.Excluded);
                e = d.GetAdvancedEnumerator(param);
                res = "";
                e.Position = 2;
                while (e.NextKey(out key))
                {
                    Assert.AreEqual(res.Length, e.Position - 2);
                    var val = e.CurrentValue;
                    Assert.AreEqual(key.ToString(CultureInfo.InvariantCulture), val);
                    res = val + res;
                }
                Assert.AreEqual(result.Substring(0, Math.Max(0, result.Length - 2)), res);
            }
        }

    }
}
