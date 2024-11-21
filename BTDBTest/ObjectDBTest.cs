using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using Assent;
using BTDB.Allocators;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDbTest : IDisposable, IFieldHandlerLogger
{
    IKeyValueDB _lowDb;
    IObjectDB _db;
    LeakDetectorWrapperAllocator _allocator;

    public class Person : IEquatable<Person>
    {
        public string Name { get; set; }
        public uint Age { get; set; }

        public bool Equals(Person other)
        {
            return Name == other.Name && Age == other.Age;
        }

        public override bool Equals(object other) => Equals(other as Person);

        public override int GetHashCode()
        {
            var hashCode = -1360180430;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(Name);
            hashCode = hashCode * -1521134295 + Age.GetHashCode();
            return hashCode;
        }
    }

    public class PersonWithNonStoredProperty
    {
        public string Name { get; set; }
        [NotStored] public uint Age { get; set; }
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
        _allocator = new(new MallocAllocator());
        _lowDb = new BTreeKeyValueDB(new KeyValueDBOptions
        {
            Allocator = _allocator,
            CompactorScheduler = null,
            Compression = new NoCompressionStrategy(),
            FileCollection = new InMemoryFileCollection()
        });
        OpenDb();
    }

    public void Dispose()
    {
        _db.Dispose();
        _lowDb.Dispose();
        _allocator.Dispose();
    }

    void ReopenDb(bool resetMetadataCache = false)
    {
        _db.Dispose();
        if (resetMetadataCache)
            ObjectDB.ResetAllMetadataCaches();
        OpenDb();
    }

    void OpenDb()
    {
        ReportedTypeIncompatibilities.Clear();
        _db = new ObjectDB();
        _db.Open(_lowDb, false,
            new DBOptions().WithSymmetricCipher(new AesGcmSymmetricCipher(new byte[]
            {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                27, 28, 29, 30, 31
            })).WithFieldHandlerLogger(this));
    }

    [Fact]
    public void NewDatabaseIsEmpty()
    {
        using (var tr = _db.StartTransaction())
        {
            Assert.Empty(tr.Enumerate<Person>());
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
    public void SupportsCommitUlong()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.SetCommitUlong(1234567);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(1234567ul, tr.GetCommitUlong());
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            Assert.Equal(1234567ul, tr.GetCommitUlong());
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
            Assert.Null(p.Comment);
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
            Assert.Single(tr.Enumerate<Person>());
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
        public IIndirect<object> DbObjectField { get; set; }
        public IIndirect<VariousFieldTypes> VariousFieldTypesField { get; set; }
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
        public Version VersionField { get; set; }
        public EncryptedString EncryptedStringField { get; set; }
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
            Assert.Equal(0u, o.DbObjectField.Oid);
            Assert.Equal(0u, o.VariousFieldTypesField.Oid);
            Assert.False(o.BoolField);
            Assert.Equal(0d, o.DoubleField);
            Assert.Equal(0f, o.FloatField);
            Assert.Equal(0m, o.DecimalField);
            Assert.Equal(new DateTime(), o.DateTimeField);
            Assert.Equal(new TimeSpan(), o.TimeSpanField);
            Assert.Equal(new Guid(), o.GuidField);
            Assert.Equal(TestEnum.Item1, o.EnumField);
            Assert.Null(o.ByteArrayField);
            Assert.Equal(ByteBuffer.NewEmpty().ToByteArray(), o.ByteBufferField.ToByteArray());
            Assert.Null(o.VersionField);
            Assert.Equal(new EncryptedString(), o.EncryptedStringField);

            o.StringField = "Text";
            o.SByteField = -10;
            o.ByteField = 10;
            o.ShortField = -1000;
            o.UShortField = 1000;
            o.IntField = -100000;
            o.UIntField = 100000;
            o.LongField = -1000000000000;
            o.ULongField = 1000000000000;
            o.DbObjectField = new DBIndirect<object>(o);
            o.VariousFieldTypesField = new DBIndirect<VariousFieldTypes>(o);
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
            o.VersionField = new Version(1, 2, 3, 4);
            o.EncryptedStringField = "pAsSwOrD";

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
        Assert.Same(o, o.DbObjectField.Value);
        Assert.Same(o, o.VariousFieldTypesField.Value);
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
        Assert.Equal(new Version(1, 2, 3, 4), o.VersionField);
        Assert.Equal("pAsSwOrD", o.EncryptedStringField);
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
        public IList<ByteBuffer> ByteBufferList { get; set; }
        public IList<int?> NullableIntList { get; set; }
        public ISet<int> IntSet { get; set; }
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
            root.ByteBufferList = new List<ByteBuffer> { ByteBuffer.NewAsync(new byte[] { 1, 2 }) };
            root.NullableIntList = new List<int?> { 1, 2 };
            root.IntSet = new HashSet<int> { 1, 2 };
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<VariousLists>();
            Assert.Equal(new List<int> { 5, 10, 2000 }, root.IntList);
            Assert.Equal(new List<string> { "A", null, "AB!" }, root.StringList);
            Assert.Equal(new List<byte> { 0, 255 }, root.ByteList);
            Assert.Equal(1, root.ByteBufferList.Count);
            Assert.Equal(new byte[] { 1, 2 }, root.ByteBufferList[0].ToByteArray());
            Assert.Equal(new List<int?> { 1, 2 }, root.NullableIntList);
            Assert.Equal(new HashSet<int> { 1, 2 }, root.IntSet);
            root.IntList = null;
            root.StringList = null;
            root.ByteList = null;
            tr.Store(root);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<VariousLists>();
            Assert.Null(root.IntList);
            Assert.Null(root.StringList);
            Assert.Null(root.ByteList);
            root.IntList = new List<int>();
            root.StringList = new List<string>();
            root.ByteList = new List<byte>();
            root.ByteBufferList = new List<ByteBuffer>();
            root.NullableIntList = new List<int?>();
            root.IntSet = new HashSet<int>();
            tr.Store(root);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<VariousLists>();
            Assert.Equal(new List<int>(), root.IntList);
            Assert.Equal(new List<string>(), root.StringList);
            Assert.Equal(new List<byte>(), root.ByteList);
            Assert.Equal(new List<ByteBuffer>(), root.ByteBufferList);
            Assert.Equal(new List<int?>(), root.NullableIntList);
            Assert.Equal(new HashSet<int>(), root.IntSet);
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
            root.ByteBufferList = new List<ByteBuffer> { ByteBuffer.NewAsync(new byte[] { 1, 2 }) };
            root.NullableIntList = new List<int?> { 1, 2 };
            root.IntSet = new HashSet<int> { 1, 2 };
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
        public Dictionary<int?, bool?> NullableInt2Bool { get; set; }
    }

    [Fact]
    public void InlineDictionariesOfSimpleValues()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<InlineDictionary>();
            root.Int2String = new Dictionary<int, string> { { 1, "one" }, { 0, null } };
            root.NullableInt2Bool = new Dictionary<int?, bool?> { { 1, true }, { 2, new bool?() } };
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<InlineDictionary>();
            Assert.Equal(2, root.Int2String.Count);
            Assert.Equal("one", root.Int2String[1]);
            Assert.Null(root.Int2String[0]);
            Assert.Equal(2, root.Int2String.Count);
            Assert.True(root.NullableInt2Bool[1]);
            Assert.False(root.NullableInt2Bool[2].HasValue);
            root.Int2String.Clear();
            tr.Store(root);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<InlineDictionary>();
            Assert.Empty(root.Int2String);
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
            root.NullableInt2Bool = new Dictionary<int?, bool?> { { 1, true }, { 2, new bool?() } };
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
        public List<int?> NullableIntList { get; set; }
    }

    [Fact]
    public void InlineListsOfSimpleValues()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<InlineList>();
            root.IntList = new List<int> { 1, 2, 3 };
            root.NullableIntList = new List<int?> { 4, new int?() };
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<InlineList>();
            Assert.Equal(3, root.IntList.Count);
            Assert.Equal(1, root.IntList[0]);
            Assert.Equal(2, root.IntList[1]);
            Assert.Equal(3, root.IntList[2]);
            Assert.Equal(2, root.NullableIntList.Count);
            Assert.Equal(4, root.NullableIntList[0]);
            Assert.False(root.NullableIntList[1].HasValue);
            root.IntList.Clear();
            tr.Store(root);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<InlineList>();
            Assert.Empty(root.IntList);
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
        public IDictionary<int?, bool?> NullableInt2Bool { get; set; }
    }

    [Fact]
    public void DictionariesOfSimpleValues()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SimpleDictionary>();
            root.Int2String = new Dictionary<int, string> { { 1, "one" }, { 0, null } };
            root.NullableInt2Bool = new Dictionary<int?, bool?> { { 1, true }, { 2, new bool?() } };
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SimpleDictionary>();
            Assert.Equal(2, root.Int2String.Count);
            Assert.Equal("one", root.Int2String[1]);
            Assert.Null(root.Int2String[0]);
            root.Int2String.Clear();
            Assert.Equal(2, root.NullableInt2Bool.Count);
            Assert.True(root.NullableInt2Bool[1]);
            Assert.False(root.NullableInt2Bool[2].HasValue);
            root.NullableInt2Bool.Clear();
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SimpleDictionary>();
            Assert.Equal(0, root.Int2String.Count);
            Assert.Equal(0, root.NullableInt2Bool.Count);
        }
    }

    [Fact]
    public void DictionariesOfSimpleValuesSkip()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SimpleDictionary>();
            root.Int2String = new Dictionary<int, string> { { 1, "one" }, { 0, null } };
            root.NullableInt2Bool = new Dictionary<int?, bool?> { { 1, true }, { 2, new bool?() } };
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
            Assert.Equal("CA",
                root.Int2String.GetReverseEnumerator().Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("AC",
                root.Int2String.GetIncreasingEnumerator(0).Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("AC",
                root.Int2String.GetIncreasingEnumerator(1).Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("C",
                root.Int2String.GetIncreasingEnumerator(2).Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("C",
                root.Int2String.GetIncreasingEnumerator(3).Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("",
                root.Int2String.GetIncreasingEnumerator(4).Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("",
                root.Int2String.GetDecreasingEnumerator(0).Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("A",
                root.Int2String.GetDecreasingEnumerator(1).Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("A",
                root.Int2String.GetDecreasingEnumerator(2).Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("CA",
                root.Int2String.GetDecreasingEnumerator(3).Aggregate("", (current, p) => current + p.Value));
            Assert.Equal("CA",
                root.Int2String.GetDecreasingEnumerator(4).Aggregate("", (current, p) => current + p.Value));
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

    public class SimpleOrderedSet
    {
        public IOrderedSet<int> IntSet { get; set; }
    }

    [Fact]
    public void OrderedSetEnumeration()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SimpleOrderedSet>();
            root.IntSet.Add(3);
            root.IntSet.Add(1);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SimpleOrderedSet>();
            Assert.Equal("13", root.IntSet.Aggregate("", (current, p) => current + p));
            Assert.Equal("31",
                root.IntSet.GetReverseEnumerator().Aggregate("", (current, p) => current + p));
            Assert.Equal("13",
                root.IntSet.GetIncreasingEnumerator(0).Aggregate("", (current, p) => current + p));
            Assert.Equal("13",
                root.IntSet.GetIncreasingEnumerator(1).Aggregate("", (current, p) => current + p));
            Assert.Equal("3",
                root.IntSet.GetIncreasingEnumerator(2).Aggregate("", (current, p) => current + p));
            Assert.Equal("3",
                root.IntSet.GetIncreasingEnumerator(3).Aggregate("", (current, p) => current + p));
            Assert.Equal("",
                root.IntSet.GetIncreasingEnumerator(4).Aggregate("", (current, p) => current + p));
            Assert.Equal("",
                root.IntSet.GetDecreasingEnumerator(0).Aggregate("", (current, p) => current + p));
            Assert.Equal("1",
                root.IntSet.GetDecreasingEnumerator(1).Aggregate("", (current, p) => current + p));
            Assert.Equal("1",
                root.IntSet.GetDecreasingEnumerator(2).Aggregate("", (current, p) => current + p));
            Assert.Equal("31",
                root.IntSet.GetDecreasingEnumerator(3).Aggregate("", (current, p) => current + p));
            Assert.Equal("31",
                root.IntSet.GetDecreasingEnumerator(4).Aggregate("", (current, p) => current + p));
        }
    }

    [Fact]
    public void CanQuerySizeOfKeyInOrderedSet()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<SimpleOrderedSet>();
            root.IntSet.Add(3);
            var qs = (IQuerySizeDictionary<int>)root.IntSet;
            var size = qs.QuerySizeByKey(3);
            Assert.Equal(3u, size.Key);
            Assert.Equal(0u, size.Value);
            root.IntSet.Add(1);
            size = qs.QuerySizeEnumerator().First();
            Assert.Equal(3u, size.Key);
            Assert.Equal(0u, size.Value);
        }
    }

    public class ComplexDictionary
    {
        public IDictionary<string, Person> String2Person { get; set; }
        public string String { get; set; }
    }

    [Fact]
    public void DictionariesOfComplexValues()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<ComplexDictionary>();
            Assert.NotNull(root.String2Person);
            root.String2Person = new Dictionary<string, Person>
                { { "Boris", new Person { Name = "Boris", Age = 35 } }, { "null", null } };
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
            Assert.Null(root.String2Person["null"]);
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
            root.String2Person = new Dictionary<string, Person>
                { { "Boris", new Person { Name = "Boris", Age = 35 } }, { "null", null } };
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
            AssertEqual(new KeyValuePair<byte[], byte[]>(new byte[] { 1, 0xFF }, new byte[] { 1 }),
                root.Bytes2Bytes.Skip(1).First());
            AssertEqual(new KeyValuePair<byte[], byte[]>(new byte[] { 2 }, new byte[] { 2, 2 }),
                root.Bytes2Bytes.Skip(2).First());
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

    public class CTestNullableEnum
    {
        public TestEnum? E { get; set; }
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
    public void EnumUpgradeToNullableEnum()
    {
        TestEnum2TestNullableEnum(TestEnum.Item1);
        TestEnum2TestNullableEnum(TestEnum.Item2);
    }

    void TestEnum2TestNullableEnum(TestEnum from)
    {
        ReopenDb();
        var testEnumObjDbName = _db.RegisterType(typeof(CTestEnum));
        using (var tr = _db.StartTransaction())
        {
            tr.Store(new CTestEnum { E = from });
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(CTestNullableEnum), testEnumObjDbName);
        using (var tr = _db.StartTransaction())
        {
            var e = tr.Enumerate<CTestNullableEnum>().First();
            Assert.Equal(from, e.E!.Value);
            tr.Delete(e);
            tr.Commit();
        }

        Assert.Empty(ReportedTypeIncompatibilities);
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
            root.String2Person.Add("master",
                new Manager { Name = "Chief", Age = 19, Managing = new List<Person> { sl1, sl2 } });
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
            Assert.Equal(dict["slave2"].Name, ((Manager)dict["master"]).Managing[1].Name);
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

    public class ListOfInlinePersons
    {
        public List<Person> InlinePersons { get; set; }
    }

    [Fact]
    public void SupportOfInlineObjects()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<ListOfInlinePersons>();
            root.InlinePersons = new List<Person> { new Person { Name = "Me" } };
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
            Assert.False(((IInternalObjectDBTransaction)tr).KeyValueDBTransaction.IsWriting());
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

    [Fact]
    public void ObjIdsAreNotReused()
    {
        using (var tr = _db.StartTransaction())
        {
            var t = tr.Singleton<IndirectTree>();
            t.Left.Value = new IndirectTree();
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var t = tr.Singleton<IndirectTree>();
            tr.Delete(t.Left.Value);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var t = tr.Singleton<IndirectTree>();
            t.Right.Value = new IndirectTree();
            tr.Store(t);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var t = tr.Singleton<IndirectTree>();
            Assert.NotNull(t.Right.Value);
            Assert.Null(t.Left.Value);
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
            tr.Store(d);
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
        public IDictionary<Person, int> Dict { get; set; }
    }

    [Fact]
    public void DictionaryContainsMustWorkInReadOnlyTransaction()
    {
        using (var tr = _db.StartReadOnlyTransaction())
        {
            tr.Singleton<ObjectWithDictWithInlineKey>().Dict.ContainsKey(new Person());
        }
    }

    public class ObjectWithDictWithInlineKeyNew
    {
        public IDictionary<PersonNew, int> Dict { get; set; }
    }

    [Fact(Skip = "This is very difficult to do")]
    public void UpgradingKeyInDictionary()
    {
        var singName = _db.RegisterType(typeof(ObjectWithDictWithInlineKey));
        var persName = _db.RegisterType(typeof(Person));
        using (var tr = _db.StartTransaction())
        {
            var d = tr.Singleton<ObjectWithDictWithInlineKey>();
            d.Dict.Add(new Person { Name = "A" }, 1);
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(ObjectWithDictWithInlineKeyNew), singName);
        _db.RegisterType(typeof(PersonNew), persName);
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

        ReopenDb(true);
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
            Assert.Single(tr.Enumerate<Person>());
            var d = tr.Singleton<IndirectValueDict>();
            Assert.Equal(10u, d.Dict[1].Value.Age);
            Assert.Null(d.Dict[2].Value);
        }
    }

    class WithIndirect
    {
        public IIndirect<Person> Indirect { get; set; }
    }

    [Fact]
    public void DeleteIndirectInTheSameTransaction()
    {
        using (var tr = _db.StartTransaction())
        {
            var d = tr.Singleton<WithIndirect>();
            d.Indirect = new DBIndirect<Person>(new Person());
            tr.Delete(d.Indirect);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var d = tr.Singleton<WithIndirect>();
            Assert.Null(d.Indirect.Value);
        }
    }

    [Fact]
    public void DeleteIndirectWithNullValueDoesNotThrow()
    {
        using (var tr = _db.StartTransaction())
        {
            var d = tr.Singleton<WithIndirect>();
            d.Indirect = new DBIndirect<Person>(null);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var d = tr.Singleton<WithIndirect>();
            tr.Delete(d.Indirect);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var d = tr.Singleton<WithIndirect>();
            Assert.Null(d.Indirect.Value);
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
            Assert.Single(tr.Enumerate<Person>());
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

    public class Rule1
    {
        public string Name { get; set; }
    }

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
            Assert.Equal(3, sd.Count);

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
            var deleted = sd.RemoveRange(
                new LogId
                {
                    Key = "key",
                    DateTime = DateTime.MinValue.ToUniversalTime(),
                    CollisionId = ushort.MinValue
                },
                true,
                new LogId
                {
                    Key = "key",
                    DateTime = DateTime.MaxValue.ToUniversalTime(),
                    CollisionId = ushort.MaxValue
                }, true);

            Assert.Equal(3, deleted);
            Assert.Equal(0, sd.Count);
        }
    }

    public enum TestRenamedEnum
    {
        [PersistedName("Item1")] ItemA,
        [PersistedName("Item2")] ItemB
    }

    public class CTestRenamedEnum
    {
        [PersistedName("E")] public TestRenamedEnum EE { get; set; }
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
            Assert.True(items.TryGetValue(guid, out value));

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
            Assert.True(items.TryGetValue(new UlongGuidKey { Ulong = 1, Guid = guid }, out value));

            Assert.Equal("a", value);
        }
    }

    public class TimeIndex
    {
        public IOrderedDictionary<TimeIndexKey, ulong> Items { get; set; }

        public class TimeIndexKey
        {
            public DateTime Time { get; set; }
        }
    }


    [Fact(Skip =
        "Very difficult without breaking backward compatibility of database. And what is worse problem string inside object is not ordered correctly!")]
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

    [Theory]
    [InlineData(-1, false, -1, false, "13579")]
    [InlineData(2, true, -1, false, "3579")]
    [InlineData(2, false, -1, false, "3579")]
    [InlineData(3, true, -1, false, "3579")]
    [InlineData(3, false, -1, false, "579")]
    [InlineData(-1, false, 8, true, "1357")]
    [InlineData(-1, false, 8, false, "1357")]
    [InlineData(-1, false, 7, true, "1357")]
    [InlineData(-1, false, 7, false, "135")]
    [InlineData(3, true, 7, true, "357")]
    [InlineData(3, true, 7, false, "35")]
    [InlineData(3, false, 7, true, "57")]
    [InlineData(3, false, 7, false, "5")]
    [InlineData(0, true, 10, true, "13579")]
    [InlineData(0, true, 10, false, "13579")]
    [InlineData(0, false, 10, true, "13579")]
    [InlineData(0, false, 10, false, "13579")]
    [InlineData(10, false, 0, false, "")]
    [InlineData(5, false, 5, false, "")]
    [InlineData(5, false, 5, true, "")]
    [InlineData(5, true, 5, false, "")]
    [InlineData(5, true, 5, true, "5")]
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
                    : includeStart
                        ? KeyProposition.Included
                        : KeyProposition.Excluded,
                end,
                end == -1 ? KeyProposition.Ignored :
                includeEnd ? KeyProposition.Included : KeyProposition.Excluded);
            var e = d.GetAdvancedEnumerator(param);
            var res = "";
            int key;
            Assert.Equal(result.Length, (int)e.Count);
            while (e.NextKey(out key))
            {
                Assert.Equal(res.Length, (int)e.Position);
                var val = e.CurrentValue;
                Assert.Equal(key.ToString(CultureInfo.InvariantCulture), val);
                res += val;
            }

            Assert.Equal(result, res);
            Assert.Equal(res.Length, (int)e.Position);
            param = new AdvancedEnumeratorParam<int>(EnumerationOrder.Descending, start,
                start == -1
                    ? KeyProposition.Ignored
                    : includeStart
                        ? KeyProposition.Included
                        : KeyProposition.Excluded,
                end,
                end == -1 ? KeyProposition.Ignored :
                includeEnd ? KeyProposition.Included : KeyProposition.Excluded);
            e = d.GetAdvancedEnumerator(param);
            res = "";
            Assert.Equal(result.Length, (int)e.Count);
            while (e.NextKey(out key))
            {
                Assert.Equal(res.Length, (int)e.Position);
                var val = e.CurrentValue;
                Assert.Equal(key.ToString(CultureInfo.InvariantCulture), val);
                res = val + res;
            }

            Assert.Equal(result, res);
            Assert.Equal(res.Length, (int)e.Position);
        }
    }

    [Theory]
    [InlineData(-1, false, -1, false, "13579")]
    [InlineData(2, true, -1, false, "3579")]
    [InlineData(2, false, -1, false, "3579")]
    [InlineData(3, true, -1, false, "3579")]
    [InlineData(3, false, -1, false, "579")]
    [InlineData(-1, false, 8, true, "1357")]
    [InlineData(-1, false, 8, false, "1357")]
    [InlineData(-1, false, 7, true, "1357")]
    [InlineData(-1, false, 7, false, "135")]
    [InlineData(3, true, 7, true, "357")]
    [InlineData(3, true, 7, false, "35")]
    [InlineData(3, false, 7, true, "57")]
    [InlineData(3, false, 7, false, "5")]
    [InlineData(0, true, 10, true, "13579")]
    [InlineData(0, true, 10, false, "13579")]
    [InlineData(0, false, 10, true, "13579")]
    [InlineData(0, false, 10, false, "13579")]
    [InlineData(10, false, 0, false, "")]
    [InlineData(5, false, 5, false, "")]
    [InlineData(5, false, 5, true, "")]
    [InlineData(5, true, 5, false, "")]
    [InlineData(5, true, 5, true, "5")]
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
                    : includeStart
                        ? KeyProposition.Included
                        : KeyProposition.Excluded,
                end,
                end == -1 ? KeyProposition.Ignored :
                includeEnd ? KeyProposition.Included : KeyProposition.Excluded);
            var e = d.GetAdvancedEnumerator(param);
            var res = "";
            int key;
            e.Position = 2;
            while (e.NextKey(out key))
            {
                Assert.Equal(res.Length, (int)(e.Position - 2));
                var val = e.CurrentValue;
                Assert.Equal(key.ToString(CultureInfo.InvariantCulture), val);
                res += val;
            }

            Assert.Equal(result.Substring(Math.Min(result.Length, 2)), res);
            param = new AdvancedEnumeratorParam<int>(EnumerationOrder.Descending, start,
                start == -1
                    ? KeyProposition.Ignored
                    : includeStart
                        ? KeyProposition.Included
                        : KeyProposition.Excluded,
                end,
                end == -1 ? KeyProposition.Ignored :
                includeEnd ? KeyProposition.Included : KeyProposition.Excluded);
            e = d.GetAdvancedEnumerator(param);
            res = "";
            e.Position = 2;
            while (e.NextKey(out key))
            {
                Assert.Equal(res.Length, (int)(e.Position - 2));
                var val = e.CurrentValue;
                Assert.Equal(key.ToString(CultureInfo.InvariantCulture), val);
                res = val + res;
            }

            Assert.Equal(result.Substring(0, Math.Max(0, result.Length - 2)), res);
        }
    }

    public enum MyEnum
    {
        Val = 0,
        Val2 = 1
    }

    public class ConversionItemOld
    {
        public ulong Id { get; set; }
        public MyEnum En { get; set; }
    }

    public class ConversionItemsOld
    {
        public IDictionary<ulong, ConversionItemOld> Items { get; set; }
    }

    public class ConversionItemNew
    {
        public ulong Id { get; set; }
        public string En { get; set; }
    }

    public class ConversionItemsNew
    {
        public IDictionary<ulong, ConversionItemNew> Items { get; set; }
    }

    public class EnumToStringTypeConvertorGenerator : DefaultTypeConvertorGenerator
    {
        public override Action<IILGen> GenerateConversion(Type from, Type to)
        {
            if (from.IsEnum && to == typeof(string))
            {
                var fromcfg = new EnumFieldHandler.EnumConfiguration(from);
                if (fromcfg.Flags)
                    return null; // Flags are hard :-)
                var cfgIdx = 0;
                while (true)
                {
                    var oldEnumCfgs = _enumCfgs;
                    var newEnumCfgs = oldEnumCfgs;
                    Array.Resize(ref newEnumCfgs, oldEnumCfgs?.Length + 1 ?? 1);
                    cfgIdx = newEnumCfgs.Length - 1;
                    newEnumCfgs[cfgIdx] = fromcfg;
                    if (Interlocked.CompareExchange(ref _enumCfgs, newEnumCfgs, oldEnumCfgs) == oldEnumCfgs)
                        break;
                }

                return il =>
                {
                    il
                        .ConvU8()
                        .LdcI4(cfgIdx)
                        .Call(() => EnumToString(0, 0));
                };
            }

            return base.GenerateConversion(from, to);
        }

        static EnumFieldHandler.EnumConfiguration[] _enumCfgs;

        public static string EnumToString(ulong value, int cfgIdx)
        {
            var cfg = _enumCfgs[cfgIdx];
            // What should happen if not found? for now just crash.
            return cfg.Names[Array.IndexOf(cfg.Values, value)];
        }
    }

    [Fact]
    public void CustomFieldConversionTest_Perform()
    {
        _db.RegisterType(typeof(ConversionItemsOld), "ConversionItems");
        _db.RegisterType(typeof(ConversionItemOld), "ConversionItem");
        using (var tr = _db.StartTransaction())
        {
            var singleton = tr.Singleton<ConversionItemsOld>();
            singleton.Items[1] = new ConversionItemOld { En = MyEnum.Val2, Id = 1 };
            tr.Store(singleton);
            tr.Commit();
        }

        ReopenDb(true);
        _db.TypeConvertorGenerator = new EnumToStringTypeConvertorGenerator();
        _db.RegisterType(typeof(ConversionItemsNew), "ConversionItems");
        _db.RegisterType(typeof(ConversionItemNew), "ConversionItem");

        using (var tr = _db.StartTransaction())
        {
            var singleton = tr.Singleton<ConversionItemsNew>().Items;

            Assert.Equal("Val2", singleton[1].En);
            tr.Commit();
        }
    }

    public class Key
    {
        public ulong Id { get; set; }
        public string Name { get; set; }
    }

    public class EmailAtttachments
    {
        public IOrderedDictionary<Key, IIndirect<Value>> Attachments { get; set; }
    }

    public class Value
    {
        public byte[] Val { get; set; }
    }

    string data =
        "QlREQkVYUDILAAAAAAAAAAMAAAAAAAESAAAAEkVtYWlsQXR0dGFjaG1lbnRzAwAAAAAAAgQAAAAES2V5AwAAAAAAAwYAAAAGVmFsdWUEAAAAAAEBATYAAAABDEF0dGFjaG1lbnRzDk9EQkRpY3Rpb25hcnkbB09iamVjdAUES2V5B09iamVjdAcGVmFsdWUEAAAAAAECARsAAAACA0lkCVVuc2lnbmVkAAVOYW1lB1N0cmluZwAEAAAAAAEDAQ0AAAABBFZhbAdCeXRlW10AAwAAAAACAQEAAAABAgAAAAADAQAAAAECAAAAAQEDAAAAAQEAAgAAAAECBAAAAAMBAgEMAAAAAgB/AgE3BmFob3BqAQAAAII=";

    [Fact]
    public void IIndirectTest()
    {
        using (var tr = _lowDb.StartWritingTransaction().Result)
        {
            KeyValueDBExportImporter.Import(tr, new MemoryStream(Convert.FromBase64String(data)));
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var att = tr.Singleton<EmailAtttachments>().Attachments;
            Assert.NotNull(att);
            tr.Commit();
        }
    }

    public class UserKey
    {
        public ulong CompanyId { get; set; }
        public string Email { get; set; }
    }

    public class ItemsDict
    {
        public IOrderedDictionary<UserKey, ulong> Items { get; set; }
    }

    string data2 =
        "QlREQkVYUDIIAAAAAAAAAAMAAAAAAAEKAAAACkl0ZW1zRGljdAMAAAAAAAIIAAAACFVzZXJLZXkEAAAAAAEBATAAAAABBkl0ZW1zDk9EQkRpY3Rpb25hcnkbB09iamVjdAkIVXNlcktleQlVbnNpZ25lZAAEAAAAAAECASMAAAACCkNvbXBhbnlJZAlVbnNpZ25lZAAGRW1haWwHU3RyaW5nAAMAAAAAAgEBAAAAAQIAAAAAAwEAAAABAgAAAAEBAwAAAAEBABYAAAACAH8CAQEQbmVrZG9AbmVrZGUubmV0AQAAAGU=";

    [Fact]
    public void NotMaterializingDict2()
    {
        using (var tr = _lowDb.StartWritingTransaction().Result)
        {
            KeyValueDBExportImporter.Import(tr, new MemoryStream(Convert.FromBase64String(data2)));
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var att = tr.Singleton<ItemsDict>().Items;
            Assert.NotNull(att);
            Assert.Equal(1, att.Count);
            var key = new UserKey() { CompanyId = 1, Email = "nekdo@nekde.net" };
            Assert.True(att.ContainsKey(key));
            Assert.Equal(101UL, att[key]);
            tr.Commit();
        }
    }

    [Fact]
    public void IIndirectCanBeUpdatedDirectlyUsingTrStore()
    {
        using (var tr = _db.StartTransaction())
        {
            var att = tr.Singleton<IndirectTree>();
            att.Content = "a";
            att.Left = new DBIndirect<IndirectTree>(new IndirectTree() { Content = "b" }) { };
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var att = tr.Singleton<IndirectTree>();
            var ii = att.Left;
            Assert.Equal("b", ii.Value.Content);
            ii.Value.Content = "c";
            tr.Store(ii); // Store directly IIndirect
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var att = tr.Singleton<IndirectTree>();
            var ii = att.Left;
            Assert.Equal("c", ii.Value.Content);
            tr.Commit();
        }
    }

    [Fact]
    public void CompatibilityObjectInsideInlineList()
    {
        const string data3 =
            "QlREQkVYUDJUAAAAAAAAAAMAAAAAAAEMAAAADExhbmd1YWdlTWFwAwAAAAAAAgsAAAALTGFuZ3VhZ2VEYgMAAAAAAAMJAAAACVNoZWV0TWFwAwAAAAAABAgAAAAIU2hlZXREYgMAAAAAAAUNAAAADUF0dHJpYnV0ZU1hcAMAAAAAAAYMAAAADEF0dHJpYnV0ZURiAwAAAAAABwoAAAAKU2NoZW1hTWFwAwAAAAAACAkAAAAJU2NoZW1hRGIDAAAAAAAJDwAAAA9CYXJjb2RlU2V0dGluZwMAAAAAAAoKAAAAClF1aWV0Wm9uZQMAAAAAAAsLAAAAC09tclNldHRpbmcDAAAAAAAMDgAAAA5Db2RlMzlTZXR0aW5nAwAAAAAADRIAAAASRGF0YU1hdHJpeFNldHRpbmcDAAAAAAAOEQAAABFJbnNlcnRlckZ1bmN0aW9uAwAAAAAADwkAAAAJRW5jb2RpbmcDAAAAAAAQDAAAAAxSdWxlU2V0dGluZwMAAAAAABEbAAAAG1ByaW9yaXRpemVkRnVuY3Rpb25TZXR0aW5nAwAAAAAAEhMAAAATTGltaXRlZFJ1bGVTZXR0aW5nAwAAAAAAExIAAAASQ29tcG9zaXRlRnVuY3Rpb24DAAAAAAAUFQAAABVVcGRhdGVQcmludGVyTGlzdE1hcAMAAAAAABUUAAAAFFVwZGF0ZVByaW50ZXJMaXN0RGIDAAAAAAAWGAAAABhQcm9jZXNzYWJsZUl0ZW1TdGF0ZU1hcAMAAAAAABcWAAAAFlByb2Nlc3NhYmxlSXRlbVN0YXRlcwMAAAAAABgVAAAAFUFpbXNDb25maWd1cmF0aW9uTWFwAwAAAAAAGRQAAAAUQWltc0NvbmZpZ3VyYXRpb25EYgMAAAAAABoQAAAAEFByb2Nlc3NhYmxlSXRlbQQAAAAAAQEBMwAAAAEGSXRlbXMOT0RCRGljdGlvbmFyeR4JVW5zaWduZWQAB09iamVjdAwLTGFuZ3VhZ2VEYgQAAAAAAQIBRwAAAAQKQ29tcGFueUlkCVVuc2lnbmVkAAVOYW1lB1N0cmluZwAKSXNEZWZhdWx0BUJvb2wAC1JldmlzaW9uSWQJVW5zaWduZWQABAAAAAABAwEwAAAAAQZJdGVtcw5PREJEaWN0aW9uYXJ5GwlVbnNpZ25lZAAHT2JqZWN0CQhTaGVldERiBAAAAAABBAGgAQAADgpDb21wYW55SWQJVW5zaWduZWQABU5hbWUHU3RyaW5nAAxEZXNjcmlwdGlvbgdTdHJpbmcADE9yaWVudGF0aW9uBUVudW0XCQlQb3J0cmFpdApMYW5kc2NhcGWBggpTaGVldEtpbmQFRW51bUEpA0E0A0E1A0EzB0xldHRlcgZMZWdhbAhUYWJsb2lkBkZvcm1BBkZvcm1CBkZvcm1DBUF1dG + AgYKDhIWGh4iJEVByaW50T25Cb3RoU2lkZXMFRW51bQsJA05vBFllc4CBB1dlaWdodAdEb3VibGUAFFByaW50ZXJQYXBlclNvdXJjZXMFTGlzdB4HT2JqZWN0FhVQcmludGVyUGFwZXJTb3VyY2VEYgpJc0RlZmF1bHQFQm9vbAAHU3RhdHVzBUVudW0UCQhFbmFibGVkCERlbGV0ZWSAgRJCYWNrc2lkZU92ZXJsYXlJZAlVbnNpZ25lZAALVXBkYXRlRGF0ZQlEYXRlVGltZQAFVGFncwVMaXN0CwlVbnNpZ25lZAALUmV2aXNpb25JZAlVbnNpZ25lZAAEAAAAAAEFATQAAAABBkl0ZW1zDk9EQkRpY3Rpb25hcnkfCVVuc2lnbmVkAAdPYmplY3QNDEF0dHJpYnV0ZURiBAAAAAABBgGZAgAADQpDb21wYW55SWQJVW5zaWduZWQABU5hbWUHU3RyaW5nAAxEZXNjcmlwdGlvbgdTdHJpbmcAClZhbHVlVHlwZQVFbnVtRRkFVGV4dAdOdW1iZXIJRGF0ZVRpbWUOUG9zdGFsQWRkcmVzcwhCb29sZWFuElBvc3RhbEFkZHJlc3NMaW5lgIGCg4SFFVN5c3RlbUF0dHJpYnV0ZVZhbHVlBUVudW2BFk0NQ3JlYXRpb25EYXRlDEZpbGVDb3VudGVyEVByaW50T25Cb3RoU2lkZXMLQ3VzdG9tZXJJZA9Eb2N1bWVudE51bWJlcg5EdWN1bWVudFRpdGxlDFRvdGFsQW1vdW50DURlbGl2ZXJ5VHlwZQ9Ub3RhbERvY3VtZW50cwxUb3RhbFNoZWV0cw9FbnZlbG9wZUZvcm1hdApUaGlja25lc3MHV2VpZ2h0CVByaW9yaXR5DUVudmVsb3BlTmFtZQ1FbnZlbG9wZVNpemUOTnVtYmVyT2ZQYWdlcxVGaXJzdEN1c3RvbUF0dHJpYnV0ZRNBbnlDdXN0b21BdHRyaWJ1dGWBgoOEhYaHiYqLjI2Oj5CRksBkwGULSXNFeHRlcm5hbAVCb29sAA9SZW1vdmVkUGF0dGVybgdTdHJpbmcAD0V4dGVybmFsQ29uZmlnB09iamVjdBIRRXh0ZXJuYWxDb25maWdEYhJQb3N0YWxBZGRyZXNzTGluZQdPYmplY3QWFU9tc1Bvc3RhbEFkZHJlc3NMaW5lEFN1YkF0dHJpYnV0ZUlkcwVMaXN0CwlVbnNpZ25lZAALUmV2aXNpb25JZAlVbnNpZ25lZAALVXBkYXRlRGF0ZQlEYXRlVGltZQAFVGFncwVMaXN0CwlVbnNpZ25lZAAEAAAAAAEHATEAAAABBkl0ZW1zDk9EQkRpY3Rpb25hcnkcCVVuc2lnbmVkAAdPYmplY3QKCVNjaGVtYURiBAAAAAABCAH5AQAADwpDb21wYW55SWQJVW5zaWduZWQAC1NjaGVtYVR5cGUFRW51bVQdB0N1c3RvbQRPbXIMT21yQWR2YW5jZWQHQ29kZTM5D0NvZGUzOUFkdmFuY2VkC0RhdGFNYXRyaXgTRGF0YU1hdHJpeEFkdmFuY2VkgIGCg4SFhgxCYXJjb2RlVHlwZQVFbnVtGw0ET21yB0NvZGUzOQtEYXRhTWF0cml4gIGCEUNvbnRlbnRNYXhMZW5ndGgHU2lnbmVkAAVOYW1lB1N0cmluZwAMRGVzY3JpcHRpb24HU3RyaW5nAAZCcmFuZAdTdHJpbmcAD0JhcmNvZGVTZXR0aW5nB09iamVjdBAPQmFyY29kZVNldHRpbmcSSW5zZXJ0ZXJGdW5jdGlvbnMFTGlzdBoHT2JqZWN0EhFJbnNlcnRlckZ1bmN0aW9uDlJ1bnRpbWVHcm91cHMFTGlzdBUHT2JqZWN0DQxSdWxlU2V0dGluZxRDb25maWd1cmF0aW9uR3JvdXBzBUxpc3QcB09iamVjdBQTTGltaXRlZFJ1bGVTZXR0aW5nE0lzQXZhaWxhYmxlRm9yQWltcwVCb29sAAtSZXZpc2lvbklkCVVuc2lnbmVkAAtVcGRhdGVEYXRlCURhdGVUaW1lAAVUYWdzBUxpc3QLCVVuc2lnbmVkAAQAAAAAAQkBAwEAAAcMVG9wUG9zaXRpb24HRG91YmxlAA1MZWZ0UG9zaXRpb24HRG91YmxlAApRdWlldFpvbmUHT2JqZWN0CwpRdWlldFpvbmUMT3JpZW50YXRpb24FRW51bT4RC0hvcml6b250YWwJVmVydGljYWwTSG9yaXpvbnRhbFJldmVyc2VkEVZlcnRpY2FsUmV2ZXJzZWSAgYKDC09tclNldHRpbmcHT2JqZWN0DAtPbXJTZXR0aW5nDkNvZGUzOVNldHRpbmcHT2JqZWN0Dw5Db2RlMzlTZXR0aW5nEkRhdGFNYXRyaXhTZXR0aW5nB09iamVjdBMSRGF0YU1hdHJpeFNldHRpbmcEAAAAAAEKATMAAAACEFZlcnRpY2FsUGFkZGluZwdEb3VibGUAEkhvcml6b250YWxQYWRkaW5nB0RvdWJsZQAEAAAAAAELAUEAAAADDFdpZHRoT2ZNYXJrB0RvdWJsZQAQVGhpY2tuZXNzT2ZNYXJrB0RvdWJsZQAMTGluZVNwYWNpbmcHRG91YmxlAAQAAAAAAQwBOwAAAAMHSGVpZ2h0B0RvdWJsZQAVVGhpY2tuZXNzT2ZOYXJyb3dCYXIHRG91YmxlAAZSYXRpbwdEb3VibGUABAAAAAABDQFFAAAAAglDZWxsU2l6ZQdEb3VibGUAC1N5bWJvbFNpemUFRW51bSMRBUF1dG8IRml4ZWQxMghGaXhlZDIwCEZpeGVkMjSAjJSYBAAAAAABDgGMCwAAGQ1GdW5jdGlvblR5cGUFRW51bYPSgSEFTm9uZQdQYWNrVXAKU3RhcnRNYXJrC1NhZmV0eU1hcmsHUGFyaXR5D051bWJlck9mU2hlZXRzDFNoZWV0TnVtYmVyDlNoZWV0U2VxdWVuY2UOU2VxdWVuY2VDaGVjaw5Hcm91cFNlcXVlbmNlCVNldE1hdGNoC0VuZE9mR3JvdXARRW5kT2ZHcm91cERlbWFuZA1GaXJzdE9mR3JvdXATRmlyc3RPZkdyb3VwRGVtYW5kB0luc2VydAtBY2N1bXVsYXRlB0FjY0lucwVTdG9wD0RpdmVydENvbnRpbnVlC0RpdmVydFN0b3AIRGl2ZXJ0MQhEaXZlcnQyD1NlYWxpbmdDb250cm9sD1NlbGVjdGl2ZUZlZWQxD1NlbGVjdGl2ZUZlZWQyD1NlbGVjdGl2ZUZlZWQzD1NlbGVjdGl2ZUZlZWQ0D1NlbGVjdGl2ZUZlZWQ1D1NlbGVjdGl2ZUZlZWQ2D1NlbGVjdGl2ZUZlZWQ3D1NlbGVjdGl2ZUZlZWQ4CkVudmVsb3BlMQpFbnZlbG9wZTIKRW52ZWxvcGUzDEV4aXRDb250cm9sBkV4aXQxBkV4aXQyBkV4aXQzEFByZXNlbnRDb250aW51ZQxQcmVzZW50U3RvcAtFbnZlbG9wZUlkC0RvY3VtZW50SWQLQ3VzdG9tZXJJZAlQYWdlTm9mTQtGb3JjZWRGb2xkCUZyYW5rZXIxCUZyYW5rZXIyCU1hdGNoaW5nEEdyb3VwQ291bnRJbkpvYhBHcm91cEluZGV4SW5Kb2IPRm9ybUluZGV4SW5Kb2IRRm9ybUNvdW50SW5Hcm91cBFGb3JtSW5kZXhJbkdyb3VwBkpvYklkDE1haWxwaWVjZUlkCUVuZE9mSm9iCUlua01hcmsxCUlua01hcmsyC0pvZ0NvbnRyb2wVUHJpbnRPbmx5T25GaXJzdFBhZ2UIRGl2ZXJ0Mw9TZWxlY3RpdmVGZWVkORBTZWxlY3RpdmVGZWVkMTAQU2VsZWN0aXZlRmVlZDExEFNlbGVjdGl2ZUZlZWQxMhBTZWxlY3RpdmVGZWVkMTMQU2VsZWN0aXZlRmVlZDE0EFNlbGVjdGl2ZUZlZWQxNQ1SZVRpbWVHcm91cDENUmVUaW1lR3JvdXAyDVJlVGltZUdyb3VwM4CKi4yVlpeYmZqbpaanqKmqq6ytrq + wsbKztLW2t7i5uru8vb6 / wEDAQcBCwEbAR8BIwEnASsBLwEzATcBOwE / AUcBSwFPAVMBVwFbAV8BYwFnAWsBbwJbAl8CYwJnAmsCbwJzAncCewJ8JRW5jb2RpbmcHT2JqZWN0CglFbmNvZGluZwVOYW1lB1N0cmluZwATQ29tcG9zaXRlRnVuY3Rpb25zBUxpc3QbB09iamVjdBMSQ29tcG9zaXRlRnVuY3Rpb24VU2V0V2l0aEZ1bmN0aW9uVHlwZXMFTGlzdIPZBUVudW2D0oEhBU5vbmUHUGFja1VwClN0YXJ0TWFyawtTYWZldHlNYXJrB1Bhcml0eQ9OdW1iZXJPZlNoZWV0cwxTaGVldE51bWJlcg5TaGVldFNlcXVlbmNlDlNlcXVlbmNlQ2hlY2sOR3JvdXBTZXF1ZW5jZQlTZXRNYXRjaAtFbmRPZkdyb3VwEUVuZE9mR3JvdXBEZW1hbmQNRmlyc3RPZkdyb3VwE0ZpcnN0T2ZHcm91cERlbWFuZAdJbnNlcnQLQWNjdW11bGF0ZQdBY2NJbnMFU3RvcA9EaXZlcnRDb250aW51ZQtEaXZlcnRTdG9wCERpdmVydDEIRGl2ZXJ0Mg9TZWFsaW5nQ29udHJvbA9TZWxlY3RpdmVGZWVkMQ9TZWxlY3RpdmVGZWVkMg9TZWxlY3RpdmVGZWVkMw9TZWxlY3RpdmVGZWVkNA9TZWxlY3RpdmVGZWVkNQ9TZWxlY3RpdmVGZWVkNg9TZWxlY3RpdmVGZWVkNw9TZWxlY3RpdmVGZWVkOApFbnZlbG9wZTEKRW52ZWxvcGUyCkVudmVsb3BlMwxFeGl0Q29udHJvbAZFeGl0MQZFeGl0MgZFeGl0MxBQcmVzZW50Q29udGludWUMUHJlc2VudFN0b3ALRW52ZWxvcGVJZAtEb2N1bWVudElkC0N1c3RvbWVySWQJUGFnZU5vZk0LRm9yY2VkRm9sZAlGcmFua2VyMQlGcmFua2VyMglNYXRjaGluZxBHcm91cENvdW50SW5Kb2IQR3JvdXBJbmRleEluSm9iD0Zvcm1JbmRleEluSm9iEUZvcm1Db3VudEluR3JvdXARRm9ybUluZGV4SW5Hcm91cAZKb2JJZAxNYWlscGllY2VJZAlFbmRPZkpvYglJbmtNYXJrMQlJbmtNYXJrMgtKb2dDb250cm9sFVByaW50T25seU9uRmlyc3RQYWdlCERpdmVydDMPU2VsZWN0aXZlRmVlZDkQU2VsZWN0aXZlRmVlZDEwEFNlbGVjdGl2ZUZlZWQxMRBTZWxlY3RpdmVGZWVkMTIQU2VsZWN0aXZlRmVlZDEzEFNlbGVjdGl2ZUZlZWQxNBBTZWxlY3RpdmVGZWVkMTUNUmVUaW1lR3JvdXAxDVJlVGltZUdyb3VwMg1SZVRpbWVHcm91cDOAiouMlZaXmJmam6Wmp6ipqqusra6vsLGys7S1tre4ubq7vL2 + v8BAwEHAQsBGwEfASMBJwErAS8BMwE3ATsBPwFHAUsBTwFTAVcBWwFfAWMBZwFrAW8CWwJfAmMCZwJrAm8CcwJ3AnsCfClZhbHVlVHlwZQVFbnVtPhUHU3RyaW5nEFN0cmluZ1VwcGVyQ2FzZRBTdHJpbmdMb3dlckNhc2UISW50ZWdlcghCb29sZWFugIGCg4QTU3RhdGljVmFsdWVCb29sZWFuBUJvb2wAE1N0YXRpY1ZhbHVlSW50ZWdlcgdTaWduZWQAElN0YXRpY1ZhbHVlU3RyaW5nB1N0cmluZwAHUHJlZml4B1N0cmluZwAQQ2hhcmFjdGVyTnVtYmVyB1NpZ25lZAATQ2hhcmFjdGVyTnVtYmVyTWluB1NpZ25lZAATQ2hhcmFjdGVyTnVtYmVyTWF4B1NpZ25lZAARQ2hlY2tlZEJ5RGVmYXVsdAVCb29sAApJc0R5bmFtaWMFQm9vbAAPSXNDb25maWd1cmFibGUFQm9vbAAOSXNDb25kaXRpb25hbAVCb29sAAlJc0ZlZWRlcgVCb29sAAtJc1ByZWZpeGVkBUJvb2wAC0lzQ29tcHV0ZWQFQm9vbAAJQ2F0ZWdvcnkFRW51bR4NB1N0YXRpYwpJbnRlZ3JpdHkIQ29udHJvbICBggtFdmFsdWF0aW9uBUVudW1fJQVOb25lBkV2ZXJ5BkZpcnN0BUxhc3QMRXhjZXB0Rmlyc3QLRXhjZXB0TGFzdBNFeGNlcHRGaXJzdEFuZExhc3QLU3RhcnRPZkpvYglFbmRPZkpvYoCBgoOEhYaHiAlTZXF1ZW5jZQVFbnVtgOVBBU5vbmUQR3JvdXBDb3VudEluSm9iElNoZWV0SW5kZXhJbkdyb3VwElNoZWV0Q291bnRJbkdyb3VwEFNoZWV0SW5kZXhJbkpvYhBHcm91cEluZGV4SW5Kb2ITU2hlZXROdW1iZXJJbkdyb3VwFURvY3VtZW50SW5kZXhJbkdyb3VwCEFkZHJlc3MMVXNlckRlZmluZWQGSm9iSWQMTWFpbHBpZWNlSWQLU3RhcnRPZkpvYg1TdGFydE9mR3JvdXALRW5kT2ZHcm91cAlFbmRPZkpvYoCBgoOEhYaHiImKi4yNjo8OU2VxdWVuY2VPcmRlcgVFbnVtGQkKQXNjZW5kaW5nC0Rlc2NlbmRpbmeAgQxQYXJpdHlMb2dpYwVFbnVtEw0FTm9uZQVFdmVuBE9kZICBggQAAAAAAQ8B3wEAAA4FVHlwZQVFbnVtgKItBU5vbmUHQmluYXJ5DEhleGFkZWNpbWFsBk9jdGFsDkJjc0NoYXJhY3RlcnMPQmNzRGl2ZXJ0VW5pdHMSQmNzTnVtYmVyT2ZTaGVldHMRQmNzRmVlZGVyQ29udHJvbB1CY3NDaGFyYWN0ZXJzUmVzZXRPbk92ZXJmbG93DU51bWVyaWNBbHBoYQ1BbHBoYU51bWVyaWOAgYKDhIWGh4iJigpHcm91cFNpemUHU2lnbmVkAAtHcm91cE9yZGVyBUVudW0ZCQpBc2NlbmRpbmcLRGVzY2VuZGluZ4CBCEdyb3VwSWQHU3RyaW5nAAhBZGljaXR5B1NpZ25lZAAMVmFsdWVPZmZzZXQHU2lnbmVkAAlNaW5WYWx1ZQdTaWduZWQACU1heFZhbHVlB1NpZ25lZAANUmVnRXhQYXR0ZXJuB1N0cmluZwARUmVnRXhSZXBsYWNlbWVudAdTdHJpbmcAEVJlc2V0T25TaXplRXJyb3IFQm9vbAAJQml0T3JkZXIFRW51bRMJB05vcm1hbAhSZXZlcnNlgIEMUGFkZGluZ0NoYXIHU3RyaW5nAApQYWRTdHJpbmcFRW51bRcNB05vdFNldAVMZWZ0BlJpZ2h0gIGCBAAAAAABEAFXAAAAAglQcmlvcml0eQdTaWduZWQAHFByaW9yaXRpemVkRnVuY3Rpb25TZXR0aW5ncwVMaXN0JAdPYmplY3QcG1ByaW9yaXRpemVkRnVuY3Rpb25TZXR0aW5nBAAAAAABEQF9BAAABglQcmlvcml0eQdTaWduZWQADUZ1bmN0aW9uVHlwZQVFbnVtg9KBIQVOb25lB1BhY2tVcApTdGFydE1hcmsLU2FmZXR5TWFyawdQYXJpdHkPTnVtYmVyT2ZTaGVldHMMU2hlZXROdW1iZXIOU2hlZXRTZXF1ZW5jZQ5TZXF1ZW5jZUNoZWNrDkdyb3VwU2VxdWVuY2UJU2V0TWF0Y2gLRW5kT2ZHcm91cBFFbmRPZkdyb3VwRGVtYW5kDUZpcnN0T2ZHcm91cBNGaXJzdE9mR3JvdXBEZW1hbmQHSW5zZXJ0C0FjY3VtdWxhdGUHQWNjSW5zBVN0b3APRGl2ZXJ0Q29udGludWULRGl2ZXJ0U3RvcAhEaXZlcnQxCERpdmVydDIPU2VhbGluZ0NvbnRyb2wPU2VsZWN0aXZlRmVlZDEPU2VsZWN0aXZlRmVlZDIPU2VsZWN0aXZlRmVlZDMPU2VsZWN0aXZlRmVlZDQPU2VsZWN0aXZlRmVlZDUPU2VsZWN0aXZlRmVlZDYPU2VsZWN0aXZlRmVlZDcPU2VsZWN0aXZlRmVlZDgKRW52ZWxvcGUxCkVudmVsb3BlMgpFbnZlbG9wZTMMRXhpdENvbnRyb2wGRXhpdDEGRXhpdDIGRXhpdDMQUHJlc2VudENvbnRpbnVlDFByZXNlbnRTdG9wC0VudmVsb3BlSWQLRG9jdW1lbnRJZAtDdXN0b21lcklkCVBhZ2VOb2ZNC0ZvcmNlZEZvbGQJRnJhbmtlcjEJRnJhbmtlcjIJTWF0Y2hpbmcQR3JvdXBDb3VudEluSm9iEEdyb3VwSW5kZXhJbkpvYg9Gb3JtSW5kZXhJbkpvYhFGb3JtQ291bnRJbkdyb3VwEUZvcm1JbmRleEluR3JvdXAGSm9iSWQMTWFpbHBpZWNlSWQJRW5kT2ZKb2IJSW5rTWFyazEJSW5rTWFyazILSm9nQ29udHJvbBVQcmludE9ubHlPbkZpcnN0UGFnZQhEaXZlcnQzD1NlbGVjdGl2ZUZlZWQ5EFNlbGVjdGl2ZUZlZWQxMBBTZWxlY3RpdmVGZWVkMTEQU2VsZWN0aXZlRmVlZDEyEFNlbGVjdGl2ZUZlZWQxMxBTZWxlY3RpdmVGZWVkMTQQU2VsZWN0aXZlRmVlZDE1DVJlVGltZUdyb3VwMQ1SZVRpbWVHcm91cDINUmVUaW1lR3JvdXAzgIqLjJWWl5iZmpulpqeoqaqrrK2ur7CxsrO0tba3uLm6u7y9vr / AQMBBwELARsBHwEjAScBKwEvATMBNwE7AT8BRwFLAU8BUwFXAVsBXwFjAWcBawFvAlsCXwJjAmcCawJvAnMCdwJ7AnxxUcnVlRXF1aXZhbGVudEludGVnZXJWYWx1ZXMFTGlzdAkHU2lnbmVkABJSZXNldEludGVnZXJWYWx1ZQdTaWduZWQAG1RydWVFcXVpdmFsZW50U3RyaW5nVmFsdWVzBUxpc3QJB1N0cmluZwARUmVzZXRTdHJpbmdWYWx1ZQdTdHJpbmcABAAAAAABEgF2BAAABQxNaW5TZWxlY3RlZAdTaWduZWQADE1heFNlbGVjdGVkB1NpZ25lZAAYUnVsZUFjdGl2YXRpbmdGdW5jdGlvbnMFTGlzdIPZBUVudW2D0oEhBU5vbmUHUGFja1VwClN0YXJ0TWFyawtTYWZldHlNYXJrB1Bhcml0eQ9OdW1iZXJPZlNoZWV0cwxTaGVldE51bWJlcg5TaGVldFNlcXVlbmNlDlNlcXVlbmNlQ2hlY2sOR3JvdXBTZXF1ZW5jZQlTZXRNYXRjaAtFbmRPZkdyb3VwEUVuZE9mR3JvdXBEZW1hbmQNRmlyc3RPZkdyb3VwE0ZpcnN0T2ZHcm91cERlbWFuZAdJbnNlcnQLQWNjdW11bGF0ZQdBY2NJbnMFU3RvcA9EaXZlcnRDb250aW51ZQtEaXZlcnRTdG9wCERpdmVydDEIRGl2ZXJ0Mg9TZWFsaW5nQ29udHJvbA9TZWxlY3RpdmVGZWVkMQ9TZWxlY3RpdmVGZWVkMg9TZWxlY3RpdmVGZWVkMw9TZWxlY3RpdmVGZWVkNA9TZWxlY3RpdmVGZWVkNQ9TZWxlY3RpdmVGZWVkNg9TZWxlY3RpdmVGZWVkNw9TZWxlY3RpdmVGZWVkOApFbnZlbG9wZTEKRW52ZWxvcGUyCkVudmVsb3BlMwxFeGl0Q29udHJvbAZFeGl0MQZFeGl0MgZFeGl0MxBQcmVzZW50Q29udGludWUMUHJlc2VudFN0b3ALRW52ZWxvcGVJZAtEb2N1bWVudElkC0N1c3RvbWVySWQJUGFnZU5vZk0LRm9yY2VkRm9sZAlGcmFua2VyMQlGcmFua2VyMglNYXRjaGluZxBHcm91cENvdW50SW5Kb2IQR3JvdXBJbmRleEluSm9iD0Zvcm1JbmRleEluSm9iEUZvcm1Db3VudEluR3JvdXARRm9ybUluZGV4SW5Hcm91cAZKb2JJZAxNYWlscGllY2VJZAlFbmRPZkpvYglJbmtNYXJrMQlJbmtNYXJrMgtKb2dDb250cm9sFVByaW50T25seU9uRmlyc3RQYWdlCERpdmVydDMPU2VsZWN0aXZlRmVlZDkQU2VsZWN0aXZlRmVlZDEwEFNlbGVjdGl2ZUZlZWQxMRBTZWxlY3RpdmVGZWVkMTIQU2VsZWN0aXZlRmVlZDEzEFNlbGVjdGl2ZUZlZWQxNBBTZWxlY3RpdmVGZWVkMTUNUmVUaW1lR3JvdXAxDVJlVGltZUdyb3VwMg1SZVRpbWVHcm91cDOAiouMlZaXmJmam6Wmp6ipqqusra6vsLGys7S1tre4ubq7vL2 + v8BAwEHAQsBGwEfASMBJwErAS8BMwE3ATsBPwFHAUsBTwFTAVcBWwFfAWMBZwFrAW8CWwJfAmMCZwJrAm8CcwJ3AnsCfCVByaW9yaXR5B1NpZ25lZAAcUHJpb3JpdGl6ZWRGdW5jdGlvblNldHRpbmdzBUxpc3QkB09iamVjdBwbUHJpb3JpdGl6ZWRGdW5jdGlvblNldHRpbmcEAAAAAAETAZcFAAAFDUZ1bmN0aW9uVHlwZQVFbnVtg9KBIQVOb25lB1BhY2tVcApTdGFydE1hcmsLU2FmZXR5TWFyawdQYXJpdHkPTnVtYmVyT2ZTaGVldHMMU2hlZXROdW1iZXIOU2hlZXRTZXF1ZW5jZQ5TZXF1ZW5jZUNoZWNrDkdyb3VwU2VxdWVuY2UJU2V0TWF0Y2gLRW5kT2ZHcm91cBFFbmRPZkdyb3VwRGVtYW5kDUZpcnN0T2ZHcm91cBNGaXJzdE9mR3JvdXBEZW1hbmQHSW5zZXJ0C0FjY3VtdWxhdGUHQWNjSW5zBVN0b3APRGl2ZXJ0Q29udGludWULRGl2ZXJ0U3RvcAhEaXZlcnQxCERpdmVydDIPU2VhbGluZ0NvbnRyb2wPU2VsZWN0aXZlRmVlZDEPU2VsZWN0aXZlRmVlZDIPU2VsZWN0aXZlRmVlZDMPU2VsZWN0aXZlRmVlZDQPU2VsZWN0aXZlRmVlZDUPU2VsZWN0aXZlRmVlZDYPU2VsZWN0aXZlRmVlZDcPU2VsZWN0aXZlRmVlZDgKRW52ZWxvcGUxCkVudmVsb3BlMgpFbnZlbG9wZTMMRXhpdENvbnRyb2wGRXhpdDEGRXhpdDIGRXhpdDMQUHJlc2VudENvbnRpbnVlDFByZXNlbnRTdG9wC0VudmVsb3BlSWQLRG9jdW1lbnRJZAtDdXN0b21lcklkCVBhZ2VOb2ZNC0ZvcmNlZEZvbGQJRnJhbmtlcjEJRnJhbmtlcjIJTWF0Y2hpbmcQR3JvdXBDb3VudEluSm9iEEdyb3VwSW5kZXhJbkpvYg9Gb3JtSW5kZXhJbkpvYhFGb3JtQ291bnRJbkdyb3VwEUZvcm1JbmRleEluR3JvdXAGSm9iSWQMTWFpbHBpZWNlSWQJRW5kT2ZKb2IJSW5rTWFyazEJSW5rTWFyazILSm9nQ29udHJvbBVQcmludE9ubHlPbkZpcnN0UGFnZQhEaXZlcnQzD1NlbGVjdGl2ZUZlZWQ5EFNlbGVjdGl2ZUZlZWQxMBBTZWxlY3RpdmVGZWVkMTEQU2VsZWN0aXZlRmVlZDEyEFNlbGVjdGl2ZUZlZWQxMxBTZWxlY3RpdmVGZWVkMTQQU2VsZWN0aXZlRmVlZDE1DVJlVGltZUdyb3VwMQ1SZVRpbWVHcm91cDINUmVUaW1lR3JvdXAzgIqLjJWWl5iZmpulpqeoqaqrrK2ur7CxsrO0tba3uLm6u7y9vr / AQMBBwELARsBHwEjAScBKwEvATMBNwE7AT8BRwFLAU8BUwFXAVsBXwFjAWcBawFvAlsCXwJjAmcCawJvAnMCdwJ7Anw1TZXF1ZW5jZVR5cGUFRW51bYDlQQVOb25lEEdyb3VwQ291bnRJbkpvYhJTaGVldEluZGV4SW5Hcm91cBJTaGVldENvdW50SW5Hcm91cBBTaGVldEluZGV4SW5Kb2IQR3JvdXBJbmRleEluSm9iE1NoZWV0TnVtYmVySW5Hcm91cBVEb2N1bWVudEluZGV4SW5Hcm91cAhBZGRyZXNzDFVzZXJEZWZpbmVkBkpvYklkDE1haWxwaWVjZUlkC1N0YXJ0T2ZKb2INU3RhcnRPZkdyb3VwC0VuZE9mR3JvdXAJRW5kT2ZKb2KAgYKDhIWGh4iJiouMjY6PDlNlcXVlbmNlT3JkZXIFRW51bRkJCkFzY2VuZGluZwtEZXNjZW5kaW5ngIEPRXZhbHVhdGlvblR5cGUFRW51bV8lBU5vbmUGRXZlcnkGRmlyc3QFTGFzdAxFeGNlcHRGaXJzdAtFeGNlcHRMYXN0E0V4Y2VwdEZpcnN0QW5kTGFzdAtTdGFydE9mSm9iCUVuZE9mSm9igIGCg4SFhoeICUVuY29kaW5nB09iamVjdAoJRW5jb2RpbmcEAAAAAAEUATwAAAABBkl0ZW1zDk9EQkRpY3Rpb25hcnknCVVuc2lnbmVkAAdPYmplY3QVFFVwZGF0ZVByaW50ZXJMaXN0RGIEAAAAAAEVAQMBAAAJHEN1cnJlbnRTZWFyY2hpbmdQcmludGVyTmFtZQdTdHJpbmcAFkF2YWlsYWJsZVByaW50ZXJOYW1lcwVMaXN0CQdTdHJpbmcACkNvbXBhbnlJZAlVbnNpZ25lZAAGRXJyb3IHT2JqZWN0CwpFcnJvckluZm8bVXBkYXRlU2VsZWN0ZWRQcmludGVyc09ubHkFQm9vbAAgVHJ5RmluZEZvcm1hdEZvclZpcnR1YWxQcmludGVycwVCb29sABFQcmludGVyc1RvVXBkYXRlBUxpc3QJB1N0cmluZwALSXNGaW5pc2hlZAVCb29sAA1DcmVhdGlvbkRhdGUJRGF0ZVRpbWUABAAAAAABFgFMFQAAAQdTdGF0ZXMOT0RCRGljdGlvbmFyeZU1BUVudW2VEIMNB05vdFNldBNJbnB1dEZpbGVfVXBsb2FkZWQZSW5wdXRGaWxlX0NvbnZlcnRlZFRvVG5vIklucHV0RmlsZV9Eb2N1bWVudFByb2ZpbGVTZWxlY3RlZBtJbnB1dEZpbGVfRG9jdW1lbnRzQ3JlYXRlZBdJbnB1dEZpbGVfUHJlcHJvY2Vzc2VkE0lucHV0RmlsZV9DYW5jZWxlZBJJbnB1dEZpbGVfRGVsZXRlZBFJbnB1dEZpbGVfRmFpbGVkFFNhbXBsZUZpbGVfVXBsb2FkZWQaU2FtcGxlRmlsZV9Db252ZXJ0ZWRUb1RubxlTYW1wbGVGaWxlX0NvbnZlcnRGYWlsZWQbU2FtcGxlRmlsZV9Db252ZXJ0Q2FuY2VsZWQdU2FtcGxlRmlsZV9UaHVtYm5haWxzQ3JlYXRlZBlTYW1wbGVGaWxlX0NyZWF0ZVByZXZpZXcbU2FtcGxlRmlsZV9QcmV2aWV3RmluaXNoZWQZU2FtcGxlRmlsZV9QcmV2aWV3RmFpbGVkG1NhbXBsZUZpbGVfUHJldmlld0NhbmNlbGVkEURvY3VtZW50X0NyZWF0ZWQWRG9jdW1lbnRfUHJlcHJvY2Vzc2VkJkRvY3VtZW50X0NvbW11bmljYXRpb25Qcm9maWxlQXNzaWduZWQURG9jdW1lbnRfVW5hc3NpZ25lZBBEb2N1bWVudF9GYWlsZWQSRG9jdW1lbnRfQ2FuY2VsZWQRRG9jdW1lbnRfRGVsZXRlZB5Db21tdW5pY2F0aW9uUGllY2VfVW5hc3NpZ25lZCJDb21tdW5pY2F0aW9uUGllY2VfUGVuZGluZ1JlbGVhc2UjQ29tbXVuaWNhdGlvblBpZWNlX1JlbGVhc2VGaW5pc2hlZCdDb21tdW5pY2F0aW9uUGllY2VfUHJvZHVjdGlvbkNvbXBsZXRlZCRDb21tdW5pY2F0aW9uUGllY2VfUHJvZHVjdGlvbkZhaWxlZCZDb21tdW5pY2F0aW9uUGllY2VfUHJvZHVjdGlvbkNhbmNlbGVkIENvbW11bmljYXRpb25QaWVjZV9JblByb2R1Y3Rpb24bQ29tbXVuaWNhdGlvblBpZWNlX0RlbGV0ZWQhQ29tbXVuaWNhdGlvblBpZWNlX1JlbGVhc2VGYWlsZWQjQ29tbXVuaWNhdGlvblBpZWNlX1JlbGVhc2VDYW5jZWxlZCVDb21tdW5pY2F0aW9uUGllY2VfUHJvZHVjdGlvbkNoYW5nZWQiQ29tbXVuaWNhdGlvblBpZWNlX0NvbnRlbnRDaGFuZ2VkJENvbW11bmljYXRpb25Qcm9maWxlX0NvbnRlbnRDaGFuZ2VkDEJhdGNoX1JlYWR5E0JhdGNoX0luUHJvZHVjdGlvbhpCYXRjaF9Qcm9kdWN0aW9uQ29tcGxldGVkF0JhdGNoX1Byb2R1Y3Rpb25GYWlsZWQZQmF0Y2hfUHJvZHVjdGlvbkNhbmNlbGVkDkJhdGNoX0RlbGV0ZWQaUHJvZHVjdGlvbl9Db250ZW50Q2hhbmdlZBNQZGZQcmV2aWV3X1JlcXVlc3QVTGljZW5zZUZpbGVfVXBsb2FkZWQTTGljZW5zZUZpbGVfRmFpbGVkFkxpY2Vuc2VGaWxlX1Byb2Nlc3NlZCFFbGVjdHJvbmljRW5jbG9zdXJlRmlsZV9VcGxvYWRlZCdFbGVjdHJvbmljRW5jbG9zdXJlRmlsZV9Db252ZXJ0ZWRUb1Rubx9FbGVjdHJvbmljRW5jbG9zdXJlRmlsZV9GYWlsZWQgRWxlY3Ryb25pY0VuY2xvc3VyZUZpbGVfRGVsZXRlZBNQcmludGVyTGlzdF9RdWV1ZWQbQ29uZmlndXJhdGlvblNoZWV0X1JlcXVlc3QXQ29tbXVuaWNhdGlvbl9VcGxvYWRlZBxDb21tdW5pY2F0aW9uX1ByZXByb2Nlc3NpbmcbQ29tbXVuaWNhdGlvbl9QcmVwcm9jZXNzZWQWQ29tbXVuaWNhdGlvbl9XYWl0aW5nGENvbW11bmljYXRpb25fU3VibWl0dGVkGENvbW11bmljYXRpb25fU3BsaXR0aW5nGENvbW11bmljYXRpb25fQ29tcGxldGVkFUNvbW11bmljYXRpb25fRmFpbGVkF0NvbW11bmljYXRpb25fQ2FuY2VsZWQZQ29tbXVuaWNhdGlvblpvbmVzX1BhcnNlG0NvbW11bmljYXRpb25ab25lc19FeHRyYWN0HENvbW11bmljYXRpb25QcmV2aWV3X1F1ZXVlZCNDb21tdW5pY2F0aW9uUHJvY2Vzc1ByZXZpZXdfUXVldWVkGFdlYlBvcnRhbENvbnRlbnRfUXVldWVkHFdlYlBvcnRhbENvbnRlbnRfRG93bmxvYWRlZBhXZWJQb3J0YWxDb250ZW50X0ZhaWxlZBpXZWJQb3J0YWxDb250ZW50X0NhbmNlbGVkE091dHB1dEZpbGVfQ3JlYXRlZBpQcmludGVyT3V0cHV0RmlsZV9DcmVhdGVkGEVtYWlsT3V0cHV0RmlsZV9DcmVhdGVkF0VtYWlsT3V0cHV0RmlsZV9GYWlsZWQaU29ydGluZ1JlcG9ydEZpbGVfQ3JlYXRlZBxTYW1wbGVGaWxlX1RodW1ibmFpbHNGYWlsZWQeRG9jdW1lbnRQcm9maWxlUHJldmlld19VcGRhdGUaRW1haWxPdXRwdXRGaWxlX0NvbmZpcm1lZCJFbWFpbE91dHB1dEZpbGVfUmVzdWJtaXRSZXF1ZXN0ZWQRRmluZFRleHRfUmVxdWVzdBZOZXBJbnB1dEZpbGVfVXBsb2FkZWQaTmVwSW5wdXRGaWxlX1ByZXByb2Nlc3NlZBROZXBJbnB1dEZpbGVfRmFpbGVkFU5lcElucHV0RmlsZV9EZWxldGVkE05lcElucHV0RmlsZV9JbkpvYh5OZXBJbnB1dEZpbGVfUHJlcHJvY2Vzc0ZhaWxlZCBOZXBJbnB1dEZpbGVfUHJlcHJvY2Vzc0NhbmNlbGVkEE5lcEpvYl9TdGVwRWRpdCFOZXBJbnB1dEZpbGVfVGh1bWJuYWlsc1JlcXVlc3RlZB5OZXBJbnB1dEZpbGVfUHJldmlld1JlcXVlc3RlZBxOZXBKb2JfUHJvY2Vzc0NvbmZpZ3VyYXRpb24XTmVwQXR0YWNobWVudF9VcGxvYWRlZBtOZXBBdHRhY2htZW50X1ByZXByb2Nlc3NlZB9OZXBBdHRhY2htZW50X1ByZXByb2Nlc3NGYWlsZWQhTmVwQXR0YWNobWVudF9QcmVwcm9jZXNzQ2FuY2VsZWQcUHJvZHVjdGlvbkNsZWFuc2luZ19SdW5uaW5nFk5lcEF0dGFjaG1lbnRfRGVsZXRlZBJBaW1zRmlsZV9VcGxvYWRlZBBBaW1zRmlsZV9GYWlsZWQTQWltc0ZpbGVfUHJvY2Vzc2VkJlNhbXBsZUZpbGVfUHVyZURvY3VtZW50UGFnZXNSZXF1ZXN0ZWQdTWl4ZWRJbnB1dEZpbGVQcmV2aWV3X1VwZGF0ZRBOZXBKb2JfRmluaXNoZWQZTmVwQ29tbXVuaWNhdGlvbl9DcmVhdGVkGk5lcENvbW11bmljYXRpb25fRmluaXNoZWQSTmVwSm9iX1N0ZXBSZXZpZXcPTmVwSm9iX1NlbmRpbmcYTmVwQ29tbXVuaWNhdGlvbl9GYWlsZWQXTmVwUHJpbnRKb2JfTm90UHJpbnRlZBROZXBQcmludEpvYl9QcmludGVkGU5lcENvbW11bmljYXRpb25fU2VuZGluZxpOZXBDb21tdW5pY2F0aW9uX1RyYWNraW5nIk5lcFNhbXBsZUpvYl9Qcm9jZXNzQ29uZmlndXJhdGlvbhhOZXBTYW1wbGVKb2JfU3RlcFJldmlldxdOZXBTYW1wbGVGaWxlX1VwbG9hZGVkG05lcFNhbXBsZUZpbGVfUHJlcHJvY2Vzc2VkH05lcFNhbXBsZUZpbGVfUHJlcHJvY2Vzc0ZhaWxlZCFOZXBTYW1wbGVGaWxlX1ByZXByb2Nlc3NDYW5jZWxlZCJOZXBTYW1wbGVGaWxlX1RodW1ibmFpbHNSZXF1ZXN0ZWQUTmVwU2FtcGxlRmlsZV9JbkpvYhlOZXBDb21tdW5pY2F0aW9uX1dhaXRpbmcgTmVwSm9iX0NvbW11bmljYXRpb25zUHJvY2Vzc2luZxdOZXBKb2JfUHJpbnRQcm9jZXNzaW5nGE5lcEpvYl9Qcm9jZXNzaW5nRmFpbGVkJU5lcENvbW11bmljYXRpb25fV2ViTGlua0VtYWlsU2VuZGluZx1OZXBDb21tdW5pY2F0aW9uX0Rvd25sb2FkaW5nFk5lcFNhbXBsZUpvYl9TdGVwRWRpdBhOZXBGaWxlRG93bmxvYWRfUmVxdWVzdBpOZXBJbnB1dEZpbGVfSm9iQ29tcGxldGVkHE5lcEpvYl9JbnB1dEZpbGVzUHJvY2Vzc2luZyJOZXBBdHRhY2htZW50X1RodW1ibmFpbHNSZXF1ZXN0ZWQaTmVwQ29tbXVuaWNhdGlvbl9QcmludGluZx5OZXBKb2JfU2l6ZUNvbXB1dGluZ1JlcXVlc3RlZBhOZXBTYW1wbGVGaWxlX1ByZXBhcmluZyROZXBDb21tdW5pY2F0aW9uX0JvdW5jZUJhY2tUcmFja2luZyJOZXBDb21tdW5pY2F0aW9uX0JvdW5jZUJhY2tGYWlsZWQdTmVwQ29tbXVuaWNhdGlvbl9NYWlsU2VuZGluZxxNaXNMaWNlbnNlUmVuZXdhbF9SZXF1ZXN0ZWQWTWlzSW5wdXRGaWxlX1VwbG9hZGVkIU1pc0lucHV0RmlsZV9UaHVtYm5haWxzUmVxdWVzdGVkIE1pc0lucHV0RmlsZV9QcmVwcm9jZXNzQ2FuY2VsZWQeTWlzSW5wdXRGaWxlX1ByZXByb2Nlc3NGYWlsZWQVTWlzSW5wdXRGaWxlX0RlbGV0ZWQaTWlzSW5wdXRGaWxlX1ByZXByb2Nlc3NlZB5NaXNJbnB1dEZpbGVfUHJldmlld1JlcXVlc3RlZA9NaXNKb2JfQ3JlYXRlZBFNaXNKb2JfU3RlcElucHV0FE1pc0pvYl9Qcm9jZXNzSW5wdXQXTWlzSm9iX1N0ZXBPcHRpY2FsTWFyaxNNaXNJbnB1dEZpbGVfSW5Kb2IaTWlzSm9iX1Byb2Nlc3NPcHRpY2FsTWFyaxJNaXNKb2JfU3RlcE91dHB1dBVNaXNKb2JfUHJvY2Vzc091dHB1dBFNaXNKb2JfQ29tcGxldGVkIU1pc09wdGljYWxNYXJrR2VuZXJhdGlvbl9SZXF1ZXN0FE1pc1ByaW50Sm9iX0NyZWF0ZWQPTWlzSm9iX0RlbGV0ZWQSTWlzSm9iX1Byb2Nlc3NpbmcRTWlzSm9iX1Byb2Nlc3NlZB9NaXNJbWFnZVpvbmVQcm9jZXNzaW5nX1JlcXVlc3QeQWRkcmVzc0NhcnJpZXJQcmV2aWV3X1JlcXVlc3QXTWlzSW5wdXRGaWxlX1VwbG9hZGluZxRFdmlkZW5jZUZpbGVfRmFpbGVkF0V2aWRlbmNlRmlsZV9Qcm9jZXNzZWQWTWlzUHJpbnRKb2JfUHJvY2Vzc2VkGk1pc1ByaW50Sm9iX1Byb2Nlc3NGYWlsZWQUTWlzUHJpbnRKb2JfUHJpbnRlZBRNaXNQcmludEpvYl9EZWxldGVkFkV2aWRlbmNlRmlsZV9VcGxvYWRlZBRNaXNJbnB1dEZpbGVfT3V0cHV0KE1pc0NvbmZpZ3VyYXRpb25TaGVldEdlbmVyYXRpb25fUmVxdWVzdCBEb2N1bWVudFRlbXBsYXRlUHJldmlld19SZXF1ZXN0Fk1pc1ByaW50ZXJMaXN0X1F1ZXVlZCNQcmludGVyT3V0cHV0RmlsZV9SZXByaW50UmVxdWVzdGVkLFByaW50ZXJPdXRwdXRGaWxlX0NyZWF0ZVRodW1ibmFpbHNSZXF1ZXN0ZWQgTWlzUHJpbnRKb2JQcm9jZXNzaW5nX1JlcXVlc3RlZBVCYXRjaF9Jbk9wdGltaXphdGlvbhVPdmVybGF5RmlsZV9VcGxvYWRlZBZPdmVybGF5RmlsZV9Qcm9jZXNzZWQTT3ZlcmxheUZpbGVfRmFpbGVkIU1pc091dHB1dFByZXByb2Nlc3NpbmdfUmVxdWVzdGVkJk5lcENvbW11bmljYXRpb25fV2VibGlua0VtYWlsVHJhY2tpbmcjTWlzT3V0cHV0UHJldmlld0dlbmVyYXRpb25fUmVxdWVzdCdDb21tdW5pY2F0aW9uUGllY2VfUHJvZHVjdGlvbkRpc2NhcmRlZBxNaXNSZWFkQWxsWm9uZVZhbHVlX1JlcXVlc3QdTWlzSW5wdXRGaWxlX1ByaW50UmVhZHlUb1VzZSNNaXNUcmFuc2FjdGlvbnNSZXBvcnRpbmdfUmVxdWVzdGVkJk1pc1NpbmdsZVByaW50Sm9iUHJvY2Vzc2luZ19SZXF1ZXN0ZWQmTWlzVGh1bWJuYWlsQmF0Y2hHZW5lcmF0aW9uX1JlcXVlc3RlZCJDb21tdW5pY2F0aW9uUGllY2VfSW5PcHRpbWl6YXRpb24aRm9udEZpbGVQcmV2aWV3X1JlcXVlc3RlZBpGb250RmlsZVByZXZpZXdfUHJvY2Vzc2VkF0ZvbnRGaWxlUHJldmlld19GYWlsZWSAgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq + wsbKztLW2t7i5uru8vb6 / wEDAQcBCwEPARMBFwEbAR8BIwEnASsBLwEzATcBOwE / AUMBRwFLAU8BUwFXAVsBXwFjAWcBawFvAXMBdwF7AX8BgwGHAYsBjwGTAZcBmwGfAaMBpwGrAdcB2wHnAesB7wH7AgsCDwITAhcCGwIfAiMCJwIrAjMCNwI7Aj8CQwJHAksCTwJTAlcCWwJfAmMCZwJrAm8CcwKHBLMEtwS7BL8EwwTHBMsE + wT / BQMFBwULBQ8FEwUXBRsFHwUjBScFKwUvBTMFNwU7BT8FQwVHBUsFTwVTBVcFWwVfBWMFZwVrBW8FcwV3BXsFfwWDBYcFkwWjBacFqwWvBbMFtwW7Bb8FwwXHBcgdPYmplY3QXFlByb2Nlc3NhYmxlSXRlbVN0YXRlcwQAAAAAARcBNQAAAAEGSXRlbXMLRGljdGlvbmFyeSMJVW5zaWduZWQAB09iamVjdBEQUHJvY2Vzc2FibGVJdGVtBAAAAAABGAE8AAAAAQZJdGVtcw5PREJEaWN0aW9uYXJ5JwlVbnNpZ25lZAAHT2JqZWN0FRRBaW1zQ29uZmlndXJhdGlvbkRiBAAAAAABGQGuAAAABwpDb21wYW55SWQJVW5zaWduZWQAB1ByZWZpeAdTdHJpbmcAGEFsbG9jYXRpb25GaWxlRXh0ZW5zaW9uB1N0cmluZwAVUmVwcmludEZpbGVFeHRlbnNpb24HU3RyaW5nABNDbG9zZUZpbGVFeHRlbnNpb24HU3RyaW5nABlNYW51YWxDbG9zZUZpbGVFeHRlbnNpb24HU3RyaW5nAAtXYXNDaGFuZ2VkBUJvb2wABAAAAAABGgE + AAAAAwpDb21wYW55SWQJVW5zaWduZWQACEluZGV4ZXMFTGlzdAsJVW5zaWduZWQACVdvcmtlcklkB1N0cmluZwADAAAAAAIBAQAAAAEDAAAAAAIDAQAAAAIDAAAAAAIFAQAAAAMDAAAAAAIHAQAAAAQDAAAAAAIUAQAAAAUDAAAAAAIWAQAAAAYDAAAAAAIYAQAAAAgCAAAAAAMBAAAABwIAAAABAQMAAAABAQACAAAAAQIDAAAAAwEBAgAAAAEDAwAAAAUBAgIAAAABBAMAAAAHAQMCAAAAAQUDAAAAFAEEAgAAAAEGAwAAABYBBQIAAAABBwwAAAAXAX8BAX4aAQF / AAACAAAAAQgDAAAAGAEGAwAAAAIAZA4AAAB / AgEBCEVuZ2xpc2gBAQMAAAACAWUoAAAAfwQBAQZQbGFpbgNBNIGAgD + 0euFHrhR7fwABgABI09vL6L3CoX4AAQMAAAACAWYtAAAAfwQBAQdMZXR0ZXIHTGV0dGVygYOAP7R64UeuFHt / AACAAEjT28vo7YSifgABAwAAAAICZyQAAAB / BgEBDEN1c3RvbWVyIElEAYDAZQAAgIB / AAFI09vL6R3cRIADAAAAAgJoKAAAAH8GAQEQRG9jdW1lbnQgTnVtYmVyAYDAZQAAgIB / AAFI09vL6T60 / IADAAAAAgJpJwAAAH8GAQEPRG9jdW1lbnQgVGl0bGUBgMBlAACAgH8AAUjT28vpPtuFgAMAAAACAmolAAAAfwYBAQ1Ub3RhbCBBbW91bnQBgcBlAACAgH8AAUjT28vpPtuFgAMAAAACA2u8BQAAfwgBAYGAgAhPTVIgU3RkDDEtdHJhY2sgT01SCE5lb3Bvc3R / CQE / jrhR64UeuD98rAgxJul5fwoBP4FocrAgxJw / gWhysCDEnIF + CwE / gm6XjU / fOz8wYk3S8an8P3FTefqX4TN9DAE / gm6XjU / fOz8wpWmxdIGyQAgAAAAAAAB8DQE / OPgeii7Ci4B + Fn0OAYuAC1N0YXJ0IE1hcmuAgIQBgAAAgICAAQAAAAAAAIKAgICAfA4BpYAYRW5kIG9mIEdyb3VwIE1hcmsgKEVPRymAgIQBgAAAgICAAQAAAAAAAIKDgICAew4BpoASRGVtYW5kIGZlZWQgKEVPRymAgIQAgAAAgICAAAAAAAAAAIKFgICAeg4BrIAFU3RvcICAhACAAACAgIAAAQABAAAAgoOAgIB5DgGtgBJEaXZlcnQgJiBDb250aW51ZYCAhACAAACAgIAAAQABAAAAgoOAgIB4DgGugA5EaXZlcnQgJiBTdG9wgICEAIAAAICAgAABAAEAAACCg4CAgHcOAbKAEVNlbGVjdGl2ZSBGZWVkIDGAgIQAgAAAgICAAAEAAAEAAIKDgICAdg4Bs4ARU2VsZWN0aXZlIEZlZWQgMoCAhACAAACAgIAAAQAAAQAAgoOAgIB1DgG0gBFTZWxlY3RpdmUgRmVlZCAzgICEAIAAAICAgAABAAABAACCg4CAgHQOAbWAEVNlbGVjdGl2ZSBGZWVkIDSAgIQAgAAAgICAAAEAAAEAAIKDgICAcw4BtoARU2VsZWN0aXZlIEZlZWQgNYCAhACAAACAgIAAAQAAAQAAgoOAgIByDgG3gBFTZWxlY3RpdmUgRmVlZCA2gICEAIAAAICAgAABAAABAACCg4CAgHEOAbiAEVNlbGVjdGl2ZSBGZWVkIDeAgIQAgAAAgICAAAEAAAEAAIKDgICAcA4BuYARU2VsZWN0aXZlIEZlZWQgOICAhACAAACAgIAAAQAAAQAAgoOAgIBvDgG6gBVFbnZlbG9wZSBTZWxlY3Rpb24gMYCAhACAAACAgIAAAQABAAAAgoOAgIBuDgG9gA1FeGl0IENvbnRyb2yAgIQAgAAAgICAAAEAAQAAAIKDgICAbQ4BwEGAHVByZXNlbnQgb24gRGVjayBhbmQgQ29udGludWWAgIQAgAAAgICAAAEAAQAAAIKDgICAbA4BwEKAGVByZXNlbnQgb24gRGVjayBhbmQgU3RvcICAhACAAACAgIAAAQABAAAAgoOAgIBrDgGxgBBTZWFsaW5nIENvbnRyb2yAgIQAgAAAgICAAAEAAQAAAIKDgICAag4BmX8PAYGAgACAgICAAAAAgAIwgA9TZXF1ZW5jZSBDaGVja4CAgwCAAACDgYMBAAEAAAAAgYCEgIBpDgGVgAdQYXJpdHmAgIQAgAAAgICAAQAAAAAAAIGAgICBaA4BjIAMU2FmZXR5IE1hcmuAgIQBgAAAgICAAQAAAAAAAIKAgICAZwhmEAGBfwV + EQGBrYCAgAB9EQGCroCAgAB8EQGDwEGAgIAAexEBhMBCgICAAHoRAYWsgICAAGUQAYJ / A34RAYGtgICAAH0RAYKugICAAHwRAYOxgICAAGQQAYN / A34RAYGtgICAAH0RAYKugICAAHwRAYO6gICAAGMQAYR / BX4RAYGtgICAAH0RAYKugICAAHwRAYPAQYCAgAB7EQGEwEKAgIAAehEBhb2AgIAAYhABhX8DfhEBgcBBgICAAH0RAYLAQoCAgAB8EQGDsYCAgABhEAGGfwN + EQGBrYCAgAB9EQGCpYCAgAB8EQGDpoCAgABgEAGHfwN + EQGBroCAgAB9EQGCpYCAgAB8EQGDpoCAgABfEAGIfwN + EQGBrICAgAB9EQGCpYCAgAB8EQGDpoCAgABeAV0SAYGBgIF / An4RAYGlgICAAH0RAYKmgICAAAABSNPby + lLENdcAAMAAAACA2ygBwAAfwgBAYKAgAhPTVIgQWR2DDEtdHJhY2sgT01SBE5UTH8JAT + OuFHrhR64P3ysCDEm6Xl / CgE / gWhysCDEnD + BaHKwIMScgX4LAT + CbpeNT987PzBiTdLxqfw / cVN5 + pfhM30MAT + CbpeNT987PzClabF0gbJACAAAAAAAAHwNAT84 + B6KLsKLgH4YfQ4Bi4AKR2F0ZSBNYXJrgICEAYAAAICAgAEAAAAAAACCgICAgHwOAaWAFUluc2VydCBvbiBNYXJrIChFT0cpgICEAYAAAICAgAAAAAAAAACCg4CAgHsOAaaAFkluc2VydCBvbiBTcGFjZSAoRU9HKYCAhAGAAACAgIAAAAAAAAAAgoWAgIB6DgGngBpGaXJzdCBvZiBHcm91cCBNYXJrIChGT0cpgICEAYAAAICAgAAAAAAAAACCgoCAgHkOAaiAEkRlbWFuZCBmZWVkIChGT0cpgICEAYAAAICAgAAAAAAAAACChICAgHgOAayABVN0b3CAgIQAgAAAgICAAAEAAQAAAIKDgICAdw4BrYASRGl2ZXJ0ICYgQ29udGludWWAgIQAgAAAgICAAAEAAQAAAIKBgICAdg4BroAORGl2ZXJ0ICYgU3RvcICAhACAAACAgIAAAQABAAAAgoGAgIB1DgGygBFTZWxlY3RpdmUgRmVlZCAxgICEAIAAAICAgAABAAABAACCg4CAgHQOAbOAEVNlbGVjdGl2ZSBGZWVkIDKAgIQAgAAAgICAAAEAAAEAAIKDgICAcw4BtIARU2VsZWN0aXZlIEZlZWQgM4CAhACAAACAgIAAAQAAAQAAgoOAgIByDgG1gBFTZWxlY3RpdmUgRmVlZCA0gICEAIAAAICAgAABAAABAACCg4CAgHEOAbaAEVNlbGVjdGl2ZSBGZWVkIDWAgIQAgAAAgICAAAEAAAEAAIKDgICAcA4Bt4ARU2VsZWN0aXZlIEZlZWQgNoCAhACAAACAgIAAAQAAAQAAgoOAgIBvDgG4gBFTZWxlY3RpdmUgRmVlZCA3gICEAIAAAICAgAABAAABAACCg4CAgG4OAbmAEVNlbGVjdGl2ZSBGZWVkIDiAgIQAgAAAgICAAAEAAAEAAIKDgICAbQ4BuoAVRW52ZWxvcGUgU2VsZWN0aW9uIDGAgIQAgAAAgICAAAEAAQAAAIKDgICAbA4BvYANRXhpdCBDb250cm9sgICEAIAAAICAgAABAAEAAACCg4CAgGsOAcBBgB1QcmVzZW50IG9uIERlY2sgYW5kIENvbnRpbnVlgICEAIAAAICAgAABAAEAAACCg4CAgGoOAcBCgBlQcmVzZW50IG9uIERlY2sgYW5kIFN0b3CAgIQAgAAAgICAAAEAAQAAAIKDgICAaQ4BsYAQU2VhbGluZyBDb250cm9sgICEAIAAAICAgAABAAEAAACCg4CAgGgOAZl / DwGBgIAAgICAgAAAAIACMIAPU2VxdWVuY2UgQ2hlY2uAgIMAgAAAg4GFAAEBAAAAAIGAhICAZw4BlYAHUGFyaXR5gICEAIAAAICAgAABAAAAAACBgICAgWYOAYyADFNhZmV0eSBNYXJrgICEAYAAAICAgAEAAAAAAACCgICAgGUWZBABgX8FfhEBga2AgIAAfREBgq6AgIAAfBEBg8BBgICAAHsRAYTAQoCAgAB6EQGFrICAgABjEAGCfwV + EQGBrYCAgAB9EQGCroCAgAB8EQGDwEGAgIAAexEBhMBCgICAAHoRAYWxgICAAGIQAYN / A34RAYGtgICAAH0RAYKugICAAHwRAYO6gICAAGEQAYR / BX4RAYGtgICAAH0RAYKugICAAHwRAYPAQYCAgAB7EQGEwEKAgIAAehEBhb2AgIAAYBABhX8FfhEBga2AgIAAfREBgqWAgIAAfBEBg6aAgIAAexEBhKeAgIAAehEBhaiAgIAAXxABhn8FfhEBga6AgIAAfREBgqWAgIAAfBEBg6aAgIAAexEBhKeAgIAAehEBhaiAgIAAXhABh38CfhEBga2AgIAAfREBgrKAgIAAXRABiH8CfhEBga2AgIAAfREBgrOAgIAAXBABiX8CfhEBga2AgIAAfREBgrSAgIAAWxABin8CfhEBga2AgIAAfREBgrWAgIAAWhABi38CfhEBga2AgIAAfREBgraAgIAAWRABjH8CfhEBga2AgIAAfREBgreAgIAAWBABjX8CfhEBga2AgIAAfREBgriAgIAAVxABjn8CfhEBga2AgIAAfREBgrmAgIAAVhABj38CfhEBga6AgIAAfREBgrKAgIAAVRABkH8CfhEBga6AgIAAfREBgrOAgIAAVBABkX8CfhEBga6AgIAAfREBgrSAgIAAUxABkn8CfhEBga6AgIAAfREBgrWAgIAAUhABk38CfhEBga6AgIAAfREBgraAgIAAURABlH8CfhEBga6AgIAAfREBgreAgIAAUBABlX8CfhEBga6AgIAAfREBgriAgIAATxABln8CfhEBga6AgIAAfREBgrmAgIAATgFNEgGAgYCBfwR + EQGBp4CAgAB9EQGCqICAgAB8EQGDpYCAgAB7EQGEpoCAgAAAAUjT28vpzFZgTAADAAAAAgNtwgYAAH8IAQGDgYAIQ29kZSAzOQcxRCBCQ1IITmVvcG9zdH8JAT + OuFHrhR64P3ysCDEm6Xl / CgE / gWhysCDEnD + BaHKwIMScgX4LAT + CbpeNT987PzBiTdLxqfw / cVN5 + pfhM30MAT + CbpeNT987PzClabF0gbJACAAAAAAAAHwNAT84 + B6KLsKLgH4YfQ4BwEl / DwGAgIAAgICAgAAAAYAAgAxQYWdlIE4gb2YgTX4CfRMBl4aAgIB8EwGWg4CAgICDAIAAAIKBggEAAQAAAACBgICAgHwOAZiAD1NoZWV0IFNlcXVlbmNlgICDAIAAAIOBgwAAAQAAAACBgISAgHsOAZqAD0dyb3VwIFNlcXVlbmNlgICDAIAAAIOBgwAAAQAAAACBgIWAgHoOAal / DwGChIAAgICAgAAAAIAAgAdJbnNlcnSAgIQAgAAAgICAAAAAAAAAAIKDgICAeQ4Bqn8PAYKEgACAgICAAAAAgACAC0FjY3VtdWxhdGWAgIQAgAAAgICAAAAAAAAAAIKFgICAeA4Br38PAYKEgACAgICAAAAAgACACURpdmVydCAxgICEAIAAAICAgAABAAEAAACCg4CAgHcOAbB / DwGChIAAgICAgAAAAIAAgAlEaXZlcnQgMoCAhACAAACAgIAAAQABAAAAgoOAgIB2DgGyfw8BgoSAAICAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgMYCAhACAAACAgIAAAQAAAQAAgoOAgIB1DgGzfw8BgoSAAICAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgMoCAhACAAACAgIAAAQAAAQAAgoOAgIB0DgG0fw8BgoSAAICAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgM4CAhACAAACAgIAAAQAAAQAAgoOAgIBzDgG1fw8BgoSAAICAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgNICAhACAAACAgIAAAQAAAQAAgoOAgIByDgG2fw8BgoSAAICAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgNYCAhACAAACAgIAAAQAAAQAAgoOAgIBxDgG3fw8BgoSAAICAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgNoCAhACAAACAgIAAAQAAAQAAgoOAgIBwDgG4fw8BgoSAAICAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgN4CAhACAAACAgIAAAQAAAQAAgoOAgIBvDgG5fw8BgoSAAICAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgOICAhACAAACAgIAAAQAAAQAAgoOAgIBuDgGxfw8BgoSAAICAgIAAAACAAIAQU2VhbGluZyBDb250cm9sgICEAIAAAICAgAABAAEAAACCg4CAgG0OAbp / DwGChIAAgICAgAAAAIAAgBVFbnZlbG9wZSBTZWxlY3Rpb24gMYCAhACAAACAgIAAAQABAAAAgoOAgIBsDgG7fw8BgoSAAICAgIAAAACAAIAVRW52ZWxvcGUgU2VsZWN0aW9uIDKAgIQAgAAAgICAAAEAAQAAAIKDgICAaw4BvH8PAYKEgACAgICAAAAAgACAFUVudmVsb3BlIFNlbGVjdGlvbiAzgICEAIAAAICAgAABAAEAAACCg4CAgGoOAb5 / DwGChIAAgICAgAAAAIAAgBFFeGl0IFNlbGVjdGlvbiAxgICEAIAAAICAgAABAAEAAACCg4CAgGkOAb9 / DwGChIAAgICAgAAAAIAAgBFFeGl0IFNlbGVjdGlvbiAygICEAIAAAICAgAABAAEAAACCg4CAgGgOAcBAfw8BgoSAAICAgIAAAACAAIARRXhpdCBTZWxlY3Rpb24gM4CAhACAAACAgIAAAQABAAAAgoOAgIBnDgGsfw8BgoSAAICAgIAAAACAAIAFU3RvcICAhACAAACAgIAAAQABAAAAgoOAgIBmDgHASH8PAYCAgACAgICAGVteQS1aYS16MC05IFwtXC5cJFwvXCslXQItAIAAgAxDdXN0b21lciBJRICAgQCAAACGgZAAAQEAAAAAgoCAgIBlBGQQAYF / A34RAYG6gICAAH0RAYK7gICAAHwRAYO8gICAAGMQAYJ / A34RAYG + gICAAH0RAYK / gICAAHwRAYPAQICAgABiEAGDfwN + EQGBr4CAgAB9EQGCsICAgAB8EQGDqYCAgABhEAGEfwJ + EQGBrICAgAB9EQGCqYCAgABgAV8SAYGAgIF / BH4RAYHASYCAgAB9EQGCqYCAgAB8EQGDqoCAgAB7EQGEwEiAgIAAAAFI09vL6hTzcl4AAwAAAAIDbnYNAAB / CAEBhIGACE5UTCBCQ1IHMUQgQkNSFE5UTCAiQWxwaGEiIEJhcmNvZGV / CQE / jrhR64UeuD98rAgxJul5fwoBP4FocrAgxJw / gWhysCDEnIF + CwE / gm6XjU / fOz8wYk3S8an8P3FTefqX4TN9DAE / gm6XjU / fOz8wpWmxdIGyQAgAAAAAAAB8DQE / OPgeii7Ci4B + KH0OAcBUfw8BgICAAICAgIAZW15BLVphLXowLTkgXC1cLlwkXC9cKyVdAi0AgAIwgQdKb2IgSUSAgIEAgAAAioqKAAEAAAABAYGAioCAfA4BwFV / DwGAgIAAgICAgBlbXkEtWmEtejAtOSBcLVwuXCRcL1wrJV0CLQCAAjCBDU1haWxwaWVjZSBJRICAgQCAAACKiooAAQAAAAABgYCLgIB7DgGlfw8Bh + AnD4AHQkNTIzFBgoCAgAAAAIAAgBtEZW1hbmQgZmVlZCAmIEVuZCBvZiBHcm91cICAhACAAACAgIABAAAAAAAAgoOAgIB6DgGnfw8Bh + AnD4AHQkNTIzFBgoCAgAAAAIAAgB1EZW1hbmQgZmVlZCAmIEZpcnN0IG9mIEdyb3VwgICEAIAAAICAgAAAAAAAAACCgoCAgHkOAcBWgClEZW1hbmQgZmVlZCwgRW5kIG9mIEdyb3VwIGFuZCBFbmQgb2YgSm9ifwJ + EwGlgICDfw8Bh + AnD4AHQkNTIzFBgoCAgAAAAIAAgH0TAcBWgICIfw8Bh + AnD4AHQkNTIzFBhICAgAAAAIAAgICEAIAAAICAgAAAAAAAAACCgICAgHgOAcBKfw8Bh + AnD4AHQkNTIzFBiICAgAAAAIAAgAxGb3JjZWQgRm9sZICAhACAAACAgIAAAQABAAAAgoGAgIB3DgHAWn8PAYaAgACAf4CAAAAAgACAJVBhZ2UgY291bnQsIFByaW50IG9ubHkgb24gZmlyc3QgcGFnZX4CfRMBwFqAgICAfBMBloCAgICAgwCAAACAgIAAAAAAAAAAgYCDgIB2DgGyfw8BhOAnD4AGQkNTIzKBgICAAAAAgACAEVNlbGVjdGl2ZSBGZWVkIDGAgIQAgAAAgICAAAEAAAEAAIKBgICAdQ4Bs38PAYTgJw + ABkJDUyMygoCAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCAygICEAIAAAICAgAABAAABAACCgYCAgHQOAbR / DwGE4CcPgAZCQ1MjMoSAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgM4CAhACAAACAgIAAAQAAAQAAgoGAgIBzDgG1fw8BhOAnD4AGQkNTIzKIgICAAAAAgACAEVNlbGVjdGl2ZSBGZWVkIDSAgIQAgAAAgICAAAEAAAEAAIKBgICAcg4Btn8PAYTgJw + ABkJDUyMzgYCAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA1gICEAIAAAICAgAABAAABAACCgYCAgHEOAbd / DwGE4CcPgAZCQ1MjM4KAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgNoCAhACAAACAgIAAAQAAAQAAgoGAgIBwDgG4fw8BhOAnD4AGQkNTIzOEgICAAAAAgACAEVNlbGVjdGl2ZSBGZWVkIDeAgIQAgAAAgICAAAEAAAEAAIKBgICAbw4BuX8PAYTgJw + ABkJDUyMziICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA4gICEAIAAAICAgAABAAABAACCgYCAgG4OAcCWfw8BhOAnD4AGQkNTIzSBgICAAAAAgACAEVNlbGVjdGl2ZSBGZWVkIDmAgIQAgAAAgICAAAEAAAEAAIKBgICAbQ4BwJd / DwGE4CcPgAZCQ1MjNIKAgIAAAACAAIASU2VsZWN0aXZlIEZlZWQgMTCAgIQAgAAAgICAAAEAAAEAAIKBgICAbA4BwJh / DwGE4CcPgAZCQ1MjNISAgIAAAACAAIASU2VsZWN0aXZlIEZlZWQgMTGAgIQAgAAAgICAAAEAAAEAAIKBgICAaw4BwJl / DwGE4CcPgAZCQ1MjNIiAgIAAAACAAIASU2VsZWN0aXZlIEZlZWQgMTKAgIQAgAAAgICAAAEAAAEAAIKBgICAag4BwJp / DwGE4CcPgAZCQ1MjNYGAgIAAAACAAIASU2VsZWN0aXZlIEZlZWQgMTOAgIQAgAAAgICAAAEAAAEAAIKBgICAaQ4BwJt / DwGE4CcPgAZCQ1MjNYKAgIAAAACAAIASU2VsZWN0aXZlIEZlZWQgMTSAgIQAgAAAgICAAAEAAAEAAIKBgICAaA4BwJx / DwGE4CcPgAZCQ1MjNYSAgIAAAACAAIASU2VsZWN0aXZlIEZlZWQgMTWAgIQAgAAAgICAAAEAAAEAAIKBgICAZw4Br38PAYXgJw + ABkJDUyM2wICAgIAAAACAAIAJRGl2ZXJ0IDGAgIQAgAAAgICAAAEAAQAAAIKBgICAZg4BsH8PAYXgJw + ABkJDUyM2wQCAgIAAAACAAIAJRGl2ZXJ0IDKAgIQAgAAAgICAAAEAAQAAAIKBgICAZQ4BwFt / DwGF4CcPgAZCQ1MjNsIAgICAAAAAgACAEERpdmVydCAzIChGRlBEKYCAhACAAACAgIAAAQABAAAAgoGAgIBkDgHAV38PAYXgJw + ABkJDUyM2gYCAgAAAAIAAgAlJbmsgTWFya4CAhACAAACAgIAAAQABAAAAgoGAgIBjDgHAWH8PAYXgJw + ABkJDUyM2goCAgAAAAIAAgAtJbmsgTWFyayAygICEAIAAAICAgAABAAEAAACCgYCAgGIOAbp / DwGF4CcPgAZCQ1MjNoSAgIAAAACAAIASRW52ZWxvcGUgRGl2ZXJ0IDGAgIQAgAAAgICAAAEAAQAAAIKBgICAYQ4Bu38PAYXgJw + ABkJDUyM2iICAgAAAAIAAgBJFbnZlbG9wZSBEaXZlcnQgMoCAhACAAACAgIAAAQABAAAAgoGAgIBgDgG8fw8BheAnD4AGQkNTIzaQgICAAAAAgACAEkVudmVsb3BlIERpdmVydCAzgICEAIAAAICAgAABAAEAAACCgYCAgF8OAcBZfw8BheAnD4AGQkNTIzaggICAAAAAgACADEpvZyBDb250cm9sgICEAIAAAICAgAABAAEAAACCgYCAgF4OAbF / DwGF4CcPgAZCQ1MjNsBAgICAAAAAgACAEFNlYWxpbmcgQ29udHJvbICAhACAAACAgIAAAQABAAAAgoGAgIBdDgHAUX8PAYSAgACAgICAAAAAgACAG0Zvcm0gc2VxdWVuY2Ugd2l0aGluIGEgSm9igICDAIAAAICAgAAAAAAAAACBgISAgFwOAcBTfw8BhICAAICAgIAAAACAAIAdRm9ybSBzZXF1ZW5jZSB3aXRoaW4gYSBHcm91cICAgwCAAACAgIAAAAAAAAAAgYCCgIBbDgHATX8PAYSAgACAgICAAAAAgACACU1hdGNoaW5ngICDAIAAAICAgAAAAAAAAACBgIWAgFoOAcBPfw8BhICAAICAgIAAAACAAIAPR3JvdXAgU2VxdWVuY2WAgIMAgAAAgICAAAAAAAAAAIGAhYCAWQ4BwFJ / DwGIgIAAgICAgAAAAIAAgCFUb3RhbCBudW1iZXIgb2YgRm9ybXMgaW4gYSBHcm91cICAgwCAAACAgIAAAAAAAAAAgYCDgIBYDgHAS38PAYSAgACAgICAAAAAgACACkZyYW5rZXIgMYCAhACAAACAgIAAAQABAAAAgoGAgIBXDgHATH8PAYSAgACAgICAAAAAgACACkZyYW5rZXIgMoCAhACAAACAgIAAAQABAAAAgoGAgIBWDgHASH8PAYCAgACAgICAGVteQS1aYS16MC05IFwtXC5cJFwvXCslXQItAIAAgAxDdXN0b21lciBJRICAgQCAAACGgZkAAQEAAAAAgoCAgIBVAlQQAYF / A34RAYGvgICAAH0RAYKwgICAAHwRAYPAW4CAgABTEAGCfwN + EQGBuoCAgAB9EQGCu4CAgAB8EQGDvICAgABSC1ESAYGAgIF / BX4RAYHAWoCAgAB9EQGCpYCAgAB8EQGDp4CAgAB7EQGEwFaAgIAAehEBhcBIgICAAFASAYCBgIJ / An4RAYHAWoCAgAB9EQGCpYCAgABPEgGAgYCDfwJ + EQGBwFqAgIAAfREBgqeAgIAAThIBgIGAhH8CfhEBgcBagICAAH0RAYLAVoCAgABNEgGAgYCFfwJ + EQGBpYCAgAB9EQGCwFaAgIAATBIBgIGAhn8CfhEBgaWAgIAAfREBgqeAgIAASxIBgIGAh38CfhEBgaeAgIAAfREBgsBWgICAAEoSAYCBgIh / An4RAYHAWoCAgAB9EQGCwEqAgIAASRIBgYB / BLa3uLmJfgR9EQGBsoCAgAB8EQGCs4CAgAB7EQGDtICAgAB6EQGEtYCAgABIEgGBgH8EwJbAl8CYwJmKfgR9EQGBtoCAgAB8EQGCt4CAgAB7EQGDuICAgAB6EQGEuYCAgABHEgGBgH8DwJrAm8Cci34EfREBgcCWgICAAHwRAYLAl4CAgAB7EQGDwJiAgIAAehEBhMCZgICAAAEBSNPby + oXsg9GAAMAAAACA2 / FBgAAfwgBAYWCgAtEYXRhTWF0cml4BzJEIEJDUghOZW9wb3N0fwkBP464UeuFHrg / fKwIMSbpeX8KAT + BaHKwIMScP4FocrAgxJyBfgsBP4Jul41P3zs / MGJN0vGp / D9xU3n6l + EzfQwBP4Jul41P3zs / MKVpsXSBskAIAAAAAAAAfA0BPzj4HoouwouAfhh9DgHASX8PAYCAgACAgICAAAABgACADFBhZ2UgTiBvZiBNfgJ9EwGXhoCAgHwTAZaDgICAgIMAgAAAgoGCAQABAAAAAIGAgICAfA4BmIAPU2hlZXQgU2VxdWVuY2WAgIMAgAAAg4GDAAABAAAAAIGAhICAew4BmoAPR3JvdXAgU2VxdWVuY2WAgIMAgAAAg4GDAAABAAAAAIGAhYCAeg4BqX8PAYKEgACAgICAAAAAgACAB0luc2VydICAhACAAACAgIAAAAAAAAAAgoOAgIB5DgGqfw8BgoSAAICAgIAAAACAAIALQWNjdW11bGF0ZYCAhACAAACAgIAAAAAAAAAAgoWAgIB4DgGvfw8BgoSAAICAgIAAAACAAIAJRGl2ZXJ0IDGAgIQAgAAAgICAAAEAAQAAAIKDgICAdw4BsH8PAYKEgACAgICAAAAAgACACURpdmVydCAygICEAIAAAICAgAABAAEAAACCg4CAgHYOAbJ / DwGChIAAgICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCAxgICEAIAAAICAgAABAAABAACCg4CAgHUOAbN / DwGChIAAgICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCAygICEAIAAAICAgAABAAABAACCg4CAgHQOAbR / DwGChIAAgICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCAzgICEAIAAAICAgAABAAABAACCg4CAgHMOAbV / DwGChIAAgICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA0gICEAIAAAICAgAABAAABAACCg4CAgHIOAbZ / DwGChIAAgICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA1gICEAIAAAICAgAABAAABAACCg4CAgHEOAbd / DwGChIAAgICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA2gICEAIAAAICAgAABAAABAACCg4CAgHAOAbh / DwGChIAAgICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA3gICEAIAAAICAgAABAAABAACCg4CAgG8OAbl / DwGChIAAgICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA4gICEAIAAAICAgAABAAABAACCg4CAgG4OAbF / DwGChIAAgICAgAAAAIAAgBBTZWFsaW5nIENvbnRyb2yAgIQAgAAAgICAAAEAAQAAAIKDgICAbQ4Bun8PAYKEgACAgICAAAAAgACAFUVudmVsb3BlIFNlbGVjdGlvbiAxgICEAIAAAICAgAABAAEAAACCg4CAgGwOAbt / DwGChIAAgICAgAAAAIAAgBVFbnZlbG9wZSBTZWxlY3Rpb24gMoCAhACAAACAgIAAAQABAAAAgoOAgIBrDgG8fw8BgoSAAICAgIAAAACAAIAVRW52ZWxvcGUgU2VsZWN0aW9uIDOAgIQAgAAAgICAAAEAAQAAAIKDgICAag4Bvn8PAYKEgACAgICAAAAAgACAEUV4aXQgU2VsZWN0aW9uIDGAgIQAgAAAgICAAAEAAQAAAIKDgICAaQ4Bv38PAYKEgACAgICAAAAAgACAEUV4aXQgU2VsZWN0aW9uIDKAgIQAgAAAgICAAAEAAQAAAIKDgICAaA4BwEB / DwGChIAAgICAgAAAAIAAgBFFeGl0IFNlbGVjdGlvbiAzgICEAIAAAICAgAABAAEAAACCg4CAgGcOAax / DwGChIAAgICAgAAAAIAAgAVTdG9wgICEAIAAAICAgAABAAEAAACCg4CAgGYOAcBIfw8BgICAAICAgIAZW15BLVphLXowLTkgXC1cLlwkXC9cKyVdAi0AgACADEN1c3RvbWVyIElEgICBAIAAAIaBmQABAQAAAACCgICAgGUEZBABgX8DfhEBgbqAgIAAfREBgruAgIAAfBEBg7yAgIAAYxABgn8DfhEBgb6AgIAAfREBgr + AgIAAfBEBg8BAgICAAGIQAYN / A34RAYGvgICAAH0RAYKwgICAAHwRAYOpgICAAGEQAYR / An4RAYGsgICAAH0RAYKpgICAAGABXxIBgYCAgX8EfhEBgcBJgICAAH0RAYKpgICAAHwRAYOqgICAAHsRAYTASICAgAAAAUjT28vqHOIvXgADAAAAAgNwfg0AAH8IAQGGgoANTlRMIDJETWF0cml4BzJEIEJDUhdOVEwgIkFscGhhIiBEYXRhTWF0cml4fwkBP464UeuFHrg / fKwIMSbpeX8KAT + BaHKwIMScP4FocrAgxJyBfgsBP4Jul41P3zs / MGJN0vGp / D9xU3n6l + EzfQwBP4Jul41P3zs / MKVpsXSBskAIAAAAAAAAfA0BPzj4HoouwouAfih9DgHAVH8PAYCAgACAgICAGVteQS1aYS16MC05IFwtXC5cJFwvXCslXQItAIACMIEHSm9iIElEgICBAIAAAIqKigABAAAAAQGBgIqAgHwOAcBVfw8BgICAAICAgIAZW15BLVphLXowLTkgXC1cLlwkXC9cKyVdAi0AgAIwgQ1NYWlscGllY2UgSUSAgIEAgAAAioqKAAEAAAAAAYGAi4CAew4BpX8PAYfgJw + AB0JDUyMxQYKAgIAAAACAAIAbRGVtYW5kIGZlZWQgJiBFbmQgb2YgR3JvdXCAgIQAgAAAgICAAQAAAAAAAIKDgICAeg4Bp38PAYfgJw + AB0JDUyMxQYKAgIAAAACAAIAdRGVtYW5kIGZlZWQgJiBGaXJzdCBvZiBHcm91cICAhACAAACAgIAAAAAAAAAAgoKAgIB5DgHAVoApRGVtYW5kIGZlZWQsIEVuZCBvZiBHcm91cCBhbmQgRW5kIG9mIEpvYn8CfhMBpYCAg38PAYfgJw + AB0JDUyMxQYKAgIAAAACAAIB9EwHAVoCAiH8PAYfgJw + AB0JDUyMxQYSAgIAAAACAAICAhACAAACAgIAAAAAAAAAAgoCAgIB4DgHASn8PAYfgJw + AB0JDUyMxQYiAgIAAAACAAIAMRm9yY2VkIEZvbGSAgIQAgAAAgICAAAEAAQAAAIKBgICAdw4BwFp / DwGGgIAAgH + AgAAAAIAAgCVQYWdlIGNvdW50LCBQcmludCBvbmx5IG9uIGZpcnN0IHBhZ2V + An0TAcBagICAgHwTAZaAgICAgIMAgAAAgICAAAAAAAAAAIGAg4CAdg4Bsn8PAYTgJw + ABkJDUyMygYCAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCAxgICEAIAAAICAgAABAAABAACCgYCAgHUOAbN / DwGE4CcPgAZCQ1MjMoKAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgMoCAhACAAACAgIAAAQAAAQAAgoGAgIB0DgG0fw8BhOAnD4AGQkNTIzKEgICAAAAAgACAEVNlbGVjdGl2ZSBGZWVkIDOAgIQAgAAAgICAAAEAAAEAAIKBgICAcw4BtX8PAYTgJw + ABkJDUyMyiICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA0gICEAIAAAICAgAABAAABAACCgYCAgHIOAbZ / DwGE4CcPgAZCQ1MjM4GAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgNYCAhACAAACAgIAAAQAAAQAAgoGAgIBxDgG3fw8BhOAnD4AGQkNTIzOCgICAAAAAgACAEVNlbGVjdGl2ZSBGZWVkIDaAgIQAgAAAgICAAAEAAAEAAIKBgICAcA4BuH8PAYTgJw + ABkJDUyMzhICAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA3gICEAIAAAICAgAABAAABAACCgYCAgG8OAbl / DwGE4CcPgAZCQ1MjM4iAgIAAAACAAIARU2VsZWN0aXZlIEZlZWQgOICAhACAAACAgIAAAQAAAQAAgoGAgIBuDgHAln8PAYTgJw + ABkJDUyM0gYCAgAAAAIAAgBFTZWxlY3RpdmUgRmVlZCA5gICEAIAAAICAgAABAAABAACCgYCAgG0OAcCXfw8BhOAnD4AGQkNTIzSCgICAAAAAgACAElNlbGVjdGl2ZSBGZWVkIDEwgICEAIAAAICAgAABAAABAACCgYCAgGwOAcCYfw8BhOAnD4AGQkNTIzSEgICAAAAAgACAElNlbGVjdGl2ZSBGZWVkIDExgICEAIAAAICAgAABAAABAACCgYCAgGsOAcCZfw8BhOAnD4AGQkNTIzSIgICAAAAAgACAElNlbGVjdGl2ZSBGZWVkIDEygICEAIAAAICAgAABAAABAACCgYCAgGoOAcCafw8BhOAnD4AGQkNTIzWBgICAAAAAgACAElNlbGVjdGl2ZSBGZWVkIDEzgICEAIAAAICAgAABAAABAACCgYCAgGkOAcCbfw8BhOAnD4AGQkNTIzWCgICAAAAAgACAElNlbGVjdGl2ZSBGZWVkIDE0gICEAIAAAICAgAABAAABAACCgYCAgGgOAcCcfw8BhOAnD4AGQkNTIzWEgICAAAAAgACAElNlbGVjdGl2ZSBGZWVkIDE1gICEAIAAAICAgAABAAABAACCgYCAgGcOAa9 / DwGF4CcPgAZCQ1MjNsCAgICAAAAAgACACURpdmVydCAxgICEAIAAAICAgAABAAEAAACCgYCAgGYOAbB / DwGF4CcPgAZCQ1MjNsEAgICAAAAAgACACURpdmVydCAygICEAIAAAICAgAABAAEAAACCgYCAgGUOAcBbfw8BheAnD4AGQkNTIzbCAICAgAAAAIAAgBBEaXZlcnQgMyAoRkZQRCmAgIQAgAAAgICAAAEAAQAAAIKBgICAZA4BwFd / DwGF4CcPgAZCQ1MjNoGAgIAAAACAAIAJSW5rIE1hcmuAgIQAgAAAgICAAAEAAQAAAIKBgICAYw4BwFh / DwGF4CcPgAZCQ1MjNoKAgIAAAACAAIALSW5rIE1hcmsgMoCAhACAAACAgIAAAQABAAAAgoGAgIBiDgG6fw8BheAnD4AGQkNTIzaEgICAAAAAgACAEkVudmVsb3BlIERpdmVydCAxgICEAIAAAICAgAABAAEAAACCgYCAgGEOAbt / DwGF4CcPgAZCQ1MjNoiAgIAAAACAAIASRW52ZWxvcGUgRGl2ZXJ0IDKAgIQAgAAAgICAAAEAAQAAAIKBgICAYA4BvH8PAYXgJw + ABkJDUyM2kICAgAAAAIAAgBJFbnZlbG9wZSBEaXZlcnQgM4CAhACAAACAgIAAAQABAAAAgoGAgIBfDgHAWX8PAYXgJw + ABkJDUyM2oICAgAAAAIAAgAxKb2cgQ29udHJvbICAhACAAACAgIAAAQABAAAAgoGAgIBeDgGxfw8BheAnD4AGQkNTIzbAQICAgAAAAIAAgBBTZWFsaW5nIENvbnRyb2yAgIQAgAAAgICAAAEAAQAAAIKBgICAXQ4BwFF / DwGEgIAAgICAgAAAAIAAgBtGb3JtIHNlcXVlbmNlIHdpdGhpbiBhIEpvYoCAgwCAAACAgIAAAAAAAAAAgYCEgIBcDgHAU38PAYSAgACAgICAAAAAgACAHUZvcm0gc2VxdWVuY2Ugd2l0aGluIGEgR3JvdXCAgIMAgAAAgICAAAAAAAAAAIGAgoCAWw4BwE1 / DwGEgIAAgICAgAAAAIAAgAlNYXRjaGluZ4CAgwCAAACAgIAAAAAAAAAAgYCFgIBaDgHAT38PAYSAgACAgICAAAAAgACAD0dyb3VwIFNlcXVlbmNlgICDAIAAAICAgAAAAAAAAACBgIWAgFkOAcBSfw8BiICAAICAgIAAAACAAIAhVG90YWwgbnVtYmVyIG9mIEZvcm1zIGluIGEgR3JvdXCAgIMAgAAAgICAAAAAAAAAAIGAg4CAWA4BwEt / DwGEgIAAgICAgAAAAIAAgApGcmFua2VyIDGAgIQAgAAAgICAAAEAAQAAAIKBgICAVw4BwEx / DwGEgIAAgICAgAAAAIAAgApGcmFua2VyIDKAgIQAgAAAgICAAAEAAQAAAIKBgICAVg4BwEh / DwGAgIAAgICAgBlbXkEtWmEtejAtOSBcLVwuXCRcL1wrJV0CLQCAAIAMQ3VzdG9tZXIgSUSAgIEAgAAAhoGZAAEBAAAAAIKAgICAVQJUEAGBfwN + EQGBr4CAgAB9EQGCsICAgAB8EQGDwFuAgIAAUxABgn8DfhEBgbqAgIAAfREBgruAgIAAfBEBg7yAgIAAUgtREgGBgICBfwV + EQGBwFqAgIAAfREBgqWAgIAAfBEBg6eAgIAAexEBhMBWgICAAHoRAYXASICAgABQEgGAgYCCfwJ + EQGBwFqAgIAAfREBgqWAgIAATxIBgIGAg38CfhEBgcBagICAAH0RAYKngICAAE4SAYCBgIR / An4RAYHAWoCAgAB9EQGCwFaAgIAATRIBgIGAhX8CfhEBgaWAgIAAfREBgsBWgICAAEwSAYCBgIZ / An4RAYGlgICAAH0RAYKngICAAEsSAYCBgId / An4RAYGngICAAH0RAYLAVoCAgABKEgGAgYCIfwJ + EQGBwFqAgIAAfREBgsBKgICAAEkSAYGAfwS2t7i5iX4EfREBgbKAgIAAfBEBgrOAgIAAexEBg7SAgIAAehEBhLWAgIAASBIBgYB / BMCWwJfAmMCZin4EfREBgbaAgIAAfBEBgreAgIAAexEBg7iAgIAAehEBhLmAgIAARxIBgYB / A8CawJvAnIt + BH0RAYHAloCAgAB8EQGCwJeAgIAAexEBg8CYgICAAHoRAYTAmYCAgAABAUjT28vqHOIvRgADAAAAAgQBEwAAAH8VAQCAAYAAAIAASNPby + on87UDAAAAAgW1AQAAAIcDAAAAAgYBGQAAAH8ZAQEET1hNBGphZgRqcmYEamNmBHRjZgE =";

        using (var tr = _lowDb.StartWritingTransaction().Result)
        {
            KeyValueDBExportImporter.Import(tr, new MemoryStream(Convert.FromBase64String(data3)));
            tr.Commit();
        }

        using (var tr = _db.StartReadOnlyTransaction())
        {
            var visitor = new ODBIteratorTest.ToStringVisitor();
            var iterator = new ODBIterator(tr, visitor);
            iterator.Iterate();
            var text = visitor.ToString();
            this.Assent(text);
        }
    }

    [Fact]
    public void Indirect2InlineAutoConversion()
    {
        var treeDBName = _db.RegisterType(typeof(IndirectTree));
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<IndirectTree>();
            root.Content = "Root";
            var left = tr.New<IndirectTree>();
            left.Content = "Left";
            root.Left = new DBIndirect<IndirectTree>(left);
            left.Parent = new DBIndirect<IndirectTree>(root);
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(Tree), treeDBName);
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<Tree>();
            root.Content = "ModifiedRoot";
            Assert.NotEqual(0u, tr.GetOid(root.Left));
            root.Left.Content = "ModifiedLeft";
            tr.Store(root);
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<Tree>();
            Assert.Equal(0, (int)tr.GetOid(root.Left));
            Assert.Equal("ModifiedRoot", root.Content);
            Assert.Equal("ModifiedLeft", root.Left.Content);
        }
    }

    public enum StateV1
    {
        A = 1,
        B = 2,
    }

    public class WithState1
    {
        public StateV1 State { get; set; }
        public IDictionary<StateV1, string> S { get; set; }
    }

    public enum StateV2
    {
        A2 = 1,
        B2 = 2,
    }

    public class WithState2
    {
        public StateV2 State { get; set; }
        public IDictionary<StateV2, string> S { get; set; }
    }

    [Fact]
    public void BinaryCompatibleEnums()
    {
        var typeName = _db.RegisterType(typeof(WithState1));
        using (var tr = _db.StartTransaction())
        {
            tr.Store(new WithState1 { State = StateV1.A, S = new Dictionary<StateV1, string> { { StateV1.B, "b" } } });
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(WithState2), typeName);
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var v = tr.Enumerate<WithState2>().First();
            Assert.Equal(StateV2.A2, v.State);
            Assert.Equal("b", v.S[StateV2.B2]);
        }
    }

    public class WithNullable
    {
        public int? FieldInt { get; set; }
        public int? FieldIntEmpty { get; set; }
    }

    [Fact]
    public void NullableWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            tr.Store(new WithNullable { FieldInt = 10 });
            tr.Commit();
        }

        using (var tr = _db.StartReadOnlyTransaction())
        {
            var v = tr.Enumerate<WithNullable>().First();
            Assert.True(v.FieldInt.HasValue);
            Assert.Equal(10, v.FieldInt.Value);
            Assert.False(v.FieldIntEmpty.HasValue);
        }
    }

    public class WithNullableUpgraded
    {
    }

    [Fact]
    public void NullableSkippingWorks()
    {
        var typeName = _db.RegisterType(typeof(WithNullable));
        using (var tr = _db.StartTransaction())
        {
            tr.Store(new WithNullable { FieldInt = 10 });
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(WithNullableUpgraded), typeName);
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var v = tr.Enumerate<WithNullableUpgraded>().First();
            Assert.NotNull(v);
        }
    }

    [Fact]
    public void DeleteAllWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<ComplexDictionary>();
            root.String = "A";
            var sl1 = new Person { Name = "Poor Slave", Age = 18 };
            var sl2 = new Person { Name = "Poor Poor Slave", Age = 17 };
            root.String2Person.Add("slave", sl1);
            root.String2Person.Add("slave2", sl2);
            root.String2Person.Add("master",
                new Manager { Name = "Chief", Age = 19, Managing = new List<Person> { sl1, sl2 } });
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            tr.DeleteAllData();
            var root = tr.Singleton<ComplexDictionary>();
            Assert.Null(root.String);
            Assert.Equal(0, root.String2Person.Count);
            tr.Commit();
        }
    }

    [Fact]
    public void RollbackAdvisedRemembersItsValue()
    {
        using (var tr = _db.StartTransaction())
        {
            Assert.False(tr.RollbackAdvised);
            tr.RollbackAdvised = true;
            Assert.True(tr.RollbackAdvised);
            tr.RollbackAdvised = false;
            Assert.False(tr.RollbackAdvised);
        }
    }

    class GenericType<T, T2>
    {
    }

    class Subtype1
    {
    }

    class Subtype2
    {
    }

    [Fact]
    public void RegisterGenericTypeUseAlsoGenericTypesNames()
    {
        var objDbName = _db.RegisterType(typeof(GenericType<GenericType<Subtype1, Subtype1>, Subtype2>));
        Assert.Equal("GenericType<GenericType<Subtype1,Subtype1>,Subtype2>", objDbName);
    }

    List<(Type? sourceType, IFieldHandler source, Type targetType, IFieldHandler? target)>
        ReportedTypeIncompatibilities = new();

    public void ReportTypeIncompatibility(Type? sourceType, IFieldHandler source, Type targetType,
        IFieldHandler? target)
    {
        ReportedTypeIncompatibilities.Add((sourceType, source, targetType, target));
    }

    public interface IDynamicValue
    {
    }

    public class DynamicValueWrapper<TValueType> : IDynamicValue
    {
        public TValueType Value { get; set; }
    }

    public class Money
    {
        public decimal MinorValue { get; init; }

        public Currency Currency { get; init; }
    }

    public class Currency
    {
        public int MinorToAmountRatio { get; init; }

        public string Code { get; init; }
    }

    public class Root2
    {
        public List<IDynamicValue> R { get; set; }
    }

    enum Test1
    {
        A = 325,
        B
    }

    [Fact(Skip = "Not implemented yet")]
    public void CanDeserializeWithReferentialIdentityAndBoxedEnum()
    {
        var usd = new Currency() { Code = "USD", MinorToAmountRatio = 100 };
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<Root2>();
            root.R = new List<IDynamicValue>()
            {
                new DynamicValueWrapper<Enum>() { Value = Test1.A },
                new DynamicValueWrapper<Money>()
                {
                    Value = new Money()
                    {
                        MinorValue = 10000,
                        Currency = usd
                    }
                },
                new DynamicValueWrapper<Money>()
                {
                    Value = new Money()
                    {
                        MinorValue = 61000,
                        Currency = usd
                    }
                }
            };
            tr.Commit();
        }

        using (var tr = _db.StartTransaction())
        {
            var obj2 = tr.Singleton<Root2>();
            Assert.Equal("A", ((DynamicValueWrapper<Enum>)obj2.R[0]).Value.ToString());
            Assert.Same(((DynamicValueWrapper<Money>)obj2.R[1]).Value.Currency,
                ((DynamicValueWrapper<Money>)obj2.R[2]).Value.Currency);
        }
    }

    public class Obj
    {
        public Obj? Ref { get; set; }
    }

    public class RootWithObj
    {
        public Obj O { get; set; }
        public int I { get; set; }
    }

    public class RootWithoutObj
    {
        public int I { get; set; }
    }

    [Fact]
    public void CanRemovePropertyTogetherWithClass()
    {
        var rootName = _db.RegisterType(typeof(RootWithObj));
        var objName = _db.RegisterType(typeof(Obj));
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<RootWithObj>();
            root.I = 42;
            root.O = new Obj { Ref = new Obj() };
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(RootWithoutObj), rootName);
        _db.RegisterType(null!, objName);
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<RootWithoutObj>();
            Assert.Equal(42, root.I);
        }
    }

    public interface IFace
    {
        int I { get; set; }
    }

    public class ObjFace: IFace
    {
        public int I { get; set; }
    }

    public class RootObj
    {
        public IFace O { get; set; }
        public int I { get; set; }
    }

    [Fact]
    public void CanRemoveDerivedClassWhenPropertyWithBaseClassExists()
    {
        _db.RegisterType(typeof(RootObj));
        _db.RegisterType(typeof(ObjFace));
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<RootObj>();
            root.I = 42;
            root.O = new ObjFace { I = 5 };
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(RootObj));
        using (var tr = _db.StartTransaction())
        {
            var root = tr.Singleton<RootObj>();
            Assert.Equal(42, root.I);
            Assert.Null(root.O);
        }
    }
}
