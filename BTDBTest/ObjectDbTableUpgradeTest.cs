using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Principal;
using BTDB.Collections;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;

namespace BTDBTest;

public class ObjectDbTableUpgradeTest : IDisposable
{
    readonly IKeyValueDB _lowDb;
    IObjectDB _db;
    StructList<string> _fieldHandlerLoggerMessages;

    public ObjectDbTableUpgradeTest()
    {
        _lowDb = new InMemoryKeyValueDB();
        OpenDb();
    }

    public void Dispose()
    {
        Assert.Empty(_fieldHandlerLoggerMessages);
        _db.Dispose();
        _lowDb.Dispose();
    }

    void ApproveFieldHandlerLoggerMessages([CallerMemberName] string? testName = null)
    {
        Assent.Extensions.Assent(this, string.Join('\n', _fieldHandlerLoggerMessages) + "\n", null, testName);
        _fieldHandlerLoggerMessages.Clear();
    }

    void ReopenDb()
    {
        _db.Dispose();
        OpenDb();
    }

    void OpenDb()
    {
        _db = new ObjectDB();
        _db.Open(_lowDb, false,
            new DBOptions().WithoutAutoRegistration()
                .WithFieldHandlerLogger(new DefaultFieldHandlerLogger(s => _fieldHandlerLoggerMessages.Add(s))));
    }

    public class JobV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public string? Name { get; set; }
    }

    public interface IJobTable1 : IRelation<JobV1>
    {
        void Insert(JobV1 job);
    }

    public class JobV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        [SecondaryKey("Name")] public string Name { get; set; }

        [SecondaryKey("Cost", IncludePrimaryKeyOrder = 1)]
        public uint Cost { get; set; }
    }

    public interface IJobTable2 : IRelation<JobV2>
    {
        void Insert(JobV2 job);
        JobV2 FindByNameOrDefault(string name);
        JobV2 FindByCostOrDefault(ulong id, uint cost);
        IEnumerator<JobV2> ListByCost(AdvancedEnumeratorParam<uint> param);
    }

    public class JobIncompatible
    {
        [PrimaryKey(1)] public Guid Id { get; set; }
    }

    public interface IJobTableIncompatible : IRelation<JobIncompatible>
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
            j = jobTable.FindByCostOrDefault(21, 42);
            Assert.Equal("Build", j.Name);

            var en = jobTable.ListByCost(new AdvancedEnumeratorParam<uint>(EnumerationOrder.Ascending));
            Assert.True(en.MoveNext());
            Assert.Equal(0u, en.Current.Cost);
            Assert.True(en.MoveNext());
            Assert.Equal(42u, en.Current.Cost);
            tr.Commit();
        }
    }

    public class Car
    {
        [PrimaryKey(1)] public ulong CompanyId { get; set; }
        [PrimaryKey(2)] public ulong Id { get; set; }
        public string Name { get; set; }
    }

    public interface ICarTable : IRelation<Car>
    {
        void Insert(Car car);
        Car FindById(ulong companyId, ulong id);
    }

    public enum SimpleEnum
    {
        One = 1,
        Two = 2
    }

    public enum SimpleEnumV2
    {
        Eins = 1,
        Zwei = 2,
        Drei = 3
    }

    public enum SimpleEnumV3
    {
        Two = 2,
        Three = 3,
        Four = 4
    }

    public class ItemWithEnumInKey
    {
        [PrimaryKey] public SimpleEnum Key { get; set; }
        public string Value { get; set; }
    }

    public class ItemWithEnumInKeyV2
    {
        [PrimaryKey] public SimpleEnumV2 Key { get; set; }
        public string Value { get; set; }
    }

    public class ItemWithEnumInKeyV3
    {
        [PrimaryKey] public SimpleEnumV3 Key { get; set; }
        public string Value { get; set; }
    }

    public interface ITableWithEnumInKey : IRelation<ItemWithEnumInKey>
    {
        void Insert(ItemWithEnumInKey person);
    }

    public interface ITableWithEnumInKeyV2 : IRelation<ItemWithEnumInKeyV2>
    {
        ItemWithEnumInKeyV2 FindById(SimpleEnumV2 key);
    }

    public interface ITableWithEnumInKeyV3 : IRelation<ItemWithEnumInKeyV3>
    {
    }

    [Fact]
    public void UpgradePrimaryKeyWithEnumWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<ITableWithEnumInKey>("EnumWithItemInKey");
            var table = creator(tr);

            table.Insert(new ItemWithEnumInKey { Key = SimpleEnum.One, Value = "A" });
            table.Insert(new ItemWithEnumInKey { Key = SimpleEnum.Two, Value = "B" });

            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<ITableWithEnumInKeyV2>("EnumWithItemInKey");
            var table = creator(tr);
            Assert.Equal("A", table.FindById(SimpleEnumV2.Eins).Value);
            Assert.False(table.Upsert(new ItemWithEnumInKeyV2 { Key = SimpleEnumV2.Zwei, Value = "B2" }));
            Assert.True(table.Upsert(new ItemWithEnumInKeyV2 { Key = SimpleEnumV2.Drei, Value = "C" }));
            Assert.Equal(3, table.Count);
        }
    }

    [Fact]
    public void UpgradePrimaryKeyWithIncompatibleEnumNotWork()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<ITableWithEnumInKey>("EnumWithItemInKeyIncompatible");
            var table = creator(tr);

            table.Insert(new ItemWithEnumInKey { Key = SimpleEnum.One, Value = "A" });

            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var ex = Assert.Throws<BTDBException>(() =>
                tr.InitRelation<ITableWithEnumInKeyV3>("EnumWithItemInKeyIncompatible"));
            Assert.Contains("Field 'Key'", ex.Message);
        }
    }

    public class JobV21
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        [SecondaryKey("Name", Order = 2)] public string Name { get; set; }

        [SecondaryKey("Name", Order = 1)]
        [SecondaryKey("Cost", IncludePrimaryKeyOrder = 1)]
        public uint Cost { get; set; }
    }

    public interface IJobTable21 : IRelation<JobV21>
    {
        void Insert(JobV21 job);
        JobV21 FindByNameOrDefault(uint cost, string name);
    }

    [Fact]
    public void ModifiedIndexesAreRecalculated()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTable2>("Job");
            var jobTable = creator(tr);
            var job = new JobV2 { Id = 11, Name = "Code", Cost = 1000 };
            jobTable.Insert(job);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTable21>("Job");
            var jobTable = creator(tr);
            var j = jobTable.FindByNameOrDefault(1000, "Code");
            Assert.NotNull(j);
            Assert.Equal("Code", j.Name);
            tr.Commit();
        }
    }

    public class JobV3
    {
        public JobV3()
        {
            Status = 100;
        }

        [PrimaryKey(1)] public ulong Id { get; set; }

        [SecondaryKey("Status")] public int Status { get; set; }
    }

    public interface IJobTable3 : IRelation<JobV3>
    {
        void Insert(JobV3 job);
        void RemoveById(ulong id);
    }

    [Fact]
    public void AddedFieldIsInsertedFromDefaultObject()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTable2>("Job");
            var jobTable = creator(tr);
            var job = new JobV2 { Id = 11, Name = "Code" };
            jobTable.Insert(job);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTable3>("Job");
            var jobTable = creator(tr);
            jobTable.RemoveById(11);
            Assert.Equal(0, jobTable.Count);
        }
    }

    public class JobV31
    {
        public JobV31()
        {
            Status = 100;
        }

        [PrimaryKey(1)] public ulong Id { get; set; }

        [SecondaryKey("Status")]
        [SecondaryKey("ExpiredStatus", Order = 2)]
        public int Status { get; set; }

        [SecondaryKey("ExpiredStatus", Order = 1)]
        public bool IsExpired { get; set; }
    }

    public interface IJobTable31 : IRelation<JobV31>
    {
        void Insert(JobV31 job);
        void RemoveById(ulong id);
        JobV31? FindByExpiredStatusOrDefault(bool isExpired, int status);
    }

    [Fact]
    public void NewIndexesOnNewFieldAreDeletedWhenItemWasDeleted()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTable3>("Job");
            var jobTable = creator(tr);
            var job1 = new JobV3 { Id = 11, Status = 300 };
            jobTable.Insert(job1);

            var job2 = new JobV3 { Id = 12, Status = 200 };
            jobTable.Insert(job2);

            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTable31>("Job");
            var jobTable = creator(tr);
            jobTable.RemoveById(11);

            Assert.Equal(1, jobTable.Count);

            Assert.Null(jobTable.FindByExpiredStatusOrDefault(false, 300));

            var item = jobTable.FindByExpiredStatusOrDefault(false, 200);
            Assert.Equal(12ul, item.Id);

            tr.Commit();
        }
    }

    public class JobV1s
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        [PersistedName("Name")] public List<string> Names { get; set; }
    }

    public interface IJobTable1s : IRelation<JobV1s>
    {
    }

    [Fact]
    public void ConvertsSingularToList()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTable1>("Job");
            var jobTable = creator(tr);
            var job1 = new JobV1 { Id = 11, Name = "A" };
            jobTable.Insert(job1);

            var job2 = new JobV1 { Id = 12, Name = null };
            jobTable.Insert(job2);

            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IJobTable1s>("Job");
            var jobTable = creator(tr);
            Assert.Equal(2, jobTable.Count);

            Assert.Equal(new[] { "A" }, jobTable.First().Names);
            Assert.Equal(Array.Empty<string>(), jobTable.Last().Names);
        }
    }

    public class EnumsInKeys1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public Dictionary<SimpleEnum, int>? E { get; set; }
    }

    public interface IEnumsInKeys1Table : IRelation<EnumsInKeys1>
    {
    }

    public class EnumsInKeys2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public Dictionary<SimpleEnumV3, int>? E { get; set; }
    }

    public interface IEnumsInKeys2Table : IRelation<EnumsInKeys2>
    {
    }

    [Fact]
    public void EnumsInDictionaryKeysIncompatibleUpgradeDoesNotWorkButAtLeastReportProblem()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IEnumsInKeys1Table>("Enums");
            var eTable = creator(tr);
            var e = new EnumsInKeys1 { Id = 1, E = new Dictionary<SimpleEnum, int> { { SimpleEnum.One, 1 } } };
            eTable.Upsert(e);
            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IEnumsInKeys2Table>("Enums");
            var eTable = creator(tr);
            Assert.Null(eTable.First().E);
            Assert.Equal(1, eTable.Count);
            var e = new EnumsInKeys2 { Id = 1, E = new Dictionary<SimpleEnumV3, int> { { SimpleEnumV3.Four, 1 } } };
            eTable.Upsert(e);
            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IEnumsInKeys1Table>("Enums");
            var eTable = creator(tr);
            Assert.Equal(1, eTable.Count);
        }

        ApproveFieldHandlerLoggerMessages();
    }

    public class DateTimeV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public DateTime Modified { get; set; }
    }

    public interface IDateTimeV1Table : IRelation<DateTimeV1>
    {
    }

    public class DateTimeV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public DateTime? Modified { get; set; }
    }

    public interface IDateTimeV2Table : IRelation<DateTimeV2>
    {
    }

    [Fact]
    public void AddingNullabilityPreserveData()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IDateTimeV1Table>("T");
            var table = creator(tr);
            table.Upsert(new DateTimeV1 { Id = 1, Modified = new(2000, 1, 1) });

            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IDateTimeV2Table>("T");
            var table = creator(tr);
            Assert.Equal(1, table.Count);

            Assert.True(table.First().Modified.HasValue);
            Assert.Equal(new(2000, 1, 1), table.First().Modified!.Value);
        }
    }

    public class Obj
    {
        public int Num { get; set; }
    }

    public class DictV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public IDictionary<Guid, Obj> D { get; set; }
    }

    public interface IDictV1Table : IRelation<DictV1>
    {
    }

    public class DictV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public Dictionary<Guid, Obj> D { get; set; }
    }

    public interface IDictV2Table : IRelation<DictV2>
    {
    }

    [Fact]
    public void MakingDictionaryEagerPreserveData()
    {
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IDictV1Table>("T");
            var table = creator(tr);
            table.Upsert(new()
            {
                Id = 1, D = new Dictionary<Guid, Obj> { { g1, new Obj { Num = 1 } }, { g2, new Obj { Num = 2 } } }
            });

            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IDictV2Table>("T");
            var table = creator(tr);
            Assert.Equal(1, table.Count);

            Assert.Equal(2, table.First().D.Count);
            Assert.Equal(1, table.First().D[g1].Num);
            Assert.Equal(2, table.First().D[g2].Num);
            table.Upsert(table.First());
            // Assert no leaks in IDictionaries
            using var cursor = tr.KeyValueDBTransaction.CreateCursor();
            Assert.Equal(0, cursor.GetKeyValueCount([ObjectDB.AllDictionariesPrefixByte]));
        }
    }

    public class ObjWithDictV1
    {
        public IDictionary<Guid, Obj> D { get; set; }
    }

    public class RowObjWithDictV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public ObjWithDictV1 Obj { get; set; }
    }

    public interface IRowObjWithDictV1Table : IRelation<RowObjWithDictV1>
    {
    }

    [RequireContentFree]
    public class ObjWithDictV2
    {
        public Dictionary<Guid, Obj> D { get; set; }
    }

    public class RowObjWithDictV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public ObjWithDictV2 Obj { get; set; }
    }

    public interface IRowObjWithDictV2Table : IRelation<RowObjWithDictV2>
    {
    }

    [Fact]
    public void MakingDictionaryEagerPreserveData2()
    {
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();
        _db.RegisterType(typeof(ObjWithDictV1), "ObjWithDict");
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IRowObjWithDictV1Table>("T");
            var table = creator(tr);
            table.Upsert(new()
            {
                Id = 1,
                Obj = new()
                {
                    D = new Dictionary<Guid, Obj> { { g1, new Obj { Num = 1 } }, { g2, new Obj { Num = 2 } } }
                }
            });

            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(ObjWithDictV2), "ObjWithDict");

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IRowObjWithDictV2Table>("T");
            var table = creator(tr);
            Assert.Equal(1, table.Count);

            Assert.Equal(2, table.First().Obj.D.Count);
            Assert.Equal(1, table.First().Obj.D[g1].Num);
            Assert.Equal(2, table.First().Obj.D[g2].Num);
            table.Upsert(table.First());
            // Assert no leaks in IDictionaries
            using var cursor = tr.KeyValueDBTransaction.CreateCursor();
            Assert.Equal(0, cursor.GetKeyValueCount([ObjectDB.AllDictionariesPrefixByte]));
        }
    }

    public class ObjInObj
    {
        public Obj O { get; set; }
    }

    public class RowObjInObjV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public ObjInObj OO { get; set; }
    }

    public interface IRowObjInObjV1Table : IRelation<RowObjInObjV1>
    {
    }

    public class ObjInObjV2
    {
    }

    public class RowObjInObjV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public ObjInObjV2 OO { get; set; }
    }

    public interface IRowObjInObjV2Table : IRelation<RowObjInObjV2>
    {
    }

    [Fact]
    public void SupportSkippingOfNotRegisteredObjects()
    {
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();
        _db.RegisterType(typeof(Obj), "ObjVoldemort");
        _db.RegisterType(typeof(ObjInObj), "ObjInObj");
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IRowObjInObjV1Table>("T");
            var table = creator(tr);
            table.Upsert(new()
            {
                Id = 1, OO = new() { O = new Obj { Num = 1 } }
            });

            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(ObjInObjV2), "ObjInObj");

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IRowObjInObjV2Table>("T");
            var table = creator(tr);
            Assert.Equal(1, table.Count);
            Assert.NotNull(table.First());
        }
    }

// Complex sample { IList<{ IDictionary<int,Obj> }> } in old version

    public class S1ObjWithDictV1
    {
        public IDictionary<int, Obj> D { get; set; }
    }

    public class S1RowObjWithListV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public IList<S1ObjWithDictV1> L { get; set; }
    }

    public interface IS1RowObjWithListV1Table : IRelation<S1RowObjWithListV1>
    {
        bool RemoveById(ulong id);
    }

    [RequireContentFree]
    public class S1ObjWithDictV2
    {
        public Dictionary<int, Obj> D { get; set; }
    }

    public class S1RowObjWithListV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public IList<S1ObjWithDictV2> L { get; set; }
    }

    public interface IS1RowObjWithListV2Table : IRelation<S1RowObjWithListV2>
    {
        bool RemoveById(ulong id);
    }

    [Fact]
    public void RemoveByIdDoesNotBreakMakingDictionaryEager()
    {
        _db.RegisterType(typeof(S1ObjWithDictV1), "S1ObjWithDict");
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IS1RowObjWithListV1Table>("T");
            var table = creator(tr);
            table.Upsert(new()
            {
                Id = 1, L = new List<S1ObjWithDictV1>
                {
                    new()
                    {
                        D = new Dictionary<int, Obj>
                        {
                            { 1, new Obj { Num = 1 } }
                        }
                    }
                }
            });
            table.Upsert(new()
            {
                Id = 2, L = new List<S1ObjWithDictV1>
                {
                    new()
                    {
                        D = new Dictionary<int, Obj>
                        {
                            { 2, new Obj { Num = 2 } }
                        }
                    }
                }
            });

            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(S1ObjWithDictV2), "S1ObjWithDict");

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IS1RowObjWithListV2Table>("T");
            var table = creator(tr);
            table.RemoveById(2);
            Assert.Equal(1, table.Count);

            Assert.Equal(1, table.First().L.Count);
            Assert.Equal(1, table.First().L[0].D.Count);
            Assert.Equal(1, table.First().L[0].D[1].Num);
            table.Upsert(table.First());
            // Assert no leaks in IDictionaries
            using var cursor = tr.KeyValueDBTransaction.CreateCursor();
            Assert.Equal(0, cursor.GetKeyValueCount([ObjectDB.AllDictionariesPrefixByte]));
        }
    }

    public class ObjChild : Obj
    {
        public int Child { get; set; }
    }


    public class S2Obj
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public Obj O { get; set; }
    }

    public interface IS2ObjTable : IRelation<S2Obj>
    {
        bool RemoveById(ulong id);
    }

    public class S2ObjV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public ObjChild O { get; set; }
    }

    public interface IS2ObjV2Table : IRelation<S2ObjV2>
    {
        bool RemoveById(ulong id);
    }

    public class MyObjToObjChildTypeConvertorGenerator : DefaultTypeConvertorGenerator
    {
        public static ObjChild Convert2ObjChild(Obj value) => new() { Num = value.Num, Child = 42 };
    }

    [Fact]
    public void InlineChildObjPreserveDataWithCustomConvertor()
    {
        _db.RegisterType(typeof(Obj));
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IS2ObjTable>("T");
            var table = creator(tr);
            table.Upsert(new()
            {
                Id = 1, O = new() { Num = 2 }
            });
            tr.Commit();
        }

        ReopenDb();

        _db.TypeConvertorGenerator = new MyObjToObjChildTypeConvertorGenerator();
        _db.RegisterType(typeof(Obj));
        _db.RegisterType(typeof(ObjChild));
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IS2ObjV2Table>("T");
            var table = creator(tr);
            Assert.Equal(1, table.Count);

            Assert.Equal(2, table.First().O.Num);
            Assert.Equal(42, table.First().O.Child);
        }
    }

    public enum TestEnum
    {
        Test
    }

    public class ObjV1
    {
        public TestEnum MyTestEnum { get; set; }
    }

    public class ObjV2
    {
        public TestEnum? MyTestEnum { get; set; }
    }

    public interface IObjV1Table : IRelation<ObjV1>
    {
    }

    public interface IObjV2Table : IRelation<ObjV2>
    {
    }

    [Fact]
    public void UpgradeNotNullableEnumToNullableEnum()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IObjV1Table>("MyRelation");
            var objV1Table = creator(tr);
            var job1 = new ObjV1 { MyTestEnum = TestEnum.Test };
            objV1Table.Upsert(job1);
            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IObjV2Table>("MyRelation");
            var objV2Table = creator(tr);
            objV2Table.ToList();
        }
    }

    public class Obj2V1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        [SecondaryKey("A")] public ulong Sk { get; set; }
    }

    public class Obj2V2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        [SecondaryKey("A")] public ulong? Sk { get; set; }
    }

    public interface IObj2V1Table : IRelation<Obj2V1>
    {
    }

    public interface IObj2V2Table : IRelation<Obj2V2>
    {
    }

    [Fact]
    public void UpgradeNotNullableSecondaryKeyToNullableSecondaryKeyWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IObj2V1Table>("MyRelation");
            var objV1Table = creator(tr);
            var job1 = new Obj2V1 { Id = 2, Sk = 4 };
            objV1Table.Upsert(job1);
            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IObj2V2Table>("MyRelation");
            var objV2Table = creator(tr);
            Assert.Equal(4ul, objV2Table.ToList()[0].Sk);
            objV2Table.Upsert(new() { Id = 2, Sk = 5 });
        }
    }

    [Fact]
    public void UpgradeNullableSecondaryKeyToNonNullableSecondaryKeyWorks()
    {
        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IObj2V2Table>("MyRelation");
            var objV1Table = creator(tr);
            var job1 = new Obj2V2 { Id = 2, Sk = 4 };
            objV1Table.Upsert(job1);
            tr.Commit();
        }

        ReopenDb();

        using (var tr = _db.StartTransaction())
        {
            var creator = tr.InitRelation<IObj2V1Table>("MyRelation");
            var objV2Table = creator(tr);
            Assert.Equal(4ul, objV2Table.ToList()[0].Sk);
            objV2Table.Upsert(new() { Id = 2, Sk = 5 });
        }
    }

    public class Obj2
    {
        public Obj2? Ref { get; set; }
    }

    public class RootWithObj
    {
        [PrimaryKey(1)] public int I { get; set; }
        public Obj2 O { get; set; }
        public int After { get; set; }
    }

    public class RootWithoutObj
    {
        [PrimaryKey(1)] public int I { get; set; }
        public int After { get; set; }
    }

    [PersistedName("R")]
    public interface IRootWithObjTable : IRelation<RootWithObj>
    {
    }

    [PersistedName("R")]
    public interface IRootWithoutObjTable : IRelation<RootWithoutObj>
    {
    }

    [Fact]
    public void CanRemovePropertyTogetherWithClass()
    {
        var objName = _db.RegisterType(typeof(Obj2));
        using (var tr = _db.StartTransaction())
        {
            var t = tr.GetRelation<IRootWithObjTable>();
            var root = new RootWithObj();
            root.I = 42;
            root.O = new Obj2 { Ref = new Obj2() };
            root.After = 43;
            t.Upsert(root);
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(null!, objName);
        using (var tr = _db.StartTransaction())
        {
            var t = tr.GetRelation<IRootWithoutObjTable>();
            var root = t.First();
            Assert.Equal(42, root.I);
            Assert.Equal(43, root.After);
        }
    }

    public interface IFace
    {
        int I { get; set; }
    }

    public class ObjFace : IFace
    {
        public int I { get; set; }
    }

    public class RootObj
    {
        public IFace O { get; set; }
        public int I { get; set; }
    }

    public interface IRootObjTable : IRelation<RootObj>
    {
    }

    [Fact]
    public void CanRemoveDerivedClassWhenPropertyWithBaseClassExists()
    {
        _db.RegisterType(typeof(ObjFace));
        using (var tr = _db.StartTransaction())
        {
            var t = tr.GetRelation<IRootObjTable>();
            var root = new RootObj
            {
                I = 42,
                O = new ObjFace { I = 5 }
            };
            t.Upsert(root);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            tr.SkipUnknownTypes = true;
            var t = tr.GetRelation<IRootObjTable>();
            var root = t.First();
            Assert.Equal(42, root.I);
            Assert.Null(root.O);
        }

        using (var tr = _db.StartTransaction())
        {
            var t = tr.GetRelation<IRootObjTable>();
            Assert.Throws<BTDBException>(() => t.First());
        }
    }
}
