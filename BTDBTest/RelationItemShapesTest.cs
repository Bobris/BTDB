using System;
using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Relation items whose stored properties come from a generic base class and from an explicit interface
/// implementation keep them across reopening, the generated metadata agrees with reflection on them, and the
/// generated relation methods work on keys declared in a generic base.
public class RelationItemShapesTest : ObjectDbTestBase
{
    public RelationItemShapesTest(ITestOutputHelper output) : base(output)
    {
    }

    public interface IHasNote
    {
        string Note { get; set; }
    }

    public class ShapeRowBase<TKey>
    {
        [PrimaryKey(1)] public TKey Id { get; set; } = default!;
        public List<TKey>? Others { get; set; }
    }

    [Generate]
    public class ShapeRow : ShapeRowBase<ulong>, IHasNote
    {
        [SecondaryKey("Name")] public string Name { get; set; } = "";
        string IHasNote.Note { get; set; } = "";
        public int Count { get; set; }
    }

    public interface IShapeRowTable : IRelation<ShapeRow>
    {
        void Insert(ShapeRow row);
        ShapeRow FindById(ulong id);
        IEnumerable<ShapeRow> FindByName(string name);
    }

    // Before the fix the explicitly implemented property was stored also under the interface property's name, through
    // a backing field that does not exist, which failed loading the assembly and disagreed with reflection.
    [Fact]
    public void GenericBaseAndExplicitInterfacePropertiesRoundTrip()
    {
        using (var tr = _db.StartTransaction())
        {
            var row = new ShapeRow { Id = 5, Others = [1, 2], Name = "n", Count = 3 };
            ((IHasNote)row).Note = "note";
            tr.GetRelation<IShapeRowTable>().Insert(row);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var table = tr.GetRelation<IShapeRowTable>();
            var read = table.FindById(5);
            Assert.Equal((5ul, "n", 3, "note"), (read.Id, read.Name, read.Count, ((IHasNote)read).Note));
            Assert.Equal([1ul, 2ul], read.Others!);
            Assert.Single(table.FindByName("n"));
            Assert.Equal(5ul, table.Query<ShapeRow>().Where(r => r.Count == 3).Single().Id);
        }
    }

    public class InheritedKeyBase<T>
    {
        [PrimaryKey(1)] public T Tenant { get; set; } = default!;
        public string? Note { get; set; }
    }

    [Generate]
    public class InheritedKeyRow : InheritedKeyBase<ulong>
    {
        [PrimaryKey(2)] public ulong Id { get; set; }
        [SecondaryKey("Age")] public int Age { get; set; }
        public string Name { get; set; } = "";
    }

    public interface IInheritedKeyRowTable : IRelation<InheritedKeyRow>
    {
        void Insert(InheritedKeyRow row);
        InheritedKeyRow? FindByIdOrDefault(ulong tenant, ulong id);
        IEnumerable<InheritedKeyRow> FindById(ulong tenant);
        IEnumerable<InheritedKeyRow> FindByAge(int age);
        IEnumerable<InheritedKeyRow> ListById(ulong tenant, AdvancedEnumeratorParam<ulong> id);
        IEnumerable<InheritedKeyRow> ListByAge(AdvancedEnumeratorParam<int> age);
        IEnumerable<InheritedKeyRow> ScanById(Constraint<ulong> tenant, Constraint<ulong> id);
        ulong GatherById(ICollection<InheritedKeyRow> items, long skip, long take, Constraint<ulong> tenant);
        bool UpdateById(ulong tenant, ulong id, string name);
        int RemoveByIdPartial(ulong tenant, int maxCount);
        bool Contains(ulong tenant, ulong id);
        int CountById(ulong tenant);
        bool AnyByAge(int age);
    }

    // Every generated relation method on a primary key partly declared in a generic base returns what the rows say.
    [Fact]
    public void RelationMethodsOnInheritedPrimaryKeyMatchModel()
    {
        var random = new Random(5);
        var model = new List<InheritedKeyRow>();
        using (var tr = _db.StartTransaction())
        {
            var t = tr.GetRelation<IInheritedKeyRowTable>();
            for (var i = 0; i < 200; i++)
            {
                var row = new InheritedKeyRow { Tenant = (ulong)random.Next(4), Id = (ulong)i, Age = random.Next(10), Name = "n" + i, Note = i % 2 == 0 ? null : "x" };
                t.Insert(row);
                model.Add(row);
            }
            tr.Commit();
        }
        ReopenDb();
        using (var tr = _db.StartTransaction())
        {
            var t = tr.GetRelation<IInheritedKeyRowTable>();
            string Ids(IEnumerable<InheritedKeyRow> rows) => string.Join(",", rows.Select(r => $"{r.Tenant}/{r.Id}/{r.Age}/{r.Name}/{r.Note}"));
            var ordered = model.OrderBy(r => r.Tenant).ThenBy(r => r.Id).ToList();
            Assert.Equal(Ids(ordered.Where(r => r.Tenant == 2)), Ids(t.FindById(2)));
            Assert.Equal(Ids(model.Where(r => r.Age == 3).OrderBy(r => r.Tenant).ThenBy(r => r.Id)), Ids(t.FindByAge(3)));
            Assert.Equal(Ids(ordered.Where(r => r.Tenant == 1 && r.Id >= 50 && r.Id < 120)),
                Ids(t.ListById(1, new(EnumerationOrder.Ascending, 50, KeyProposition.Included, 120, KeyProposition.Excluded))));
            Assert.Equal(Ids(ordered.Where(r => r.Tenant == 1 && r.Id >= 50 && r.Id < 120).Reverse()),
                Ids(t.ListById(1, new(EnumerationOrder.Descending, 50, KeyProposition.Included, 120, KeyProposition.Excluded))));
            Assert.Equal(Ids(model.Where(r => r.Age > 2 && r.Age <= 5).OrderBy(r => r.Age).ThenBy(r => r.Tenant).ThenBy(r => r.Id)),
                Ids(t.ListByAge(new(EnumerationOrder.Ascending, 2, KeyProposition.Excluded, 5, KeyProposition.Included))));
            Assert.Equal(Ids(ordered.Where(r => r.Tenant >= 1 && r.Tenant <= 2 && r.Id <= 99)),
                Ids(t.ScanById(Constraint.Unsigned.Predicate(x => x >= 1 && x <= 2), Constraint.Unsigned.UpTo(99))));
            var gathered = new List<InheritedKeyRow>();
            var count = t.GatherById(gathered, 3, 5, Constraint.Unsigned.Exact(3));
            Assert.Equal((ulong)ordered.Count(r => r.Tenant == 3), count);
            Assert.Equal(Ids(ordered.Where(r => r.Tenant == 3).Skip(3).Take(5)), Ids(gathered));
            var target = ordered.First(r => r.Tenant == 0);
            Assert.True(t.UpdateById(0, target.Id, "renamed"));
            Assert.False(t.UpdateById(0, 9999, "missing"));
            target.Name = "renamed";
            Assert.Equal(Ids([target]), Ids([t.FindByIdOrDefault(0, target.Id)!]));
            Assert.True(t.Contains(target.Tenant, target.Id));
            Assert.Equal(ordered.Count(r => r.Tenant == 1), t.CountById(1));
            Assert.Equal(model.Any(r => r.Age == 9), t.AnyByAge(9));
            Assert.Equal(3, t.RemoveByIdPartial(2, 3));
            var tenant2 = ordered.Where(r => r.Tenant == 2).ToList();
            Assert.Equal(Ids(tenant2.Skip(3)), Ids(t.FindById(2)));
            Assert.Equal(Ids(model.Where(r => r.Age == 3 && !tenant2.Take(3).Contains(r)).OrderBy(r => r.Tenant).ThenBy(r => r.Id)), Ids(t.FindByAge(3)));
        }
    }

    public enum TupleKind
    {
        A,
        B
    }

    [Generate]
    public class TupleRow
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public (DateTime, byte) DateAndByte { get; set; }
        public ((int, string), TupleKind) NestedWithEnum { get; set; }
        public (Guid, byte)? NullableGuidAndByte { get; set; }
    }

    public interface ITupleRowTable : IRelation<TupleRow>
    {
        void Insert(TupleRow row);
        TupleRow FindById(ulong id);
    }

    // Before the fix the generated metadata measured tuple layouts on stand-in structs with sequential layout, so
    // relations stored a DateTime next to a byte as default and cut the end of a Guid next to a byte.
    [Fact]
    public void TuplesWithStructItemsRoundTrip()
    {
        var when = new DateTime(2025, 6, 7, 8, 9, 10, DateTimeKind.Utc);
        var guid = Guid.NewGuid();
        using (var tr = _db.StartTransaction())
        {
            tr.GetRelation<ITupleRowTable>().Insert(new()
            {
                Id = 1, DateAndByte = (when, 7), NestedWithEnum = ((1, "s"), TupleKind.B), NullableGuidAndByte = (guid, 3)
            });
            tr.Commit();
        }

        ReopenDb();
        using var readTr = _db.StartReadOnlyTransaction();
        var read = readTr.GetRelation<ITupleRowTable>().FindById(1);
        Assert.Equal(((when, (byte)7), ((1, "s"), TupleKind.B), ((Guid, byte)?)(guid, 3)),
            (read.DateAndByte, read.NestedWithEnum, read.NullableGuidAndByte));
    }

    // A damaged stored value can name a version the relation never had. Before the fix reading it failed with
    // IndexOutOfRangeException or NullReferenceException instead of reporting the damage.
    [Theory]
    [InlineData(0)]
    [InlineData(5)]
    [InlineData(1000)]
    public void DamagedValueVersionIsReported(uint version)
    {
        byte[] prefix;
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<ITupleRowTable>();
            table.Insert(new() { Id = 1 });
            prefix = ((IRelationDbManipulator)table).RelationInfo.Prefix;
            tr.Commit();
        }

        using (var tr = _lowDb.StartTransaction())
        {
            using var cursor = tr.CreateCursor();
            Assert.True(cursor.FindFirstKey(prefix));
            var buffer = Span<byte>.Empty;
            var value = cursor.GetValueSpan(ref buffer).ToArray();
            var writer = new MemWriter();
            writer.WriteVUInt32(version);
            var reader = MemReader.CreateFromPinnedSpan(value);
            reader.SkipVUInt32();
            writer.WriteBlock(value.AsSpan((int)reader.GetCurrentPosition()));
            cursor.SetValue(writer.GetSpan());
            tr.Commit();
        }

        using var readTr = _db.StartReadOnlyTransaction();
        Assert.Throws<BTDBException>(() => readTr.GetRelation<ITupleRowTable>().FindById(1));
    }
}
