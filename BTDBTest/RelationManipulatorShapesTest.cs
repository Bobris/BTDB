using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// Exercises less common relation method shapes which are forwarded to RelationDBManipulator (sizes variants, shallow
// variants, range upserts, InKeyValue keys, OnBeforeRemove, custom property setters, projections and value upgrades)
// in both relation implementation modes.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class RelationManipulatorShapesTest : ObjectDbTestBase
{
    public RelationManipulatorShapesTest(ITestOutputHelper output) : base(output)
    {
    }

    static void RunInMode(bool noEmit, Action action)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            action();
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    public class Plain
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public string? Name { get; set; }
    }

    public interface IPlainTable : IRelation<Plain>
    {
        void Insert(Plain item);
        bool ShallowUpsert(Plain item);
        void ShallowUpdate(Plain item);
        void Update(Plain item);
        Plain? FindByIdOrDefault(ulong id);

        (bool Inserted, uint KeySize, uint OldValueSize, uint NewValueSize) ShallowUpsertWithSizes(Plain obj,
            bool allowInsert, bool allowUpdate);

        (ulong Count, ulong KeySizes, ulong ValueSizes) RemoveWithSizesById(Constraint<ulong> id);
        bool ShallowRemoveById(ulong id);
        void RemoveById(ulong id);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PlainRelationManipulation(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IPlainTable>();
            t.Insert(new() { Id = 1, Name = "a" });
            Assert.StartsWith("Trying to insert duplicate key in ",
                Assert.Throws<BTDBException>(() => t.Insert(new() { Id = 1, Name = "b" })).Message);

            var r = t.ShallowUpsertWithSizes(new() { Id = 2, Name = "bb" }, false, true);
            Assert.Equal((false, 0u, 0u, 0u), r);
            Assert.Null(t.FindByIdOrDefault(2));
            r = t.ShallowUpsertWithSizes(new() { Id = 2, Name = "bb" }, true, false);
            Assert.True(r.Inserted);
            Assert.True(r.KeySize > 0);
            Assert.Equal(0u, r.OldValueSize);
            var insertedSize = r.NewValueSize;
            r = t.ShallowUpsertWithSizes(new() { Id = 2, Name = "bbbb" }, true, false);
            Assert.Equal((false, insertedSize, insertedSize), (r.Inserted, r.OldValueSize, r.NewValueSize));
            Assert.Equal("bb", t.FindByIdOrDefault(2)!.Name);
            r = t.ShallowUpsertWithSizes(new() { Id = 2, Name = "bbbb" }, true, true);
            Assert.False(r.Inserted);
            Assert.Equal(insertedSize, r.OldValueSize);
            Assert.Equal(insertedSize + 2, r.NewValueSize);
            Assert.Equal("bbbb", t.FindByIdOrDefault(2)!.Name);

            Assert.True(t.ShallowUpsert(new() { Id = 3, Name = "c" }));
            Assert.False(t.ShallowUpsert(new() { Id = 3, Name = "cc" }));
            t.ShallowUpdate(new() { Id = 3, Name = "ccc" });
            Assert.Equal("ccc", t.FindByIdOrDefault(3)!.Name);
            t.Update(new() { Id = 3, Name = "d" });
            Assert.Equal("d", t.FindByIdOrDefault(3)!.Name);
            Assert.Throws<BTDBException>(() => t.ShallowUpdate(new() { Id = 30 }));
            Assert.Throws<BTDBException>(() => t.Update(new() { Id = 30 }));

            Assert.Equal((2L, 1L), t.UpsertRange([new() { Id = 4 }, new() { Id = 5 }, new() { Id = 1, Name = "x" }]));
            Assert.Equal("x", t.FindByIdOrDefault(1)!.Name);

            Assert.True(t.ShallowRemoveById(5));
            Assert.False(t.ShallowRemoveById(5));
            Assert.Throws<BTDBException>(() => t.RemoveById(5));

            Assert.Throws<NotSupportedException>(() => t.RemoveWithSizesById(Constraint.Unsigned.UpTo(2)));
            var sizes = t.RemoveWithSizesById(Constraint.Unsigned.Exact(2));
            Assert.Equal(1ul, sizes.Count);
            Assert.True(sizes.KeySizes > 0);
            Assert.Equal((ulong)insertedSize + 2, sizes.ValueSizes);
            Assert.Equal(new ulong[] { 1, 3, 4 }, t.Select(p => p.Id));
            Assert.Equal(0ul, t.RemoveWithSizesById(Constraint.Unsigned.Exact(100)).Count);
        });
    }

    public class WithSk
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [SecondaryKey("Name")] public string Name { get; set; } = "";
        public IDictionary<ulong, string>? Dict { get; set; }
    }

    public interface IWithSkTable : IRelation<WithSk>
    {
        bool Insert(WithSk item);
        bool ShallowUpsert(WithSk item);
        void ShallowUpdate(WithSk item);
        void Update(WithSk item);
        IEnumerable<WithSk> FindByName(string name);
        WithSk FindById(ulong id);
        bool ShallowRemoveById(ulong id);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void SecondaryKeysAreMaintainedByShallowOperations(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.GetRelation<IWithSkTable>();
                Assert.True(t.Insert(new() { Id = 1, Name = "a" }));
                Assert.False(t.Insert(new() { Id = 1, Name = "b" }));
                Assert.True(t.ShallowUpsert(new() { Id = 2, Name = "b" }));
                Assert.False(t.ShallowUpsert(new() { Id = 2, Name = "c" }));
                Assert.Empty(t.FindByName("b"));
                Assert.Single(t.FindByName("c"));
                t.ShallowUpdate(new() { Id = 2, Name = "d" });
                Assert.Empty(t.FindByName("c"));
                Assert.Single(t.FindByName("d"));
                Assert.Throws<BTDBException>(() => t.ShallowUpdate(new() { Id = 20, Name = "d" }));
                Assert.Equal((1L, 2L), t.UpsertRange([
                    new() { Id = 1, Name = "e", Dict = new Dictionary<ulong, string> { [1] = "x" } },
                    new() { Id = 2, Name = "e" }, new() { Id = 3, Name = "f" }
                ]));
                Assert.Equal(2, t.FindByName("e").Count());
                Assert.Equal("x", t.FindById(1).Dict![1]);
                t.Update(new() { Id = 1, Name = "g" });
                Assert.Single(t.FindByName("g"));
                Assert.True(t.ShallowRemoveById(3));
                Assert.False(t.ShallowRemoveById(3));
                Assert.Empty(t.FindByName("f"));
                tr.Commit();
            }

            using (var tr = _db.StartTransaction())
            {
                // Update removed only reference to IDictionary, so it must be freed
                using var cursor = tr.KeyValueDBTransaction.CreateCursor();
                Assert.Equal(0, cursor.GetKeyValueCount([ObjectDB.AllDictionariesPrefixByte]));
            }
        });
    }

    public class InKv
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [InKeyValue(2)] public ulong Version { get; set; }
        public string Name { get; set; } = "";
    }

    public interface IInKvTable : IRelation<InKv>
    {
        bool ShallowUpsert(InKv item);
        void ShallowUpdate(InKv item);
        void Update(InKv item);
        InKv? FindByIdOrDefault(ulong id);

        (bool Inserted, uint KeySize, uint OldValueSize, uint NewValueSize) ShallowUpsertWithSizes(InKv obj,
            bool allowInsert, bool allowUpdate);

        bool ShallowRemoveById(ulong id);
        bool RemoveById(ulong id);
        bool UpdateById(ulong id, ulong version);
        bool UpdateById(ulong id, ulong version, string name);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void InKeyValueRelationManipulation(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IInKvTable>();
            var r = t.ShallowUpsertWithSizes(new() { Id = 1, Version = 1, Name = "a" }, false, true);
            Assert.Equal((false, 0u, 0u, 0u), r);
            r = t.ShallowUpsertWithSizes(new() { Id = 1, Version = 1, Name = "a" }, true, true);
            Assert.True(r.Inserted);
            var size = r.NewValueSize;
            r = t.ShallowUpsertWithSizes(new() { Id = 1, Version = 2, Name = "b" }, true, false);
            Assert.Equal((false, size, size), (r.Inserted, r.OldValueSize, r.NewValueSize));
            Assert.Equal(1ul, t.FindByIdOrDefault(1)!.Version);
            r = t.ShallowUpsertWithSizes(new() { Id = 1, Version = 3, Name = "bb" }, true, true);
            Assert.False(r.Inserted);
            Assert.Equal(3ul, t.FindByIdOrDefault(1)!.Version);
            Assert.Equal("bb", t.FindByIdOrDefault(1)!.Name);

            Assert.True(t.ShallowUpsert(new() { Id = 2, Version = 1, Name = "c" }));
            Assert.False(t.ShallowUpsert(new() { Id = 2, Version = 5, Name = "c" }));
            Assert.Equal(5ul, t.FindByIdOrDefault(2)!.Version);
            t.ShallowUpdate(new() { Id = 2, Version = 6, Name = "d" });
            Assert.Equal((6ul, "d"), (t.FindByIdOrDefault(2)!.Version, t.FindByIdOrDefault(2)!.Name));
            t.Update(new() { Id = 2, Version = 7, Name = "e" });
            Assert.Equal((7ul, "e"), (t.FindByIdOrDefault(2)!.Version, t.FindByIdOrDefault(2)!.Name));
            Assert.Throws<BTDBException>(() => t.ShallowUpdate(new() { Id = 3 }));
            Assert.Throws<BTDBException>(() => t.Update(new() { Id = 3 }));

            Assert.True(t.UpdateById(2, 8));
            Assert.False(t.UpdateById(3, 8));
            Assert.True(t.UpdateById(2, 9, "f"));
            Assert.False(t.UpdateById(3, 9, "f"));
            Assert.Equal((9ul, "f"), (t.FindByIdOrDefault(2)!.Version, t.FindByIdOrDefault(2)!.Name));

            Assert.True(t.ShallowRemoveById(2));
            Assert.False(t.ShallowRemoveById(2));
            Assert.True(t.RemoveById(1));
            Assert.False(t.RemoveById(1));
            Assert.Empty(t);
        });
    }

    public class WithBeforeRemove
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        [InKeyValue(2)] public ulong Version { get; set; }
        public bool Protected { get; set; }

        [OnBeforeRemove]
        public bool BeforeRemove(IObjectDBTransaction tr)
        {
            Assert.NotNull(tr);
            return Protected;
        }
    }

    public interface IWithBeforeRemoveTable : IRelation<WithBeforeRemove>
    {
        bool ShallowRemoveById(ulong id);
        bool RemoveById(ulong id);
        void Insert(WithBeforeRemove item);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OnBeforeRemoveWithInKeyValue(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<IWithBeforeRemoveTable>();
            t.Insert(new() { Id = 1, Version = 10, Protected = true });
            t.Insert(new() { Id = 2, Version = 20 });
            t.Insert(new() { Id = 3, Version = 30 });
            Assert.False(t.RemoveById(1));
            Assert.False(t.ShallowRemoveById(1));
            Assert.Equal(3, t.Count);
            Assert.True(t.RemoveById(2));
            Assert.True(t.ShallowRemoveById(3));
            Assert.False(t.RemoveById(4));
            Assert.False(t.ShallowRemoveById(4));
            Assert.Equal(new ulong[] { 1 }, t.Select(i => i.Id));
        });
    }

    public class CustomSetters
    {
        ulong _id;
        string _code = "";
        int _count;
        string? _note;
        (long, long, long) _triple;
        Guid _guid;

        [PrimaryKey(1)]
        public ulong Id
        {
            get => _id;
            set => _id = value;
        }

        [PrimaryKey(2)]
        public string Code
        {
            get => _code;
            set => _code = value;
        }

        public int Count
        {
            get => _count;
            set => _count = value;
        }

        public string? Note
        {
            get => _note;
            set => _note = value;
        }

        public (long, long, long) Triple
        {
            get => _triple;
            set => _triple = value;
        }

        public Guid Guid
        {
            get => _guid;
            set => _guid = value;
        }
    }

    public class CustomTupleKey
    {
        (long, long, long) _key;
        string _name = "";

        [PrimaryKey(1)]
        public (long, long, long) Key
        {
            get => _key;
            set => _key = value;
        }

        public string Name
        {
            get => _name;
            set => _name = value;
        }
    }

    public interface ICustomTupleKeyTable : IRelation<CustomTupleKey>
    {
        void Insert(CustomTupleKey item);
        CustomTupleKey FindById((long, long, long) key);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PrimaryKeyBiggerThan16BytesWithCustomSetter(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<ICustomTupleKeyTable>();
            t.Insert(new() { Key = (1, -2, long.MaxValue), Name = "a" });
            t.Insert(new() { Key = (-1, 2, 3), Name = "b" });
            Assert.Equal("a", t.FindById((1, -2, long.MaxValue)).Name);
            Assert.Equal(new[] { (-1L, 2L, 3L), (1L, -2L, long.MaxValue) }, t.Select(i => i.Key));
        });
    }

    public class CustomSettersCount
    {
        ulong _id;
        int _count;

        public CustomSettersCount(int dummy)
        {
            _count = dummy;
        }

        public ulong Id
        {
            get => _id;
            set => _id = value;
        }

        public int Count
        {
            get => _count;
            set => _count = value;
        }
    }

    public interface ICustomSettersTable : IRelation<CustomSetters>
    {
        void Insert(CustomSetters item);
        CustomSetters FindById(ulong id, string code);
        CustomSettersCount FindByIdJustCount(ulong id, string code);
        IEnumerable<CustomSetters> ListById(ulong id);
        IEnumerable<CustomSettersCount> ListByIdJustCount(ulong id);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void PropertiesWithCustomSettersAndProjections(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            var guid = Guid.NewGuid();
            using var tr = _db.StartTransaction();
            var t = tr.GetRelation<ICustomSettersTable>();
            t.Insert(new() { Id = 1, Code = "a", Count = 5, Note = "n", Triple = (1, -2, 3), Guid = guid });
            t.Insert(new() { Id = 1, Code = "b", Count = 6 });
            var item = t.FindById(1, "a");
            Assert.Equal((1ul, "a", 5, "n", (1L, -2L, 3L), guid),
                (item.Id, item.Code, item.Count, item.Note, item.Triple, item.Guid));
            var light = t.FindByIdJustCount(1, "b");
            Assert.Equal((1ul, 6), (light.Id, light.Count));
            Assert.Equal(new[] { "a", "b" }, t.ListById(1).Select(i => i.Code));
            Assert.Equal(new[] { 5, 6 }, t.ListByIdJustCount(1).Select(i => i.Count));
        });
    }

    public class VersionedV1
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public string Name { get; set; } = "";
        public int Removed { get; set; }
    }

    public interface IVersionedV1Table : IRelation<VersionedV1>
    {
        void Insert(VersionedV1 item);
    }

    public class VersionedV2
    {
        IDictionary<ulong, ulong>? _map;

        [PrimaryKey(1)] public ulong Id { get; set; }
        public string Name { get; set; } = "";
        public long Added { get; set; }

        public IDictionary<ulong, ulong> Map
        {
            get => _map!;
            set => _map = value;
        }

        public IDictionary<string, string> AutoMap { get; set; } = null!;
    }

    public interface IVersionedV2Table : IRelation<VersionedV2>
    {
        VersionedV2 FindById(ulong id);
        bool UpdateById(ulong id, string name);
        bool UpdateById(ulong id, long added);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void UpdateByIdUpgradesValueStoredInOldVersion(bool noEmit)
    {
        RunInMode(noEmit, () =>
        {
            using (var tr = _db.StartTransaction())
            {
                var t = tr.InitRelation<IVersionedV1Table>("Versioned")(tr);
                t.Insert(new() { Id = 1, Name = "a", Removed = 5 });
                t.Insert(new() { Id = 2, Name = "b", Removed = 6 });
                tr.Commit();
            }

            ReopenDb();
            using (var tr = _db.StartTransaction())
            {
                var t = tr.InitRelation<IVersionedV2Table>("Versioned")(tr);
                var old = t.FindById(2);
                Assert.Equal(("b", 0L), (old.Name, old.Added));
                // fields which need initialization must be initialized when loading old version
                Assert.NotNull(old.Map);
                Assert.NotNull(old.AutoMap);
                Assert.True(t.UpdateById(1, "aa"));
                Assert.True(t.UpdateById(2, 42L));
                Assert.False(t.UpdateById(3, "x"));
                var v = t.FindById(1);
                Assert.Equal(("aa", 0L), (v.Name, v.Added));
                v = t.FindById(2);
                Assert.Equal(("b", 42L), (v.Name, v.Added));
                v.Map[1] = 2;
                v.AutoMap["k"] = "v";
                Assert.Equal(2ul, t.FindById(2).Map[1]);
                Assert.Equal("v", t.FindById(2).AutoMap["k"]);
                tr.Commit();
            }
        });
    }
}
