using BTDB;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace AotActionsSmoke;

[Generate]
public class Row
{
    [PrimaryKey(1)] public ulong Id { get; set; }
    [SecondaryKey("Name")] public string Name { get; set; } = "";
    public Payload? Payload { get; set; }
    public IDictionary<int, string>? Lazy { get; set; }
    public IOrderedSet<string>? Set { get; set; }
}

public interface IRows : IRelation<Row>
{
    void Insert(Row row);
    void Update(Row row);
    Row? FindByIdOrDefault(ulong id);
    IEnumerable<Row> ListByName(AdvancedEnumeratorParam<string> param);
    bool RemoveById(ulong id);
}

[Generate]
public class Root
{
    public IOrderedDictionary<ulong, Detail>? ById { get; set; }
    public string Title { get; set; } = "";
}

[Generate]
public class Holder
{
    public object? Value { get; set; }
}

[Generate]
public class StoredV1
{
    public int Count { get; set; }
    public string Gone { get; set; } = "";
}

[Generate]
public class StoredV2
{
    public long Count { get; set; }
    public string Added { get; set; } = "default";
}

[Generate]
public class RowV1
{
    [PrimaryKey(1)] public ulong Id { get; set; }
    public int Count { get; set; }
    public string Gone { get; set; } = "";
    public List<int>? Items { get; set; }
    public int Plain { get; set; }
}

[Generate]
public class RowV2
{
    [PrimaryKey(1)] public ulong Id { get; set; }
    [SecondaryKey("Count")] public long Count { get; set; }
    public string Added { get; set; } = "default";
    public List<long>? Items { get; set; }
    public int? Plain { get; set; }
}

[PersistedName("Upgraded")]
public interface IRowsV1 : IRelation<RowV1>
{
    void Insert(RowV1 row);
}

[PersistedName("Upgraded")]
public interface IRowsV2 : IRelation<RowV2>
{
    RowV2? FindByIdOrDefault(ulong id);
    IEnumerable<RowV2> ListByCount(AdvancedEnumeratorParam<long> param);
}

[Generate]
public class EventV1
{
    public int Count { get; set; }
    public List<int>? Items { get; set; }
    public Dictionary<string, int>? Map { get; set; }
    public int Plain { get; set; }
    public Shade Shade { get; set; }
    public string Gone { get; set; } = "";
    public Detail? Nested { get; set; }
}

[Generate]
public class EventV2
{
    public long Count { get; set; }
    public List<long>? Items { get; set; }
    public Dictionary<string, long>? Map { get; set; }
    public int? Plain { get; set; }
    public Shade? Shade { get; set; }
    public string Added { get; set; } = "default";
    public Detail? Nested { get; set; }
}

/// <summary>Writes events as one type and reads them as another, as after an upgrade of the event class.</summary>
sealed class UpgradingTypeMapper(Type written, Type read) : ITypeNameMapper
{
    readonly FullNameTypeMapper _parent = new();
    public string ToName(Type type) => type == written || type == read ? "Upgraded" : _parent.ToName(type);
    public Type? ToType(string name) => name == "Upgraded" ? read : _parent.ToType(name);
}

/// <summary>The ObjectDB and event serializer paths an application uses beside its actions.</summary>
static class Storage
{
    internal static void Run()
    {
        using var files = new InMemoryFileCollection();
        using var keyValues = new BTreeKeyValueDB(files);
        using var db = new ObjectDB();
        db.Open(keyValues, false);
        Program.Step("relation with every field shape", () =>
        {
            using (var tr = db.StartTransaction())
            {
                var rows = tr.GetRelation<IRows>();
                rows.Insert(new()
                {
                    Id = 1, Name = "one", Payload = Shapes.Sample(), Lazy = new Dictionary<int, string> { [1] = "a" },
                    Set = new OrderedSet<string> { "s" }
                });
                rows.Insert(new() { Id = 2, Name = "two" });
                tr.Commit();
            }
            using (var tr = db.StartReadOnlyTransaction())
            {
                var rows = tr.GetRelation<IRows>();
                var row = rows.FindByIdOrDefault(1)!;
                var differences = Shapes.Differences(Shapes.Describe(Shapes.Sample()), Shapes.Describe(row.Payload), "row")
                    .ToList();
                if (differences.Count != 0) throw new InvalidDataException(string.Join("; ", differences));
                if (row.Lazy![1] != "a" || !row.Set!.Contains("s")) throw new InvalidDataException("lazy collections");
                if (rows.ListByName(new(EnumerationOrder.Ascending)).Count() != 2)
                    throw new InvalidDataException("secondary key");
            }
        });
        Program.Step("relation update and remove", () =>
        {
            using var tr = db.StartTransaction();
            var rows = tr.GetRelation<IRows>();
            var row = rows.FindByIdOrDefault(1)!;
            row.Lazy![2] = "b";
            row.Name = "uno";
            rows.Update(row);
            if (!rows.RemoveById(2)) throw new InvalidDataException("remove");
            tr.Commit();
        });
        Program.Step("singleton with an ordered dictionary", () =>
        {
            using (var tr = db.StartTransaction())
            {
                var root = tr.Singleton<Root>();
                root.Title = "title";
                root.ById![7] = new() { Label = "seven" };
                tr.Store(root);
                tr.Commit();
            }
            using (var tr = db.StartReadOnlyTransaction())
            {
                var root = tr.Singleton<Root>();
                if (root.Title != "title" || root.ById![7].Label != "seven") throw new InvalidDataException("singleton");
            }
        });
        Program.Step("stored object upgrade", () =>
        {
            using var upgradeFiles = new InMemoryFileCollection();
            using var upgradeKeyValues = new BTreeKeyValueDB(upgradeFiles);
            using (var first = new ObjectDB())
            {
                first.Open(upgradeKeyValues, false);
                first.RegisterType(typeof(StoredV1), "Stored");
                using var tr = first.StartTransaction();
                tr.Store(new Holder { Value = new StoredV1 { Count = 3, Gone = "x" } });
                tr.Commit();
            }
            using var second = new ObjectDB();
            second.Open(upgradeKeyValues, false);
            second.RegisterType(typeof(StoredV2), "Stored");
            using (var tr = second.StartReadOnlyTransaction())
                if (tr.Enumerate<Holder>().Single().Value is not StoredV2 { Count: 3, Added: "default" })
                    throw new InvalidDataException("upgraded object");
        });
        Program.Step("relation upgrade", () =>
        {
            using var upgradeFiles = new InMemoryFileCollection();
            using var upgradeKeyValues = new BTreeKeyValueDB(upgradeFiles);
            using (var first = new ObjectDB())
            {
                first.Open(upgradeKeyValues, false);
                using var tr = first.StartTransaction();
                tr.GetRelation<IRowsV1>().Insert(new() { Id = 1, Count = 7, Gone = "x", Items = [1, 2], Plain = 9 });
                tr.Commit();
            }
            using var second = new ObjectDB();
            second.Open(upgradeKeyValues, false);
            using (var tr = second.StartTransaction())
            {
                var rows = tr.GetRelation<IRowsV2>();
                if (rows.FindByIdOrDefault(1) is not { Count: 7, Added: "default", Plain: 9, Items: [1, 2] })
                    throw new InvalidDataException("upgraded row");
                if (rows.ListByCount(new(EnumerationOrder.Ascending)).Count() != 1)
                    throw new InvalidDataException("new secondary key");
                tr.Commit();
            }
        });
        Program.Step("event upgrade", () =>
        {
            var serializer = new EventSerializer(new UpgradingTypeMapper(typeof(EventV1), typeof(EventV1)));
            var value = new EventV1
            {
                Count = 5, Items = [1, 2], Map = new() { ["a"] = 3 }, Plain = 4, Shade = Shade.Light, Gone = "x",
                Nested = new() { Label = "nested", Score = 2 }
            };
            var metadata = serializer.Serialize(out var hasMetadata, value).ToAsyncSafe();
            var data = metadata;
            if (hasMetadata)
            {
                serializer.ProcessMetadataLog(metadata);
                data = serializer.Serialize(out _, value).ToAsyncSafe();
            }
            var deserializer = new EventDeserializer(new UpgradingTypeMapper(typeof(EventV1), typeof(EventV2)));
            if (hasMetadata) deserializer.ProcessMetadataLog(metadata);
            if (!deserializer.Deserialize(out var read, data)) throw new InvalidDataException("metadata missing");
            if (read is not EventV2
                {
                    Count: 5, Items: [1, 2], Plain: 4, Shade: Shade.Light, Added: "default", Nested.Score: 2
                } upgraded || upgraded.Map!["a"] != 3)
                throw new InvalidDataException("upgraded event");
        });
    }
}
