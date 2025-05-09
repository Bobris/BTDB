using System.Collections.Generic;
using System.Linq;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbEventSerializeTest : ObjectDbTestBase
{
    public ObjectDbEventSerializeTest(ITestOutputHelper output) : base(output)
    {
    }

    public class Item
    {
        public ulong Id { get; set; }
        public string Name { get; set; }
    }

    public class ObjWithIDictionary
    {
        [PrimaryKey(1)] public uint TenantId { get; set; }

        public IDictionary<ulong, Item> Dict { get; set; }
    }

    public interface IObjWithIDictionaryTable : IRelation<ObjWithIDictionary>
    {
    }

    [Fact]
    public void SerializeLazyDictionaryToEventLayerWorks()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IObjWithIDictionaryTable>();
        var obj = new ObjWithIDictionary
            { TenantId = 1, Dict = new Dictionary<ulong, Item> { { 1, new Item { Id = 1, Name = "A" } } } };
        t.Upsert(obj);
        var obj2 = t.First();
        Assert.IsType<ODBDictionary<ulong, Item>>(obj2.Dict);

        var storage = new MemoryEventFileStorage();

        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(storage);
        appender.Store(null, [obj2]);
        appender.FinalizeStore();

        manager = new EventStoreManager();
        var eventObserver = new EventStoreTest.StoringEventObserver();
        var reader = manager.OpenReadOnlyStore(storage);
        reader.ReadToEnd(eventObserver);

        var obj3 = eventObserver.Events[0][0] as ObjWithIDictionary;
        Assert.NotNull(obj3);
        Assert.Equal(obj.TenantId, obj3.TenantId);
        Assert.Equal(obj.Dict.Count, obj3.Dict.Count);
        Assert.Equal(obj.Dict.First().Key, obj3.Dict.First().Key);
        Assert.Equal(obj.Dict.First().Value.Id, obj3.Dict.First().Value.Id);
        Assert.Equal(obj.Dict.First().Value.Name, obj3.Dict.First().Value.Name);
    }

    [Fact]
    public void SerializeLazyDictionaryToEventLayer2Works()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IObjWithIDictionaryTable>();
        var obj = new ObjWithIDictionary
            { TenantId = 1, Dict = new Dictionary<ulong, Item> { { 1, new Item { Id = 1, Name = "A" } } } };
        t.Upsert(obj);
        var obj2 = t.First();
        Assert.IsType<ODBDictionary<ulong, Item>>(obj2.Dict);

        var serializer = new EventSerializer();

        var meta = serializer.Serialize(out var hasMetadata, obj2);
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj2);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var objx, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out objx, data));

        var obj3 = objx as ObjWithIDictionary;

        Assert.NotNull(obj3);
        Assert.Equal(obj.TenantId, obj3.TenantId);
        Assert.Equal(obj.Dict.Count, obj3.Dict.Count);
        Assert.Equal(obj.Dict.First().Key, obj3.Dict.First().Key);
        Assert.Equal(obj.Dict.First().Value.Id, obj3.Dict.First().Value.Id);
        Assert.Equal(obj.Dict.First().Value.Name, obj3.Dict.First().Value.Name);
    }

    [Fact]
    public void SerializeLazyDictionaryToEventLayerMustThrowIfForbidden()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IObjWithIDictionaryTable>();
        var obj = new ObjWithIDictionary
            { TenantId = 1, Dict = new Dictionary<ulong, Item> { { 1, new Item { Id = 1, Name = "A" } } } };
        t.Upsert(obj);
        var obj2 = t.First();
        Assert.IsType<ODBDictionary<ulong, Item>>(obj2.Dict);

        var storage = new MemoryEventFileStorage();

        var manager = new EventStoreManager(new() { ForbidSerializeLazyDBObjects = true });
        var appender = manager.AppendToStore(storage);
        Assert.Throws<BTDBException>(() => appender.Store(null, [obj2]));
    }

    [Fact]
    public void SerializeLazyDictionaryToEventLayer2MustThrowIfForbidden()
    {
        using var tr = _db.StartTransaction();
        var t = tr.GetRelation<IObjWithIDictionaryTable>();
        var obj = new ObjWithIDictionary
            { TenantId = 1, Dict = new Dictionary<ulong, Item> { { 1, new Item { Id = 1, Name = "A" } } } };
        t.Upsert(obj);
        var obj2 = t.First();
        Assert.IsType<ODBDictionary<ulong, Item>>(obj2.Dict);

        var serializer = new EventSerializer(forbidSerializationOfLazyDBObjects: true);

        Assert.Throws<BTDBException>(() => serializer.Serialize(out var hasMetadata, obj2));
    }
}
