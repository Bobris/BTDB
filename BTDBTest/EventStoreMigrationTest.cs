using System.Collections.Generic;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using Xunit;

namespace BTDBTest
{
    public class EventStoreMigrationTest
    {
        public class Item
        {
            public string Field { get; set; }
        }

        public class EventRoot
        {
            public List<Item> Items { get; set; }
        }

        [Fact]
        public void CanMigrateList()
        {
            var obj = PassThroughEventStorage(new EventRoot
            {
                Items = new List<Item> { new Item { Field = "A" } }
            }, new FullNameTypeMapper());
            var serializer = new EventSerializer();
            bool hasMetadata;
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        static object PassThroughEventStorage(object @event, ITypeNameMapper mapper)
        {
            var manager = new EventStoreManager();
            var storage = new MemoryEventFileStorage();
            var appender = manager.AppendToStore(storage);
            var events = new[]
                {
                    @event
                };
            appender.Store(null, events);
            manager = new EventStoreManager();
            manager.SetNewTypeNameMapper(mapper);
            var eventObserver = new EventStoreTest.StoringEventObserver();
            manager.OpenReadOnlyStore(storage).ReadFromStartToEnd(eventObserver);
            return eventObserver.Events[0][0];
        }

        public class Item2
        {
            public string Field { get; set; }
            public string Field2 { get; set; }
        }

        public class EventRoot2
        {
            public List<Item2> Items { get; set; }
        }

        [Fact]
        public void CanMigrateListWithNewFields()
        {
            var parentMapper = new FullNameTypeMapper();
            var mapper = new EventStoreTest.OverloadableTypeMapper(typeof(Item2), parentMapper.ToName(typeof(Item)),
                new EventStoreTest.OverloadableTypeMapper(typeof(EventRoot2), parentMapper.ToName(typeof(EventRoot)),
                parentMapper
                ));
            var obj = PassThroughEventStorage(new EventRoot
            {
                Items = new List<Item> { new Item { Field = "A" } }
            }, mapper);
            var serializer = new EventSerializer(mapper);
            bool hasMetadata;
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer(mapper);
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        public class EventDictListRoot
        {
            public IDictionary<ulong, IList<Item>> Items { get; set; }
        }

        [Fact]
        public void CanMigrateListInDict()
        {
            var obj = PassThroughEventStorage(new EventDictListRoot
            {
                Items = new Dictionary<ulong, IList<Item>> { { 1, new List<Item> { new Item { Field = "A" } } } }
            }, new FullNameTypeMapper());
            var serializer = new EventSerializer();
            bool hasMetadata;
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        public enum ItemEn
        {
            One = 1
        }

        public class EventRootEn
        {
            public List<ItemEn> Items { get; set; }
        }

        public enum ItemEn2
        {
            One = 1,
            Two = 2
        }

        public class EventRootEn2
        {
            public List<ItemEn2> Items { get; set; }
        }

        [Fact]
        public void CanMigrateListWithChangedEnum()
        {
            var parentMapper = new FullNameTypeMapper();
            var mapper = new EventStoreTest.OverloadableTypeMapper(typeof(ItemEn2), parentMapper.ToName(typeof(ItemEn)),
                new EventStoreTest.OverloadableTypeMapper(typeof(EventRootEn2), parentMapper.ToName(typeof(EventRootEn)),
                parentMapper
                ));
            var obj = PassThroughEventStorage(new EventRootEn
            {
                Items = new List<ItemEn> { ItemEn.One }
            }, mapper);
            var serializer = new EventSerializer(mapper);
            bool hasMetadata;
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer(mapper);
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        public class EventWithInt
        {
            public int A { get; set; }
        }

        public class EventWithUlong
        {
            public ulong A { get; set; }
        }

        [Fact]
        public void CanMigrateIntToUlong()
        {
            var parentMapper = new FullNameTypeMapper();
            var mapper = new EventStoreTest.OverloadableTypeMapper(typeof(EventWithUlong), parentMapper.ToName(typeof(EventWithInt)),
                    parentMapper
                );
            var obj = (EventWithUlong)PassThroughEventStorage(new EventWithInt
            {
                A = 42
            }, mapper);
            Assert.Equal(42ul, obj.A);
            var obj2 = (EventWithUlong)PassThroughEventStorage(new EventWithInt
            {
                A = -1
            }, mapper);
            Assert.Equal(0xffffffff, obj2.A);
        }
    }
}
