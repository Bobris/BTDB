using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
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
                Items = new List<Item> { new Item {  Field = "A" } }
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

        static object PassThroughEventStorage(EventRoot @event, ITypeNameMapper mapper)
        {
            var manager = new EventStoreManager();
            var storage = new MemoryEventFileStorage();
            var appender = manager.AppendToStore(storage);
            var events = new object[]
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

    }
}