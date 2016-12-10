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
            public IList<Item> Items { get; set; }
        }

        [Fact]
        public void CanWriteMultipleEventsWithMetadata()
        {
            var obj = PassThroughEventStorage(new EventRoot
            {
                Items = new List<Item> { new Item {  Field = "A" } }
            });
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

        static object PassThroughEventStorage(EventRoot @event)
        {
            var manager = new EventStoreManager();
            var appender = manager.AppendToStore(new MemoryEventFileStorage());
            var events = new object[]
                {
                    @event
                };
            appender.Store(null, events);
            var eventObserver = new EventStoreTest.StoringEventObserver();
            appender.ReadFromStartToEnd(eventObserver);
            return eventObserver.Events[0][0];
        }
    }
}