using System.Collections.Generic;
using BTDB.EventStoreLayer;
using NUnit.Framework;

namespace BTDBTest
{
    [TestFixture]
    public class EventStoreTest
    {
        [Test]
        public void CanWriteSimpleEvent()
        {
            var manager = new EventStoreManager();
            var appender = manager.AppendToStore(new MemoryEventFileStorage());
            appender.Store(null, new object[] { 1 }).Wait();
            var eventObserver = new StoringEventObserver();
            appender.ReadFromStartToEnd(eventObserver).Wait();
            Assert.AreEqual(new object[] { null }, eventObserver.Metadata);
            Assert.AreEqual(new[] { new object[] { 1 } }, eventObserver.Events);
        }

        class StoringEventObserver : IEventStoreObserver
        {
            public readonly List<object> Metadata = new List<object>();
            public readonly List<object[]> Events = new List<object[]>();

            public bool ObservedMetadata(object metadata)
            {
                Metadata.Add(metadata);
                return true;
            }

            public void ObservedEvents(object[] events)
            {
                Events.Add(events);
            }
        }
    }
}