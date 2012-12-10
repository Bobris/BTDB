using System;
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
            uint _lastEventCount;

            public bool ObservedMetadata(object metadata, uint eventCount)
            {
                Metadata.Add(metadata);
                _lastEventCount = eventCount;
                return true;
            }

            public void ObservedEvents(object[] events)
            {
                Assert.AreEqual(_lastEventCount, events.Length);
                Events.Add(events);
            }
        }

        [Test]
        public void CanWriteMultipleEventsWithMetadata()
        {
            var manager = new EventStoreManager();
            var appender = manager.AppendToStore(new MemoryEventFileStorage());
            var metadata = new ObjectDbTest.Person { Name = "A", Age = 1 };
            var events = new object[]
                {
                    new ObjectDbTest.Person { Name = "B", Age = 2 },
                    new ObjectDbTest.Person { Name = "C", Age = 3 }
                };
            appender.Store(metadata, events).Wait();
            var eventObserver = new StoringEventObserver();
            appender.ReadFromStartToEnd(eventObserver).Wait();
            Assert.AreEqual(new object[] { metadata }, eventObserver.Metadata);
            Assert.AreEqual(new[] { events }, eventObserver.Events);
        }

        [Test]
        public void CanWriteSimpleEventAndReadItIndependently()
        {
            var manager = new EventStoreManager();
            manager.SetNewTypeNameMapper(new TypeMapper());
            var file = new MemoryEventFileStorage();
            var appender = manager.AppendToStore(file);
            var person = new ObjectDbTest.Person { Name = "A", Age = 1 };
            appender.Store(null, new object[] { person }).Wait();

            manager = new EventStoreManager();
            manager.SetNewTypeNameMapper(new TypeMapper());
            var reader = manager.OpenReadOnlyStore(file);
            var eventObserver = new StoringEventObserver();
            reader.ReadFromStartToEnd(eventObserver).Wait();
            Assert.AreEqual(new object[] { null }, eventObserver.Metadata);
            Assert.AreEqual(new[] { new object[] { person } }, eventObserver.Events);
        }

        public class TypeMapper : ITypeNameMapper
        {
            public string ToName(Type type)
            {
                if (type == typeof(ObjectDbTest.Person)) return "Person";
                throw new ArgumentOutOfRangeException();
            }

            public Type ToType(string name)
            {
                if (name == "Person") return typeof (ObjectDbTest.Person);
                throw new ArgumentOutOfRangeException();
            }
        }
    }
}