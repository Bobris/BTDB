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

        public class User : IEquatable<User>
        {
            public string Name { get; set; }
            public int Age { get; set; }

            public bool Equals(User other)
            {
                if (other == null) return false;
                return Name == other.Name && Age == other.Age;
            }
        }

        [Test]
        public void CanWriteMultipleEventsWithMetadata()
        {
            var manager = new EventStoreManager();
            var appender = manager.AppendToStore(new MemoryEventFileStorage());
            var metadata = new User { Name = "A", Age = 1 };
            var events = new object[]
                {
                    new User { Name = "B", Age = 2 },
                    new User { Name = "C", Age = 3 }
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
            manager.SetNewTypeNameMapper(new SimplePersonTypeMapper());
            var file = new MemoryEventFileStorage();
            var appender = manager.AppendToStore(file);
            var user = new User { Name = "A", Age = 1 };
            appender.Store(null, new object[] { user }).Wait();

            manager = new EventStoreManager();
            manager.SetNewTypeNameMapper(new SimplePersonTypeMapper());
            var reader = manager.OpenReadOnlyStore(file);
            var eventObserver = new StoringEventObserver();
            reader.ReadFromStartToEnd(eventObserver).Wait();
            Assert.AreEqual(new object[] { null }, eventObserver.Metadata);
            Assert.AreEqual(new[] { new object[] { user } }, eventObserver.Events);
        }

        public class SimplePersonTypeMapper : ITypeNameMapper
        {
            public string ToName(Type type)
            {
                if (type == typeof(User)) return "User";
                throw new ArgumentOutOfRangeException();
            }

            public Type ToType(string name)
            {
                if (name == "User") return typeof(User);
                throw new ArgumentOutOfRangeException();
            }
        }

        public class GenericTypeMapper : ITypeNameMapper
        {
            public string ToName(Type type)
            {
                return type.FullName;
            }

            public Type ToType(string name)
            {
                return Type.GetType(name, false);
            }
        }

        public class UserEvent : IEquatable<UserEvent>
        {
            public long Id { get; set; }
            public User User1 { get; set; }
            public User User2 { get; set; }

            public bool Equals(UserEvent other)
            {
                if (Id != other.Id) return false;
                if (User1 != other.User1 && (User1 == null || !User1.Equals(other.User1))) return false;
                return User2 == other.User2 || (User2 != null && User2.Equals(other.User2));
            }
        }

        [Test]
        public void NestedObjectsTest()
        {
            var manager = new EventStoreManager();
            manager.SetNewTypeNameMapper(new GenericTypeMapper());
            var file = new MemoryEventFileStorage();
            var appender = manager.AppendToStore(file);
            var userEvent = new UserEvent { Id = 10, User1 = new User { Name = "A", Age = 1 } };
            appender.Store(null, new object[] { userEvent }).Wait();

            manager = new EventStoreManager();
            manager.SetNewTypeNameMapper(new GenericTypeMapper());
            var reader = manager.OpenReadOnlyStore(file);
            var eventObserver = new StoringEventObserver();
            reader.ReadFromStartToEnd(eventObserver).Wait();
            Assert.AreEqual(new object[] { null }, eventObserver.Metadata);
            Assert.AreEqual(new[] { new object[] { userEvent } }, eventObserver.Events);
        }

        [Test]
        public void SameReferenceTest()
        {
            var manager = new EventStoreManager();
            manager.SetNewTypeNameMapper(new GenericTypeMapper());
            var file = new MemoryEventFileStorage();
            var appender = manager.AppendToStore(file);
            var user = new User {Name = "A", Age = 1};
            var userEvent = new UserEvent { Id = 10, User1 = user, User2 = user };
            appender.Store(null, new object[] { userEvent }).Wait();

            manager = new EventStoreManager();
            manager.SetNewTypeNameMapper(new GenericTypeMapper());
            var reader = manager.OpenReadOnlyStore(file);
            var eventObserver = new StoringEventObserver();
            reader.ReadFromStartToEnd(eventObserver).Wait();
            var readUserEvent = (UserEvent) eventObserver.Events[0][0];
            Assert.AreSame(readUserEvent.User1, readUserEvent.User2);
        }
    }
}