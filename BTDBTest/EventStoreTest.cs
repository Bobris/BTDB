using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB;
using BTDB.Buffer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using Xunit;
using NoCompressionStrategy = BTDB.EventStoreLayer.NoCompressionStrategy;

namespace BTDBTest;

public class EventStoreTest
{
    [Fact]
    public void WhenReadOnlyStoreIsCreatedFromNewEventStoreManagerThenItShouldNotLeakMemory()
    {
        var storage = new MemoryEventFileStorage();

        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(storage);
        var metadata = new User { Name = "A", Age = 1 };
        var events = new object[]
        {
            new User { Name = "B", Age = 2 },
            new User { Name = "C", Age = 3 }
        };
        appender.Store(metadata, events);
        appender.FinalizeStore();

        manager = new EventStoreManager();
        var eventObserver = new StoringEventObserver();
        long baselineMemory = 0;
        for (int i = 0; i <= 10000; i++)
        {
            var reader = manager.OpenReadOnlyStore(storage);
            reader.ReadToEnd(eventObserver);

            Assert.Single(eventObserver.Events);
            eventObserver.Events.Clear();
            eventObserver.Metadata.Clear();

            if (i == 10)
            {
                GC.Collect(2);
                GC.WaitForPendingFinalizers();
                GC.Collect(2);
                baselineMemory = GC.GetTotalMemory(false);
            }
        }

        GC.Collect(2);
        GC.WaitForPendingFinalizers();
        GC.Collect(2);
        Assert.InRange(GC.GetTotalMemory(false), 0, baselineMemory * 3F);
    }

    [Fact]
    public void CanWriteSimpleEvent()
    {
        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());
        appender.Store(null, new object[] { 1 });
        var eventObserver = new StoringEventObserver();
        appender.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        Assert.Equal(new[] { new object[] { 1 } }, eventObserver.Events);
        Assert.True(appender.IsKnownAsAppendable());
        Assert.False(appender.IsKnownAsCorrupted());
        Assert.False(appender.IsKnownAsFinished());
        Assert.Equal(10ul, appender.KnownAppendablePosition());
    }

    [Fact]
    public void CanFinalizeEventStore()
    {
        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());
        appender.FinalizeStore();
        Assert.False(appender.IsKnownAsAppendable());
        Assert.False(appender.IsKnownAsCorrupted());
        Assert.True(appender.IsKnownAsFinished());
    }

    [Fact]
    public void CanFinalizeEventStoreAfterReadFromStart()
    {
        var manager = new EventStoreManager();
        manager.CompressionStrategy = new NoCompressionStrategy();
        var file1 = new MemoryEventFileStorage(4096, 4096);
        var appender = manager.AppendToStore(file1);
        appender.Store(null, new object[] { new byte[4000] });
        appender.Store(null, new object[] { new byte[4000] });
        Assert.NotSame(file1, appender.CurrentFileStorage);
        var reader = manager.OpenReadOnlyStore(file1);
        reader.ReadFromStartToEnd(new SkippingEventObserver());
        Assert.False(reader.IsKnownAsAppendable());
        Assert.False(reader.IsKnownAsCorrupted());
        Assert.True(reader.IsKnownAsFinished());
        Assert.True(appender.IsKnownAsAppendable());
        Assert.False(appender.IsKnownAsCorrupted());
        Assert.False(appender.IsKnownAsFinished());
    }

    [Fact]
    public void CanReadLongerEventsFromIncompleteFile()
    {
        var manager = new EventStoreManager();
        manager.CompressionStrategy = new NoCompressionStrategy();
        var file1 = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file1);
        appender.Store(null, new object[] { new byte[8000] });
        var file2 = new MemoryEventFileStorage();
        var buf = ByteBuffer.NewSync(new byte[4096]);
        file1.Read(buf, 0);
        file2.Write(buf, 0);
        var reader = manager.OpenReadOnlyStore(file2);
        reader.ReadFromStartToEnd(new SkippingEventObserver());
        Assert.False(reader.IsKnownAsCorrupted());
        Assert.False(reader.IsKnownAsAppendable());
        Assert.False(reader.IsKnownAsFinished());
        buf = ByteBuffer.NewSync(new byte[4096]);
        file1.Read(buf, 4096);
        file2.Write(buf, 4096);
        reader.ReadToEnd(new SkippingEventObserver());
        Assert.False(reader.IsKnownAsCorrupted());
        Assert.True(reader.IsKnownAsAppendable());
        Assert.False(reader.IsKnownAsFinished());
    }

    [Fact]
    public void CanFinalizeEventStoreOnEverySectorPosition()
    {
        for (int i = 4000; i < 4600; i++)
        {
            var manager = new EventStoreManager();
            manager.CompressionStrategy = new NoCompressionStrategy();
            var file1 = new MemoryEventFileStorage(4096, 8192);
            var appender = manager.AppendToStore(file1);
            appender.Store(null, new object[] { new byte[i] });
            appender.Store(null, new object[] { new byte[7000] });
        }
    }

    [Fact]
    public void CanFinalizeEventStoreAfterReadFromStartFirstNearlyFull()
    {
        var manager = new EventStoreManager();
        manager.CompressionStrategy = new NoCompressionStrategy();
        var file1 = new MemoryEventFileStorage(4096, 4096);
        var appender = manager.AppendToStore(file1);
        appender.Store(null, new object[] { new byte[4080] });
        appender.Store(null, new object[] { new byte[4000] });
        Assert.NotSame(file1, appender.CurrentFileStorage);
        var reader = manager.OpenReadOnlyStore(file1);
        reader.ReadFromStartToEnd(new SkippingEventObserver());
        Assert.False(reader.IsKnownAsAppendable());
        Assert.False(reader.IsKnownAsCorrupted());
        Assert.True(reader.IsKnownAsFinished());
        Assert.True(appender.IsKnownAsAppendable());
        Assert.False(appender.IsKnownAsCorrupted());
        Assert.False(appender.IsKnownAsFinished());
    }

    [Fact]
    public void CreatesNewFileWhenOldOneIsFull()
    {
        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());
        appender.Store(null, new object[] { 1 });
        appender.ReadFromStartToEnd(new SkippingEventObserver());
        appender.FinalizeStore();
        Assert.False(appender.IsKnownAsAppendable());
        Assert.False(appender.IsKnownAsCorrupted());
        Assert.True(appender.IsKnownAsFinished());
    }

    internal class StoringEventObserver : IEventStoreObserver
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

        public virtual bool ShouldStopReadingNextEvents()
        {
            return false;
        }

        public void ObservedEvents(object[] events)
        {
            Assert.Equal((int)_lastEventCount, events.Length);
            Events.Add(events);
        }
    }

    class StoringEventObserverWithStop : StoringEventObserver
    {
        public override bool ShouldStopReadingNextEvents()
        {
            return true;
        }
    }

#pragma warning disable 659

    [Generate]
    public class User : IEquatable<User>
    {
        public string Name { get; set; }
        public int Age { get; set; }

        public bool Equals(User other)
        {
            if (other == null) return false;
            return Name == other.Name && Age == other.Age;
        }


        public override bool Equals(object obj)
        {
            return Equals(obj as User);
        }
    }
#pragma warning restore 659

    [Fact]
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
        appender.Store(metadata, events);
        var eventObserver = new StoringEventObserver();
        appender.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { metadata }, eventObserver.Metadata);
        Assert.Equal(new[] { events }, eventObserver.Events);
    }

    [Fact]
    public void CanWriteSimpleEventAndReadItIndependently()
    {
        var manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new SimplePersonTypeMapper());
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var user = new User { Name = "A", Age = 1 };
        appender.Store(null, new object[] { user });

        manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new SimplePersonTypeMapper());
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        Assert.Equal(new[] { new object[] { user } }, eventObserver.Events);
    }

    [Fact]
    public void CustomEventIsReadFromSecondSplit()
    {
        var manager = new EventStoreManager();
        manager.CompressionStrategy = new NoCompressionStrategy();
        manager.SetNewTypeNameMapper(new SimplePersonTypeMapper());
        var appender = manager.AppendToStore(new MemoryEventFileStorage(4096, 4096));
        var first = appender.CurrentFileStorage;
        var user = new User { Name = "A", Age = 1 };
        while (appender.CurrentFileStorage == first)
            appender.Store(null, new object[] { user });
        var second = appender.CurrentFileStorage;

        manager = new EventStoreManager();
        manager.CompressionStrategy = new NoCompressionStrategy();
        manager.SetNewTypeNameMapper(new SimplePersonTypeMapper());
        var reader = manager.OpenReadOnlyStore(second);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        Assert.Equal(new[] { new object[] { user } }, eventObserver.Events);
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

#pragma warning disable 659
    [Generate]
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

        public override bool Equals(object obj)
        {
            return Equals(obj as UserEvent);
        }
    }
#pragma warning restore 659

    [Fact]
    public void NestedObjectsTest()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var userEvent = new UserEvent { Id = 10, User1 = new User { Name = "A", Age = 1 } };
        appender.Store(null, new object[] { userEvent });

        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        Assert.Equal(new[] { new object[] { userEvent } }, eventObserver.Events);
    }

    [Fact]
    public void SameReferenceTest()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var user = new User { Name = "A", Age = 1 };
        var userEvent = new UserEvent { Id = 10, User1 = user, User2 = user };
        appender.Store(null, [userEvent]);

        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        var readUserEvent = (UserEvent)eventObserver.Events[0][0];
        Assert.Same(readUserEvent.User1, readUserEvent.User2);
    }

    [Generate]
    public class UserEventMore : IEquatable<UserEventMore>
    {
        public long Id { get; set; }
        public User User1 { get; set; }
        public User User2 { get; set; }
        public User User3 { get; set; }

        public bool Equals(UserEventMore other)
        {
            if (Id != other.Id) return false;
            if (User1 != other.User1 && (User1 == null || !User1.Equals(other.User1))) return false;
            if (User2 != other.User2 && (User2 == null || !User2.Equals(other.User2))) return false;
            return User3 == other.User3 || (User3 != null && User3.Equals(other.User3));
        }
    }

    public class OverloadableTypeMapper : ITypeNameMapper
    {
        readonly ITypeNameMapper _parent;
        readonly Type _type;
        readonly string _name;

        public OverloadableTypeMapper(Type type, string name, ITypeNameMapper parent = null)
        {
            _type = type;
            _name = name;
            _parent = parent ?? new FullNameTypeMapper();
        }

        public string ToName(Type type)
        {
            if (type == _type) return _name;
            return _parent.ToName(type);
        }

        public Type ToType(string name)
        {
            if (name == _name) return _type;
            return _parent.ToType(name);
        }
    }

    [Fact]
    public void UpgradeToMoreObjectProperties()
    {
        var manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(UserEvent), "UserEvent"));
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var user = new User { Name = "A", Age = 1 };
        var userEvent = new UserEvent { Id = 10, User1 = user, User2 = user };
        appender.Store(null, new object[] { userEvent, new User { Name = "B" } });

        manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(UserEventMore), "UserEvent"));
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        var readUserEvent = (UserEventMore)eventObserver.Events[0][0];
        Assert.Same(readUserEvent.User1, readUserEvent.User2);
        Assert.Equal("A", readUserEvent.User1.Name);
        Assert.Equal(10, readUserEvent.Id);
        Assert.Null(readUserEvent.User3);
        Assert.Equal("B", ((User)eventObserver.Events[0][1]).Name);
    }

    public class UserEventLess : IEquatable<UserEventLess>
    {
        public long Id { get; set; }
        public User User2 { get; set; }

        public bool Equals(UserEventLess other)
        {
            if (Id != other.Id) return false;
            return User2 == other.User2 || (User2 != null && User2.Equals(other.User2));
        }
    }

    [Fact]
    public void UpgradeToLessObjectProperties()
    {
        var manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(UserEvent), "UserEvent"));
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var user = new User { Name = "A", Age = 1 };
        var userEvent = new UserEvent { Id = 10, User1 = user, User2 = user };
        appender.Store(null, new object[] { userEvent, new User { Name = "B" } });

        manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(UserEventLess), "UserEvent"));
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        var readUserEvent = (UserEventLess)eventObserver.Events[0][0];
        Assert.Equal("A", readUserEvent.User2.Name);
        Assert.Equal(10, readUserEvent.Id);
        Assert.Equal("B", ((User)eventObserver.Events[0][1]).Name);
    }

    public enum ApplicationsType
    {
        None = 0,
        First = 1,
    }

    public enum ApplicationsRenamedType
    {
        None = 0,
        First = 1,
    }

    public class ApplicationInfo
    {
        public ApplicationsType Type { get; set; }
    }

    public class ApplicationInfoPropertyEnumTypeChanged
    {
        public ApplicationsRenamedType Type { get; set; }
    }

    [Fact]
    public void UpgradeToDifferentEnumProperties()
    {
        var manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(ApplicationInfo), "ApplicationInfo"));
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var applicationInfo = new ApplicationInfo { Type = ApplicationsType.First };
        appender.Store(null, new object[] { applicationInfo });

        manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(ApplicationInfoPropertyEnumTypeChanged),
            "ApplicationInfo"));
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        var readApplicationInfo = (ApplicationInfoPropertyEnumTypeChanged)eventObserver.Events[0][0];
        Assert.Equal(ApplicationsRenamedType.First, readApplicationInfo.Type);
    }

    [Generate]
    public class UserEventList : IEquatable<UserEventList>
    {
        public long Id { get; set; }
        public List<User> List { get; set; }

        public bool Equals(UserEventList other)
        {
            if (Id != other.Id) return false;
            if (List == other.List) return true;
            if (List == null || other.List == null) return false;
            if (List.Count != other.List.Count) return false;
            return List.Zip(other.List, (u1, u2) => u1 == u2 || (u1 != null && u1.Equals(u2))).All(b => b);
        }
    }

    [Fact]
    public void SupportsList()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var userA = new User { Name = "A", Age = 1 };
        var userB = new User { Name = "B", Age = 2 };
        var userEvent = new UserEventList { Id = 10, List = new List<User> { userA, userB, userA } };
        appender.Store(null, new object[] { userEvent });

        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        var readUserEvent = (UserEventList)eventObserver.Events[0][0];
        Assert.Equal(readUserEvent, userEvent);
    }

    [Fact]
    public void SkipListOnUpgrade()
    {
        var manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(UserEventList), "UserEvent"));
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var userA = new User { Name = "A", Age = 1 };
        var userB = new User { Name = "B", Age = 2 };
        var userEvent = new UserEventList { Id = 10, List = new() { userA, userB, userA } };
        appender.Store(null, new object[] { userEvent });

        manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(UserEvent), "UserEvent"));
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        var readUserEvent = (UserEvent)eventObserver.Events[0][0];
        Assert.Equal(10, readUserEvent.Id);
        Assert.Null(readUserEvent.User1);
    }

    [Generate]
    public class UserEventDictionary : IEquatable<UserEventDictionary>
    {
        public long Id { get; set; }
        public Dictionary<string, User> Dict { get; set; }

        public bool Equals(UserEventDictionary other)
        {
            if (Id != other.Id) return false;
            if (Dict == other.Dict) return true;
            if (Dict == null || other.Dict == null) return false;
            if (Dict.Count != other.Dict.Count) return false;
            foreach (var p in Dict)
            {
                User u;
                if (!other.Dict.TryGetValue(p.Key, out u)) return false;
                if (p.Value != u && (p.Value == null || !p.Value.Equals(u))) return false;
            }

            return true;
        }
    }

    [Fact]
    public void SupportsDictionary()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var userA = new User { Name = "A", Age = 1 };
        var userB = new User { Name = "B", Age = 2 };
        var userEvent = new UserEventDictionary
            { Id = 10, Dict = new Dictionary<string, User> { { "A", userA }, { "B", userB } } };
        appender.Store(null, new object[] { userEvent });

        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        var readUserEvent = (UserEventDictionary)eventObserver.Events[0][0];
        Assert.Equal(readUserEvent, userEvent);
    }

    [Fact]
    public void SupportsTuple()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var userEvent = (1, 2u);
        appender.Store(null, new object[] { userEvent });

        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        var readUserEvent = (ValueTuple<int, uint>)eventObserver.Events[0][0];
        Assert.Equal(readUserEvent, userEvent);
    }

    [Generate]
    public class ErrorInfo
    {
        public IDictionary<string, IList<ErrorInfo>> PropertyErrors { get; set; }
    }

    [Fact]
    public void DeserializeErrorInfoWorks()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var obj = new ErrorInfo();
        obj.PropertyErrors = new Dictionary<string, IList<ErrorInfo>>();
        var items = obj.PropertyErrors;
        items["a"] = new List<ErrorInfo> { new ErrorInfo() };
        appender.Store(null, new object[] { obj });

        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        var readUserEvent = (ErrorInfo)eventObserver.Events[0][0];
        Assert.Equal(1, readUserEvent.PropertyErrors.Count);
    }

    [Fact]
    public void SupportsDataOverMaxBlockSize()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var randomData = new byte[20000];
        new Random().NextBytes(randomData);
        appender.Store(null, new object[] { randomData });
        Assert.True(10000 < appender.KnownAppendablePosition());

        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        Assert.Equal(new[] { new object[] { randomData } }, eventObserver.Events);
    }

    [Fact]
    public void CompressionShortensData()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var compressibleData = new byte[20000];
        appender.Store(null, new object[] { compressibleData });
        Assert.True(2000 > appender.KnownAppendablePosition());

        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        Assert.Equal(new[] { new object[] { compressibleData } }, eventObserver.Events);
    }

    [Fact]
    public void CanStopReadBatchesAfterFirst()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var metadata = new User { Name = "A", Age = 1 };
        var events = new object[]
        {
            new User { Name = "B", Age = 2 },
            new User { Name = "C", Age = 3 }
        };
        appender.Store(metadata, events);
        appender.Store(metadata, events);

        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserverWithStop();
        reader.ReadFromStartToEnd(eventObserver);
        Assert.False(reader.IsKnownAsCorrupted());
        Assert.False(reader.IsKnownAsFinished());
        Assert.False(reader.IsKnownAsAppendable());
        Assert.Equal(new List<object> { metadata }, eventObserver.Metadata);
        Assert.Equal(new[] { events }, eventObserver.Events);
    }

    [Fact]
    public void MoreComplexRepeatedAppendingAndReading()
    {
        var manager = new EventStoreManager();
        for (var i = 490; i < 520; i += 2)
        {
            var file = new MemoryEventFileStorage();
            var appender = manager.AppendToStore(file);
            appender.Store(null, new object[] { new byte[i] });
            var eventObserver = new StoringEventObserver();
            appender.ReadFromStartToEnd(eventObserver);
            appender.Store(null, new object[] { new byte[i] });
            appender.ReadFromStartToEnd(eventObserver);
        }
    }

    public class SpecificList
    {
        public List<ulong> Ulongs { get; set; }
    }

    public class SpecificDictIList
    {
        public IDictionary<ulong, IList<ulong>> Dict { get; set; }
    }

    [Fact]
    public void MixedIListAndList()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var e1 = new SpecificDictIList { Dict = new Dictionary<ulong, IList<ulong>>() };
        var e2 = new SpecificList { Ulongs = new List<ulong>() };
        appender.Store(null, new object[] { e2 });
        appender.Store(null, new object[] { e1 });

        manager = new EventStoreManager();
        appender = manager.AppendToStore(file);
        var eventObserver = new StoringEventObserver();
        appender.ReadFromStartToEnd(eventObserver);
        appender.Store(null, new object[] { e1 });
    }

    [Generate]
    public class UsersIList
    {
        public IList<User> Users { get; set; }
    }

    [Fact]
    public void UseArrayForStoreIList()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var e = new UsersIList { Users = new[] { new User { Name = "A" }, new User { Name = "B" } } };
        appender.Store(null, new object[] { e });
    }

    public class ClassWithChangedUINTtoULONG
    {
        public ulong A { get; set; }
        public uint B { get; set; }
    }

    public class Ev1
    {
        public ClassWithChangedUINTtoULONG Credit { get; set; }
    }

    // event stream generated with uint  (ClassWithChangedUINTtoULONG.A)
    string base64EventFile =
        "J3uX1C1UAAAzAR9CVERCVGVzdC5FdmVudFN0b3JlVGVzdCtDcmVkaXQCAkEIAkIIMgEcQlREQlRlc3QuRXZlbnRTdG9yZVRlc3QrRXYxAQdDcmVkaXQzADIzAQIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    [Fact]
    public void Read()
    {
        var manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(ClassWithChangedUINTtoULONG),
            "BTDBTest.EventStoreTest+Credit"));

        using (var file = new StreamEventFileStorage(new MemoryStream(Convert.FromBase64String(base64EventFile))))
        {
            var appender = manager.AppendToStore(file);

            var observer = new StoringEventObserver();
            appender.ReadFromStartToEnd(observer);
            Assert.Single(observer.Events);
            Assert.Single(observer.Events[0]);
            var e = observer.Events[0][0] as Ev1;
            Assert.Equal(1u, e.Credit.A);
            Assert.Equal(2u, e.Credit.B);
        }
    }

    public class EventDictionaryInDictionary
    {
        public Dictionary<string, IDictionary<string, string>> DictInDict { get; set; }
    }

    [Fact]
    public void SupportsDictionaryInDictionary()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);

        var dictInDictEvent = new EventDictionaryInDictionary
        {
            DictInDict = new Dictionary<string, IDictionary<string, string>>
            {
                { "level-A", new Dictionary<string, string> { { "level-B", "level-C" } } }
            }
        };
        appender.Store(null, new object[] { dictInDictEvent });
        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
    }

    public class EventWithIIndirect
    {
        public string Name { get; set; }
        public IIndirect<User> Ind1 { get; set; }
        public List<IIndirect<User>> Ind2 { get; set; }
    }

    [Fact]
    public void SkipsIIndirect()
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);

        var ev = new EventWithIIndirect
        {
            Name = "A",
            Ind1 = new DBIndirect<User>(),
            Ind2 = new List<IIndirect<User>>()
        };
        appender.Store(null, new object[] { ev });
        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
    }

    public class SomethingWithoutList
    {
    }

    public class SomethingWithList
    {
        public Dictionary<ulong, List<string>> B { get; set; }
    }

    public class SomethingWithNestedIList
    {
        public Dictionary<ulong, IList<string>> B { get; set; }
    }

    [Fact]
    public void SurvivesListVsIList()
    {
        var manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(SomethingWithoutList), "Some"));
        var file = new MemoryEventFileStorage();
        var file2 = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);

        var ev = new SomethingWithList();
        appender.Store(null, new object[] { ev });
        var ev2 = new SomethingWithNestedIList
        {
            B = new Dictionary<ulong, IList<string>> { { 1, new List<string> { "a1" } } }
        };
        appender.Store(null, new object[] { ev2 });
        var appender2 = manager.AppendToStore(file);
        appender2.Store(null, new object[] { ev2 });
        manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new OverloadableTypeMapper(typeof(SomethingWithList), "Some"));
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        reader = manager.OpenReadOnlyStore(file2);
        reader.ReadFromStartToEnd(eventObserver);
    }

    public class SimpleWithIndexer
    {
        public string OddName { get; set; }
        public string EvenName { get; set; }
        public string this[int i] => i % 2 == 0 ? EvenName : OddName;
    }

    [Fact]
    public void CanWriteEventWithIndexer()
    {
        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());
        appender.Store(null, new object[] { new SimpleWithIndexer { EvenName = "e", OddName = "o" } });
        var eventObserver = new StoringEventObserver();
        appender.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        var ev = eventObserver.Events[0][0] as SimpleWithIndexer;
        Assert.Equal("o", ev[11]);
    }

    [Generate]
    public class StrangeVisibilities
    {
        public string? A { get; internal set; }
        public string? B { get; private set; }
        public string? C { internal get; set; }
        public string? D { private get; set; }
    }

    [Fact]
    public void SupportStrangeVisibilities()
    {
        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());
        appender.Store(null, new object[] { new StrangeVisibilities { A = "a", C = "c", D = "d" } });
        var eventObserver = new StoringEventObserver();
        appender.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        var ev = eventObserver.Events[0][0] as StrangeVisibilities;
        Assert.Equal("a", ev.A);
        Assert.Null(ev.B);
        Assert.Equal("c", ev.C);
    }

    public class PureArray
    {
        public string[] A { get; set; }
        public int[] B { get; set; }
    }

    [Fact]
    public void SupportPureArray()
    {
        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());
        appender.Store(null, new object[] { new PureArray { A = new[] { "A", "B" }, B = new[] { 42, 7 } } });
        var eventObserver = new StoringEventObserver();
        appender.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        var ev = eventObserver.Events[0][0] as PureArray;
        Assert.Equal(ev.A, new[] { "A", "B" });
        Assert.Equal(ev.B, new[] { 42, 7 });
    }

    public struct Structure
    {
    }

    public class EventWithStruct
    {
        public ulong EventId { get; set; }
        public Structure Structure { get; set; }
    }

    [Fact]
    public void CannotStoreStruct()
    {
        var testEvent = new EventWithStruct
        {
            EventId = 1,
            Structure = new Structure()
        };

        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());

        var e = Assert.Throws<BTDBException>(() => appender.Store(null, new object[] { testEvent }));
        Assert.Contains("Unsupported", e.Message);
    }

    public class EventWithNullable
    {
        public ulong EventId { get; set; }
        public int? NullableInt { get; set; }
        public int? NullableEmpty { get; set; }

        public List<int?> ListWithNullables { get; set; }
        public IDictionary<int?, bool?> DictionaryWithNullables { get; set; }
    }

    [Fact]
    public void CanStoreNullable()
    {
        var testEvent = new EventWithNullable
        {
            EventId = 1,
            NullableInt = 42,
            ListWithNullables = new List<int?> { 4, new int?() },
            DictionaryWithNullables = new Dictionary<int?, bool?> { { 1, true }, { 2, new bool?() } }
        };

        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());
        appender.Store(null, new object[] { testEvent });

        var eventObserver = new StoringEventObserver();
        appender.ReadFromStartToEnd(eventObserver);
        Assert.Equal(new object[] { null }, eventObserver.Metadata);
        var ev = eventObserver.Events[0][0] as EventWithNullable;
        Assert.Equal(42, ev.NullableInt.Value);
        Assert.False(ev.NullableEmpty.HasValue);
        Assert.Equal(2, ev.ListWithNullables.Count);
        Assert.Equal(4, ev.ListWithNullables[0].Value);
        Assert.False(ev.ListWithNullables[1].HasValue);
        Assert.Equal(2, ev.DictionaryWithNullables.Count);
        Assert.True(ev.DictionaryWithNullables[1]);
        Assert.False(ev.DictionaryWithNullables[2].HasValue);
    }

    public class SelectiveTypeMapper : ITypeNameMapper
    {
        readonly ITypeNameMapper _parent;
        readonly string _name;

        public SelectiveTypeMapper(string name, ITypeNameMapper parent = null)
        {
            _name = name;
            _parent = parent ?? new FullNameTypeMapper();
        }

        public string ToName(Type type)
        {
            return _parent.ToName(type);
        }

        public Type ToType(string name)
        {
            if (name == _name) throw new EventSkippedException();
            return _parent.ToType(name);
        }
    }

    internal class SimpleEventObserver : IEventStoreObserver
    {
        public readonly List<object[]> Events = new List<object[]>();

        public bool ObservedMetadata(object metadata, uint eventCount)
        {
            return true;
        }

        public virtual bool ShouldStopReadingNextEvents()
        {
            return false;
        }

        public void ObservedEvents(object[] events)
        {
            Events.Add(events);
        }
    }

    [Fact]
    public void TypeMapperCanForceSkipEvents()
    {
        var manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new FullNameTypeMapper());
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        var user = new User { Name = "ABC", Age = 88 };
        var userEvent = new UserEvent { Id = 10, User1 = user, User2 = user };
        var userEventMore = new UserEventMore { Id = 11, User1 = user, User2 = user };

        appender.Store(null, new object[] { userEvent, userEventMore });

        manager = new EventStoreManager();
        manager.SetNewTypeNameMapper(new SelectiveTypeMapper("BTDBTest.EventStoreTest+UserEvent"));
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new SimpleEventObserver();
        reader.ReadFromStartToEnd(eventObserver);
        Assert.Single(eventObserver.Events[0]);
        var readUserEvent = (UserEventMore)eventObserver.Events[0][0];
        Assert.Same(readUserEvent.User1, readUserEvent.User2);
    }

    public class SomeSets
    {
        public ISet<string> A { get; set; }
        public HashSet<int> B { get; set; }
    }

    [Fact]
    public void SupportSets()
    {
        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());
        appender.Store(null,
            new object[] { new SomeSets { A = new HashSet<string> { "A", "B" }, B = new HashSet<int> { 42, 7 } } });
        var eventObserver = new StoringEventObserver();
        appender.ReadFromStartToEnd(eventObserver);
        var ev = eventObserver.Events[0][0] as SomeSets;
        Assert.Equal(new[] { "A", "B" }, ev!.A.OrderBy(a => a));
        Assert.Equal(new[] { 7, 42 }, ev.B.OrderBy(b => b));
    }

    [Fact]
    public void StoreListThenArray()
    {
        var a = new EventStoreManager().AppendToStore(
            new MemoryEventFileStorage());
        a.Store(null, new[] { new List<bool>() });
        a.Store(null, new[] { new[] { true } });
        a.Store(null, new[] { new HashSet<bool>() });
    }

    [Fact]
    public void StoreVariousComplexCombinations()
    {
        var a = new EventStoreManager().AppendToStore(
            new MemoryEventFileStorage());
        a.Store(null, new[] { new Dictionary<int, List<bool>> { { 1, new List<bool> { true } } } });
        a.Store(null, new[] { new Dictionary<int, IList<bool>> { { 1, new List<bool> { true } } } });
        a.Store(null, new[] { new Dictionary<int, IList<bool>> { { 1, new[] { true } } } });
    }

    public class ObjWithReadOnlyMemory
    {
        public ReadOnlyMemory<byte> Data { get; set; }
    }

    [Fact]
    public void SupportsReadOnlyMemory()
    {
        var manager = new EventStoreManager();
        var appender = manager.AppendToStore(new MemoryEventFileStorage());
        appender.Store(null, new object[] { new ObjWithReadOnlyMemory { Data = "ahoj"u8.ToArray() } });
        var eventObserver = new StoringEventObserver();
        appender.ReadFromStartToEnd(eventObserver);
        var ev = eventObserver.Events[0][0] as ObjWithReadOnlyMemory;
        Assert.Equal(ev.Data.ToArray(), "ahoj"u8.ToArray());
    }

    public interface IDynamicValue
    {
    }

    [GenerateFor(typeof(DynamicValueWrapper<Enum>))]
    [GenerateFor(typeof(DynamicValueWrapper<Money>))]
    public class DynamicValueWrapper<TValueType> : IDynamicValue
    {
        public TValueType Value { get; set; }
    }

    public class Money
    {
        public decimal MinorValue { get; init; }

        public Currency Currency { get; init; }
    }

    public class Currency
    {
        public int MinorToAmountRatio { get; init; }

        public string Code { get; init; }
    }

    [Generate]
    public class Root
    {
        public List<IDynamicValue> R { get; set; }
    }

    enum Test1
    {
        A = 325,
        B
    }

    T SerializationInternal<T>(object input)
    {
        var manager = new EventStoreManager();
        var file = new MemoryEventFileStorage();
        var appender = manager.AppendToStore(file);
        appender.Store(null, new object[] { input });
        manager = new EventStoreManager();
        var reader = manager.OpenReadOnlyStore(file);
        var eventObserver = new StoringEventObserver();
        reader.ReadFromStartToEnd(eventObserver);

        return (T)eventObserver.Events[0][0];
    }

    [Fact]
    public void CanDeserializeWithReferentialIdentityAndBoxedEnum()
    {
        var usd = new Currency() { Code = "USD", MinorToAmountRatio = 100 };
        var obj = new Root()
        {
            R =
            [
                new DynamicValueWrapper<Enum> { Value = Test1.A },
                new DynamicValueWrapper<Money>
                {
                    Value = new()
                    {
                        MinorValue = 10000,
                        Currency = usd
                    }
                },

                new DynamicValueWrapper<Money>
                {
                    Value = new()
                    {
                        MinorValue = 61000,
                        Currency = usd
                    }
                }
            ]
        };

        var obj2 = SerializationInternal<Root>(obj);
        Assert.Equal(obj.R.Count, obj2.R.Count);
        Assert.Equal("A", ((DynamicValueWrapper<Enum>)obj2.R[0]).Value.ToString());
        Assert.Same(((DynamicValueWrapper<Money>)obj2.R[1]).Value.Currency,
            ((DynamicValueWrapper<Money>)obj2.R[2]).Value.Currency);
    }
}
