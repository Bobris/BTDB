using System;
using System.Collections.Generic;
using BTDB.EventStore2Layer;
using BTDB.EventStoreLayer;
using Xunit;
using static BTDBTest.EventStoreTest;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using System.Linq;
using BTDB.Encrypted;
using BTDB.StreamLayer;

namespace BTDBTest;

public class EventStore2Test
{
    const string GivenEventsMetadataFilePath = "..\\..\\TestData\\meta.txt";
    const string GivenEventsDataFilePath = "..\\..\\TestData\\events.txt";
    const char DataFileSeparator = ' ';

    [Fact]
    public void SerializingNewObjectsWritesNewMetadata()
    {
        var serializer = new EventSerializer();

        var data = serializer.Serialize(out var hasMetadata, new User());
        Assert.True(hasMetadata);
        Assert.InRange(data.Length, 1, 100);
    }

    [Fact]
    public void ParsingMetadataStopsGeneratingThem()
    {
        var serializer = new EventSerializer();

        var meta = serializer.Serialize(out var hasMetadata, new User());
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, new User());
        Assert.False(hasMetadata);
        Assert.InRange(data.Length, 1, 10);
    }

    [Fact(Skip = "Waiting for allocated bytes method to be precise")]
    public void SerializationRunsAndDoesNotLeak1Byte()
    {
        var serializer = new EventSerializer();

        var meta = serializer.Serialize(out var hasMetadata, new User());
        serializer.ProcessMetadataLog(meta);
        long baselineMemory = 0;
        for (var i = 0; i < 100; i++)
        {
            serializer.Serialize(out hasMetadata, new User());
            Assert.False(hasMetadata);
            if (i == 2)
            {
                GC.Collect(2);
                GC.WaitForPendingFinalizers();
                GC.Collect(2);
                GC.GetTotalAllocatedBytes(true);
                baselineMemory = GC.GetTotalMemory(false);
            }
        }

        GC.Collect(2);
        GC.WaitForPendingFinalizers();
        GC.Collect(2);
        Assert.InRange(GC.GetTotalMemory(false), 0, baselineMemory + 400);
    }

    [Fact]
    public void DeserializeSimpleClass()
    {
        var serializer = new EventSerializer();
        var obj = new User { Name = "Boris", Age = 40 };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);
    }

    public enum StateEnum
    {
        Dead = 0,
        Alive = 1
    }

    public class ObjectWithEnum : IEquatable<ObjectWithEnum>
    {
        public StateEnum State { get; set; }

        public bool Equals(ObjectWithEnum other)
        {
            if (other == null) return false;
            return State == other.State;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ObjectWithEnum);
        }

        public override int GetHashCode() => (int)State;
    }

    [Fact]
    public void DeserializesClassWithEnum()
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithEnum { State = StateEnum.Alive };
        var meta = serializer.Serialize(out _, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);
    }

    public class ObjectWithList : IEquatable<ObjectWithList>
    {
        public List<int> Items { get; set; }

        public bool Equals(ObjectWithList other)
        {
            if (other == null)
                return false;

            if (Items == null && other.Items == null)
                return true;
            if (Items == null && other.Items != null)
                return false;
            if (Items != null && other.Items == null)
                return false;

            for (var i = 0; i < Items.Count; i++)
            {
                if (Items[i] != other.Items[i])
                    return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ObjectWithList);
        }

        public override int GetHashCode() => Items?.GetHashCode() ?? 0;
    }

    public class ObjectWithIList : IEquatable<ObjectWithIList>
    {
        public IList<int> Items { get; set; }

        public bool Equals(ObjectWithIList other)
        {
            if (other == null)
                return false;

            if (Items == null && other.Items == null)
                return true;
            if (Items == null && other.Items != null)
                return false;
            if (Items != null && other.Items == null)
                return false;

            for (var i = 0; i < Items.Count; i++)
            {
                if (Items[i] != other.Items[i])
                    return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ObjectWithIList);
        }

        public override int GetHashCode() => Items?.GetHashCode() ?? 0;
    }

    public class ObjectWithIList2 : IEquatable<ObjectWithIList2>
    {
        public IList<ObjectDbTest.Person> Items { get; set; }

        public bool Equals(ObjectWithIList2 other)
        {
            if (other == null)
                return false;

            if (Items == null && other.Items == null)
                return true;
            if (Items == null && other.Items != null)
                return false;
            if (Items != null && other.Items == null)
                return false;

            for (var i = 0; i < Items.Count; i++)
            {
                if (!Items[i].Equals(other.Items[i]))
                    return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ObjectWithIList2);
        }

        public override int GetHashCode() => Items?.GetHashCode() ?? 0;
    }

    [Fact]
    public void DeserializesClassWithList()
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithList { Items = new List<int> { 1 } };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);
    }

    [Fact]
    public void DeserializesAsObjectClassWithList()
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithList { Items = new List<int> { 1 } };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer(new TypeSerializersTest.ToDynamicMapper());
        Assert.False(deserializer.Deserialize(out dynamic obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(1, obj2.Items[0]);
    }

    [Fact]
    public void DeserializesClassWithIList()
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithIList { Items = new List<int> { 1 } };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);

        deserializer = new EventDeserializer();
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);
    }

    [Fact]
    public void DeserializesClassWithIList2()
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithIList2
            { Items = new List<ObjectDbTest.Person> { new ObjectDbTest.Person { Name = "A", Age = 1 } } };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        Assert.Equal(99, meta.Length);
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);

        deserializer = new EventDeserializer();
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);
    }

    [Fact]
    public void DeserializesClassWithIListArray()
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithIList2
            { Items = new ObjectDbTest.Person[] { new ObjectDbTest.Person { Name = "A", Age = 1 } } };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        Assert.Equal(99, meta.Length);
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        serializer = new EventSerializer();
        serializer.ProcessMetadataLog(meta);
        var data2 = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);

        deserializer = new EventDeserializer();
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);
    }

    [Fact]
    public void DeserializesClassWithIListArrayFirstEmpty()
    {
        var serializer = new EventSerializer();
        var objE = new ObjectWithIList2 { Items = null };
        var obj = new ObjectWithIList2
            { Items = new ObjectDbTest.Person[] { new ObjectDbTest.Manager { Name = "A", Age = 1 } } };
        var meta = serializer.Serialize(out var hasMetadata, objE).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, objE);
        var data2 = serializer.Serialize(out hasMetadata, obj);
    }

    public class ObjectWithDictionaryOfSimpleType : IEquatable<ObjectWithDictionaryOfSimpleType>
    {
        public IDictionary<int, string> Items { get; set; }

        public bool Equals(ObjectWithDictionaryOfSimpleType other)
        {
            if (other == null)
                return false;
            if (Items == null && other.Items == null)
                return true;


            if (Items == null && other.Items != null || Items != null && other.Items == null)
                return false;

            if (Items.Count != other.Items.Count)
                return false;

            foreach (var key in Items.Keys)
            {
                if (Items[key] != other.Items[key])
                    return false;
            }

            return true;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as ObjectWithDictionaryOfSimpleType);
        }

        public override int GetHashCode() => Items?.GetHashCode() ?? 0;
    }

    [Fact]
    public void DeserializesClassWithDictionaryOfSimpleTypes()
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithDictionaryOfSimpleType { Items = new Dictionary<int, string>() { { 1, "Ahoj" } } };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);
    }

    [Fact]
    public void DeserializesAsObjectClassWithDictionaryOfSimpleTypes()
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithDictionaryOfSimpleType { Items = new Dictionary<int, string>() { { 1, "Ahoj" } } };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer(new TypeSerializersTest.ToDynamicMapper());
        Assert.False(deserializer.Deserialize(out dynamic obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal("Ahoj", obj2.Items[1].ToString());
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
        var serializer = new EventSerializer();
        var obj = new EventWithIIndirect
        {
            Name = "A",
            Ind1 = new DBIndirect<User>(),
            Ind2 = new List<IIndirect<User>>()
        };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
    }

    [Fact]
    public void SupportStrangeVisibilities()
    {
        var serializer = new EventSerializer();
        var obj = new StrangeVisibilities { A = "a", C = "c", D = "d" };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        var ev = obj2 as StrangeVisibilities;
        Assert.Equal("a", ev.A);
        Assert.Null(ev.B);
        Assert.Equal("c", ev.C);
    }

    public class EventWithUser
    {
        public User User { get; set; }
    }

    [Fact]
    public void SimpleNestedObjects()
    {
        var serializer = new EventSerializer();
        var obj = new EventWithUser
        {
            User = new User()
        };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
    }

    public class DtoWithNotStored
    {
        public string Name { get; set; }
        [NotStored] public int Skip { get; set; }
    }

    [Fact]
    public void SerializingSkipsNotStoredProperties()
    {
        var serializer = new EventSerializer();
        var obj = new DtoWithNotStored { Name = "Boris", Skip = 1 };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(0, ((DtoWithNotStored)obj2).Skip);
    }

    public class DtoWithObject
    {
        public object Something { get; set; }
    }

    [Fact]
    public void SerializingBoxedDoubleDoesNotCrash()
    {
        var serializer = new EventSerializer();
        var obj = new DtoWithObject { Something = 1.2 };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(1.2, ((DtoWithObject)obj2).Something);
    }

    public class PureArray
    {
        public string[] A { get; set; }
        public int[] B { get; set; }
    }

    [Fact]
    public void SupportPureArray()
    {
        var serializer = new EventSerializer();
        var obj = new PureArray { A = new[] { "A", "B" }, B = new[] { 42, 7 } };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        var ev = obj2 as PureArray;
        Assert.Equal(ev.A, new[] { "A", "B" });
        Assert.Equal(ev.B, new[] { 42, 7 });
    }

    public struct Structure
    {
    }

    public class EventWithStruct
    {
        public Structure Structure { get; set; }
    }

    [Fact]
    public void CannotStoreStruct()
    {
        var testEvent = new EventWithStruct();

        var serializer = new EventSerializer();
        bool hasMetadata;
        var e = Assert.Throws<BTDBException>(() => serializer.Serialize(out hasMetadata, testEvent).ToAsyncSafe());
        Assert.Contains("Unsupported", e.Message);
    }

    public class EventWithPropertyWithoutSetter
    {
        public int NoSetter { get; }
    }

    [Fact]
    public void ThrowsWithPropertyWithoutSetter()
    {
#if DEBUG
        var testEvent = new EventWithPropertyWithoutSetter();

        var serializer = new EventSerializer();
        bool hasMetadata;
        var e = Assert.Throws<InvalidOperationException>(() =>
            serializer.Serialize(out hasMetadata, testEvent).ToAsyncSafe());
        Assert.Contains("NoSetter", e.Message);
#endif
    }

    public class EventWithPropertyWithoutGetter
    {
        int _x;

        public int NoGetter
        {
            set => _x = value;
        }
    }

    [Fact]
    public void ThrowsWithPropertyWithoutGetter()
    {
#if DEBUG
        var testEvent = new EventWithPropertyWithoutGetter();

        var serializer = new EventSerializer();
        bool hasMetadata;
        var e = Assert.Throws<InvalidOperationException>(() =>
            serializer.Serialize(out hasMetadata, testEvent).ToAsyncSafe());
        Assert.Contains("NoGetter", e.Message);
#endif
    }

    public class EventWithPublicField
    {
        public int PublicField;
    }

    [Fact]
    public void ThrowsWithPublicField()
    {
#if DEBUG
        var testEvent = new EventWithPublicField();

        var serializer = new EventSerializer();
        bool hasMetadata;
        var e = Assert.Throws<InvalidOperationException>(() =>
            serializer.Serialize(out hasMetadata, testEvent).ToAsyncSafe());
        Assert.Contains("PublicField", e.Message);
#endif
    }

    public class EventWithNotStoredPublicField
    {
        [NotStored] public int PublicField;
    }

    [Fact]
    public void SerializesWithNotStoredPublicField()
    {
        var testEvent = new EventWithNotStoredPublicField();

        var serializer = new EventSerializer();
        serializer.Serialize(out _, testEvent);
    }

    public enum WorkStatus
    {
        Unemployed = 0,
        Employed = 1
    }

    public class EventWithEnum
    {
        public WorkStatus Status { get; set; }
    }

    public class EventWithInt
    {
        public int Status { get; set; }

        protected bool Equals(EventWithInt other)
        {
            return Status == other.Status;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((EventWithInt)obj);
        }

        public override int GetHashCode()
        {
            return Status;
        }
    }

    [Fact]
    public void EnumToIntIsAutomaticConversionOnLoad()
    {
        var fullNameMapper = new FullNameTypeMapper();
        var overridedMapper = new OverloadableTypeMapper(typeof(EventWithInt),
            fullNameMapper.ToName(typeof(EventWithEnum)),
            fullNameMapper);

        var serializer = new EventSerializer(fullNameMapper);
        var original = new EventWithEnum { Status = WorkStatus.Employed };
        var metadata = serializer.Serialize(out var hasMetadata, original).ToAsyncSafe();
        Assert.True(hasMetadata);

        serializer.ProcessMetadataLog(metadata);

        var data = serializer.Serialize(out hasMetadata, original).ToAsyncSafe();
        Assert.False(hasMetadata);

        var deserializer = new EventDeserializer(overridedMapper);
        deserializer.ProcessMetadataLog(metadata);
        Assert.True(deserializer.Deserialize(out var readed, data));

        var readedEvem = readed as EventWithInt;
        Assert.Equal(readedEvem.Status, (int)original.Status);
    }

    public class EventWithString
    {
        public string Status { get; set; }
    }

    [Fact]
    public void StringToIntThrowErrorWithFullNameOfTypeInConversionOnLoad()
    {
        var fullNameMapper = new FullNameTypeMapper();
        var overridedMapper = new OverloadableTypeMapper(typeof(EventWithInt),
            fullNameMapper.ToName(typeof(EventWithString)),
            fullNameMapper);

        var serializer = new EventSerializer(fullNameMapper);
        var original = new EventWithString { Status = "Test string" };
        var metadata = serializer.Serialize(out var hasMetadata, original).ToAsyncSafe();
        Assert.True(hasMetadata);

        serializer.ProcessMetadataLog(metadata);

        var data = serializer.Serialize(out hasMetadata, original).ToAsyncSafe();
        Assert.False(hasMetadata);

        var deserializer = new EventDeserializer(overridedMapper);
        deserializer.ProcessMetadataLog(metadata);
        object readed;
        //Assert.True(deserializer.Deserialize(out readed, data));
        var e = Assert.Throws<BTDBException>(() => deserializer.Deserialize(out readed, data));
        Assert.Contains("Deserialization of type " + typeof(EventWithInt).FullName, e.Message);
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
    public void SerializeDeserializeWithNullable()
    {
        var serializer = new EventSerializer();
        var obj = new EventWithNullable
        {
            EventId = 1,
            NullableInt = 42,
            ListWithNullables = new List<int?> { 4, new int?() },
            DictionaryWithNullables = new Dictionary<int?, bool?> { { 1, true }, { 2, new bool?() } }
        };
        var meta = serializer.Serialize(out _, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        var ev = (EventWithNullable)obj2;
        Assert.Equal(42, ev.NullableInt.Value);
        Assert.False(ev.NullableEmpty.HasValue);
        Assert.Equal(2, ev.ListWithNullables.Count);
        Assert.Equal(4, ev.ListWithNullables[0].Value);
        Assert.False(ev.ListWithNullables[1].HasValue);
        Assert.Equal(2, ev.DictionaryWithNullables.Count);
        Assert.True(ev.DictionaryWithNullables[1]);
        Assert.False(ev.DictionaryWithNullables[2].HasValue);
    }

    [Fact]
    public void
        GivenObjectWithMultipleReferencesToSingleInstance_WhenDeserializationFailsDeepDownBecauseOfMissingMetadata_ThenNextReferenceShouldBeResolvedCorrectlyAfterApplicationOfTheMetadata()
    {
        // Familiarize the serializer with top-level type
        var serializer = new EventSerializer();
        var input1 = new ObjectWithMultipleReferences();
        var meta1 = serializer.Serialize(out var metadataProduced, input1).ToAsyncSafe();
        Assert.True(metadataProduced);
        serializer.ProcessMetadataLog(meta1);
        var data1 = serializer.Serialize(out metadataProduced, input1).ToAsyncSafe();
        Assert.False(metadataProduced);

        // Serialize the top-level type containing properties with a not-yet-encountered object type
        var reusableObj = new EventWithInt { Status = 42 };
        var input2 = new ObjectWithMultipleReferences
        {
            Reference1 = reusableObj,
            Reference2 = reusableObj
        };
        var meta2 = serializer.Serialize(out metadataProduced, input2).ToAsyncSafe();
        Assert.True(metadataProduced);
        serializer.ProcessMetadataLog(meta2);
        var data2 = serializer.Serialize(out metadataProduced, input2).ToAsyncSafe();
        Assert.False(metadataProduced);

        // Familiarize the deserializer with the top-level type
        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out _, data1));
        deserializer.ProcessMetadataLog(meta1);
        Assert.True(deserializer.Deserialize(out var obj1, data1));
        Assert.Equal(input1, obj1);

        // Deserialize the top-level type with properties containing instances of a not-yet-encountered object type
        Assert.False(deserializer.Deserialize(out _, data2));
        deserializer.ProcessMetadataLog(meta2);
        Assert.True(deserializer.Deserialize(out var obj2, data2));
        Assert.Equal(input2, obj2);
    }

    public class ObjectWithMultipleReferences
    {
        public object Reference1 { get; set; }
        public object Reference2 { get; set; }

        protected bool Equals(ObjectWithMultipleReferences other)
        {
            return Equals(Reference1, other.Reference1) && Equals(Reference2, other.Reference2);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ObjectWithMultipleReferences)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Reference1 != null ? Reference1.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Reference2 != null ? Reference2.GetHashCode() : 0);
                return hashCode;
            }
        }
    }

    public class ComplexObject
    {
        public ComplexObject Obj { get; set; }
    }

    public class ComplexObjectEx : ComplexObject
    {
        public int Int { get; set; }
    }

    public class EventWithDeepDictWithComplexObject
    {
        public ulong EventId { get; set; }
        public Dictionary<ulong, Dictionary<string, ComplexObject>> Prop { get; set; }
        public List<List<ComplexObject>> PropList { get; set; }
    }

    [Fact]
    public void SerializeDeserializeDeepDictWithComplexObject()
    {
        var serializer = new EventSerializer();
        var obj = new EventWithDeepDictWithComplexObject
        {
            EventId = 1,
            Prop = new Dictionary<ulong, Dictionary<string, ComplexObject>>
            {
                {
                    1,
                    new Dictionary<string, ComplexObject> { { "a", new ComplexObjectEx { Obj = new ComplexObject() } } }
                }
            },
            PropList = new List<List<ComplexObject>> { new List<ComplexObject> { new ComplexObjectEx() } }
        };
        var meta = serializer.Serialize(out _, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        var ev = (EventWithDeepDictWithComplexObject)obj2;
        Assert.Equal(1ul, ev.Prop.First().Key);
    }

    public class EventWithEncryptedString
    {
        public EncryptedString Secret { get; set; }
    }

    [Fact]
    public void SerializeDeserializeEventWithEncryptedString()
    {
        var cipher = new AesGcmSymmetricCipher(new byte[]
        {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
            28, 29, 30, 31
        });
        var serializer = new EventSerializer(null, null, cipher);
        var obj = new EventWithEncryptedString
        {
            Secret = "pass"
        };
        var meta = serializer.Serialize(out _, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, obj);

        var deserializer = new EventDeserializer(null, null, cipher);
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        var ev = (EventWithEncryptedString)obj2;
        Assert.Equal("pass", ev.Secret);
    }

    public class ObjectWithIDictionary
    {
        public IDictionary<ulong, string> Items { get; set; }
    }

    [Fact]
    public void CanSerializeOdbDictionary()
    {
        using (var kvDb = new KeyValueDB(new InMemoryFileCollection()))
        using (var objDb = new ObjectDB())
        {
            objDb.Open(kvDb, false);

            using (var tr = objDb.StartWritingTransaction().Result)
            {
                var singleton = tr.Singleton<ObjectWithIDictionary>();
                singleton.Items[1] = "ahoj";

                SerializeOdbDict(singleton.Items);

                tr.Commit();
            }
        }
    }

    void SerializeOdbDict(IDictionary<ulong, string> dictItems)
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithIDictionary
        {
            Items = dictItems
        };
        var meta = serializer.Serialize(out _, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));

        Assert.Equal(obj.Items, ((ObjectWithIDictionary)obj2).Items);
    }

    public class SomeSets
    {
        public ISet<string> A { get; set; }
        public HashSet<int> B { get; set; }
    }

    [Fact]
    public void SupportSets()
    {
        var serializer = new EventSerializer();
        var obj = new SomeSets { A = new HashSet<string> { "A", "B" }, B = new HashSet<int> { 42, 7 } };
        var meta = serializer.Serialize(out _, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));

        var ev = obj2 as SomeSets;
        Assert.Equal(new[] { "A", "B" }, ev!.A.OrderBy(a => a));
        Assert.Equal(new[] { 7, 42 }, ev.B.OrderBy(b => b));
    }

    [Fact]
    public void StoreListThenArray()
    {
        var serializer = new EventSerializer();

        void Store(object obj)
        {
            var meta = serializer.Serialize(out var hasMetaData, obj).ToAsyncSafe();
            if (hasMetaData)
            {
                serializer.ProcessMetadataLog(meta);
                serializer.Serialize(out _, obj);
            }
        }

        Store(new List<bool>());
        Store(new[] { true });
        Store(new HashSet<bool>());
    }

    [Fact]
    public void StoreVariousComplexCombinations()
    {
        var serializer = new EventSerializer();

        void Store(object obj)
        {
            var meta = serializer.Serialize(out var hasMetaData, obj).ToAsyncSafe();
            if (hasMetaData)
            {
                serializer.ProcessMetadataLog(meta);
                serializer.Serialize(out _, obj);
            }
        }

        Store(new Dictionary<int, List<bool>> { { 1, new List<bool> { true } } });
        Store(new Dictionary<int, IList<bool>> { { 1, new List<bool> { true } } });
        Store(new Dictionary<int, IList<bool>> { { 1, new[] { true } } });
    }

    [Fact]
    public void DictionaryWithSomeNullListAsValue()
    {
        var serializer = new EventSerializer();
        var obj = new Dictionary<ulong, IList<ulong>>
        {
            { 1, null },
            { 2, new List<ulong> { 21, 22 } },
            { 3, null },
            { 4, new List<ulong> { 41, 42 } }
        };
        var meta = serializer.Serialize(out _, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));

        Assert.Equivalent(obj, obj2);
    }

    [Fact]
    public void DictionaryWithSomeNullArrayAsValue()
    {
        var serializer = new EventSerializer();
        var obj = new Dictionary<ulong, ulong[]>
        {
            { 1, null },
            { 2, new ulong[] { 21, 22 } },
            { 3, null },
            { 4, new ulong[] { 41, 42 } }
        };
        var meta = serializer.Serialize(out _, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));

        Assert.Equivalent(obj, obj2);
    }

    [Fact(Skip = "Generic serialization of structs is hard to implement, that's why it is not working for now.")]
    public void SerializationOfStruct_Succeeds()
    {
        var testStruct = new TestStruct { TestData = new TestStruct.TestStructData { Data = "TestData" } };

        var result = SerializationInternal<TestStruct>(testStruct);

        Assert.NotNull(result);
        Assert.Equal(testStruct.TestData.Data, result.TestData.Data);
    }

    [Fact]
    public void SerializationOfBaseClassWithPrivateSet_Succeeds()
    {
        var testBaseClass = new TestClassWithBaseClass();
        var result = SerializationInternal<TestClassWithBaseClass>(testBaseClass);

        Assert.NotNull(result);
        Assert.Equal(testBaseClass.TestData, result.TestData);
    }

    public class GenClass<T>
    {
        public T Member { get; set; }
    }

    [Fact]
    public void GenericClassSerializationSucceeds()
    {
        var test = new GenClass<int>();
        test.Member = 42;
        var result = SerializationInternal<GenClass<int>>(test);

        Assert.NotNull(result);
        Assert.Equal(test.Member, result.Member);
    }

    [Fact]
    public void DeserializesClassWithGenericType()
    {
        var serializer = new EventSerializer();
        var obj = new ObjectWithGenericType
        {
            TypeA = new GenericType<SomeTypeA>
            {
                Type = new SomeTypeA
                {
                    A = "A",
                    Name = "Name A"
                }
            },
            TypeB = new GenericType<SomeTypeB>
            {
                Type = new SomeTypeB
                {
                    B = "B",
                    Name = "Name B"
                }
            }
        };
        var meta = serializer.Serialize(out var hasMetadata, obj).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out hasMetadata, obj);

        var deserializer = new EventDeserializer();
        Assert.False(deserializer.Deserialize(out var obj2, data));
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);

        deserializer = new EventDeserializer();
        deserializer.ProcessMetadataLog(meta);
        Assert.True(deserializer.Deserialize(out obj2, data));
        Assert.Equal(obj, obj2);
    }

    public class ObjectWithGenericType : IEquatable<ObjectWithGenericType>
    {
        public GenericType<SomeTypeA> TypeA { get; set; }
        public GenericType<SomeTypeB> TypeB { get; set; }

        public bool Equals(ObjectWithGenericType? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return TypeA.Equals(other.TypeA) && TypeB.Equals(other.TypeB);
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ObjectWithGenericType)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(TypeA, TypeB);
        }
    }

    public class GenericType<T> : IEquatable<GenericType<T>> where T : ISomeType, new()
    {
        public T Type { get; set; }

        public bool Equals(GenericType<T>? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return EqualityComparer<T>.Default.Equals(Type, other.Type);
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((GenericType<T>)obj);
        }

        public override int GetHashCode()
        {
            return EqualityComparer<T>.Default.GetHashCode(Type);
        }
    }

    public interface ISomeType
    {
        public string Name { get; set; }
    }

    public class SomeTypeA : ISomeType, IEquatable<SomeTypeA>
    {
        public string Name { get; set; }
        public string A { get; set; }

        public bool Equals(SomeTypeA? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Name == other.Name && A == other.A;
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SomeTypeA)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, A);
        }
    }

    public class SomeTypeB : ISomeType, IEquatable<SomeTypeB>
    {
        public string Name { get; set; }
        public string B { get; set; }

        public bool Equals(SomeTypeB? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Name == other.Name && B == other.B;
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SomeTypeB)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, B);
        }
    }

    T SerializationInternal<T>(object input)
    {
        var serializer = new EventSerializer();
        var meta = serializer.Serialize(out _, input).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, input);

        var deserializer = new EventDeserializer();
        deserializer.ProcessMetadataLog(meta);
        deserializer.Deserialize(out var deserializedObj, data);

        return (T)deserializedObj;
    }

    class TestStruct
    {
        public TestStructData TestData { get; set; }

        internal struct TestStructData
        {
            public string Data { get; set; }
        }
    }

    class TestClassWithBaseClass : TestBaseClass
    {
        [NotStored] public string TestData => Data;
    }

    class TestBaseClass
    {
        public string Data { get; private set; }

        public TestBaseClass()
        {
            Data = "TestData";
        }
    }

    public class Obj
    {
        public int Num { get; set; }
    }

    public class ObjChild : Obj
    {
        public int Child { get; set; }
    }

    public class EObj
    {
        public Obj O { get; set; }
    }

    public class EObjV2
    {
        [PrimaryKey(1)] public ulong Id { get; set; }

        public ObjChild O { get; set; }
    }

    public class MyObjToObjChildTypeConvertorGenerator : DefaultTypeConvertorGenerator
    {
        public static ObjChild Convert2ObjChild(Obj value) => new() { Num = value.Num, Child = 42 };
    }

    [Fact]
    public void CanUpgradeToDifferentType()
    {
        var value = new EObj() { O = new() { Num = 1 } };
        var serializer = new EventSerializer(new OverloadableTypeMapper(typeof(EObj), "EObj"));
        var meta = serializer.Serialize(out _, value).ToAsyncSafe();
        serializer.ProcessMetadataLog(meta);
        var data = serializer.Serialize(out _, value);

        var deserializer = new EventDeserializer(new OverloadableTypeMapper(typeof(EObjV2), "EObj"),
            new MyObjToObjChildTypeConvertorGenerator());
        deserializer.ProcessMetadataLog(meta);
        deserializer.Deserialize(out var deserializedObj, data);

        var valueV2 = deserializedObj as EObjV2;
        Assert.Equal(1, valueV2.O.Num);
        Assert.Equal(42, valueV2.O.Child);
    }

    public interface IContent
    {
        public ulong ContentId { get; set; }
    }

    public class Content : IContent, IEquatable<Content>
    {
        public ulong ContentId { get; set; }
        public string Name { get; set; }

        public bool Equals(Content? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ContentId == other.ContentId && Name == other.Name;
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Content)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(ContentId, Name);
        }
    }

    public class ClassWithIEnumerable : IEquatable<ClassWithIEnumerable>
    {
        public IEnumerable<IContent> Items { get; set; }

        public bool Equals(ClassWithIEnumerable? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Items.SequenceEqual(other.Items);
        }

        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ClassWithIEnumerable)obj);
        }

        public override int GetHashCode()
        {
            return 1;
        }
    }

    [Fact]
    public void TryingToSerializeIEnumerableCreatedByLinqSelectThrows()
    {
        var serializer = new EventSerializer();
        var obj = new ClassWithIEnumerable
        {
            Items = new[] { new Content { ContentId = 1, Name = "A" }, new Content { ContentId = 2, Name = "B" } }
                .Select(c => c)
        };
        Assert.Throws<InvalidOperationException>(() => serializer.Serialize(out var hasMetadata, obj));
    }
}
