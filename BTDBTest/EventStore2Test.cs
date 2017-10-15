using System;
using System.Collections.Generic;
using BTDB.EventStore2Layer;
using Xunit;
using static BTDBTest.EventStoreTest;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace BTDBTest
{
    public class EventStore2Test
    {
        const string GivenEventsMetadataFilePath = "..\\..\\TestData\\meta.txt";
        const string GivenEventsDataFilePath = "..\\..\\TestData\\events.txt";
        const char DataFileSeparator = ' ';

        [Fact]
        public void SerializingNewObjectsWritesNewMetadata()
        {
            var serializer = new EventSerializer();

            bool hasMetadata;
            var data = serializer.Serialize(out hasMetadata, new User());
            Assert.True(hasMetadata);
            Assert.InRange(data.Length, 1, 100);
        }

        [Fact]
        public void ParsingMetadataStopsGeneratingThem()
        {
            var serializer = new EventSerializer();

            bool hasMetadata;

            var meta = serializer.Serialize(out hasMetadata, new User());
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, new User());
            Assert.False(hasMetadata);
            Assert.InRange(data.Length, 1, 10);
        }

        [Fact(Skip = "Waiting for allocated bytes method to be precise")]
        public void SerializationRunsAndDoesNotLeak1Byte()
        {
            var serializer = new EventSerializer();

            bool hasMetadata;

            var meta = serializer.Serialize(out hasMetadata, new User());
            serializer.ProcessMetadataLog(meta);
            long baselineMemory = 0;
            for (int i = 0; i < 100; i++)
            {
                serializer.Serialize(out hasMetadata, new User());
                Assert.False(hasMetadata);
                if (i == 2)
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
            Assert.InRange(GC.GetTotalMemory(false), 0, baselineMemory + 400);
        }

        [Fact]
        public void DeserializeSimpleClass()
        {
            var serializer = new EventSerializer();
            bool hasMetadata;
            var obj = new User { Name = "Boris", Age = 40 };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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
            bool hasMetadata;
            var obj = new ObjectWithEnum { State = StateEnum.Alive };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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

                for (int i = 0; i < Items.Count; i++)
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

                for (int i = 0; i < Items.Count; i++)
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

                for (int i = 0; i < Items.Count; i++)
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
            bool hasMetadata;
            var obj = new ObjectWithList { Items = new List<int> { 1 } };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
            Assert.Equal(obj, obj2);
        }

        [Fact]
        public void DeserializesAsObjectClassWithList()
        {
            var serializer = new EventSerializer();
            bool hasMetadata;
            var obj = new ObjectWithList { Items = new List<int> { 1 } };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer(new TypeSerializersTest.ToDynamicMapper());
            dynamic obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
            Assert.Equal(1, obj2.Items[0]);
        }

        [Fact]
        public void DeserializesClassWithIList()
        {
            var serializer = new EventSerializer();
            bool hasMetadata;
            var obj = new ObjectWithIList { Items = new List<int> { 1 } };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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
            bool hasMetadata;
            var obj = new ObjectWithIList2 { Items = new List<ObjectDbTest.Person> { new ObjectDbTest.Person { Name = "A", Age = 1 } } };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            Assert.Equal(99, meta.Length);
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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
            bool hasMetadata;
            var obj = new ObjectWithIList2 { Items = new ObjectDbTest.Person[] { new ObjectDbTest.Person { Name = "A", Age = 1 } } };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            Assert.Equal(99, meta.Length);
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            serializer = new EventSerializer();
            serializer.ProcessMetadataLog(meta);
            var data2 = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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
            bool hasMetadata;
            var objE = new ObjectWithIList2 { Items = null };
            var obj = new ObjectWithIList2 { Items = new ObjectDbTest.Person[] { new ObjectDbTest.Manager { Name = "A", Age = 1 } } };
            var meta = serializer.Serialize(out hasMetadata, objE).ToAsyncSafe();
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
            bool hasMetadata;
            var obj = new ObjectWithDictionaryOfSimpleType { Items = new Dictionary<int, string>() { { 1, "Ahoj" } } };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
            Assert.Equal(obj, obj2);
        }

        [Fact]
        public void DeserializesAsObjectClassWithDictionaryOfSimpleTypes()
        {
            var serializer = new EventSerializer();
            bool hasMetadata;
            var obj = new ObjectWithDictionaryOfSimpleType { Items = new Dictionary<int, string>() { { 1, "Ahoj" } } };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer(new TypeSerializersTest.ToDynamicMapper());
            dynamic obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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
            bool hasMetadata;
            var obj = new EventWithIIndirect
            {
                Name = "A",
                Ind1 = new DBIndirect<User>(),
                Ind2 = new List<IIndirect<User>>()
            };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        [Fact]
        public void SupportStrangeVisibilities()
        {
            var serializer = new EventSerializer();
            bool hasMetadata;
            var obj = new StrangeVisibilities { A = "a", C = "c", D = "d" };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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
            bool hasMetadata;
            var obj = new EventWithUser
            {
                User = new User()
            };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
            deserializer.ProcessMetadataLog(meta);
            Assert.True(deserializer.Deserialize(out obj2, data));
        }

        public class DtoWithNotStored
        {
            public string Name { get; set; }
            [NotStored]
            public int Skip { get; set; }
        }

        [Fact]
        public void SerializingSkipsNotStoredProperties()
        {
            var serializer = new EventSerializer();
            bool hasMetadata;
            var obj = new DtoWithNotStored { Name = "Boris", Skip = 1 };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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
            bool hasMetadata;
            var obj = new DtoWithObject { Something = 1.2 };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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
            bool hasMetadata;
            var obj = new PureArray { A = new[] { "A", "B" }, B = new[] { 42, 7 } };
            var meta = serializer.Serialize(out hasMetadata, obj).ToAsyncSafe();
            serializer.ProcessMetadataLog(meta);
            var data = serializer.Serialize(out hasMetadata, obj);

            var deserializer = new EventDeserializer();
            object obj2;
            Assert.False(deserializer.Deserialize(out obj2, data));
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
            var e = Assert.Throws<InvalidOperationException>(() => serializer.Serialize(out hasMetadata, testEvent).ToAsyncSafe());
            Assert.Contains("NoSetter", e.Message);
#endif
        }

        public class EventWithPropertyWithoutGetter
        {
            private int _x;
            public int NoGetter { set => _x = value; }
        }

        [Fact]
        public void ThrowsWithPropertyWithoutGetter()
        {
#if DEBUG
            var testEvent = new EventWithPropertyWithoutGetter();

            var serializer = new EventSerializer();
            bool hasMetadata;
            var e = Assert.Throws<InvalidOperationException>(() => serializer.Serialize(out hasMetadata, testEvent).ToAsyncSafe());
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
            var e = Assert.Throws<InvalidOperationException>(() => serializer.Serialize(out hasMetadata, testEvent).ToAsyncSafe());
            Assert.Contains("PublicField", e.Message);
#endif
        }

        public class EventWithNotStoredPublicField
        {
            [NotStored]
            public int PublicField;
        }

        [Fact]
        public void SerializesWithNotStoredPublicField()
        {
            var testEvent = new EventWithNotStoredPublicField();

            var serializer = new EventSerializer();
            bool hasMetadata;
            serializer.Serialize(out hasMetadata, testEvent);
        }

    }
}
