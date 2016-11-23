using System;
using System.Collections.Generic;
using System.Configuration;
using BTDB.EventStore2Layer;
using BTDB.StreamLayer;
using Xunit;
using static BTDBTest.EventStoreTest;

namespace BTDBTest
{
    public class EventStore2Test
    {
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

        [Fact]
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
            Assert.InRange(GC.GetTotalMemory(false), 0, baselineMemory+400);
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
        }

        [Fact]
        public void DeserializesClassWithEnum()
        {
            var serializer = new EventSerializer();
            bool hasMetadata;
            var obj = new ObjectWithEnum { State = StateEnum.Alive};
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
        }

        [Fact]
        public void DeserializesClassWithList()
        {
            var serializer = new EventSerializer();
            bool hasMetadata;
            var obj = new ObjectWithList { Items = new List<int> {1} };
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
        }

        [Fact]
        public void DeserializesClassWithDictionaryOfSimpleTypes()
        {
            var serializer = new EventSerializer();
            bool hasMetadata;
            var obj = new ObjectWithDictionaryOfSimpleType { Items = new Dictionary<int, string>(){ {1, "Ahoj"} } };
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
    }
}
