using System;
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
            Assert.InRange(GC.GetTotalMemory(false), 0, baselineMemory+100);
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
    }
}
