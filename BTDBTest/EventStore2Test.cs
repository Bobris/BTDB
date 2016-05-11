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

            var writer = new ByteBufferWriter();

            Assert.True(serializer.Serialize(writer, new User()));
            Assert.InRange(writer.Data.Length, 1, 100);
        }

        [Fact]
        public void ParsingMetadataStopsGeneratingThem()
        {
            var serializer = new EventSerializer();

            var writer = new ByteBufferWriter();

            serializer.Serialize(writer, new User());
            serializer.ProcessMetadataLog(writer.Data);
            writer = new ByteBufferWriter();
            Assert.False(serializer.Serialize(writer, new User()));
            Assert.InRange(writer.Data.Length, 1, 10);
        }

        [Fact]
        public void SerializationRunsAndDoesNotLeak1Byte()
        {
            var serializer = new EventSerializer();

            var writer = new ByteBufferWriter();

            serializer.Serialize(writer, new User());
            serializer.ProcessMetadataLog(writer.Data);
            writer = new ByteBufferWriter();
            long baselineMemory = 0;
            for (int i = 0; i < 10; i++)
            {
                Assert.False(serializer.Serialize(writer, new User()));
                writer.Reset();
                GC.Collect(2);
                GC.WaitForPendingFinalizers();
                GC.Collect(2);
                if (i == 2)
                    baselineMemory = GC.GetTotalMemory(false);
            }
            Assert.InRange(GC.GetTotalMemory(false), 0, baselineMemory);
        }
    }
}
