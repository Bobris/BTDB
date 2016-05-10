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
        }

    }
}
