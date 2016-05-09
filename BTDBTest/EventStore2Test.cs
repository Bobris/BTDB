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
    }
}
