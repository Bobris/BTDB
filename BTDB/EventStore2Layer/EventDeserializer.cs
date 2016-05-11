using BTDB.Buffer;

namespace BTDB.EventStore2Layer
{
    class EventDeserializer : IEventDeserializer
    {
        public void ProcessMetadataLog(ByteBuffer buffer)
        {
            throw new System.NotImplementedException();
        }

        public object Deserialize(ByteBuffer buffer)
        {
            throw new System.NotImplementedException();
        }
    }
}