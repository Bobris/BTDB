using BTDB.Buffer;

namespace BTDB.EventStore2Layer;

public interface IEventDeserializer
{
    /// <summary>
    /// Call when new data from metadata log were received
    /// </summary>
    /// <param name="buffer">content of single log item</param>
    void ProcessMetadataLog(ByteBuffer buffer);

    /// <summary>
    /// Deserialize data to object
    /// </summary>
    /// <param name="object">contains deserializer object</param>
    /// <param name="buffer">buffer from data log</param>
    /// <returns>false if it needs to wait for metadata</returns>
    bool Deserialize(out object @object, ByteBuffer buffer);
}
