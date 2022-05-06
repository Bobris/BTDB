using BTDB.Buffer;

namespace BTDB.EventStore2Layer;

public interface IEventSerializer
{
    /// <summary>
    /// Call when new data from metadata log were received
    /// </summary>
    /// <param name="buffer">content of single log item</param>
    void ProcessMetadataLog(ByteBuffer buffer);
    /// <summary>
    /// returns actual data or metadata to write to log, 
    /// if hasMetaData is true they must go to metadata log
    /// </summary>
    /// <param name="hasMetaData">true if returned data must go to metadata log</param>
    /// <param name="object">what to write</param>
    /// <returns>data or metadata to store</returns>
    ByteBuffer Serialize(out bool hasMetaData, object @object);
}
