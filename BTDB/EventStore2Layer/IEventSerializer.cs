using BTDB.Buffer;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer
{
    public interface IEventSerializer
    {
        /// <summary>
        /// Call when new data from metadata log were received
        /// </summary>
        /// <param name="buffer">content of single log item</param>
        void ProcessMetadataLog(ByteBuffer buffer);
        /// <summary>
        /// returns true if written data must go to metadata log, 
        /// after they will be processed call this again, so it will return false
        /// and writer data are real data
        /// </summary>
        /// <param name="writer">where to write data</param>
        /// <param name="object">what to write</param>
        /// <returns>true if there are new metadata to store</returns>
        bool Serialize(AbstractBufferedWriter writer, object @object);
    }
}
