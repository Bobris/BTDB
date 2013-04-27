using System.Threading.Tasks;

namespace BTDB.EventStoreLayer
{
    public interface IEventFileStorage
    {
        uint MaxBlockSize { get; }
        uint Read(Buffer.ByteBuffer buf, ulong position);
        void SetWritePosition(ulong position);
        void Write(Buffer.ByteBuffer buf);
    }
}