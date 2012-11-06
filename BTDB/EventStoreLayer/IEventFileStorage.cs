using System.Threading.Tasks;

namespace BTDB.EventStoreLayer
{
    public interface IEventFileStorage
    {
        uint MaxBlockSize { get; }
        Task<uint> Read(Buffer.ByteBuffer buf, ulong position);
        void SetWritePosition(ulong position);
        Task Write(Buffer.ByteBuffer buf);
    }
}