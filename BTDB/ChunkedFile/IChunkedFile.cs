using System.Threading.Tasks;
using BTDB.Buffer;

namespace BTDB.ChunkedFile;

public interface IChunkedFile
{
    ByteBuffer RootChunk { get; }
    int HashSize { get; }
    int LeafChunkSize { get; }
    int HashChunkSize { get; }
    ulong FileSize { get; }
    Task<ByteBuffer> ReadChunk(ByteBuffer hash);
}
