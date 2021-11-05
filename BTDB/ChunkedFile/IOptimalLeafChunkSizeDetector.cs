using BTDB.Buffer;

namespace BTDB.ChunkedFile;

public interface IOptimalLeafChunkSizeDetector
{
    int StartingBytesNeeded { get; }
    int Detect(ByteBuffer startingBytes);
}
