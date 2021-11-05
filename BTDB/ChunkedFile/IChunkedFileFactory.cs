using System;
using BTDB.Buffer;

namespace BTDB.ChunkedFile;

public interface IChunkedFileFactory
{
    IChunkedFile Create(IObservable<ByteBuffer> source);
}
