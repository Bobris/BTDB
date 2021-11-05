using System;
using System.Threading.Tasks;
using BTDB.Buffer;

namespace BTDB.KVDBLayer;

interface IChunkStorageTransaction : IDisposable
{
    void Put(ByteBuffer key, ByteBuffer content, bool isLeaf);
    Task<ByteBuffer> Get(ByteBuffer key);
}
