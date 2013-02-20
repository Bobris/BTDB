using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public interface IFileCollectionFile
    {
        uint Index { get; }
        AbstractBufferedReader GetExclusiveReader();

        void RandomRead(byte[] data, int offset, int size, ulong position);
        AbstractBufferedWriter GetAppenderWriter();
        void HardFlush();

        ulong GetSize();

        void Remove();
    }
}