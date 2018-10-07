using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public interface IFileCollectionFile
    {
        uint Index { get; }
        AbstractBufferedReader GetExclusiveReader();

        void RandomRead(Span<byte> data, ulong position, bool doNotCache);
        AbstractBufferedWriter GetAppenderWriter();
        void HardFlush();
        void SetSize(long size);
        void Truncate();

        ulong GetSize();

        void Remove();
    }
}
