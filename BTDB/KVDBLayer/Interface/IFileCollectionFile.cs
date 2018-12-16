using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    public interface IFileCollectionFile
    {
        uint Index { get; }
        AbstractBufferedReader GetExclusiveReader();

        void RandomRead(byte[] data, int offset, int size, ulong position, bool doNotCache);
        // can use RandomRead and when will stop writing SwitchToReadOnlyMode will be called
        AbstractBufferedWriter GetAppenderWriter();

        // will just write and not use RandomRead, after writing will finish SwitchToDisposedMode will be called
        // this should not need to cache written data in memory, saving memory
        AbstractBufferedWriter GetExclusiveAppenderWriter();

        // called in non-durable transaction commit, asynchronous Writer.FlushBuffers
        void Flush();
        void HardFlush();
        void SetSize(long size);
        void Truncate();

        // will only use RandomRead for this file till end of process
        void SwitchToReadOnlyMode();

        // combination of three methods could be done asynchronously
        void HardFlushTruncateSwitchToReadOnlyMode();
        // will not read or write this file till end of process
        void SwitchToDisposedMode();

        // combination of three methods could be done asynchronously
        void HardFlushTruncateSwitchToDisposedMode();

        ulong GetSize();

        void Remove();
    }
}
