using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public interface IFileCollectionFile
{
    uint Index { get; }
    ISpanReader GetExclusiveReader();

    // RandomRead will be used, it is good to have this file in cache
    void AdvisePrefetch();

    void RandomRead(Span<byte> data, ulong position, bool doNotCache);

    // can use RandomRead and when will stop writing SwitchToReadOnlyMode will be called
    ISpanWriter GetAppenderWriter();

    // will just write and not use RandomRead, after writing will finish SwitchToDisposedMode will be called
    // this should not need to cache written data in memory, saving memory
    ISpanWriter GetExclusiveAppenderWriter();

    // called in non-durable transaction commit, kind of asynchronous Writer.FlushBuffers
    void Flush();

    // Flush() and synchronously wait for OS file buffers to flush
    void HardFlush();
    void SetSize(long size);
    void Truncate();

    // combination of three methods could be done asynchronously
    // will only use RandomRead for this file till end of process
    void HardFlushTruncateSwitchToReadOnlyMode();

    // combination of three methods could be done asynchronously
    // will not read or write this file till dispose of KeyValueDB
    void HardFlushTruncateSwitchToDisposedMode();

    ulong GetSize();

    void Remove();
}
