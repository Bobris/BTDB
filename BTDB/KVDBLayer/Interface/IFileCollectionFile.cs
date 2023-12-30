using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public interface IFileCollectionFile
{
    uint Index { get; }
    IMemReader GetExclusiveReader();

    // RandomRead will be used, it is good to have this file in cache
    void AdvisePrefetch();

    void RandomRead(Span<byte> data, ulong position, bool doNotCache);

    // can use RandomRead and when will stop writing SwitchToReadOnlyMode will be called
    IMemWriter GetAppenderWriter();

    // will just write and not use RandomRead, after writing will finish SwitchToDisposedMode will be called
    // this should not need to cache written data in memory, saving memory
    IMemWriter GetExclusiveAppenderWriter();

    // Synchronously wait for OS file buffers to flush => forcing durability
    void HardFlush();

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
