using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer;

public interface ILazyUlongList : IEnumerable<ulong>
{
    void Add(ulong value);
    void Clear();
    IEnumerable<ulong> EnumerateFromIndex(ulong index);
    ulong Count { get; }
    bool IsComplete();
    void Flush();
    void ApplyCommands(ReadOnlyMemory<byte> commands);
}
