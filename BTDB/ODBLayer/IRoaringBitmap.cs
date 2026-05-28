using System;
using System.Collections.Generic;

namespace BTDB.ODBLayer;

public interface IRoaringBitmap : IEnumerable<ulong>
{
    bool Get(ulong value);
    void Set(ulong value, bool enabled);
    void Clear();
    ulong Count { get; }
    bool IsComplete();
    void Flush();
    void ApplyCommands(ReadOnlyMemory<byte> commands);
}
