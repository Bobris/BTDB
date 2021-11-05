using System;

namespace BTDB.IL;

public interface IILLocal
{
    int Index { get; }
    bool Pinned { get; }
    Type LocalType { get; }
}
