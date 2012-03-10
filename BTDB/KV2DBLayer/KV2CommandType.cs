using System;

namespace BTDB.KV2DBLayer
{
    [Flags]
    internal enum KV2CommandType : byte
    {
        CreateOrUpdate,
        EraseOne,
        EraseRange,
        Commit,
        Rollback,
        CommandMask = 31,
        FirstParamCompressed = 32,
        SecondParamCompressed = 64
    }
}