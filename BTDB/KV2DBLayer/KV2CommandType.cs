using System;

namespace BTDB.KV2DBLayer
{
    [Flags]
    internal enum KV2CommandType : byte
    {
        CreateOrUpdate,
        EraseOne,
        EraseRange,
        TransactionStart,
        Commit,
        Rollback,
        EndOfFile,
        CommandMask = 31,
        FirstParamCompressed = 32,
        SecondParamCompressed = 64
    }
}