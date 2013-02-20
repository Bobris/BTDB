using System;

namespace BTDB.KVDBLayer
{
    [Flags]
    internal enum KVCommandType : byte
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