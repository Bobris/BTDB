using System;

namespace BTDB.KVDBLayer
{
    [Flags]
    internal enum KVCommandType : byte
    {
        CreateOrUpdateDeprecated,
        EraseOne,
        EraseRange,
        TransactionStart,
        Commit,
        Rollback,
        EndOfFile,
        CreateOrUpdate,
        TemporaryEndOfFile,
        CommandMask = 31,
        FirstParamCompressed = 32,
        SecondParamCompressed = 64
    }
}