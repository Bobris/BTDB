using System;

namespace BTDB.KVDBLayer
{
    [Flags]
    enum KVCommandType : byte
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
        CommitWithDeltaUlong,
        CommandMask = 31,
        FirstParamCompressed = 32,
        SecondParamCompressed = 64
    }
}