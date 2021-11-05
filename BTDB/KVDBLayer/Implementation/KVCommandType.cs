using System;

namespace BTDB.KVDBLayer;

[Flags]
public enum KVCommandType : byte
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
    DeltaUlongs,
    CommandMask = 31,
    FirstParamCompressed = 32,
    SecondParamCompressed = 64
}
