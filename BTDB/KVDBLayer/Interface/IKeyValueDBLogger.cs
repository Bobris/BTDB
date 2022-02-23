﻿using System;

namespace BTDB.KVDBLayer;

public interface IKeyValueDBLogger
{
    void ReportTransactionLeak(IKeyValueDBTransaction transaction);
    void CompactionStart(ulong totalWaste);
    void CompactionCreatedPureValueFile(uint fileId, ulong size, uint itemsInMap, ulong roughMemory);
    void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration);
    void TransactionLogCreated(uint fileId);
    void FileMarkedForDelete(uint fileId);

    void LogWarning(string message)
    {
    }

    void LogInfo(string message)
    {
    }
}
