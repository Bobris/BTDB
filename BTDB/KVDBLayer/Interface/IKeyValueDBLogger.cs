using System;

namespace BTDB.KVDBLayer;

public interface IKeyValueDBLogger
{
    void ReportTransactionLeak(IKeyValueDBTransaction transaction);
    void CompactionStart(ulong totalWaste);
    void CompactionCreatedPureValueFile(uint fileId, ulong size, uint itemsInMap, ulong roughMemory);

    void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration,
        ulong beforeCompressionSize);

    void TransactionLogCreated(uint fileId);
    void FileMarkedForDelete(uint fileId);

    void LogWarning(string message)
    {
    }

    /// <returns>true when exception should be rethrown</returns>
    bool ReportCompactorException(Exception exception)
    {
        LogWarning("Compactor failed with " + exception);
        return true;
    }
}
