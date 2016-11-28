using System;

namespace BTDB.KVDBLayer
{
    public interface IKeyValueDBLogger
    {
        void ReportTransactionLeak(IKeyValueDBTransaction transaction);
        void CompactionStart(ulong totalWaste);
        void CompactionCreatedPureValueFile(uint fileId, ulong size);
        void KeyValueIndexCreated(uint fileId, long keyValueCount, ulong size, TimeSpan duration);
    }
}