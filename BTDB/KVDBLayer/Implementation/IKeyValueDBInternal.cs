using System;
using System.Collections.Generic;
using System.Threading;
using BTDB.KVDBLayer.BTree;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    interface IKeyValueDBInternal : IKeyValueDB
    {
        long GetGeneration(uint fileId);
        void MarkAsUnknown(IEnumerable<uint> fileIds);
        IFileCollectionWithFileInfos FileCollection { get; }
        bool ContainsValuesAndDoesNotTouchGeneration(uint fileKey, long dontTouchGeneration);
        long MaxTrLogFileSize { get; }
        bool AreAllTransactionsBeforeFinished(long transactionId);
        IRootNodeInternal OldestRoot { get; }
        IRootNodeInternal LastCommited { get; }
        ulong CompactorWriteBytesPerSecondLimit { get; }
        ulong CompactorReadBytesPerSecondLimit { get; }
        long ReplaceBTreeValues(CancellationToken cancellation, Dictionary<ulong, ulong> newPositionMap);
        void CreateIndexFile(CancellationToken cancellation, long preserveKeyIndexGeneration);
        AbstractBufferedWriter StartPureValuesFile(out uint fileId);
        bool LoadUsedFilesFromKeyIndex(uint fileId, IKeyIndex info);
        long CalculatePreserveKeyIndexGeneration(uint preserveKeyIndexKey);
        ulong DistanceFromLastKeyIndex(IRootNodeInternal root);
        List<KeyIndexInfo> BuildKeyIndexInfos();
        uint CalculatePreserveKeyIndexKeyFromKeyIndexInfos(List<KeyIndexInfo> keyIndexes);
        uint GetTrLogFileId(IRootNodeInternal root);
        void IterateRoot(IRootNodeInternal root, ValuesIterateAction visit);
    }
}
