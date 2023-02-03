using System;
using System.Collections.Generic;
using System.Threading;
using BTDB.Collections;
using BTDB.KVDBLayer.BTree;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

interface IKeyValueDBInternal : IKeyValueDB
{
    long GetGeneration(uint fileId);
    void MarkAsUnknown(IEnumerable<uint> fileIds);
    IFileCollectionWithFileInfos FileCollection { get; }
    bool ContainsValuesAndDoesNotTouchGeneration(uint fileKey, long dontTouchGeneration);
    bool AreAllTransactionsBeforeFinished(long transactionId);
    // This will reference that root, after use you need to call DereferenceRootNodeInternal
    IRootNodeInternal ReferenceAndGetOldestRoot();
    // This will reference that root, after use you need to call DereferenceRootNodeInternal
    IRootNodeInternal ReferenceAndGetLastCommitted();
    void DereferenceRootNodeInternal(IRootNodeInternal root);
    long ReplaceBTreeValues(CancellationToken cancellation, RefDictionary<ulong, uint> newPositionMap, uint targetFileId);
    long[] CreateIndexFile(CancellationToken cancellation, long preserveKeyIndexGeneration);
    ISpanWriter StartPureValuesFile(out uint fileId);
    bool LoadUsedFilesFromKeyIndex(uint fileId, IKeyIndex info);
    long CalculatePreserveKeyIndexGeneration(uint preserveKeyIndexKey);
    ulong DistanceFromLastKeyIndex(IRootNodeInternal root);
    Span<KeyIndexInfo> BuildKeyIndexInfos();
    uint CalculatePreserveKeyIndexKeyFromKeyIndexInfos(ReadOnlySpan<KeyIndexInfo> keyIndexes);
    uint GetTrLogFileId(IRootNodeInternal root);
    void IterateRoot(IRootNodeInternal root, ValuesIterateAction visit);
}
