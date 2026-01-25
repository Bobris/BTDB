using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BTDB.Collections;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

public delegate void ValuesIterateAction(uint valueFileId, uint valueOfs, int valueSize);

interface IKeyValueDBInternal : IKeyValueDB
{
    long GetGeneration(uint fileId);

    // Returns true if any marked file was TRL
    bool MarkAsUnknown(IEnumerable<uint> fileIds);
    IFileCollectionWithFileInfos FileCollection { get; }
    bool ContainsValuesAndDoesNotTouchGeneration(uint fileKey, long dontTouchGeneration);

    bool AreAllTransactionsBeforeFinished(long transactionId);

    // This will reference that root, after use you need to call DereferenceRootNodeInternal
    IRootNodeInternal ReferenceAndGetOldestRoot();

    // This will reference that root, after use you need to call DereferenceRootNodeInternal
    IRootNodeInternal ReferenceAndGetLastCommitted();
    void DereferenceRootNodeInternal(IRootNodeInternal root);

    ValueTask<long> ReplaceBTreeValues(CancellationToken cancellation, RefDictionary<ulong, uint> newPositionMap,
        uint targetFileId);

    long[] CreateIndexFile(CancellationToken cancellation, long preserveKeyIndexGeneration);
    MemWriter StartPureValuesFile(out uint fileId);
    bool LoadUsedFilesFromKeyIndex(uint fileId, IKeyIndex info);
    long CalculatePreserveKeyIndexGeneration(uint preserveKeyIndexKey);
    ulong DistanceFromLastKeyIndex(IRootNodeInternal root);
    Span<KeyIndexInfo> BuildKeyIndexInfos();
    uint CalculatePreserveKeyIndexKeyFromKeyIndexInfos(ReadOnlySpan<KeyIndexInfo> keyIndexes);
    uint GetTrLogFileId(IRootNodeInternal root);
    void IterateRoot(IRootNodeInternal root, ValuesIterateAction visit);
    void GatherUsedFiles(CancellationToken cancellation, IRootNodeInternal root, ISet<uint> usedFileIds);
}
