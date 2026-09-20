using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer;

public interface IFileCollectionWithFileInfos
{
    IEnumerable<KeyValuePair<uint, IFileInfo>> FileInfos { get; }
    IEnumerable<KeyValuePair<uint, KVFileType>> FileTypes
    {
        get
        {
            foreach (var (id, info) in FileInfos) yield return new(id, info.FileType);
        }
    }
    long LastFileGeneration { get; }
    Guid? Guid { get; }
    IFileInfo? FileInfoByIdx(uint idx);
    void MakeIdxUnknown(uint key);
    void DeleteAllUnknownFiles();
    IFileCollectionFile? GetFile(uint fileId);
    uint GetCount();
    ulong GetSize(uint key);
    IFileCollectionFile AddFile(string humanHint);
    long NextGeneration();
    void SetInfo(uint idx, IFileInfo fileInfo);
    void ConcurentTemporaryTruncate(uint idx, uint offset);
}
