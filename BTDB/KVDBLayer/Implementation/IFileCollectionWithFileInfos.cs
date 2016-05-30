using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer
{
    interface IFileCollectionWithFileInfos
    {
        IEnumerable<KeyValuePair<uint, IFileInfo>> FileInfos { get; }
        long LastFileGeneration { get; }
        Guid? Guid { get; }
        IFileInfo FileInfoByIdx(uint idx);
        void MakeIdxUnknown(uint key);
        void DeleteAllUnknownFiles();
        IFileCollectionFile GetFile(uint fileId);
        uint GetCount();
        ulong GetSize(uint key);
        IFileCollectionFile AddFile(string humanHint);
        long NextGeneration();
        void SetInfo(uint idx, IFileInfo fileInfo);
        void ConcurentTemporaryTruncate(uint idx, uint offset);
    }
}