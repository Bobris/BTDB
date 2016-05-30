using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer
{
    public interface IFileCollection : IDisposable
    {
        IFileCollectionFile AddFile(string humanHint);
        uint GetCount();
        IFileCollectionFile GetFile(uint index);
        IEnumerable<IFileCollectionFile> Enumerate();
        void ConcurentTemporaryTruncate(uint index, uint offset);
    }
}