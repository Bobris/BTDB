using System;
using System.Collections.Generic;

namespace BTDB.KV2DBLayer
{
    public interface IFileCollection : IDisposable
    {
        IFileCollectionFile AddFile(string humanHint);
        uint GetCount();
        IFileCollectionFile GetFile(uint index);
        IEnumerable<IFileCollectionFile> Enumerate();
    }
}