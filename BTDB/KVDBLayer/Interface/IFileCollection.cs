using System;
using System.Collections.Generic;

namespace BTDB.KVDBLayer;

public interface IFileCollection : IDisposable
{
    IFileCollectionFile AddFile(string humanHint);
    /// Allocate a fresh identity with the requested parity, without creating intermediate files.
    IFileCollectionFile AddFile(string humanHint, FileIdParity parity)
    {
        if (parity != FileIdParity.Any)
            throw new NotSupportedException("This file collection does not support constrained file ID parity.");
        return AddFile(humanHint);
    }

    uint GetCount();
    IFileCollectionFile GetFile(uint index);
    IEnumerable<IFileCollectionFile> Enumerate();
    void ConcurrentTemporaryTruncate(uint index, uint offset);
}
