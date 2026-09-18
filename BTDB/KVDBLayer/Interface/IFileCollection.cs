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

    /// Allocate above the given ID, preserving parity without creating intermediate files.
    IFileCollectionFile AddFile(string humanHint, FileIdParity parity, uint afterFileId)
    {
        if (afterFileId != 0) throw new NotSupportedException("This collection cannot allocate above a given file ID.");
        return AddFile(humanHint, parity);
    }

    uint GetCount();
    IFileCollectionFile GetFile(uint index);
    IEnumerable<IFileCollectionFile> Enumerate();
    void ConcurrentTemporaryTruncate(uint index, uint offset);
}
