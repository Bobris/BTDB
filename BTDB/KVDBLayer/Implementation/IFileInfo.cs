using System;

namespace BTDB.KVDBLayer;

public interface IFileInfo
{
    KVFileType FileType { get; }
    Guid? Guid { get; }
    long Generation { get; }
    long SubDBId { get; }
}
