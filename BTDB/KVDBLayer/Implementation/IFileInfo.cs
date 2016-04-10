using System;

namespace BTDB.KVDBLayer
{
    interface IFileInfo
    {
        KVFileType FileType { get; }
        Guid? Guid { get; }
        long Generation { get; }
        long SubDBId { get; }
    }
}