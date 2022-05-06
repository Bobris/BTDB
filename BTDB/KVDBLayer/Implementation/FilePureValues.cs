using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

class FilePureValues : IFileInfo
{
    readonly long _generation;
    readonly Guid? _guid;

    public FilePureValues(ref SpanReader reader, Guid? guid)
    {
        _guid = guid;
        _generation = reader.ReadVInt64();
    }

    public FilePureValues(long generation, Guid? guid)
    {
        _guid = guid;
        _generation = generation;
    }

    public KVFileType FileType => KVFileType.PureValues;

    public Guid? Guid => _guid;

    public long Generation => _generation;

    public long SubDBId => 0;

    public void WriteHeader(ref SpanWriter writer)
    {
        FileCollectionWithFileInfos.WriteHeader(ref writer, _guid);
        writer.WriteUInt8((byte)KVFileType.PureValues);
        writer.WriteVInt64(_generation);
    }
}
