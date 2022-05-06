using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

class FilePureValuesWithId : IFileInfo
{
    readonly Guid? _guid;
    readonly long _subId;
    readonly long _generation;

    public FilePureValuesWithId(ref SpanReader reader, Guid? guid)
    {
        _guid = guid;
        _subId = reader.ReadVInt64();
        _generation = reader.ReadVInt64();
    }

    public FilePureValuesWithId(long subId, long generation, Guid? guid)
    {
        _guid = guid;
        _subId = subId;
        _generation = generation;
    }

    public KVFileType FileType => KVFileType.PureValuesWithId;

    public Guid? Guid => _guid;

    public long Generation => _generation;

    public long SubDBId => _subId;

    public void WriteHeader(ref SpanWriter writer)
    {
        FileCollectionWithFileInfos.WriteHeader(ref writer, _guid);
        writer.WriteUInt8((byte)KVFileType.PureValuesWithId);
        writer.WriteVInt64(_subId);
        writer.WriteVInt64(_generation);
    }
}
