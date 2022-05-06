using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer;

class HashKeyIndex : IFileInfo, IHashKeyIndex
{
    readonly Guid? _guid;
    readonly long _subId;
    readonly long _generation;
    readonly uint _keyLen;

    public KVFileType FileType => KVFileType.HashKeyIndex;

    public Guid? Guid => _guid;

    public long Generation => _generation;

    public long SubDBId => _subId;

    public uint KeyLen => _keyLen;

    public HashKeyIndex(ref SpanReader reader, Guid? guid)
    {
        _guid = guid;
        _subId = reader.ReadVInt64();
        _generation = reader.ReadVInt64();
        _keyLen = reader.ReadVUInt32();
    }

    public HashKeyIndex(long subId, long generation, Guid? guid, uint keyLen)
    {
        _guid = guid;
        _subId = subId;
        _generation = generation;
        _keyLen = keyLen;
    }

    internal static void SkipHeader(ref SpanReader reader)
    {
        FileCollectionWithFileInfos.SkipHeader(ref reader);
        reader.SkipUInt8(); // type of file
        reader.SkipVInt64(); // subId
        reader.SkipVInt64(); // generation
        reader.SkipVUInt32(); // keyLen
    }

    internal void WriteHeader(ref SpanWriter writer)
    {
        FileCollectionWithFileInfos.WriteHeader(ref writer, _guid);
        writer.WriteUInt8((byte)KVFileType.HashKeyIndex);
        writer.WriteVInt64(_subId);
        writer.WriteVInt64(_generation);
        writer.WriteVUInt32(_keyLen);
    }
}
