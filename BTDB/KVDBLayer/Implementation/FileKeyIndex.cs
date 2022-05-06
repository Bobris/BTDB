using System;
using BTDB.StreamLayer;
using System.Linq;

namespace BTDB.KVDBLayer;

public class FileKeyIndex : IKeyIndex
{
    readonly long _generation;
    readonly Guid? _guid;
    readonly uint _trLogFileId;
    readonly uint _trLogOffset;
    readonly long _keyValueCount;
    readonly ulong _commitUlong;
    readonly ulong[]? _ulongs;
    readonly KeyIndexCompression _compressionType;

    public KVFileType FileType => KVFileType.KeyIndex;

    public Guid? Guid => _guid;

    public long Generation => _generation;

    public long SubDBId => 0;

    public uint TrLogFileId => _trLogFileId;

    public uint TrLogOffset => _trLogOffset;

    public long KeyValueCount => _keyValueCount;

    public ulong CommitUlong => _commitUlong;

    public ulong[]? Ulongs => _ulongs;

    public KeyIndexCompression Compression => _compressionType;

    public long[]? UsedFilesInOlderGenerations { get; set; }

    public FileKeyIndex(ref SpanReader reader, Guid? guid, bool withCommitUlong, bool modern, bool withUlongs)
    {
        _guid = guid;
        _generation = reader.ReadVInt64();
        _trLogFileId = reader.ReadVUInt32();
        _trLogOffset = reader.ReadVUInt32();
        _keyValueCount = (long)reader.ReadVUInt64();
        _commitUlong = withCommitUlong ? reader.ReadVUInt64() : 0;
        _compressionType = modern ? (KeyIndexCompression)reader.ReadUInt8() : KeyIndexCompression.Old;
        _ulongs = null;
        if (withUlongs)
        {
            _ulongs = new ulong[reader.ReadVUInt32()];
            for (var i = 0; i < _ulongs.Length; i++)
            {
                _ulongs[i] = reader.ReadVUInt64();
            }
        }
    }

    public FileKeyIndex(long generation, Guid? guid, uint trLogFileId, uint trLogOffset, long keyCount, ulong commitUlong, KeyIndexCompression compression, ulong[]? ulongs)
    {
        _guid = guid;
        _generation = generation;
        _trLogFileId = trLogFileId;
        _trLogOffset = trLogOffset;
        _keyValueCount = keyCount;
        _commitUlong = commitUlong;
        _compressionType = compression;
        _ulongs = ulongs?.ToArray();
    }

    public static void SkipHeader(ref SpanReader reader)
    {
        FileCollectionWithFileInfos.SkipHeader(ref reader);
        var type = (KVFileType)reader.ReadUInt8();
        var withCommitUlong = type == KVFileType.KeyIndexWithCommitUlong || type == KVFileType.ModernKeyIndex || type == KVFileType.ModernKeyIndexWithUlongs;
        reader.SkipVInt64(); // generation
        reader.SkipVUInt32(); // trLogFileId
        reader.SkipVUInt32(); // trLogOffset
        reader.SkipVUInt64(); // keyValueCount
        if (withCommitUlong) reader.SkipVUInt64(); // commitUlong
        if (type == KVFileType.ModernKeyIndex || type == KVFileType.ModernKeyIndexWithUlongs) reader.SkipUInt8();
        if (type == KVFileType.ModernKeyIndexWithUlongs)
        {
            var ulongCount = reader.ReadVUInt32();
            while (ulongCount-- > 0) reader.SkipVUInt64();
        }
    }

    internal void WriteHeader(ref SpanWriter writer)
    {
        FileCollectionWithFileInfos.WriteHeader(ref writer, _guid);
        writer.WriteUInt8((byte)KVFileType.ModernKeyIndexWithUlongs);
        writer.WriteVInt64(_generation);
        writer.WriteVUInt32(_trLogFileId);
        writer.WriteVUInt32(_trLogOffset);
        writer.WriteVUInt64((ulong)_keyValueCount);
        writer.WriteVUInt64(_commitUlong);
        writer.WriteUInt8((byte)_compressionType);
        var ulongCount = (uint)(_ulongs?.Length ?? 0);
        writer.WriteVUInt32(ulongCount);
        if (ulongCount > 0)
        {
            for (var i = 0; i < ulongCount; i++)
            {
                writer.WriteVUInt64(_ulongs![i]);
            }
        }
    }
}
