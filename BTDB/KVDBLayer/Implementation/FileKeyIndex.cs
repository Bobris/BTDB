using System;
using System.Collections.Generic;
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
    readonly bool _hasChecksum;

    // Set in the compression byte of a file ending with the XxHash64 of its content (KeyIndexChecksum) after the end
    // marker. Versions before it refuse such a file as of unknown compression.
    const byte ChecksumFlag = 0x80;

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

    public bool HasChecksum => _hasChecksum;

    public long[]? UsedFilesInOlderGenerations { get; set; }

    public FileKeyIndex(ref MemReader reader, Guid? guid, bool withCommitUlong, bool modern, bool withUlongs)
    {
        _guid = guid;
        _generation = reader.ReadVInt64();
        _trLogFileId = reader.ReadVUInt32();
        _trLogOffset = reader.ReadVUInt32();
        _keyValueCount = (long)reader.ReadVUInt64();
        _commitUlong = withCommitUlong ? reader.ReadVUInt64() : 0;
        var compression = modern ? reader.ReadUInt8() : (byte)KeyIndexCompression.Old;
        _hasChecksum = (compression & ChecksumFlag) != 0;
        _compressionType = (KeyIndexCompression)(compression & ~ChecksumFlag);
        _ulongs = null;
        if (withUlongs)
        {
            // Each value takes at least a byte, so a damaged count fails at the end of the file, growing with the values
            // read instead of being allocated at once.
            var count = reader.ReadVUInt32();
            var ulongs = new List<ulong>((int)Math.Min(count, 64u));
            for (var i = 0u; i < count; i++) ulongs.Add(reader.ReadVUInt64());
            _ulongs = ulongs.ToArray();
        }
    }

    public FileKeyIndex(long generation, Guid? guid, uint trLogFileId, uint trLogOffset, long keyCount,
        ulong commitUlong, KeyIndexCompression compression, ulong[]? ulongs)
    {
        _guid = guid;
        _generation = generation;
        _trLogFileId = trLogFileId;
        _trLogOffset = trLogOffset;
        _keyValueCount = keyCount;
        _commitUlong = commitUlong;
        _compressionType = compression;
        _ulongs = ulongs?.ToArray();
        _hasChecksum = true;
    }

    public static void SkipHeader(ref MemReader reader)
    {
        FileCollectionWithFileInfos.SkipHeader(ref reader);
        var type = (KVFileType)reader.ReadUInt8();
        var withCommitUlong = type is KVFileType.KeyIndexWithCommitUlong or KVFileType.ModernKeyIndex
            or KVFileType.ModernKeyIndexWithUlongs;
        reader.SkipVInt64(); // generation
        reader.SkipVUInt32(); // trLogFileId
        reader.SkipVUInt32(); // trLogOffset
        reader.SkipVUInt64(); // keyValueCount
        if (withCommitUlong) reader.SkipVUInt64(); // commitUlong
        if (type is KVFileType.ModernKeyIndex or KVFileType.ModernKeyIndexWithUlongs) reader.Skip1Byte();
        if (type == KVFileType.ModernKeyIndexWithUlongs)
        {
            var ulongCount = reader.ReadVUInt32();
            while (ulongCount-- > 0) reader.SkipVUInt64();
        }
    }

    internal void WriteHeader(ref MemWriter writer)
    {
        FileCollectionWithFileInfos.WriteHeader(ref writer, _guid);
        writer.WriteUInt8((byte)KVFileType.ModernKeyIndexWithUlongs);
        writer.WriteVInt64(_generation);
        writer.WriteVUInt32(_trLogFileId);
        writer.WriteVUInt32(_trLogOffset);
        writer.WriteVUInt64((ulong)_keyValueCount);
        writer.WriteVUInt64(_commitUlong);
        writer.WriteUInt8((byte)((byte)_compressionType | (_hasChecksum ? ChecksumFlag : 0)));
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
