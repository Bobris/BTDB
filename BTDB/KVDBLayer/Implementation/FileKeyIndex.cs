using System;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    class FileKeyIndex : IFileInfo, IKeyIndex
    {
        readonly long _generation;
        readonly Guid? _guid;
        readonly uint _trLogFileId;
        readonly uint _trLogOffset;
        readonly long _keyValueCount;
        readonly ulong _commitUlong;
        readonly KeyIndexCompression _compressionType;

        public KVFileType FileType => KVFileType.KeyIndex;

        public Guid? Guid => _guid;

        public long Generation => _generation;

        public long SubDBId => 0;

        public uint TrLogFileId => _trLogFileId;

        public uint TrLogOffset => _trLogOffset;

        public long KeyValueCount => _keyValueCount;

        public ulong CommitUlong => _commitUlong;

        public KeyIndexCompression Compression => _compressionType;

        public FileKeyIndex(AbstractBufferedReader reader, Guid? guid, bool withCommitUlong, bool modern)
        {
            _guid = guid;
            _generation = reader.ReadVInt64();
            _trLogFileId = reader.ReadVUInt32();
            _trLogOffset = reader.ReadVUInt32();
            _keyValueCount = (long)reader.ReadVUInt64();
            _commitUlong = withCommitUlong ? reader.ReadVUInt64() : 0;
            _compressionType = modern ? (KeyIndexCompression)reader.ReadUInt8() : KeyIndexCompression.Old;
        }

        public FileKeyIndex(long generation, Guid? guid, uint trLogFileId, uint trLogOffset, long keyCount, ulong commitUlong, KeyIndexCompression compression)
        {
            _guid = guid;
            _generation = generation;
            _trLogFileId = trLogFileId;
            _trLogOffset = trLogOffset;
            _keyValueCount = keyCount;
            _commitUlong = commitUlong;
            _compressionType = compression;
        }

        internal static void SkipHeader(AbstractBufferedReader reader)
        {
            FileCollectionWithFileInfos.SkipHeader(reader);
            var type = (KVFileType)reader.ReadUInt8();
            var withCommitUlong = type == KVFileType.KeyIndexWithCommitUlong || type == KVFileType.ModernKeyIndex;
            reader.SkipVInt64(); // generation
            reader.SkipVUInt32(); // trLogFileId
            reader.SkipVUInt32(); // trLogOffset
            reader.SkipVUInt64(); // keyValueCount
            if (withCommitUlong) reader.SkipVUInt64(); // commitUlong
            if (type == KVFileType.ModernKeyIndex) reader.SkipUInt8();
        }

        internal void WriteHeader(AbstractBufferedWriter writer)
        {
            FileCollectionWithFileInfos.WriteHeader(writer, _guid);
            writer.WriteUInt8((byte)KVFileType.ModernKeyIndex);
            writer.WriteVInt64(_generation);
            writer.WriteVUInt32(_trLogFileId);
            writer.WriteVUInt32(_trLogOffset);
            writer.WriteVUInt64((ulong)_keyValueCount);
            writer.WriteVUInt64(_commitUlong);
            writer.WriteUInt8((byte) _compressionType);
        }
    }
}
