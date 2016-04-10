using System;
using BTDB.Buffer;
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

        public KVFileType FileType => KVFileType.KeyIndex;

        public Guid? Guid => _guid;

        public long Generation => _generation;

        public long SubDBId => 0;

        public uint TrLogFileId => _trLogFileId;

        public uint TrLogOffset => _trLogOffset;

        public long KeyValueCount => _keyValueCount;

        public ulong CommitUlong => _commitUlong;

        public FileKeyIndex(AbstractBufferedReader reader, Guid? guid, bool withCommitUlong)
        {
            _guid = guid;
            _generation = reader.ReadVInt64();
            _trLogFileId = reader.ReadVUInt32();
            _trLogOffset = reader.ReadVUInt32();
            _keyValueCount = (long)reader.ReadVUInt64();
            _commitUlong = withCommitUlong ? reader.ReadVUInt64() : 0;
        }

        public FileKeyIndex(long generation, Guid? guid, uint trLogFileId, uint trLogOffset, long keyCount, ulong commitUlong)
        {
            _guid = guid;
            _generation = generation;
            _trLogFileId = trLogFileId;
            _trLogOffset = trLogOffset;
            _keyValueCount = keyCount;
            _commitUlong = commitUlong;
        }

        internal static void SkipHeader(AbstractBufferedReader reader)
        {
            FileCollectionWithFileInfos.SkipHeader(reader);
            var withCommitUlong = reader.ReadUInt8() == (byte)KVFileType.KeyIndexWithCommitUlong;
            reader.SkipVInt64(); // generation
            reader.SkipVUInt32(); // trLogFileId
            reader.SkipVUInt32(); // trLogOffset
            reader.SkipVUInt64(); // keyValueCount
            if (withCommitUlong) reader.SkipVUInt64(); // commitUlong
        }

        internal void WriteHeader(AbstractBufferedWriter writer)
        {
            FileCollectionWithFileInfos.WriteHeader(writer, _guid);
            var withCommitUlong = _commitUlong != 0;
            writer.WriteUInt8((byte)(withCommitUlong ? KVFileType.KeyIndexWithCommitUlong : KVFileType.KeyIndex));
            writer.WriteVInt64(_generation);
            writer.WriteVUInt32(_trLogFileId);
            writer.WriteVUInt32(_trLogOffset);
            writer.WriteVUInt64((ulong)_keyValueCount);
            if (withCommitUlong)
                writer.WriteVUInt64(_commitUlong);
        }
    }
}
