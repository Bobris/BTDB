using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    class FileKeyIndex : IFileInfo, IKeyIndex
    {
        readonly long _generation;
        readonly uint _trLogFileId;
        readonly uint _trLogOffset;
        readonly long _keyValueCount;

        public KVFileType FileType => KVFileType.KeyIndex;

        public long Generation => _generation;

        public long SubDBId => 0;

        public uint TrLogFileId => _trLogFileId;

        public uint TrLogOffset => _trLogOffset;

        public long KeyValueCount => _keyValueCount;

        public FileKeyIndex(AbstractBufferedReader reader)
        {
            _generation = reader.ReadVInt64();
            _trLogFileId = reader.ReadVUInt32();
            _trLogOffset = reader.ReadVUInt32();
            _keyValueCount = (long) reader.ReadVUInt64();
        }

        public FileKeyIndex(long generation, uint trLogFileId, uint trLogOffset, long keyCount)
        {
            _generation = generation;
            _trLogFileId = trLogFileId;
            _trLogOffset = trLogOffset;
            _keyValueCount = keyCount;
        }

        internal static void SkipHeader(AbstractBufferedReader reader)
        {
            reader.SkipBlock(FileCollectionWithFileInfos.MagicStartOfFile.Length + 1); // magic + type of file
            reader.SkipVInt64(); // generation
            reader.SkipVUInt32(); // trLogFileId
            reader.SkipVUInt32(); // trLogOffset
            reader.SkipVUInt64(); // keyValueCount
        }

        internal void WriteHeader(AbstractBufferedWriter writer)
        {
            writer.WriteByteArrayRaw(FileCollectionWithFileInfos.MagicStartOfFile);
            writer.WriteUInt8((byte)KVFileType.KeyIndex);
            writer.WriteVInt64(_generation);
            writer.WriteVUInt32(_trLogFileId);
            writer.WriteVUInt32(_trLogOffset);
            writer.WriteVUInt64((ulong)_keyValueCount);
        }
    }
}