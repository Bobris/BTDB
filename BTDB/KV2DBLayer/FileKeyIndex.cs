using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    internal class FileKeyIndex : IFileInfo, IKeyIndex
    {
        readonly long _generation;
        readonly uint _trLogFileId;
        readonly uint _trLogOffset;
        readonly long _keyValueCount;

        public KV2FileType FileType
        {
            get { return KV2FileType.KeyIndex; }
        }

        public long Generation
        {
            get { return _generation; }
        }

        public long SubDBId
        {
            get { return 0; }
        }

        public uint TrLogFileId
        {
            get { return _trLogFileId; }
        }

        public uint TrLogOffset
        {
            get { return _trLogOffset; }
        }

        public long KeyValueCount
        {
            get { return _keyValueCount; }
        }

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
            writer.WriteUInt8((byte)KV2FileType.KeyIndex);
            writer.WriteVInt64(_generation);
            writer.WriteVUInt32(_trLogFileId);
            writer.WriteVUInt32(_trLogOffset);
            writer.WriteVUInt64((ulong)_keyValueCount);
        }
    }
}