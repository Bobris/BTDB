using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    class HashKeyIndex : IFileInfo, IHashKeyIndex
    {
        readonly long _subId;
        readonly long _generation;
        readonly uint _keyLen;

        public KVFileType FileType
        {
            get { return KVFileType.HashKeyIndex; }
        }

        public long Generation
        {
            get { return _generation; }
        }

        public long SubDBId
        {
            get { return _subId; }
        }

        public uint KeyLen
        {
            get { return _keyLen; }
        }

        public HashKeyIndex(AbstractBufferedReader reader)
        {
            _subId = reader.ReadVInt64();
            _generation = reader.ReadVInt64();
            _keyLen = reader.ReadVUInt32();
        }

        public HashKeyIndex(long subId, long generation, uint keyLen)
        {
            _subId = subId;
            _generation = generation;
            _keyLen = keyLen;
        }

        internal static void SkipHeader(AbstractBufferedReader reader)
        {
            reader.SkipBlock(FileCollectionWithFileInfos.MagicStartOfFile.Length + 1); // magic + type of file
            reader.SkipVInt64(); // subId
            reader.SkipVInt64(); // generation
            reader.SkipVUInt32(); // keyLen
        }

        internal void WriteHeader(AbstractBufferedWriter writer)
        {
            writer.WriteByteArrayRaw(FileCollectionWithFileInfos.MagicStartOfFile);
            writer.WriteUInt8((byte) KVFileType.HashKeyIndex);
            writer.WriteVInt64(_subId);
            writer.WriteVInt64(_generation);
            writer.WriteVUInt32(_keyLen);
        }
    }
}