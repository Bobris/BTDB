using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    internal class FileKeyIndex : IFileInfo, IKeyIndex
    {
        readonly long _generation;
        readonly int _trLogFileId;
        readonly int _trLogOffset;
        readonly long _keyValueCount;

        public KV2FileType FileType
        {
            get { return KV2FileType.KeyIndex; }
        }

        public long Generation
        {
            get { return _generation; }
        }

        public int TrLogFileId
        {
            get { return _trLogFileId; }
        }

        public int TrLogOffset
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
            _trLogFileId = (int) reader.ReadVUInt32();
            _trLogOffset = (int) reader.ReadVUInt32();
            _keyValueCount = (long) reader.ReadVUInt64();
        }

        internal static void SkipHeader(AbstractBufferedReader reader)
        {
            reader.SkipBlock(KeyValue2DB.MagicStartOfFile.Length + 1); // magic + type of file
            reader.SkipVInt64(); // generation
            reader.SkipVUInt32(); // trLogFileId
            reader.SkipVUInt32(); // trLogOffset
            reader.SkipVUInt64(); // keyValueCount
        }

    }
}