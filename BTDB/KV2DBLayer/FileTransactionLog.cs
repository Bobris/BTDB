using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    internal class FileTransactionLog : IFileInfo, IFileTransactionLog
    {
        readonly int _previousFileId;
        int _nextFileId;
        readonly long _generation;

        public FileTransactionLog(AbstractBufferedReader reader)
        {
            _generation = reader.ReadVInt64();
            _previousFileId = reader.ReadVInt32();
            _nextFileId = -2;
        }

        public KV2FileType FileType
        {
            get { return KV2FileType.TransactionLog; }
        }

        public long Generation
        {
            get { return _generation; }
        }

        public int PreviousFileId
        {
            get { return _previousFileId; }
        }

        public int NextFileId
        {
            get { return _nextFileId; }
            set { _nextFileId = value; }
        }

        internal static void SkipHeader(AbstractBufferedReader reader)
        {
            reader.SkipBlock(KeyValue2DB.MagicStartOfFile.Length + 1); // magic + type of file
            reader.SkipVInt64(); // generation
            reader.SkipVInt32(); // previous file id
        }
    }
}