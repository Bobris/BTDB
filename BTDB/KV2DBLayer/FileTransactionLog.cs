using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    internal class FileTransactionLog : IFileInfo, IFileTransactionLog
    {
        readonly int _previousFileId;
        int _nextFileId;
        long _startingTransactionId;

        public FileTransactionLog(AbstractBufferedReader reader)
        {
            _previousFileId = reader.ReadVInt32();
            _startingTransactionId = reader.ReadVInt64();
            _nextFileId = -2;
        }

        public KV2FileType FileType
        {
            get { return KV2FileType.TransactionLog; }
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

        public long StartingTransactionId
        {
            get { return _startingTransactionId; }
            set { _startingTransactionId = value; }
        }
    }
}