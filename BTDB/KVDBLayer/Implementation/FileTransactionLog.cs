using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    class FileTransactionLog : IFileInfo, IFileTransactionLog
    {
        readonly uint _previousFileId;
        readonly long _generation;

        public FileTransactionLog(AbstractBufferedReader reader)
        {
            _generation = reader.ReadVInt64();
            _previousFileId = (uint) reader.ReadVInt32();
        }

        public FileTransactionLog(long generation, uint fileIdWithPreviousTransactionLog)
        {
            _generation = generation;
            _previousFileId = fileIdWithPreviousTransactionLog;
        }

        public KVFileType FileType
        {
            get { return KVFileType.TransactionLog; }
        }

        public long Generation
        {
            get { return _generation; }
        }

        public long SubDBId
        {
            get { return 0; }
        }

        public uint PreviousFileId
        {
            get { return _previousFileId; }
        }

        public uint NextFileId { get; set; }

        internal static void SkipHeader(AbstractBufferedReader reader)
        {
            reader.SkipBlock(FileCollectionWithFileInfos.MagicStartOfFile.Length + 1); // magic + type of file
            reader.SkipVInt64(); // generation
            reader.SkipVInt32(); // previous file id
        }

        internal void WriteHeader(AbstractBufferedWriter writer)
        {
            writer.WriteByteArrayRaw(FileCollectionWithFileInfos.MagicStartOfFile);
            writer.WriteUInt8((byte)KVFileType.TransactionLog);
            writer.WriteVInt64(_generation);
            writer.WriteVInt32((int) _previousFileId);
        }
    }
}