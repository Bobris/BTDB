using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    class FilePureValuesWithId : IFileInfo
    {
        readonly long _subId;
        readonly long _generation;

        public FilePureValuesWithId(AbstractBufferedReader reader)
        {
            _subId = reader.ReadVInt64();
            _generation = reader.ReadVInt64();
        }

        public FilePureValuesWithId(long subId, long generation)
        {
            _subId = subId;
            _generation = generation;
        }

        public KVFileType FileType
        {
            get { return KVFileType.PureValuesWithId; }
        }

        public long Generation
        {
            get { return _generation; }
        }

        public long SubDBId
        {
            get { return _subId; }
        }

        public void WriteHeader(AbstractBufferedWriter writer)
        {
            writer.WriteByteArrayRaw(FileCollectionWithFileInfos.MagicStartOfFile);
            writer.WriteUInt8((byte)KVFileType.PureValuesWithId);
            writer.WriteVInt64(_subId);
            writer.WriteVInt64(_generation);
        }
    }
}