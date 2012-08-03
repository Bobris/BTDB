using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    internal class FilePureValuesWithId : IFileInfo
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

        public KV2FileType FileType
        {
            get { return KV2FileType.PureValuesWithId; }
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
            writer.WriteUInt8((byte)KV2FileType.PureValuesWithId);
            writer.WriteVInt64(_subId);
            writer.WriteVInt64(_generation);
        }
    }
}