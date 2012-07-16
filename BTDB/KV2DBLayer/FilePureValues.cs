using BTDB.StreamLayer;

namespace BTDB.KV2DBLayer
{
    internal class FilePureValues : IFileInfo
    {
        readonly long _generation;

        public FilePureValues(AbstractBufferedReader reader)
        {
            _generation = reader.ReadVInt64();
        }

        public FilePureValues(long generation)
        {
            _generation = generation;
        }

        public KV2FileType FileType
        {
            get { return KV2FileType.PureValues; }
        }

        public long Generation
        {
            get { return _generation; }
        }

        public long SubDBId
        {
            get { return 0; }
        }

        public void WriteHeader(AbstractBufferedWriter writer)
        {
            writer.WriteByteArrayRaw(KeyValue2DB.MagicStartOfFile);
            writer.WriteUInt8((byte)KV2FileType.PureValues);
            writer.WriteVInt64(_generation);
        }
    }
}