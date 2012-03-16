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

        public KV2FileType FileType
        {
            get { return KV2FileType.PureValues; }
        }

        public long Generation
        {
            get { return _generation; }
        }
    }
}