using BTDB.KV2DBLayer;
using BTDB.StreamLayer;

namespace BTDB.ChunkCache
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

        public DiskChunkFileType FileType
        {
            get { return DiskChunkFileType.PureValues; }
        }

        public long Generation
        {
            get { return _generation; }
        }

        public void WriteHeader(AbstractBufferedWriter writer)
        {
            writer.WriteByteArrayRaw(KeyValue2DB.MagicStartOfFile);
            writer.WriteUInt8((byte)KV2FileType.PureValues);
            writer.WriteVInt64(_generation);
        }
    }
}