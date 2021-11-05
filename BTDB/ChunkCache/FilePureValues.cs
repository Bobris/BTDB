using BTDB.StreamLayer;

namespace BTDB.ChunkCache;

class FilePureValues : IFileInfo
{
    readonly long _generation;

    public FilePureValues(ref SpanReader reader)
    {
        _generation = reader.ReadVInt64();
    }

    public FilePureValues(long generation)
    {
        _generation = generation;
    }

    public DiskChunkFileType FileType => DiskChunkFileType.PureValues;

    public long Generation => _generation;

    public void WriteHeader(ref SpanWriter writer)
    {
        writer.WriteByteArrayRaw(DiskChunkCache.MagicStartOfFile);
        writer.WriteUInt8((byte)DiskChunkFileType.PureValues);
        writer.WriteVInt64(_generation);
    }
}
