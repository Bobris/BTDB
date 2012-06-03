using BTDB.KV2DBLayer;
using BTDB.StreamLayer;

namespace BTDB.ChunkCache
{
    internal class FileHashIndex : IFileInfo
    {
        readonly long _generation;
        readonly long _keyValueCount;
        readonly int _keySize;

        public DiskChunkFileType FileType
        {
            get { return DiskChunkFileType.HashIndex; }
        }

        public long Generation
        {
            get { return _generation; }
        }

        internal int KeySize
        {
            get { return _keySize; }
        }

        internal long KeyValueCount
        {
            get { return _keyValueCount; }
        }

        public FileHashIndex(AbstractBufferedReader reader)
        {
            _generation = reader.ReadVInt64();
            _keySize = (int) reader.ReadVUInt32();
            _keyValueCount = (long)reader.ReadVUInt64();
        }

        public FileHashIndex(long generation, int keySize, long keyCount)
        {
            _generation = generation;
            _keySize = keySize;
            _keyValueCount = keyCount;
        }

        internal static void SkipHeader(AbstractBufferedReader reader)
        {
            reader.SkipBlock(DiskChunkCache.MagicStartOfFile.Length + 1); // magic + type of file
            reader.SkipVInt64(); // generation
            reader.SkipVUInt32(); // keySize
            reader.SkipVUInt64(); // keyValueCount
        }

        internal void WriteHeader(AbstractBufferedWriter writer)
        {
            writer.WriteByteArrayRaw(DiskChunkCache.MagicStartOfFile);
            writer.WriteUInt8((byte)DiskChunkFileType.HashIndex);
            writer.WriteVInt64(_generation);
            writer.WriteVUInt32((uint)KeySize);
            writer.WriteVUInt64((ulong)KeyValueCount);
        }
    }
}