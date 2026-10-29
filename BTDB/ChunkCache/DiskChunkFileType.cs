namespace BTDB.ChunkCache;

enum DiskChunkFileType : byte
{
    HashIndex,
    // Values without checksums, written before checksums were added; their files are dropped like unknown ones.
    PureValues,
    Unknown,
    // Values each followed by the XxHash64 of its content.
    CheckedPureValues
}
