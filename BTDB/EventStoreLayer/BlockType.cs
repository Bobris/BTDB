using System;

namespace BTDB.EventStoreLayer;

[Flags]
enum BlockType : byte
{
    FirstBlock = 1,
    MiddleBlock = 2,
    LastBlock = 4,
    HasTypeDeclaration = 8,
    HasMetadata = 16,
    HasOneEvent = 32,
    HasMoreEvents = 64,
    Compressed = 128
}
