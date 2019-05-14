using System;

namespace BTDB.ARTLib
{
    // Node Header
    // NodeType                                          0    1
    // Child Count (Node256 0=>256)                      1    1
    // Key Prefix Length (0xffff=>Stored as 4 bytes)     2    2
    // Reference Count                                   4    4
    // Recursive Child Count                             8    8
    // [Over 0xffff Key Prefix Length]                base    4
    // [HasSuffixes Children Suffix Offsets]                  2*(MaxChildren+1)
    // [HasSuffixes Suffix Bytes]                             *(-2)
    // [Prefix Key Data]                                      PrefixLen
    // [Value Data]                               upAlign4    12

    [Flags]
    enum NodeType12 : byte
    {
        NodeLeaf = 0,
        Node4 = 2,
        Node16 = 3,
        Node48 = 4,
        Node256 = 5,
        NodeSizeMask = 7,
        HasSuffixes = 8,
        IsLeaf = 16
    }
}
