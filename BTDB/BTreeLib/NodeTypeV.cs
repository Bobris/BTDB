using System;

namespace BTDB.BTreeLib;

// Node Header
// NodeType                                          0    1
// Child Count                                       1    1
// Key Prefix Length                                 2    2
// Reference Count                                   4    4
// Recursive Child Count (Branch)                    8    8
// [Prefix Key Data]                              8/16    PrefixLen

// **** Short Keys ****
// upAlign2                                               0-1
// [Key Suffixes Offsets]                                 2*(Key Count+1)
// [Key Suffix Bytes]
// upAlign8 (Branch) upAlign4 (Leaf)
// [ChildPtrs 8] (Branch) [ValuePtrs 8] (Leaf) * Value Count

// **** Long Keys ****
// upAlign 8
// [LongKeySuffixPtrs 8] * Key Count
// [ChildPtrs 8] (Branch) [ValuePtrs 8] (Leaf) * Value Count

// LongKeySuffix 4 bytes length, data follows
// Value 4 bytes length, data follows

[Flags]
enum NodeTypeV : byte
{
    IsLeaf = 1,
    HasLongKeys = 2
}
