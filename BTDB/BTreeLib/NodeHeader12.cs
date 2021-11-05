using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.BTreeLib;

[StructLayout(LayoutKind.Explicit, Pack = 8, Size = 16)]
struct NodeHeader12
{
    [FieldOffset(0)]
    internal NodeType12 _nodeType;
    [FieldOffset(1)]
    internal byte _childCount;
    [FieldOffset(2)]
    internal ushort _keyPrefixLength; // must be zero for branches
    [FieldOffset(4)]
    internal int _referenceCount;
    // Next field is only for Branches because for Leafs it is equal to _childCount
    [FieldOffset(8)]
    internal ulong _recursiveChildCount;

    public const int LeafHeaderSize = 8;
    public const int BranchHeaderSize = 16;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Reference()
    {
        _referenceCount++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool Dereference()
    {
        return --_referenceCount == 0;
    }

    public bool IsNodeLeaf
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (_nodeType & NodeType12.IsLeaf) != 0;
    }

    public bool HasLongKeys
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (_nodeType & NodeType12.HasLongKeys) != 0;
    }

    public uint Size
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => IsNodeLeaf ? 8u : 16u;
    }

    public ulong RecursiveChildCount
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => IsNodeLeaf ? _childCount : _recursiveChildCount;
    }
    public int KeyCount
    {
        get
        {
            if (IsNodeLeaf) return _childCount;
            return _childCount - 1;
        }
    }

    public bool IsDegenerated => !IsNodeLeaf && _childCount == 1;
}

