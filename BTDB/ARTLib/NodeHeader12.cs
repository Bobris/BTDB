using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.ARTLib
{
    [StructLayout(LayoutKind.Explicit, Pack = 8, Size = 16)]
    struct NodeHeader12
    {
        [FieldOffset(0)]
        internal NodeType12 _nodeType;
        [FieldOffset(1)]
        internal byte _childCount;
        [FieldOffset(2)]
        internal ushort _keyPrefixLength;
        [FieldOffset(4)]
        internal int _referenceCount;
        [FieldOffset(8)]
        internal ulong _recursiveChildCount;

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsNode256() => (_nodeType & NodeType12.NodeSizeMask) == NodeType12.Node256;

        internal int ChildCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (_childCount == 0 && IsNode256()) ? 256 : _childCount;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => _childCount = (byte)value;
        }

        public bool IsFull
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => !IsNode256() && _childCount == NodeUtils12.MaxChildren(_nodeType);
        }
    }
}

