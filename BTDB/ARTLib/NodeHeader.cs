using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.ARTLib
{
    [StructLayout(LayoutKind.Explicit, Pack = 8, Size = 16)]
    struct NodeHeader
    {
        [FieldOffset(0)]
        internal NodeType _nodeType;
        [FieldOffset(1)]
        internal byte _childCount;
        [FieldOffset(2)]
        internal ushort _keyPrefixLength;
        [FieldOffset(4)]
        internal int _referenceCount;
        [FieldOffset(8)]
        internal ulong _recursiveChildCount;

        internal void Reference()
        {
            _referenceCount++;
        }

        internal bool Dereference()
        {
            return --_referenceCount == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsNode256() => (_nodeType & NodeType.NodeSizeMask) == NodeType.Node256;

        internal int ChildCount {
            get => (_childCount == 0 && IsNode256()) ? 256 : _childCount;
            set => _childCount = (byte)value;
        }

        public bool IsFull { get => !IsNode256() && _childCount == NodeUtils.MaxChildren(_nodeType); }
    }
}
