using System;
using System.Runtime.CompilerServices;

namespace BTDB.ARTLib
{
    struct CursorItem
    {
        public CursorItem(IntPtr node, uint keyOffset, short posInNode, byte @byte)
        {
            _node = node;
            _keyOffset = keyOffset;
            _posInNode = posInNode;
            _byte = @byte;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(IntPtr node, uint keyOffset, short posInNode, byte @byte)
        {
            _node = node;
            _keyOffset = keyOffset;
            _posInNode = posInNode;
            _byte = @byte;
        }

        internal IntPtr _node;
        internal uint _keyOffset; // keyLength on this node
        internal short _posInNode; // -1 = it is leaf position
        internal byte _byte; // valid only if _posInNode >= 0
    }
}
