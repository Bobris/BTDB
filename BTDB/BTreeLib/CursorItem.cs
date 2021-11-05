using System;
using System.Runtime.CompilerServices;

namespace BTDB.BTreeLib;

struct CursorItem
{
    public CursorItem(IntPtr node, byte posInNode)
    {
        _node = node;
        _posInNode = posInNode;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Set(IntPtr node, byte posInNode)
    {
        _node = node;
        _posInNode = posInNode;
    }

    internal IntPtr _node;
    internal byte _posInNode;
}
