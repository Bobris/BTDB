using BTDB.Buffer;
using BTDB.Collections;
using BTDB.KVDBLayer;
using System;
using System.Runtime.CompilerServices;

namespace BTDB.BTreeLib
{
    class Cursor12 : ICursor
    {
        RootNode12 _rootNode;
        StructList<CursorItem> _stack;

        public Cursor12(RootNode12 rootNode)
        {
            _rootNode = rootNode;
            _stack = new StructList<CursorItem>();
        }

        Cursor12(Cursor12 from)
        {
            _rootNode = from._rootNode;
            _stack = new StructList<CursorItem>(from._stack);
        }

        public void SetNewRoot(IRootNode artRoot)
        {
            var newRoot = (RootNode12)artRoot;
            if (newRoot._impl != _rootNode._impl)
                throw new ArgumentException("SetNewRoot allows only same db instance");
            _rootNode = (RootNode12)artRoot;
        }

        public long CalcDistance(ICursor to)
        {
            if (_rootNode != ((Cursor12)to)._rootNode)
                throw new ArgumentException("Cursor must be from same transaction", nameof(to));
            return to.CalcIndex() - CalcIndex();
        }

        public long CalcIndex()
        {
            return _rootNode._impl.CalcIndex(_stack.AsSpan());
        }

        public ICursor Clone()
        {
            return new Cursor12(this);
        }

        public void Erase()
        {
            AssertWritable();
            AssertValid();
            _rootNode._impl.EraseRange(_rootNode, ref _stack, ref _stack);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AssertWritable()
        {
            if (!_rootNode._writable)
            {
                TreeNodeUtils.ThrowCursorNotWritable();
            }
        }

        public long EraseTo(ICursor to)
        {
            AssertWritable();
            if (_rootNode != ((Cursor12)to)._rootNode)
                throw new ArgumentException("Both cursors must be from same transaction", nameof(to));
            if (!to.IsValid())
                throw new ArgumentException("Cursor must be valid", nameof(to));
            if (!IsValid())
                throw new ArgumentException("Cursor must be valid", "this");
            return _rootNode._impl.EraseRange(_rootNode, ref _stack, ref ((Cursor12)to)._stack);
        }

        public bool FindExact(ReadOnlySpan<byte> key)
        {
            return _rootNode._impl.FindExact(_rootNode, ref _stack, key);
        }

        public FindResult Find(ReadOnlySpan<byte> key)
        {
            return BTreeImpl12.Find(_rootNode, ref _stack, key);
        }

        public FindResult Find(ReadOnlySpan<byte> keyPrefix, ReadOnlySpan<byte> key)
        {
            return _rootNode._impl.Find(_rootNode, ref _stack, keyPrefix, key);
        }

        public bool FindFirst(ReadOnlySpan<byte> keyPrefix)
        {
            return _rootNode._impl.FindFirst(_rootNode, ref _stack, keyPrefix);
        }

        public long FindLastWithPrefix(ReadOnlySpan<byte> keyPrefix)
        {
            return BTreeImpl12.FindLastWithPrefix(_rootNode, keyPrefix);
        }

        public int GetKeyLength()
        {
            if (_stack.Count == 0) return -1;
            ref var stackItem = ref _stack[_stack.Count - 1];
            ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
            if (header.HasLongKeys)
            {
                var keys = NodeUtils12.GetLongKeyPtrs(stackItem._node);
                return header._keyPrefixLength + TreeNodeUtils.ReadInt32Aligned(keys[stackItem._posInNode]);
            }
            var keyOffsets = NodeUtils12.GetKeySpans(stackItem._node, out var _);
            return header._keyPrefixLength + keyOffsets[stackItem._posInNode + 1] - keyOffsets[stackItem._posInNode];
        }

        public bool IsValid()
        {
            return _stack.Count != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AssertValid()
        {
            if (_stack.Count == 0)
            {
                TreeNodeUtils.ThrowCursorHaveToBeValid();
            }
        }

        public unsafe Span<byte> FillByKey(Span<byte> buffer)
        {
            AssertValid();
            ref var stackItem = ref _stack[_stack.Count - 1];
            ref var header = ref NodeUtils12.Ptr2NodeHeader(stackItem._node);
            if (header.HasLongKeys)
            {
                var keys = NodeUtils12.GetLongKeyPtrs(stackItem._node);
                var keyPtr = keys[stackItem._posInNode];
                var lenSuffix = TreeNodeUtils.ReadInt32Aligned(keyPtr);
                var len = header._keyPrefixLength + lenSuffix;
                var res = buffer.Slice(0, len);
                NodeUtils12.GetPrefixSpan(stackItem._node).CopyTo(res);
                new Span<byte>((keyPtr + 4).ToPointer(), lenSuffix).CopyTo(res.Slice(header._keyPrefixLength));
                return res;
            }
            else
            {
                var keyOffsets = NodeUtils12.GetKeySpans(stackItem._node, out var keySuffixes);
                var ofs = keyOffsets[stackItem._posInNode];
                var lenSuffix = keyOffsets[stackItem._posInNode + 1] - ofs;
                var len = header._keyPrefixLength + lenSuffix;
                var res = buffer.Slice(0, len);
                NodeUtils12.GetPrefixSpan(stackItem._node).CopyTo(res);
                keySuffixes.Slice(ofs, lenSuffix).CopyTo(res.Slice(header._keyPrefixLength));
                return res;
            }
        }

        public byte[] GetKeyAsByteArray()
        {
            AssertValid();
            var result = new byte[GetKeyLength()];
            FillByKey(result);
            return result;
        }

        public unsafe bool KeyHasPrefix(ReadOnlySpan<byte> prefix)
        {
            if (_stack.Count == 0)
                return false;
            ref var stackItem = ref _stack[_stack.Count - 1];
            return BTreeImpl12.IsKeyPrefix(stackItem._node, stackItem._posInNode, prefix);
        }

        public int GetValueLength()
        {
            AssertValid();
            return 12;
        }

        public ReadOnlySpan<byte> GetValue()
        {
            AssertValid();
            var stackItem = _stack[_stack.Count - 1];
            var vals = NodeUtils12.GetLeafValues(stackItem._node);
            return vals.Slice(stackItem._posInNode * 12, 12);
        }

        public bool MoveNext()
        {
            if (_stack.Count == 0)
            {
                return FindFirst(new ReadOnlySpan<byte>());
            }

            return _rootNode._impl.MoveNext(ref _stack);
        }

        public bool MovePrevious()
        {
            if (_stack.Count == 0)
            {
                return BTreeImpl12.MoveToLast(_rootNode._root, ref _stack);
            }

            return _rootNode._impl.MovePrevious(ref _stack);
        }

        public bool SeekIndex(long index)
        {
            _stack.Clear();
            if (index < 0)
            {
                return false;
            }

            return _rootNode._impl.SeekIndex(index, _rootNode._root, ref _stack);
        }

        public bool Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> content)
        {
            AssertWritable();
            return _rootNode._impl.Upsert(_rootNode, ref _stack, key, content);
        }

        public void WriteValue(ReadOnlySpan<byte> content)
        {
            AssertWritable();
            AssertValid();
            _rootNode._impl.WriteValue(_rootNode, ref _stack, content);
        }

        public void Invalidate()
        {
            _stack.Clear();
        }

        public void BuildTree(long keyCount, BuildTreeCallback generator)
        {
            AssertWritable();
            Invalidate();
            _rootNode._impl.BuildTree(_rootNode, keyCount, generator);
        }

        public void ValueReplacer(ref ValueReplacerCtx ctx)
        {
            AssertWritable();
            if (_rootNode._root == IntPtr.Zero)
                return;
            AssertValid();
            var newRoot = _rootNode._impl.ValueReplacer(ref ctx, _stack.AsSpan(), 0);
            if (_rootNode._root != newRoot)
            {
                _rootNode._impl.Dereference(_rootNode._root);
                _rootNode._root = newRoot;
            }
            _stack.Clear();
        }
    }
}
