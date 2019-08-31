using BTDB.Buffer;
using BTDB.Collections;
using BTDB.KVDBLayer;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.ARTLib
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
            if (newRoot._root != _rootNode._root)
                throw new ArgumentException("SetNewRoot allows only upgrades to writable identical root");
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
            return _rootNode._impl.CalcIndex(_stack);
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

        public void StructureCheck()
        {
            _rootNode._impl.StructureCheck(_rootNode._root);
        }

        public bool FindExact(ReadOnlySpan<byte> key)
        {
            return _rootNode._impl.FindExact(_rootNode, ref _stack, key);
        }

        public FindResult Find(ReadOnlySpan<byte> keyPrefix, ReadOnlySpan<byte> key)
        {
            return _rootNode._impl.Find(_rootNode, ref _stack, keyPrefix, key);
        }

        public bool FindFirst(ReadOnlySpan<byte> keyPrefix)
        {
            return _rootNode._impl.FindFirst(_rootNode, ref _stack, keyPrefix);
        }

        public bool FindLast(ReadOnlySpan<byte> keyPrefix)
        {
            return _rootNode._impl.FindLast(_rootNode, ref _stack, keyPrefix);
        }

        public int GetKeyLength()
        {
            if (_stack.Count == 0) return -1;
            ref var stackItem = ref _stack[_stack.Count - 1];
            var offset = stackItem._keyOffset;
            if (stackItem._posInNode >= 0)
            {
                offset += NodeUtils12.GetSuffixSize(stackItem._node, stackItem._posInNode);
            }
            return (int)offset;
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
            var stack = _stack;
            var offset = 0;
            var i = 0u;
            while (i < stack.Count)
            {
                ref var stackItem = ref stack[i++];
                if (offset < stackItem._keyOffset - (stackItem._posInNode == -1 ? 0 : 1))
                {
                    var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(stackItem._node);
                    new Span<byte>(keyPrefixPtr.ToPointer(), (int)keyPrefixSize).CopyTo(buffer.Slice(offset));
                    offset += (int)keyPrefixSize;
                }

                if (stackItem._posInNode != -1)
                {
                    buffer[offset++] = stackItem._byte;
                    if (i == stack.Count)
                    {
                        var (suffixSize, suffixPtr) = NodeUtils12.GetSuffixSizeAndPtr(stackItem._node, stackItem._posInNode);
                        new Span<byte>(suffixPtr.ToPointer(), (int)suffixSize).CopyTo(buffer.Slice(offset));
                        offset += (int)suffixSize;
                        break;
                    }
                }
            }

            return buffer.Slice(0, offset);
        }

        public byte[] GetKeyAsByteArray()
        {
            AssertValid();
            var result = new byte[GetKeyLength()];
            FillByKey(result);
            return result;
        }

        public bool KeyHasPrefix(ReadOnlySpan<byte> prefix)
        {
            if (_stack.Count == 0)
                return false;
            var stack = _stack;
            var offset = 0;
            var i = 0;
            while (offset < prefix.Length && i < stack.Count)
            {
                ref var stackItem = ref stack[(uint)i++];
                if (offset < stackItem._keyOffset - (stackItem._posInNode == -1 ? 0 : 1))
                {
                    var (keyPrefixSize, keyPrefixPtr) = NodeUtils12.GetPrefixSizeAndPtr(stackItem._node);
                    unsafe
                    {
                        var commonLength = Math.Min((int)keyPrefixSize, prefix.Length - offset);
                        if (!new Span<byte>(keyPrefixPtr.ToPointer(), commonLength).SequenceEqual(prefix.Slice(offset,
                            commonLength)))
                            return false;
                    }

                    offset += (int)keyPrefixSize;
                    if (offset >= prefix.Length)
                        return true;
                }

                if (stackItem._posInNode != -1)
                {
                    if (prefix[offset] != stackItem._byte)
                        return false;
                    offset++;
                    if (offset >= prefix.Length)
                        return true;
                    if (i == stack.Count)
                    {
                        var (suffixSize, suffixPtr) = NodeUtils12.GetSuffixSizeAndPtr(stackItem._node, stackItem._posInNode);
                        unsafe
                        {
                            var commonLength = Math.Min((int)suffixSize, prefix.Length - offset);
                            if (!new Span<byte>(suffixPtr.ToPointer(), commonLength).SequenceEqual(prefix.Slice(offset,
                                commonLength)))
                                return false;
                            offset += commonLength;
                        }
                    }
                }
            }

            return offset >= prefix.Length;
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
            if (stackItem._posInNode == -1)
            {
                var (size, ptr) = NodeUtils12.GetValueSizeAndPtr(stackItem._node);
                unsafe
                {
                    return new Span<byte>(ptr.ToPointer(), (int)size);
                }
            }

            var ptr2 = NodeUtils12.PtrInNode(stackItem._node, stackItem._posInNode);
            unsafe
            {
                return new Span<byte>(ptr2.ToPointer(), 12);
            }
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
                return FindLast(new ReadOnlySpan<byte>());
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

        public void IterateNodeInfo(Action<ArtNodeInfo> iterator)
        {
            _rootNode._impl.IterateNodeInfo(_rootNode._root, 0, iterator);
        }
    }
}
