using BTDB.Collections;
using BTDB.KVDBLayer;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BTDB.ARTLib
{
    class CursorV : ICursor
    {
        RootNodeV _rootNode;
        StructList<CursorItem> _stack;

        public CursorV(RootNodeV rootNode)
        {
            _rootNode = rootNode;
            _stack = new StructList<CursorItem>();
        }

        CursorV(CursorV from)
        {
            _rootNode = from._rootNode;
            _stack = new StructList<CursorItem>(from._stack);
        }

        public void SetNewRoot(IRootNode artRoot)
        {
            var newRoot = (RootNodeV)artRoot;
            if (newRoot._root != _rootNode._root)
                throw new ArgumentException("SetNewRoot allows only upgrades to writtable identical root");
            _rootNode = (RootNodeV)artRoot;
        }

        public long CalcDistance(ICursor to)
        {
            if (_rootNode != ((CursorV)to)._rootNode)
                throw new ArgumentException("Cursor must be from same transaction", nameof(to));
            return to.CalcIndex() - CalcIndex();
        }

        public long CalcIndex()
        {
            return _rootNode._impl.CalcIndex(_stack);
        }

        public ICursor Clone()
        {
            return new CursorV(this);
        }

        public void Erase()
        {
            AssertWrittable();
            AssertValid();
            _rootNode._impl.EraseRange(_rootNode, ref _stack, ref _stack);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AssertWrittable()
        {
            if (!_rootNode._writtable)
            {
                ArtUtils.ThrowCursorNotWrittable();
            }
        }

        public long EraseTo(ICursor to)
        {
            AssertWrittable();
            if (_rootNode != ((CursorV)to)._rootNode)
                throw new ArgumentException("Both cursors must be from same transaction", nameof(to));
            if (!to.IsValid())
                throw new ArgumentException("Cursor must be valid", nameof(to));
            if (!IsValid())
                throw new ArgumentException("Cursor must be valid", "this");
            return _rootNode._impl.EraseRange(_rootNode, ref _stack, ref ((CursorV)to)._stack);
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
            return (int)_stack[_stack.Count - 1]._keyOffset;
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
                ArtUtils.ThrowCursorHaveToBeValid();
            }
        }

        public Span<byte> FillByKey(Span<byte> buffer)
        {
            AssertValid();
            var stack = _stack;
            var keyLength = (int)stack[stack.Count - 1]._keyOffset;
            if (buffer.Length < keyLength || keyLength < 0)
                throw new ArgumentOutOfRangeException(nameof(buffer),
                    "Key has " + keyLength + " bytes, but provided buffer has only " + buffer.Length);
            var offset = 0;
            var i = 0u;
            while (offset < keyLength)
            {
                ref var stackItem = ref stack[i++];
                if (offset < stackItem._keyOffset - (stackItem._posInNode == -1 ? 0 : 1))
                {
                    var (keyPrefixSize, keyPrefixPtr) = NodeUtilsV.GetPrefixSizeAndPtr(stackItem._node);
                    unsafe
                    {
                        Unsafe.CopyBlockUnaligned(ref MemoryMarshal.GetReference(buffer.Slice(offset)),
                            ref Unsafe.AsRef<byte>(keyPrefixPtr.ToPointer()), keyPrefixSize);
                    }

                    offset += (int)keyPrefixSize;
                }

                if (stackItem._posInNode != -1)
                {
                    buffer[offset++] = stackItem._byte;
                }
            }

            return buffer.Slice(0, keyLength);
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
            var keyLength = (int)stack[stack.Count - 1]._keyOffset;
            if (prefix.Length > keyLength)
                return false;
            var offset = 0;
            var i = 0;
            while (offset < prefix.Length)
            {
                ref var stackItem = ref stack[(uint)i++];
                if (offset < stackItem._keyOffset - (stackItem._posInNode == -1 ? 0 : 1))
                {
                    var (keyPrefixSize, keyPrefixPtr) = NodeUtilsV.GetPrefixSizeAndPtr(stackItem._node);
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
                }
            }

            return true;
        }

        public int GetValueLength()
        {
            AssertValid();
            var stackItem = _stack[_stack.Count - 1];
            if (stackItem._posInNode == -1)
            {
                var (size, _) = NodeUtilsV.GetValueSizeAndPtr(stackItem._node);
                return (int)size;
            }

            return (int)NodeUtilsV.ReadLenFromPtr(NodeUtilsV.PtrInNode(stackItem._node, stackItem._posInNode));
        }

        public ReadOnlySpan<byte> GetValue()
        {
            AssertValid();
            var stackItem = _stack[_stack.Count - 1];
            if (stackItem._posInNode == -1)
            {
                var (size, ptr) = NodeUtilsV.GetValueSizeAndPtr(stackItem._node);
                unsafe
                {
                    return new Span<byte>(ptr.ToPointer(), (int)size);
                }
            }

            var ptr2 = NodeUtilsV.PtrInNode(stackItem._node, stackItem._posInNode);
            var size2 = NodeUtilsV.ReadLenFromPtr(ptr2);
            unsafe
            {
                return new Span<byte>(NodeUtilsV.SkipLenFromPtr(ptr2).ToPointer(), (int)size2);
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
            AssertWrittable();
            return _rootNode._impl.Upsert(_rootNode, ref _stack, key, content);
        }

        public void WriteValue(ReadOnlySpan<byte> content)
        {
            AssertWrittable();
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
