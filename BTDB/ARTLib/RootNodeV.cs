using System;
using System.Threading;

namespace BTDB.ARTLib
{
    class RootNodeV : IRootNode
    {
        internal RootNodeV(ARTImplV impl)
        {
            _impl = impl;
            _root = IntPtr.Zero;
            _writable = true;
            _referenceCount = 1;
        }

        int _referenceCount;
        internal IntPtr _root;
        internal ARTImplV _impl;
        internal bool _writable;

        public ulong CommitUlong { get; set; }
        public long TransactionId { get; set; }
        public uint TrLogFileId { get; set; }
        public uint TrLogOffset { get; set; }
        public string DescriptionForLeaks { get; set; }

        public void Dispose()
        {
            _impl.Dereference(_root);
        }

        public IRootNode Snapshot()
        {
            var snapshot = new RootNodeV(_impl);
            snapshot._writable = false;
            snapshot._root = _root;
            snapshot.CommitUlong = CommitUlong;
            snapshot.TransactionId = TransactionId;
            snapshot.TrLogFileId = TrLogFileId;
            snapshot.TrLogOffset = TrLogOffset;
            snapshot._ulongs = _ulongs == null ? null : (ulong[])_ulongs.Clone();
            if (_writable)
                TransactionId++;
            NodeUtilsV.Reference(_root);
            return snapshot;
        }

        public IRootNode CreateWritableTransaction()
        {
            if (_writable) throw new InvalidOperationException("Only readonly root node could be CreateWritableTransaction");
            var node = new RootNodeV(_impl);
            node._writable = true;
            node._root = _root;
            node.CommitUlong = CommitUlong;
            node.TransactionId = TransactionId + 1;
            node.TrLogFileId = TrLogFileId;
            node.TrLogOffset = TrLogOffset;
            node._ulongs = _ulongs == null ? null : (ulong[])_ulongs.Clone();
            NodeUtilsV.Reference(_root);
            return node;
        }

        public void Commit()
        {
            _writable = false;
        }

        public ICursor CreateCursor()
        {
            return new CursorV(this);
        }

        public long GetCount()
        {
            if (_root == IntPtr.Zero) return 0;
            ref var header = ref NodeUtilsV.Ptr2NodeHeader(_root);
            return (long)header._recursiveChildCount;
        }

        public void RevertTo(IRootNode snapshot)
        {
            if (!_writable)
                throw new InvalidOperationException("Only writable root node could be reverted");
            var oldRoot = _root;
            _root = ((RootNodeV)snapshot)._root;
            _ulongs = ((RootNodeV)snapshot)._ulongs == null ? null : (ulong[])((RootNodeV)snapshot)._ulongs.Clone();
            CommitUlong = ((RootNodeV)snapshot).CommitUlong;
            TransactionId = ((RootNodeV)snapshot).TransactionId;
            TrLogFileId = ((RootNodeV)snapshot).TrLogFileId;
            TrLogOffset = ((RootNodeV)snapshot).TrLogOffset;
            if (oldRoot != _root)
            {
                NodeUtilsV.Reference(_root);
                _impl.Dereference(oldRoot);
            }
        }

        ulong[] _ulongs;

        public ulong GetUlong(uint idx)
        {
            if (_ulongs == null) return 0;
            if (idx >= _ulongs.Length) return 0;
            return _ulongs[idx];
        }

        public void SetUlong(uint idx, ulong value)
        {
            if (_ulongs == null || idx >= _ulongs.Length)
                Array.Resize(ref _ulongs, (int)(idx + 1));
            _ulongs[idx] = value;
        }

        public uint GetUlongCount()
        {
            return _ulongs == null ? 0U : (uint)_ulongs.Length;
        }

        public ulong[] UlongsArray => _ulongs;

        public bool Reference()
        {
            while (true)
            {
                var original = Thread.VolatileRead(ref _referenceCount);
                if (original == 0)
                    return true;
                if (Interlocked.CompareExchange(ref _referenceCount, original + 1, original) == original)
                {
                    return false;
                }
            }
        }

        public bool Dereference()
        {
            return Interlocked.Decrement(ref _referenceCount) == 0;
        }

        public bool ShouldBeDisposed => _referenceCount == 0;
    }
}
