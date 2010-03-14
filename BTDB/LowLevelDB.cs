using System;
using System.Diagnostics;
using System.Collections.Concurrent;

namespace BTDB
{
    /* 64 bits for offset (56bit offset + 8bit (Length/max free space)
     * 32 bits for http://en.wikipedia.org/wiki/Fletcher%27s_checksum
     * 65280 max sector size in bytes
     * Allocation granularity 256 bytes (8 bits)
     * 256 pointers to lower pages in 1 page (3072)
     * Allocation pages:
     *    8MB in one page 256*8*4096
     *    2GB in 2nd level
     *  512GB in 3rd level
     *  128TB in 4th level
     *   32PB in 5th level
     * 8192PB in 6th level max is 7 levels
     * 
     * Root: 128(Header)+64*2=160
     *   16 - B+Tree (8 ofs+4 check+4 levels)
     *   16 - Free Space Tree (8 ofs+4 check+4 levels)
     *    8 - Wanted Size
     *    8 - Transaction Number
     *    8 - Transaction Log position
     *    4 - Transaction Log Allocated Size
     *    4 - Checksum
     */

    internal class BTDBState
    {
        internal BTDBSecPtr RootBTree;
        internal uint RootBTreeLevels;
        internal BTDBSecPtr RootAllocPage;
        internal uint RootAllocPageLevels;
        internal long WantedDatabaseLength;
        internal ulong TransactionCounter;
        internal ulong TransactionLogPtr;
        internal uint TransactionAllocSize;
        internal uint Position;
    }

    internal struct BTDBSecPtr
    {
        internal long Ptr;
        internal uint Checksum;
    }

    [Serializable]
    public class BTDBException : Exception
    {
        public BTDBException(string aMessage) : base(aMessage)
        {
        }
    }

    [Serializable]
    public class BTDBTransactionRetryException : Exception
    {
        public BTDBTransactionRetryException(string aMessage)
            : base(aMessage)
        {
        }
    }

    internal class ReadTrLink
    {
        internal PtrLenList SpaceToReuse;
        internal ReadTrLink Prev;
        internal ReadTrLink Next;
        internal ulong TransactionNumber;
        internal int ReadTrRunningCount;
        internal BTDBSecPtr RootBTree;
    }

    internal class Transaction : ILowLevelDBTransaction
    {
        private readonly LowLevelDB _owner;
        // if this is null then this transaction is writing kind
        private ReadTrLink _readLink;
        private BTDBSector _currentKeySector;
        private int _currentKeyIndex;

        internal Transaction(LowLevelDB aOwner,ReadTrLink readLink)
        {
            _owner = aOwner;
            _readLink = readLink;
            _currentKeySector = null;
            _currentKeyIndex = -1;
        }

        public void Dispose()
        {
            if (_readLink!=null)
            {
                _owner.DisposeReadTransaction(_readLink);
            }
            else
            {
                _owner.DisposeWriteTransaction();
            }
        }

        private void UpgradeToWriteTransaction()
        {
            if (_readLink == null) return;
            _owner.UpgradeTransactionToWriteOne(this, _readLink);
            _readLink = null;
        }

        public bool FindPreviousKey()
        {
            if (_currentKeyIndex < 0) throw new BTDBException("Current Key is invalid");
            if (_currentKeyIndex>0)
            {
                _currentKeyIndex--;
                return true;
            }
            throw new NotImplementedException();
        }

        public bool FindNextKey()
        {
            if (_currentKeyIndex < 0) throw new BTDBException("Current Key is invalid");
            throw new NotImplementedException();
        }

        public FindKeyResult FindKey(byte[] keyBuf, int keyOfs, int keyLen, FindKeyStrategy strategy)
        {
            if (keyLen<0) throw new ArgumentOutOfRangeException("keyLen");
            if (strategy == FindKeyStrategy.Create) UpgradeToWriteTransaction();
            BTDBSecPtr rootBTree = _readLink != null ? _readLink.RootBTree : _owner._newState.RootBTree;
            if (rootBTree.Ptr==0)
            {
                switch (strategy)
                {
                    case FindKeyStrategy.Create:
                        var newRootBTreeSector = _owner.NewSector();
                        newRootBTreeSector.Length = 1 + 4 + 8 + keyLen % LowLevelDB.AllocationGranularity +
                                                    (keyLen >= LowLevelDB.AllocationGranularity ? LowLevelDB.PtrDownSize : 0) ;
                        newRootBTreeSector.Data[0] = 1;
                        PackUnpack.PackUInt32(newRootBTreeSector.Data,1,(uint)keyLen);
                        Array.Copy(keyBuf, keyOfs + keyLen & ~(LowLevelDB.AllocationGranularity - 1), newRootBTreeSector.Data,
                                   5, keyLen%LowLevelDB.AllocationGranularity);
                        _owner._newState.RootBTree.Ptr = newRootBTreeSector.Position;
                        _owner._newState.RootBTreeLevels = 1;
                        _owner.PublishSector(newRootBTreeSector);
                        _currentKeySector = newRootBTreeSector;
                        _currentKeyIndex = 0;
                        return FindKeyResult.Created;
                    case FindKeyStrategy.ExactMatch:
                    case FindKeyStrategy.PreferPrevious:
                    case FindKeyStrategy.PreferNext:
                    case FindKeyStrategy.OnlyPrevious:
                    case FindKeyStrategy.OnlyNext:
                        _currentKeySector = null;
                        _currentKeyIndex = -1;
                        return FindKeyResult.NotFound;
                    default:
                        throw new ArgumentOutOfRangeException("strategy");
                }
            }
            throw new NotImplementedException();
        }

        public int GetKeySize()
        {
            if (_currentKeyIndex < 0) return -1;
            int skip = _currentKeyIndex;
            int p = 1;
            var data = _currentKeySector.Data;
            while (true)
            {
                uint kl = PackUnpack.UnpackUInt32(data, p);
                if (skip == 0) return (int)kl;
                p += 4;
                ulong vl = PackUnpack.UnpackUInt64(data, p);
                p += 8;
                p += (int) (kl%LowLevelDB.AllocationGranularity);
                if (kl >= LowLevelDB.AllocationGranularity) p += LowLevelDB.PtrDownSize;
                p += (int) (vl%LowLevelDB.AllocationGranularity);
                if (vl >= LowLevelDB.AllocationGranularity) p += LowLevelDB.PtrDownSize;
                skip--;
            }
        }

        public long GetValueSize()
        {
            if (_currentKeyIndex < 0) return -1;
            int skip = _currentKeyIndex;
            int p = 1;
            var data = _currentKeySector.Data;
            while (true)
            {
                uint kl = PackUnpack.UnpackUInt32(data, p);
                p += 4;
                ulong vl = PackUnpack.UnpackUInt64(data, p);
                if (skip == 0) return (long)vl;
                p += 8;
                p += (int)(kl % LowLevelDB.AllocationGranularity);
                if (kl >= LowLevelDB.AllocationGranularity) p += LowLevelDB.PtrDownSize;
                p += (int)(vl % LowLevelDB.AllocationGranularity);
                if (vl >= LowLevelDB.AllocationGranularity) p += LowLevelDB.PtrDownSize;
                skip--;
            }
        }

        public long CountRange(byte[] key1Buf, int key1Ofs, int key1Len, bool key1Open, byte[] key2Buf, int key2Ofs, int key2Len, bool key2Open)
        {
            throw new NotImplementedException();
        }

        public long CountPrefix(byte[] prefix, int prefixOfs, int prefixLen)
        {
            throw new NotImplementedException();
        }

        public void PeekKey(int ofs, out int len, out byte[] buf, out int bufOfs)
        {
            if (_currentKeyIndex < 0) throw new BTDBException("Current Key is invalid");
            int skip = _currentKeyIndex;
            int p = 1;
            var data = _currentKeySector.Data;
            uint kl;
            while (true)
            {
                kl = PackUnpack.UnpackUInt32(data, p);
                if (skip == 0) break;
                p += 4;
                ulong vl = PackUnpack.UnpackUInt64(data, p);
                p += 8;
                p += (int)(kl % LowLevelDB.AllocationGranularity);
                if (kl >= LowLevelDB.AllocationGranularity) p += LowLevelDB.PtrDownSize;
                p += (int)(vl % LowLevelDB.AllocationGranularity);
                if (vl >= LowLevelDB.AllocationGranularity) p += LowLevelDB.PtrDownSize;
                skip--;
            }
            p += 12;
            if (ofs >= kl)
            {
                len = 0;
                buf = null;
                bufOfs = 0;
                return;
            }
            if (ofs >= kl - kl % LowLevelDB.AllocationGranularity)
            {
                buf = data;
                bufOfs = p + ofs%LowLevelDB.AllocationGranularity;
                len = (int)(kl % LowLevelDB.AllocationGranularity) - bufOfs;
                return;
            }
            throw new NotImplementedException();
        }

        public void ReadKey(int ofs, int len, byte[] buf, int bufOfs)
        {
            int pos = 0;
            while (len > 0)
            {
                byte[] lBuf;
                int lBufOfs;
                int lOutLen;
                PeekKey(ofs, out lOutLen, out lBuf, out lBufOfs);
                if (lOutLen == 0) throw new BTDBException("Trying to read key outside of its boundary");
                Array.Copy(lBuf, lBufOfs, buf, pos, lOutLen);
                pos += lOutLen;
                len -= lOutLen;
            }
        }

        public void PeekValue(long ofs, out int len, out byte[] buf, out int bufOfs)
        {
            throw new NotImplementedException();
        }

        public void ReadValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            int pos = 0;
            while (len > 0)
            {
                byte[] lBuf;
                int lBufOfs;
                int lOutLen;
                PeekValue(ofs, out lOutLen, out lBuf, out lBufOfs);
                if (lOutLen == 0) throw new BTDBException("Trying to read value outside of its boundary");
                Array.Copy(lBuf, lBufOfs, buf, pos, lOutLen);
                pos += lOutLen;
                len -= lOutLen;
            }
        }

        public void WriteValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            UpgradeToWriteTransaction();
            throw new NotImplementedException();
        }

        public void SetValueSize(long newSize)
        {
            UpgradeToWriteTransaction();
            throw new NotImplementedException();
        }

        public void EraseCurrent()
        {
            UpgradeToWriteTransaction();
            throw new NotImplementedException();
        }

        public void EraseRange(byte[] key1Buf, int key1Ofs, int key1Len, bool key1Open, byte[] key2Buf, int key2Ofs, int key2Len, bool key2Open)
        {
            UpgradeToWriteTransaction();
            throw new NotImplementedException();
        }

        public void ErasePrefix(byte[] prefix, int prefixOfs, int prefixLen)
        {
            UpgradeToWriteTransaction();
            throw new NotImplementedException();
        }

        public void Commit()
        {
            if (_readLink != null) return; // It is read only transaction nothing to commit
            _owner.CommitWriteTransaction();
        }
    }

    internal class BTDBSector
    {
        private long _position;
        private bool _dirty;
        private byte[] _data;

        internal bool Dirty
        {
            get { return _dirty; }
        }

        internal void MakeDirty()
        {
            _dirty = true;
        }

        internal void MakeClean()
        {
            _dirty = false;
        }

        internal bool Allocated
        {
            get { return _position>0; }
        }

        internal long Position
        {
            get { return _position; }
            set { _position = value; }
        }

        internal byte[] Data
        {
            get { return _data; }
        }

        internal int Length
        {
            get
            {
                if (_data == null) return 0;
                return _data.Length;
            }
            set
            {
                Debug.Assert(value >= 0 && value <= LowLevelDB.MaxSectorSize);
                Debug.Assert(value % LowLevelDB.AllocationGranularity == 0);
                if (value == 0)
                {
                    _data = null;
                    return;
                }
                if (_data == null)
                {
                    _data = new byte[value];
                    return;
                }
                byte[] oldData = _data;
                _data = new byte[value];
                Array.Copy(oldData, _data, Math.Min(oldData.Length, value));
            }
        }

        internal BTDBSector Parent { get; set; }
        internal BTDBSector NextLink { get; set; }
        internal BTDBSector PrevLink { get; set; }
    }

    public class LowLevelDB : ILowLevelDB
    {
        internal const int FirstRootOffset = 128;
        internal const int RootSize = 64;
        internal const int RootSizeWithoutChecksum = RootSize - 4;
        internal const int SecondRootOffset = FirstRootOffset + RootSize;
        internal const int TotalHeaderSize = SecondRootOffset + RootSize;
        internal const int AllocationGranularity = 256;
        internal const long MaskOfPosition = 0x7FFFFFFFFFFFFF00L;
        internal const int MaxSectorSize = 256 * AllocationGranularity;
        internal const int PtrDownSize = 12;
        internal const int MaxChildren = 256;

        private IStream _stream;
        private bool _disposeStream;

        private readonly ConcurrentDictionary<long, Lazy<BTDBSector>> _sectorCache = new ConcurrentDictionary<long, Lazy<BTDBSector>>();
        private readonly byte[] _headerData = new byte[TotalHeaderSize];
        private BTDBState _currentState = new BTDBState();
        internal BTDBState _newState = new BTDBState();
        private readonly PtrLenList _spaceAllocatedInTransaction = new PtrLenList();
        private readonly PtrLenList _spaceDeallocatedInTransaction = new PtrLenList();
        private readonly PtrLenList _spaceTemporaryNotReusable = new PtrLenList();
        volatile private PtrLenList _spaceSoonReusable;
        private readonly object _spaceSoonReusableLock = new object();
        private ReadTrLink _readTrLinkTail;
        private ReadTrLink _readTrLinkHead;
        private readonly object _readLinkLock = new object();
        private Transaction _writeTr;
        private bool _commitNeeded;
        private bool _currentTrCommited;
        private long _unallocatedCounter;
        private BTDBSector _unallocatedSectorHeadLink;
        private BTDBSector _unallocatedSectorTailLink;
        private BTDBSector _dirtySectorHeadLink;
        private BTDBSector _dirtySectorTailLink;

        internal BTDBSector TryGetSector(long position)
        {
            Lazy<BTDBSector> res;
            if (_sectorCache.TryGetValue(position, out res))
            {
                return res.Value;
            }
            return null;
        }

        internal BTDBSector ReadSector(long positionWithSize, uint checksum)
        {
            return ReadSector(positionWithSize & MaskOfPosition, (int) (positionWithSize & 0xFF)+1, checksum);
        }

        internal BTDBSector ReadSector(long position, int size, uint checksum)
        {
            Debug.Assert(position > 0);
            Debug.Assert(size > 0);
            Debug.Assert(size <= MaxSectorSize/AllocationGranularity);
            size = size * AllocationGranularity;
            var lazy = new Lazy<BTDBSector>(() =>
            {
                var res = new BTDBSector { Position = position, Length = size };
                if (_stream.Read(res.Data, 0, size, (ulong)position) != size)
                {
                    throw new BTDBException("Data reading error");
                }
                if (Checksum.CalcFletcher(res.Data, 0, (uint)size) != checksum)
                {
                    throw new BTDBException("Checksum error");
                }
                return res;
            });
            lazy = _sectorCache.GetOrAdd(position, lazy);
            return lazy.Value;
        }

        private void InitEmptyDB()
        {
            Array.Clear(_headerData, 0, TotalHeaderSize);
            _headerData[0] = (byte) 'B';
            _headerData[1] = (byte) 'T';
            _headerData[2] = (byte) 'D';
            _headerData[3] = (byte) 'B';
            _headerData[4] = (byte) '1';
            _headerData[5] = (byte) '0';
            _headerData[6] = (byte) '0';
            _headerData[7] = (byte) '0';
            _currentState = new BTDBState();
            _newState = new BTDBState();
            _newState.WantedDatabaseLength = TotalHeaderSize;
            _newState.TransactionCounter = 1;
            _currentState.Position = FirstRootOffset;
            _newState.Position = SecondRootOffset;
            StoreStateToHeaderBuffer(_newState);
            TransferNewStateToCurrentState();
            StoreStateToHeaderBuffer(_newState);
            _stream.Write(_headerData, 0, TotalHeaderSize, 0);
            _stream.Flush();
        }

        public bool Open(IStream stream, bool dispose)
        {
            _stream = stream;
            _disposeStream = dispose;
            _spaceSoonReusable = null;
            bool newDB = false;
            if (stream.GetSize() == 0)
            {
                InitEmptyDB();
                newDB = true;
            }
            else
            {
                if (_stream.Read(_headerData, 0, TotalHeaderSize, 0) != TotalHeaderSize)
                {
                    throw new BTDBException("Too short header");
                }
            }
            if (_headerData[0] != (byte) 'B' || _headerData[1] != (byte) 'T' || _headerData[2] != (byte) 'D'
                || _headerData[3] != (byte) 'B' || _headerData[4] != (byte) '1' || _headerData[5] != (byte) '0'
                || _headerData[6] != (byte) '0' || _headerData[7] != (byte) '0')
            {
                throw new BTDBException("Wrong header");
            }
            _newState.Position = FirstRootOffset;
            _currentState.Position = SecondRootOffset;
            if (RetrieveStateFromHeaderBuffer(_newState))
            {
                if (RetrieveStateFromHeaderBuffer(_currentState))
                {
                    if (_currentState.TransactionCounter > _newState.TransactionCounter)
                    {
                        SwapCurrentAndNewState();
                    }
                }
            }
            else
            {
                SwapCurrentAndNewState();
                if (RetrieveStateFromHeaderBuffer(_newState) == false)
                {
                    throw new BTDBException("Both root headers corrupted");
                }
            }
            TransferNewStateToCurrentState();
            if (_currentState.TransactionAllocSize>0)
            {
                // TODO restore TransactionLog
                throw new BTDBException("TransactionLog is not supported");
            }
            return newDB;
        }

        private void SwapCurrentAndNewState()
        {
            var temp = _currentState;
            _currentState = _newState;
            _newState = temp;
        }

        private void TransferNewStateToCurrentState()
        {
            _currentState.RootBTree = _newState.RootBTree;
            _currentState.RootBTreeLevels = _newState.RootBTreeLevels;
            _currentState.RootAllocPage = _newState.RootAllocPage;
            _currentState.RootAllocPageLevels = _newState.RootAllocPageLevels;
            _currentState.TransactionCounter = _newState.TransactionCounter;
            _currentState.TransactionLogPtr = _newState.TransactionLogPtr;
            _currentState.TransactionAllocSize = _newState.TransactionAllocSize;
            _currentState.WantedDatabaseLength = _newState.WantedDatabaseLength;
            SwapCurrentAndNewState();
        }

        internal void StoreStateToHeaderBuffer(BTDBState state)
        {
            Debug.Assert(state.RootBTree.Ptr >= 0);
            Debug.Assert(state.RootAllocPage.Ptr >= 0);
            Debug.Assert(state.WantedDatabaseLength >= 0);
            int o = (int) state.Position;
            PackUnpack.PackUInt64(_headerData, o, (ulong)state.RootBTree.Ptr);
            o += 8;
            PackUnpack.PackUInt32(_headerData, o, state.RootBTree.Checksum);
            o += 4;
            PackUnpack.PackUInt32(_headerData, o, state.RootBTreeLevels);
            o += 4;
            PackUnpack.PackUInt64(_headerData, o, (ulong)state.RootAllocPage.Ptr);
            o += 8;
            PackUnpack.PackUInt32(_headerData, o, state.RootAllocPage.Checksum);
            o += 4;
            PackUnpack.PackUInt32(_headerData, o, state.RootAllocPageLevels);
            o += 4;
            PackUnpack.PackInt64(_headerData, o, state.WantedDatabaseLength);
            o += 8;
            PackUnpack.PackUInt64(_headerData, o, state.TransactionCounter);
            o += 8;
            PackUnpack.PackUInt64(_headerData, o, state.TransactionLogPtr);
            o += 8;
            PackUnpack.PackUInt32(_headerData, o, state.TransactionAllocSize);
            o += 4;
            PackUnpack.PackUInt32(_headerData, o, Checksum.CalcFletcher(_headerData, state.Position, RootSizeWithoutChecksum));
        }

        internal bool RetrieveStateFromHeaderBuffer(BTDBState state)
        {
            int o = (int) state.Position;
            if (Checksum.CalcFletcher(_headerData, state.Position, RootSizeWithoutChecksum) !=
                PackUnpack.UnpackUInt32(_headerData, o + RootSizeWithoutChecksum))
            {
                return false;
            }
            state.RootBTree.Ptr = (long)PackUnpack.UnpackUInt64(_headerData, o);
            if (state.RootBTree.Ptr < 0) return false;
            o += 8;
            state.RootBTree.Checksum = PackUnpack.UnpackUInt32(_headerData, o);
            o += 4;
            state.RootBTreeLevels = PackUnpack.UnpackUInt32(_headerData, o);
            o += 4;
            state.RootAllocPage.Ptr = (long)PackUnpack.UnpackUInt64(_headerData, o);
            if (state.RootAllocPage.Ptr < 0) return false;
            o += 8;
            state.RootAllocPage.Checksum = PackUnpack.UnpackUInt32(_headerData, o);
            o += 4;
            state.RootAllocPageLevels = PackUnpack.UnpackUInt32(_headerData, o);
            o += 4;
            state.WantedDatabaseLength = PackUnpack.UnpackInt64(_headerData, o);
            if (state.WantedDatabaseLength < AllocationGranularity) return false;
            o += 8;
            state.TransactionCounter = PackUnpack.UnpackUInt64(_headerData, o);
            o += 8;
            state.TransactionLogPtr = PackUnpack.UnpackUInt64(_headerData, o);
            o += 8;
            state.TransactionAllocSize = PackUnpack.UnpackUInt32(_headerData, o);
            return true;
        }

        /*
        internal void commit(bool aInDBClose)
        {
            Debug.Assert(mWriteTrRunning);
            if (mCommitNeeded == false)
                return;
            mMadeTransactionAfterOpen = true;
            if ((_newState._RootObj._Ptr & 1) != 0)
            {
                allocSectorsRecursively(ref _newState._RootObj._Ptr);
            }
            while (moreSpaceToReuse())
            {
            }
            if (!mSpaceToReuse.Empty)
            {
                mInCommitDeallocing = true;
                PtrLenList originalReuse = mSpaceToReuse.clone();
                foreach (var space in originalReuse)
                {
                    // mSpaceToReuse could be made only smaller in this method
                    dealloc(space.Key, space.Value);
                }
                if (aInDBClose)
                {
                    var alsoRemove = new PtrLenList();
                    while (!mSpaceToRemove.Empty)
                    {
                        PtrLenList prepareRemove = mSpaceToRemove.cloneAndClear();
                        alsoRemove.mergeInPlace(prepareRemove);
                        foreach (var space in prepareRemove)
                        {
                            // mSpaceToReuse could be made only smaller in this method
                            dealloc(space.Key, space.Value);
                        }
                    }
                    foreach (var space in alsoRemove)
                    {
                        dealloc2(space.Key, space.Value);
                    }
                }
                foreach (var space in mSpaceToReuse)
                {
                    dealloc2(space.Key, space.Value);
                }
                mSpaceToReuse.clear();
                mInCommitDeallocing = false;
            }
            flushDirty();
            foreach(BTDBSector sec in _sectorCache.Values)
            {
                Debug.Assert(sec.mReferenceCount == 0);
            }
            if (!mSpaceToRemove.Empty)
            {
                mReadTrLinkHead.mSpaceToRemove = mSpaceToRemove.cloneAndClear();
            }
            StoreStateToHeaderBuffer(_newState);
            _stream.flush();
            _stream.write(_headerData, (int) _newState._Position, RootSize, _newState._Position);
            TransferNewStateToCurrentState();
            mCommitNeeded = false;
        }

        void flushDirty()
        {
            var Dirty = new BTDBSector[1];
            while (mDirtySectors.Count > 0)
            {
                mDirtySectors.CopyTo(Dirty, 0, 1);
                BTDBSector d = TryGetSector(Dirty[0].position);
                try
                {
                    _stream.write(d.Data, 0, d.Length, d.position);
                    uint checksum = CheckSum.calcFletcher(d.Data, 0, (uint) d.Length);
                    BTDBSector p = d.parentInSuperTransaction;
                    if (p != null)
                    {
                        p.setChecksum(d.indexInParent, checksum);
                    }
                    else
                    {
                        if (d.indexInParent == cParentIndexOfAllocRoot)
                        {
                            _newState._RootAllocPage._Checksum = checksum;
                        }
                        else
                        {
                            _newState._RootObj._Checksum = checksum;
                        }
                    }
                    d.MakeClean();
                    d.parentInSuperTransaction = null;
                    d.indexInParent = -1;
                    if (p != null) derefSector(p);
                }
                finally
                {
                    derefSector(d);
                }
            }
        }

        void allocSectorsRecursively(ref ulong aSecPtr)
        {
            BTDBSector sec = TryGetSector(aSecPtr);
            try
            {
                Debug.Assert(sec != null);
                int len = sec.Length/PtrDownSize;
                for (int i = 0; i < len; i++)
                {
                    uint flag = sec.getFlags(i);
                    if (BTDBPackUnpack.unpackLevel(flag) <= 0) continue;
                    ulong secptr = sec.getPtr(i);
                    if ((secptr & 1) == 0) continue;
                    allocSectorsRecursively(ref secptr);
                    sec.setPtr(i, secptr);
                }
                aSecPtr = alloc(sec.Length/AllocationGranularity);
                setSectorPos(sec, aSecPtr);
            }
            finally
            {
                derefSector(sec);
            }
        }

        }*/

        internal void DisposeReadTransaction(ReadTrLink link)
        {
            DereferenceReadLink(link);
        }

        public ILowLevelDBTransaction StartTransaction()
        {
            ReadTrLink link;
            lock(_readLinkLock)
            {
                if (_readTrLinkHead == null || _readTrLinkHead.TransactionNumber != _currentState.TransactionCounter)
                {
                    link = new ReadTrLink
                                      {
                                          Prev = _readTrLinkHead,
                                          TransactionNumber = _currentState.TransactionCounter,
                                          ReadTrRunningCount = 1,
                                          RootBTree = _currentState.RootBTree
                                      };
                    if (_readTrLinkHead != null)
                    {
                        _readTrLinkHead.Next = link;
                    }
                    else
                    {
                        _readTrLinkTail = link;
                    }
                    _readTrLinkHead = link;
                }
                else
                {
                    link=_readTrLinkHead;
                    link.ReadTrRunningCount++;
                }
            }
            try
            {
                return new Transaction(this, link);
            }
            catch (Exception)
            {
                DereferenceReadLink(link);
                throw;
            }
        }

        private void DereferenceReadLink(ReadTrLink link)
        {
            lock (_readLinkLock)
            {
                link.ReadTrRunningCount--;
                if (link != _readTrLinkTail) return;
                while (true)
                {
                    if (link.ReadTrRunningCount > 0) return;
                    if (link.SpaceToReuse!=null)
                    {
                        if (_spaceSoonReusable == null) _spaceSoonReusable = link.SpaceToReuse;
                        else
                        {
                            lock (_spaceSoonReusableLock)
                            {
                                if (_spaceSoonReusable == null) _spaceSoonReusable = link.SpaceToReuse;
                                else
                                {
                                    _spaceSoonReusable.MergeInPlace(link.SpaceToReuse);
                                }
                            }
                        }
                    }
                    _readTrLinkTail = link.Next;
                    _readTrLinkTail.Prev = null;
                    if (_readTrLinkHead == link)
                    {
                        _readTrLinkHead = null;
                        return;
                    }
                    link = _readTrLinkTail;
                }
            }
        }

        public void Dispose()
        {
            Debug.Assert(_writeTr==null);
            if (_disposeStream)
            {
                var disposable = _stream as IDisposable;
                if (disposable!=null) disposable.Dispose();
            }
            _stream = null;
        }

        internal void CommitWriteTransaction()
        {
            Debug.Assert(_writeTr!=null);
            if (_currentTrCommited) throw new BTDBException("Only dispose is allowed after commit");
            if (_commitNeeded == false) return;
            while (_unallocatedSectorHeadLink!=null)
            {
                RealSectorAllocate(_unallocatedSectorHeadLink);
            }
            while (_dirtySectorHeadLink!=null)
            {
                FlushDirtySector(_dirtySectorHeadLink);
            }
            _readTrLinkHead.SpaceToReuse = _spaceDeallocatedInTransaction.CloneAndClear();
            StoreStateToHeaderBuffer(_newState);
            _stream.Flush();
            _stream.Write(_headerData, (int)_newState.Position, RootSize, _newState.Position);
            TransferNewStateToCurrentState();
            _commitNeeded = false;
            _currentTrCommited = true;
        }

        private void FlushDirtySector(BTDBSector dirtySector)
        {
            _stream.Write(dirtySector.Data, 0, dirtySector.Length, (ulong)dirtySector.Position);
            var checksum = Checksum.CalcFletcher(dirtySector.Data, 0, (uint)dirtySector.Length);
            dirtySector.MakeClean();
            UnlinkFromDirtySectors(dirtySector);
        }

        private void RealSectorAllocate(BTDBSector unallocatedSector)
        {
            if (_newState.RootAllocPageLevels==0)
            {
                unallocatedSector.Position = _newState.WantedDatabaseLength;
                _newState.WantedDatabaseLength += RoundToAllocationGranularity(unallocatedSector.Length);
                UnlinkFromUnallocatedSectors(unallocatedSector);
                LinkToTailOfDirtySectors(unallocatedSector);
            }
        }

        private void UnlinkFromUnallocatedSectors(BTDBSector unallocatedSector)
        {
            if (unallocatedSector.PrevLink==null)
            {
                _unallocatedSectorHeadLink = unallocatedSector.NextLink;
                if (unallocatedSector.NextLink!=null)
                {
                    unallocatedSector.NextLink.PrevLink = null;
                }
                else
                {
                    _unallocatedSectorTailLink = null;
                }
            }
            else if (unallocatedSector.NextLink==null)
            {
                _unallocatedSectorTailLink = unallocatedSector.PrevLink;
                unallocatedSector.PrevLink.NextLink = null;
            }
            else
            {
                unallocatedSector.PrevLink.NextLink = unallocatedSector.NextLink;
                unallocatedSector.NextLink.PrevLink = unallocatedSector.PrevLink;
            }
        }

        private void UnlinkFromDirtySectors(BTDBSector dirtySector)
        {
            if (dirtySector.PrevLink == null)
            {
                _dirtySectorHeadLink = dirtySector.NextLink;
                if (dirtySector.NextLink != null)
                {
                    dirtySector.NextLink.PrevLink = null;
                }
                else
                {
                    _dirtySectorTailLink = null;
                }
            }
            else if (dirtySector.NextLink == null)
            {
                _dirtySectorTailLink = dirtySector.PrevLink;
                dirtySector.PrevLink.NextLink = null;
            }
            else
            {
                dirtySector.PrevLink.NextLink = dirtySector.NextLink;
                dirtySector.NextLink.PrevLink = dirtySector.PrevLink;
            }
        }

        internal void DisposeWriteTransaction()
        {
            try
            {
                if (_commitNeeded)
                {
                    // rollback
                    SwapCurrentAndNewState();
                    TransferNewStateToCurrentState();
                    _commitNeeded = false;
                }
            }
            finally
            {
                _writeTr = null;
            }
            DereferenceReadLink(_readTrLinkHead);
        }

        internal void UpgradeTransactionToWriteOne(Transaction transaction,ReadTrLink link)
        {
            lock (_readLinkLock)
            {
                if (_writeTr!=null) throw new BTDBTransactionRetryException("Write transaction already running");
                if (link != _readTrLinkHead)
                    throw new BTDBTransactionRetryException("Newer write transaction already finished");
                _writeTr = transaction;
                _currentTrCommited = false;
                _commitNeeded = false;
                _newState.TransactionCounter++;
                _unallocatedCounter = 0;
                Debug.Assert(_unallocatedSectorHeadLink != null);
                Debug.Assert(_unallocatedSectorTailLink != null);
                Debug.Assert(_dirtySectorHeadLink != null);
                Debug.Assert(_dirtySectorTailLink != null);
            }
        }

        internal BTDBSector NewSector()
        {
            var result = new BTDBSector();
            result.MakeDirty();
            _unallocatedCounter--;
            result.Position = _unallocatedCounter;
            return result;
        }

        internal void PublishSector(BTDBSector newSector)
        {
            Debug.Assert(!_sectorCache.ContainsKey(newSector.Position));
            _sectorCache.TryAdd(newSector.Position, new Lazy<BTDBSector>(() => newSector));
            _commitNeeded = true;
            LinkToTailOfUnallocatedSectors(newSector);
        }

        private void LinkToTailOfUnallocatedSectors(BTDBSector newSector)
        {
            newSector.PrevLink = _unallocatedSectorTailLink;
            if (_unallocatedSectorTailLink!=null)
            {
                _unallocatedSectorTailLink.NextLink = newSector;
            }
            else
            {
                _unallocatedSectorHeadLink = newSector;
            }
            _unallocatedSectorTailLink = newSector;
        }

        private void LinkToTailOfDirtySectors(BTDBSector dirtizeSector)
        {
            dirtizeSector.NextLink = null;
            dirtizeSector.PrevLink = _dirtySectorTailLink;
            if (_dirtySectorTailLink != null)
            {
                _dirtySectorTailLink.NextLink = dirtizeSector;
            }
            else
            {
                _dirtySectorHeadLink = dirtizeSector;
            }
            _dirtySectorTailLink = dirtizeSector;
        }

        internal static long RoundToAllocationGranularity(long value)
        {
            Debug.Assert(value>0);
            return (value + AllocationGranularity - 1) & ~(AllocationGranularity - 1);
        }
    }
}
