using System;
using System.Collections.Concurrent;
using System.Diagnostics;

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
     *    8EB in 6th level
     * 
     * Root: 128(Header)+64*2=256
     *   16 - B+Tree (8 ofs+4 check+4 levels)
     *   16 - Free Space Tree (8 ofs+4 check+4 levels)
     *    8 - Wanted Size
     *    8 - Transaction Number
     *    8 - Transaction Log Position
     *    4 - Transaction Log Allocated Size
     *    4 - Checksum
     */

    internal class State
    {
        internal SectorPtr RootBTree;
        internal uint RootBTreeLevels;
        internal SectorPtr RootAllocPage;
        internal uint RootAllocPageLevels;
        internal long WantedDatabaseLength;
        internal ulong TransactionCounter;
        internal ulong TransactionLogPtr;
        internal uint TransactionAllocSize;
        internal uint Position;
    }

    internal struct SectorPtr
    {
        internal long Ptr;
        internal uint Checksum;

        internal static void Pack(byte[] data, int offset, SectorPtr value)
        {
            PackUnpack.PackInt64(data, offset, value.Ptr);
            PackUnpack.PackUInt32(data, offset + 8, value.Checksum);
        }

        internal static SectorPtr Unpack(byte[] data, int offset)
        {
            SectorPtr result;
            result.Ptr = PackUnpack.UnpackInt64(data, offset);
            result.Checksum = PackUnpack.UnpackUInt32(data, offset + 8);
            return result;
        }
    }

    internal class ReadTrLink
    {
        internal PtrLenList SpaceToReuse;
        internal ReadTrLink Prev;
        internal ReadTrLink Next;
        internal ulong TransactionNumber;
        internal int ReadTrRunningCount;
        internal SectorPtr RootBTree;
    }

    internal class Transaction : ILowLevelDBTransaction
    {
        readonly LowLevelDB _owner;

        // if this is null then this transaction is writing kind
        ReadTrLink _readLink;
        Sector _currentKeySector;
        int _currentKeyIndex;

        internal Transaction(LowLevelDB owner, ReadTrLink readLink)
        {
            _owner = owner;
            _readLink = readLink;
            _currentKeySector = null;
            _currentKeyIndex = -1;
        }

        public void Dispose()
        {
            if (_readLink != null)
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
            if (_currentKeyIndex > 0)
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
            if (keyLen < 0) throw new ArgumentOutOfRangeException("keyLen");
            if (strategy == FindKeyStrategy.Create) UpgradeToWriteTransaction();
            SectorPtr rootBTree = _readLink != null ? _readLink.RootBTree : _owner._newState.RootBTree;
            if (rootBTree.Ptr == 0)
            {
                switch (strategy)
                {
                    case FindKeyStrategy.Create:
                        Sector newRootBTreeSector = CreateBTreeChildWith1Key(keyBuf, keyOfs, keyLen);
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
                        return FindKeyNotFound();
                    default:
                        throw new ArgumentOutOfRangeException("strategy");
                }
            }
            Sector sector = _owner.TryGetSector(rootBTree.Ptr);
            if (sector == null)
            {
                sector = _owner.ReadSector(rootBTree.Ptr, rootBTree.Checksum, _readLink == null);
                sector.Type = (sector.Data[0] & 0x80) != 0 ? SectorType.BTreeParent : SectorType.BTreeChild;
            }
            if (sector.Type == SectorType.BTreeChild)
            {
                var iter = new BTreeChildIterator(sector.Data);
                int bindex = iter.BinarySearch(keyBuf, keyOfs, keyLen, SectorDataCompare);
                _currentKeySector = sector;
                _currentKeyIndex = bindex / 2;
                if ((bindex & 1) != 0)
                {
                    return FindKeyResult.FoundExact;
                }
                switch (strategy)
                {
                    case FindKeyStrategy.Create:
                        break;
                    case FindKeyStrategy.ExactMatch:
                        return FindKeyNotFound();
                    case FindKeyStrategy.OnlyNext:
                        if (_currentKeyIndex < iter.Count)
                        {
                            return FindKeyResult.FoundNext;
                        }
                        _currentKeyIndex--;
                        if (FindNextKey())
                        {
                            return FindKeyResult.FoundNext;
                        }
                        return FindKeyNotFound();
                    case FindKeyStrategy.PreferNext:
                        if (_currentKeyIndex < iter.Count)
                        {
                            return FindKeyResult.FoundNext;
                        }
                        _currentKeyIndex--;
                        if (FindNextKey())
                        {
                            return FindKeyResult.FoundNext;
                        }
                        return FindKeyResult.FoundPrevious;
                    case FindKeyStrategy.OnlyPrevious:
                        if (_currentKeyIndex > 0)
                        {
                            _currentKeyIndex--;
                            return FindKeyResult.FoundPrevious;
                        }
                        if (FindPreviousKey())
                        {
                            return FindKeyResult.FoundPrevious;
                        }
                        return FindKeyNotFound();
                    case FindKeyStrategy.PreferPrevious:
                        if (_currentKeyIndex > 0)
                        {
                            _currentKeyIndex--;
                            return FindKeyResult.FoundPrevious;
                        }
                        if (FindPreviousKey())
                        {
                            return FindKeyResult.FoundPrevious;
                        }
                        return FindKeyResult.FoundNext;
                    default:
                        throw new ArgumentOutOfRangeException("strategy");
                }
                int additionalLengthNeeded = BTreeChildIterator.CalcEntrySize(keyLen);
                if (iter.TotalLength+additionalLengthNeeded<=4096 && iter.Count<127)
                {
                    Sector newSector = _owner.ResizeSector(sector, iter.TotalLength + additionalLengthNeeded);
                    newSector.Data[0] = (byte)(iter.Count + 1);
                    int insertOfs=iter.OffsetOfIndex(_currentKeyIndex);
                    Array.Copy(iter.Data, 1, newSector.Data, 1, insertOfs - 1);
                    SetBTreeChildKeyData(newSector, keyBuf, keyOfs, keyLen, insertOfs);
                    Array.Copy(iter.Data, insertOfs, newSector.Data, insertOfs+additionalLengthNeeded, iter.TotalLength-insertOfs);
                }
                else
                {
                    throw new NotImplementedException();
                }
                return FindKeyResult.Created;
            }
            throw new NotImplementedException();
        }

        FindKeyResult FindKeyNotFound()
        {
            _currentKeySector = null;
            _currentKeyIndex = -1;
            return FindKeyResult.NotFound;
        }

        int SectorDataCompare(byte[] buf, int ofs, int len, SectorPtr sectorPtr, int dataLen)
        {
            Sector sector = _owner.TryGetSector(sectorPtr.Ptr);
            if (sector == null)
            {
                sector = _owner.ReadSector(sectorPtr.Ptr, sectorPtr.Checksum, _readLink == null);
                sector.Type = dataLen > sector.Length ? SectorType.DataParent : SectorType.DataChild;
            }
            if (sector.Type==SectorType.DataChild)
            {
                return BitArrayManipulation.CompareByteArray(buf,
                                                             ofs,
                                                             dataLen > sector.Length && len > sector.Length ? sector.Length : len,
                                                             sector.Data, 
                                                             0, 
                                                             sector.Length);
            }
            throw new NotImplementedException();
        }

        Sector CreateBTreeChildWith1Key(byte[] keyBuf, int keyOfs, int keyLen)
        {
            var newRootBTreeSector = _owner.NewSector();
            newRootBTreeSector.Type = SectorType.BTreeChild;
            newRootBTreeSector.SetLengthWithRound(1 + BTreeChildIterator.CalcEntrySize(keyLen));
            newRootBTreeSector.Data[0] = 1;
            SetBTreeChildKeyData(newRootBTreeSector, keyBuf, keyOfs, keyLen, 1);
            return newRootBTreeSector;
        }

        void SetBTreeChildKeyData(Sector inSector, byte[] keyBuf, int keyOfs, int keyLen, int sectorDataOfs)
        {
            byte[] sectorData = inSector.Data;
            int keyLenInline = BTreeChildIterator.CalcKeyLenInline(keyLen);
            PackUnpack.PackUInt32(sectorData, sectorDataOfs, (uint)keyLen);
            Array.Copy(keyBuf, keyOfs, sectorData, sectorDataOfs + 4 + 8, keyLenInline);
            if (keyLen > BTreeChildIterator.MaxKeyLenInline)
            {
                SectorPtr keySecPtr = CreateContentSector(keyBuf, keyOfs + keyLenInline, keyLen - keyLenInline, inSector);
                SectorPtr.Pack(sectorData, sectorDataOfs + 4 + 8 + keyLenInline, keySecPtr);
            }
        }

        SectorPtr CreateContentSector(byte[] buf, int ofs, int len, Sector parent)
        {
            if (len <= LowLevelDB.MaxSectorSize)
            {
                var newSector = _owner.NewSector();
                newSector.Type = SectorType.DataChild;
                newSector.SetLengthWithRound(len);
                newSector.Parent = parent;
                if (buf != null) Array.Copy(buf, ofs, newSector.Data, 0, len);
                _owner.PublishSector(newSector);
                return newSector.ToPtrWithLen();
            }
            throw new NotImplementedException();
        }

        public int GetKeySize()
        {
            if (_currentKeyIndex < 0) return -1;
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndex);
            return iter.KeyLen;
        }

        public long GetValueSize()
        {
            if (_currentKeyIndex < 0) return -1;
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndex);
            return iter.ValueLen;
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
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndex);
            if (ofs >= iter.KeyLen)
            {
                len = 0;
                buf = null;
                bufOfs = 0;
                return;
            }
            if (ofs < iter.KeyLenInline)
            {
                len = iter.KeyLenInline - ofs;
                buf = _currentKeySector.Data;
                bufOfs = iter.KeyOffset + ofs;
                return;
            }
            throw new NotImplementedException();
        }

        public void ReadKey(int ofs, int len, byte[] buf, int bufOfs)
        {
            while (len > 0)
            {
                byte[] localBuf;
                int localBufOfs;
                int localOutLen;
                PeekKey(ofs, out localOutLen, out localBuf, out localBufOfs);
                if (localOutLen == 0) throw new BTDBException("Trying to read key outside of its boundary");
                Array.Copy(localBuf, localBufOfs, buf, bufOfs, localOutLen);
                ofs += localOutLen;
                bufOfs += localOutLen;
                len -= localOutLen;
            }
        }

        public void PeekValue(long ofs, out int len, out byte[] buf, out int bufOfs)
        {
            throw new NotImplementedException();
        }

        public void ReadValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            while (len > 0)
            {
                byte[] localBuf;
                int localBufOfs;
                int localOutLen;
                PeekValue(ofs, out localOutLen, out localBuf, out localBufOfs);
                if (localOutLen == 0) throw new BTDBException("Trying to read value outside of its boundary");
                Array.Copy(localBuf, localBufOfs, buf, bufOfs, localOutLen);
                ofs += localOutLen;
                bufOfs += localOutLen;
                len -= localOutLen;
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

    internal enum SectorType
    {
        BTreeParent,
        BTreeChild,
        AllocParent,
        AllocChild,
        DataParent,
        DataChild,
    }

    internal class Sector
    {
        internal SectorType Type { get; set; }

        internal long Position { get; set; }

        internal bool InTransaction { get; set; }

        internal bool Dirty { get; set; }

        internal Sector Parent { get; set; }

        internal Sector NextLink { get; set; }

        internal Sector PrevLink { get; set; }

        internal bool Allocated
        {
            get { return Position > 0; }
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

        internal void SetLengthWithRound(int length)
        {
            Length = LowLevelDB.RoundToAllocationGranularity(length);
        }

        internal SectorPtr ToPtrWithLen()
        {
            SectorPtr result;
            result.Ptr = Position;
            result.Checksum = 0;
            if (Position > 0)
            {
                result.Ptr += Length / LowLevelDB.AllocationGranularity - 1;
                if (Dirty == false) result.Checksum = Checksum.CalcFletcher(Data, 0, (uint)Length);
            }
            return result;
        }

        private byte[] _data;
    }

    public class LowLevelDB : ILowLevelDB
    {
        internal const int FirstRootOffset = 128;
        internal const int RootSize = 64;
        internal const int RootSizeWithoutChecksum = RootSize - 4;
        internal const int SecondRootOffset = FirstRootOffset + RootSize;
        internal const int TotalHeaderSize = SecondRootOffset + RootSize;
        internal const int AllocationGranularity = 256;
        internal const long MaskOfPosition = -256; // 0xFFFFFFFFFFFFFF00
        internal const int MaxSectorSize = 256 * AllocationGranularity;
        internal const int PtrDownSize = 12;
        internal const int MaxChildren = 256;

        IStream _stream;
        bool _disposeStream;

        readonly ConcurrentDictionary<long, Lazy<Sector>> _sectorCache = new ConcurrentDictionary<long, Lazy<Sector>>();
        readonly byte[] _headerData = new byte[TotalHeaderSize];
        State _currentState = new State();
        internal State _newState = new State();
        readonly PtrLenList _spaceAllocatedInTransaction = new PtrLenList();
        readonly PtrLenList _spaceDeallocatedInTransaction = new PtrLenList();
        readonly PtrLenList _spaceTemporaryNotReusable = new PtrLenList();
        volatile PtrLenList _spaceSoonReusable;
        readonly object _spaceSoonReusableLock = new object();
        ReadTrLink _readTrLinkTail;
        ReadTrLink _readTrLinkHead;
        readonly object _readLinkLock = new object();
        Transaction _writeTr;
        bool _commitNeeded;
        bool _currentTrCommited;
        long _unallocatedCounter;
        Sector _unallocatedSectorHeadLink;
        Sector _unallocatedSectorTailLink;
        Sector _dirtySectorHeadLink;
        Sector _dirtySectorTailLink;

        internal Sector TryGetSector(long positionWithSize)
        {
            Lazy<Sector> res;
            if (_sectorCache.TryGetValue(positionWithSize & MaskOfPosition, out res))
            {
                return res.Value;
            }
            return null;
        }

        internal Sector ReadSector(long positionWithSize, uint checksum, bool inWriteTransaction)
        {
            Debug.Assert(positionWithSize > 0);
            return ReadSector(positionWithSize & MaskOfPosition, (int)(positionWithSize & 0xFF) + 1, checksum, inWriteTransaction);
        }

        internal Sector ReadSector(long position, int size, uint checksum, bool inWriteTransaction)
        {
            Debug.Assert(position > 0);
            Debug.Assert(size > 0);
            Debug.Assert(size <= MaxSectorSize / AllocationGranularity);
            size = size * AllocationGranularity;
            var lazy = new Lazy<Sector>(() =>
            {
                var res = new Sector { Position = position, Length = size };
                if (inWriteTransaction)
                {
                    res.InTransaction = _spaceAllocatedInTransaction.Contains((ulong)position);
                }
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
            _headerData[0] = (byte)'B';
            _headerData[1] = (byte)'T';
            _headerData[2] = (byte)'D';
            _headerData[3] = (byte)'B';
            _headerData[4] = (byte)'1';
            _headerData[5] = (byte)'0';
            _headerData[6] = (byte)'0';
            _headerData[7] = (byte)'0';
            _currentState = new State();
            _newState = new State();
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
            if (_headerData[0] != (byte)'B' || _headerData[1] != (byte)'T' || _headerData[2] != (byte)'D'
                || _headerData[3] != (byte)'B' || _headerData[4] != (byte)'1' || _headerData[5] != (byte)'0'
                || _headerData[6] != (byte)'0' || _headerData[7] != (byte)'0')
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
            if (_currentState.TransactionAllocSize > 0)
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

        internal void StoreStateToHeaderBuffer(State state)
        {
            Debug.Assert(state.RootBTree.Ptr >= 0);
            Debug.Assert(state.RootAllocPage.Ptr >= 0);
            Debug.Assert(state.WantedDatabaseLength >= 0);
            int o = (int)state.Position;
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

        internal bool RetrieveStateFromHeaderBuffer(State state)
        {
            int o = (int)state.Position;
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

        internal void DisposeReadTransaction(ReadTrLink link)
        {
            DereferenceReadLink(link);
        }

        public ILowLevelDBTransaction StartTransaction()
        {
            ReadTrLink link;
            lock (_readLinkLock)
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
                    link = _readTrLinkHead;
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
                    if (link.SpaceToReuse != null)
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
                    if (_readTrLinkHead == link)
                    {
                        _readTrLinkHead = null;
                        return;
                    }
                    _readTrLinkTail.Prev = null;
                    link = _readTrLinkTail;
                }
            }
        }

        public void Dispose()
        {
            Debug.Assert(_writeTr == null);
            if (_disposeStream)
            {
                var disposable = _stream as IDisposable;
                if (disposable != null) disposable.Dispose();
            }
            _stream = null;
        }

        internal void CommitWriteTransaction()
        {
            Debug.Assert(_writeTr != null);
            if (_currentTrCommited) throw new BTDBException("Only dispose is allowed after commit");
            if (_commitNeeded == false) return;
            while (_unallocatedSectorHeadLink != null)
            {
                RealSectorAllocate(_unallocatedSectorHeadLink);
            }
            while (_dirtySectorHeadLink != null)
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

        private void FlushDirtySector(Sector dirtySector)
        {
            _stream.Write(dirtySector.Data, 0, dirtySector.Length, (ulong)dirtySector.Position);
            var checksum = Checksum.CalcFletcher(dirtySector.Data, 0, (uint)dirtySector.Length);
            long ptr = dirtySector.Position;
            switch (dirtySector.Type)
            {
                case SectorType.BTreeParent:
                case SectorType.BTreeChild:
                case SectorType.DataParent:
                case SectorType.DataChild:
                    ptr += dirtySector.Length / AllocationGranularity - 1;
                    break;
                case SectorType.AllocParent:
                    {
                        int m = 0;
                        for (int i = 0; i < dirtySector.Length / PtrDownSize; i++)
                        {
                            int c = (int)PackUnpack.UnpackUInt64(dirtySector.Data, i * PtrDownSize) &
                                    (AllocationGranularity - 1);
                            if (m < c)
                            {
                                m = c;
                                if (m == 255) break;
                            }
                        }
                        ptr += m;
                        break;
                    }
                case SectorType.AllocChild:
                    ptr += BitArrayManipulation.SizeOfBiggestHoleUpTo255(dirtySector.Data);
                    break;
                default:
                    throw new InvalidOperationException();
            }
            if (dirtySector.Parent == null)
            {
                switch (dirtySector.Type)
                {
                    case SectorType.BTreeParent:
                    case SectorType.BTreeChild:
                        {
                            _newState.RootBTree.Checksum = checksum;
                            _newState.RootBTree.Ptr = ptr;
                            break;
                        }
                    case SectorType.AllocChild:
                    case SectorType.AllocParent:
                        {
                            _newState.RootAllocPage.Checksum = checksum;
                            _newState.RootAllocPage.Ptr = ptr;
                            break;
                        }
                    default:
                        throw new InvalidOperationException();
                }
            }
            else
            {
                int ofs = FindOfsInParent(dirtySector, dirtySector.Parent);
                dirtySector.Parent = DirtizeSector(dirtySector.Parent);
                PackUnpack.PackInt64(dirtySector.Parent.Data, ofs, ptr);
                PackUnpack.PackUInt32(dirtySector.Parent.Data, ofs + 8, checksum);
            }
            dirtySector.Dirty = false;
            UnlinkFromDirtySectors(dirtySector);
        }

        private Sector DirtizeSector(Sector sector)
        {
            if (sector.Dirty) return sector;
            if (sector.InTransaction == false)
            {
                var newParent = sector.Parent;
                if (newParent != null)
                {
                    newParent = DirtizeSector(newParent);
                }
                var clone = NewSector();
                clone.Length = sector.Length;
                clone.Type = sector.Type;
                Array.Copy(sector.Data, clone.Data, clone.Length);
                clone.Parent = newParent;
                PublishSector(clone);
                if (newParent == null)
                {
                    if ((_newState.RootBTree.Ptr & MaskOfPosition) == sector.Position)
                    {
                        _newState.RootBTree.Ptr = clone.Position; // Length encoding is not needed as it is temporary anyway
                    }
                    else
                    {
                        Debug.Assert((_newState.RootAllocPage.Ptr & MaskOfPosition) == sector.Position);
                        _newState.RootAllocPage.Ptr = clone.Position; // Max free space encoding is not needed as it is temporary anyway
                    }
                }
                else
                {
                    int ofs = FindOfsInParent(sector, newParent);
                    PackUnpack.PackInt64(newParent.Data, ofs, clone.Position);
                }
                return clone;
            }
            sector.Dirty = true;
            LinkToTailOfDirtySectors(sector);
            return sector;
        }

        internal Sector ResizeSector(Sector sector, int newLength)
        {
            newLength = RoundToAllocationGranularity(newLength);
            if (sector.Length == newLength) return DirtizeSector(sector);
            if (sector.InTransaction)
            {
                if (!sector.Allocated)
                {
                    sector.Length = newLength;
                    return sector;
                }
                if (sector.Dirty)
                {
                    UnlinkFromDirtySectors(sector);
                }
                Lazy<Sector> forget;
                _sectorCache.TryRemove(sector.Position, out forget);
            }
            var newParent = sector.Parent;
            if (newParent != null)
            {
                newParent = DirtizeSector(newParent);
            }
            var clone = NewSector();
            clone.Length = newLength;
            clone.Type = sector.Type;
            clone.Parent = newParent;
            PublishSector(clone);
            if (newParent == null)
            {
                if ((_newState.RootBTree.Ptr & MaskOfPosition) == sector.Position)
                {
                    _newState.RootBTree.Ptr = clone.Position; // Length encoding is not needed as it is temporary anyway
                }
                else
                {
                    Debug.Assert((_newState.RootAllocPage.Ptr & MaskOfPosition) == sector.Position);
                    _newState.RootAllocPage.Ptr = clone.Position; // Max free space encoding is not needed as it is temporary anyway
                }
            }
            else
            {
                int ofs = FindOfsInParent(sector, newParent);
                PackUnpack.PackInt64(newParent.Data, ofs, clone.Position);
            }
            return clone;
        }

        private int FindOfsInParent(Sector sector, Sector where)
        {
            switch(where.Type)
            {
                case SectorType.BTreeParent:
                    break;
                case SectorType.BTreeChild:
                    var iter = new BTreeChildIterator(where.Data);
                    do
                    {
                        if ((iter.KeySectorPos & LowLevelDB.MaskOfPosition) == sector.Position)
                            return iter.KeySectorPtrOffset;
                        if ((iter.ValueSectorPos & LowLevelDB.MaskOfPosition) == sector.Position)
                            return iter.ValueSectorPtrOffset;
                    } 
                    while (iter.MoveNext());
                    throw new BTDBException("Cannot FindOfsInParrent");
                case SectorType.AllocParent:
                    break;
                case SectorType.AllocChild:
                    break;
                case SectorType.DataParent:
                    break;
                case SectorType.DataChild:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            throw new NotImplementedException();
        }

        private void RealSectorAllocate(Sector unallocatedSector)
        {
            if (_newState.RootAllocPageLevels == 0)
            {
                int ofsInParent = -1;
                if (unallocatedSector.Parent != null)
                {
                    unallocatedSector.Parent = DirtizeSector(unallocatedSector.Parent);
                    ofsInParent = FindOfsInParent(unallocatedSector, unallocatedSector.Parent);
                }
                unallocatedSector.Position = _newState.WantedDatabaseLength;
                _newState.WantedDatabaseLength += unallocatedSector.Length;
                _spaceAllocatedInTransaction.TryInclude((ulong)unallocatedSector.Position, (ulong)unallocatedSector.Length);
                UnlinkFromUnallocatedSectors(unallocatedSector);
                LinkToTailOfDirtySectors(unallocatedSector);
                if (unallocatedSector.Parent != null)
                {
                    PackUnpack.PackUInt64(unallocatedSector.Parent.Data, ofsInParent, (ulong)unallocatedSector.Position);
                }
                else
                {
                    switch (unallocatedSector.Type)
                    {
                        case SectorType.BTreeParent:
                        case SectorType.BTreeChild:
                            {
                                _newState.RootBTree.Ptr = unallocatedSector.Position;
                                break;
                            }
                        case SectorType.AllocChild:
                        case SectorType.AllocParent:
                            {
                                _newState.RootAllocPage.Ptr = unallocatedSector.Position;
                                break;
                            }
                        default:
                            throw new InvalidOperationException();
                    }
                }
                return;
            }
            throw new NotImplementedException();
        }

        private void UnlinkFromUnallocatedSectors(Sector unallocatedSector)
        {
            if (unallocatedSector.PrevLink == null)
            {
                _unallocatedSectorHeadLink = unallocatedSector.NextLink;
                if (unallocatedSector.NextLink != null)
                {
                    unallocatedSector.NextLink.PrevLink = null;
                }
                else
                {
                    _unallocatedSectorTailLink = null;
                }
            }
            else if (unallocatedSector.NextLink == null)
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

        private void UnlinkFromDirtySectors(Sector dirtySector)
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

        internal void UpgradeTransactionToWriteOne(Transaction transaction, ReadTrLink link)
        {
            lock (_readLinkLock)
            {
                if (_writeTr != null) throw new BTDBTransactionRetryException("Write transaction already running");
                if (link != _readTrLinkHead)
                    throw new BTDBTransactionRetryException("Newer write transaction already finished");
                _writeTr = transaction;
                _currentTrCommited = false;
                _commitNeeded = false;
                _newState.TransactionCounter++;
                _unallocatedCounter = 0;
                Debug.Assert(_unallocatedSectorHeadLink == null);
                Debug.Assert(_unallocatedSectorTailLink == null);
                Debug.Assert(_dirtySectorHeadLink == null);
                Debug.Assert(_dirtySectorTailLink == null);
            }
        }

        internal Sector NewSector()
        {
            var result = new Sector { Dirty = true, InTransaction = true };
            _unallocatedCounter--;
            result.Position = _unallocatedCounter * AllocationGranularity;
            return result;
        }

        internal void PublishSector(Sector newSector)
        {
            Debug.Assert(!_sectorCache.ContainsKey(newSector.Position));
            _sectorCache.TryAdd(newSector.Position, new Lazy<Sector>(() => newSector));
            _commitNeeded = true;
            LinkToTailOfUnallocatedSectors(newSector);
        }

        private void LinkToTailOfUnallocatedSectors(Sector newSector)
        {
            newSector.PrevLink = _unallocatedSectorTailLink;
            if (_unallocatedSectorTailLink != null)
            {
                _unallocatedSectorTailLink.NextLink = newSector;
            }
            else
            {
                _unallocatedSectorHeadLink = newSector;
            }
            _unallocatedSectorTailLink = newSector;
        }

        private void LinkToTailOfDirtySectors(Sector dirtizeSector)
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

        internal static int RoundToAllocationGranularity(int value)
        {
            Debug.Assert(value > 0);
            return (value + AllocationGranularity - 1) & ~(AllocationGranularity - 1);
        }

        internal static long RoundToAllocationGranularity(long value)
        {
            Debug.Assert(value > 0);
            return (value + AllocationGranularity - 1) & ~(AllocationGranularity - 1);
        }
    }
}
