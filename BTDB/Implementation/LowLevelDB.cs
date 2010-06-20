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
     * Root: 96(Header)+80*2=256
     *   16 - B+Tree (8 ofs+4 check+4 levels)
     *   16 - Free Space Tree (8 ofs+4 check+4 levels)
     *    8 - Count of key/value pairs stored
     *    8 - Used Size - including first 256 bytes of header and includes paddings to allocation granularity
     *    8 - Transaction Number
     *    8 - Wanted Size - size of database stream could be trimmed to this size
     *    8 - Transaction Log Position
     *    4 - Transaction Log Allocated Size
     *    4 - Checksum
     */

    public class LowLevelDB : ILowLevelDB
    {
        internal class State
        {
            internal SectorPtr RootBTree;
            internal uint RootBTreeLevels;
            internal SectorPtr RootAllocPage;
            internal uint RootAllocPageLevels;
            internal ulong KeyValuePairCount;
            internal ulong UsedSize;
            internal ulong TransactionCounter;
            internal ulong WantedDatabaseLength;
            internal ulong TransactionLogPtr;
            internal uint TransactionAllocSize;
            internal uint Position;
        }

        internal class ReadTrLink
        {
            internal PtrLenList SpaceToReuse;
            internal ReadTrLink Next;
            internal ulong TransactionNumber;
            internal int ReadTrRunningCount;
            internal SectorPtr RootBTree;
            internal ulong KeyValuePairCount;
            internal ulong UsedSize;
            internal ulong WantedDatabaseLength;
        }

        const int FirstRootOffset = 96;
        const int RootSize = 80;
        const int RootSizeWithoutChecksum = RootSize - 4;
        const int SecondRootOffset = FirstRootOffset + RootSize;
        const int TotalHeaderSize = SecondRootOffset + RootSize;
        internal const int AllocationGranularity = 256;
        const long MaskOfPosition = -256; // 0xFFFFFFFFFFFFFF00
        internal const int MaxSectorSize = 256 * AllocationGranularity;
        internal const int MaxLeafDataSectorSize = 4096;
        const int MaxLeafAllocSectorGrans = 4096 * 8;
        internal const int PtrDownSize = 12;
        internal const int MaxChildren = 256;

        IStream _stream;
        bool _disposeStream;

        readonly ConcurrentDictionary<long, Lazy<Sector>> _sectorCache = new ConcurrentDictionary<long, Lazy<Sector>>();
        readonly byte[] _headerData = new byte[TotalHeaderSize];
        State _currentState = new State();
        State _newState = new State();
        readonly PtrLenList _spaceAllocatedInTransaction = new PtrLenList();
        readonly PtrLenList _spaceDeallocatedInTransaction = new PtrLenList();
        readonly PtrLenList _spaceUsedByReadOnlyTransactions = new PtrLenList();
        volatile PtrLenList _spaceSoonReusable;
        readonly object _spaceSoonReusableLock = new object();
        ReadTrLink _readTrLinkTail;
        ReadTrLink _readTrLinkHead;
        readonly object _readLinkLock = new object();
        LowLevelDBTransaction _writeTr;
        bool _commitNeeded;
        bool _currentTrCommited;
        long _unallocatedCounter;
        Sector _unallocatedSectorHeadLink;
        Sector _unallocatedSectorTailLink;
        Sector _dirtySectorHeadLink;
        Sector _dirtySectorTailLink;
        Sector _inTransactionSectorHeadLink;
        Sector _inTransactionSectorTailLink;

        internal State NewState
        {
            get { return _newState; }
        }

        internal Sector TryGetSector(long positionWithSize)
        {
            Lazy<Sector> res;
            if (_sectorCache.TryGetValue(positionWithSize & MaskOfPosition, out res))
            {
                return res.Value;
            }
            return null;
        }

        internal Sector ReadSector(SectorPtr sectorPtr, bool inWriteTransaction)
        {
            return ReadSector(sectorPtr.Ptr, sectorPtr.Checksum, inWriteTransaction);
        }

        Sector ReadSector(long positionWithSize, uint checksum, bool inWriteTransaction)
        {
            Debug.Assert(positionWithSize > 0);
            return ReadSector(positionWithSize & MaskOfPosition, (int)(positionWithSize & 0xFF) + 1, checksum, inWriteTransaction);
        }

        Sector ReadSector(long position, int size, uint checksum, bool inWriteTransaction)
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
            _currentState = new State { Position = FirstRootOffset };
            _newState = new State
                            {
                                Position = SecondRootOffset,
                                WantedDatabaseLength = TotalHeaderSize,
                                UsedSize = TotalHeaderSize,
                                TransactionCounter = 1
                            };
            StoreStateToHeaderBuffer(_newState);
            TransferNewStateToCurrentState();
            StoreStateToHeaderBuffer(_newState);
            _stream.Write(_headerData, 0, TotalHeaderSize, 0);
            _stream.Flush();
        }

        public bool Open(IStream stream, bool dispose)
        {
            if (stream == null) throw new ArgumentNullException("stream");
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
                if (_headerData[0] != (byte)'B' || _headerData[1] != (byte)'T' || _headerData[2] != (byte)'D'
                    || _headerData[3] != (byte)'B' || _headerData[4] != (byte)'1' || _headerData[5] != (byte)'0'
                    || _headerData[6] != (byte)'0' || _headerData[7] != (byte)'0')
                {
                    throw new BTDBException("Wrong header");
                }
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
            _currentState.KeyValuePairCount = _newState.KeyValuePairCount;
            _currentState.UsedSize = _newState.UsedSize;
            _currentState.TransactionCounter = _newState.TransactionCounter;
            _currentState.TransactionLogPtr = _newState.TransactionLogPtr;
            _currentState.TransactionAllocSize = _newState.TransactionAllocSize;
            _currentState.WantedDatabaseLength = _newState.WantedDatabaseLength;
            SwapCurrentAndNewState();
        }

        void StoreStateToHeaderBuffer(State state)
        {
            Debug.Assert(state.RootBTree.Ptr >= 0);
            Debug.Assert(state.RootAllocPage.Ptr >= 0);
            Debug.Assert(state.WantedDatabaseLength >= 0);
            var o = (int)state.Position;
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
            PackUnpack.PackUInt64(_headerData, o, state.KeyValuePairCount);
            o += 8;
            PackUnpack.PackUInt64(_headerData, o, state.UsedSize);
            o += 8;
            PackUnpack.PackUInt64(_headerData, o, state.TransactionCounter);
            o += 8;
            PackUnpack.PackUInt64(_headerData, o, state.WantedDatabaseLength);
            o += 8;
            PackUnpack.PackUInt64(_headerData, o, state.TransactionLogPtr);
            o += 8;
            PackUnpack.PackUInt32(_headerData, o, state.TransactionAllocSize);
            o += 4;
            PackUnpack.PackUInt32(_headerData, o, Checksum.CalcFletcher(_headerData, state.Position, RootSizeWithoutChecksum));
        }

        bool RetrieveStateFromHeaderBuffer(State state)
        {
            var o = (int)state.Position;
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
            state.KeyValuePairCount = PackUnpack.UnpackUInt64(_headerData, o);
            o += 8;
            state.UsedSize = PackUnpack.UnpackUInt64(_headerData, o);
            if (state.UsedSize < AllocationGranularity) return false;
            o += 8;
            state.TransactionCounter = PackUnpack.UnpackUInt64(_headerData, o);
            o += 8;
            state.WantedDatabaseLength = PackUnpack.UnpackUInt64(_headerData, o);
            if (state.WantedDatabaseLength < AllocationGranularity) return false;
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
                                          TransactionNumber = _currentState.TransactionCounter,
                                          ReadTrRunningCount = 1,
                                          RootBTree = _currentState.RootBTree,
                                          KeyValuePairCount = _currentState.KeyValuePairCount,
                                          UsedSize = _currentState.UsedSize,
                                          WantedDatabaseLength = _currentState.WantedDatabaseLength
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
                return new LowLevelDBTransaction(this, link);
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
                        lock (_spaceSoonReusableLock)
                        {
                            if (_spaceSoonReusable == null) _spaceSoonReusable = link.SpaceToReuse;
                            else
                            {
                                _spaceSoonReusable.MergeInPlace(link.SpaceToReuse);
                            }
                        }
                    }
                    _readTrLinkTail = link.Next;
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
            while (_inTransactionSectorHeadLink != null)
            {
                DetransactionalizeSector(_inTransactionSectorHeadLink);
            }
            _readTrLinkHead.SpaceToReuse = _spaceDeallocatedInTransaction.CloneAndClear();
            _spaceAllocatedInTransaction.Clear();
            StoreStateToHeaderBuffer(_newState);
            _stream.Flush();
            _stream.Write(_headerData, (int)_newState.Position, RootSize, _newState.Position);
            TransferNewStateToCurrentState();
            _commitNeeded = false;
            _currentTrCommited = true;
        }

        void DetransactionalizeSector(Sector sector)
        {
            sector.InTransaction = false;
            UnlinkFromInTransactionSectors(sector);
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
                dirtySector.Parent = DirtizeSector(dirtySector.Parent, dirtySector.Parent.Parent);
                PackUnpack.PackInt64(dirtySector.Parent.Data, ofs, ptr);
                PackUnpack.PackUInt32(dirtySector.Parent.Data, ofs + 8, checksum);
            }
            dirtySector.Dirty = false;
            UnlinkFromDirtySectors(dirtySector);
            LinkToTailOfInTransactionSectors(dirtySector);
        }

        internal Sector DirtizeSector(Sector sector, Sector newParent)
        {
            if (sector.Dirty) return sector;
            if (sector.InTransaction == false)
            {
                if (newParent != null)
                {
                    newParent = DirtizeSector(newParent, newParent.Parent);
                }
                var clone = NewSector();
                clone.Length = sector.Length;
                clone.Type = sector.Type;
                Array.Copy(sector.Data, clone.Data, clone.Length);
                clone.Parent = newParent;
                PublishSector(clone);
                UpdatePositionOfSector(clone, sector, newParent);
                DeallocateSector(sector);
                return clone;
            }
            sector.Dirty = true;
            UnlinkFromInTransactionSectors(sector);
            LinkToTailOfDirtySectors(sector);
            return sector;
        }

        private Sector ResizeSector(Sector sector, int newLength, Sector newParent, bool aUpdatePositionInParent)
        {
            newLength = RoundToAllocationGranularity(newLength);
            if (sector.Length == newLength) return DirtizeSector(sector, newParent);
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
            if (newParent != null)
            {
                newParent = DirtizeSector(newParent, newParent.Parent);
            }
            var clone = NewSector();
            clone.Length = newLength;
            clone.Type = sector.Type;
            clone.Parent = newParent;
            PublishSector(clone);
            if (aUpdatePositionInParent)
                UpdatePositionOfSector(clone, sector, newParent);
            DeallocateSector(sector);
            return clone;
        }

        internal Sector ResizeSectorWithUpdatePosition(Sector sector, int newLength, Sector newParent)
        {
            return ResizeSector(sector, newLength, newParent, true);
        }

        internal Sector ResizeSectorNoUpdatePosition(Sector sector, int newLength, Sector newParent)
        {
            return ResizeSector(sector, newLength, newParent, false);
        }

        void UpdatePositionOfSector(Sector newSector, Sector oldSector, Sector inParent)
        {
            if (inParent == null)
            {
                UpdatePostitionOfRootSector(newSector);
            }
            else
            {
                int ofs = FindOfsInParent(oldSector, inParent);
                PackUnpack.PackInt64(inParent.Data, ofs, newSector.Position);
            }
        }

        void UpdatePostitionOfRootSector(Sector rootSector)
        {
            switch (rootSector.Type)
            {
                case SectorType.BTreeParent:
                case SectorType.BTreeChild:
                    {
                        _newState.RootBTree.Ptr = rootSector.Position; // Length encoding is not needed as it is temporary anyway
                        break;
                    }
                case SectorType.AllocChild:
                case SectorType.AllocParent:
                    {
                        _newState.RootAllocPage.Ptr = rootSector.Position; // Max free space encoding is not needed as it is temporary anyway
                        break;
                    }
                default:
                    throw new InvalidOperationException();
            }
        }

        internal void DeallocateSector(Sector sector)
        {
            if (sector.InTransaction)
            {
                if (!sector.Allocated)
                {
                    UnlinkFromUnallocatedSectors(sector);
                    return;
                }
                if (sector.Dirty)
                {
                    UnlinkFromDirtySectors(sector);
                }
                else
                {
                    UnlinkFromInTransactionSectors(sector);
                }
                _spaceAllocatedInTransaction.TryExclude((ulong)sector.Position, (ulong)sector.Length);
            }
            else
            {
                _spaceDeallocatedInTransaction.TryInclude((ulong)sector.Position, (ulong)sector.Length);
                _spaceUsedByReadOnlyTransactions.TryInclude((ulong)sector.Position, (ulong)sector.Length);
            }
            if (_newState.RootAllocPageLevels == 0)
            {
                CreateInitialAllocPages();
            }
            long startGran = sector.Position / AllocationGranularity;
            int grans = sector.Length / AllocationGranularity;
            var totalGrans = (long)(_newState.WantedDatabaseLength / AllocationGranularity);
            UnsetBitsInAlloc(startGran, grans, ref _newState.RootAllocPage, _newState.RootAllocPageLevels, totalGrans);
            _newState.UsedSize -= (ulong)sector.Length;
        }

        void UnsetBitsInAlloc(long startGran, int grans, ref SectorPtr sectorPtr, uint level, long totalGrans)
        {
            Sector sector = TryGetSector(sectorPtr.Ptr);
            if (level == 1)
            {
                if (sector == null)
                {
                    sector = ReadSector(sectorPtr.Ptr & MaskOfPosition,
                                        RoundToAllocationGranularity(((int)totalGrans + 7) / 8),
                                        sectorPtr.Checksum,
                                        true);
                }
                sector = DirtizeSector(sector, sector.Parent);
                BitArrayManipulation.UnsetBits(sector.Data, (int)startGran, grans);
                if (sector.Length < MaxLeafAllocSectorGrans / 8)
                {
                    sectorPtr.Ptr = sector.Position | 255;
                }
                else
                {
                    sectorPtr.Ptr = sector.Position | (uint)BitArrayManipulation.SizeOfBiggestHoleUpTo255(sector.Data);
                }
                return;
            }
            throw new NotImplementedException();
        }

        void CreateInitialAllocPages()
        {
            var grans = (long)(_newState.WantedDatabaseLength / AllocationGranularity);
            long leafSectors = grans / MaxLeafAllocSectorGrans;
            if (grans % MaxLeafAllocSectorGrans != 0) leafSectors++;
            uint levels = 1;
            if (leafSectors > 1)
            {
                long currentLevelLeafSectors = 1;
                levels = 2;
                while (currentLevelLeafSectors * MaxChildren < leafSectors)
                {
                    currentLevelLeafSectors *= MaxChildren;
                    levels++;
                }
            }
            _newState.RootAllocPage = CreateFullAllocationSector(grans, levels, null);
            _newState.RootAllocPageLevels = levels;
        }

        SectorPtr CreateFullAllocationSector(long grans, uint levels, Sector parent)
        {
            if (levels == 1)
            {
                var newLeafSector = NewSector();
                newLeafSector.Type = SectorType.AllocChild;
                newLeafSector.SetLengthWithRound(((int)grans + 7) / 8);
                newLeafSector.Parent = parent;
                BitArrayManipulation.SetBits(newLeafSector.Data, 0, (int)grans);
                PublishSector(newLeafSector);
                return newLeafSector.ToPtrWithLen();
            }

            long currentLevelLeafSectors = Pow(MaxChildren, levels - 1);
            long gransInDownLevel = currentLevelLeafSectors * MaxLeafAllocSectorGrans;
            var newSector = NewSector();
            newSector.Type = SectorType.AllocParent;
            var downPtrCount = (int)((grans + gransInDownLevel - 1) / gransInDownLevel);
            newSector.SetLengthWithRound(downPtrCount * PtrDownSize);
            newSector.Parent = parent;
            for (int i = 0; i < downPtrCount; i++)
            {
                SectorPtr sectorPtr = CreateFullAllocationSector(Math.Min(grans, gransInDownLevel), levels - 1, newSector);
                SectorPtr.Pack(newSector.Data, i * PtrDownSize, sectorPtr);
                grans -= gransInDownLevel;
            }
            PublishSector(newSector);
            return newSector.ToPtrWithLen();
        }

        static long Pow(long x, uint y)
        {
            long result = (y % 2 != 0) ? x : 1;
            while ((y >>= 1) != 0)
            {
                x = x * x;
                if (y % 2 != 0) result *= x;
            }
            return result;
        }

        static int FindOfsInParent(Sector sector, Sector where)
        {
            switch (where.Type)
            {
                case SectorType.BTreeParent:
                    break;
                case SectorType.BTreeChild:
                    var iter = new BTreeChildIterator(where.Data);
                    do
                    {
                        if ((iter.KeySectorPos & MaskOfPosition) == sector.Position)
                            return iter.KeySectorPtrOffset;
                        if ((iter.ValueSectorPos & MaskOfPosition) == sector.Position)
                            return iter.ValueSectorPtrOffset;
                    }
                    while (iter.MoveNext());
                    throw new BTDBException("Cannot FindOfsInParrent");
                case SectorType.AllocParent:
                case SectorType.DataParent:
                    for (int i = 0; i < where.Length / PtrDownSize; i++)
                    {
                        if ((PackUnpack.UnpackInt64(where.Data, i * PtrDownSize) & MaskOfPosition) == sector.Position)
                            return i * PtrDownSize;
                    }
                    throw new BTDBException("Cannot FindOfsInParrent");
                case SectorType.DataChild:
                    throw new BTDBException("DataChild cannot be parent");
                case SectorType.AllocChild:
                    throw new BTDBException("AllocChild cannot be parent");
                default:
                    throw new ArgumentOutOfRangeException();
            }
            throw new NotImplementedException();
        }

        void RealSectorAllocate(Sector unallocatedSector)
        {
            int ofsInParent = -1;
            if (unallocatedSector.Parent != null)
            {
                unallocatedSector.Parent = DirtizeSector(unallocatedSector.Parent, unallocatedSector.Parent.Parent);
                ofsInParent = FindOfsInParent(unallocatedSector, unallocatedSector.Parent);
            }
            long newPosition = AllocateSpace(unallocatedSector.Length);
            Lazy<Sector> lazyTemp;
            _sectorCache.TryRemove(unallocatedSector.Position, out lazyTemp);
            unallocatedSector.Position = newPosition;
            Lazy<Sector> forgetTemp;
            _sectorCache.TryRemove(newPosition, out forgetTemp);
            _sectorCache.TryAdd(newPosition, lazyTemp);
            _spaceAllocatedInTransaction.TryInclude((ulong)newPosition, (ulong)unallocatedSector.Length);
            UnlinkFromUnallocatedSectors(unallocatedSector);
            LinkToTailOfDirtySectors(unallocatedSector);
            if (unallocatedSector.Parent != null)
            {
                PackUnpack.PackUInt64(unallocatedSector.Parent.Data, ofsInParent, (ulong)newPosition);
            }
            else
            {
                UpdatePostitionOfRootSector(unallocatedSector);
            }
        }

        long AllocateSpace(int size)
        {
            Debug.Assert(size > 0);
            Debug.Assert(size % AllocationGranularity == 0);
            int grans = size / AllocationGranularity;
            Debug.Assert(grans < 256);
            if (_newState.RootAllocPageLevels == 0)
            {
                var result = (long)_newState.WantedDatabaseLength;
                _newState.WantedDatabaseLength += (ulong)size;
                _newState.UsedSize += (ulong)size;
                return result;
            }
            if (_spaceSoonReusable != null)
            {
                PtrLenList reuse;
                lock (_spaceSoonReusableLock)
                {
                    reuse = _spaceSoonReusable;
                    _spaceSoonReusable = null;
                }
                _spaceUsedByReadOnlyTransactions.UnmergeInPlace(reuse);
            }
            var totalGrans = (long)(_newState.WantedDatabaseLength / AllocationGranularity);
            long posInGrans = AllocBitsInAlloc(grans, ref _newState.RootAllocPage, _newState.RootAllocPageLevels, totalGrans, 0);
            if (posInGrans < 0) throw new BTDBException("Cannot allocate more space");
            if (posInGrans + grans >= totalGrans)
            {
                _newState.WantedDatabaseLength = (ulong)(posInGrans + grans) * AllocationGranularity;
            }
            _newState.UsedSize += (ulong)size;
            return posInGrans * AllocationGranularity;
        }

        long AllocBitsInAlloc(int grans, ref SectorPtr sectorPtr, uint level, long totalGrans, ulong startInBytes)
        {
            Sector sector = TryGetSector(sectorPtr.Ptr);
            if (level == 1)
            {
                if (sector == null)
                {
                    sector = ReadSector(sectorPtr.Ptr & MaskOfPosition,
                                        RoundToAllocationGranularity(((int)totalGrans + 7) / 8),
                                        sectorPtr.Checksum,
                                        true);
                }
                int startGranSearch = 0;
                int startGran;
                while (true)
                {
                    startGran = BitArrayManipulation.IndexOfFirstHole(sector.Data, grans, startGranSearch);
                    if (startGran < 0) return -1;
                    ulong checkStartInBytes = startInBytes + (ulong)startGran * AllocationGranularity;
                    ulong foundFreeInBytes = _spaceUsedByReadOnlyTransactions.FindFreeSizeAfter(checkStartInBytes, (ulong)grans * AllocationGranularity);
                    if (checkStartInBytes == foundFreeInBytes) break;
                    ulong newStartGranSearch = (foundFreeInBytes - startInBytes) / AllocationGranularity;
                    if (newStartGranSearch > MaxLeafAllocSectorGrans) return -1;
                    startGranSearch = (int)newStartGranSearch;
                }
                sector = DirtizeSector(sector, sector.Parent);
                BitArrayManipulation.SetBits(sector.Data, startGran, grans);
                if (sector.Length < MaxLeafAllocSectorGrans / 8)
                {
                    sectorPtr.Ptr = sector.Position | 255;
                }
                else
                {
                    sectorPtr.Ptr = sector.Position | (uint)BitArrayManipulation.SizeOfBiggestHoleUpTo255(sector.Data);
                }
                return startGran;
            }
            throw new NotImplementedException();
        }

        void UnlinkFromUnallocatedSectors(Sector unallocatedSector)
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

        void UnlinkFromDirtySectors(Sector dirtySector)
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

        void UnlinkFromInTransactionSectors(Sector inTransactionSector)
        {
            if (inTransactionSector.PrevLink == null)
            {
                _inTransactionSectorHeadLink = inTransactionSector.NextLink;
                if (inTransactionSector.NextLink != null)
                {
                    inTransactionSector.NextLink.PrevLink = null;
                }
                else
                {
                    _inTransactionSectorTailLink = null;
                }
            }
            else if (inTransactionSector.NextLink == null)
            {
                _inTransactionSectorTailLink = inTransactionSector.PrevLink;
                inTransactionSector.PrevLink.NextLink = null;
            }
            else
            {
                inTransactionSector.PrevLink.NextLink = inTransactionSector.NextLink;
                inTransactionSector.NextLink.PrevLink = inTransactionSector.PrevLink;
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
                    _spaceUsedByReadOnlyTransactions.UnmergeInPlace(_spaceDeallocatedInTransaction);
                    _spaceAllocatedInTransaction.Clear();
                    _spaceDeallocatedInTransaction.Clear();
                }
            }
            finally
            {
                _writeTr = null;
            }
            DereferenceReadLink(_readTrLinkHead);
        }

        internal void UpgradeTransactionToWriteOne(LowLevelDBTransaction transaction, ReadTrLink link)
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
                Debug.Assert(_inTransactionSectorHeadLink == null);
                Debug.Assert(_inTransactionSectorTailLink == null);
                Debug.Assert(_spaceAllocatedInTransaction.Empty);
                Debug.Assert(_spaceDeallocatedInTransaction.Empty);
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
            var lazy = new Lazy<Sector>(() => newSector);

            // Immediately to evaluate helps display value in debugger
#pragma warning disable 168
            var forget = lazy.Value;
#pragma warning restore 168

            _sectorCache.TryAdd(newSector.Position, lazy);
            _commitNeeded = true;
            LinkToTailOfUnallocatedSectors(newSector);
        }

        void LinkToTailOfUnallocatedSectors(Sector newSector)
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

        void LinkToTailOfDirtySectors(Sector dirtizeSector)
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

        void LinkToTailOfInTransactionSectors(Sector inTransactionSector)
        {
            inTransactionSector.NextLink = null;
            inTransactionSector.PrevLink = _inTransactionSectorTailLink;
            if (_inTransactionSectorTailLink != null)
            {
                _inTransactionSectorTailLink.NextLink = inTransactionSector;
            }
            else
            {
                _inTransactionSectorHeadLink = inTransactionSector;
            }
            _inTransactionSectorTailLink = inTransactionSector;
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

        internal LowLevelDBStats CalculateStats(ReadTrLink readLink)
        {
            var result = new LowLevelDBStats
                             {
                                 DatabaseStreamSize = _stream.GetSize()
                             };
            if (readLink!=null)
            {
                result.TransactionNumber = readLink.TransactionNumber;
                result.KeyValuePairCount = readLink.KeyValuePairCount;
                result.ReallyUsedSize = readLink.UsedSize;
                result.WastedSize = readLink.WantedDatabaseLength - readLink.UsedSize;
            }
            else
            {
                result.TransactionNumber = _newState.TransactionCounter;
                result.KeyValuePairCount = _newState.KeyValuePairCount;
                result.ReallyUsedSize = _newState.UsedSize;
                result.WastedSize = _newState.WantedDatabaseLength - _newState.UsedSize;
            }
            return result;
        }
    }
}
