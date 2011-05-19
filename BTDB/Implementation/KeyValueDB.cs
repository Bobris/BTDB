using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace BTDB
{
    /* 64 bits for offset (55bit offset + 9bit length)
     * 32 bits for http://en.wikipedia.org/wiki/Fletcher%27s_checksum
     * 262144 max sector size in bytes
     * Allocation granularity 512 bytes (9 bits)
     * 256 pointers to lower pages in 1 page (3072)
     * Allocation pages:
     *   16MB in one page 512*8*4096
     *    4GB in 2nd level
     *    1TB in 3rd level
     *  256TB in 4th level
     *   64PB in 5th level
     *   16EB in 6th level
     * 
     * Root: 352(Header)+80*2=512
     *   16 - B+Tree (8 ofs+4 check+4 levels)
     *   12 - Free Space Tree (8 ofs+4 check)
     *    4 - Unused - Zeros
     *    8 - Count of key/value pairs stored
     *    8 - Used Size - including first 512 bytes of header and includes paddings to allocation granularity
     *    8 - Transaction Number
     *    8 - Wanted Size - size of database stream could be trimmed to this size, also this determines Free Space Tree
     *    8 - Transaction Log Position
     *    4 - Transaction Log Allocated Size
     *    4 - Checksum
     */

    public sealed class KeyValueDB : IKeyValueDB
    {
        internal class State
        {
            internal SectorPtr RootBTree;
            internal uint RootBTreeLevels;
            internal SectorPtr RootAllocPage;
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

        internal const int AllocationGranularity = 512;
        const int RootSize = 80;
        const int RootSizeWithoutChecksum = RootSize - 4;
        const int FirstRootOffset = AllocationGranularity - 2 * RootSize;
        const int SecondRootOffset = FirstRootOffset + RootSize;
        const int TotalHeaderSize = SecondRootOffset + RootSize;
        internal const long MaskOfPosition = -AllocationGranularity; // 0xFFFFFFFFFFFFFF00
        internal const int MaxSectorSize = AllocationGranularity * AllocationGranularity;
        internal const int MaxLeafDataSectorSize = 4096;
        const int MaxLeafAllocSectorSize = 4096;
        internal const int MaxLeafAllocSectorGrans = MaxLeafAllocSectorSize * 8;
        internal const int PtrDownSize = 12;
        internal const int MaxChildren = 256;
        internal const int MaskOfGranLength = AllocationGranularity - 1;

        IPositionLessStream _positionLessStream;
        bool _disposeStream;

        IKeyValueDBTweaks _keyValueDBTweaks = new DefaultKeyValueDBTweaks();
        readonly ConcurrentDictionary<long, Lazy<Sector>> _sectorCache = new ConcurrentDictionary<long, Lazy<Sector>>();
        int _bytesInCache;
        long _currentCacheTime = 1;
        readonly ReaderWriterLockSlim _cacheCompactionLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        bool _runningCacheCompaction;
        bool _runningWriteCacheCompaction;
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
        KeyValueDBTransaction _writeTr;
        bool _commitNeeded;
        bool _currentTrCommited;
        bool _inSpaceAllocation;
        readonly List<Sector> _postponedDeallocateSectors = new List<Sector>();
        long _unallocatedCounter;
        Sector _unallocatedSectorHeadLink;
        Sector _unallocatedSectorTailLink;
        Sector _dirtySectorHeadLink;
        Sector _dirtySectorTailLink;
        Sector _inTransactionSectorHeadLink;
        Sector _inTransactionSectorTailLink;
        int _unallocatedSectorCount;
        int _dirtySectorCount;
        int _inTransactionSectorCount;
        ulong _totalBytesRead;
        ulong _totalBytesWritten;
        bool _durableTransactions = true;
        long _nextAllocStartInGrans;
        readonly FreeSpaceAllocatorOptimizer _freeSpaceAllocatorOptimizer = new FreeSpaceAllocatorOptimizer();
        readonly ConcurrentQueue<TaskCompletionSource<IKeyValueDBTransaction>> _writtingQueue =
            new ConcurrentQueue<TaskCompletionSource<IKeyValueDBTransaction>>();
        bool _writeTrInCreation;
        bool _wasAnyCommits;

        internal State NewState
        {
            get { return _newState; }
        }

        Sector TryGetSectorNoUpdateAccess(long positionWithSize)
        {
            Lazy<Sector> res;
            if (_sectorCache.TryGetValue(positionWithSize & MaskOfPosition, out res))
            {
                var sector = res.Value;
                return sector;
            }
            return null;
        }

        internal Sector TryGetSector(long positionWithSize)
        {
            Lazy<Sector> res;
            if (_sectorCache.TryGetValue(positionWithSize & MaskOfPosition, out res))
            {
                var sector = res.Value;
                UpdateLastAccess(sector);
                return sector;
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
            return ReadSector(positionWithSize & MaskOfPosition, (int)(positionWithSize & MaskOfGranLength) + 1, checksum, inWriteTransaction);
        }

        Sector ReadSector(long position, int size, uint checksum, bool inWriteTransaction)
        {
            if (position <= 0) throw new BTDBException("Wrong data in db (negative position)");
            if (size <= 0 || size > MaxSectorSize / AllocationGranularity) throw new BTDBException("Wrong sector length");
            TruncateSectorCache(inWriteTransaction);
            size = size * AllocationGranularity;
            var lazy = new Lazy<Sector>(() =>
            {
                var res = new Sector { Position = position, Length = size, InternalLastAccessTime = 0 };
                if (inWriteTransaction)
                {
                    res.InTransaction = _spaceAllocatedInTransaction.Contains((ulong)position);
                    if (res.InTransaction)
                        LinkToTailOfInTransactionSectors(res);
                }
                //Console.WriteLine("Reading {0} len:{1}", position, size);
                if (_positionLessStream.Read(res.Data, 0, size, (ulong)position) != size)
                {
                    throw new BTDBException("Data reading error");
                }
                _totalBytesRead += (ulong)size;
                if (Checksum.CalcFletcher(res.Data, 0, (uint)size) != checksum)
                {
                    throw new BTDBException("Checksum error");
                }
                Interlocked.Add(ref _bytesInCache, size);
                UpdateLastAccess(res);
                return res;
            });
            var lazy2 = _sectorCache.GetOrAdd(position, lazy);
            Sector result = lazy2.Value;
            UpdateLastAccess(result);
            return result;
        }

        internal void TruncateSectorCache(bool inWriteTransaction)
        {
            if (!_keyValueDBTweaks.ShouldAttemptCacheCompaction(_sectorCache.Count, _bytesInCache)) return;
            if (_runningWriteCacheCompaction) return;
            if (inWriteTransaction)
            {
                if (_inSpaceAllocation)
                    inWriteTransaction = false;
            }
            bool compacting = false;
            bool runningCompactingSet = false;
            try
            {
                if (inWriteTransaction)
                {
                    _cacheCompactionLock.EnterUpgradeableReadLock();
                    compacting = true;
                }
                else
                {
                    compacting = _cacheCompactionLock.TryEnterUpgradeableReadLock(0);
                    if (compacting == false) return;
                }
                if (_runningCacheCompaction) return;
                _runningCacheCompaction = true;
                _runningWriteCacheCompaction = inWriteTransaction;
                runningCompactingSet = true;
                var sectors = new List<Sector>();
                foreach (var pair in _sectorCache)
                {
                    Sector sector;
                    try
                    {
                        sector = pair.Value.Value;
                    }
                    catch (Exception)
                    {
                        _sectorCache.TryRemove(pair.Key);
                        continue;
                    }
                    if (!inWriteTransaction && sector.InTransaction) continue;
                    sector.LastAccessTime = (ulong) Interlocked.Read(ref sector.InternalLastAccessTime);
                    sectors.Add(sector);
                }
                if (sectors.Count == 0) return;
                _keyValueDBTweaks.WhichSectorsToRemoveFromCache(sectors);
                if (sectors.Count == 0) return;
                using (_cacheCompactionLock.WriteLock())
                {
                    foreach (var sector in sectors)
                    {
                        if (!sector.Deleted)
                        {
                            if (!sector.Allocated) RealSectorAllocate(sector);
                            Debug.Assert(sector.Deleted == false);
                            if (sector.Dirty) FlushDirtySector(sector);
                            Debug.Assert(sector.Deleted == false);
                            if (sector.InTransaction) UnlinkFromInTransactionSectors(sector);
                        }
                        LowLevelRemoveFromSectorCache(sector);
                    }
                }
            }
            finally
            {
                if (runningCompactingSet)
                {
                    _runningCacheCompaction = false;
                    _runningWriteCacheCompaction = false;
                }
                if (compacting) _cacheCompactionLock.ExitUpgradeableReadLock();
            }
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
            _headerData[7] = (byte)'2';
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
            _totalBytesWritten += TotalHeaderSize;
            _positionLessStream.Write(_headerData, 0, TotalHeaderSize, 0);
            _positionLessStream.Flush();
        }

        public IKeyValueDBTweaks KeyValueDBTweaks
        {
            get { return _keyValueDBTweaks; }
            set { _keyValueDBTweaks = value; }
        }

        public bool DurableTransactions
        {
            get { return _durableTransactions; }
            set { _durableTransactions = value; }
        }

        public bool Open(IPositionLessStream positionLessStream, bool dispose)
        {
            if (positionLessStream == null) throw new ArgumentNullException("positionLessStream");
            _positionLessStream = positionLessStream;
            _disposeStream = dispose;
            _spaceSoonReusable = null;
            _freeSpaceAllocatorOptimizer.GlobalInvalidate();
            _wasAnyCommits = false;
            bool newDB = false;
            if (positionLessStream.GetSize() == 0)
            {
                InitEmptyDB();
                newDB = true;
            }
            else
            {
                if (_positionLessStream.Read(_headerData, 0, TotalHeaderSize, 0) != TotalHeaderSize)
                {
                    throw new BTDBException("Too short header");
                }
                _totalBytesRead += TotalHeaderSize;
                if (_headerData[0] != (byte)'B' || _headerData[1] != (byte)'T' || _headerData[2] != (byte)'D'
                    || _headerData[3] != (byte)'B' || _headerData[4] != (byte)'1' || _headerData[5] != (byte)'0'
                    || _headerData[6] != (byte)'0' || _headerData[7] != (byte)'2')
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
                    if (_currentState.TransactionCounter < _newState.TransactionCounter)
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
            PackUnpack.PackUInt64LE(_headerData, o, (ulong)state.RootBTree.Ptr);
            o += 8;
            PackUnpack.PackUInt32LE(_headerData, o, state.RootBTree.Checksum);
            o += 4;
            PackUnpack.PackUInt32LE(_headerData, o, state.RootBTreeLevels);
            o += 4;
            PackUnpack.PackUInt64LE(_headerData, o, (ulong)state.RootAllocPage.Ptr);
            o += 8;
            PackUnpack.PackUInt32LE(_headerData, o, state.RootAllocPage.Checksum);
            o += 4;
            PackUnpack.PackUInt32LE(_headerData, o, 0);
            o += 4;
            PackUnpack.PackUInt64LE(_headerData, o, state.KeyValuePairCount);
            o += 8;
            PackUnpack.PackUInt64LE(_headerData, o, state.UsedSize);
            o += 8;
            PackUnpack.PackUInt64LE(_headerData, o, state.TransactionCounter);
            o += 8;
            PackUnpack.PackUInt64LE(_headerData, o, state.WantedDatabaseLength);
            o += 8;
            PackUnpack.PackUInt64LE(_headerData, o, state.TransactionLogPtr);
            o += 8;
            PackUnpack.PackUInt32LE(_headerData, o, state.TransactionAllocSize);
            o += 4;
            PackUnpack.PackUInt32LE(_headerData, o, Checksum.CalcFletcher(_headerData, state.Position, RootSizeWithoutChecksum));
        }

        bool RetrieveStateFromHeaderBuffer(State state)
        {
            var o = (int)state.Position;
            if (Checksum.CalcFletcher(_headerData, state.Position, RootSizeWithoutChecksum) !=
                PackUnpack.UnpackUInt32LE(_headerData, o + RootSizeWithoutChecksum))
            {
                return false;
            }
            state.RootBTree.Ptr = (long)PackUnpack.UnpackUInt64LE(_headerData, o);
            if (state.RootBTree.Ptr < 0) return false;
            o += 8;
            state.RootBTree.Checksum = PackUnpack.UnpackUInt32LE(_headerData, o);
            o += 4;
            state.RootBTreeLevels = PackUnpack.UnpackUInt32LE(_headerData, o);
            o += 4;
            state.RootAllocPage.Ptr = (long)PackUnpack.UnpackUInt64LE(_headerData, o);
            if (state.RootAllocPage.Ptr < 0) return false;
            o += 8;
            state.RootAllocPage.Checksum = PackUnpack.UnpackUInt32LE(_headerData, o);
            o += 4;
            // Unused space
            o += 4;
            state.KeyValuePairCount = PackUnpack.UnpackUInt64LE(_headerData, o);
            o += 8;
            state.UsedSize = PackUnpack.UnpackUInt64LE(_headerData, o);
            if (state.UsedSize < AllocationGranularity) return false;
            o += 8;
            state.TransactionCounter = PackUnpack.UnpackUInt64LE(_headerData, o);
            o += 8;
            state.WantedDatabaseLength = PackUnpack.UnpackUInt64LE(_headerData, o);
            if (state.WantedDatabaseLength < AllocationGranularity) return false;
            o += 8;
            state.TransactionLogPtr = PackUnpack.UnpackUInt64LE(_headerData, o);
            o += 8;
            state.TransactionAllocSize = PackUnpack.UnpackUInt32LE(_headerData, o);
            return true;
        }

        internal void DisposeReadTransaction(ReadTrLink link)
        {
            DereferenceReadLink(link);
        }

        public IKeyValueDBTransaction StartTransaction()
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
                return new KeyValueDBTransaction(this, link);
            }
            catch (Exception)
            {
                DereferenceReadLink(link);
                throw;
            }
        }

        public Task<IKeyValueDBTransaction> StartWritingTransaction()
        {
            var taskCompletionSource = new TaskCompletionSource<IKeyValueDBTransaction>();
            _writtingQueue.Enqueue(taskCompletionSource);
            TryToRunNextWrittingTransaction();
            return taskCompletionSource.Task;
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

        [Conditional("DEBUG")]
        void InDebuggerCheckDisposeInvariants()
        {
            Debug.Assert(_writeTr == null);
            int bytesInCache = 0;
            foreach (var item in _sectorCache)
            {
                bytesInCache += item.Value.Value.Length;
            }
            Debug.Assert(bytesInCache == _bytesInCache);
        }

        public void Dispose()
        {
            InDebuggerCheckDisposeInvariants();
            if (_wasAnyCommits)
            {
                StoreStateToHeaderBuffer(_newState);
                _totalBytesWritten += RootSize;
                _positionLessStream.Write(_headerData, (int)_newState.Position, RootSize, _newState.Position);
            }
            if (_disposeStream)
            {
                var disposable = _positionLessStream as IDisposable;
                if (disposable != null) disposable.Dispose();
            }
            _positionLessStream = null;
        }

        internal void CommitWriteTransaction()
        {
            Debug.Assert(_writeTr != null);
            if (_currentTrCommited) throw new BTDBException("Only dispose is allowed after commit");
            if (_commitNeeded == false) return;
            using (_cacheCompactionLock.UpgradableReadLock())
            {
                AllocateAndFlushSectors();
            }
            while (_inTransactionSectorHeadLink != null)
            {
                DetransactionalizeSector(_inTransactionSectorHeadLink);
            }
            TruncateSectorCache(true);
            _readTrLinkHead.SpaceToReuse = _spaceDeallocatedInTransaction.CloneAndClear();
            _spaceAllocatedInTransaction.Clear();
            StoreStateToHeaderBuffer(_newState);
            _totalBytesWritten += RootSize;
            _positionLessStream.Write(_headerData, (int)_newState.Position, RootSize, _newState.Position);
            _wasAnyCommits = true;
            if (_durableTransactions)
                _positionLessStream.HardFlush();
            else
                _positionLessStream.Flush();
            TransferNewStateToCurrentState();
            _freeSpaceAllocatorOptimizer.CommitWriteTransaction();
            _commitNeeded = false;
            _currentTrCommited = true;
        }

        void AllocateAndFlushSectors()
        {
            AllocateAndFlushSectorsByType(SectorType.DataChild);
            AllocateAndFlushSectorsByType(SectorType.DataParent);
            AllocateAndFlushSectorsByType(SectorType.BTreeChild);
            AllocateAndFlushSectorsByType(SectorType.BTreeParent);
            while (_unallocatedSectorHeadLink != null)
            {
                RealSectorAllocate(_unallocatedSectorHeadLink);
            }
            while (_dirtySectorHeadLink != null)
            {
                FlushDirtySector(_dirtySectorHeadLink);
            }
        }

        void AllocateAndFlushSectorsByType(SectorType sectorType)
        {
            for (int d = FindMaxDeepness(sectorType); d > 0; d--)
            {
                var s = _unallocatedSectorHeadLink;
                while (s != null)
                {
                    var cs = s;
                    s = s.NextLink;
                    if (cs.Type == sectorType && cs.Deepness == d)
                    {
                        RealSectorAllocate(cs); FlushDirtySector(cs);
                    }
                }
                s = _dirtySectorHeadLink;
                while (s != null)
                {
                    var cs = s;
                    s = s.NextLink;
                    if (cs.Type == sectorType && cs.Deepness == d)
                    {
                        FlushDirtySector(cs);
                    }
                }
            }
        }

        int FindMaxDeepness(SectorType sectorType)
        {
            int maxdeep = 0;
            var s = _unallocatedSectorHeadLink;
            while (s != null)
            {
                if (s.Type == sectorType) maxdeep = Math.Max(maxdeep, s.Deepness);
                s = s.NextLink;
            }
            s = _dirtySectorHeadLink;
            while (s != null)
            {
                if (s.Type == sectorType) maxdeep = Math.Max(maxdeep, s.Deepness);
                s = s.NextLink;
            }
            return maxdeep;
        }

        void DetransactionalizeSector(Sector sector)
        {
            sector.InTransaction = false;
            UnlinkFromInTransactionSectors(sector);
        }

        internal void ForceFlushSector(Sector sector)
        {
            if (!sector.InTransaction) return;
            if (!sector.Allocated) RealSectorAllocate(sector);
            if (sector.Dirty) FlushDirtySector(sector);
        }

        private void FlushDirtySector(Sector dirtySector)
        {
            //Console.WriteLine("Writing {0} len:{1}", dirtySector.Position, dirtySector.Length);
            _totalBytesWritten += (ulong)dirtySector.Length;
            _positionLessStream.Write(dirtySector.Data, 0, dirtySector.Length, (ulong)dirtySector.Position);
            var checksum = Checksum.CalcFletcher(dirtySector.Data, 0, (uint)dirtySector.Length);
            long ptr = dirtySector.Position;
            ptr += dirtySector.Length / AllocationGranularity - 1;
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
                dirtySector.Parent = DirtizeSector(dirtySector.Parent, dirtySector.Parent.Parent, null);
                PackUnpack.PackInt64LE(dirtySector.Parent.Data, ofs, ptr);
                PackUnpack.PackUInt32LE(dirtySector.Parent.Data, ofs + 8, checksum);
            }
            dirtySector.Dirty = false;
            UnlinkFromDirtySectors(dirtySector);
            LinkToTailOfInTransactionSectors(dirtySector);
        }

        internal Sector DirtizeSector(Sector sector, Sector newParent, List<Sector> unlockStack)
        {
            if (sector.Dirty) return sector;
            if (newParent != null)
            {
                newParent = DirtizeSector(newParent, newParent.Parent, unlockStack);
            }
            if (sector.InTransaction == false)
            {
                var clone = NewSector();
                clone.Length = sector.Length;
                clone.Type = sector.Type;
                Array.Copy(sector.Data, clone.Data, clone.Length);
                clone.Parent = newParent;
                PublishSector(clone);
                UpdatePositionOfSector(clone, sector, newParent);
                DeallocateSector(sector);
                UpdateCurrentParents(sector, clone, unlockStack);
                return clone;
            }
            Lazy<Sector> lazyActual;
            if (_sectorCache.TryGetValue(sector.Position, out lazyActual))
            {
                if (!ReferenceEquals(lazyActual.Value, sector))
                {
                    return DirtizeSector(lazyActual.Value, newParent, unlockStack);
                }
                UnlinkFromInTransactionSectors(sector);
            }
            else
            {
                var localSector = sector;
                if (_sectorCache.TryAdd(sector.Position & MaskOfPosition, new Lazy<Sector>(() => localSector).Force()))
                {
                    Interlocked.Add(ref _bytesInCache, localSector.Length);
                }
            }
            sector.Dirty = true;
            LinkToTailOfDirtySectors(sector);
            return sector;
        }

        private Sector ResizeSector(Sector sector, int newLength, Sector newParent, bool aUpdatePositionInParent, List<Sector> unlockStack)
        {
            if (newLength < 0 || newLength > MaxSectorSize) throw new BTDBException("Sector cannot be bigger than MaxSectorSize");
            newLength = RoundToAllocationGranularity(newLength);
            if (sector.Length == newLength && (aUpdatePositionInParent || sector.InTransaction)) return DirtizeSector(sector, newParent, unlockStack);
            if (sector.InTransaction)
            {
                if (!sector.Allocated)
                {
                    var oldLength = sector.Length;
                    sector.Length = newLength;
                    if (_sectorCache.ContainsKey(sector.Position & MaskOfPosition))
                    {
                        Interlocked.Add(ref _bytesInCache, newLength - oldLength);
                    }
                    else
                    {
                        var localSector = sector;
                        if (_sectorCache.TryAdd(sector.Position & MaskOfPosition, new Lazy<Sector>(() => localSector).Force()))
                        {
                            Interlocked.Add(ref _bytesInCache, localSector.Length);
                        }
                    }
                    return sector;
                }
            }
            if (newParent != null)
            {
                newParent = DirtizeSector(newParent, newParent.Parent, unlockStack);
            }
            var clone = NewSector();
            clone.Length = newLength;
            clone.Type = sector.Type;
            clone.Parent = newParent;
            PublishSector(clone);
            if (aUpdatePositionInParent)
                UpdatePositionOfSector(clone, sector, newParent);
            DeallocateSector(sector);
            UpdateCurrentParents(sector, clone, unlockStack);
            return clone;
        }

        static void UpdateCurrentParents(Sector oldSector, Sector newSector, List<Sector> unlockStack)
        {
            if (unlockStack == null) return;
            for (int i = 0; i < unlockStack.Count; i++)
            {
                if (unlockStack[i] == oldSector)
                {
                    unlockStack[i] = newSector;
                    return;
                }
            }
        }

        internal Sector ResizeSectorWithUpdatePosition(Sector sector, int newLength, Sector newParent, List<Sector> unlockStack)
        {
            return ResizeSector(sector, newLength, newParent, true, unlockStack);
        }

        internal Sector ResizeSectorNoUpdatePosition(Sector sector, int newLength, Sector newParent, List<Sector> unlockStack)
        {
            return ResizeSector(sector, newLength, newParent, false, unlockStack);
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
                PackUnpack.PackInt64LE(inParent.Data, ofs, newSector.Position);
            }
        }

        void UpdatePostitionOfRootSector(Sector rootSector)
        {
            switch (rootSector.Type)
            {
                case SectorType.BTreeParent:
                case SectorType.BTreeChild:
                    {
                        // Length encoding is not needed as it is temporary anyway
                        _newState.RootBTree.Ptr = rootSector.Position;
                        break;
                    }
                case SectorType.AllocChild:
                case SectorType.AllocParent:
                    {
                        // Length encoding is not needed as it is temporary anyway
                        _newState.RootAllocPage.Ptr = rootSector.Position;
                        break;
                    }
                default:
                    throw new InvalidOperationException();
            }
        }

        internal void DeallocateSector(Sector sector)
        {
            if (_inSpaceAllocation)
            {
                Debug.Assert(sector.Deleted == false);
                sector.Deleted = true;
                _postponedDeallocateSectors.Add(sector);
                return;
            }
            _inSpaceAllocation = true;
            try
            {
                if (sector.InTransaction)
                {
                    sector.Deleted = true;
                    LowLevelRemoveFromSectorCache(sector);
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
                if (_newState.RootAllocPage.Ptr == 0)
                {
                    _inSpaceAllocation = false;
                    CreateInitialAllocPages();
                    _inSpaceAllocation = true;
                }
                long startGran = sector.Position / AllocationGranularity;
                int grans = sector.Length / AllocationGranularity;
                var totalGrans = (long)(_newState.WantedDatabaseLength / AllocationGranularity);
                _freeSpaceAllocatorOptimizer.InvalidateForNextTransaction(startGran, grans);
                UnsetBitsInAlloc(startGran, grans, ref _newState.RootAllocPage, totalGrans, null);
                _newState.UsedSize -= (ulong)sector.Length;
            }
            finally
            {
                FinishPostponedSpaceDeallocation();
            }
        }

        void FinishPostponedSpaceDeallocation()
        {
            _inSpaceAllocation = false;
            if (_postponedDeallocateSectors.Count > 0)
            {
                var postponed = _postponedDeallocateSectors.ToArray();
                _postponedDeallocateSectors.Clear();
                foreach (var postbonedDeallocateSector in postponed)
                {
                    DeallocateSector(postbonedDeallocateSector);
                }
            }
        }

        static long GetGransInDownLevel(long len, out int downPtrCount)
        {
            if (len <= MaxLeafAllocSectorGrans)
            {
                downPtrCount = (int)(len + MaxLeafAllocSectorGrans / MaxChildren - 1) / (MaxLeafAllocSectorGrans / MaxChildren);
                return MaxLeafAllocSectorGrans / MaxChildren;
            }
            long leafSectors = len / MaxLeafAllocSectorGrans;
            if (len % MaxLeafAllocSectorGrans != 0) leafSectors++;
            long currentLevelLeafSectors = 1;
            while (currentLevelLeafSectors * MaxChildren < leafSectors)
                currentLevelLeafSectors *= MaxChildren;
            long gransInDownLevel = currentLevelLeafSectors * MaxLeafAllocSectorGrans;
            downPtrCount = (int)((leafSectors + currentLevelLeafSectors - 1) / currentLevelLeafSectors);
            return gransInDownLevel;
        }

        void UnsetBitsInAlloc(long startGran, int grans, ref SectorPtr sectorPtr, long totalGrans, Sector parent)
        {
            Sector sector = TryGetSector(sectorPtr.Ptr);
            if (totalGrans <= MaxLeafAllocSectorGrans)
            {
                if (sector == null)
                {
                    sector = ReadSector(sectorPtr.Ptr & MaskOfPosition,
                                        RoundToAllocationGranularity(((int)totalGrans + 7) / 8) / AllocationGranularity,
                                        sectorPtr.Checksum,
                                        true);
                    sector.Type = SectorType.AllocChild;
                }
                sector.Parent = parent;
                sector = DirtizeSector(sector, sector.Parent, null);
                BitArrayManipulation.UnsetBits(sector.Data, (int)startGran, grans);
                sectorPtr = sector.ToSectorPtr();
                return;
            }
            int childSectors;
            long gransInChild = GetGransInDownLevel(totalGrans, out childSectors);
            if (sector == null)
            {
                sector = ReadSector(sectorPtr.Ptr & MaskOfPosition,
                                    RoundToAllocationGranularity(childSectors * PtrDownSize) / AllocationGranularity,
                                    sectorPtr.Checksum,
                                    true);
                sector.Type = SectorType.AllocParent;
            }
            sector.Parent = parent;
            sector = DirtizeSector(sector, sector.Parent, null);
            for (var i = (int)(startGran / gransInChild); i < childSectors; i++)
            {
                var startingGranOfChild = i * gransInChild;
                if (startGran + grans <= startingGranOfChild) break;
                var childSectorPtr = SectorPtr.Unpack(sector.Data, i * PtrDownSize);
                var currentStartGran = Math.Max(0, startGran - startingGranOfChild);
                UnsetBitsInAlloc(currentStartGran,
                                 (int)(Math.Min(startGran + grans - startingGranOfChild, gransInChild) - currentStartGran),
                                 ref childSectorPtr,
                                 Math.Min(totalGrans - startingGranOfChild, gransInChild),
                                 sector);
                SectorPtr.Pack(sector.Data, i * PtrDownSize, childSectorPtr);
            }
            sectorPtr = sector.ToSectorPtr();
        }

        class InitAllocItem
        {
            internal Sector Sector { get; set; }
            internal int Level { get; set; }
            internal int Children { get; set; }
            internal bool Full { get { return Level == 0 || Children == MaxChildren; } }
        }

        void CreateInitialAllocPages()
        {
            var createdGrans = 0L;
            var createGrans = (long)(_newState.WantedDatabaseLength / AllocationGranularity);
            var sectorStack = new Stack<InitAllocItem>();
            while (createdGrans < createGrans)
            {
                int newgrans = MaxLeafAllocSectorGrans;
                if (createGrans - createdGrans < MaxLeafAllocSectorGrans) newgrans = (int)(createGrans - createdGrans);
                if (sectorStack.Count > 0)
                {
                    while (true)
                    {
                        var last = sectorStack.Pop();
                        if (sectorStack.Count == 0 || sectorStack.Peek().Level > last.Level + 1)
                        {
                            Sector newParentSector = NewSector();
                            newParentSector.Type = SectorType.AllocParent;
                            newParentSector.SetLengthWithRound(PtrDownSize);
                            if (sectorStack.Count > 0) newParentSector.Parent = sectorStack.Peek().Sector;
                            SectorPtr.Pack(newParentSector.Data, 0, last.Sector.ToSectorPtr());
                            last.Sector.Parent = newParentSector;
                            sectorStack.Push(new InitAllocItem { Level = last.Level + 1, Children = 1, Sector = newParentSector });
                            PublishSector(newParentSector);
                            break;
                        }
                        var last2 = sectorStack.Peek();
                        last2.Sector.SetLengthWithRound((last2.Children + 1) * PtrDownSize);
                        SectorPtr.Pack(last2.Sector.Data, last2.Children * PtrDownSize, last.Sector.ToSectorPtr());
                        last2.Children++;
                        if (!last2.Full) break;
                    }
                }
                CreateInitialAllocLeafSector(newgrans, sectorStack);
                createdGrans += newgrans;
                createGrans = (long)(_newState.WantedDatabaseLength / AllocationGranularity);
            }
            while (sectorStack.Count > 1)
            {
                var last = sectorStack.Pop();
                var last2 = sectorStack.Peek();
                last2.Sector.SetLengthWithRound((last2.Children + 1) * PtrDownSize);
                last.Sector.Parent = last2.Sector;
                SectorPtr.Pack(last2.Sector.Data, last2.Children * PtrDownSize, last.Sector.ToSectorPtr());
            }
            _newState.RootAllocPage = sectorStack.Peek().Sector.ToSectorPtr();
        }

        void CreateInitialAllocLeafSector(int grans, Stack<InitAllocItem> sectorStack)
        {
            var newLeafSector = NewSector();
            newLeafSector.Type = SectorType.AllocChild;
            newLeafSector.SetLengthWithRound((grans + 7) / 8);
            if (sectorStack.Count > 0) newLeafSector.Parent = sectorStack.Peek().Sector;
            BitArrayManipulation.SetBits(newLeafSector.Data, 0, grans);
            sectorStack.Push(new InitAllocItem { Sector = newLeafSector, Level = 0 });
            PublishSector(newLeafSector);
            TruncateSectorCache(true);
        }

        static int FindOfsInParent(Sector sector, Sector where)
        {
            switch (where.Type)
            {
                case SectorType.BTreeParent:
                    {
                        var iter = new BTreeParentIterator(where.Data);
                        if ((iter.FirstChildSectorPos & MaskOfPosition) == sector.Position)
                            return BTreeParentIterator.FirstChildSectorPtrOffset;
                        if (iter.Count != 0)
                        {
                            iter.MoveFirst();
                            do
                            {
                                if ((iter.KeySectorPos & MaskOfPosition) == sector.Position)
                                    return iter.KeySectorPtrOffset;
                                if ((iter.ChildSectorPos & MaskOfPosition) == sector.Position)
                                    return iter.ChildSectorPtrOffset;
                            }
                            while (iter.MoveNext());
                        }
                        throw new BTDBException("Cannot FindOfsInParent");
                    }
                case SectorType.BTreeChild:
                    {
                        var iter = new BTreeChildIterator(where.Data);
                        iter.MoveFirst();
                        do
                        {
                            if ((iter.KeySectorPos & MaskOfPosition) == sector.Position)
                                return iter.KeySectorPtrOffset;
                            if ((iter.ValueSectorPos & MaskOfPosition) == sector.Position)
                                return iter.ValueSectorPtrOffset;
                        }
                        while (iter.MoveNext());
                        throw new BTDBException("Cannot FindOfsInParent");
                    }
                case SectorType.AllocParent:
                case SectorType.DataParent:
                    for (int i = 0; i < where.Length / PtrDownSize; i++)
                    {
                        if ((PackUnpack.UnpackInt64LE(where.Data, i * PtrDownSize) & MaskOfPosition) == sector.Position)
                            return i * PtrDownSize;
                    }
                    throw new BTDBException("Cannot FindOfsInParent");
                case SectorType.DataChild:
                    throw new BTDBException("DataChild cannot be parent");
                case SectorType.AllocChild:
                    throw new BTDBException("AllocChild cannot be parent");
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        void RealSectorAllocate(Sector unallocatedSector)
        {
            int ofsInParent = -1;
            if (unallocatedSector.Parent != null)
            {
                unallocatedSector.Parent = DirtizeSector(unallocatedSector.Parent, unallocatedSector.Parent.Parent, null);
                ofsInParent = FindOfsInParent(unallocatedSector, unallocatedSector.Parent);
            }
            long newPosition = AllocateSpace(unallocatedSector.Length);
            Lazy<Sector> lazyTemp;
            _sectorCache.TryRemove(unallocatedSector.Position, out lazyTemp);
            unallocatedSector.Position = newPosition;
            Lazy<Sector> lazyRemoved;
            if (_sectorCache.TryRemove(newPosition, out lazyRemoved))
            {
                try
                {
                    Interlocked.Add(ref _bytesInCache, -lazyRemoved.Value.Length);
                }
                // ReSharper disable EmptyGeneralCatchClause
                catch
                // ReSharper restore EmptyGeneralCatchClause
                {
                }
            }
            _sectorCache.TryAdd(newPosition, lazyTemp);
            _spaceAllocatedInTransaction.TryInclude((ulong)newPosition, (ulong)unallocatedSector.Length);
            UnlinkFromUnallocatedSectors(unallocatedSector);
            LinkToTailOfDirtySectors(unallocatedSector);
            if (unallocatedSector.Parent != null)
            {
                PackUnpack.PackUInt64LE(unallocatedSector.Parent.Data, ofsInParent, (ulong)newPosition);
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
            if (_newState.RootAllocPage.Ptr == 0)
            {
                var result = (long)_newState.WantedDatabaseLength;
                _newState.WantedDatabaseLength += (ulong)size;
                _newState.UsedSize += (ulong)size;
                return result;
            }
            ReuseReusables();
            Debug.Assert(_inSpaceAllocation == false);
            _inSpaceAllocation = true;
            try
            {
                var totalGrans = (long)(_newState.WantedDatabaseLength / AllocationGranularity);
                if (_nextAllocStartInGrans >= totalGrans)
                {
                    _nextAllocStartInGrans = 0;
                }
                long posInGrans = -1;
                if (_nextAllocStartInGrans > 0)
                {
                    posInGrans = AllocBitsInAlloc(grans, ref _newState.RootAllocPage, totalGrans, 0, null, _nextAllocStartInGrans);
                }
                if (posInGrans < 0)
                {
                    posInGrans = AllocBitsInAlloc(grans, ref _newState.RootAllocPage, totalGrans, 0, null);
                }
                if (posInGrans < 0)
                {
                    int childSectors;
                    long gransInChild = GetGransInDownLevel(totalGrans, out childSectors);
                    var newParentSector = NewSector();
                    newParentSector.Type = SectorType.AllocParent;
                    newParentSector.SetLengthWithRound(2 * PtrDownSize);
                    var newLeafSector = NewSector();
                    newLeafSector.Type = SectorType.AllocChild;
                    newLeafSector.SetLengthWithRound((grans + 7) / 8);
                    newLeafSector.Parent = newParentSector;
                    BitArrayManipulation.SetBits(newLeafSector.Data, 0, grans);
                    SectorPtr.Pack(newParentSector.Data, 0, _newState.RootAllocPage);
                    SectorPtr.Pack(newParentSector.Data, PtrDownSize, newLeafSector.ToSectorPtr());
                    PublishSector(newLeafSector);
                    PublishSector(newParentSector);
                    FixChildParentPointer(_newState.RootAllocPage.Ptr, newParentSector);
                    _newState.RootAllocPage = newParentSector.ToSectorPtr();
                    posInGrans = gransInChild * childSectors;
                }
                _nextAllocStartInGrans = posInGrans + grans;
                if (posInGrans + grans >= totalGrans)
                {
                    _newState.WantedDatabaseLength = (ulong)(posInGrans + grans) * AllocationGranularity;
                }
                _newState.UsedSize += (ulong)size;
                return posInGrans * AllocationGranularity;
            }
            finally
            {
                FinishPostponedSpaceDeallocation();
            }
        }

        void ReuseReusables()
        {
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
        }

        long AllocBitsInAlloc(int grans, ref SectorPtr sectorPtr, long totalGrans, ulong startInBytes, Sector parent, long startSearchInGrans)
        {
            Sector sector;
            long startGran;
            if (totalGrans <= MaxLeafAllocSectorGrans)
            {
                var longestFree = _freeSpaceAllocatorOptimizer.QueryLongestForGran(startInBytes);
                if (longestFree < grans) return -1;
                sector = TryGetSector(sectorPtr.Ptr);
                if (sector == null)
                {
                    sector = ReadSector(sectorPtr, true);
                    sector.Type = SectorType.AllocChild;
                }
                sector.Parent = parent;
                int startGranSearch = 0;
                while (true)
                {
                    startGran = BitArrayManipulation.IndexOfFirstHole(sector.Data, grans, startGranSearch);
                    if (startGran < 0)
                    {
                        if (sector.Length == MaxLeafAllocSectorSize)
                        {
                            _freeSpaceAllocatorOptimizer.UpdateLongestForGran(startInBytes, grans - 1);
                        }
                        return -1;
                    }
                    ulong checkStartInBytes = startInBytes + (ulong)startGran * AllocationGranularity;
                    ulong foundFreeInBytes = _spaceUsedByReadOnlyTransactions.FindFreeSizeAfter(checkStartInBytes, (ulong)grans * AllocationGranularity);
                    if (checkStartInBytes == foundFreeInBytes) break;
                    ulong newStartGranSearch = (foundFreeInBytes - startInBytes) / AllocationGranularity;
                    if (newStartGranSearch > MaxLeafAllocSectorGrans)
                    {
                        _freeSpaceAllocatorOptimizer.UpdateLongestForGran(startInBytes, grans - 1);
                        return -1;
                    }
                    startGranSearch = (int)newStartGranSearch;
                }
                sector = DirtizeSector(sector, sector.Parent, null);
                BitArrayManipulation.SetBits(sector.Data, (int)startGran, grans);
                sectorPtr = sector.ToSectorPtr();
                return startGran;
            }
            int childSectors;
            long gransInChild = GetGransInDownLevel(totalGrans, out childSectors);
            sector = TryGetSector(sectorPtr.Ptr);
            if (sector == null)
            {
                sector = ReadSector(sectorPtr, true);
                sector.Type = SectorType.AllocParent;
            }
            sector.Parent = parent;
            sector = DirtizeSector(sector, sector.Parent, null);
            sectorPtr = sector.ToSectorPtr();
            var i = 0;
            long startingGranOfChild;
            SectorPtr childSectorPtr;
            if (startSearchInGrans > 0)
            {
                i = (int)(startSearchInGrans / gransInChild);
                Debug.Assert(i < childSectors);
            }
            for (; i < childSectors - 1; i++)
            {
                startingGranOfChild = i * gransInChild;
                childSectorPtr = SectorPtr.Unpack(sector.Data, i * PtrDownSize);
                startGran = AllocBitsInAlloc(grans, ref childSectorPtr, gransInChild, startInBytes + (ulong)(i * gransInChild * AllocationGranularity), sector, startSearchInGrans - startingGranOfChild);
                if (startGran < 0) continue;
                SectorPtr.Pack(sector.Data, i * PtrDownSize, childSectorPtr);
                startGran += startingGranOfChild;
                return startGran;
            }
            startingGranOfChild = i * gransInChild;
            childSectorPtr = SectorPtr.Unpack(sector.Data, i * PtrDownSize);
            startGran = AllocBitsInAlloc(grans, ref childSectorPtr, totalGrans - startingGranOfChild, startInBytes + (ulong)(i * gransInChild * AllocationGranularity), sector, startSearchInGrans - startingGranOfChild);
            if (startGran >= 0)
            {
                SectorPtr.Pack(sector.Data, i * PtrDownSize, childSectorPtr);
                startGran += startingGranOfChild;
                return startGran;
            }
            return -1;
        }

        long AllocBitsInAlloc(int grans, ref SectorPtr sectorPtr, long totalGrans, ulong startInBytes, Sector parent)
        {
            long startGran;
            Sector sector;
            if (totalGrans <= MaxLeafAllocSectorGrans)
            {
                var longestFree = _freeSpaceAllocatorOptimizer.QueryLongestForGran(startInBytes);
                if (longestFree < grans) return -1;
                sector = TryGetSector(sectorPtr.Ptr);
                if (sector == null)
                {
                    sector = ReadSector(sectorPtr, true);
                    sector.Type = SectorType.AllocChild;
                }
                sector.Parent = parent;
                int startGranSearch = 0;
                while (true)
                {
                    startGran = BitArrayManipulation.IndexOfFirstHole(sector.Data, grans, startGranSearch);
                    if (startGran < 0)
                    {
                        if (sector.Length < MaxLeafAllocSectorSize)
                        {
                            var oldData = sector.Data;
                            sector = ResizeSectorNoUpdatePosition(sector, sector.Length + AllocationGranularity, sector.Parent, null);
                            Array.Copy(oldData, 0, sector.Data, 0, oldData.Length);
                            sectorPtr.Ptr = sector.Position;
                            continue;
                        }
                        _freeSpaceAllocatorOptimizer.UpdateLongestForGran(startInBytes, grans - 1);
                        return -1;
                    }
                    ulong checkStartInBytes = startInBytes + (ulong)startGran * AllocationGranularity;
                    ulong foundFreeInBytes = _spaceUsedByReadOnlyTransactions.FindFreeSizeAfter(checkStartInBytes, (ulong)grans * AllocationGranularity);
                    if (checkStartInBytes == foundFreeInBytes) break;
                    ulong newStartGranSearch = (foundFreeInBytes - startInBytes) / AllocationGranularity;
                    if (newStartGranSearch > MaxLeafAllocSectorGrans)
                    {
                        _freeSpaceAllocatorOptimizer.UpdateLongestForGran(startInBytes, grans - 1);
                        return -1;
                    }
                    startGranSearch = (int)newStartGranSearch;
                }
                sector = DirtizeSector(sector, sector.Parent, null);
                BitArrayManipulation.SetBits(sector.Data, (int)startGran, grans);
                sectorPtr = sector.ToSectorPtr();
                return startGran;
            }
            int childSectors;
            long gransInChild = GetGransInDownLevel(totalGrans, out childSectors);
            sector = TryGetSector(sectorPtr.Ptr);
            if (sector == null)
            {
                sector = ReadSector(sectorPtr, true);
                sector.Type = SectorType.AllocParent;
            }
            sector.Parent = parent;
            sector = DirtizeSector(sector, sector.Parent, null);
            sectorPtr = sector.ToSectorPtr();
            var i = 0;
            long startingGranOfChild;
            SectorPtr childSectorPtr;
            for (; i < childSectors - 1; i++)
            {
                startingGranOfChild = i * gransInChild;
                childSectorPtr = SectorPtr.Unpack(sector.Data, i * PtrDownSize);
                startGran = AllocBitsInAlloc(grans, ref childSectorPtr, gransInChild, startInBytes + (ulong)(i * gransInChild * AllocationGranularity), sector);
                if (startGran < 0) continue;
                SectorPtr.Pack(sector.Data, i * PtrDownSize, childSectorPtr);
                startGran += startingGranOfChild;
                return startGran;
            }
            startingGranOfChild = i * gransInChild;
            childSectorPtr = SectorPtr.Unpack(sector.Data, i * PtrDownSize);
            startGran = AllocBitsInAlloc(grans, ref childSectorPtr, totalGrans - startingGranOfChild, startInBytes + (ulong)(i * gransInChild * AllocationGranularity), sector);
            if (startGran >= 0)
            {
                SectorPtr.Pack(sector.Data, i * PtrDownSize, childSectorPtr);
                startGran += startingGranOfChild;
                return startGran;
            }
            int childSectors2;
            long gransInChild2 = GetGransInDownLevel(totalGrans - startingGranOfChild, out childSectors2);
            Sector newLeafSector;
            if (gransInChild / MaxChildren != gransInChild2)
            {
                Debug.Assert(childSectors2 == MaxChildren);
                var newParentSector = NewSector();
                newParentSector.Type = SectorType.AllocParent;
                newParentSector.SetLengthWithRound(2 * PtrDownSize);
                newParentSector.Parent = sector;
                newLeafSector = NewSector();
                newLeafSector.Type = SectorType.AllocChild;
                newLeafSector.SetLengthWithRound((grans + 7) / 8);
                newLeafSector.Parent = newParentSector;
                BitArrayManipulation.SetBits(newLeafSector.Data, 0, grans);
                SectorPtr.Pack(newParentSector.Data, 0, childSectorPtr);
                SectorPtr.Pack(newParentSector.Data, PtrDownSize, newLeafSector.ToSectorPtr());
                PublishSector(newLeafSector);
                PublishSector(newParentSector);
                SectorPtr.Pack(sector.Data, i * PtrDownSize, newParentSector.ToSectorPtr());
                var childSector = TryGetSectorNoUpdateAccess(childSectorPtr.Ptr);
                if (childSector != null)
                {
                    childSector.Parent = newParentSector;
                }
                return startingGranOfChild + gransInChild2 * MaxChildren;
            }
            if (childSectors == MaxChildren) return -1;
            if (sector.Length < (childSectors + 1) * PtrDownSize)
            {
                var oldData = sector.Data;
                sector = ResizeSectorNoUpdatePosition(sector, (childSectors + 1) * PtrDownSize, sector.Parent, null);
                sectorPtr = sector.ToSectorPtr();
                Array.Copy(oldData, 0, sector.Data, 0, oldData.Length);
                for (int j = 0; j < childSectors; j++)
                {
                    FixChildParentPointer(PackUnpack.UnpackInt64LE(sector.Data, j * PtrDownSize), sector);
                }
            }
            newLeafSector = NewSector();
            newLeafSector.Type = SectorType.AllocChild;
            newLeafSector.SetLengthWithRound((grans + 7) / 8);
            newLeafSector.Parent = sector;
            BitArrayManipulation.SetBits(newLeafSector.Data, 0, grans);
            SectorPtr.Pack(sector.Data, childSectors * PtrDownSize, newLeafSector.ToSectorPtr());
            PublishSector(newLeafSector);
            return gransInChild * childSectors;
        }

        void UnlinkFromUnallocatedSectors(Sector unallocatedSector)
        {
            Debug.Assert(_unallocatedSectorCount > 0);
            _unallocatedSectorCount--;
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
            Debug.Assert(_dirtySectorCount > 0);
            _dirtySectorCount--;
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
            Debug.Assert(_inTransactionSectorCount > 0);
            _inTransactionSectorCount--;
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
                    _freeSpaceAllocatorOptimizer.RollbackWriteTransaction();
                    SwapCurrentAndNewState();
                    TransferNewStateToCurrentState();
                    _commitNeeded = false;
                    _spaceUsedByReadOnlyTransactions.UnmergeInPlace(_spaceDeallocatedInTransaction);
                    _spaceAllocatedInTransaction.Clear();
                    _spaceDeallocatedInTransaction.Clear();
                    while (_unallocatedSectorHeadLink != null)
                    {
                        LowLevelRemoveFromSectorCache(_unallocatedSectorHeadLink);
                        UnlinkFromUnallocatedSectors(_unallocatedSectorHeadLink);
                    }
                    while (_dirtySectorHeadLink != null)
                    {
                        LowLevelRemoveFromSectorCache(_dirtySectorHeadLink);
                        UnlinkFromDirtySectors(_dirtySectorHeadLink);
                    }
                    while (_inTransactionSectorHeadLink != null)
                    {
                        LowLevelRemoveFromSectorCache(_inTransactionSectorHeadLink);
                        DetransactionalizeSector(_inTransactionSectorHeadLink);
                    }
                }
                //Debug.Assert(_sectorCache.Keys.ToList().Exists(l => l < 0) == false);
            }
            finally
            {
                DereferenceReadLink(_readTrLinkHead);
                lock (_readLinkLock)
                {
                    _writeTr = null;
                }
            }
            if (!_writtingQueue.IsEmpty) TryToRunNextWrittingTransaction();
        }

        void TryToRunNextWrittingTransaction()
        {
            TaskCompletionSource<IKeyValueDBTransaction> result;
            lock (_readLinkLock)
            {
                if (_writeTr != null) return;
                if (_writeTrInCreation) return;
                if (!_writtingQueue.TryDequeue(out result)) return;
                _writeTrInCreation = true;
            }
            try
            {
                var tr = (KeyValueDBTransaction)StartTransaction();
                tr.SafeUpgradeToWriteTransaction();
                result.SetResult(tr);
            }
            catch (Exception e)
            {
                result.SetException(e);
            }
        }

        void LowLevelRemoveFromSectorCache(Sector sector)
        {
            Lazy<Sector> lazySector;
            if (_sectorCache.TryRemove(sector.Position,out lazySector))
            {
                Debug.Assert(lazySector.Value==sector);
                Interlocked.Add(ref _bytesInCache, -lazySector.Value.Length);
            }
        }

        internal void UpgradeTransactionToWriteOne(KeyValueDBTransaction transaction, ReadTrLink link)
        {
            lock (_readLinkLock)
            {
                if (link==null && _writeTrInCreation)
                {
                    _writeTrInCreation = false;
                }
                else
                {
                    if (_writeTr != null || _writeTrInCreation) throw new BTDBTransactionRetryException("Write transaction already running");
                    if (link.TransactionNumber != _currentState.TransactionCounter)
                        throw new BTDBTransactionRetryException("Newer write transaction already finished");
                }
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
            var result = new Sector { Dirty = true, InTransaction = true, InternalLastAccessTime = 0 };
            _unallocatedCounter--;
            result.Position = _unallocatedCounter * AllocationGranularity;
            return result;
        }

        internal void PublishSector(Sector newSector)
        {
            Debug.Assert(!_sectorCache.ContainsKey(newSector.Position));
            var lazy = new Lazy<Sector>(() => newSector).Force();

            UpdateLastAccess(newSector);
            if (_sectorCache.TryAdd(newSector.Position, lazy))
            {
                Interlocked.Add(ref _bytesInCache, newSector.Length);
            }
            else
            {
                throw new BTDBException("Internal error: Republishing already published sector");
            }
            _commitNeeded = true;
            LinkToTailOfUnallocatedSectors(newSector);
            _keyValueDBTweaks.NewSectorAddedToCache(newSector, _sectorCache.Count, _bytesInCache);
        }

        internal void UpdateLastAccess(Sector sector)
        {
            Interlocked.Exchange(ref sector.InternalLastAccessTime, Interlocked.Increment(ref _currentCacheTime));
        }

        void LinkToTailOfUnallocatedSectors(Sector newSector)
        {
            _unallocatedSectorCount++;
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
            _dirtySectorCount++;
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
            _inTransactionSectorCount++;
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

        internal KeyValueDBStats CalculateStats(ReadTrLink readLink)
        {
            var result = new KeyValueDBStats
                             {
                                 DatabaseStreamSize = _positionLessStream.GetSize(),
                                 TotalBytesRead = _totalBytesRead,
                                 TotalBytesWritten = _totalBytesWritten
                             };
            if (readLink != null)
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

        internal void FixChildParentPointer(long childSectorPtr, Sector parent)
        {
            var sector = TryGetSectorNoUpdateAccess(childSectorPtr);
            if (sector != null)
            {
                if (sector.InTransaction)
                {
                    sector.Parent = parent;
                }
            }
        }
    }
}
