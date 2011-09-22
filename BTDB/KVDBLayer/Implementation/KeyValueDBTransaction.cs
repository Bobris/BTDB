using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using BTDB.StreamLayer;

namespace BTDB.KVDBLayer
{
    internal sealed class KeyValueDBTransaction : IKeyValueDBTransaction
    {
        readonly KeyValueDB _owner;

        // if this is null then this transaction is writing kind
        KeyValueDB.ReadTrLink _readLink;
        Sector _currentKeySector;
        readonly List<Sector> _currentKeySectorParents = new List<Sector>();
        int _currentKeyIndexInLeaf;
        long _currentKeyIndex;
        byte[] _prefix;
        long _prefixKeyStart;
        long _prefixKeyCount;
        static readonly byte[] EmptyByteArray = new byte[0];

        internal KeyValueDBTransaction(KeyValueDB owner, KeyValueDB.ReadTrLink readLink)
        {
            _owner = owner;
            _readLink = readLink;
            _currentKeySector = null;
            _currentKeyIndexInLeaf = -1;
            _currentKeyIndex = -1;
            _prefix = EmptyByteArray;
            _prefixKeyStart = 0;
            _prefixKeyCount = (long)_readLink.KeyValuePairCount;
        }

        public void Dispose()
        {
            InvalidateCurrentKey();
            if (_readLink != null)
            {
                _owner.DisposeReadTransaction(_readLink);
            }
            else
            {
                _owner.DisposeWriteTransaction();
            }
        }

        internal void SafeUpgradeToWriteTransaction()
        {
            Debug.Assert(_currentKeySector == null);
            Debug.Assert(!IsWritting());
            _owner.UpgradeTransactionToWriteOne(this, null);
            _readLink = null;
        }

        private void UpgradeToWriteTransaction()
        {
            if (IsWritting()) return;
            _owner.UpgradeTransactionToWriteOne(this, _readLink);
            _readLink = null;
            var sector = _currentKeySector;
            for (int i = _currentKeySectorParents.Count - 1; i >= 0; i--)
            {
                var sectorParent = _currentKeySectorParents[i];
                sector.Parent = sectorParent;
                sector = sectorParent;
            }
        }

        public void SetKeyPrefix(byte[] prefix, int prefixOfs, int prefixLen)
        {
            _prefix = EmptyByteArray;
            if (prefixLen == 0)
            {
                _prefixKeyStart = 0;
                _prefixKeyCount = (long)(IsWritting() ? _owner.NewState.KeyValuePairCount : _readLink.KeyValuePairCount);
                InvalidateCurrentKey();
                return;
            }
            _prefixKeyStart = 0;
            FindKey(prefix, prefixOfs, prefixLen, FindKeyStrategy.OnlyNext);
            _prefixKeyStart = _currentKeyIndex;
            _prefixKeyCount = 0;
            if (GetKeySize() >= prefixLen)
            {
                var buf = new byte[prefixLen];
                ReadKey(0, prefixLen, buf, 0);
                if (BitArrayManipulation.CompareByteArray(prefix, prefixOfs, prefixLen, buf, 0, prefixLen) == 0)
                {
                    _prefixKeyCount = -1;
                }
                _prefix = buf;
            }
            else
            {
                _prefix = new byte[prefixLen];
            }
            Array.Copy(prefix, prefixOfs, _prefix, 0, prefixLen);
            InvalidateCurrentKey();
        }

        public void InvalidateCurrentKey()
        {
            _currentKeyIndexInLeaf = -1;
            UnlockCurrentKeySector();
            _currentKeyIndex = -1;
        }

        void UnlockCurrentKeySector()
        {
            _currentKeySector = null;
            _currentKeySectorParents.Clear();
        }

        public bool FindFirstKey()
        {
            if (_prefixKeyCount == 0)
            {
                InvalidateCurrentKey();
                return false;
            }
            FindKey(EmptyByteArray, 0, 0, FindKeyStrategy.OnlyNext);
            return true;
        }

        public bool FindLastKey()
        {
            if (_prefixKeyCount == 0)
            {
                InvalidateCurrentKey();
                return false;
            }
            FindKey(EmptyByteArray, -1, 0, FindKeyStrategy.OnlyPrevious);
            _prefixKeyCount = _currentKeyIndex - _prefixKeyStart + 1;
            return true;
        }

        public bool FindPreviousKey()
        {
            if (_currentKeyIndexInLeaf < 0) throw new BTDBException("Current Key is invalid");
            if (_currentKeyIndex == _prefixKeyStart) return false;
            try
            {
                if (_currentKeyIndexInLeaf > 0)
                {
                    _currentKeyIndexInLeaf--;
                    _currentKeyIndex--;
                    _owner.UpdateLastAccess(_currentKeySector);
                    return true;
                }
                var sector = _currentKeySector;
                var parent = PopCurrentKeyParent();
                while (parent != null)
                {
                    var iter = new BTreeParentIterator(parent.Data);
                    var childByPos = iter.FindChildByPos(sector.Position);
                    if (childByPos == 0)
                    {
                        sector = parent;
                        parent = PopCurrentKeyParent();
                        continue;
                    }
                    var childSectorPtr = iter.GetChildSectorPtr(childByPos - 1);
                    while (true)
                    {
                        sector = LoadBTreeSector(childSectorPtr);
                        if (sector.Type == SectorType.BTreeChild)
                        {
                            _currentKeyIndexInLeaf = (int)(BTreeChildIterator.CountFromSectorData(sector.Data) - 1);
                            _currentKeyIndex--;
                            return true;
                        }
                        iter = new BTreeParentIterator(sector.Data);
                        childSectorPtr = iter.GetChildSectorPtr(iter.Count);
                    }
                }
                throw new BTDBException("Internal error");
            }
            catch
            {
                InvalidateCurrentKey();
                throw;
            }
        }

        Sector PopCurrentKeyParent()
        {
            var count = _currentKeySectorParents.Count;
            if (count == 0) return null;
            var parent = _currentKeySectorParents[count - 1];
            _currentKeySectorParents.RemoveAt(count - 1);
            _currentKeySector = parent;
            return parent;
        }

        public bool FindNextKey()
        {
            if (_currentKeyIndexInLeaf < 0) throw new BTDBException("Current Key is invalid");
            if (_prefixKeyCount != -1 && _currentKeyIndex + 1 >= _prefixKeyStart + _prefixKeyCount) return false;
            try
            {
                if (_currentKeyIndexInLeaf + 1 < BTreeChildIterator.CountFromSectorData(_currentKeySector.Data))
                {
                    _owner.UpdateLastAccess(_currentKeySector);
                    _currentKeyIndexInLeaf++;
                    if (CheckPrefix())
                    {
                        _currentKeyIndex++;
                        return true;
                    }
                    _prefixKeyCount = _currentKeyIndex - _prefixKeyStart + 1;
                    _currentKeyIndexInLeaf--;
                    return false;
                }
                var sector = _currentKeySector;
                var parent = PopCurrentKeyParent();
                if (parent == null)
                {
                    _prefixKeyCount = _currentKeyIndex - _prefixKeyStart + 1;
                    FindLastKey();
                    return false;
                }
                while (true)
                {
                    var iter = new BTreeParentIterator(parent.Data);
                    var childByPos = iter.FindChildByPos(sector.Position);
                    if (childByPos == iter.Count)
                    {
                        sector = parent;
                        parent = PopCurrentKeyParent();
                        if (parent == null)
                        {
                            _prefixKeyCount = _currentKeyIndex - _prefixKeyStart + 1;
                            FindLastKey();
                            return false;
                        }
                        continue;
                    }
                    var childSectorPtr = iter.GetChildSectorPtr(childByPos + 1);
                    while (true)
                    {
                        sector = LoadBTreeSector(childSectorPtr);
                        if (sector.Type == SectorType.BTreeChild)
                        {
                            _currentKeyIndexInLeaf = 0;
                            _currentKeyIndex++;
                            if (CheckPrefix())
                            {
                                return true;
                            }
                            _prefixKeyCount = _currentKeyIndex - _prefixKeyStart;
                            FindPreviousKey();
                            return false;
                        }
                        iter = new BTreeParentIterator(sector.Data);
                        childSectorPtr = iter.GetChildSectorPtr(0);
                    }
                }
            }
            catch
            {
                InvalidateCurrentKey();
                throw;
            }
        }

        bool CheckPrefix()
        {
            if (_prefix.Length == 0) return true;
            var backupPrefix = _prefix;
            _prefix = EmptyByteArray;
            try
            {
                if (GetKeySize() < backupPrefix.Length)
                    return false;
                var prefixLen = backupPrefix.Length;
                var buf = new byte[prefixLen];
                ReadKey(0, prefixLen, buf, 0);
                if (BitArrayManipulation.CompareByteArray(backupPrefix, 0, prefixLen, buf, 0, prefixLen) == 0)
                    return true;
                return false;
            }
            finally
            {
                _prefix = backupPrefix;
            }
        }

        public FindKeyResult FindKey(byte[] keyBuf, int keyOfs, int keyLen, FindKeyStrategy strategy)
        {
            return FindKey(keyBuf, keyOfs, keyLen, strategy, null, 0, -1);
        }

        private FindKeyResult FindKey(byte[] keyBuf, int keyOfs, int keyLen, FindKeyStrategy strategy, byte[] valueBuf, int valueOfs, int valueLen)
        {
            if (keyLen < 0) throw new ArgumentOutOfRangeException("keyLen");
            if (strategy == FindKeyStrategy.Create) UpgradeToWriteTransaction();
            InvalidateCurrentKey();
            var rootBTree = GetRootBTreeSectorPtr();
            if (rootBTree.Ptr == 0)
            {
                return FindKeyInEmptyBTree(keyBuf, keyOfs, keyLen, strategy, valueBuf, valueOfs, valueLen);
            }
            Sector sector;
            try
            {
                long keyIndex = 0;
                while (true)
                {
                    sector = LoadBTreeSector(rootBTree);
                    if (sector.Type == SectorType.BTreeChild) break;
                    var iterParent = new BTreeParentIterator(sector.Data);
                    int bindexParent = iterParent.BinarySearch(_prefix, keyBuf, keyOfs, keyLen, sector, SectorDataCompare);
                    rootBTree = iterParent.GetChildSectorPtr((bindexParent + 1) / 2, ref keyIndex);
                }
                var iter = new BTreeChildIterator(sector.Data);
                int bindex = iter.BinarySearch(_prefix, keyBuf, keyOfs, keyLen, sector, SectorDataCompare);
                _currentKeyIndexInLeaf = bindex / 2;
                _currentKeyIndex = keyIndex + _currentKeyIndexInLeaf;
                if ((bindex & 1) != 0)
                {
                    return FindKeyResult.FoundExact;
                }
                if (strategy != FindKeyStrategy.Create)
                {
                    return FindKeyNoncreateStrategy(strategy, iter);
                }
                int additionalLengthNeeded = BTreeChildIterator.HeaderForEntry +
                    (valueLen >= 0 ? BTreeChildIterator.CalcEntrySize(_prefix.Length + keyLen, valueLen) :
                    BTreeChildIterator.CalcEntrySize(_prefix.Length + keyLen));
                if (KeyValueDB.ShouldSplitBTreeChild(iter.TotalLength, additionalLengthNeeded, iter.Count))
                {
                    SplitBTreeChild(keyBuf, keyOfs, keyLen, valueBuf, valueOfs, valueLen, sector, iter, additionalLengthNeeded);
                }
                else
                {
                    var origsector = sector;
                    sector = _owner.ResizeSectorWithUpdatePosition(sector, iter.TotalLength + additionalLengthNeeded,
                                                                   sector.Parent, _currentKeySectorParents);
                    if (sector != origsector)
                    {
                        Array.Copy(origsector.Data, sector.Data, origsector.Data.Length);
                        _owner.FixChildrenParentPointers(sector);
                    }
                    _currentKeySector = sector;
                    int insertOfs = iter.AddEntry(additionalLengthNeeded, sector.Data, _currentKeyIndexInLeaf);
                    IncrementChildCountInBTreeParents(sector);
                    SetBTreeChildKeyData(sector, keyBuf, keyOfs, keyLen, valueBuf, valueOfs, valueLen, insertOfs);
                }
                _owner.NewState.KeyValuePairCount++;
                if (_prefixKeyCount != -1) _prefixKeyCount++;
                return FindKeyResult.Created;
            }
            catch
            {
                InvalidateCurrentKey();
                throw;
            }
        }

        public bool CreateOrUpdateKeyValue(byte[] keyBuf, int keyOfs, int keyLen, byte[] valueBuf, int valueOfs, int valueLen)
        {
            if (FindKey(keyBuf, keyOfs, keyLen, FindKeyStrategy.Create, valueBuf, valueOfs, valueLen) == FindKeyResult.Created)
                return true;
            SetValue(valueBuf, valueOfs, valueLen);
            return false;
        }

        void SplitBTreeChild(byte[] keyBuf, int keyOfs, int keyLen, byte[] valueBuf, int valueOfs, int valueLen, Sector sector, BTreeChildIterator iter, int additionalLengthNeeded)
        {
            int middleoffset = (iter.TotalLength + iter.FirstOffset + additionalLengthNeeded) / 2;
            iter.MoveFirst();
            bool beforeNew = true;
            int splitIndex = 0;
            int currentPos = iter.FirstOffset;
            while (currentPos < middleoffset)
            {
                if (beforeNew && splitIndex == _currentKeyIndexInLeaf)
                {
                    beforeNew = false;
                    currentPos += additionalLengthNeeded - BTreeChildIterator.HeaderForEntry;
                }
                else
                {
                    currentPos += iter.CurrentEntrySize;
                    splitIndex++;
                    iter.MoveNext();
                }
            }
            var rightSector = _owner.NewSector();
            rightSector.Type = SectorType.BTreeChild;
            int rightCount = iter.Count - splitIndex + (beforeNew ? 1 : 0);
            rightSector.SetLengthWithRound(BTreeChildIterator.HeaderSize + rightCount * BTreeChildIterator.HeaderForEntry + iter.TotalLength + additionalLengthNeeded - BTreeChildIterator.HeaderForEntry - currentPos);
            BTreeChildIterator.SetCountToSectorData(rightSector.Data, rightCount);
            rightSector.Parent = sector.Parent;
            int leftCount = splitIndex + (beforeNew ? 0 : 1);
            var leftSector = _owner.ResizeSectorWithUpdatePosition(sector, BTreeChildIterator.HeaderSize + leftCount * BTreeChildIterator.HeaderForEntry + currentPos - iter.FirstOffset, sector.Parent,
                                                                      _currentKeySectorParents);
            _currentKeySector = leftSector;
            Sector newKeySector;
            BTreeChildIterator.SetCountToSectorData(leftSector.Data, leftCount);
            int newItemPos = iter.OffsetOfIndex(_currentKeyIndexInLeaf);
            int setActualDataPos;
            if (beforeNew)
            {
                Array.Copy(iter.Data, iter.FirstOffset, leftSector.Data, BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry * leftCount, currentPos - iter.FirstOffset);
                Array.Copy(iter.Data, currentPos, rightSector.Data, BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry * rightCount, newItemPos - currentPos);
                int rightPos = BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry * rightCount + newItemPos - currentPos;
                setActualDataPos = rightPos;
                SetBTreeChildKeyDataJustLengths(rightSector, keyLen, valueLen, rightPos);
                rightPos += additionalLengthNeeded - BTreeChildIterator.HeaderForEntry;
                Array.Copy(iter.Data, newItemPos, rightSector.Data, rightPos, iter.TotalLength - newItemPos);
                newKeySector = rightSector;
                _currentKeyIndexInLeaf -= splitIndex;
            }
            else
            {
                Array.Copy(iter.Data, iter.FirstOffset, leftSector.Data, BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry * leftCount, newItemPos - iter.FirstOffset);
                int leftPosInsert = BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry * leftCount + newItemPos - iter.FirstOffset;
                int leftPos = leftPosInsert;
                leftPos += additionalLengthNeeded - BTreeChildIterator.HeaderForEntry;
                Array.Copy(iter.Data, currentPos - additionalLengthNeeded + BTreeChildIterator.HeaderForEntry, rightSector.Data, BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry * rightCount,
                           iter.TotalLength + additionalLengthNeeded - BTreeChildIterator.HeaderForEntry - currentPos);
                Array.Copy(iter.Data, newItemPos, leftSector.Data, leftPos, currentPos - newItemPos - additionalLengthNeeded + BTreeChildIterator.HeaderForEntry);
                setActualDataPos = leftPosInsert;
                SetBTreeChildKeyDataJustLengths(leftSector, keyLen, valueLen, leftPosInsert);
                newKeySector = leftSector;
            }
            BTreeChildIterator.RecalculateHeader(leftSector.Data, leftCount);
            BTreeChildIterator.RecalculateHeader(rightSector.Data, rightCount);
            _owner.FixChildrenParentPointers(leftSector);
            _owner.PublishSector(rightSector, true);
            Interlocked.Increment(ref leftSector.ChildrenInCache);
            Interlocked.Increment(ref rightSector.ChildrenInCache);
            try
            {
                _owner.TruncateSectorCache(true, 0);
                SetBTreeChildKeyData(newKeySector, keyBuf, keyOfs, keyLen, valueBuf, valueOfs, valueLen, setActualDataPos);
                if (leftSector.Parent == null)
                {
                    CreateBTreeParentFromTwoLeafs(leftSector, rightSector);
                }
                else
                {
                    iter = new BTreeChildIterator(rightSector.Data);
                    iter.MoveFirst();
                    if (iter.HasKeySectorPtr) ForceKeyFlush(iter.KeySectorPtr, iter.KeyLen - iter.KeyLenInline, rightSector);
                    int keyLenInSector = iter.KeyLenInline + (iter.HasKeySectorPtr ? KeyValueDB.PtrDownSize : 0);
                    AddToBTreeParent(leftSector, rightSector, iter.Data, iter.KeyLen, iter.KeyOffset,
                                     keyLenInSector);
                }
            }
            finally
            {
                Interlocked.Decrement(ref leftSector.ChildrenInCache);
                Interlocked.Decrement(ref rightSector.ChildrenInCache);
            }
            UnlockUselessAndFixKeySectorParents(newKeySector, leftSector, rightSector);
            _currentKeySector = newKeySector;
        }

        void ForceKeyFlush(SectorPtr keySectorPtr, int keySize, Sector parent)
        {
            // Because parent of sector could be just one and we are going to point to same sector from 2 places it needs to be forcibly flushed
            ForceFlushContentSector(keySectorPtr, keySize, parent);
        }

        void ForceFlushContentSector(SectorPtr sectorPtr, long len, Sector parent)
        {
            var sector = _owner.TryGetSector(sectorPtr.Ptr, true, parent);
            if (len <= KeyValueDB.MaxLeafDataSectorSize)
            {
                if (sector == null) return;
                _owner.ForceFlushSector(sector);
                return;
            }
            int downPtrCount;
            long bytesInDownLevel = KeyValueDB.GetBytesInDownLevel(len, out downPtrCount);
            sector = sector ?? _owner.ReadSector(sectorPtr, true, SectorTypeInit.DataParent, parent);
            for (int i = 0; i < downPtrCount; i++)
            {
                var downSectorPtr = SectorPtr.Unpack(sector.Data, i * KeyValueDB.PtrDownSize);
                ForceFlushContentSector(downSectorPtr, Math.Min(len, bytesInDownLevel), sector);
                len -= bytesInDownLevel;
            }
            _owner.ForceFlushSector(sector);
        }

        void UnlockUselessAndFixKeySectorParents(Sector newKeySector, Sector leftSector, Sector rightSector)
        {
            if (newKeySector != leftSector)
            {
                var pi = _currentKeySectorParents.Count;
                do
                {
                    if (pi < _currentKeySectorParents.Count)
                    {
                        Debug.Assert(_currentKeySectorParents[pi] == leftSector);
                        _currentKeySectorParents[pi] = rightSector;
                    }
                    leftSector = leftSector.Parent;
                    rightSector = rightSector.Parent;
                    pi--;
                } while (leftSector != rightSector);
            }
        }

        public long GetKeyValueCount()
        {
            if (_prefixKeyCount != -1)
            {
                InvalidateCurrentKey();
                return _prefixKeyCount;
            }
            FindLastKey();
            InvalidateCurrentKey();
            return _prefixKeyCount;
        }

        public long GetKeyIndex()
        {
            if (_currentKeyIndex == -1) return -1;
            return _currentKeyIndex - _prefixKeyStart;
        }

        Sector LoadBTreeSector(SectorPtr sectorPtr)
        {
            Sector sector = _owner.GetOrReadSector(sectorPtr, IsWritting(), SectorTypeInit.BTreeChildOrParent, _currentKeySector);
            if (_currentKeySector != null) _currentKeySectorParents.Add(_currentKeySector);
            _currentKeySector = sector;
            return sector;
        }

        void AddToBTreeParent(Sector leftSector, Sector rightSector, byte[] middleKeyData, int middleKeyLen, int middleKeyOfs, int middleKeyLenInSector)
        {
            var parentSector = leftSector.Parent;
            var iter = new BTreeParentIterator(parentSector.Data);
            int additionalLengthNeeded = BTreeParentIterator.HeaderForEntry + BTreeParentIterator.CalcEntrySize(middleKeyLen);
            int leftIndexInParent = iter.FindChildByPos(leftSector.Position);
            bool splitting = true;
            if (!KeyValueDB.ShouldSplitBTreeParent(iter.TotalLength, additionalLengthNeeded, iter.Count + 1))
            {
                parentSector = _owner.ResizeSectorWithUpdatePositionNoWriteTruncate(
                    parentSector,
                    iter.TotalLength + additionalLengthNeeded,
                    parentSector.Parent,
                    _currentKeySectorParents);
                splitting = false;
            }
            var mergedData = new byte[iter.TotalLength + additionalLengthNeeded];
            BTreeParentIterator.SetCountToSectorData(mergedData, iter.Count + 1);
            int ofs;
            int splitOfs = iter.OffsetOfIndex(leftIndexInParent);
            if (leftIndexInParent == 0)
            {
                SectorPtr.Pack(mergedData, BTreeParentIterator.FirstChildSectorPtrOffset, leftSector.ToSectorPtr());
                PackUnpack.PackUInt64LE(mergedData, BTreeParentIterator.FirstChildSectorPtrOffset + KeyValueDB.PtrDownSize, CalcKeysInSector(leftSector));
                ofs = BTreeParentIterator.HeaderSize + BTreeParentIterator.HeaderForEntry * (iter.Count + 1);
            }
            else
            {
                Array.Copy(iter.Data, BTreeParentIterator.FirstChildSectorPtrOffset, mergedData, BTreeParentIterator.FirstChildSectorPtrOffset, KeyValueDB.PtrDownSize + 8);
                ofs = BTreeParentIterator.HeaderSize + BTreeParentIterator.HeaderForEntry * (iter.Count + 1);
                int splitOfsPrev = iter.OffsetOfIndex(leftIndexInParent - 1);
                Array.Copy(iter.Data, iter.FirstOffset, mergedData, ofs, splitOfsPrev - iter.FirstOffset);
                ofs += splitOfsPrev - iter.FirstOffset;
                Array.Copy(iter.Data, splitOfsPrev, mergedData, ofs, 4 + KeyValueDB.PtrDownSize);
                ofs += 4 + KeyValueDB.PtrDownSize;
                PackUnpack.PackUInt64LE(mergedData, ofs, CalcKeysInSector(leftSector));
                ofs += 8;
                splitOfsPrev += 4 + KeyValueDB.PtrDownSize + 8;
                Array.Copy(iter.Data, splitOfsPrev, mergedData, ofs, splitOfs - splitOfsPrev);
                ofs += splitOfs - splitOfsPrev;
            }
            PackUnpack.PackInt32LE(mergedData, ofs, middleKeyLen);
            ofs += 4;
            SectorPtr.Pack(mergedData, ofs, rightSector.ToSectorPtr());
            ofs += KeyValueDB.PtrDownSize;
            PackUnpack.PackUInt64LE(mergedData, ofs, CalcKeysInSector(rightSector));
            ofs += 8;
            Array.Copy(middleKeyData, middleKeyOfs, mergedData, ofs, middleKeyLenInSector);
            ofs += middleKeyLenInSector;
            Array.Copy(iter.Data, splitOfs, mergedData, ofs, iter.TotalLength - splitOfs);
            BTreeParentIterator.RecalculateHeader(mergedData, iter.Count + 1);
            if (!splitting)
            {
                Array.Copy(mergedData, parentSector.Data, mergedData.Length);
                leftSector.Parent = parentSector;
                rightSector.Parent = parentSector;
                IncrementChildCountInBTreeParents(parentSector);
                _owner.FixChildrenParentPointers(parentSector);
            }
            else
            {
                iter = new BTreeParentIterator(mergedData);
                int middleoffset = mergedData.Length / 2;
                iter.MoveFirst();
                int splitIndex = 0;
                int currentPos = iter.FirstOffset;
                while (currentPos < middleoffset)
                {
                    currentPos += iter.CurrentEntrySize;
                    splitIndex++;
                    iter.MoveNext();
                }
                Sector leftParentSector = null;
                Sector rightParentSector = null;
                try
                {
                    rightParentSector = _owner.NewSector();
                    rightParentSector.Type = SectorType.BTreeParent;
                    var rightCount = iter.Count - splitIndex - 1;
                    var rightFirstOffset = BTreeParentIterator.HeaderSize + rightCount * BTreeParentIterator.HeaderForEntry;
                    rightParentSector.SetLengthWithRound(rightFirstOffset + mergedData.Length - iter.NextEntryOffset);
                    BTreeParentIterator.SetCountToSectorData(rightParentSector.Data, rightCount);
                    rightParentSector.Parent = parentSector.Parent;
                    var leftFirstOffset = BTreeParentIterator.HeaderSize + splitIndex * BTreeParentIterator.HeaderForEntry;
                    leftParentSector = _owner.ResizeSectorWithUpdatePosition(parentSector,
                                                                             leftFirstOffset + currentPos -
                                                                             iter.FirstOffset, parentSector.Parent,
                                                                             _currentKeySectorParents);
                    BTreeParentIterator.SetCountToSectorData(leftParentSector.Data, splitIndex);
                    leftSector.Parent = leftParentSector;
                    rightSector.Parent = leftParentSector;
                    Array.Copy(mergedData, BTreeParentIterator.FirstChildSectorPtrOffset, leftParentSector.Data,
                               BTreeParentIterator.FirstChildSectorPtrOffset,
                               KeyValueDB.PtrDownSize + 8 + BTreeParentIterator.HeaderForEntry * splitIndex);
                    Array.Copy(mergedData, iter.FirstOffset, leftParentSector.Data, leftFirstOffset,
                               currentPos - iter.FirstOffset);
                    Array.Copy(mergedData, iter.ChildSectorPtrOffset, rightParentSector.Data, BTreeParentIterator.FirstChildSectorPtrOffset, KeyValueDB.PtrDownSize + 8);
                    Array.Copy(mergedData, iter.NextEntryOffset, rightParentSector.Data, rightFirstOffset, iter.TotalLength - iter.NextEntryOffset);
                    BTreeParentIterator.RecalculateHeader(rightParentSector.Data, rightCount);
                    _owner.FixChildrenParentPointers(leftParentSector);
                    _owner.PublishSector(rightParentSector, true);
                    Interlocked.Increment(ref leftParentSector.ChildrenInCache);
                    Interlocked.Increment(ref rightParentSector.ChildrenInCache);
                    try
                    {
                        _owner.TruncateSectorCache(true, 0);
                        int keyLenInSector = iter.KeyLenInline + (iter.HasKeySectorPtr ? KeyValueDB.PtrDownSize : 0);
                        if (leftParentSector.Parent == null)
                        {
                            CreateBTreeParentFromTwoChildren(leftParentSector, rightParentSector, iter.Data, iter.KeyLen, iter.KeyOffset, keyLenInSector);
                        }
                        else
                        {
                            AddToBTreeParent(leftParentSector, rightParentSector, iter.Data, iter.KeyLen, iter.KeyOffset, keyLenInSector);
                        }
                    }
                    finally
                    {
                        Interlocked.Decrement(ref leftParentSector.ChildrenInCache);
                        Interlocked.Decrement(ref rightParentSector.ChildrenInCache);
                    }
                }
                finally
                {
                    if (leftSector.Parent == rightParentSector)
                    {
                        for (int i = 0; i < _currentKeySectorParents.Count; i++)
                        {
                            if (_currentKeySectorParents[i] == leftParentSector)
                            {
                                _currentKeySectorParents[i] = rightParentSector;
                                break;
                            }
                        }
                    }
                }
            }
        }

        void CreateBTreeParentFromTwoChildren(Sector leftSector, Sector rightSector, byte[] middleKeyData, int middleKeyLen, int middleKeyOfs, int middleKeyLenInSector)
        {
            Sector parentSector = _owner.NewSector();
            parentSector.Type = SectorType.BTreeParent;
            var entrySize = BTreeParentIterator.CalcEntrySize(middleKeyLen);
            parentSector.SetLengthWithRound(BTreeParentIterator.HeaderSize + BTreeParentIterator.HeaderForEntry + entrySize);
            BTreeParentIterator.SetCountToSectorData(parentSector.Data, 1);
            int ofs = 2;
            SectorPtr.Pack(parentSector.Data, ofs, leftSector.ToSectorPtr());
            ofs += KeyValueDB.PtrDownSize;
            PackUnpack.PackUInt64LE(parentSector.Data, ofs, CalcKeysInSector(leftSector));
            ofs += 8;
            PackUnpack.PackUInt16LE(parentSector.Data, ofs, (ushort)entrySize);
            ofs += 2;
            PackUnpack.PackUInt32LE(parentSector.Data, ofs, (uint)middleKeyLen);
            ofs += 4;
            SectorPtr.Pack(parentSector.Data, ofs, rightSector.ToSectorPtr());
            ofs += KeyValueDB.PtrDownSize;
            PackUnpack.PackUInt64LE(parentSector.Data, ofs, CalcKeysInSector(rightSector));
            ofs += 8;
            Array.Copy(middleKeyData, middleKeyOfs, parentSector.Data, ofs, middleKeyLenInSector);
            _owner.NewState.RootBTree.Ptr = parentSector.Position;
            _owner.NewState.RootBTreeLevels++;
            leftSector.Parent = parentSector;
            rightSector.Parent = parentSector;
            _currentKeySectorParents.Insert(0, parentSector);
            _owner.PublishSector(parentSector, false);
            _owner.TruncateSectorCache(true, 0);
        }

        static ulong CalcKeysInSector(Sector sector)
        {
            if (sector.Type == SectorType.BTreeChild)
            {
                return BTreeChildIterator.CountFromSectorData(sector.Data);
            }
            Debug.Assert(sector.Type == SectorType.BTreeParent);
            var iter = new BTreeParentIterator(sector.Data);
            var res = (ulong)iter.FirstChildKeyCount;
            if (iter.Count != 0)
            {
                iter.MoveFirst();
                do
                {
                    res += (ulong)iter.ChildKeyCount;
                } while (iter.MoveNext());
            }
            return res;
        }

        static void IncrementChildCountInBTreeParents(Sector sector)
        {
            while (sector.Parent != null)
            {
                Sector parentSector = sector.Parent;
                Debug.Assert(parentSector.Dirty);
                BTreeParentIterator.IncrementChildCount(parentSector.Data, sector.Position);
                sector = parentSector;
            }
        }

        FindKeyResult FindKeyInEmptyBTree(byte[] keyBuf, int keyOfs, int keyLen, FindKeyStrategy strategy, byte[] valueBuf, int valueOfs, int valueLen)
        {
            switch (strategy)
            {
                case FindKeyStrategy.Create:
                    var newRootBTreeSector1 = _owner.NewSector();
                    newRootBTreeSector1.Type = SectorType.BTreeChild;
                    var entrySize = BTreeChildIterator.CalcEntrySize(_prefix.Length + keyLen, valueLen < 0 ? 0 : valueLen);
                    newRootBTreeSector1.SetLengthWithRound(BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry + entrySize);
                    BTreeChildIterator.SetOneEntryCount(newRootBTreeSector1.Data, entrySize);
                    SetBTreeChildKeyData(newRootBTreeSector1, keyBuf, keyOfs, keyLen, valueBuf, valueOfs, valueLen, BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry);
                    Sector newRootBTreeSector = newRootBTreeSector1;
                    _owner.NewState.RootBTree.Ptr = newRootBTreeSector.Position;
                    _owner.NewState.RootBTreeLevels = 1;
                    _owner.NewState.KeyValuePairCount = 1;
                    _owner.PublishSector(newRootBTreeSector, false);
                    _owner.TruncateSectorCache(true, newRootBTreeSector.Position);
                    _currentKeySector = newRootBTreeSector;
                    _currentKeyIndexInLeaf = 0;
                    _currentKeyIndex = 0;
                    _prefixKeyStart = 0;
                    _prefixKeyCount = 1;
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

        SectorPtr GetRootBTreeSectorPtr()
        {
            return IsWritting() ? _owner.NewState.RootBTree : _readLink.RootBTree;
        }

        void CreateBTreeParentFromTwoLeafs(Sector leftSector, Sector rightSector)
        {
            var iter = new BTreeChildIterator(rightSector.Data);
            iter.MoveFirst();
            int keyLenInSector = iter.KeyLenInline + (iter.HasKeySectorPtr ? KeyValueDB.PtrDownSize : 0);
            if (iter.HasKeySectorPtr)
            {
                ForceKeyFlush(iter.KeySectorPtr, iter.KeyLen - iter.KeyLenInline, rightSector);
            }
            CreateBTreeParentFromTwoChildren(leftSector, rightSector, iter.Data, iter.KeyLen, iter.KeyOffset,
                                            keyLenInSector);
        }

        FindKeyResult FindKeyNoncreateStrategy(FindKeyStrategy strategy, BTreeChildIterator iter)
        {
            switch (strategy)
            {
                case FindKeyStrategy.ExactMatch:
                    return FindKeyNotFound();
                case FindKeyStrategy.OnlyNext:
                    if (_currentKeyIndexInLeaf < iter.Count)
                    {
                        return FindKeyResult.FoundNext;
                    }
                    _currentKeyIndexInLeaf--;
                    _currentKeyIndex--;
                    return FindNextKey() ? FindKeyResult.FoundNext : FindKeyNotFound();
                case FindKeyStrategy.PreferNext:
                    if (_currentKeyIndexInLeaf < iter.Count)
                    {
                        return FindKeyResult.FoundNext;
                    }
                    _currentKeyIndexInLeaf--;
                    _currentKeyIndex--;
                    return FindNextKey() ? FindKeyResult.FoundNext : FindKeyResult.FoundPrevious;
                case FindKeyStrategy.OnlyPrevious:
                    if (_currentKeyIndexInLeaf > 0)
                    {
                        _currentKeyIndexInLeaf--;
                        _currentKeyIndex--;
                        return FindKeyResult.FoundPrevious;
                    }
                    return FindPreviousKey() ? FindKeyResult.FoundPrevious : FindKeyNotFound();
                case FindKeyStrategy.PreferPrevious:
                    if (_currentKeyIndexInLeaf > 0)
                    {
                        _currentKeyIndexInLeaf--;
                        _currentKeyIndex--;
                        return FindKeyResult.FoundPrevious;
                    }
                    return FindPreviousKey() ? FindKeyResult.FoundPrevious : FindKeyResult.FoundNext;
                default:
                    throw new ArgumentOutOfRangeException("strategy");
            }
        }

        FindKeyResult FindKeyNotFound()
        {
            InvalidateCurrentKey();
            return FindKeyResult.NotFound;
        }

        int SectorDataCompare(int startOfs, byte[] buf, int ofs, int len, SectorPtr sectorPtr, int dataLen, Sector parent)
        {
            var sector = GetOrReadDataSector(sectorPtr, dataLen, parent);
            if (sector.Type == SectorType.DataChild)
            {
                int dataOfs = 0;
                if (startOfs < _prefix.Length)
                {
                    int compareLen = Math.Min(_prefix.Length - startOfs, sector.Length);
                    int res = BitArrayManipulation.CompareByteArray(_prefix, startOfs, compareLen, sector.Data, dataOfs,
                                                                    compareLen);
                    if (res != 0) return res;
                    startOfs += compareLen;
                    if (startOfs < _prefix.Length) return 0;
                    dataOfs += compareLen;
                }
                if (ofs == -1) return 1;
                startOfs -= _prefix.Length;
                return BitArrayManipulation.CompareByteArray(buf,
                                                             ofs + startOfs,
                                                             Math.Min(len - startOfs, sector.Length - dataOfs),
                                                             sector.Data,
                                                             dataOfs,
                                                             sector.Length - dataOfs);
            }
            int downPtrCount;
            var bytesInDownLevel = (int)KeyValueDB.GetBytesInDownLevel(dataLen, out downPtrCount);
            int i;
            SectorPtr downSectorPtr;
            for (i = 0; i < downPtrCount - 1; i++)
            {
                downSectorPtr = SectorPtr.Unpack(sector.Data, i * KeyValueDB.PtrDownSize);
                int res = SectorDataCompare(startOfs,
                                            buf,
                                            ofs,
                                            len,
                                            downSectorPtr,
                                            Math.Min(dataLen, bytesInDownLevel),
                                            sector);
                if (res != 0) return res;
                startOfs += bytesInDownLevel;
                dataLen -= bytesInDownLevel;
            }
            downSectorPtr = SectorPtr.Unpack(sector.Data, i * KeyValueDB.PtrDownSize);
            return SectorDataCompare(startOfs, buf, ofs, len, downSectorPtr, dataLen, sector);
        }

        Sector GetOrReadDataSector(SectorPtr sectorPtr, long dataLen, Sector parent)
        {
            return _owner.GetOrReadSector(sectorPtr, IsWritting(), dataLen > KeyValueDB.MaxLeafDataSectorSize ? SectorTypeInit.DataParent : SectorTypeInit.DataChild, parent);
        }

        void SetBTreeChildKeyDataJustLengths(Sector inSector, int keyLen, int valueLen, int sectorDataOfs)
        {
            byte[] sectorData = inSector.Data;
            int realKeyLen = _prefix.Length + keyLen;
            int realValueLen = valueLen < 0 ? 0 : valueLen;
            PackUnpack.PackVUInt(sectorData, ref sectorDataOfs, (uint)realKeyLen);
            PackUnpack.PackVUInt(sectorData, ref sectorDataOfs, (uint)realValueLen);
        }

        void SetBTreeChildKeyData(Sector inSector, byte[] keyBuf, int keyOfs, int keyLen, byte[] valueBuf, int valueOfs, int valueLen, int sectorDataOfs)
        {
            byte[] sectorData = inSector.Data;
            int realKeyLen = _prefix.Length + keyLen;
            int realValueLen = valueLen < 0 ? 0 : valueLen;
            int keyLenInline = BTreeChildIterator.CalcKeyLenInline(realKeyLen);
            int valueLenInline = BTreeChildIterator.CalcValueLenInline(realValueLen);
            PackUnpack.PackVUInt(sectorData, ref sectorDataOfs, (uint)realKeyLen);
            PackUnpack.PackVUInt(sectorData, ref sectorDataOfs, (uint)realValueLen);
            var usedPrefixLen = Math.Min(_prefix.Length, keyLenInline);
            Array.Copy(_prefix, 0, sectorData, sectorDataOfs, usedPrefixLen);
            Array.Copy(keyBuf, keyOfs, sectorData, sectorDataOfs + _prefix.Length, keyLenInline - usedPrefixLen);
            sectorDataOfs += keyLenInline;
            if (realKeyLen > BTreeChildIterator.MaxKeyLenInline)
            {
                CreateContentSector(_prefix, keyLenInline, _prefix.Length - usedPrefixLen, keyBuf, keyOfs + keyLenInline - usedPrefixLen, keyLen - keyLenInline + usedPrefixLen, inSector, sectorDataOfs);
                sectorDataOfs += KeyValueDB.PtrDownSize;
            }
            if (valueLen <= 0) return;
            Array.Copy(valueBuf, valueOfs + realValueLen - valueLenInline, sectorData, sectorDataOfs, valueLenInline);
            sectorDataOfs += valueLenInline;
            if (realValueLen > BTreeChildIterator.MaxValueLenInline)
            {
                CreateContentSector(valueBuf, valueOfs, realValueLen - valueLenInline, inSector, sectorDataOfs);
            }
        }

        void CreateContentSector(byte[] buf, int ofs, int len, Sector parent, int ofsInParent)
        {
            CreateContentSector(buf, ofs, len, null, 0, 0, parent, ofsInParent);
        }

        void CreateContentSector(byte[] buf, int ofs, int len, byte[] buf2, int ofs2, int len2, Sector parent, int ofsInParent)
        {
            if (len + len2 <= KeyValueDB.MaxLeafDataSectorSize)
            {
                var newLeafSector = _owner.NewSector();
                newLeafSector.Type = SectorType.DataChild;
                newLeafSector.SetLengthWithRound(len + len2);
                newLeafSector.Parent = parent;
                if (len > 0) Array.Copy(buf, ofs, newLeafSector.Data, 0, len);
                if (len2 > 0) Array.Copy(buf2, ofs2, newLeafSector.Data, len, len2);
                _owner.PublishSector(newLeafSector, false);
                Debug.Assert(parent.Dirty);
                SectorPtr.Pack(parent.Data, ofsInParent, newLeafSector.ToSectorPtr());
                return;
            }
            int downPtrCount;
            var bytesInDownLevel = (int)KeyValueDB.GetBytesInDownLevel(len + len2, out downPtrCount);
            var newSector = _owner.NewSector();
            newSector.Type = SectorType.DataParent;
            newSector.SetLengthWithRound(downPtrCount * KeyValueDB.PtrDownSize);
            newSector.Parent = parent;
            _owner.PublishSector(newSector, false);
            Debug.Assert(parent.Dirty);
            SectorPtr.Pack(parent.Data, ofsInParent, newSector.ToSectorPtr());
            for (int i = 0; i < downPtrCount; i++)
            {
                var usedLen = Math.Min(len, bytesInDownLevel);
                var usedLen2 = Math.Min(len2, bytesInDownLevel - usedLen);
                CreateContentSector(buf, ofs, usedLen, buf2, ofs2, usedLen2, newSector, i * KeyValueDB.PtrDownSize);
                _owner.TruncateSectorCache(true, newSector.Position);
                ofs += usedLen;
                ofs2 += usedLen2;
                len -= usedLen;
                len2 -= usedLen2;
            }
        }

        void CreateContentSector(long len, Sector parent, int ofsInParent)
        {
            if (len <= KeyValueDB.MaxLeafDataSectorSize)
            {
                var newLeafSector = _owner.NewSector();
                newLeafSector.Type = SectorType.DataChild;
                newLeafSector.SetLengthWithRound((int)len);
                newLeafSector.Parent = parent;
                _owner.PublishSector(newLeafSector, false);
                Debug.Assert(parent.Dirty);
                SectorPtr.Pack(parent.Data, ofsInParent, newLeafSector.ToSectorPtr());
                return;
            }
            int downPtrCount;
            long bytesInDownLevel = KeyValueDB.GetBytesInDownLevel(len, out downPtrCount);
            var newSector = _owner.NewSector();
            newSector.Type = SectorType.DataParent;
            newSector.SetLengthWithRound(downPtrCount * KeyValueDB.PtrDownSize);
            newSector.Parent = parent;
            _owner.PublishSector(newSector, false);
            Debug.Assert(parent.Dirty);
            SectorPtr.Pack(parent.Data, ofsInParent, newSector.ToSectorPtr());
            for (int i = 0; i < downPtrCount; i++)
            {
                CreateContentSector(Math.Min(len, bytesInDownLevel), newSector, i * KeyValueDB.PtrDownSize);
                _owner.TruncateSectorCache(true, newSector.Position);
                len -= bytesInDownLevel;
            }
        }

        void DeleteContentSector(SectorPtr sectorPtr, long len, Sector parent)
        {
            var sector = GetOrReadDataSector(sectorPtr, len, parent);
            if (sector.Type == SectorType.DataChild)
            {
                _owner.DeallocateSector(sector);
                return;
            }
            int downPtrCount;
            long bytesInDownLevel = KeyValueDB.GetBytesInDownLevel(len, out downPtrCount);
            for (int i = 0; i < downPtrCount; i++)
            {
                var downSectorPtr = SectorPtr.Unpack(sector.Data, i * KeyValueDB.PtrDownSize);
                DeleteContentSector(downSectorPtr, Math.Min(len, bytesInDownLevel), sector);
                len -= bytesInDownLevel;
            }
            _owner.DeallocateSector(sector);
        }

        public int GetKeySize()
        {
            if (_currentKeyIndexInLeaf < 0) return -1;
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndexInLeaf);
            return iter.KeyLen - _prefix.Length;
        }

        public long GetValueSize()
        {
            if (_currentKeyIndexInLeaf < 0) return -1;
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndexInLeaf);
            return iter.ValueLen;
        }

        public bool IsWritting()
        {
            return _readLink == null;
        }

        void ThrowIfCurrentKeyIsInvalid()
        {
            if (_currentKeyIndexInLeaf < 0) throw new BTDBException("Current Key is invalid");
        }

        public void PeekKey(int ofs, out int len, out byte[] buf, out int bufOfs)
        {
            ThrowIfCurrentKeyIsInvalid();
            ofs += _prefix.Length;
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndexInLeaf);
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
            ofs -= iter.KeyLenInline;
            SectorPtr dataSectorPtr = iter.KeySectorPtr;
            int dataLen = iter.KeyLen - iter.KeyLenInline;
            Sector parentOfSector = _currentKeySector;
            while (true)
            {
                Sector dataSector = GetOrReadDataSector(dataSectorPtr, dataLen, parentOfSector);
                if (dataSector.Type == SectorType.DataChild)
                {
                    buf = dataSector.Data;
                    bufOfs = ofs;
                    len = dataSector.Length - ofs;
                    return;
                }
                parentOfSector = dataSector;
                int downPtrCount;
                var bytesInDownLevel = (int)KeyValueDB.GetBytesInDownLevel(dataLen, out downPtrCount);
                int i = ofs / bytesInDownLevel;
                ofs = ofs % bytesInDownLevel;
                dataSectorPtr = SectorPtr.Unpack(dataSector.Data, i * KeyValueDB.PtrDownSize);
                if (i < downPtrCount - 1)
                {
                    dataLen = bytesInDownLevel;
                }
                else
                {
                    dataLen = dataLen % bytesInDownLevel;
                    if (dataLen == 0) dataLen = bytesInDownLevel;
                }
            }
        }

        public void ReadKey(int ofs, int len, byte[] buf, int bufOfs)
        {
            ThrowIfCurrentKeyIsInvalid();
            ofs += _prefix.Length;
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndexInLeaf);
            var keyLen = iter.KeyLen;
            if (ofs < 0 || ofs > keyLen) throw new ArgumentOutOfRangeException("ofs");
            if (len < 0 || ofs + len > keyLen) throw new ArgumentOutOfRangeException("len");
            if (len == 0) return;
            var keyLenInline = iter.KeyLenInline;
            if (ofs < keyLenInline)
            {
                var copyLen = Math.Min(len, keyLenInline - ofs);
                Array.Copy(_currentKeySector.Data, iter.KeyOffset + ofs, buf, bufOfs, copyLen);
                len -= copyLen;
                bufOfs += copyLen;
                ofs += copyLen;
            }
            if (len > 0)
            {
                ofs -= keyLenInline;
                int dataLen = keyLen - keyLenInline;
                RecursiveReadOverflown(ofs, iter.KeySectorPtr, _currentKeySector, dataLen, buf, ref bufOfs, ref len);
            }
        }

        public void PeekValue(long ofs, out int len, out byte[] buf, out int bufOfs)
        {
            ThrowIfCurrentKeyIsInvalid();
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndexInLeaf);
            if (ofs < 0 || ofs >= iter.ValueLen)
            {
                len = 0;
                buf = null;
                bufOfs = 0;
                return;
            }
            if (ofs >= iter.ValueLen - iter.ValueLenInline)
            {
                len = (int)(iter.ValueLen - ofs);
                buf = _currentKeySector.Data;
                bufOfs = iter.ValueOffset + iter.ValueLenInline - len;
                return;
            }
            SectorPtr dataSectorPtr = iter.ValueSectorPtr;
            long dataLen = iter.ValueLen - iter.ValueLenInline;
            Sector parentOfSector = _currentKeySector;
            Debug.Assert(ofs < dataLen);
            while (true)
            {
                Sector dataSector = GetOrReadDataSector(dataSectorPtr, dataLen, parentOfSector);
                if (dataSector.Type == SectorType.DataChild)
                {
                    buf = dataSector.Data;
                    bufOfs = (int)ofs;
                    len = (int)(dataSector.Length - ofs);
                    return;
                }
                parentOfSector = dataSector;
                int downPtrCount;
                long bytesInDownLevel = KeyValueDB.GetBytesInDownLevel(dataLen, out downPtrCount);
                var i = (int)(ofs / bytesInDownLevel);
                ofs = ofs % bytesInDownLevel;
                dataSectorPtr = SectorPtr.Unpack(dataSector.Data, i * KeyValueDB.PtrDownSize);
                if (i < downPtrCount - 1)
                {
                    dataLen = bytesInDownLevel;
                }
                else
                {
                    dataLen = dataLen % bytesInDownLevel;
                    if (dataLen == 0) dataLen = bytesInDownLevel;
                }
            }
        }

        public void ReadValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            ThrowIfCurrentKeyIsInvalid();
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndexInLeaf);
            var valueLen = iter.ValueLen;
            if (ofs < 0 || ofs > valueLen) throw new ArgumentOutOfRangeException("ofs");
            if (len < 0 || ofs + len > valueLen) throw new ArgumentOutOfRangeException("len");
            if (len == 0) return;
            var valueLenInline = iter.ValueLenInline;
            long dataLen = valueLen - valueLenInline;
            if (ofs < dataLen)
            {
                RecursiveReadOverflown(ofs, iter.ValueSectorPtr, _currentKeySector, dataLen, buf, ref bufOfs, ref len);
                ofs = dataLen;
            }
            if (len > 0)
            {
                Debug.Assert(ofs >= dataLen);
                Array.Copy(_currentKeySector.Data, iter.ValueOffset + (int)(ofs - dataLen), buf, bufOfs, len);
            }
        }

        void RecursiveReadOverflown(long ofs, SectorPtr dataSectorPtr, Sector parentOfSector, long dataLen, byte[] buf, ref int bufOfs, ref int len)
        {
            Sector dataSector = GetOrReadDataSector(dataSectorPtr, dataLen, parentOfSector);
            if (dataSector.Type == SectorType.DataChild)
            {
                var copyLen = Math.Min(len, (int)(dataSector.Length - ofs));
                Array.Copy(dataSector.Data, (int)ofs, buf, bufOfs, copyLen);
                len -= copyLen;
                bufOfs += copyLen;
                return;
            }
            int downPtrCount;
            long bytesInDownLevel = KeyValueDB.GetBytesInDownLevel(dataLen, out downPtrCount);
            var childIndex = (int)(ofs / bytesInDownLevel);
            ofs = ofs % bytesInDownLevel;
            while (childIndex < downPtrCount && len > 0)
            {
                long childDataLen;
                if (childIndex < downPtrCount - 1)
                {
                    childDataLen = bytesInDownLevel;
                }
                else
                {
                    childDataLen = dataLen % bytesInDownLevel;
                    if (childDataLen == 0) childDataLen = bytesInDownLevel;
                }
                dataSectorPtr = SectorPtr.Unpack(dataSector.Data, childIndex * KeyValueDB.PtrDownSize);
                RecursiveReadOverflown(ofs, dataSectorPtr, dataSector, childDataLen, buf, ref bufOfs, ref len);
                ofs = 0;
                childIndex++;
            }

        }

        public void WriteValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            if (len < 0) throw new ArgumentOutOfRangeException("len");
            if (ofs < 0) throw new ArgumentOutOfRangeException("ofs");
            if (len == 0) return;
            ThrowIfCurrentKeyIsInvalid();
            UpgradeToWriteTransaction();
            if (ofs + len > GetValueSize()) SetValueSize(ofs + len);
            InternalWriteValue(ofs, len, buf, bufOfs);
        }

        void InternalWriteValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            _currentKeySector = _owner.DirtizeSector(_currentKeySector, _currentKeySector.Parent, _currentKeySectorParents);
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndexInLeaf);
            var valueLen = iter.ValueLen;
            var valueLenInline = iter.ValueLenInline;
            if (ofs + len > valueLen - valueLenInline)
            {
                var inlineEnd = (int)(ofs + len - (valueLen - valueLenInline));
                var inlineStart = 0;
                if (ofs > valueLen - valueLenInline)
                {
                    inlineStart = (int)(ofs - (valueLen - valueLenInline));
                }
                if (buf != null)
                {
                    var inlineBufOfs = bufOfs + (int)(valueLen - valueLenInline + inlineStart - ofs);
                    Array.Copy(buf, inlineBufOfs, iter.Data, iter.ValueOffset + inlineStart, inlineEnd - inlineStart);
                }
                else
                {
                    Array.Clear(iter.Data, iter.ValueOffset + inlineStart, inlineEnd - inlineStart);
                }
                len -= inlineEnd - inlineStart;
                if (len == 0) return;
            }
            RecursiveWriteValue(iter.ValueSectorPtr, valueLen - valueLenInline, ofs, len, buf, bufOfs, _currentKeySector, iter.ValueSectorPtrOffset);
        }

        void RecursiveWriteValue(SectorPtr sectorPtr, long valueLen, long ofs, int len, byte[] buf, int bufOfs, Sector newParent, int ofsInParent)
        {
            if (ofs < 0) throw new ArgumentOutOfRangeException("ofs");
            if (ofs + len > valueLen) throw new ArgumentOutOfRangeException("ofs");
            var dataSector = GetOrReadDataSector(sectorPtr, valueLen, newParent);
            if (dataSector.Type == SectorType.DataChild)
            {
                Debug.Assert(valueLen <= dataSector.Length);
                dataSector = _owner.ResizeSectorNoUpdatePosition(dataSector, dataSector.Length, newParent, null); // DirtizeSector but without update position
                if (buf != null)
                {
                    Array.Copy(buf, bufOfs, dataSector.Data, (int)ofs, len);
                }
                else
                {
                    Array.Clear(dataSector.Data, (int)ofs, len);
                }
                SectorPtr.Pack(newParent.Data, ofsInParent, dataSector.ToSectorPtr());
                return;
            }
            dataSector = _owner.DirtizeSector(dataSector, newParent, null);
            SectorPtr.Pack(newParent.Data, ofsInParent, dataSector.ToSectorPtr());
            int downPtrCount;
            long bytesInDownLevel = KeyValueDB.GetBytesInDownLevel(valueLen, out downPtrCount);
            var i = (int)(ofs / bytesInDownLevel);
            while (i < downPtrCount)
            {
                long newofs = ofs - i * bytesInDownLevel;
                if (newofs + len <= 0) break;
                long downValueLen;
                if (i < downPtrCount - 1)
                {
                    downValueLen = bytesInDownLevel;
                }
                else
                {
                    downValueLen = valueLen % bytesInDownLevel;
                    if (downValueLen == 0) downValueLen = bytesInDownLevel;
                }
                SectorPtr downSectorPtr = SectorPtr.Unpack(dataSector.Data, i * KeyValueDB.PtrDownSize);
                int newBufOfs = bufOfs;
                int newlen = len;
                if (newofs < 0)
                {
                    newlen += (int)newofs;
                    newBufOfs -= (int)newofs;
                    newofs = 0;
                }
                if (downValueLen - newofs < newlen)
                {
                    newlen = (int)(downValueLen - newofs);
                }
                RecursiveWriteValue(downSectorPtr, downValueLen, newofs, newlen, buf, newBufOfs, dataSector, i * KeyValueDB.PtrDownSize);
                i++;
            }
        }

        public void SetValueSize(long newSize)
        {
            if (newSize < 0) throw new ArgumentOutOfRangeException("newSize");
            if (_currentKeyIndexInLeaf < 0) throw new BTDBException("Current Key is invalid");
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndexInLeaf);
            long oldSize = iter.ValueLen;
            if (oldSize == newSize) return;
            UpgradeToWriteTransaction();
            int oldInlineSize = BTreeChildIterator.CalcValueLenInline(oldSize);
            int newInlineSize = BTreeChildIterator.CalcValueLenInline(newSize);
            var newEndContent = new byte[newInlineSize];
            byte[] oldEndContent = null;
            long newEndContentOfs = newSize - newEndContent.Length;
            if (oldSize < newSize)
            {
                oldEndContent = new byte[oldInlineSize];
                ReadValue(oldSize - oldInlineSize, oldInlineSize, oldEndContent, 0);
            }
            else
            {
                ReadValue(newEndContentOfs, (int)Math.Min(newEndContent.Length, oldSize - newEndContentOfs), newEndContent, 0);
            }
            long oldDeepSize = oldSize - oldInlineSize;
            long newDeepSize = newSize - newInlineSize;
            if (oldDeepSize > 0 && newDeepSize == 0)
                DeleteContentSector(iter.ValueSectorPtr, oldDeepSize, _currentKeySector);
            _currentKeySector = _owner.ResizeSectorWithUpdatePosition(_currentKeySector, iter.TotalLength - iter.CurrentEntrySize + BTreeChildIterator.CalcEntrySize(iter.KeyLen, newSize), _currentKeySector.Parent, _currentKeySectorParents);
            iter.ResizeValue(_currentKeySector.Data, newSize);
            _owner.FixChildrenParentPointers(_currentKeySector);
            if (oldDeepSize != newDeepSize)
            {
                if (oldDeepSize == 0)
                {
                    CreateContentSector(newDeepSize, _currentKeySector, iter.ValueSectorPtrOffset);
                }
                else if (newDeepSize != 0)
                {
                    ResizeContentSector(iter.ValueSectorPtr, oldDeepSize, _currentKeySector, iter.ValueSectorPtrOffset, newDeepSize, null, 0);
                }
            }
            if (newEndContent.Length > 0) InternalWriteValue(newEndContentOfs, newEndContent.Length, newEndContent, 0);
            if (oldEndContent != null && oldEndContent.Length > 0) InternalWriteValue(oldSize - oldInlineSize, oldInlineSize, oldEndContent, 0);
        }

        public void SetValue(byte[] buf, int bufOfs, int len)
        {
            if (len < 0) throw new ArgumentOutOfRangeException("len");
            if (_currentKeyIndexInLeaf < 0) throw new BTDBException("Current Key is invalid");
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndexInLeaf);
            long oldSize = iter.ValueLen;
            if (oldSize == len)
            {
                WriteValue(0, len, buf, bufOfs);
                return;
            }
            UpgradeToWriteTransaction();
            int oldInlineSize = BTreeChildIterator.CalcValueLenInline(oldSize);
            int newInlineSize = BTreeChildIterator.CalcValueLenInline(len);
            long oldDeepSize = oldSize - oldInlineSize;
            int newDeepSize = len - newInlineSize;
            if (oldDeepSize > 0 && newDeepSize == 0)
                DeleteContentSector(iter.ValueSectorPtr, oldDeepSize, _currentKeySector);
            _currentKeySector = _owner.ResizeSectorWithUpdatePosition(_currentKeySector, iter.TotalLength - iter.CurrentEntrySize + BTreeChildIterator.CalcEntrySize(iter.KeyLen, len), _currentKeySector.Parent, _currentKeySectorParents);
            iter.ResizeValue(_currentKeySector.Data, len);
            _owner.FixChildrenParentPointers(_currentKeySector);
            Array.Copy(buf, bufOfs + len - newInlineSize, _currentKeySector.Data, iter.ValueOffset, newInlineSize);
            if (oldDeepSize == 0)
            {
                if (newDeepSize != 0)
                {
                    CreateContentSector(buf, bufOfs, newDeepSize, _currentKeySector, iter.ValueSectorPtrOffset);
                }
            }
            else if (newDeepSize != 0)
            {
                ResizeContentSector(iter.ValueSectorPtr, oldDeepSize, _currentKeySector, iter.ValueSectorPtrOffset, newDeepSize, buf, bufOfs);
            }
        }

        void ResizeContentSector(SectorPtr oldSectorPtr, long oldSize, Sector parentSector, int ofsInParent, long newSize, byte[] buf, int bufOfs)
        {
            Debug.Assert(oldSize != 0 && newSize != 0);
            if (oldSize == newSize)
            {
                if (buf != null)
                {
                    RecursiveWriteValue(oldSectorPtr, newSize, 0, (int)newSize, buf, bufOfs, parentSector, ofsInParent);
                    return;
                }
                SectorPtr.Pack(parentSector.Data, ofsInParent, oldSectorPtr);
                return;
            }
            int oldDownPtrCount;
            var oldBytesInDownLevel = KeyValueDB.GetBytesInDownLevel(oldSize, out oldDownPtrCount);
            int newDownPtrCount;
            var newBytesInDownLevel = KeyValueDB.GetBytesInDownLevel(newSize, out newDownPtrCount);
            Sector sector;
            if (oldBytesInDownLevel < newBytesInDownLevel)
            {
                sector = _owner.NewSector();
                sector.SetLengthWithRound(newDownPtrCount * KeyValueDB.PtrDownSize);
                sector.Parent = parentSector;
                sector.Type = SectorType.DataParent;
                SectorPtr.Pack(sector.Data, 0, oldSectorPtr);
                _owner.PublishSector(sector, true);
                SectorPtr.Pack(parentSector.Data, ofsInParent, sector.ToSectorPtr());
                Interlocked.Increment(ref sector.ChildrenInCache);
                try
                {
                    _owner.TruncateSectorCache(true, oldSectorPtr.Ptr);
                }
                finally
                {
                    Interlocked.Decrement(ref sector.ChildrenInCache);
                }
                ResizeContentSector(oldSectorPtr, oldSize, sector, 0, newBytesInDownLevel, buf, bufOfs);
                for (int i = 1; i < newDownPtrCount; i++)
                {
                    long downLevelSize = Math.Min(newSize - i * newBytesInDownLevel, newBytesInDownLevel);
                    if (buf != null)
                        CreateContentSector(buf, (int)(bufOfs + i * newBytesInDownLevel), (int)downLevelSize, sector, i * KeyValueDB.PtrDownSize);
                    else
                        CreateContentSector(downLevelSize, sector, i * KeyValueDB.PtrDownSize);
                }
                return;
            }
            if (oldBytesInDownLevel > newBytesInDownLevel)
            {
                sector = _owner.GetOrReadSector(oldSectorPtr, true, SectorTypeInit.DataParent, parentSector);
                for (int i = 1; i < oldDownPtrCount; i++)
                {
                    long downLevelSize = Math.Min(oldSize - i * oldBytesInDownLevel, oldBytesInDownLevel);
                    DeleteContentSector(SectorPtr.Unpack(sector.Data, i * KeyValueDB.PtrDownSize), downLevelSize, sector);
                }
                _owner.DeallocateSector(sector);
                ResizeContentSector(SectorPtr.Unpack(sector.Data, 0), oldBytesInDownLevel, parentSector, ofsInParent, newSize, buf, bufOfs);
                return;
            }
            if (oldBytesInDownLevel == 1)
            {
                ResizeContentSectorChild(oldSectorPtr, oldDownPtrCount, newDownPtrCount, parentSector, ofsInParent, buf, bufOfs);
                return;
            }
            sector = _owner.GetOrReadSector(oldSectorPtr, true, SectorTypeInit.DataParent, parentSector);
            SectorPtr lastSectorPtr;
            long lastOffset;
            for (int i = newDownPtrCount; i < oldDownPtrCount; i++)
            {
                lastOffset = i * oldBytesInDownLevel;
                lastSectorPtr = SectorPtr.Unpack(sector.Data, i * KeyValueDB.PtrDownSize);
                DeleteContentSector(lastSectorPtr, Math.Min(oldSize - lastOffset, oldBytesInDownLevel), sector);
            }
            var lastCommonPtrCount = Math.Min(oldDownPtrCount, newDownPtrCount) - 1;
            byte[] oldData = sector.Data;
            sector = _owner.ResizeSectorNoUpdatePosition(sector, newDownPtrCount * KeyValueDB.PtrDownSize, parentSector, null);
            var commonLength = (lastCommonPtrCount + 1) * KeyValueDB.PtrDownSize;
            Array.Copy(oldData, 0, sector.Data, 0, commonLength);
            Array.Clear(sector.Data, commonLength, sector.Data.Length - commonLength);
            SectorPtr.Pack(parentSector.Data, ofsInParent, sector.ToSectorPtr());
            _owner.FixChildrenParentPointers(sector);
            if (buf != null)
            {
                for (int i = 0; i < lastCommonPtrCount; i++)
                {
                    lastSectorPtr = SectorPtr.Unpack(sector.Data, i * KeyValueDB.PtrDownSize);
                    lastOffset = i * newBytesInDownLevel;
                    RecursiveWriteValue(lastSectorPtr, newBytesInDownLevel, 0, (int)newBytesInDownLevel, buf, (int)(bufOfs + lastOffset), sector, i * KeyValueDB.PtrDownSize);
                }
            }
            lastSectorPtr = SectorPtr.Unpack(sector.Data, lastCommonPtrCount * KeyValueDB.PtrDownSize);
            lastOffset = lastCommonPtrCount * newBytesInDownLevel;
            ResizeContentSector(lastSectorPtr, Math.Min(oldSize - lastOffset, oldBytesInDownLevel), sector,
                                lastCommonPtrCount * KeyValueDB.PtrDownSize,
                                Math.Min(newSize - lastOffset, newBytesInDownLevel), buf, (int)(bufOfs + lastOffset));
            for (int i = oldDownPtrCount; i < newDownPtrCount; i++)
            {
                lastOffset = i * newBytesInDownLevel;
                if (buf != null)
                    CreateContentSector(buf, (int)(bufOfs + lastOffset),
                                        (int)Math.Min(newSize - lastOffset, newBytesInDownLevel), sector,
                                        i * KeyValueDB.PtrDownSize);
                else
                    CreateContentSector(Math.Min(newSize - lastOffset, newBytesInDownLevel), sector,
                                        i * KeyValueDB.PtrDownSize);
            }
        }

        void ResizeContentSectorChild(SectorPtr oldSectorPtr, int oldLength, int newLength, Sector parentSector, int ofsInParent, byte[] buf, int bufOfs)
        {
            var sector = _owner.GetOrReadSector(oldSectorPtr, true, SectorTypeInit.DataChild, parentSector);
            byte[] oldData = sector.Data;
            sector = _owner.ResizeSectorNoUpdatePosition(sector, newLength, parentSector, null);
            if (buf == null)
            {
                Array.Copy(oldData, 0, sector.Data, 0, Math.Min(oldLength, newLength));
            }
            else
            {
                Array.Copy(buf, bufOfs, sector.Data, 0, newLength);
            }
            SectorPtr.Pack(parentSector.Data, ofsInParent, sector.ToSectorPtr());
        }

        public void EraseCurrent()
        {
            if (_currentKeyIndexInLeaf < 0) throw new BTDBException("Current Key is invalid");
            var relativeKeyIndex = _currentKeyIndex - _prefixKeyStart;
            EraseRange(relativeKeyIndex, relativeKeyIndex);
        }

        public void EraseAll()
        {
            EraseRange(0, long.MaxValue);
        }

        public void EraseRange(long firstKeyIndex, long lastKeyIndex)
        {
            if (firstKeyIndex < 0) firstKeyIndex = 0;
            if (lastKeyIndex >= GetKeyValueCount()) lastKeyIndex = _prefixKeyCount - 1;
            if (lastKeyIndex < firstKeyIndex) return;
            UpgradeToWriteTransaction();
            firstKeyIndex += _prefixKeyStart;
            lastKeyIndex += _prefixKeyStart;
            InvalidateCurrentKey();
            _prefixKeyCount -= lastKeyIndex - firstKeyIndex + 1;
            var rootBTree = GetRootBTreeSectorPtr();
            var sector = GetBTreeSector(rootBTree, null);
            if (firstKeyIndex == 0 && lastKeyIndex == (long)(_owner.NewState.KeyValuePairCount - 1))
            {
                EraseCompletely(ref sector);
            }
            else
            {
                ErasePartially(ref sector, firstKeyIndex, lastKeyIndex);
                SimplifySingleSubChild(ref sector);
            }
            _owner.NewState.KeyValuePairCount -= (ulong)(lastKeyIndex - firstKeyIndex + 1);
            if (sector == null)
            {
                _owner.NewState.RootBTree.Ptr = 0;
                _owner.NewState.RootBTree.Checksum = 0;
                _owner.NewState.RootBTreeLevels = 0;
            }
            else
            {
                _owner.NewState.RootBTree = sector.ToSectorPtr();
            }
        }

        void SimplifySingleSubChild(ref Sector sector)
        {
            while (sector.Type == SectorType.BTreeParent)
            {
                var iter = new BTreeParentIterator(sector.Data);
                if (iter.Count > 0) break;
                var sectorPtr = iter.FirstChildSectorPtr;
                _owner.DeallocateSector(sector);
                sector = GetBTreeSector(sectorPtr, null);
                _owner.NewState.RootBTreeLevels--;
            }
        }

        void EraseCompletely(ref Sector sector)
        {
            if (sector.Type == SectorType.BTreeChild)
            {
                var iter = new BTreeChildIterator(sector.Data);
                iter.MoveFirst();
                do
                {
                    if (iter.HasKeySectorPtr)
                        DeleteContentSector(iter.KeySectorPtr, iter.KeyLen - iter.KeyLenInline, sector);
                    if (iter.HasValueSectorPtr)
                        DeleteContentSector(iter.ValueSectorPtr, iter.ValueLen - iter.ValueLenInline, sector);
                } while (iter.MoveNext());
            }
            else
            {
                var iter = new BTreeParentIterator(sector.Data);
                for (int i = 0; i <= iter.Count; i++)
                {
                    var childSectorPtr = iter.GetChildSectorPtr(i);
                    var childSector = GetBTreeSector(childSectorPtr, sector);
                    EraseCompletely(ref childSector);
                }
            }
            _owner.DeallocateSector(sector);
            sector = null;
        }

        void ErasePartially(ref Sector sector, long firstKeyIndex, long lastKeyIndex)
        {
            if (sector.Type == SectorType.BTreeParent)
            {
                ErasePartiallyParent(ref sector, firstKeyIndex, lastKeyIndex);
            }
            else
            {
                ErasePartiallyChild(ref sector, firstKeyIndex, lastKeyIndex);
            }
        }

        void ErasePartiallyParent(ref Sector sector, long firstKeyIndex, long lastKeyIndex)
        {
            sector = _owner.DirtizeSector(sector, sector.Parent, null);
            var iter = new BTreeParentIterator(sector.Data);
            int? firstChildErasedCompletely = null;
            int? lastChildErasedCompletely = null;
            int? firstChildErasedPartialy = null;

            if (firstKeyIndex < iter.FirstChildKeyCount)
            {
                firstChildErasedPartialy = 0;
                var childSectorPtr = iter.FirstChildSectorPtr;
                Sector childSector = GetBTreeSector(childSectorPtr, sector);
                if (firstKeyIndex == 0 && lastKeyIndex + 1 >= iter.FirstChildKeyCount)
                {
                    EraseCompletely(ref childSector);
                    firstChildErasedCompletely = 0;
                    lastChildErasedCompletely = 1;
                    lastKeyIndex -= iter.FirstChildKeyCount;
                }
                else
                {
                    var removeCount = Math.Min(lastKeyIndex + 1, iter.FirstChildKeyCount) - firstKeyIndex;
                    ErasePartially(ref childSector, firstKeyIndex, firstKeyIndex + removeCount - 1);
                    firstKeyIndex = 0;
                    lastKeyIndex -= iter.FirstChildKeyCount;
                    iter.FirstChildSectorPtr = childSector.ToSectorPtr();
                    iter.FirstChildKeyCount -= removeCount;
                }
            }
            else
            {
                firstKeyIndex -= iter.FirstChildKeyCount;
                lastKeyIndex -= iter.FirstChildKeyCount;
            }
            iter.MoveFirst();
            for (int i = 1; i <= iter.Count; i++, iter.MoveNext())
            {
                if (lastKeyIndex < 0) break;
                var childKeyCount = iter.ChildKeyCount;
                if (firstKeyIndex >= childKeyCount)
                {
                    firstKeyIndex -= childKeyCount;
                    lastKeyIndex -= childKeyCount;
                    continue;
                }
                if (!firstChildErasedPartialy.HasValue) firstChildErasedPartialy = i;
                var childSectorPtr = iter.ChildSectorPtr;
                Sector childSector = GetBTreeSector(childSectorPtr, sector);
                if (firstKeyIndex == 0 && lastKeyIndex + 1 >= childKeyCount)
                {
                    EraseCompletely(ref childSector);
                    if (!firstChildErasedCompletely.HasValue) firstChildErasedCompletely = i;
                    lastChildErasedCompletely = i + 1;
                    lastKeyIndex -= childKeyCount;
                }
                else
                {
                    var removeCount = Math.Min(lastKeyIndex + 1, childKeyCount) - firstKeyIndex;
                    ErasePartially(ref childSector, firstKeyIndex, firstKeyIndex + removeCount - 1);
                    iter.ChildSectorPtr = childSector.ToSectorPtr();
                    iter.ChildKeyCount -= removeCount;
                    if (firstKeyIndex == 0)
                    {
                        // update key because current key was removed
                        UpdateKeyAfterRemoval(ref sector, ref iter, childSector);
                        iter.MoveTo(i - 1);
                    }
                    lastKeyIndex -= childKeyCount;
                    firstKeyIndex = 0;
                }
            }
            if (firstChildErasedCompletely.HasValue)
            {
                InternalBTreeParentEraseRange(ref sector, ref iter, firstChildErasedCompletely.Value, lastChildErasedCompletely.Value);
            }
            SimplifyBTree(ref sector, firstChildErasedPartialy ?? 0);
        }

        void InternalBTreeParentEraseRange(ref Sector sector, ref BTreeParentIterator iter, int firstIndexToErase, int lastIndexToErase)
        {
            var originalLength = iter.TotalLength;
            Debug.Assert(iter.Count >= lastIndexToErase - firstIndexToErase);
            var eraseFromOfs = iter.OffsetOfIndex(firstIndexToErase);
            var eraseToOfs = lastIndexToErase - 1 == iter.Count
                                 ? originalLength + KeyValueDB.PtrDownSize + 8
                                 : iter.OffsetOfIndex(lastIndexToErase);
            sector = _owner.ResizeSectorNoUpdatePosition(sector,
                                                         originalLength - eraseToOfs + eraseFromOfs,
                                                         sector.Parent,
                                                         null);
            var newCount = iter.Count - (lastIndexToErase - firstIndexToErase);
            BTreeParentIterator.SetCountToSectorData(sector.Data, newCount);
            var ofs = BTreeParentIterator.HeaderSize + newCount * BTreeParentIterator.HeaderForEntry;
            if (firstIndexToErase == 0)
            {
                iter.MoveTo(lastIndexToErase - 1);
                Array.Copy(iter.Data, iter.ChildSectorPtrOffset, sector.Data, BTreeParentIterator.FirstChildSectorPtrOffset, KeyValueDB.PtrDownSize + 8);
                Array.Copy(iter.Data, eraseToOfs, sector.Data, ofs, originalLength - eraseToOfs);
            }
            else
            {
                Array.Copy(iter.Data, BTreeParentIterator.FirstChildSectorPtrOffset, sector.Data, BTreeParentIterator.FirstChildSectorPtrOffset, KeyValueDB.PtrDownSize + 8);
                iter.MoveTo(firstIndexToErase - 1);
                if (lastIndexToErase - 1 == iter.Count)
                {
                    Array.Copy(iter.Data, iter.FirstOffset, sector.Data, ofs, iter.EntryOffset - iter.FirstOffset);
                }
                else
                {
                    var ofs2 = ofs + iter.ChildSectorPtrOffset - iter.FirstOffset;
                    iter.MoveTo(lastIndexToErase - 1);
                    Array.Copy(iter.Data, iter.FirstOffset, sector.Data, ofs, eraseFromOfs - iter.FirstOffset);
                    ofs += eraseFromOfs - iter.FirstOffset;
                    Array.Copy(iter.Data, iter.ChildSectorPtrOffset, sector.Data, ofs2, KeyValueDB.PtrDownSize + 8);
                    Array.Copy(iter.Data, eraseToOfs, sector.Data, ofs, originalLength - eraseToOfs);
                }
            }
            BTreeParentIterator.RecalculateHeader(sector.Data, newCount);
        }

        void SimplifyBTree(ref Sector sector, int mergeAroundIndex)
        {
            var iter = new BTreeParentIterator(sector.Data);
            if (iter.Count == 0 || mergeAroundIndex > iter.Count)
                return;
            var lenCurrent = ApproximateLengthOfBTreeChild(iter.GetChildSectorPtr(mergeAroundIndex));
            var lenPrevious = -1;
            if (mergeAroundIndex > 0)
                lenPrevious = ApproximateLengthOfBTreeChild(iter.GetChildSectorPtr(mergeAroundIndex - 1));
            var lenNext = -1;
            if (mergeAroundIndex < iter.Count)
                lenNext = ApproximateLengthOfBTreeChild(iter.GetChildSectorPtr(mergeAroundIndex + 1));
            ShouldMergeResult result = KeyValueDB.ShouldMergeBTreeParent(lenPrevious, lenCurrent, lenNext);
            if (result == ShouldMergeResult.NoMerge)
                return;
            if (result == ShouldMergeResult.MergeWithPrevious)
                mergeAroundIndex--;
            long mergedPairs;
            var leftSectorPtr = iter.GetChildSectorPtrWithKeyCount(mergeAroundIndex, out mergedPairs);
            Sector mergedSector;
            var leftSector = GetBTreeSector(leftSectorPtr, sector);
            long tempPairs;
            var rightSectorPtr = iter.GetChildSectorPtrWithKeyCount(mergeAroundIndex + 1, out tempPairs);
            mergedPairs += tempPairs;
            var rightSector = GetBTreeSector(rightSectorPtr, sector);
            Debug.Assert(leftSector.Type == rightSector.Type);
            iter.MoveTo(mergeAroundIndex);
            if (leftSector.Type == SectorType.BTreeChild)
            {
                var leftIter = new BTreeChildIterator(leftSector.Data);
                var rightIter = new BTreeChildIterator(rightSector.Data);
                if (!KeyValueDB.ShouldMerge2BTreeChild(leftIter.Count, leftIter.TotalLength, rightIter.Count,
                                                          rightIter.TotalLength))
                    return;
                mergedSector = _owner.NewSector();
                mergedSector.Type = SectorType.BTreeChild;
                mergedSector.Parent = sector;
                mergedSector.SetLengthWithRound(leftIter.TotalLength + rightIter.TotalLength - BTreeChildIterator.HeaderSize);
                var mergedCount = leftIter.Count + rightIter.Count;
                BTreeChildIterator.SetCountToSectorData(mergedSector.Data, mergedCount);
                var ofs = BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry * mergedCount;
                Array.Copy(leftIter.Data, leftIter.FirstOffset, mergedSector.Data, ofs, leftIter.TotalLength - leftIter.FirstOffset);
                ofs += leftIter.TotalLength - leftIter.FirstOffset;
                Array.Copy(rightIter.Data, rightIter.FirstOffset, mergedSector.Data, ofs, rightIter.TotalLength - rightIter.FirstOffset);
                BTreeChildIterator.RecalculateHeader(mergedSector.Data, mergedCount);
            }
            else
            {
                var keyStorageLen = 4 + iter.KeyLenInline + (iter.HasKeySectorPtr ? KeyValueDB.PtrDownSize : 0);
                var leftIter = new BTreeParentIterator(leftSector.Data);
                var rightIter = new BTreeParentIterator(rightSector.Data);
                if (!KeyValueDB.ShouldMerge2BTreeParent(leftIter.Count, leftIter.TotalLength, rightIter.Count,
                                                           rightIter.TotalLength, keyStorageLen))
                    return;
                mergedSector = _owner.NewSector();
                mergedSector.Type = SectorType.BTreeParent;
                mergedSector.Parent = sector;
                mergedSector.SetLengthWithRound(leftIter.TotalLength + rightIter.TotalLength + keyStorageLen);
                var mergedCount = leftIter.Count + rightIter.Count + 1;
                BTreeParentIterator.SetCountToSectorData(mergedSector.Data, mergedCount);
                Array.Copy(leftIter.Data, BTreeParentIterator.FirstChildSectorPtrOffset, mergedSector.Data,
                           BTreeParentIterator.FirstChildSectorPtrOffset, KeyValueDB.PtrDownSize + 8);
                var ofs = BTreeParentIterator.HeaderSize + BTreeParentIterator.HeaderForEntry * mergedCount;
                Array.Copy(leftIter.Data, leftIter.FirstOffset, mergedSector.Data, ofs, leftIter.TotalLength - leftIter.FirstOffset);
                ofs += leftIter.TotalLength - leftIter.FirstOffset;
                PackUnpack.PackInt32LE(mergedSector.Data, ofs, iter.KeyLen);
                ofs += 4;
                Array.Copy(rightIter.Data, BTreeParentIterator.FirstChildSectorPtrOffset, mergedSector.Data, ofs, KeyValueDB.PtrDownSize + 8);
                ofs += KeyValueDB.PtrDownSize + 8;
                Array.Copy(iter.Data, iter.KeyOffset, mergedSector.Data, ofs, keyStorageLen - 4);
                ofs += keyStorageLen - 4;
                Array.Copy(rightIter.Data, rightIter.FirstOffset, mergedSector.Data, ofs, rightIter.TotalLength - rightIter.FirstOffset);
                BTreeParentIterator.RecalculateHeader(mergedSector.Data, mergedCount);
            }
            _owner.PublishSector(mergedSector, true);
            _owner.DeallocateSector(leftSector);
            _owner.DeallocateSector(rightSector);
            InternalBTreeParentEraseRange(ref sector, ref iter, mergeAroundIndex, mergeAroundIndex + 1);
            new BTreeParentIterator(sector.Data).SetChildSectorPtrWithKeyCount(mergeAroundIndex,
                                                                               mergedSector.ToSectorPtr(),
                                                                               mergedPairs);
            _owner.TruncateSectorCache(true, 0);
        }

        int ApproximateLengthOfBTreeChild(SectorPtr childSectorPtr)
        {
            if (childSectorPtr.Ptr >= 0)
            {
                return (int)(childSectorPtr.Ptr & KeyValueDB.MaskOfGranLength) * KeyValueDB.AllocationGranularity;
            }
            Sector child = _owner.TryGetSector(childSectorPtr.Ptr, false, null);
            return child.Length;
        }

        void UpdateKeyAfterRemoval(ref Sector sector, ref BTreeParentIterator iter, Sector childSector)
        {
            var entryOffset = iter.EntryOffset;
            var nextEntryOffset = iter.NextEntryOffset;
            var originalEntryLength = nextEntryOffset - entryOffset;
            byte[] data;
            int ofs;
            int len;
            int keyLen;
            ExtractFirstKey(childSector, out data, out ofs, out len, out keyLen);
            var newEntryLength = BTreeParentIterator.CalcEntrySize(keyLen);
            // structure of data is inlinekey/var, [downptr/12]
            int originalLength = iter.TotalLength;
            sector = _owner.ResizeSectorNoUpdatePosition(sector,
                                                         originalLength - originalEntryLength + newEntryLength,
                                                         sector.Parent,
                                                         null);
            Array.Copy(iter.Data, 0, sector.Data, 0, iter.KeyOffset);
            Array.Copy(iter.Data, nextEntryOffset, sector.Data, entryOffset + newEntryLength, originalLength - nextEntryOffset);
            PackUnpack.PackInt32LE(sector.Data, entryOffset, keyLen);
            Array.Copy(data, ofs, sector.Data, iter.KeyOffset, len);
            BTreeParentIterator.RecalculateHeader(sector.Data, iter.Count);
            iter = new BTreeParentIterator(sector.Data);
        }

        void ExtractFirstKey(Sector sector, out byte[] data, out int ofs, out int len, out int keyLen)
        {
            if (sector.Type == SectorType.BTreeChild)
            {
                var iter = new BTreeChildIterator(sector.Data);
                iter.MoveFirst();
                data = iter.Data;
                ofs = iter.KeyOffset;
                len = iter.ValueOffset - ofs;
                keyLen = iter.KeyLen;
                if (iter.HasKeySectorPtr) ForceKeyFlush(iter.KeySectorPtr, iter.KeyLen - iter.KeyLenInline, sector);
                Debug.Assert(data == sector.Data);
                return;
            }
            else
            {
                var iter = new BTreeParentIterator(sector.Data);
                var childSector = GetBTreeSector(iter.FirstChildSectorPtr, sector);
                ExtractFirstKey(childSector, out data, out ofs, out len, out keyLen);
            }
        }

        Sector GetBTreeSector(SectorPtr childSectorPtr, Sector parent)
        {
            return _owner.GetOrReadSector(childSectorPtr, true, SectorTypeInit.BTreeChildOrParent, parent);
        }

        void ErasePartiallyChild(ref Sector sector, long firstKeyIndex, long lastKeyIndex)
        {
            var iter = new BTreeChildIterator(sector.Data);
            iter.MoveTo((int)firstKeyIndex);
            var eraseFromOfs = iter.EntryOffset;
            while (true)
            {
                if (iter.HasKeySectorPtr)
                    DeleteContentSector(iter.KeySectorPtr, iter.KeyLen - iter.KeyLenInline, sector);
                if (iter.HasValueSectorPtr)
                    DeleteContentSector(iter.ValueSectorPtr, iter.ValueLen - iter.ValueLenInline, sector);
                if (iter.Index == (int)lastKeyIndex) break;
                iter.MoveNext();
            }
            var eraseToOfs = iter.EntryOffset + iter.CurrentEntrySize;
            var originalLength = iter.TotalLength;
            var newCount = iter.Count - 1 - (int)(lastKeyIndex - firstKeyIndex);
            sector = _owner.ResizeSectorNoUpdatePosition(sector,
                                                         originalLength - eraseToOfs + eraseFromOfs - BTreeChildIterator.HeaderForEntry * (iter.Count - newCount),
                                                         sector.Parent,
                                                         null);
            BTreeChildIterator.SetCountToSectorData(sector.Data, newCount);
            var ofs = BTreeChildIterator.HeaderSize + BTreeChildIterator.HeaderForEntry * newCount;
            Array.Copy(iter.Data, iter.FirstOffset, sector.Data, ofs, eraseFromOfs - iter.FirstOffset);
            ofs += eraseFromOfs - iter.FirstOffset;
            Array.Copy(iter.Data, eraseToOfs, sector.Data, ofs, originalLength - eraseToOfs);
            BTreeChildIterator.RecalculateHeader(sector.Data, newCount);
        }

        public void Commit()
        {
            InvalidateCurrentKey();
            if (_readLink != null) return; // It is read only transaction nothing to commit
            _owner.CommitWriteTransaction();
        }

        public KeyValueDBStats CalculateStats()
        {
            return _owner.CalculateStats(_readLink);
        }
    }
}
