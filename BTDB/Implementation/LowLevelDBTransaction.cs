using System;
using System.Diagnostics;

namespace BTDB
{
    internal class LowLevelDBTransaction : ILowLevelDBTransaction
    {
        readonly LowLevelDB _owner;

        // if this is null then this transaction is writing kind
        LowLevelDB.ReadTrLink _readLink;
        Sector _currentKeySector;
        int _currentKeyIndex;

        internal LowLevelDBTransaction(LowLevelDB owner, LowLevelDB.ReadTrLink readLink)
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
            if (IsWriteTransaction()) return;
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
            if (_currentKeySector.Parent == null)
            {
                return false;
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
            SectorPtr rootBTree = IsWriteTransaction() ? _owner.NewState.RootBTree : _readLink.RootBTree;
            if (rootBTree.Ptr == 0)
            {
                switch (strategy)
                {
                    case FindKeyStrategy.Create:
                        Sector newRootBTreeSector = CreateBTreeChildWith1Key(keyBuf, keyOfs, keyLen);
                        _owner.NewState.RootBTree.Ptr = newRootBTreeSector.Position;
                        _owner.NewState.RootBTreeLevels = 1;
                        _owner.NewState.KeyValuePairCount = 1;
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
                sector = _owner.ReadSector(rootBTree, IsWriteTransaction());
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
                if (strategy != FindKeyStrategy.Create)
                {
                    return FindKeyNoncreateStrategy(strategy, iter);
                }
                int additionalLengthNeeded = BTreeChildIterator.CalcEntrySize(keyLen);
                if (iter.TotalLength + additionalLengthNeeded <= 4096 && iter.Count < 127)
                {
                    Sector newSector = _owner.ResizeSector(sector, iter.TotalLength + additionalLengthNeeded);
                    newSector.Data[0] = (byte)(iter.Count + 1);
                    int insertOfs = iter.OffsetOfIndex(_currentKeyIndex);
                    Array.Copy(iter.Data, 1, newSector.Data, 1, insertOfs - 1);
                    SetBTreeChildKeyData(newSector, keyBuf, keyOfs, keyLen, insertOfs);
                    Array.Copy(iter.Data, insertOfs, newSector.Data, insertOfs + additionalLengthNeeded, iter.TotalLength - insertOfs);
                }
                else
                {
                    throw new NotImplementedException();
                }
                _owner.NewState.KeyValuePairCount++;
                return FindKeyResult.Created;
            }
            throw new NotImplementedException();
        }

        FindKeyResult FindKeyNoncreateStrategy(FindKeyStrategy strategy, BTreeChildIterator iter)
        {
            switch (strategy)
            {
                case FindKeyStrategy.ExactMatch:
                    return FindKeyNotFound();
                case FindKeyStrategy.OnlyNext:
                    if (_currentKeyIndex < iter.Count)
                    {
                        return FindKeyResult.FoundNext;
                    }
                    _currentKeyIndex--;
                    return FindNextKey() ? FindKeyResult.FoundNext : FindKeyNotFound();
                case FindKeyStrategy.PreferNext:
                    if (_currentKeyIndex < iter.Count)
                    {
                        return FindKeyResult.FoundNext;
                    }
                    _currentKeyIndex--;
                    return FindNextKey() ? FindKeyResult.FoundNext : FindKeyResult.FoundPrevious;
                case FindKeyStrategy.OnlyPrevious:
                    if (_currentKeyIndex > 0)
                    {
                        _currentKeyIndex--;
                        return FindKeyResult.FoundPrevious;
                    }
                    return FindPreviousKey() ? FindKeyResult.FoundPrevious : FindKeyNotFound();
                case FindKeyStrategy.PreferPrevious:
                    if (_currentKeyIndex > 0)
                    {
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
            _currentKeySector = null;
            _currentKeyIndex = -1;
            return FindKeyResult.NotFound;
        }

        int SectorDataCompare(byte[] buf, int ofs, int len, SectorPtr sectorPtr, int dataLen)
        {
            Sector sector = _owner.TryGetSector(sectorPtr.Ptr);
            if (sector == null)
            {
                sector = _owner.ReadSector(sectorPtr, IsWriteTransaction());
                sector.Type = dataLen > sector.Length ? SectorType.DataParent : SectorType.DataChild;
            }
            if (sector.Type == SectorType.DataChild)
            {
                return BitArrayManipulation.CompareByteArray(buf,
                                                             ofs,
                                                             dataLen > sector.Length && len > sector.Length ? sector.Length : len,
                                                             sector.Data,
                                                             0,
                                                             sector.Length);
            }
            int leafSectors = dataLen / LowLevelDB.MaxLeafDataSectorSize;
            if (dataLen % LowLevelDB.MaxLeafDataSectorSize != 0) leafSectors++;
            int currentLevelLeafSectors = 1;
            while (currentLevelLeafSectors * LowLevelDB.MaxChildren < leafSectors)
                currentLevelLeafSectors *= LowLevelDB.MaxChildren;
            int bytesInDownLevel = currentLevelLeafSectors * LowLevelDB.MaxLeafDataSectorSize;
            int downPtrCount = (leafSectors + currentLevelLeafSectors - 1) / currentLevelLeafSectors;
            int i;
            SectorPtr downSectorPtr;
            for (i = 0; i < downPtrCount - 1; i++)
            {
                downSectorPtr = SectorPtr.Unpack(sector.Data, i * LowLevelDB.PtrDownSize);
                int res = SectorDataCompare(buf,
                                            ofs,
                                            Math.Min(len, bytesInDownLevel),
                                            downSectorPtr,
                                            Math.Min(dataLen, bytesInDownLevel));
                if (res != 0) return res;
                ofs += bytesInDownLevel;
                len -= bytesInDownLevel;
                dataLen -= bytesInDownLevel;
            }
            downSectorPtr = SectorPtr.Unpack(sector.Data, i * LowLevelDB.PtrDownSize);
            return SectorDataCompare(buf, ofs, len, downSectorPtr, dataLen);
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
            if (len <= LowLevelDB.MaxLeafDataSectorSize)
            {
                var newLeafSector = _owner.NewSector();
                newLeafSector.Type = SectorType.DataChild;
                newLeafSector.SetLengthWithRound(len);
                newLeafSector.Parent = parent;
                if (buf != null) Array.Copy(buf, ofs, newLeafSector.Data, 0, len);
                _owner.PublishSector(newLeafSector);
                return newLeafSector.ToPtrWithLen();
            }
            int leafSectors = len / LowLevelDB.MaxLeafDataSectorSize;
            if (len % LowLevelDB.MaxLeafDataSectorSize != 0) leafSectors++;
            int currentLevelLeafSectors = 1;
            while (currentLevelLeafSectors * LowLevelDB.MaxChildren < leafSectors)
                currentLevelLeafSectors *= LowLevelDB.MaxChildren;
            int bytesInDownLevel = currentLevelLeafSectors * LowLevelDB.MaxLeafDataSectorSize;
            var newSector = _owner.NewSector();
            newSector.Type = SectorType.DataParent;
            int downPtrCount = (leafSectors + currentLevelLeafSectors - 1) / currentLevelLeafSectors;
            newSector.SetLengthWithRound(downPtrCount * LowLevelDB.PtrDownSize);
            newSector.Parent = parent;
            for (int i = 0; i < downPtrCount; i++)
            {
                SectorPtr sectorPtr = CreateContentSector(buf, ofs, Math.Min(len, bytesInDownLevel), newSector);
                SectorPtr.Pack(newSector.Data, i * LowLevelDB.PtrDownSize, sectorPtr);
                ofs += bytesInDownLevel;
                len -= bytesInDownLevel;
            }
            _owner.PublishSector(newSector);
            return newSector.ToPtrWithLen();
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
            ofs -= iter.KeyLenInline;
            SectorPtr dataSectorPtr = iter.KeySectorPtr;
            int dataLen = iter.KeyLen - iter.KeyLenInline;
            while (true)
            {
                Sector dataSector = _owner.TryGetSector(dataSectorPtr.Ptr);
                if (dataLen <= LowLevelDB.MaxLeafDataSectorSize)
                {
                    if (dataSector == null)
                    {
                        dataSector = _owner.ReadSector(dataSectorPtr, IsWriteTransaction());
                        dataSector.Type = SectorType.DataChild;
                    }
                    buf = dataSector.Data;
                    bufOfs = ofs;
                    len = dataSector.Length - ofs;
                    return;
                }
                if (dataSector == null)
                {
                    dataSector = _owner.ReadSector(dataSectorPtr, IsWriteTransaction());
                    dataSector.Type = SectorType.DataParent;
                }
                int leafSectors = dataLen / LowLevelDB.MaxLeafDataSectorSize;
                if (dataLen % LowLevelDB.MaxLeafDataSectorSize != 0) leafSectors++;
                int currentLevelLeafSectors = 1;
                while (currentLevelLeafSectors * LowLevelDB.MaxChildren < leafSectors)
                    currentLevelLeafSectors *= LowLevelDB.MaxChildren;
                int bytesInDownLevel = currentLevelLeafSectors * LowLevelDB.MaxLeafDataSectorSize;
                int downPtrCount = (leafSectors + currentLevelLeafSectors - 1) / currentLevelLeafSectors;
                int i = ofs / bytesInDownLevel;
                ofs = ofs % bytesInDownLevel;
                dataSectorPtr = SectorPtr.Unpack(dataSector.Data, i * LowLevelDB.PtrDownSize);
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

        bool IsWriteTransaction()
        {
            return _readLink == null;
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
            if (_currentKeyIndex < 0) throw new BTDBException("Current Key is invalid");
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndex);
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
            Debug.Assert(ofs < dataLen);
            while (true)
            {
                Sector dataSector = _owner.TryGetSector(dataSectorPtr.Ptr);
                if (dataLen <= LowLevelDB.MaxLeafDataSectorSize)
                {
                    if (dataSector == null)
                    {
                        dataSector = _owner.ReadSector(dataSectorPtr, IsWriteTransaction());
                        dataSector.Type = SectorType.DataChild;
                    }
                    buf = dataSector.Data;
                    bufOfs = (int)ofs;
                    len = (int)(dataSector.Length - ofs);
                    return;
                }
                if (dataSector == null)
                {
                    dataSector = _owner.ReadSector(dataSectorPtr, IsWriteTransaction());
                    dataSector.Type = SectorType.DataParent;
                }
                long leafSectors = dataLen / LowLevelDB.MaxLeafDataSectorSize;
                if (dataLen % LowLevelDB.MaxLeafDataSectorSize != 0) leafSectors++;
                long currentLevelLeafSectors = 1;
                while (currentLevelLeafSectors * LowLevelDB.MaxChildren < leafSectors)
                    currentLevelLeafSectors *= LowLevelDB.MaxChildren;
                long bytesInDownLevel = currentLevelLeafSectors * LowLevelDB.MaxLeafDataSectorSize;
                var downPtrCount = (int)((leafSectors + currentLevelLeafSectors - 1) / currentLevelLeafSectors);
                var i = (int)(ofs / bytesInDownLevel);
                ofs = ofs % bytesInDownLevel;
                dataSectorPtr = SectorPtr.Unpack(dataSector.Data, i * LowLevelDB.PtrDownSize);
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
            if (_currentKeyIndex < 0) throw new BTDBException("Current Key is invalid");
            UpgradeToWriteTransaction();
            if (ofs + len > GetValueSize()) SetValueSize(ofs + len);
            throw new NotImplementedException();
        }

        public void SetValueSize(long newSize)
        {
            UpgradeToWriteTransaction();
            throw new NotImplementedException();
        }

        public void EraseCurrent()
        {
            if (_currentKeyIndex < 0) throw new BTDBException("Current Key is invalid");
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

        public LowLevelDBStats CalculateStats()
        {
            return _owner.CalculateStats(_readLink);
        }
    }
}