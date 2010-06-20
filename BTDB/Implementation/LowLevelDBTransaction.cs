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
                    Sector newSector = _owner.ResizeSectorWithUpdatePosition(sector, iter.TotalLength + additionalLengthNeeded, sector.Parent);
                    newSector.Data[0] = (byte)(iter.Count + 1);
                    int insertOfs = iter.OffsetOfIndex(_currentKeyIndex);
                    Array.Copy(iter.Data, 1, newSector.Data, 1, insertOfs - 1);
                    Array.Copy(iter.Data, insertOfs, newSector.Data, insertOfs + additionalLengthNeeded, iter.TotalLength - insertOfs);
                    SetBTreeChildKeyData(newSector, keyBuf, keyOfs, keyLen, insertOfs);
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
            int downPtrCount;
            var bytesInDownLevel = (int)GetBytesInDownLevel(dataLen, out downPtrCount);
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
            int downPtrCount;
            var bytesInDownLevel = (int)GetBytesInDownLevel(len, out downPtrCount);
            var newSector = _owner.NewSector();
            newSector.Type = SectorType.DataParent;
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

        SectorPtr CreateContentSector(long len, Sector parent)
        {
            if (len <= LowLevelDB.MaxLeafDataSectorSize)
            {
                var newLeafSector = _owner.NewSector();
                newLeafSector.Type = SectorType.DataChild;
                newLeafSector.SetLengthWithRound((int)len);
                newLeafSector.Parent = parent;
                _owner.PublishSector(newLeafSector);
                return newLeafSector.ToPtrWithLen();
            }
            int downPtrCount;
            long bytesInDownLevel = GetBytesInDownLevel(len, out downPtrCount);
            var newSector = _owner.NewSector();
            newSector.Type = SectorType.DataParent;
            newSector.SetLengthWithRound(downPtrCount * LowLevelDB.PtrDownSize);
            newSector.Parent = parent;
            for (int i = 0; i < downPtrCount; i++)
            {
                SectorPtr sectorPtr = CreateContentSector(Math.Min(len, bytesInDownLevel), newSector);
                SectorPtr.Pack(newSector.Data, i * LowLevelDB.PtrDownSize, sectorPtr);
                len -= bytesInDownLevel;
            }
            _owner.PublishSector(newSector);
            return newSector.ToPtrWithLen();
        }

        void DeleteContentSector(SectorPtr sectorPtr, long len)
        {
            Sector sector = _owner.TryGetSector(sectorPtr.Ptr);
            if (len <= LowLevelDB.MaxLeafDataSectorSize)
            {
                if (sector == null)
                {
                    sector = _owner.ReadSector(sectorPtr, true);
                    sector.Type = SectorType.DataChild;
                }
                _owner.DeallocateSector(sector);
                return;
            }
            int downPtrCount;
            long bytesInDownLevel = GetBytesInDownLevel(len, out downPtrCount);
            if (sector == null)
            {
                sector = _owner.ReadSector(sectorPtr, true);
                sector.Type = SectorType.DataParent;
            }
            for (int i = 0; i < downPtrCount; i++)
            {
                var downSectorPtr = SectorPtr.Unpack(sector.Data, i * LowLevelDB.PtrDownSize);
                DeleteContentSector(downSectorPtr, Math.Min(len, bytesInDownLevel));
                len -= bytesInDownLevel;
            }
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
                int downPtrCount;
                var bytesInDownLevel = (int)GetBytesInDownLevel(dataLen, out downPtrCount);
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
                Array.Copy(localBuf, localBufOfs, buf, bufOfs, Math.Min(len, localOutLen));
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
                int downPtrCount;
                long bytesInDownLevel = GetBytesInDownLevel(dataLen, out downPtrCount);
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
                Array.Copy(localBuf, localBufOfs, buf, bufOfs, Math.Min(len, localOutLen));
                ofs += localOutLen;
                bufOfs += localOutLen;
                len -= localOutLen;
            }
        }

        public void WriteValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            if (len < 0) throw new ArgumentOutOfRangeException("len");
            if (ofs < 0) throw new ArgumentOutOfRangeException("ofs");
            if (len == 0) return;
            if (_currentKeyIndex < 0) throw new BTDBException("Current Key is invalid");
            UpgradeToWriteTransaction();
            if (ofs + len > GetValueSize()) SetValueSize(ofs + len);
            InternalWriteValue(ofs, len, buf, bufOfs);
        }

        void InternalWriteValue(long ofs, int len, byte[] buf, int bufOfs)
        {
            _currentKeySector = _owner.DirtizeSector(_currentKeySector, _currentKeySector.Parent);
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndex);
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
            iter.ValueSectorPtr = RecursiveWriteValue(iter.ValueSectorPtr, valueLen - valueLenInline, ofs, len, buf, bufOfs, _currentKeySector);
        }

        SectorPtr RecursiveWriteValue(SectorPtr sectorPtr, long valueLen, long ofs, int len, byte[] buf, int bufOfs, Sector newParent)
        {
            if (ofs < 0) throw new ArgumentOutOfRangeException("ofs");
            if (ofs + len > valueLen) throw new ArgumentOutOfRangeException("ofs");
            Sector dataSector = _owner.TryGetSector(sectorPtr.Ptr);
            if (valueLen <= LowLevelDB.MaxLeafDataSectorSize)
            {
                if (dataSector == null)
                {
                    dataSector = _owner.ReadSector(sectorPtr, true);
                    dataSector.Type = SectorType.DataChild;
                }
                Debug.Assert(valueLen <= dataSector.Length);
                dataSector = _owner.DirtizeSector(dataSector, newParent);
                if (buf != null)
                {
                    Array.Copy(buf, bufOfs, dataSector.Data, (int)ofs, len);
                }
                else
                {
                    Array.Clear(dataSector.Data, (int)ofs, len);
                }
                return dataSector.ToPtrWithLen();
            }
            if (dataSector == null)
            {
                dataSector = _owner.ReadSector(sectorPtr, true);
                dataSector.Type = SectorType.DataParent;
            }
            dataSector = _owner.DirtizeSector(dataSector, newParent);
            int downPtrCount;
            long bytesInDownLevel = GetBytesInDownLevel(valueLen, out downPtrCount);
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
                SectorPtr downSectorPtr = SectorPtr.Unpack(dataSector.Data, i * LowLevelDB.PtrDownSize);
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
                downSectorPtr = RecursiveWriteValue(downSectorPtr, downValueLen, newofs, newlen, buf, newBufOfs, dataSector);
                SectorPtr.Pack(dataSector.Data, i * LowLevelDB.PtrDownSize, downSectorPtr);
                i++;
            }
            return dataSector.ToPtrWithLen();
        }

        static long GetBytesInDownLevel(long len, out int downPtrCount)
        {
            if (len <= LowLevelDB.MaxLeafDataSectorSize)
            {
                downPtrCount = (int)len;
                return 1;
            }
            long leafSectors = len / LowLevelDB.MaxLeafDataSectorSize;
            if (len % LowLevelDB.MaxLeafDataSectorSize != 0) leafSectors++;
            long currentLevelLeafSectors = 1;
            while (currentLevelLeafSectors * LowLevelDB.MaxChildren < leafSectors)
                currentLevelLeafSectors *= LowLevelDB.MaxChildren;
            long bytesInDownLevel = currentLevelLeafSectors * LowLevelDB.MaxLeafDataSectorSize;
            downPtrCount = (int)((leafSectors + currentLevelLeafSectors - 1) / currentLevelLeafSectors);
            return bytesInDownLevel;
        }

        public void SetValueSize(long newSize)
        {
            if (newSize < 0) throw new ArgumentOutOfRangeException("newSize");
            if (_currentKeyIndex < 0) throw new BTDBException("Current Key is invalid");
            var iter = new BTreeChildIterator(_currentKeySector.Data);
            iter.MoveTo(_currentKeyIndex);
            long oldSize = iter.ValueLen;
            if (oldSize == newSize) return;
            UpgradeToWriteTransaction();
            int oldInlineSize = BTreeChildIterator.CalcValueLenInline(oldSize);
            int newInlineSize = BTreeChildIterator.CalcValueLenInline(newSize);
            var newEndContent = new byte[oldInlineSize > newInlineSize && newSize > oldSize ? newInlineSize + LowLevelDB.AllocationGranularity : newInlineSize];
            byte[] oldEndContent = null;
            long newEndContentOfs = newSize - newEndContent.Length;
            if (oldSize - newEndContentOfs < 0)
            {
                oldEndContent = new byte[oldInlineSize];
                ReadValue(oldSize - oldInlineSize, oldInlineSize, oldEndContent, 0);
            }
            else
            {
                ReadValue(newEndContentOfs, (int)Math.Min(newEndContent.Length, oldSize - newEndContentOfs), newEndContent, 0);
            }
            _currentKeySector = _owner.ResizeSectorWithUpdatePosition(_currentKeySector, iter.TotalLength - iter.CurrentEntrySize + BTreeChildIterator.CalcEntrySize(iter.KeyLen, newSize), _currentKeySector.Parent);
            iter.ResizeValue(_currentKeySector.Data, newSize);
            long oldDeepSize = oldSize - oldInlineSize;
            long newDeepSize = newSize - newInlineSize;
            if (oldDeepSize != newDeepSize)
            {
                if (oldDeepSize == 0)
                {
                    SectorPtr.Pack(_currentKeySector.Data, iter.ValueOffset + newInlineSize,
                                   CreateContentSector(newDeepSize, _currentKeySector));
                }
                else if (newDeepSize == 0)
                {
                    DeleteContentSector(iter.ValueSectorPtr, oldDeepSize);
                }
                else
                {
                    SectorPtr.Pack(_currentKeySector.Data, iter.ValueOffset + newInlineSize,
                                   ResizeContentSector(iter.ValueSectorPtr, oldDeepSize, _currentKeySector, newDeepSize));
                }
            }
            if (newEndContent.Length > 0) InternalWriteValue(newEndContentOfs, newEndContent.Length, newEndContent, 0);
            if (oldEndContent != null && oldEndContent.Length > 0) InternalWriteValue(oldSize - oldInlineSize, oldInlineSize, oldEndContent, 0);
        }

        SectorPtr ResizeContentSector(SectorPtr oldSectorPtr, long oldSize, Sector parentSector, long newSize)
        {
            if (oldSize == newSize) return oldSectorPtr;
            if (oldSize == 0)
            {
                return CreateContentSector(newSize, parentSector);
            }
            if (newSize == 0)
            {
                DeleteContentSector(oldSectorPtr, oldSize);
                return new SectorPtr();
            }
            int oldDownPtrCount;
            var oldBytesInDownLevel = GetBytesInDownLevel(oldSize, out oldDownPtrCount);
            int newDownPtrCount;
            var newBytesInDownLevel = GetBytesInDownLevel(newSize, out newDownPtrCount);
            if (oldBytesInDownLevel < newBytesInDownLevel)
            {
                var sector = _owner.NewSector();
                sector.SetLengthWithRound(newDownPtrCount * LowLevelDB.PtrDownSize);
                sector.Parent = parentSector;
                sector.Type = SectorType.DataParent;
                _owner.PublishSector(sector);
                SectorPtr.Pack(sector.Data, 0, ResizeContentSector(oldSectorPtr, oldSize, sector, newBytesInDownLevel));
                for (int i = 1; i < newDownPtrCount; i++)
                {
                    long downLevelSize = Math.Min(newSize - i * newBytesInDownLevel, newBytesInDownLevel);
                    SectorPtr.Pack(sector.Data, i * LowLevelDB.PtrDownSize, CreateContentSector(downLevelSize, sector));
                }
                return sector.ToPtrWithLen();
            }
            if (oldBytesInDownLevel > newBytesInDownLevel)
            {
                var sector = _owner.TryGetSector(oldSectorPtr.Ptr);
                if (sector == null)
                {
                    sector = _owner.ReadSector(oldSectorPtr, true);
                    sector.Type = SectorType.DataParent;
                }
                for (int i = 1; i < oldDownPtrCount; i++)
                {
                    long downLevelSize = Math.Min(oldSize - i * oldBytesInDownLevel, oldBytesInDownLevel);
                    DeleteContentSector(SectorPtr.Unpack(sector.Data, i * LowLevelDB.PtrDownSize), downLevelSize);
                }
                _owner.DeallocateSector(sector);
                return ResizeContentSector(SectorPtr.Unpack(sector.Data, 0), oldBytesInDownLevel, parentSector, newSize);
            }
            if (oldBytesInDownLevel == 1)
            {
                Sector sector = _owner.TryGetSector(oldSectorPtr.Ptr);
                if (sector == null)
                {
                    sector = _owner.ReadSector(oldSectorPtr, true);
                    sector.Type = SectorType.DataChild;
                }
                var oldData = sector.Data;
                sector = _owner.ResizeSectorNoUpdatePosition(sector, newDownPtrCount, parentSector);
                Array.Copy(oldData, 0, sector.Data, 0, Math.Min(oldDownPtrCount, newDownPtrCount));
                return sector.ToPtrWithLen();
            }
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