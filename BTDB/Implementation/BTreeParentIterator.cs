using System;
using System.Diagnostics;

namespace BTDB
{
    internal struct BTreeParentIterator
    {
        internal const int HeaderSize = 1 + LowLevelDB.PtrDownSize + 8;
        internal const int FirstChildSectorPtrOffset = 1;

        readonly byte[] _data;
        readonly int _count;
        int _totalLength;
        int _ofs;
        int _pos;
        int _keyLen;

        public BTreeParentIterator(byte[] data)
        {
            Debug.Assert(data[0] >= 128);
            _data = data;
            _count = data[0] - 128;
            _totalLength = 0;
            _ofs = HeaderSize;
            _pos = 0;
            _keyLen = (int)PackUnpack.UnpackUInt32(_data, _ofs);
        }

        internal static int CalcEntrySize(int keyLen)
        {
            return 4 + BTreeChildIterator.CalcKeyLenInline(keyLen) +
                   (keyLen > BTreeChildIterator.MaxKeyLenInline ? LowLevelDB.PtrDownSize : 0) +
                   LowLevelDB.PtrDownSize + 8;
        }

        internal void MoveFirst()
        {
            _ofs = HeaderSize;
            _pos = 0;
            LoadEntry();
        }

        void LoadEntry()
        {
            _keyLen = (int)PackUnpack.UnpackUInt32(_data, _ofs);
        }

        internal bool MoveNext()
        {
            if (_pos + 1 >= _count) return false;
            _pos++;
            _ofs += CurrentEntrySize;
            LoadEntry();
            return true;
        }

        internal void MoveTo(int pos)
        {
            Debug.Assert(pos >= 0);
            Debug.Assert(pos < _count);
            if (pos < _pos) MoveFirst();
            while (_pos < pos) MoveNext();
        }

        internal int KeyLen
        {
            get { return _keyLen; }
        }

        internal int KeyOffset
        {
            get { return _ofs + 4; }
        }

        internal int KeyLenInline
        {
            get { return BTreeChildIterator.CalcKeyLenInline(_keyLen); }
        }

        internal bool HasKeySectorPtr
        {
            get { return _keyLen > BTreeChildIterator.MaxKeyLenInline; }
        }

        internal int KeySectorPtrOffset
        {
            get { return KeyOffset + KeyLenInline; }
        }

        internal long KeySectorPos
        {
            get
            {
                return HasKeySectorPtr ? PackUnpack.UnpackInt64(_data, KeySectorPtrOffset) : 0;
            }
        }

        internal SectorPtr KeySectorPtr
        {
            get
            {
                if (!HasKeySectorPtr) throw new InvalidOperationException();
                return SectorPtr.Unpack(_data, KeySectorPtrOffset);
            }
        }

        internal int ChildSectorPtrOffset
        {
            get { return KeyOffset + KeyLenInline; }
        }

        internal long ChildSectorPos
        {
            get
            {
                return PackUnpack.UnpackInt64(_data, ChildSectorPtrOffset);
            }
        }

        internal SectorPtr ChildSectorPtr
        {
            get
            {
                return SectorPtr.Unpack(_data, ChildSectorPtrOffset);
            }
            set
            {
                SectorPtr.Pack(_data, ChildSectorPtrOffset, value);
            }
        }

        internal int ChildKeyCountOffset
        {
            get { return ChildSectorPtrOffset + LowLevelDB.PtrDownSize; }
        }

        internal long ChildKeyCount
        {
            get { return PackUnpack.UnpackInt64(_data, ChildKeyCountOffset); }
            set { PackUnpack.PackInt64(_data, ChildKeyCountOffset, value); }
        }

        internal int OffsetOfIndex(int index)
        {
            if (index == -1) return FirstChildSectorPtrOffset;
            if (index == _count) return TotalLength;
            MoveTo(index);
            return _ofs;
        }

        internal int TotalLength
        {
            get
            {
                if (_totalLength > 0) return _totalLength;
                if (_count==0)
                {
                    _totalLength = HeaderSize;
                    return _totalLength;
                }
                var backupOfs = _ofs;
                for (int i = _pos + 1; i < _count; i++)
                {
                    _ofs += CurrentEntrySize;
                    LoadEntry();
                }
                _totalLength = _ofs + CurrentEntrySize;
                _ofs = backupOfs;
                LoadEntry();
                return _totalLength;
            }
        }

        internal int CurrentEntrySize
        {
            get
            {
                int result = 4 + KeyLenInline + LowLevelDB.PtrDownSize + 8;
                if (_keyLen > BTreeChildIterator.MaxKeyLenInline) result += LowLevelDB.PtrDownSize;
                return result;
            }
        }

        internal int Count
        {
            get { return _count; }
        }

        internal int Index
        {
            get { return _pos; }
        }

        internal byte[] Data
        {
            get { return _data; }
        }

        internal int EntryOffset
        {
            get { return _ofs; }
        }

        internal long FirstChildSectorPos
        {
            get
            {
                return PackUnpack.UnpackInt64(_data, FirstChildSectorPtrOffset);
            }
        }

        internal SectorPtr FirstChildSectorPtr
        {
            get { return SectorPtr.Unpack(_data, FirstChildSectorPtrOffset); }
            set { SectorPtr.Pack(_data, FirstChildSectorPtrOffset, value); }
        }

        internal long FirstChildKeyCount
        {
            get { return PackUnpack.UnpackInt64(_data, FirstChildSectorPtrOffset + LowLevelDB.PtrDownSize); }
            set { PackUnpack.PackInt64(_data, FirstChildSectorPtrOffset + LowLevelDB.PtrDownSize, value); }
        }

        internal int BinarySearch(byte[] prefix, byte[] keyBuf, int keyOfs, int keyLen, Sector parent, Func<int, byte[], int, int, SectorPtr, int, Sector, int> compare)
        {
            int l = 0;
            int r = _count;
            while (l < r)
            {
                int m = (l + r) / 2;
                MoveTo(m);
                int keyLenInline = KeyLenInline;
                var compareLen = Math.Min(prefix.Length, keyLenInline);
                int result = BitArrayManipulation.CompareByteArray(prefix, 0, compareLen,
                                                                   _data, KeyOffset,
                                                                   compareLen);
                if (result == 0)
                {
                    if (keyLenInline>compareLen)
                    {
                        if (keyOfs==-1)
                        {
                            result = 1;
                        }
                        else
                        {
                            result = BitArrayManipulation.CompareByteArray(keyBuf,
                                                                           keyOfs,
                                                                           Math.Min(keyLen, keyLenInline - compareLen),
                                                                           _data,
                                                                           KeyOffset + compareLen,
                                                                           keyLenInline - compareLen);
                        }
                    }
                    if (result == 0)
                    {
                        if (prefix.Length + keyLen <= BTreeChildIterator.MaxKeyLenInline)
                        {
                            if (prefix.Length + keyLen == keyLenInline) return m * 2 + 1;
                            l = m + 1;
                            continue;
                        }
                        result = compare(keyLenInline, keyBuf, keyOfs, keyLen, KeySectorPtr, _keyLen - keyLenInline, parent);
                        if (result == 0)
                        {
                            if (_keyLen == prefix.Length + keyLen) return m * 2 + 1;
                            l = m + 1;
                            continue;
                        }
                    }
                }
                if (result < 0)
                {
                    r = m;
                }
                else
                {
                    l = m + 1;
                }
            }
            return l * 2;
        }

        internal SectorPtr GetChildSectorPtr(int index)
        {
            if (index == 0) return FirstChildSectorPtr;
            if (index == Index)
            {
                return SectorPtr.Unpack(_data, _ofs - 8 - LowLevelDB.PtrDownSize);
            }
            MoveTo(index - 1);
            return ChildSectorPtr;
        }

        internal SectorPtr GetChildSectorPtr(int index, ref long keyIndex)
        {
            if (index == 0)
            {
                return FirstChildSectorPtr;
            }
            keyIndex += FirstChildKeyCount;
            MoveFirst();
            while (_pos < index - 1)
            {
                keyIndex += ChildKeyCount;
                MoveNext();
            }
            return ChildSectorPtr;
        }

        internal static void ModifyChildCount(byte[] parentData, long childPos, long delta)
        {
            var iterParent = new BTreeParentIterator(parentData);
            if ((iterParent.FirstChildSectorPos & LowLevelDB.MaskOfPosition) == childPos)
            {
                iterParent.FirstChildKeyCount = iterParent.FirstChildKeyCount + delta;
                return;
            }
            do
            {
                if ((iterParent.ChildSectorPos & LowLevelDB.MaskOfPosition) == childPos)
                {
                    iterParent.ChildKeyCount = iterParent.ChildKeyCount + delta;
                    return;
                }
            } while (iterParent.MoveNext());
            throw new BTDBException("ModifyChildCount child not found");
        }

        internal int FindChildByPos(long position)
        {
            if ((FirstChildSectorPos & LowLevelDB.MaskOfPosition) == position)
            {
                return 0;
            }
            MoveFirst();
            do
            {
                if ((ChildSectorPos & LowLevelDB.MaskOfPosition) == position)
                {
                    return Index + 1;
                }
            } while (MoveNext());
            throw new BTDBException("FindChildByPos child not found");
        }
    }
}