using System;
using System.Diagnostics;
using BTDB.Buffer;

namespace BTDB.KVDBLayer
{
    internal struct BTreeParentIterator
    {
        internal const int HeaderSize = 2 + KeyValueDB.PtrDownSize + 8;
        internal const int HeaderForEntry = 2;
        internal const int FirstChildSectorPtrOffset = 2;

        readonly byte[] _data;
        readonly int _count;
        int _totalLength;
        int _ofs;
        int _pos;
        int _keyLen;

        public BTreeParentIterator(byte[] data)
        {
            Debug.Assert(data[1] >= 128);
            _data = data;
            _count = CountFromSectorData(data);
            _totalLength = 0;
            _ofs = -1;
            _pos = -1;
            _keyLen = -1;
        }

        internal static int CountFromSectorData(byte[] data)
        {
            return (data[1] - 128) * 256 + data[0];
        }

        internal static void SetCountToSectorData(byte[] data, int count)
        {
            Debug.Assert(count >= 0 && count < 128 * 256);
            data[0] = (byte)count;
            data[1] = (byte)((count >> 8) + 128);
        }

        internal static int CalcEntrySize(int keyLen)
        {
            return 4 + KeyValueDB.PtrDownSize + 8 + BTreeChildIterator.CalcKeyLenInline(keyLen) +
                   (keyLen > BTreeChildIterator.MaxKeyLenInline ? KeyValueDB.PtrDownSize : 0);
        }

        internal int FirstOffset
        {
            get { return HeaderSize + _count * HeaderForEntry; }
        }

        internal void MoveFirst()
        {
            _ofs = FirstOffset;
            _pos = 0;
            _keyLen = -1;
        }

        internal bool MoveNext()
        {
            if (_pos + 1 >= _count) return false;
            _ofs = FirstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + _pos * HeaderForEntry);
            _pos++;
            _keyLen = -1;
            return true;
        }

        internal void MoveTo(int pos)
        {
            Debug.Assert(pos >= 0);
            Debug.Assert(pos < _count);
            if (pos == 0)
            {
                MoveFirst();
                return;
            }
            _pos = pos;
            _ofs = FirstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + (pos - 1) * HeaderForEntry);
            _keyLen = -1;
        }

        internal int KeyLen
        {
            get
            {
                if (_keyLen == -1) _keyLen = PackUnpack.UnpackInt32LE(_data, _ofs);
                return _keyLen;
            }
        }

        internal int KeyOffset
        {
            get { return _ofs + 4 + KeyValueDB.PtrDownSize + 8; }
        }

        internal int KeyLenInline
        {
            get { return BTreeChildIterator.CalcKeyLenInline(KeyLen); }
        }

        internal bool HasKeySectorPtr
        {
            get { return KeyLen > BTreeChildIterator.MaxKeyLenInline; }
        }

        internal int KeySectorPtrOffset
        {
            get { return KeyOffset + KeyLenInline; }
        }

        internal long KeySectorPos
        {
            get
            {
                return HasKeySectorPtr ? PackUnpack.UnpackInt64LE(_data, KeySectorPtrOffset) : 0;
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
            get { return _ofs + 4; }
        }

        internal long ChildSectorPos
        {
            get
            {
                return PackUnpack.UnpackInt64LE(_data, ChildSectorPtrOffset);
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
            get { return ChildSectorPtrOffset + KeyValueDB.PtrDownSize; }
        }

        internal long ChildKeyCount
        {
            get { return PackUnpack.UnpackInt64LE(_data, ChildKeyCountOffset); }
            set { PackUnpack.PackInt64LE(_data, ChildKeyCountOffset, value); }
        }

        void IncrementChildKeyCount()
        {
            PackUnpack.IncrementInt64LE(_data, ChildKeyCountOffset);
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
                if (_count == 0)
                {
                    _totalLength = FirstOffset;
                }
                else
                {
                    _totalLength = FirstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + (_count - 1) * HeaderForEntry);
                }
                return _totalLength;
            }
        }

        internal int CurrentEntrySize
        {
            get
            {
                int result = 4 + KeyValueDB.PtrDownSize + 8 + KeyLenInline;
                if (_keyLen > BTreeChildIterator.MaxKeyLenInline) result += KeyValueDB.PtrDownSize;
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

        public int NextEntryOffset
        {
            get
            {
                return FirstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + _pos * HeaderForEntry);
            }
        }

        internal long FirstChildSectorPos
        {
            get
            {
                return PackUnpack.UnpackInt64LE(_data, FirstChildSectorPtrOffset);
            }
        }

        internal SectorPtr FirstChildSectorPtr
        {
            get { return SectorPtr.Unpack(_data, FirstChildSectorPtrOffset); }
            set { SectorPtr.Pack(_data, FirstChildSectorPtrOffset, value); }
        }

        internal long FirstChildKeyCount
        {
            get { return PackUnpack.UnpackInt64LE(_data, FirstChildSectorPtrOffset + KeyValueDB.PtrDownSize); }
            set { PackUnpack.PackInt64LE(_data, FirstChildSectorPtrOffset + KeyValueDB.PtrDownSize, value); }
        }

        void IncrementFirstChildKeyCount()
        {
            PackUnpack.IncrementInt64LE(_data, FirstChildSectorPtrOffset + KeyValueDB.PtrDownSize);
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
                    if (keyLenInline > compareLen)
                    {
                        if (keyOfs == -1)
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
                        if (prefix.Length + keyLen <= BTreeChildIterator.MaxKeyLenInline || _keyLen - keyLenInline == 0)
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

        internal long GetChildSectorPos(int index)
        {
            if (index == 0) return FirstChildSectorPos;
            if (index == 1)
            {
                return PackUnpack.UnpackInt64LE(_data, FirstOffset + 4);
            }
            return PackUnpack.UnpackInt64LE(_data, FirstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + (index - 2) * HeaderForEntry) + 4);
        }

        internal SectorPtr GetChildSectorPtr(int index)
        {
            if (index == 0) return FirstChildSectorPtr;
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
            var firstOffset = FirstOffset;
            _ofs = firstOffset;
            _pos = 0;
            _keyLen = -1;
            while (_pos < index - 1)
            {
                keyIndex += ChildKeyCount;
                _ofs = firstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + _pos * HeaderForEntry);
                _pos++;
            }
            return ChildSectorPtr;
        }

        public SectorPtr GetChildSectorPtrByKeyIndex(long index, ref long keyIndex)
        {
            var curKeyCount = FirstChildKeyCount;
            if (index < curKeyCount)
            {
                return FirstChildSectorPtr;
            }
            keyIndex += curKeyCount;
            index -= curKeyCount;
            var firstOffset = FirstOffset;
            _ofs = firstOffset;
            _pos = 0;
            _keyLen = -1;
            curKeyCount = ChildKeyCount;
            while (index >= curKeyCount)
            {
                keyIndex += curKeyCount;
                index -= curKeyCount;
                _ofs = firstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + _pos * HeaderForEntry);
                _pos++;
                curKeyCount = ChildKeyCount;
            }
            return ChildSectorPtr;
        }

        internal SectorPtr GetChildSectorPtrWithKeyCount(int index, out long keyCount)
        {
            if (index == 0)
            {
                keyCount = FirstChildKeyCount;
                return FirstChildSectorPtr;
            }
            MoveTo(index - 1);
            keyCount = ChildKeyCount;
            return ChildSectorPtr;
        }

        public void SetChildSectorPtrWithKeyCount(int index, SectorPtr childSectorPtr, long keyCount)
        {
            if (index == 0)
            {
                FirstChildSectorPtr = childSectorPtr;
                FirstChildKeyCount = keyCount;
                return;
            }
            MoveTo(index - 1);
            ChildSectorPtr = childSectorPtr;
            ChildKeyCount = keyCount;
        }

        internal static void IncrementChildCount(byte[] parentData, long childPos)
        {
            var iterParent = new BTreeParentIterator(parentData);
            if ((iterParent.FirstChildSectorPos & KeyValueDB.MaskOfPosition) == childPos)
            {
                iterParent.IncrementFirstChildKeyCount();
                return;
            }
            if (iterParent.Count != 0)
            {
                iterParent.MoveFirst();
                do
                {
                    if ((iterParent.ChildSectorPos & KeyValueDB.MaskOfPosition) == childPos)
                    {
                        iterParent.IncrementChildKeyCount();
                        return;
                    }
                } while (iterParent.MoveNext());
            }
            throw new BTDBException("ModifyChildCount child not found");
        }

        internal int FindChildByPos(long position)
        {
            if ((FirstChildSectorPos & KeyValueDB.MaskOfPosition) == position)
            {
                return 0;
            }
            if (Count != 0)
            {
                MoveFirst();
                do
                {
                    if ((ChildSectorPos & KeyValueDB.MaskOfPosition) == position)
                    {
                        return Index + 1;
                    }
                } while (MoveNext());
            }
            throw new BTDBException("FindChildByPos child not found");
        }

        internal static void RecalculateHeader(byte[] data, int count)
        {
            var ofs1 = HeaderSize + HeaderForEntry * count;
            var ofs = ofs1;
            for (int i = 0; i < count; i++)
            {
                ofs += CalcEntrySize(PackUnpack.UnpackInt32LE(data, ofs));
                PackUnpack.PackUInt16LE(data, HeaderSize + HeaderForEntry * i, (ushort)(ofs - ofs1));
            }
        }
    }
}
