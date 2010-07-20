using System;
using System.Diagnostics;

namespace BTDB
{
    internal struct BTreeChildIterator
    {
        internal const int MaxKeyLenInline = LowLevelDB.AllocationGranularity + 12;
        internal const int MaxValueLenInline = LowLevelDB.AllocationGranularity + 12;

        long _valueLen;
        readonly byte[] _data;
        readonly int _count;
        int _ofs;
        int _pos;
        int _keyLen;
        int _totalLength;

        internal BTreeChildIterator(byte[] data)
        {
            _data = data;
            _count = data[0];
            Debug.Assert(_count < 128);
            _totalLength = 0;
            _ofs = 1;
            _pos = 0;
            _keyLen = (int)PackUnpack.UnpackUInt32(_data, _ofs);
            _valueLen = (long)PackUnpack.UnpackUInt64(_data, _ofs + 4);
        }

        internal static int CalcKeyLenInline(int keyLen)
        {
            if (keyLen <= MaxKeyLenInline) return keyLen;
            return keyLen - LowLevelDB.RoundToAllocationGranularity(keyLen - MaxKeyLenInline);
        }

        internal static int CalcValueLenInline(long valueLen)
        {
            if (valueLen <= MaxValueLenInline) return (int)valueLen;
            return (int)(valueLen - LowLevelDB.RoundToAllocationGranularity(valueLen - MaxValueLenInline));
        }

        internal static int CalcEntrySize(int keyLen)
        {
            return 4 + 8 + CalcKeyLenInline(keyLen) +
                   (keyLen > MaxKeyLenInline ? LowLevelDB.PtrDownSize : 0);
        }

        internal static int CalcEntrySize(int keyLen, long valueLen)
        {
            return 4 + 8 + CalcKeyLenInline(keyLen) +
                   (keyLen > MaxKeyLenInline ? LowLevelDB.PtrDownSize : 0) +
                   CalcValueLenInline(valueLen) +
                   (valueLen > MaxValueLenInline ? LowLevelDB.PtrDownSize : 0);
        }

        internal void MoveFirst()
        {
            _ofs = 1;
            _pos = 0;
            LoadEntry();
        }

        void LoadEntry()
        {
            _keyLen = (int)PackUnpack.UnpackUInt32(_data, _ofs);
            _valueLen = (long)PackUnpack.UnpackUInt64(_data, _ofs + 4);
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

        internal long ValueLen
        {
            get { return _valueLen; }
        }

        internal int KeyOffset
        {
            get { return _ofs + 4 + 8; }
        }

        internal int KeyLenInline
        {
            get { return CalcKeyLenInline(_keyLen); }
        }

        internal int ValueLenInline
        {
            get { return CalcValueLenInline(_valueLen); }
        }

        internal int ValueOffset
        {
            get
            {
                return _ofs + 4 + 8 + KeyLenInline + ((_keyLen > MaxKeyLenInline) ? LowLevelDB.PtrDownSize : 0);
            }
        }

        internal bool HasKeySectorPtr
        {
            get { return _keyLen > MaxKeyLenInline; }
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

        internal bool HasValueSectorPtr
        {
            get { return _valueLen > MaxValueLenInline; }
        }

        internal int ValueSectorPtrOffset
        {
            get { return ValueOffset + ValueLenInline; }
        }

        internal long ValueSectorPos
        {
            get
            {
                return HasValueSectorPtr ? PackUnpack.UnpackInt64(_data, ValueSectorPtrOffset) : 0;
            }
        }

        internal SectorPtr ValueSectorPtr
        {
            get
            {
                if (!HasValueSectorPtr) throw new InvalidOperationException();
                return SectorPtr.Unpack(_data, ValueSectorPtrOffset);
            }
            set
            {
                if (!HasValueSectorPtr) throw new InvalidOperationException();
                SectorPtr.Pack(_data, ValueSectorPtrOffset, value);
            }
        }

        internal int OffsetOfIndex(int index)
        {
            if (index == _count) return TotalLength;
            MoveTo(index);
            return _ofs;
        }

        internal int TotalLength
        {
            get
            {
                if (_totalLength > 0) return _totalLength;
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
                int result = 4 + 8 + KeyLenInline + ValueLenInline;
                if (_keyLen > MaxKeyLenInline) result += LowLevelDB.PtrDownSize;
                if (_valueLen > MaxValueLenInline) result += LowLevelDB.PtrDownSize;
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
                    result = BitArrayManipulation.CompareByteArray(keyBuf,
                                                                   keyOfs,
                                                                   Math.Min(keyLen, keyLenInline - compareLen),
                                                                   _data,
                                                                   KeyOffset + compareLen,
                                                                   keyLenInline - compareLen);
                    if (result == 0)
                    {
                        if (prefix.Length + keyLen <= MaxKeyLenInline)
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

        internal void ResizeValue(byte[] newData, long newSize)
        {
            // preserves all before current item including current sizes + key
            Array.Copy(Data, 0, newData, 0, ValueOffset);
            // preserves all after current item
            int withValuePtr = 0;
            if (HasValueSectorPtr && newSize > MaxValueLenInline)
            {
                withValuePtr = LowLevelDB.PtrDownSize;
            }
            Array.Copy(Data,
                       EntryOffset + CurrentEntrySize - withValuePtr,
                       newData,
                       EntryOffset + CalcEntrySize(KeyLen, newSize) - withValuePtr,
                       TotalLength - EntryOffset - CurrentEntrySize + withValuePtr);
            PackUnpack.PackUInt64(newData, EntryOffset + 4, (ulong)newSize);
        }
    }
}