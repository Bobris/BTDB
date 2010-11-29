using System;
using System.Diagnostics;

namespace BTDB
{
    internal struct BTreeChildIterator
    {
        internal const int MaxKeyLenInline = LowLevelDB.AllocationGranularity + 12;
        internal const int MaxValueLenInline = LowLevelDB.AllocationGranularity + 12;
        internal const int HeaderSize = 2;
        internal const int HeaderForEntry = 2;

        long _valueLen;
        byte[] _data;
        readonly int _count;
        int _ofs;
        int _pos;
        int _keyLen;
        int _totalLength;

        internal BTreeChildIterator(byte[] data)
        {
            Debug.Assert(IsChildFromSectorData(data));
            _data = data;
            _count = (int)CountFromSectorData(data);
            _totalLength = 0;
            _pos = 0;
            _ofs = HeaderSize + _count * HeaderForEntry;
            _keyLen = -1;
            _valueLen = -1;
        }

        internal static bool IsChildFromSectorData(byte[] data)
        {
            return data[1] < 128;
        }

        internal static uint CountFromSectorData(byte[] data)
        {
            return PackUnpack.UnpackUInt16(data, 0);
        }

        internal static void SetCountToSectorData(byte[] data, int count)
        {
            Debug.Assert(count > 0 && count < 128 * 256);
            PackUnpack.PackUInt16(data, 0, (ushort)count);
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

        internal int FirstOffset
        {
            get { return HeaderSize + _count * HeaderForEntry; }
        }

        internal void MoveFirst()
        {
            _ofs = FirstOffset;
            _pos = 0;
            _keyLen = -1;
            _valueLen = -1;
        }

        internal bool MoveNext()
        {
            if (_pos + 1 >= _count) return false;
            _ofs = FirstOffset + PackUnpack.UnpackUInt16(_data, HeaderSize + _pos * HeaderForEntry);
            _pos++;
            _keyLen = -1;
            _valueLen = -1;
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
            _ofs = FirstOffset + PackUnpack.UnpackUInt16(_data, HeaderSize + (pos - 1) * HeaderForEntry);
            _keyLen = -1;
            _valueLen = -1;
        }

        internal int KeyLen
        {
            get
            {
                if (_keyLen == -1) _keyLen = PackUnpack.UnpackInt32(_data, _ofs);
                return _keyLen;
            }
        }

        internal long ValueLen
        {
            get
            {
                if (_valueLen == -1) _valueLen = PackUnpack.UnpackInt64(_data, _ofs + 4);
                return _valueLen;
            }
        }

        internal int KeyOffset
        {
            get { return _ofs + 4 + 8; }
        }

        internal int KeyLenInline
        {
            get { return CalcKeyLenInline(KeyLen); }
        }

        internal int ValueLenInline
        {
            get { return CalcValueLenInline(ValueLen); }
        }

        internal int ValueOffset
        {
            get
            {
                return _ofs + 4 + 8 + KeyLenInline + ((KeyLen > MaxKeyLenInline) ? LowLevelDB.PtrDownSize : 0);
            }
        }

        internal bool HasKeySectorPtr
        {
            get { return KeyLen > MaxKeyLenInline; }
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
            get { return ValueLen > MaxValueLenInline; }
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
            if (index == 0) return FirstOffset;
            if (index == _count) return TotalLength;
            return FirstOffset + PackUnpack.UnpackUInt16(_data, HeaderSize + (index - 1) * HeaderForEntry);
        }

        internal int TotalLength
        {
            get
            {
                if (_totalLength > 0) return _totalLength;
                _totalLength = FirstOffset + PackUnpack.UnpackUInt16(_data, HeaderSize + (_count - 1) * HeaderForEntry);
                return _totalLength;
            }
        }

        internal int CurrentEntrySize
        {
            get
            {
                return OffsetOfIndex(_pos + 1) - _ofs;
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
            var currentEntrySize = CurrentEntrySize;
            var newEntrySize = CalcEntrySize(KeyLen, newSize);
            int withValuePtr = 0;
            if (HasValueSectorPtr && newSize > MaxValueLenInline)
            {
                withValuePtr = LowLevelDB.PtrDownSize;
            }
            Array.Copy(Data,
                       EntryOffset + currentEntrySize - withValuePtr,
                       newData,
                       EntryOffset + newEntrySize - withValuePtr,
                       TotalLength - EntryOffset - currentEntrySize + withValuePtr);
            PackUnpack.PackUInt64(newData, EntryOffset + 4, (ulong)newSize);
            _data = newData;
            _valueLen = newSize;
            var delta = newEntrySize - currentEntrySize;
            for (int i = _pos; i < _count; i++)
            {
                var o = HeaderSize + HeaderForEntry * i;
                PackUnpack.PackUInt16(_data, o, (ushort)(PackUnpack.UnpackUInt16(_data, o) + delta));
            }
        }

        internal int AddEntry(int additionalLengthNeeded, byte[] newData, int entryIndex)
        {
            var newCount = Count + 1;
            SetCountToSectorData(newData, newCount);
            var insertOfs = OffsetOfIndex(entryIndex);
            Array.Copy(Data, HeaderSize, newData, HeaderSize, entryIndex * HeaderForEntry);
            var additionalLengthNeededWoHeader = additionalLengthNeeded - HeaderForEntry;
            Array.Copy(Data, insertOfs, newData, insertOfs + additionalLengthNeeded,
                       TotalLength - insertOfs);
            Array.Copy(
                Data,
                HeaderSize + Count * HeaderForEntry,
                newData,
                HeaderSize + newCount * HeaderForEntry,
                insertOfs - (HeaderSize + Count * HeaderForEntry));
            for (int i = entryIndex; i < newCount; i++)
            {
                ushort o;
                if (i == 0)
                    o = (ushort)additionalLengthNeededWoHeader;
                else
                    o = (ushort)(PackUnpack.UnpackUInt16(Data, HeaderSize + (i - 1) * HeaderForEntry) + additionalLengthNeededWoHeader);
                PackUnpack.PackUInt16(newData, HeaderSize + i * HeaderForEntry, o);
            }
            return insertOfs + HeaderForEntry;
        }

        internal static void SetOneEntryCount(byte[] data, int entrySize)
        {
            SetCountToSectorData(data, 1);
            PackUnpack.PackUInt16(data, HeaderSize, (ushort)entrySize);
        }

        internal static void RecalculateHeader(byte[] data, int count)
        {
            var ofs1 = HeaderSize + HeaderForEntry * count;
            var ofs = ofs1;
            for (int i = 0; i < count; i++)
            {
                ofs += CalcEntrySize(PackUnpack.UnpackInt32(data, ofs), PackUnpack.UnpackInt64(data, ofs + 4));
                PackUnpack.PackUInt16(data, HeaderSize + HeaderForEntry * i, (ushort)(ofs - ofs1));
            }
        }
    }
}
