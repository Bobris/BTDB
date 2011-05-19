using System;
using System.Diagnostics;

namespace BTDB
{
    internal struct BTreeChildIterator
    {
        internal const int MaxKeyLenInline = KeyValueDB.AllocationGranularity + 12;
        internal const int MaxValueLenInline = KeyValueDB.AllocationGranularity + 12;
        internal const int HeaderSize = 2;
        internal const int HeaderForEntry = 2;

        long _valueLen;
        byte[] _data;
        readonly int _count;
        int _ofs;
        int _ofsAfterKeyAndValueLen;
        int _pos;
        int _keyLen;
        int _totalLength;

        internal BTreeChildIterator(byte[] data)
        {
            Debug.Assert(IsChildFromSectorData(data));
            _data = data;
            _count = (int)CountFromSectorData(data);
            _totalLength = 0;
            _pos = -1;
            _ofs = -1;
            _ofsAfterKeyAndValueLen = -1;
            _valueLen = -1;
            _keyLen = -1;
        }

        internal static bool IsChildFromSectorData(byte[] data)
        {
            return data[1] < 128;
        }

        internal static uint CountFromSectorData(byte[] data)
        {
            return PackUnpack.UnpackUInt16LE(data, 0);
        }

        internal static void SetCountToSectorData(byte[] data, int count)
        {
            Debug.Assert(count > 0 && count < 128 * 256);
            PackUnpack.PackUInt16LE(data, 0, (ushort)count);
        }

        internal static int CalcKeyLenInline(int keyLen)
        {
            if (keyLen <= MaxKeyLenInline) return keyLen;
            return keyLen - KeyValueDB.RoundToAllocationGranularity(keyLen - MaxKeyLenInline);
        }

        internal static int CalcValueLenInline(long valueLen)
        {
            if (valueLen <= MaxValueLenInline) return (int)valueLen;
            return (int)(valueLen - KeyValueDB.RoundToAllocationGranularity(valueLen - MaxValueLenInline));
        }

        internal static int CalcEntrySize(int keyLen)
        {
            return PackUnpack.LengthVUInt((uint)keyLen) + 1 + CalcKeyLenInline(keyLen) +
                   (keyLen > MaxKeyLenInline ? KeyValueDB.PtrDownSize : 0);
        }

        internal static int CalcEntrySizeWOLengths(int keyLen, long valueLen)
        {
            return CalcKeyLenInline(keyLen) +
                   (keyLen > MaxKeyLenInline ? KeyValueDB.PtrDownSize : 0) +
                   CalcValueLenInline(valueLen) +
                   (valueLen > MaxValueLenInline ? KeyValueDB.PtrDownSize : 0);
        }

        internal static int CalcEntrySize(int keyLen, long valueLen)
        {
            return PackUnpack.LengthVUInt((uint)keyLen) + PackUnpack.LengthVUInt((ulong)valueLen) +
                   CalcEntrySizeWOLengths(keyLen, valueLen);
        }

        internal int FirstOffset
        {
            get { return HeaderSize + _count * HeaderForEntry; }
        }

        private void LoadItem()
        {
            int ofs = _ofs;
            _keyLen = (int)PackUnpack.UnpackVUInt(_data, ref ofs);
            _valueLen = (long)PackUnpack.UnpackVUInt(_data, ref ofs);
            _ofsAfterKeyAndValueLen = ofs;
        }

        internal void MoveFirst()
        {
            _ofs = FirstOffset;
            _pos = 0;
            LoadItem();
        }

        internal bool MoveNext()
        {
            if (_pos + 1 >= _count) return false;
            _ofs = FirstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + _pos * HeaderForEntry);
            _pos++;
            LoadItem();
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
            LoadItem();
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
            get { return _ofsAfterKeyAndValueLen; }
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
                return _ofsAfterKeyAndValueLen + KeyLenInline + ((KeyLen > MaxKeyLenInline) ? KeyValueDB.PtrDownSize : 0);
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
                return HasValueSectorPtr ? PackUnpack.UnpackInt64LE(_data, ValueSectorPtrOffset) : 0;
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
            return FirstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + (index - 1) * HeaderForEntry);
        }

        internal int TotalLength
        {
            get
            {
                if (_totalLength > 0) return _totalLength;
                _totalLength = FirstOffset + PackUnpack.UnpackUInt16LE(_data, HeaderSize + (_count - 1) * HeaderForEntry);
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
            var currentEntrySize = CurrentEntrySize;
            var newEntrySize = CalcEntrySize(KeyLen, newSize);
            int withValuePtr = 0;
            if (HasValueSectorPtr && newSize > MaxValueLenInline)
            {
                withValuePtr = KeyValueDB.PtrDownSize;
            }
            // preserves all before current item including current sizes + key
            int ofs = EntryOffset + PackUnpack.LengthVUInt((uint)KeyLen);
            if (!ReferenceEquals(Data,newData)) Array.Copy(Data, 0, newData, 0, ofs);
            if (newSize>ValueLen) // because resize could be inplace bytes have to be copied correct order
            {
                // preserves all after current item
                Array.Copy(Data,
                           EntryOffset + currentEntrySize - withValuePtr,
                           newData,
                           EntryOffset + newEntrySize - withValuePtr,
                           TotalLength - EntryOffset - currentEntrySize + withValuePtr);
                // preserves key of current item
                Array.Copy(Data, KeyOffset, newData, ofs + PackUnpack.LengthVUInt((ulong)newSize), ValueOffset - KeyOffset);
            }
            else
            {
                // preserves key of current item
                Array.Copy(Data, KeyOffset, newData, ofs + PackUnpack.LengthVUInt((ulong)newSize), ValueOffset - KeyOffset);
                // preserves all after current item
                Array.Copy(Data,
                           EntryOffset + currentEntrySize - withValuePtr,
                           newData,
                           EntryOffset + newEntrySize - withValuePtr,
                           TotalLength - EntryOffset - currentEntrySize + withValuePtr);
            }
            PackUnpack.PackVUInt(newData, ref ofs, (ulong)newSize);
            _data = newData;
            _valueLen = newSize;
            _ofsAfterKeyAndValueLen = ofs;
            var delta = newEntrySize - currentEntrySize;
            for (int i = _pos; i < _count; i++)
            {
                var o = HeaderSize + HeaderForEntry * i;
                PackUnpack.PackUInt16LE(_data, o, (ushort)(PackUnpack.UnpackUInt16LE(_data, o) + delta));
            }
        }

        internal int AddEntry(int additionalLengthNeeded, byte[] newData, int entryIndex)
        {
            var newCount = Count + 1;
            SetCountToSectorData(newData, newCount);
            var insertOfs = OffsetOfIndex(entryIndex);
            Array.Copy(Data, HeaderSize, newData, HeaderSize, entryIndex * HeaderForEntry);
            var additionalLengthNeededWOHeader = additionalLengthNeeded - HeaderForEntry;
            Array.Copy(Data, insertOfs, newData, insertOfs + additionalLengthNeeded,
                       TotalLength - insertOfs);
            Array.Copy(
                Data,
                HeaderSize + Count * HeaderForEntry,
                newData,
                HeaderSize + newCount * HeaderForEntry,
                insertOfs - (HeaderSize + Count * HeaderForEntry));
            for (int i = newCount - 1; i >= entryIndex; i--)
            {
                ushort o;
                if (i == 0)
                    o = (ushort)additionalLengthNeededWOHeader;
                else
                    o = (ushort)(PackUnpack.UnpackUInt16LE(Data, HeaderSize + (i - 1) * HeaderForEntry) + additionalLengthNeededWOHeader);
                PackUnpack.PackUInt16LE(newData, HeaderSize + i * HeaderForEntry, o);
            }
            return insertOfs + HeaderForEntry;
        }

        internal static void SetOneEntryCount(byte[] data, int entrySize)
        {
            SetCountToSectorData(data, 1);
            PackUnpack.PackUInt16LE(data, HeaderSize, (ushort)entrySize);
        }

        internal static void RecalculateHeader(byte[] data, int count)
        {
            var ofs1 = HeaderSize + HeaderForEntry * count;
            var ofs = ofs1;
            for (int i = 0; i < count; i++)
            {
                var keyLen = (int)PackUnpack.UnpackVUInt(data, ref ofs);
                var valueLen = (long)PackUnpack.UnpackVUInt(data, ref ofs);
                ofs += CalcEntrySizeWOLengths(keyLen, valueLen);
                PackUnpack.PackUInt16LE(data, HeaderSize + HeaderForEntry * i, (ushort)(ofs - ofs1));
            }
        }
    }
}
