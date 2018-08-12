using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.ARTLib;

namespace BTDB.KVDBLayer
{
    class ArtInMemoryKeyValueDBTransaction : IKeyValueDBTransaction
    {
        readonly ArtInMemoryKeyValueDB _keyValueDB;
        IRootNode _artRoot;
        ICursor _cursor;
        ICursor _cursor2;
        byte[] _prefix;
        bool _writting;
        readonly bool _readOnly;
        bool _preapprovedWritting;
        long _prefixKeyStart;
        long _prefixKeyCount;
        long _keyIndex;

        public ArtInMemoryKeyValueDBTransaction(ArtInMemoryKeyValueDB keyValueDB, IRootNode artRoot, bool writting, bool readOnly)
        {
            _preapprovedWritting = writting;
            _readOnly = readOnly;
            _keyValueDB = keyValueDB;
            _artRoot = artRoot;
            _prefix = BitArrayManipulation.EmptyByteArray;
            _prefixKeyStart = 0;
            _prefixKeyCount = -1;
            _keyIndex = -1;
            _cursor = _artRoot.CreateCursor();
        }

        internal IRootNode ArtRoot => _artRoot;

        public void SetKeyPrefix(ByteBuffer prefix)
        {
            _prefix = prefix.ToByteArray();
            if (_prefix.Length == 0)
            {
                _prefixKeyStart = 0;
            }
            else
            {
                _prefixKeyStart = -1;
            }
            _prefixKeyCount = -1;
            InvalidateCurrentKey();
        }

        public bool FindFirstKey()
        {
            return SetKeyIndex(0);
        }

        public bool FindLastKey()
        {
            var count = GetKeyValueCount();
            if (count <= 0) return false;
            return SetKeyIndex(count - 1);
        }

        public bool FindPreviousKey()
        {
            if (_keyIndex < 0) return FindLastKey();
            if (_cursor.MovePrevious())
            {
                if (_cursor.KeyHasPrefix(_prefix))
                {
                    _keyIndex--;
                    return true;
                }
            }
            InvalidateCurrentKey();
            return false;
        }

        public bool FindNextKey()
        {
            if (_keyIndex < 0) return FindFirstKey();
            if (_cursor.MoveNext())
            {
                if (_cursor.KeyHasPrefix(_prefix))
                {
                    _keyIndex++;
                    return true;
                }
            }
            InvalidateCurrentKey();
            return false;
        }

        public FindResult Find(ByteBuffer key)
        {
            var result = _cursor.Find(_prefix, key.AsSyncReadOnlySpan());
            _keyIndex = _cursor.CalcIndex();
            return result;
        }

        public bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value)
        {
            MakeWrittable();
            bool result;
            var keyLen = _prefix.Length + key.Length;
            if (_prefix.Length == 0)
            {
                result = _cursor.Upsert(key.AsSyncReadOnlySpan(), value.AsSyncReadOnlySpan());
            }
            else if (key.Length == 0)
            {
                result = _cursor.Upsert(_prefix, value.AsSyncReadOnlySpan());
            }
            else
            {
                Span<byte> temp = keyLen<256 ? stackalloc byte[keyLen]: new byte[keyLen];
                _prefix.CopyTo(temp);
                key.AsSyncReadOnlySpan().CopyTo(temp.Slice(_prefix.Length));
                result = _cursor.Upsert(_prefix, value.AsSyncReadOnlySpan());
            }
            _keyIndex = _cursor.CalcIndex();
            if (result && _prefixKeyCount >= 0) _prefixKeyCount++;
            return result;
        }

        void MakeWrittable()
        {
            if (_writting) return;
            if (_preapprovedWritting)
            {
                _writting = true;
                _preapprovedWritting = false;
                return;
            }
            if (_readOnly)
            {
                throw new BTDBTransactionRetryException("Cannot write from readOnly transaction");
            }
            var oldArtRoot = _artRoot;
            _artRoot = _keyValueDB.MakeWrittableTransaction(this, oldArtRoot);
            _cursor = _artRoot.CreateCursor();
            _cursor2 = null;
            _artRoot.DescriptionForLeaks = _descriptionForLeaks;
            _writting = true;
            InvalidateCurrentKey();
        }

        public long GetKeyValueCount()
        {
            if (_prefixKeyCount >= 0) return _prefixKeyCount;
            if (_prefix.Length == 0)
            {
                _prefixKeyCount = _artRoot.GetCount();
                return _prefixKeyCount;
            }
            CalcPrefixKeyStart();
            if (_prefixKeyStart < 0)
            {
                _prefixKeyCount = 0;
                return 0;
            }
            if (_cursor2==null)
            {
                _cursor2 = _artRoot.CreateCursor();
            }
            _cursor2.FindLast(_prefix);
            _prefixKeyCount = _cursor2.CalcIndex() - _prefixKeyStart + 1;
            return _prefixKeyCount;
        }

        public long GetKeyIndex()
        {
            if (_keyIndex < 0) return -1;
            CalcPrefixKeyStart();
            return _keyIndex - _prefixKeyStart;
        }

        void CalcPrefixKeyStart()
        {
            if (_prefixKeyStart >= 0) return;
            if (_cursor2==null)
            {
                _cursor2 = _artRoot.CreateCursor();
            }
            if (_cursor2.FindFirst(_prefix))
            {
                _prefixKeyStart = _cursor2.CalcIndex();
            }
            else
            {
                _prefixKeyStart = -1;
            }
        }

        public bool SetKeyIndex(long index)
        {
            CalcPrefixKeyStart();
            if (_prefixKeyStart < 0)
            {
                InvalidateCurrentKey();
                return false;
            }
            _keyIndex = index + _prefixKeyStart;
            if (!_cursor.SeekIndex(_keyIndex))
            {
                InvalidateCurrentKey();
                return false;
            }
            if (_cursor.KeyHasPrefix(_prefix))
            {
                return true;
            }
            InvalidateCurrentKey();
            return false;
        }

        ByteBuffer GetCurrentKeyFromStack()
        {
            var result = ByteBuffer.NewAsync(new byte[_cursor.GetKeyLength()]);
            _cursor.FillByKey(result.AsSyncSpan());
            return result;
        }

        public void InvalidateCurrentKey()
        {
            _keyIndex = -1;
            _cursor.Invalidate();
        }

        public bool IsValidKey()
        {
            return _keyIndex >= 0;
        }

        public ByteBuffer GetKey()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            var wholeKey = GetCurrentKeyFromStack();
            return ByteBuffer.NewAsync(wholeKey.Buffer, wholeKey.Offset + _prefix.Length, wholeKey.Length - _prefix.Length);
        }

        public ByteBuffer GetValue()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            return ByteBuffer.NewAsync(_cursor.GetValue());
        }

        void EnsureValidKey()
        {
            if (_keyIndex < 0)
            {
                throw new InvalidOperationException("Current key is not valid");
            }
        }

        public void SetValue(ByteBuffer value)
        {
            EnsureValidKey();
            var keyIndexBackup = _keyIndex;
            MakeWrittable();
            if (_keyIndex != keyIndexBackup)
            {
                _keyIndex = keyIndexBackup;
                _cursor.SeekIndex(_keyIndex);
            }
            _cursor.WriteValue(value.AsSyncReadOnlySpan());
        }

        public void EraseCurrent()
        {
            EnsureValidKey();
            var keyIndex = _keyIndex;
            MakeWrittable();
            _cursor.SeekIndex(keyIndex);
            _cursor.Erase();
            InvalidateCurrentKey();
            if (_prefixKeyCount>=0) _prefixKeyCount--;
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
            MakeWrittable();
            firstKeyIndex += _prefixKeyStart;
            lastKeyIndex += _prefixKeyStart;
            if (_cursor2 == null)
            {
                _cursor2 = _artRoot.CreateCursor();
            }
            _cursor.SeekIndex(firstKeyIndex);
            _cursor2.SeekIndex(lastKeyIndex);
            _cursor.EraseTo(_cursor2);
            InvalidateCurrentKey();
            _prefixKeyCount -= lastKeyIndex - firstKeyIndex + 1;
        }

        public bool IsWritting()
        {
            return _writting;
        }

        public ulong GetCommitUlong()
        {
            return _artRoot.CommitUlong;
        }

        public void SetCommitUlong(ulong value)
        {
            if (_artRoot.CommitUlong != value)
            {
                MakeWrittable();
                _artRoot.CommitUlong = value;
            }
        }

        public void NextCommitTemporaryCloseTransactionLog()
        {
            // There is no transaction log ...
        }

        public void Commit()
        {
            if (_artRoot == null) throw new BTDBException("Transaction already commited or disposed");
            InvalidateCurrentKey();
            var currentArtRoot = _artRoot;
            _artRoot = null;
            if (_preapprovedWritting)
            {
                _preapprovedWritting = false;
                _keyValueDB.RevertWrittingTransaction();
            }
            else if (_writting)
            {
                _keyValueDB.CommitWrittingTransaction(currentArtRoot);
                _writting = false;
            }
        }

        public void Dispose()
        {
            if (_writting || _preapprovedWritting)
            {
                _keyValueDB.RevertWrittingTransaction();
                _writting = false;
                _preapprovedWritting = false;
            }
            _artRoot = null;
        }

        public long GetTransactionNumber()
        {
            return _artRoot.TransactionId;
        }

        public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
        {
            return new KeyValuePair<uint, uint>((uint)_cursor.GetKeyLength(), (uint)_cursor.GetValueLength());
        }

        public byte[] GetKeyPrefix()
        {
            return _prefix;
        }

        public ulong GetUlong(uint idx)
        {
            return _artRoot.GetUlong(idx);
        }

        public void SetUlong(uint idx, ulong value)
        {
            if (_artRoot.GetUlong(idx) != value)
            {
                MakeWrittable();
                _artRoot.SetUlong(idx, value);
            }
        }

        public uint GetUlongCount()
        {
            return _artRoot.GetUlongCount();
        }

        string _descriptionForLeaks;
        public string DescriptionForLeaks
        {
            get { return _descriptionForLeaks; }
            set
            {
                _descriptionForLeaks = value;
                if (_preapprovedWritting || _writting) _artRoot.DescriptionForLeaks = value;
            }
        }

        public bool RollbackAdvised { get; set; }
    }
}