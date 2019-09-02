using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BTDB.BTreeLib;
using BTDB.Buffer;

namespace BTDB.KVDBLayer
{
    class BTreeKeyValueDBTransaction : IKeyValueDBTransaction
    {
        readonly BTreeKeyValueDB _keyValueDB;
        internal IRootNode BTreeRoot { get; private set; }
        ICursor _cursor;
        ICursor _cursor2;
        byte[] _prefix;
        bool _readOnly;
        bool _writing;
        bool _preapprovedWriting;
        long _prefixKeyStart;
        long _prefixKeyCount;
        long _keyIndex;
        bool _temporaryCloseTransactionLog;

        public BTreeKeyValueDBTransaction(BTreeKeyValueDB keyValueDB, IRootNode artRoot, bool writing, bool readOnly)
        {
            _preapprovedWriting = writing;
            _readOnly = readOnly;
            _keyValueDB = keyValueDB;
            _prefix = Array.Empty<byte>();
            _prefixKeyStart = 0;
            _prefixKeyCount = -1;
            _keyIndex = -1;
            _cursor = artRoot.CreateCursor();
            _cursor2 = null;
            BTreeRoot = artRoot;
        }

        ~BTreeKeyValueDBTransaction()
        {
            if (BTreeRoot != null)
            {
                Dispose();
                _keyValueDB.Logger?.ReportTransactionLeak(this);
            }
        }

        public void SetKeyPrefix(ByteBuffer prefix)
        {
            _prefix = prefix.ToByteArray();
            _prefixKeyStart = _prefix.Length == 0 ? 0 : -1;
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
            if (!_cursor.IsValid()) return FindLastKey();
            if (_cursor.MovePrevious())
            {
                if (_cursor.KeyHasPrefix(_prefix))
                {
                    if (_keyIndex >= 0)
                        _keyIndex--;
                    return true;
                }
            }
            InvalidateCurrentKey();
            return false;
        }

        public bool FindNextKey()
        {
            if (!_cursor.IsValid()) return FindFirstKey();
            if (_cursor.MoveNext())
            {
                if (_cursor.KeyHasPrefix(_prefix))
                {
                    if (_keyIndex >= 0)
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
            _keyIndex = -1;
            return result;
        }

        public bool CreateOrUpdateKeyValue(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            MakeWritable();
            Span<byte> trueValue = stackalloc byte[12];
            bool result;
            var keyLen = _prefix.Length + key.Length;
            if (_prefix.Length == 0)
            {
                _keyValueDB.WriteCreateOrUpdateCommand(key, value, trueValue);
                result = _cursor.Upsert(key, trueValue);
            }
            else if (key.Length == 0)
            {
                _keyValueDB.WriteCreateOrUpdateCommand(_prefix, value, trueValue);
                result = _cursor.Upsert(_prefix, trueValue);
            }
            else
            {
                var temp = keyLen < 256 ? stackalloc byte[keyLen] : new byte[keyLen];
                _prefix.CopyTo(temp);
                key.CopyTo(temp.Slice(_prefix.Length));
                _keyValueDB.WriteCreateOrUpdateCommand(temp, value, trueValue);
                result = _cursor.Upsert(temp, trueValue);
            }
            _keyIndex = -1;
            if (result && _prefixKeyCount >= 0) _prefixKeyCount++;
            return result;
        }

        public bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value)
        {
            return CreateOrUpdateKeyValue(key.AsSyncReadOnlySpan(), value.AsSyncReadOnlySpan());
        }

        void MakeWritable()
        {
            if (_writing) return;
            if (_preapprovedWriting)
            {
                _writing = true;
                _preapprovedWriting = false;
                _keyValueDB.WriteStartTransaction();
                return;
            }
            if (_readOnly)
            {
                throw new BTDBTransactionRetryException("Cannot write from readOnly transaction");
            }
            BTreeRoot = _keyValueDB.MakeWritableTransaction(this, BTreeRoot);
            _cursor.SetNewRoot(BTreeRoot);
            _cursor2?.SetNewRoot(BTreeRoot);
            BTreeRoot.DescriptionForLeaks = _descriptionForLeaks;
            _writing = true;
            _keyValueDB.WriteStartTransaction();
        }

        public long GetKeyValueCount()
        {
            if (_prefixKeyCount >= 0) return _prefixKeyCount;
            if (_prefix.Length == 0)
            {
                _prefixKeyCount = BTreeRoot.GetCount();
                return _prefixKeyCount;
            }
            CalcPrefixKeyStart();
            if (_prefixKeyStart < 0)
            {
                _prefixKeyCount = 0;
                return 0;
            }
            if (_cursor2 == null)
            {
                _cursor2 = BTreeRoot.CreateCursor();
            }
            _cursor2.FindLast(_prefix);
            _prefixKeyCount = _cursor2.CalcIndex() - _prefixKeyStart + 1;
            return _prefixKeyCount;
        }

        public long GetKeyIndex()
        {
            if (_keyIndex < 0)
            {
                if (!_cursor.IsValid())
                    return -1;
                _keyIndex = _cursor.CalcIndex();
            }
            CalcPrefixKeyStart();
            return _keyIndex - _prefixKeyStart;
        }

        void CalcPrefixKeyStart()
        {
            if (_prefixKeyStart >= 0) return;
            if (_cursor2 == null)
            {
                _cursor2 = BTreeRoot.CreateCursor();
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
            return _cursor.IsValid();
        }

        public ByteBuffer GetKey()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            var wholeKey = GetCurrentKeyFromStack();
            return ByteBuffer.NewAsync(wholeKey.Buffer, wholeKey.Offset + _prefix.Length, wholeKey.Length - _prefix.Length);
        }

        public ByteBuffer GetKeyIncludingPrefix()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            return GetCurrentKeyFromStack();
        }

        public ByteBuffer GetValue()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            var trueValue = _cursor.GetValue();
            try
            {
                return ByteBuffer.NewAsync(_keyValueDB.ReadValue(trueValue));
            }
            catch (BTDBException ex)
            {
                var oldestRoot = (IRootNode)_keyValueDB.ReferenceAndGetOldestRoot();
                var lastCommitted = _keyValueDB._lastCommitted;
                throw new BTDBException($"GetValue failed in TrId:{BTreeRoot.TransactionId},TRL:{BTreeRoot.TrLogFileId},Ofs:{BTreeRoot.TrLogOffset},ComUlong:{BTreeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong} innerMessage:{ex.Message}", ex);
            }
        }

        public ReadOnlySpan<byte> GetValueAsReadOnlySpan()
        {
            if (!IsValidKey()) return new ReadOnlySpan<byte>();
            var trueValue = _cursor.GetValue();
            try
            {
                return _keyValueDB.ReadValue(trueValue);
            }
            catch (BTDBException ex)
            {
                var oldestRoot = (IRootNode)_keyValueDB.ReferenceAndGetOldestRoot();
                var lastCommitted = (IRootNode)_keyValueDB.ReferenceAndGetLastCommitted();
                try
                {
                    throw new BTDBException($"GetValue failed in TrId:{BTreeRoot.TransactionId},TRL:{BTreeRoot.TrLogFileId},Ofs:{BTreeRoot.TrLogOffset},ComUlong:{BTreeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong} innerMessage:{ex.Message}", ex);
                }
                finally
                {
                    _keyValueDB.DereferenceRootNodeInternal(oldestRoot);
                    _keyValueDB.DereferenceRootNodeInternal(lastCommitted);
                }
            }
        }

        void EnsureValidKey()
        {
            if (!_cursor.IsValid())
            {
                throw new InvalidOperationException("Current key is not valid");
            }
        }

        public void SetValue(ByteBuffer value)
        {
            EnsureValidKey();
            MakeWritable();
            Span<byte> trueValue = stackalloc byte[12];
            _keyValueDB.WriteCreateOrUpdateCommand(GetCurrentKeyFromStack().AsSyncReadOnlySpan(), value.AsSyncReadOnlySpan(), trueValue);
            _cursor.WriteValue(trueValue);
        }

        public void EraseCurrent()
        {
            EnsureValidKey();
            MakeWritable();
            _keyValueDB.WriteEraseOneCommand(GetCurrentKeyFromStack());
            _cursor.Erase();
            InvalidateCurrentKey();
            if (_prefixKeyCount >= 0) _prefixKeyCount--;
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
            MakeWritable();
            firstKeyIndex += _prefixKeyStart;
            lastKeyIndex += _prefixKeyStart;
            _cursor.SeekIndex(firstKeyIndex);
            if (lastKeyIndex != firstKeyIndex)
            {
                if (_cursor2 == null)
                {
                    _cursor2 = BTreeRoot.CreateCursor();
                }
                _cursor2.SeekIndex(lastKeyIndex);
                var firstKey = GetCurrentKeyFromStack();
                var secondKey = ByteBuffer.NewAsync(new byte[_cursor2.GetKeyLength()]);
                _cursor2.FillByKey(secondKey.AsSyncSpan());
                _keyValueDB.WriteEraseRangeCommand(firstKey, secondKey);
                _cursor.EraseTo(_cursor2);
            }
            else
            {
                _keyValueDB.WriteEraseOneCommand(GetCurrentKeyFromStack());
                _cursor.Erase();
            }
            InvalidateCurrentKey();
            _prefixKeyCount -= lastKeyIndex - firstKeyIndex + 1;
        }

        public bool IsWriting()
        {
            return _writing;
        }

        public ulong GetCommitUlong()
        {
            return BTreeRoot.CommitUlong;
        }

        public void SetCommitUlong(ulong value)
        {
            if (BTreeRoot.CommitUlong != value)
            {
                MakeWritable();
                BTreeRoot.CommitUlong = value;
            }
        }

        public void NextCommitTemporaryCloseTransactionLog()
        {
            MakeWritable();
            _temporaryCloseTransactionLog = true;
        }

        internal void CommitFromCompactor()
        {
            if (BTreeRoot == null) throw new BTDBException("Transaction already commited or disposed");
            var currentArtRoot = BTreeRoot;
            BTreeRoot = null;
            _preapprovedWriting = false;
            _keyValueDB.CommitFromCompactor(currentArtRoot);
        }

        public void Commit()
        {
            if (BTreeRoot == null) throw new BTDBException("Transaction already committed or disposed");
            InvalidateCurrentKey();
            var currentArtRoot = BTreeRoot;
            BTreeRoot = null;
            if (_preapprovedWriting)
            {
                _preapprovedWriting = false;
                _keyValueDB.RevertWritingTransaction(currentArtRoot, true);
            }
            else if (_writing)
            {
                _keyValueDB.CommitWritingTransaction(currentArtRoot, _temporaryCloseTransactionLog);
                _writing = false;
            }
            else
            {
                _keyValueDB.DereferenceRoot(currentArtRoot);
            }
        }

        public void Dispose()
        {
            var currentArtRoot = BTreeRoot;
            BTreeRoot = null;
            if (_writing || _preapprovedWriting)
            {
                _keyValueDB.RevertWritingTransaction(currentArtRoot, _preapprovedWriting);
                _writing = false;
                _preapprovedWriting = false;
            }
            else if (currentArtRoot != null)
            {
                _keyValueDB.DereferenceRoot(currentArtRoot);
            }
            GC.SuppressFinalize(this);
        }

        public long GetTransactionNumber()
        {
            return BTreeRoot.TransactionId;
        }

        public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
        {
            if (!IsValidKey()) return new KeyValuePair<uint, uint>();
            var keyLen = _cursor.GetKeyLength();
            var trueValue = _cursor.GetValue();
            return new KeyValuePair<uint, uint>(
                (uint)keyLen,
                _keyValueDB.CalcValueSize(MemoryMarshal.Read<uint>(trueValue), MemoryMarshal.Read<uint>(trueValue.Slice(4)), MemoryMarshal.Read<int>(trueValue.Slice(8))));
        }

        public byte[] GetKeyPrefix()
        {
            return _prefix;
        }

        public ulong GetUlong(uint idx)
        {
            return BTreeRoot.GetUlong(idx);
        }

        public void SetUlong(uint idx, ulong value)
        {
            if (BTreeRoot.GetUlong(idx) != value)
            {
                MakeWritable();
                BTreeRoot.SetUlong(idx, value);
            }
        }

        public uint GetUlongCount()
        {
            return BTreeRoot.GetUlongCount();
        }

        string _descriptionForLeaks;
        public string DescriptionForLeaks
        {
            get { return _descriptionForLeaks; }
            set
            {
                _descriptionForLeaks = value;
                if (_preapprovedWriting || _writing) BTreeRoot.DescriptionForLeaks = value;
            }
        }

        public bool RollbackAdvised { get; set; }
    }
}
