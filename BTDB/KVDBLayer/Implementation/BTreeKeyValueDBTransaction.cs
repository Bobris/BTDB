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
        internal IRootNode? BTreeRoot { get; private set; }
        readonly ICursor _cursor;
        ICursor? _cursor2;
        readonly bool _readOnly;
        bool _writing;
        bool _preapprovedWriting;
        bool _temporaryCloseTransactionLog;
        long _keyIndex;
        long _cursorMovedCounter;

        public BTreeKeyValueDBTransaction(BTreeKeyValueDB keyValueDB, IRootNode artRoot, bool writing, bool readOnly)
        {
            _preapprovedWriting = writing;
            _readOnly = readOnly;
            _keyValueDB = keyValueDB;
            _keyIndex = -1;
            _cursor = artRoot.CreateCursor();
            _cursor2 = null;
            BTreeRoot = artRoot;
            _cursorMovedCounter = 0;
        }

        ~BTreeKeyValueDBTransaction()
        {
            if (BTreeRoot != null)
            {
                Dispose();
                _keyValueDB.Logger?.ReportTransactionLeak(this);
            }
        }

        public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
        {
            _cursorMovedCounter++;
            if (_cursor.FindFirst(prefix))
            {
                _keyIndex = _cursor.CalcIndex();
                return true;
            }
            _keyIndex = -1;
            return false;
        }

        public bool FindLastKey(in ReadOnlySpan<byte> prefix)
        {
            _cursorMovedCounter++;
            _keyIndex = _cursor.FindLastWithPrefix(prefix);
            return _keyIndex >= 0;
        }

        public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
        {
            if (_keyIndex == -1) return FindLastKey(prefix);
            _cursorMovedCounter++;
            if (_cursor.MovePrevious())
            {
                if (_cursor.KeyHasPrefix(prefix))
                {
                    _keyIndex--;
                    return true;
                }
            }
            InvalidateCurrentKey();
            return false;
        }

        public bool FindNextKey(in ReadOnlySpan<byte> prefix)
        {
            if (_keyIndex == -1) return FindFirstKey(prefix);
            _cursorMovedCounter++;
            if (_cursor.MoveNext())
            {
                if (_cursor.KeyHasPrefix(prefix))
                {
                    _keyIndex++;
                    return true;
                }
            }
            InvalidateCurrentKey();
            return false;
        }

        public FindResult Find(in ReadOnlySpan<byte> key, uint prefixLen)
        {
            _cursorMovedCounter++;
            var result = _cursor.Find(key);
            _keyIndex = _cursor.CalcIndex();
            if (prefixLen == 0) return result;
            switch (result)
            {
                case FindResult.Previous when !_cursor.KeyHasPrefix(key.Slice(0,(int)prefixLen)):
                {
                    if (!_cursor.MoveNext())
                    {
                        return FindResult.NotFound;
                    }

                    if (_cursor.KeyHasPrefix(key.Slice(0,(int)prefixLen)))
                    {
                        _keyIndex++;
                        return FindResult.Next;
                    }

                    InvalidateCurrentKey();
                    return FindResult.NotFound;
                }
                case FindResult.Next when !_cursor.KeyHasPrefix(key.Slice(0,(int)prefixLen)):
                    // FindResult.Previous is preferred that's why it has to be NotFound when next does not match prefix
                    InvalidateCurrentKey();
                    return FindResult.NotFound;
                default:
                    return result;
            }
        }

        public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
        {
            _cursorMovedCounter++;
            MakeWritable();
            Span<byte> trueValue = stackalloc byte[12];
            _keyValueDB.WriteCreateOrUpdateCommand(key, value, trueValue);
            var result = _cursor.Upsert(key, trueValue);
            _keyIndex = _cursor.CalcIndex();
            return result;
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
            BTreeRoot = _keyValueDB.MakeWritableTransaction(this, BTreeRoot!);
            _cursor.SetNewRoot(BTreeRoot);
            _cursor2?.SetNewRoot(BTreeRoot);
            BTreeRoot.DescriptionForLeaks = _descriptionForLeaks;
            _writing = true;
            _keyValueDB.WriteStartTransaction();
        }

        public long GetKeyValueCount()
        {
            return BTreeRoot!.GetCount();
        }

        public long GetKeyIndex()
        {
            return _keyIndex;
        }

        public bool SetKeyIndex(long index)
        {
            _cursorMovedCounter++;
            _keyIndex = index;
            if (_cursor.SeekIndex(index)) return true;
            InvalidateCurrentKey();
            return false;
        }

        public bool SetKeyIndex(in ReadOnlySpan<byte> prefix, long index)
        {
            _cursorMovedCounter++;
            if (!_cursor.FindFirst(prefix))
            {
                InvalidateCurrentKey();
                return false;
            }

            index += _cursor.CalcIndex();
            if (_cursor.SeekIndex(index))
            {
                _keyIndex = index;
                if (_cursor.KeyHasPrefix(prefix))
                    return true;
            }
            InvalidateCurrentKey();
            return false;
        }

        ReadOnlySpan<byte> GetCurrentKeyFromStack()
        {
            var result = new byte[_cursor.GetKeyLength()];
            _cursor.FillByKey(result);
            return result;
        }

        public void InvalidateCurrentKey()
        {
            _cursorMovedCounter++;
            _keyIndex = -1;
            _cursor.Invalidate();
        }

        public bool IsValidKey()
        {
            return _keyIndex != -1;
        }

        public ReadOnlySpan<byte> GetKeyAsReadOnlySpan()
        {
            if (!IsValidKey()) return new ReadOnlySpan<byte>();
            return GetCurrentKeyFromStack();
        }

        public ByteBuffer GetKeyIncludingPrefix()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            return ByteBuffer.NewAsync(GetCurrentKeyFromStack());
        }

        public ReadOnlySpan<byte> GetClonedValue(ref byte buffer, int bufferLength)
        {
            if (!IsValidKey()) return new ReadOnlySpan<byte>();
            var trueValue = _cursor.GetValue();
            try
            {
                return _keyValueDB.ReadValue(trueValue, ref buffer, bufferLength);
            }
            catch (BTDBException ex)
            {
                var oldestRoot = (IRootNode)_keyValueDB.ReferenceAndGetOldestRoot();
                var lastCommitted = (IRootNode)_keyValueDB.ReferenceAndGetLastCommitted();
                try
                {
                    throw new BTDBException($"GetValue failed in TrId:{BTreeRoot!.TransactionId},TRL:{BTreeRoot.TrLogFileId},Ofs:{BTreeRoot.TrLogOffset},ComUlong:{BTreeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong} innerMessage:{ex.Message}", ex);
                }
                finally
                {
                    _keyValueDB.DereferenceRootNodeInternal(oldestRoot);
                    _keyValueDB.DereferenceRootNodeInternal(lastCommitted);
                }
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
                    throw new BTDBException($"GetValue failed in TrId:{BTreeRoot!.TransactionId},TRL:{BTreeRoot.TrLogFileId},Ofs:{BTreeRoot.TrLogOffset},ComUlong:{BTreeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong} innerMessage:{ex.Message}", ex);
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
            if (_keyIndex == -1)
            {
                throw new InvalidOperationException("Current key is not valid");
            }
        }

        public void SetValue(in ReadOnlySpan<byte> value)
        {
            EnsureValidKey();
            MakeWritable();
            Span<byte> trueValue = stackalloc byte[12];
            _keyValueDB.WriteCreateOrUpdateCommand(GetCurrentKeyFromStack(), value, trueValue);
            _cursor.WriteValue(trueValue);
        }

        public void EraseCurrent()
        {
            EnsureValidKey();
            MakeWritable();
            _keyValueDB.WriteEraseOneCommand(GetCurrentKeyFromStack());
            _cursor.Erase();
            InvalidateCurrentKey();
        }

        public bool EraseCurrent(in ReadOnlySpan<byte> exactKey)
        {
            if (_cursor.Find(exactKey) != FindResult.Exact)
            {
                InvalidateCurrentKey();
                return false;
            }
            MakeWritable();
            _keyValueDB.WriteEraseOneCommand(exactKey);
            _cursor.Erase();
            InvalidateCurrentKey();
            return true;
        }

        public bool EraseCurrent(in ReadOnlySpan<byte> exactKey, ref byte buffer, int bufferLength, out ReadOnlySpan<byte> value)
        {
            if (_cursor.Find(exactKey) != FindResult.Exact)
            {
                InvalidateCurrentKey();
                value = ReadOnlySpan<byte>.Empty;
                return false;
            }

            _keyIndex = 0; // fake value key index is enough to pass IsValidKey test
            value = GetClonedValue(ref buffer, bufferLength);
            MakeWritable();
            _keyValueDB.WriteEraseOneCommand(exactKey);
            _cursor.Erase();
            InvalidateCurrentKey();
            return true;
        }

        public void EraseAll()
        {
            EraseRange(0, GetKeyValueCount() - 1);
        }

        public void EraseRange(long firstKeyIndex, long lastKeyIndex)
        {
            if (firstKeyIndex < 0) firstKeyIndex = 0;
            if (lastKeyIndex >= GetKeyValueCount()) lastKeyIndex = GetKeyValueCount() - 1;
            if (lastKeyIndex < firstKeyIndex) return;
            MakeWritable();
            _cursor.SeekIndex(firstKeyIndex);
            if (lastKeyIndex != firstKeyIndex)
            {
                _cursor2 ??= BTreeRoot!.CreateCursor();
                _cursor2.SeekIndex(lastKeyIndex);
                var firstKey = GetCurrentKeyFromStack();
                var secondKey = new byte[_cursor2!.GetKeyLength()];
                _cursor2.FillByKey(secondKey);
                _keyValueDB.WriteEraseRangeCommand(firstKey, secondKey);
                _cursor.EraseTo(_cursor2);
            }
            else
            {
                _keyValueDB.WriteEraseOneCommand(GetCurrentKeyFromStack());
                _cursor.Erase();
            }
            InvalidateCurrentKey();
        }

        public bool IsWriting()
        {
            return _writing || _preapprovedWriting;
        }

        public bool IsReadOnly()
        {
            return _readOnly;
        }

        public ulong GetCommitUlong()
        {
            return BTreeRoot!.CommitUlong;
        }

        public void SetCommitUlong(ulong value)
        {
            if (BTreeRoot!.CommitUlong != value)
            {
                MakeWritable();
                BTreeRoot!.CommitUlong = value;
            }
        }

        public void NextCommitTemporaryCloseTransactionLog()
        {
            MakeWritable();
            _temporaryCloseTransactionLog = true;
        }

        internal void CommitFromCompactor()
        {
            if (BTreeRoot == null) throw new BTDBException("Transaction already committed or disposed");
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
                _keyValueDB.RevertWritingTransaction(currentArtRoot!, true);
            }
            else if (_writing)
            {
                _keyValueDB.CommitWritingTransaction(currentArtRoot!, _temporaryCloseTransactionLog);
                _writing = false;
            }
            else
            {
                _keyValueDB.DereferenceRoot(currentArtRoot!);
            }
        }

        public void Dispose()
        {
            var currentArtRoot = BTreeRoot;
            BTreeRoot = null;
            if (_writing || _preapprovedWriting)
            {
                _keyValueDB.RevertWritingTransaction(currentArtRoot!, _preapprovedWriting);
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
            return BTreeRoot!.TransactionId;
        }

        public long CursorMovedCounter => _cursorMovedCounter;

        public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
        {
            if (!IsValidKey()) return new KeyValuePair<uint, uint>();
            var keyLen = _cursor.GetKeyLength();
            var trueValue = _cursor.GetValue();
            return new KeyValuePair<uint, uint>(
                (uint)keyLen,
                _keyValueDB.CalcValueSize(MemoryMarshal.Read<uint>(trueValue), MemoryMarshal.Read<uint>(trueValue.Slice(4)), MemoryMarshal.Read<int>(trueValue.Slice(8))));
        }

        public ulong GetUlong(uint idx)
        {
            return BTreeRoot!.GetUlong(idx);
        }

        public void SetUlong(uint idx, ulong value)
        {
            if (BTreeRoot!.GetUlong(idx) != value)
            {
                MakeWritable();
                BTreeRoot!.SetUlong(idx, value);
            }
        }

        public uint GetUlongCount()
        {
            return BTreeRoot!.GetUlongCount();
        }

        string? _descriptionForLeaks;

        public string? DescriptionForLeaks
        {
            get => _descriptionForLeaks;
            set
            {
                _descriptionForLeaks = value;
                if (_preapprovedWriting || _writing) BTreeRoot!.DescriptionForLeaks = value;
            }
        }

        public IKeyValueDB Owner => _keyValueDB;

        public bool RollbackAdvised { get; set; }
    }
}
