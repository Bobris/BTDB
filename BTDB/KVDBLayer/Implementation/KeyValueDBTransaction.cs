using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KVDBLayer.BTree;

namespace BTDB.KVDBLayer
{
    class KeyValueDBTransaction : IKeyValueDBTransaction
    {
        readonly KeyValueDB _keyValueDB;
        IBTreeRootNode? _btreeRoot;
        readonly List<NodeIdxPair> _stack = new List<NodeIdxPair>();
        bool _writing;
        readonly bool _readOnly;
        bool _preapprovedWriting;
        bool _temporaryCloseTransactionLog;
        long _keyIndex;

        public KeyValueDBTransaction(KeyValueDB keyValueDB, IBTreeRootNode btreeRoot, bool writing, bool readOnly)
        {
            _preapprovedWriting = writing;
            _readOnly = readOnly;
            _keyValueDB = keyValueDB;
            _btreeRoot = btreeRoot;
            _keyIndex = -1;
            _keyValueDB.StartedUsingBTreeRoot(_btreeRoot);
        }

        ~KeyValueDBTransaction()
        {
            if (_btreeRoot != null)
            {
                Dispose();
                _keyValueDB.Logger?.ReportTransactionLeak(this);
            }
        }

        internal IBTreeRootNode? BtreeRoot => _btreeRoot;

        public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
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
            if (_btreeRoot!.FindPreviousKey(_stack))
            {
                if (CheckPrefixIn(GetCurrentKeyFromStack()))
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
            if (_btreeRoot!.FindNextKey(_stack))
            {
                if (CheckPrefixIn(GetCurrentKeyFromStack()))
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
            return _btreeRoot!.FindKey(_stack, out _keyIndex, _prefix, key);
        }

        public bool CreateOrUpdateKeyValue(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            return CreateOrUpdateKeyValue(ByteBuffer.NewAsync(key), ByteBuffer.NewAsync(value));
        }

        public bool CreateOrUpdateKeyValue(ByteBuffer key, ByteBuffer value)
        {
            MakeWritable();
            _keyValueDB.WriteCreateOrUpdateCommand(_prefix, key, value, out var valueFileId, out var valueOfs, out var valueSize);
            var ctx = new CreateOrUpdateCtx
            {
                KeyPrefix = _prefix,
                Key = key,
                ValueFileId = valueFileId,
                ValueOfs = valueOfs,
                ValueSize = valueSize,
                Stack = _stack
            };
            _btreeRoot!.CreateOrUpdate(ctx);
            _keyIndex = ctx.KeyIndex;
            if (ctx.Created && _prefixKeyCount >= 0) _prefixKeyCount++;
            return ctx.Created;
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
            var oldBTreeRoot = _btreeRoot;
            _btreeRoot = _keyValueDB.MakeWritableTransaction(this, oldBTreeRoot!);
            _keyValueDB.StartedUsingBTreeRoot(_btreeRoot);
            _keyValueDB.FinishedUsingBTreeRoot(oldBTreeRoot);
            _btreeRoot.DescriptionForLeaks = _descriptionForLeaks;
            _writing = true;
            InvalidateCurrentKey();
            _keyValueDB.WriteStartTransaction();
        }

        public long GetKeyValueCount()
        {
            if (_prefixKeyCount >= 0) return _prefixKeyCount;
            if (_prefix.Length == 0)
            {
                _prefixKeyCount = _btreeRoot!.CalcKeyCount();
                return _prefixKeyCount;
            }
            CalcPrefixKeyStart();
            if (_prefixKeyStart < 0)
            {
                _prefixKeyCount = 0;
                return 0;
            }
            _prefixKeyCount = _btreeRoot!.FindLastWithPrefix(_prefix) - _prefixKeyStart + 1;
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
            if (_btreeRoot!.FindKey(new List<NodeIdxPair>(), out _prefixKeyStart, _prefix, ByteBuffer.NewEmpty()) == FindResult.NotFound)
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
            if (_keyIndex >= _btreeRoot!.CalcKeyCount())
            {
                InvalidateCurrentKey();
                return false;
            }
            _btreeRoot!.FillStackByIndex(_stack, _keyIndex);
            if (_prefixKeyCount >= 0)
                return true;
            var key = GetCurrentKeyFromStack();
            if (CheckPrefixIn(key))
            {
                return true;
            }
            InvalidateCurrentKey();
            return false;
        }

        bool CheckPrefixIn(ByteBuffer key)
        {
            return BTreeRoot.KeyStartsWithPrefix(_prefix, key);
        }

        ReadOnlySpan<byte> GetCurrentKeyFromStack()
        {
            var nodeIdxPair = _stack[^1];
            return ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx);
        }

        public void InvalidateCurrentKey()
        {
            _keyIndex = -1;
            _stack.Clear();
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

        public ByteBuffer GetKeyIncludingPrefix()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            return GetCurrentKeyFromStack();
        }

        public ByteBuffer GetValue()
        {
            if (!IsValidKey()) return ByteBuffer.NewEmpty();
            var nodeIdxPair = _stack[^1];
            var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
            try
            {
                return _keyValueDB.ReadValue(leafMember.ValueFileId, leafMember.ValueOfs, leafMember.ValueSize);
            }
            catch (BTDBException ex)
            {
                var oldestRoot = (IBTreeRootNode)_keyValueDB.ReferenceAndGetOldestRoot();
                var lastCommitted = (IBTreeRootNode)_keyValueDB.ReferenceAndGetLastCommitted();
                // no need to dereference roots because we know it is managed
                throw new BTDBException($"GetValue failed in TrId:{_btreeRoot!.TransactionId},TRL:{_btreeRoot!.TrLogFileId},Ofs:{_btreeRoot!.TrLogOffset},ComUlong:{_btreeRoot!.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong} innerMessage:{ex.Message}", ex);
            }
        }

        public ReadOnlySpan<byte> GetValueAsReadOnlySpan()
        {
            return GetValue().AsSyncReadOnlySpan();
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
            MakeWritable();
            if (_keyIndex != keyIndexBackup)
            {
                _keyIndex = keyIndexBackup;
                _btreeRoot!.FillStackByIndex(_stack, _keyIndex);
            }
            var nodeIdxPair = _stack[^1];
            var memberValue = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);
            var memberKey = ((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx);
            _keyValueDB.WriteCreateOrUpdateCommand(Array.Empty<byte>(), memberKey, value, out memberValue.ValueFileId, out memberValue.ValueOfs, out memberValue.ValueSize);
            ((IBTreeLeafNode)nodeIdxPair.Node).SetMemberValue(nodeIdxPair.Idx, memberValue);
        }

        public void EraseCurrent()
        {
            EnsureValidKey();
            var keyIndex = _keyIndex;
            MakeWritable();
            InvalidateCurrentKey();
            _prefixKeyCount--;
            _btreeRoot!.FillStackByIndex(_stack, keyIndex);
            _keyValueDB.WriteEraseOneCommand(GetCurrentKeyFromStack());
            _btreeRoot!.EraseRange(keyIndex, keyIndex);
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
            InvalidateCurrentKey();
            _prefixKeyCount -= lastKeyIndex - firstKeyIndex + 1;
            _btreeRoot!.FillStackByIndex(_stack, firstKeyIndex);
            if (firstKeyIndex == lastKeyIndex)
            {
                _keyValueDB.WriteEraseOneCommand(GetCurrentKeyFromStack());
            }
            else
            {
                var firstKey = GetCurrentKeyFromStack();
                _btreeRoot!.FillStackByIndex(_stack, lastKeyIndex);
                _keyValueDB.WriteEraseRangeCommand(firstKey, GetCurrentKeyFromStack());
            }
            _btreeRoot!.EraseRange(firstKeyIndex, lastKeyIndex);
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
            return _btreeRoot!.CommitUlong;
        }

        public void SetCommitUlong(ulong value)
        {
            if (_btreeRoot!.CommitUlong != value)
            {
                MakeWritable();
                _btreeRoot!.CommitUlong = value;
            }
        }

        public void NextCommitTemporaryCloseTransactionLog()
        {
            MakeWritable();
            _temporaryCloseTransactionLog = true;
        }

        public void Commit()
        {
            if (BtreeRoot == null) throw new BTDBException("Transaction already committed or disposed");
            InvalidateCurrentKey();
            var currentBtreeRoot = _btreeRoot;
            _keyValueDB.FinishedUsingBTreeRoot(_btreeRoot!);
            _btreeRoot = null;
            GC.SuppressFinalize(this);
            if (_preapprovedWriting)
            {
                _preapprovedWriting = false;
                _keyValueDB.RevertWritingTransaction(true);
            }
            else if (_writing)
            {
                _keyValueDB.CommitWritingTransaction(currentBtreeRoot!, _temporaryCloseTransactionLog);
                _writing = false;
            }
        }

        public void Dispose()
        {
            if (_writing || _preapprovedWriting)
            {
                _keyValueDB.RevertWritingTransaction(_preapprovedWriting);
                _writing = false;
                _preapprovedWriting = false;
            }
            if (_btreeRoot == null) return;
            _keyValueDB.FinishedUsingBTreeRoot(_btreeRoot);
            _btreeRoot = null;
            GC.SuppressFinalize(this);
        }

        public long GetTransactionNumber()
        {
            return _btreeRoot!.TransactionId;
        }

        public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
        {
            if (!IsValidKey()) return new KeyValuePair<uint, uint>();
            var nodeIdxPair = _stack[^1];
            var leafMember = ((IBTreeLeafNode)nodeIdxPair.Node).GetMemberValue(nodeIdxPair.Idx);

            return new KeyValuePair<uint, uint>(
                (uint)((IBTreeLeafNode)nodeIdxPair.Node).GetKey(nodeIdxPair.Idx).Length,
                KeyValueDB.CalcValueSize(leafMember.ValueFileId, leafMember.ValueOfs, leafMember.ValueSize));
        }

        public byte[] GetKeyPrefix()
        {
            return _prefix;
        }

        public ulong GetUlong(uint idx)
        {
            return _btreeRoot!.GetUlong(idx);
        }

        public void SetUlong(uint idx, ulong value)
        {
            if (_btreeRoot!.GetUlong(idx) != value)
            {
                MakeWritable();
                _btreeRoot!.SetUlong(idx, value);
            }
        }

        public uint GetUlongCount()
        {
            return _btreeRoot!.UlongsArray == null ? 0U : (uint)_btreeRoot!.UlongsArray.Length;
        }

        string? _descriptionForLeaks;

        public IKeyValueDB Owner => _keyValueDB;

        public string? DescriptionForLeaks
        {
            get => _descriptionForLeaks;
            set
            {
                _descriptionForLeaks = value;
                if (_preapprovedWriting || _writing) _btreeRoot!.DescriptionForLeaks = value;
            }
        }

        public bool RollbackAdvised { get; set; }
    }
}
