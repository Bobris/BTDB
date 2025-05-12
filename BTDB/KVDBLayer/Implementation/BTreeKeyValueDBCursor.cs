using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using BTDB.BTreeLib;
using Microsoft.Extensions.ObjectPool;

namespace BTDB.KVDBLayer;

public class BTreeKeyValueDBCursor : IKeyValueDBCursorInternal
{
    BTreeKeyValueDBTransaction? _transaction;
    ICursor? _cursor;
    bool _modifiedFromLastFind;
    bool _removedCurrent;
    bool _modificationForbidden;
    bool _forbidPooling;
    internal bool Disposed;
    long _keyIndex;

    public static BTreeKeyValueDBCursor Create(BTreeKeyValueDBTransaction transaction, bool forbidPooling)
    {
        var cursor = forbidPooling ? new() : PooledCursors.Get();
        cursor._forbidPooling = forbidPooling;
        cursor.Disposed = false;
        if (cursor._cursor == null)
        {
            cursor._cursor = transaction.BTreeRoot!.CreateCursor();
        }
        else
        {
            cursor._cursor.SetNewRoot(transaction.BTreeRoot!);
        }

        cursor._transaction = transaction;
        cursor._keyIndex = -1;
        if (transaction.FirstCursor == null)
        {
            transaction.FirstCursor = cursor;
        }
        else
        {
            ((IKeyValueDBCursorInternal)transaction.LastCursor!).NextCursor = cursor;
            cursor.PrevCursor = (IKeyValueDBCursorInternal)transaction.LastCursor;
        }

        transaction.LastCursor = cursor;

        return cursor;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref Disposed, true)) return;
        var transaction = _transaction;
        if (transaction == null)
        {
            Debug.Fail("Transaction is already closed");
            return;
        }

        if (transaction.Reused1 == null)
        {
            Invalidate();
            transaction.Reused1 = this;
            return;
        }

        if (transaction.Reused2 == null)
        {
            Invalidate();
            transaction.Reused2 = this;
            return;
        }

        RealDispose(transaction);
    }

    internal void RealDispose(BTreeKeyValueDBTransaction transaction)
    {
        var nextCursor = NextCursor;
        var prevCursor = PrevCursor;
        if (nextCursor == null)
        {
            if (prevCursor == null)
            {
                transaction.FirstCursor = null!;
                transaction.LastCursor = null!;
            }
            else
            {
                prevCursor.NextCursor = null;
                transaction.LastCursor = prevCursor;
            }
        }
        else
        {
            nextCursor.PrevCursor = prevCursor;
            if (prevCursor == null)
            {
                transaction.FirstCursor = nextCursor;
            }
            else
            {
                prevCursor.NextCursor = nextCursor;
            }
        }

        _cursor!.Invalidate();
        _transaction = null;
        NextCursor = null;
        PrevCursor = null;
        _modifiedFromLastFind = false;
        _removedCurrent = false;
        if (!_forbidPooling)
            PooledCursors.Return(this);
    }

    static readonly DefaultObjectPool<BTreeKeyValueDBCursor> PooledCursors =
        new(new DefaultPooledObjectPolicy<BTreeKeyValueDBCursor>(), 50);

    public IKeyValueDBTransaction Transaction => _transaction!;

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        _modifiedFromLastFind = false;
        _keyIndex = -1;
        return _cursor!.FindFirst(prefix);
    }

    public bool FindLastKey(in ReadOnlySpan<byte> prefix)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        _modifiedFromLastFind = false;
        _keyIndex = _cursor!.FindLastWithPrefix(prefix);
        return _keyIndex >= 0;
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (_modifiedFromLastFind)
        {
            if (FindKeyIndex(_keyIndex - 1) && _cursor!.KeyHasPrefix(prefix))
            {
                return true;
            }
        }
        else
        {
            if (!_cursor!.IsValid()) return FindLastKey(prefix);
            if (_cursor.MovePrevious())
            {
                if (_cursor.KeyHasPrefix(prefix))
                {
                    if (_keyIndex != -1) _keyIndex--;
                    return true;
                }
            }
        }

        _keyIndex = -1;
        _cursor!.Invalidate();
        return false;
    }

    public bool FindNextKey(in ReadOnlySpan<byte> prefix)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (_modifiedFromLastFind)
        {
            if (!_removedCurrent) _keyIndex++;
            if (FindKeyIndex(_keyIndex) && _cursor!.KeyHasPrefix(prefix))
            {
                return true;
            }
        }
        else
        {
            if (!_cursor!.IsValid()) return FindFirstKey(prefix);
            if (_cursor.MoveNext())
            {
                if (_cursor.KeyHasPrefix(prefix))
                {
                    if (_keyIndex != -1) _keyIndex++;
                    return true;
                }
            }
        }

        _keyIndex = -1;
        _cursor!.Invalidate();
        return false;
    }

    public FindResult Find(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        _modifiedFromLastFind = false;
        var result = _cursor!.Find(key);
        _keyIndex = -1;
        if (prefixLen == 0) return result;
        switch (result)
        {
            case FindResult.Previous when !_cursor.KeyHasPrefix(key[..(int)prefixLen]):
            {
                if (!_cursor.MoveNext())
                {
                    return FindResult.NotFound;
                }

                if (_cursor.KeyHasPrefix(key[..(int)prefixLen]))
                {
                    return FindResult.Next;
                }

                _cursor.Invalidate();
                return FindResult.NotFound;
            }
            case FindResult.Next when !_cursor.KeyHasPrefix(key[..(int)prefixLen]):
                // FindResult.Previous is preferred that's why it has to be NotFound when next does not match prefix
                _cursor.Invalidate();
                return FindResult.NotFound;
            default:
                return result;
        }
    }

    public long GetKeyIndex()
    {
        if (_keyIndex == -1) _keyIndex = _cursor!.CalcIndex();
        if (_modifiedFromLastFind && _removedCurrent && _keyIndex != -1)
        {
            return _keyIndex - 1;
        }

        return _keyIndex;
    }

    public bool FindKeyIndex(in ReadOnlySpan<byte> prefix, long index)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        _modifiedFromLastFind = false;
        if (!_cursor!.FindFirst(prefix))
        {
            _keyIndex = -1;
            return false;
        }

        index += _cursor.CalcIndex();
        if (_cursor.SeekIndex(index))
        {
            _keyIndex = index;
            if (_cursor.KeyHasPrefix(prefix))
                return true;
        }

        _keyIndex = -1;
        _cursor.Invalidate();
        return false;
    }

    public bool FindKeyIndex(long index)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        _modifiedFromLastFind = false;
        _keyIndex = -1;
        if (_cursor!.SeekIndex(index))
        {
            _keyIndex = index;
            return true;
        }

        _cursor.Invalidate();
        return false;
    }

    public bool KeyHasPrefix(in ReadOnlySpan<byte> prefix)
    {
        return _cursor!.KeyHasPrefix(prefix);
    }

    public void Invalidate()
    {
        _modifiedFromLastFind = false;
        _removedCurrent = false;
        _keyIndex = -1;
        _cursor!.Invalidate();
    }

    public bool IsValid()
    {
        return _cursor!.IsValid();
    }

    public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
    {
        if (!IsValid()) return new();
        var keyLen = _cursor!.GetKeyLength();
        var trueValue = _cursor.GetValue();
        return new(
            (uint)keyLen,
            _transaction!.KeyValueDB.CalcValueSize(MemoryMarshal.Read<uint>(trueValue),
                MemoryMarshal.Read<uint>(trueValue[4..]),
                MemoryMarshal.Read<int>(trueValue[8..])));
    }

    public ReadOnlyMemory<byte> GetKeyMemory(ref Memory<byte> buffer, bool copy = false)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        return _cursor!.GetKeyMemory(ref buffer, copy);
    }

    public ReadOnlySpan<byte> GetKeySpan(scoped ref Span<byte> buffer, bool copy = false)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        return _cursor!.GetKeySpan(ref buffer, copy);
    }

    public ReadOnlySpan<byte> GetKeySpan(Span<byte> buffer, bool copy = false)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        return _cursor!.GetKeySpan(buffer, copy);
    }

    public bool IsValueCorrupted()
    {
        if (!IsValid()) return false;
        var trueValue = _cursor!.GetValue();
        return _transaction!.KeyValueDB.IsCorruptedValue(trueValue);
    }

    public ReadOnlyMemory<byte> GetValueMemory(ref Memory<byte> buffer, bool copy = false)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (!IsValid()) return new();
        var trueValue = _cursor!.GetValue();
        var keyValueDB = _transaction!.KeyValueDB;
        try
        {
            return keyValueDB.ReadValueMemory(trueValue, ref buffer, copy);
        }
        catch (BTDBException ex)
        {
            var oldestRoot = (IRootNode)keyValueDB.ReferenceAndGetOldestRoot();
            var lastCommitted = (IRootNode)keyValueDB.ReferenceAndGetLastCommitted();
            try
            {
                var treeRoot = _transaction.BTreeRoot!;
                throw new BTDBException(
                    $"{ex.Message} in TrId:{treeRoot.TransactionId},TRL:{treeRoot.TrLogFileId},Ofs:{treeRoot.TrLogOffset},ComUlong:{treeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong}",
                    ex);
            }
            finally
            {
                keyValueDB.DereferenceRootNodeInternal(oldestRoot);
                keyValueDB.DereferenceRootNodeInternal(lastCommitted);
            }
        }
    }

    public ReadOnlySpan<byte> GetValueSpan(scoped ref Span<byte> buffer, bool copy = false)
    {
        EnsureValidCursor();
        if (!IsValid()) return new();
        var trueValue = _cursor!.GetValue();
        var keyValueDB = _transaction!.KeyValueDB;
        try
        {
            return keyValueDB.ReadValueSpan(trueValue, ref buffer, copy);
        }
        catch (BTDBException ex)
        {
            var oldestRoot = (IRootNode)keyValueDB.ReferenceAndGetOldestRoot();
            var lastCommitted = (IRootNode)keyValueDB.ReferenceAndGetLastCommitted();
            try
            {
                var treeRoot = _transaction.BTreeRoot!;
                throw new BTDBException(
                    $"{ex.Message} in TrId:{treeRoot.TransactionId},TRL:{treeRoot.TrLogFileId},Ofs:{treeRoot.TrLogOffset},ComUlong:{treeRoot.CommitUlong} and LastTrId:{lastCommitted.TransactionId},ComUlong:{lastCommitted.CommitUlong} OldestTrId:{oldestRoot.TransactionId},TRL:{oldestRoot.TrLogFileId},ComUlong:{oldestRoot.CommitUlong}",
                    ex);
            }
            finally
            {
                keyValueDB.DereferenceRootNodeInternal(oldestRoot);
                keyValueDB.DereferenceRootNodeInternal(lastCommitted);
            }
        }
    }

    void EnsureValidKey()
    {
        if (!_cursor!.IsValid())
        {
            if (_modifiedFromLastFind)
            {
                throw new InvalidOperationException("Current key is not valid because it was modified from last find");
            }

            throw new InvalidOperationException("Current key is not valid");
        }
    }

    void EnsureValidCursor()
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        if (!_cursor!.IsValid())
        {
            if (_modifiedFromLastFind && _keyIndex != -1)
            {
                if (FindKeyIndex(_keyIndex))
                {
                    return;
                }
            }

            throw new InvalidOperationException("Current key is not valid");
        }
    }

    [SkipLocalsInit]
    public void SetValue(in ReadOnlySpan<byte> value)
    {
        EnsureValidKey();
        _transaction!.MakeWritable();
        Span<byte> trueValue = stackalloc byte[12];
        _transaction.KeyValueDB.WriteCreateOrUpdateCommand(_cursor!.GetKeyParts(out var keySuffix), keySuffix, value,
            trueValue);
        _cursor.WriteValue(trueValue);
    }

    public void EraseCurrent()
    {
        EnsureValidCursor();
        var keyIndexToRemove = (ulong)GetKeyIndex();
        var cursor = (IKeyValueDBCursorInternal)_transaction!.FirstCursor;
        while (cursor != null)
        {
            if (cursor != this)
            {
                cursor.NotifyRemove(keyIndexToRemove, keyIndexToRemove);
            }

            cursor = cursor.NextCursor;
        }

        _transaction.MakeWritable();
        _transaction.KeyValueDB.WriteEraseOneCommand(_cursor!.GetKeyParts(out var keySuffix), keySuffix);

        _cursor.Erase();
        _cursor.Invalidate();
        _keyIndex = (long)keyIndexToRemove;
        _modifiedFromLastFind = true;
        _removedCurrent = true;
    }

    public long EraseUpTo(IKeyValueDBCursor to)
    {
        EnsureValidCursor();
        var trueTo = (BTreeKeyValueDBCursor)to;
        trueTo.EnsureValidCursor();
        var firstKeyIndex = (ulong)GetKeyIndex();
        var lastKeyIndex = (ulong)trueTo.GetKeyIndex();
        if (lastKeyIndex < firstKeyIndex) return 0;
        if (firstKeyIndex == lastKeyIndex)
        {
            EraseCurrent();
            return 1;
        }

        var cursor = (IKeyValueDBCursorInternal)_transaction!.FirstCursor;
        while (cursor != null)
        {
            if (cursor != this && cursor != to)
            {
                cursor.NotifyRemove(firstKeyIndex, lastKeyIndex);
            }

            cursor = cursor.NextCursor;
        }

        _transaction.MakeWritable();
        var firstKeyPrefix = _cursor!.GetKeyParts(out var firstKeySuffix);
        var secondKeyPrefix = trueTo._cursor!.GetKeyParts(out var secondKeySuffix);
        _transaction.KeyValueDB.WriteEraseRangeCommand(firstKeyPrefix, firstKeySuffix, secondKeyPrefix,
            secondKeySuffix);
        _cursor.EraseTo(trueTo._cursor);

        _cursor.Invalidate();
        trueTo._cursor.Invalidate();
        _keyIndex = (long)firstKeyIndex;
        trueTo._keyIndex = (long)firstKeyIndex;
        _modifiedFromLastFind = true;
        trueTo._modifiedFromLastFind = true;
        _removedCurrent = true;
        trueTo._removedCurrent = true;
        return (long)(lastKeyIndex - firstKeyIndex + 1);
    }

    [SkipLocalsInit]
    public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        var cursor = (IKeyValueDBCursorInternal)_transaction!.FirstCursor;
        while (cursor != null)
        {
            if (cursor != this)
            {
                cursor.PreNotifyUpsert();
            }

            cursor = cursor.NextCursor;
        }

        _transaction.MakeWritable();
        Span<byte> trueValue = stackalloc byte[12];
        _transaction.KeyValueDB.WriteCreateOrUpdateCommand(key, default, value, trueValue);
        var result = _cursor!.Upsert(key, trueValue);
        _keyIndex = _cursor.CalcIndex();
        if (result)
        {
            cursor = (IKeyValueDBCursorInternal)_transaction.FirstCursor;
            while (cursor != null)
            {
                if (cursor != this)
                {
                    cursor.NotifyInsert((ulong)_keyIndex);
                }

                cursor = cursor.NextCursor;
            }
        }

        return result;
    }

    public UpdateKeySuffixResult UpdateKeySuffix(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        var cursor = (IKeyValueDBCursorInternal)_transaction!.FirstCursor;
        while (cursor != null)
        {
            if (cursor != this)
            {
                cursor.PreNotifyUpsert();
            }

            cursor = cursor.NextCursor;
        }

        _transaction.MakeWritable();
        if (!_cursor!.FindFirst(key[..(int)prefixLen])) return UpdateKeySuffixResult.NotFound;
        if (_cursor.MoveNext())
        {
            if (_cursor.KeyHasPrefix(key[..(int)prefixLen]))
            {
                return UpdateKeySuffixResult.NotUniquePrefix;
            }
        }

        _cursor.MovePrevious();

        _keyIndex = -1;
        if (_cursor.KeyHasPrefix(key) && _cursor.GetKeyLength() == key.Length)
        {
            return UpdateKeySuffixResult.NothingToDo;
        }

        _cursor.UpdateKeySuffix(key);
        _transaction.KeyValueDB.WriteUpdateKeySuffixCommand(key, prefixLen);
        return UpdateKeySuffixResult.Updated;
    }

    public void FastIterate(ref Span<byte> buffer, CursorIterateCallback callback)
    {
        ObjectDisposedException.ThrowIf(Disposed, this);
        _modificationForbidden = true;
        try
        {
            if (!_cursor!.IsValid())
                if (!FindFirstKey([]))
                    return;
            if (_keyIndex == -1) _keyIndex = _cursor!.CalcIndex();
            _cursor!.FastIterate(ref buffer, ref _keyIndex, callback);
        }
        finally
        {
            _modificationForbidden = false;
        }
    }

    public void NotifyRemove(ulong startIndex, ulong endIndex)
    {
        if (_modificationForbidden) throw new BTDBException("DB modification during FastIterate is forbidden");
        if (_modifiedFromLastFind)
        {
            if (_keyIndex == -1) return;
        }
        else
        {
            if (_keyIndex == -1)
            {
                if (!_cursor!.IsValid()) return;
                _keyIndex = _cursor.CalcIndex();
            }

            _cursor!.Invalidate();
            _modifiedFromLastFind = true;
        }

        if ((ulong)_keyIndex >= startIndex && (ulong)_keyIndex <= endIndex)
        {
            _keyIndex = (long)startIndex;
            _removedCurrent = true;
        }
        else if ((ulong)_keyIndex > endIndex)
        {
            _keyIndex -= (long)(endIndex - startIndex + 1);
        }
    }

    public void PreNotifyUpsert()
    {
        if (_modificationForbidden) throw new BTDBException("DB modification during FastIterate is forbidden");
        if (_keyIndex == -1)
        {
            if (!_cursor!.IsValid()) return;
            _keyIndex = _cursor.CalcIndex();
        }

        _cursor!.Invalidate();
        if (!_modifiedFromLastFind)
        {
            _removedCurrent = false;
        }

        _modifiedFromLastFind = true;
    }

    public void NotifyInsert(ulong index)
    {
        if (_keyIndex != -1)
        {
            if (_removedCurrent && (ulong)_keyIndex == index)
            {
                _removedCurrent = false;
            }
            else if ((ulong)_keyIndex >= index)
            {
                _keyIndex++;
            }
        }
    }

    public IKeyValueDBCursorInternal? PrevCursor { get; set; }
    public IKeyValueDBCursorInternal? NextCursor { get; set; }

    public void NotifyWritableTransaction()
    {
        if (_modificationForbidden) throw new BTDBException("DB modification during FastIterate is forbidden");
        _cursor!.SetNewRoot(_transaction!.BTreeRoot!);
    }
}
