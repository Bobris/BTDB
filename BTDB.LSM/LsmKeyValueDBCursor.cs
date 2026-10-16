using System;
using System.Collections.Generic;
using BTDB.KVDBLayer;
using BTDB.LSM.Engine;
using BTDB.LSM.Format;

namespace BTDB.LSM;

/// Cursor positioned by key (ImplementationPlan.md, section 7.4). Each move is a seek over the merged view; the key
/// index is computed by rank and cached until the transaction is modified. After its key is erased the cursor is
/// fuzzy invalid: it remembers the key, so its index is the rank of the key minus one, next and previous move to the
/// neighbors of the key, and reading moves to the next key. Creating the key again makes it current again.
sealed class LsmKeyValueDBCursor : IKeyValueDBCursorInternal
{
    LsmKeyValueDBTransaction? _transaction;
    byte[]? _key;
    LsmValue _value;
    ReadOnlyMemory<byte>? _resolvedValue;
    bool _removed;
    long _stamp;
    long _index = -1;
    MergingIterator? _iterator;
    long _iteratorStamp;

    public LsmKeyValueDBCursor(LsmKeyValueDBTransaction transaction)
    {
        _transaction = transaction;
        if (transaction.FirstCursor == null)
        {
            transaction.FirstCursor = this;
        }
        else
        {
            ((IKeyValueDBCursorInternal)transaction.LastCursor!).NextCursor = this;
            PrevCursor = (IKeyValueDBCursorInternal)transaction.LastCursor;
        }

        transaction.LastCursor = this;
    }

    LsmKeyValueDBTransaction Tr
    {
        get
        {
            ObjectDisposedException.ThrowIf(_transaction == null, this);
            return _transaction;
        }
    }

    public IKeyValueDBTransaction Transaction => Tr;
    internal byte[]? DebugKey => _key;
    public IKeyValueDBCursorInternal? PrevCursor { get; set; }
    public IKeyValueDBCursorInternal? NextCursor { get; set; }

    public void Dispose()
    {
        var transaction = _transaction;
        if (transaction == null) return;
        if (NextCursor == null) transaction.LastCursor = PrevCursor;
        else NextCursor.PrevCursor = PrevCursor;
        if (PrevCursor == null) transaction.FirstCursor = NextCursor;
        else PrevCursor.NextCursor = NextCursor;
        NextCursor = null;
        PrevCursor = null;
        _transaction = null;
        _key = null;
    }

    /// The iterator if it is positioned on the current key of the current view; otherwise null.
    MergingIterator? PositionedIterator()
    {
        var iterator = _iterator;
        if (iterator == null || !iterator.Valid || iterator.View != Tr.View ||
            _iteratorStamp != Tr.ModificationStamp || _key == null ||
            !iterator.Key.AsSpan().SequenceEqual(_key)) return null;
        return iterator;
    }

    MergingIterator FreshIterator()
    {
        var view = Tr.View;
        if (_iterator == null || _iterator.View != view) _iterator = new(view);
        _iteratorStamp = Tr.ModificationStamp;
        return _iterator;
    }

    bool SetFromIterator(MergingIterator iterator, bool found, in ReadOnlySpan<byte> prefix, long index)
    {
        if (!found || !iterator.Key.AsSpan().StartsWith(prefix)) return Fail();
        Set(iterator.Key, iterator.Value, index);
        return true;
    }

    void Set(byte[] key, LsmValue value, long index)
    {
        _key = key;
        _value = value;
        _resolvedValue = null;
        _removed = false;
        _stamp = Tr.ModificationStamp;
        _index = index;
    }

    bool Fail()
    {
        _key = null;
        _removed = false;
        _index = -1;
        return false;
    }

    /// Makes the cursor point to a live key, refreshing state invalidated by modifications.
    void EnsureCurrent()
    {
        var transaction = Tr;
        if (_key == null) throw new InvalidOperationException("Current key is not valid");
        if (!IsRemovedNow()) return;
        if (transaction.View.SeekGE(_key, false, out var next, out var value))
        {
            Set(next, value, -1);
            return;
        }

        Fail();
        throw new InvalidOperationException("Current key is not valid");
    }

    /// Whether the cursor's key no longer exists; a key erased and created again makes the cursor current again.
    bool IsRemovedNow()
    {
        if (_stamp == Tr.ModificationStamp) return _removed;
        if (Tr.View.TryGet(_key!, out var value))
        {
            Set(_key!, value, -1);
            return false;
        }

        _removed = true;
        _stamp = Tr.ModificationStamp;
        return true;
    }

    public bool FindFirstKey(in ReadOnlySpan<byte> prefix)
    {
        var iterator = FreshIterator();
        return SetFromIterator(iterator, iterator.SeekGE(prefix, true), prefix, -1);
    }

    public bool FindLastKey(in ReadOnlySpan<byte> prefix)
    {
        var iterator = FreshIterator();
        var successor = LsmKeys.PrefixSuccessor(prefix);
        return SetFromIterator(iterator, iterator.SeekLE(successor, successor == null, false), prefix, -1);
    }

    public bool FindPreviousKey(in ReadOnlySpan<byte> prefix)
    {
        if (_key == null) return FindLastKey(prefix);
        var removed = IsRemovedNow();
        var index = removed || _index < 0 ? -1 : _index - 1;
        var iterator = removed ? null : PositionedIterator();
        if (iterator != null) return SetFromIterator(iterator, iterator.Prev(), prefix, index);
        iterator = FreshIterator();
        return SetFromIterator(iterator, iterator.SeekLE(_key, false, false), prefix, index);
    }

    public bool FindNextKey(in ReadOnlySpan<byte> prefix)
    {
        if (_key == null) return FindFirstKey(prefix);
        var removed = IsRemovedNow();
        var index = removed || _index < 0 ? -1 : _index + 1;
        var iterator = removed ? null : PositionedIterator();
        if (iterator != null) return SetFromIterator(iterator, iterator.Next(), prefix, index);
        iterator = FreshIterator();
        return SetFromIterator(iterator, iterator.SeekGE(_key, false), prefix, index);
    }

    public FindResult Find(scoped in ReadOnlySpan<byte> key, uint prefixLen)
    {
        var view = Tr.View;
        if (view.TryGet(key, out var value))
        {
            Set(key.ToArray(), value, -1);
            return FindResult.Exact;
        }

        // The cursor's iterator ends positioned on the found key, so stepping from it needs no seek.
        var prefix = key[..(int)prefixLen];
        var iterator = FreshIterator();
        if (iterator.SeekLE(key, false, false) && iterator.Key.AsSpan().StartsWith(prefix))
        {
            Set(iterator.Key, iterator.Value, -1);
            return FindResult.Previous;
        }

        // The key is absent, so the key after the previous one is the first key after key.
        if ((iterator.Valid ? iterator.Next() : iterator.SeekGE(key, false)) &&
            iterator.Key.AsSpan().StartsWith(prefix))
        {
            Set(iterator.Key, iterator.Value, -1);
            return FindResult.Next;
        }

        Fail();
        return FindResult.NotFound;
    }

    public long GetKeyIndex()
    {
        if (_key == null) return -1;
        if (IsRemovedNow()) return Tr.View.Rank(_key) - 1;
        if (_index < 0) _index = Tr.View.Rank(_key);
        return _index;
    }

    public bool FindKeyIndex(in ReadOnlySpan<byte> prefix, long index)
    {
        if (!FindFirstKey(prefix)) return false;
        if (!FindKeyIndex(GetKeyIndex() + index)) return false;
        return _key.AsSpan().StartsWith(prefix) || Fail();
    }

    public bool FindKeyIndex(long index)
    {
        var view = Tr.View;
        if (index < 0 || index >= view.Count) return Fail();
        // The cursor's iterator stays positioned on the selected key, so stepping from it needs no seek.
        var iterator = FreshIterator();
        view.Select(index, iterator);
        Set(iterator.Key, iterator.Value, index);
        return true;
    }

    public bool KeyHasPrefix(in ReadOnlySpan<byte> prefix)
    {
        if (_key == null) return false;
        EnsureCurrent();
        return _key.AsSpan().StartsWith(prefix);
    }

    public void Invalidate() => Fail();

    public bool IsValid() => _key != null;

    /// The current value, read from its value file on first access when separated.
    ReadOnlyMemory<byte> Value => _resolvedValue ??= Tr.Db.Resolve(_value);

    public KeyValuePair<uint, uint> GetStorageSizeOfCurrentKey()
    {
        EnsureCurrent();
        var valueLength = _value.IsSeparated ? ValueHandle.Decode(_value.Data.Span).Length : _value.Data.Length;
        return new((uint)_key!.Length, (uint)valueLength);
    }

    public ReadOnlyMemory<byte> GetKeyMemory(ref Memory<byte> buffer, bool copy = false)
    {
        EnsureCurrent();
        // Keys are never mutated after creation, so returning them without copy is safe.
        if (!copy) return _key;
        if (buffer.Length < _key!.Length) buffer = GC.AllocateUninitializedArray<byte>(_key.Length);
        _key.CopyTo(buffer.Span);
        return buffer[.._key.Length];
    }

    public ReadOnlySpan<byte> GetKeySpan(scoped ref Span<byte> buffer, bool copy = false)
    {
        EnsureCurrent();
        if (!copy) return _key;
        if (buffer.Length < _key!.Length) buffer = GC.AllocateUninitializedArray<byte>(_key.Length);
        _key.CopyTo(buffer);
        return buffer[.._key.Length];
    }

    public ReadOnlySpan<byte> GetKeySpan(Span<byte> buffer, bool copy = false)
    {
        EnsureCurrent();
        if (!copy) return _key;
        if (buffer.Length < _key!.Length) return _key.AsSpan().ToArray();
        _key.CopyTo(buffer);
        return buffer[.._key.Length];
    }

    public bool IsValueCorrupted()
    {
        EnsureCurrent();
        try
        {
            _ = Value;
            return false;
        }
        catch (Exception)
        {
            return true;
        }
    }

    public ReadOnlyMemory<byte> GetValueMemory(ref Memory<byte> buffer, bool copy = false)
    {
        EnsureCurrent();
        var value = Value;
        if (!copy) return value;
        if (buffer.Length < value.Length) buffer = GC.AllocateUninitializedArray<byte>(value.Length);
        value.CopyTo(buffer);
        return buffer[..value.Length];
    }

    public ReadOnlySpan<byte> GetValueSpan(scoped ref Span<byte> buffer, bool copy = false)
    {
        EnsureCurrent();
        var value = Value;
        if (!copy) return value.Span;
        if (buffer.Length < value.Length) buffer = GC.AllocateUninitializedArray<byte>(value.Length);
        value.Span.CopyTo(buffer);
        return buffer[..value.Length];
    }

    public void SetValue(in ReadOnlySpan<byte> value)
    {
        EnsureCurrent();
        var index = _index;
        Tr.Upsert(_key!, value);
        Set(_key!, new(value.ToArray(), false), index);
    }

    public void EraseCurrent()
    {
        EnsureCurrent();
        Tr.Erase(_key!);
        _removed = true;
        _index = -1;
    }

    public long EraseUpTo(IKeyValueDBCursor to)
    {
        EnsureCurrent();
        var target = (LsmKeyValueDBCursor)to;
        target.EnsureCurrent();
        if (LsmKeys.Compare(target._key, _key) < 0) return 0;
        var count = Tr.EraseRange(_key!, target._key!);
        _removed = true;
        _index = -1;
        target._removed = true;
        target._index = -1;
        return count;
    }

    public bool CreateOrUpdateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
    {
        var keyCopy = key.ToArray();
        var created = Tr.Upsert(keyCopy, value);
        Set(keyCopy, new(value.ToArray(), false), -1);
        return created;
    }

    /// The key keeps its index when its suffix changes, so cursors on the old key follow it.
    internal void NotifyRenamed(byte[] oldKey, byte[] newKey)
    {
        if (_key == null || _removed || !_key.AsSpan().SequenceEqual(oldKey)) return;
        Tr.View.TryGet(newKey, out var value);
        Set(newKey, value, -1);
    }

    public UpdateKeySuffixResult UpdateKeySuffix(in ReadOnlySpan<byte> key, uint prefixLen)
    {
        var view = Tr.View;
        var prefix = key[..(int)prefixLen];
        if (!view.SeekGE(prefix, true, out var existing, out var value) || !existing.AsSpan().StartsWith(prefix))
        {
            Fail();
            return UpdateKeySuffixResult.NotFound;
        }

        if (view.SeekGE(existing, false, out var second, out _) && second.AsSpan().StartsWith(prefix))
        {
            Fail();
            return UpdateKeySuffixResult.NotUniquePrefix;
        }

        if (existing.AsSpan().SequenceEqual(key))
        {
            Set(existing, value, -1);
            return UpdateKeySuffixResult.NothingToDo;
        }

        var valueCopy = Tr.Db.Resolve(value).ToArray();
        Tr.Erase(existing);
        var keyCopy = key.ToArray();
        Tr.Upsert(keyCopy, valueCopy);
        Set(keyCopy, new(valueCopy, false), -1);
        Tr.NotifyRenamed(existing, keyCopy, this);
        return UpdateKeySuffixResult.Updated;
    }

    public void FastIterate(ref Span<byte> buffer, CursorIterateCallback callback)
    {
        var transaction = Tr;
        transaction.ModificationForbidden = true;
        try
        {
            if (_key == null && !FindFirstKey([])) return;
            EnsureCurrent();
            var index = GetKeyIndex();
            while (true)
            {
                if (callback(index, _key)) return;
                if (!FindNextKey([])) return;
                index++;
                _index = index;
            }
        }
        finally
        {
            transaction.ModificationForbidden = false;
        }
    }

    public void NotifyRemove(ulong startIndex, ulong endIndex)
    {
    }

    public void PreNotifyUpsert()
    {
    }

    public void NotifyInsert(ulong index)
    {
    }

    public void NotifyWritableTransaction()
    {
    }
}
