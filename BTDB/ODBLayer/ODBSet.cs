using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public class ODBSet<TKey> : IOrderedSet<TKey>, IQuerySizeDictionary<TKey>, IAmLazyDBObject
{
    readonly IInternalObjectDBTransaction _tr;
    readonly IFieldHandler _keyHandler;
    readonly ReaderFun<TKey> _keyReader;
    readonly WriterFun<TKey> _keyWriter;
    readonly IKeyValueDBTransaction _keyValueTr;
    readonly ulong _id;
    readonly byte[] _prefix;
    int _count;

    // ReSharper disable once MemberCanBePrivate.Global used by FieldHandler.Load
    public ODBSet(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config, ulong id)
    {
        _tr = tr;
        _keyHandler = config.KeyHandler;
        _id = id;
        var len = PackUnpack.LengthVUInt(id);
        var prefix = new byte[ObjectDB.AllDictionariesPrefixLen + len];
        MemoryMarshal.GetReference(prefix.AsSpan()) = ObjectDB.AllDictionariesPrefixByte;
        PackUnpack.UnsafePackVUInt(
            ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(prefix.AsSpan()),
                ObjectDB.AllDictionariesPrefixLen), id, len);
        _prefix = prefix;
        _keyReader = ((ReaderFun<TKey>)config.KeyReader)!;
        _keyWriter = ((WriterFun<TKey>)config.KeyWriter)!;
        _keyValueTr = _tr.KeyValueDBTransaction;
        _count = -1;
    }

    // ReSharper disable once UnusedMember.Global
    public ODBSet(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config) : this(tr, config,
        tr.AllocateDictionaryId())
    {
    }

    static void ThrowModifiedDuringEnum()
    {
        throw new InvalidOperationException("DB modified during iteration");
    }

    public static void DoSave(ref MemWriter writer, IWriterCtx ctx, IOrderedSet<TKey>? dictionary, int cfgId)
    {
        var writerCtx = (IDBWriterCtx)ctx;
        if (!(dictionary is ODBSet<TKey> goodDict))
        {
            var tr = writerCtx.GetTransaction();
            var id = tr.AllocateDictionaryId();
            goodDict = new ODBSet<TKey>(tr, ODBDictionaryConfiguration.Get(cfgId), id);
            if (dictionary != null)
                foreach (var pair in dictionary)
                    goodDict.Add(pair);
        }

        writer.WriteVUInt64(goodDict._id);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    void ICollection<TKey>.Add(TKey item)
    {
        Add(item!);
    }

    public void ExceptWith(IEnumerable<TKey> other)
    {
        foreach (var key in other)
        {
            Remove(key);
        }
    }

    public void IntersectWith(IEnumerable<TKey> other)
    {
        throw new NotSupportedException();
    }

    public bool IsProperSubsetOf(IEnumerable<TKey> other)
    {
        throw new NotSupportedException();
    }

    public bool IsProperSupersetOf(IEnumerable<TKey> other)
    {
        throw new NotSupportedException();
    }

    public bool IsSubsetOf(IEnumerable<TKey> other)
    {
        throw new NotSupportedException();
    }

    public bool IsSupersetOf(IEnumerable<TKey> other)
    {
        throw new NotSupportedException();
    }

    public bool Overlaps(IEnumerable<TKey> other)
    {
        throw new NotSupportedException();
    }

    public bool SetEquals(IEnumerable<TKey> other)
    {
        throw new NotSupportedException();
    }

    public void SymmetricExceptWith(IEnumerable<TKey> other)
    {
        throw new NotSupportedException();
    }

    public void UnionWith(IEnumerable<TKey> other)
    {
        foreach (var key in other)
        {
            Add(key!);
        }
    }

    public void Clear()
    {
        using var cursor = _keyValueTr.CreateCursor();
        cursor.EraseAll(_prefix);
        _count = 0;
    }

    [SkipLocalsInit]
    public bool Contains(TKey item)
    {
        using var cursor = _keyValueTr.CreateCursor();
        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        var keyBytes = KeyToByteArray(item, ref writer);
        return cursor.Find(keyBytes, 0) == FindResult.Exact;
    }

    public void CopyTo(TKey[] array, int arrayIndex)
    {
        if (array == null) throw new ArgumentNullException(nameof(array));
        if ((arrayIndex < 0) || (arrayIndex > array.Length))
        {
            throw new ArgumentOutOfRangeException(nameof(arrayIndex), arrayIndex, "Needs to be nonnegative ");
        }

        if ((array.Length - arrayIndex) < Count)
        {
            throw new ArgumentException("Array too small");
        }

        foreach (var item in this)
        {
            array[arrayIndex++] = item;
        }
    }

    public int Count
    {
        get
        {
            if (_count == -1)
            {
                using var cursor = _keyValueTr.CreateCursor();
                _count = (int)Math.Min(cursor.GetKeyValueCount(_prefix), int.MaxValue);
            }

            return _count;
        }
    }

    public bool IsReadOnly => false;


    [SkipLocalsInit]
    FindResult FindKey(IKeyValueDBCursor cursor, TKey key)
    {
        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        var keyBytes = KeyToByteArray(key, ref writer);
        return cursor.Find(keyBytes, (uint)_prefix.Length);
    }

    ReadOnlySpan<byte> KeyToByteArray(TKey key, ref MemWriter writer)
    {
        writer.WriteBlock(_prefix);
        IWriterCtx ctx = null;
        if (_keyHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr);
        _keyWriter(key, ref writer, ctx);
        return writer.GetScopedSpanAndReset();
    }

    [SkipLocalsInit]
    unsafe TKey CurrentToKey(IKeyValueDBCursor cursor)
    {
        Span<byte> buffer = stackalloc byte[2048];
        var keySpan = cursor.GetKeySpan(ref buffer)[_prefix.Length..];
        fixed (byte* _ = keySpan)
        {
            var reader = MemReader.CreateFromPinnedSpan(keySpan);
            IReaderCtx ctx = null;
            if (_keyHandler.NeedsCtx()) ctx = new DBReaderCtx(_tr);
            return _keyReader(ref reader, ctx);
        }
    }

    [SkipLocalsInit]
    public bool Add(TKey key)
    {
        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        var keyBytes = KeyToByteArray(key, ref writer);
        using var cursor = _keyValueTr.CreateCursor();
        var created = cursor.CreateOrUpdateKeyValue(keyBytes, new());
        if (created) NotifyAdded();
        return created;
    }

    [SkipLocalsInit]
    public bool Remove(TKey key)
    {
        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        var keyBytes = KeyToByteArray(key, ref writer);
        using var cursor = _keyValueTr.CreateCursor();
        if (cursor.FindExactKey(keyBytes))
        {
            cursor.EraseCurrent();
            NotifyRemoved();
            return true;
        }

        return false;
    }

    void NotifyAdded()
    {
        if (_count != -1)
        {
            if (_count != int.MaxValue) _count++;
        }
    }

    void NotifyRemoved()
    {
        if (_count != -1)
        {
            if (_count == int.MaxValue)
            {
                _count = -1;
            }
            else
            {
                _count--;
            }
        }
    }

    public IEnumerator<TKey> GetEnumerator()
    {
        using var cursor = _keyValueTr.CreateCursor();
        while (cursor.FindNextKey(_prefix))
        {
            var key = CurrentToKey(cursor);
            yield return key;
        }
    }

    public IEnumerable<TKey> GetReverseEnumerator()
    {
        using var cursor = _keyValueTr.CreateCursor();
        while (cursor.FindPreviousKey(_prefix))
        {
            var key = CurrentToKey(cursor);
            yield return key;
        }
    }

    public IEnumerable<TKey> GetIncreasingEnumerator(TKey start)
    {
        using var cursor = _keyValueTr.CreateCursor();
        switch (FindKey(cursor, start))
        {
            case FindResult.Exact:
            case FindResult.Next:
                break;
            case FindResult.Previous:
                if (!cursor.FindNextKey(_prefix)) yield break;
                break;
            case FindResult.NotFound:
                yield break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        do
        {
            var key = CurrentToKey(cursor);
            yield return key;
        } while (cursor.FindNextKey(_prefix));
    }

    public IEnumerable<TKey> GetDecreasingEnumerator(TKey start)
    {
        using var cursor = _keyValueTr.CreateCursor();
        switch (FindKey(cursor, start))
        {
            case FindResult.Exact:
            case FindResult.Previous:
                break;
            case FindResult.Next:
                if (!cursor.FindPreviousKey(_prefix)) yield break;
                break;
            case FindResult.NotFound:
                yield break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        do
        {
            var key = CurrentToKey(cursor);
            yield return key;
        } while (cursor.FindPreviousKey(_prefix));
    }

    public long RemoveRange(AdvancedEnumeratorParam<TKey> param)
    {
        using var startCursor = _keyValueTr.CreateCursor();
        var result = param.StartProposition == KeyProposition.Ignored
            ? (startCursor.FindFirstKey(_prefix) ? FindResult.Next : FindResult.NotFound)
            : FindKey(startCursor, param.Start);
        if (result == FindResult.NotFound) return 0;
        if (result == FindResult.Exact)
        {
            if (param.StartProposition == KeyProposition.Excluded)
                if (!startCursor.FindNextKey(_prefix))
                    return 0;
        }
        else if (result == FindResult.Previous)
        {
            if (!startCursor.FindNextKey(_prefix))
                return 0;
        }

        using var endCursor = _keyValueTr.CreateCursor();
        result = param.EndProposition == KeyProposition.Ignored
            ? endCursor.FindLastKey(_prefix) ? FindResult.Previous : throw new InvalidOperationException()
            : FindKey(endCursor, param.End);
        if (result == FindResult.Exact)
        {
            if (param.EndProposition == KeyProposition.Excluded)
                if (!endCursor.FindPreviousKey(_prefix))
                    return 0;
        }
        else if (result == FindResult.Next)
        {
            if (!endCursor.FindPreviousKey(_prefix))
                return 0;
        }

        _count = -1;
        return startCursor.EraseUpTo(endCursor);
    }

    public IEnumerable<KeyValuePair<uint, uint>> QuerySizeEnumerator()
    {
        using var cursor = _keyValueTr.CreateCursor();
        while (cursor.FindNextKey(_prefix))
        {
            var size = cursor.GetStorageSizeOfCurrentKey();
            yield return size;
        }
    }

    public KeyValuePair<uint, uint> QuerySizeByKey(TKey key)
    {
        using var cursor = _keyValueTr.CreateCursor();
        if (FindKey(cursor, key) != FindResult.Exact)
        {
            throw new ArgumentException("Key not found in Set");
        }

        return cursor.GetStorageSizeOfCurrentKey();
    }

    class AdvancedEnumerator : IEnumerable<TKey>, IEnumerator<TKey>
    {
        readonly ODBSet<TKey> _owner;
        readonly IKeyValueDBCursor? _startCursor;
        readonly IKeyValueDBCursor? _endCursor;
        readonly IKeyValueDBCursor? _cursor;
        SeekState _seekState;
        readonly bool _ascending;

        public AdvancedEnumerator(ODBSet<TKey> owner, AdvancedEnumeratorParam<TKey> param)
        {
            _owner = owner;
            var keyValueTr = _owner._keyValueTr;
            _ascending = param.Order == EnumerationOrder.Ascending;
            _startCursor = keyValueTr.CreateCursor();
            if (param.StartProposition == KeyProposition.Ignored)
            {
                if (!_startCursor.FindFirstKey(_owner._prefix))
                {
                    return;
                }
            }
            else
            {
                switch (_owner.FindKey(_startCursor, param.Start))
                {
                    case FindResult.Exact:
                        if (param.StartProposition == KeyProposition.Excluded)
                        {
                            if (!_startCursor.FindNextKey(_owner._prefix)) return;
                        }

                        break;
                    case FindResult.Previous:
                        if (!_startCursor.FindNextKey(_owner._prefix)) return;
                        break;
                    case FindResult.Next:
                        break;
                    case FindResult.NotFound:
                        return;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            _endCursor = keyValueTr.CreateCursor();
            if (param.EndProposition == KeyProposition.Ignored)
            {
                if (!_endCursor.FindLastKey(_owner._prefix)) return;
            }
            else
            {
                switch (_owner.FindKey(_endCursor, param.End))
                {
                    case FindResult.Exact:
                        if (param.EndProposition == KeyProposition.Excluded)
                        {
                            if (!_endCursor.FindPreviousKey(_owner._prefix)) return;
                        }

                        break;
                    case FindResult.Previous:
                        break;
                    case FindResult.Next:
                        if (!_endCursor.FindPreviousKey(_owner._prefix)) return;
                        break;
                    case FindResult.NotFound:
                        return;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }


            var startIndex = _startCursor.GetKeyIndex();
            var endIndex = _endCursor.GetKeyIndex();
            if (startIndex > endIndex) return;
            _cursor = keyValueTr.CreateCursor();
            _cursor.FindKeyIndex(_ascending ? startIndex : endIndex);
            _seekState = SeekState.Undefined;
        }

        public IEnumerator<TKey> GetEnumerator()
        {
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this;
        }

        public bool MoveNext()
        {
            if (_cursor == null)
            {
                Current = default;
                return false;
            }

            if (_seekState == SeekState.Ready)
            {
                if (_ascending)
                {
                    if (!_cursor.FindNextKey(_owner._prefix))
                    {
                        Current = default;
                        return false;
                    }

                    if (_cursor.GetKeyIndex() > _endCursor!.GetKeyIndex())
                    {
                        Current = default;
                        return false;
                    }
                }
                else
                {
                    if (!_cursor.FindPreviousKey(_owner._prefix))
                    {
                        Current = default;
                        return false;
                    }

                    if (_cursor.GetKeyIndex() < _startCursor!.GetKeyIndex())
                    {
                        Current = default;
                        return false;
                    }
                }
            }
            else
            {
                _seekState = SeekState.Ready;
            }

            Current = _owner.CurrentToKey(_cursor);
            return true;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public TKey Current { get; private set; }

        object? IEnumerator.Current => Current;

        public void Dispose()
        {
            _startCursor?.Dispose();
            _endCursor?.Dispose();
            _cursor?.Dispose();
        }
    }

    public IEnumerable<TKey> GetAdvancedEnumerator(AdvancedEnumeratorParam<TKey> param)
    {
        return new AdvancedEnumerator(this, param);
    }
}
