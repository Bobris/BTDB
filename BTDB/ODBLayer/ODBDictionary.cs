using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public delegate T ReaderFun<out T>(ref MemReader reader, IReaderCtx? ctx);

delegate void WriterFun<in T>(T value, ref MemWriter writer, IWriterCtx? ctx);

delegate void FreeContentFun(IInternalObjectDBTransaction transaction, ref MemReader reader, IList<ulong> dictIds);

public class ODBDictionary<TKey, TValue> : IOrderedDictionary<TKey, TValue>, IQuerySizeDictionary<TKey>, IAmLazyDBObject
{
    readonly IInternalObjectDBTransaction _tr;
    readonly IFieldHandler _keyHandler;
    readonly IFieldHandler _valueHandler;

    readonly ReaderFun<TKey> _keyReader;
    readonly WriterFun<TKey> _keyWriter;
    readonly ReaderFun<TValue> _valueReader;
    readonly WriterFun<TValue> _valueWriter;
    readonly IKeyValueDBTransaction _keyValueTr;
    readonly ulong _id;
    readonly byte[] _prefix;
    int _count;
    KeysCollection? _keysCollection;
    ValuesCollection? _valuesCollection;

    // ReSharper disable once MemberCanBePrivate.Global used by FieldHandler.Load
    public ODBDictionary(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config, ulong id)
    {
        _tr = tr;
        _keyHandler = config.KeyHandler!;
        _valueHandler = config.ValueHandler!;
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
        _valueReader = ((ReaderFun<TValue>)config.ValueReader)!;
        _valueWriter = ((WriterFun<TValue>)config.ValueWriter)!;
        _keyValueTr = _tr.KeyValueDBTransaction;
        _count = -1;
    }

    // ReSharper disable once UnusedMember.Global
    public ODBDictionary(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config) : this(tr, config,
        tr.AllocateDictionaryId())
    {
    }

    public static void DoSave(ref MemWriter writer, IWriterCtx ctx, IDictionary<TKey, TValue>? dictionary, int cfgId)
    {
        var writerCtx = (IDBWriterCtx)ctx;
        if (!(dictionary is ODBDictionary<TKey, TValue> goodDict))
        {
            var tr = writerCtx.GetTransaction();
            var id = tr.AllocateDictionaryId();
            goodDict = new ODBDictionary<TKey, TValue>(tr, ODBDictionaryConfiguration.Get(cfgId), id);
            if (dictionary != null)
                foreach (var pair in dictionary)
                    goodDict.Add(pair.Key, pair.Value);
        }

        writer.WriteVUInt64(goodDict._id);
    }

    public static void DoFreeContent(IReaderCtx ctx, ulong id, int cfgId)
    {
        var readerCtx = (DBReaderCtx)ctx;
        var tr = readerCtx.GetTransaction();
        var dict = new ODBDictionary<TKey, TValue>(tr!, ODBDictionaryConfiguration.Get(cfgId), id);
        dict.FreeContent(ctx, cfgId);
    }

    [SkipLocalsInit]
    unsafe void FreeContent(IReaderCtx readerCtx, int cfgId)
    {
        var config = ODBDictionaryConfiguration.Get(cfgId);
        var ctx = (DBReaderWithFreeInfoCtx)readerCtx;

        if (config.FreeContent == null)
        {
            var method = ILBuilder.Instance.NewMethod<FreeContentFun>($"IDictFinder_Cfg_{cfgId}");
            var ilGenerator = method.Generator;

            var readerLoc = ilGenerator.DeclareLocal(typeof(IReaderCtx));
            ilGenerator
                .Ldarg(0)
                .Ldarg(2)
                // ReSharper disable once ObjectCreationAsStatement
                .Newobj(() => new DBReaderWithFreeInfoCtx(null, null))
                .Stloc(readerLoc);

            var readerOrCtx = _valueHandler.NeedsCtx() ? (Action<IILGen>?)(il => il.Ldloc(readerLoc)) : null;
            _valueHandler.FreeContent(ilGenerator, il => il.Ldarg(1), readerOrCtx);
            ilGenerator.Ret();
            config.FreeContent = method.Create();
        }

        var findIDictAction = (FreeContentFun)config.FreeContent;

        Span<byte> buffer = stackalloc byte[4096];
        using var cursor = _keyValueTr.CreateCursor();
        while (cursor.FindNextKey(_prefix))
        {
            var valueSpan = cursor.GetValueSpan(ref buffer);
            fixed (void* _ = valueSpan)
            {
                var valueReader = MemReader.CreateFromPinnedSpan(valueSpan);
                findIDictAction(ctx.GetTransaction()!, ref valueReader, ctx.DictIds);
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void Add(KeyValuePair<TKey, TValue> item)
    {
        Add(item.Key, item.Value);
    }

    public void Clear()
    {
        using var cursor = _keyValueTr.CreateCursor();
        cursor.EraseAll(_prefix);
        _count = 0;
    }

    public bool Contains(KeyValuePair<TKey, TValue> item)
    {
        if (!TryGetValue(item.Key, out var value)) return false;
        return EqualityComparer<TValue>.Default.Equals(value, item.Value);
    }

    public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
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

    public bool Remove(KeyValuePair<TKey, TValue> item)
    {
        if (Contains(item))
        {
            Remove(item.Key);
            return true;
        }

        return false;
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

    ReadOnlySpan<byte> ValueToByteArray(TValue value, ref MemWriter writer)
    {
        IWriterCtx ctx = null;
        if (_valueHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr);
        _valueWriter(value, ref writer, ctx);
        return writer.GetScopedSpanAndReset();
    }

    [SkipLocalsInit]
    unsafe TKey CurrentToKey(IKeyValueDBCursor cursor)
    {
        Span<byte> buffer = stackalloc byte[4096];
        var keySpan = cursor.GetKeySpan(ref buffer);
        fixed (byte* keyPtr = keySpan)
        {
            var reader = new MemReader(keyPtr, keySpan.Length);
            IReaderCtx ctx = null;
            if (_keyHandler.NeedsCtx()) ctx = new DBReaderCtx(_tr);
            return _keyReader(ref reader, ctx);
        }
    }

    TValue ByteArrayToValue(ref MemReader reader)
    {
        IReaderCtx ctx = null;
        if (_valueHandler.NeedsCtx()) ctx = new DBReaderCtx(_tr);
        return _valueReader(ref reader, ctx);
    }

    [SkipLocalsInit]
    public bool ContainsKey(TKey key)
    {
        using var cursor = _keyValueTr.CreateCursor();
        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        var keyBytes = KeyToByteArray(key, ref writer);
        return cursor.Find(keyBytes, 0) == FindResult.Exact;
    }

    [SkipLocalsInit]
    public void Add(TKey key, TValue value)
    {
        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        var keyBytes = KeyToByteArray(key, ref writer);
        var valueBytes = ValueToByteArray(value, ref writer);
        using var cursor = _keyValueTr.CreateCursor();
        if (cursor.FindExactKey(keyBytes))
        {
            throw new ArgumentException("Cannot Add duplicate key to Dictionary");
        }

        cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        NotifyAdded();
    }

    [SkipLocalsInit]
    public bool Remove(TKey key)
    {
        using var cursor = _keyValueTr.CreateCursor();
        if (FindKey(cursor, key) != FindResult.Exact) return false;
        cursor.EraseCurrent();
        NotifyRemoved();
        return true;
    }

    [SkipLocalsInit]
    public unsafe bool TryGetValue(TKey key, out TValue value)
    {
        Span<byte> buf = stackalloc byte[4096];
        using var cursor = _keyValueTr.CreateCursor();
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        var keyBytes = KeyToByteArray(key, ref writer);
        var found = cursor.FindExactKey(keyBytes);
        if (!found)
        {
            value = default;
            return false;
        }

        var valueSpan = cursor.GetValueSpan(ref buf);
        fixed (void* valuePtr = valueSpan)
        {
            var reader = new MemReader(valuePtr, valueSpan.Length);
            value = ByteArrayToValue(ref reader);
        }

        return true;
    }

    public unsafe TValue this[TKey key]
    {
        [SkipLocalsInit]
        get
        {
            Span<byte> buf = stackalloc byte[4096];
            using var cursor = _keyValueTr.CreateCursor();
            var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
            var keyBytes = KeyToByteArray(key, ref writer);
            var found = cursor.FindExactKey(keyBytes);
            if (!found)
            {
                throw new ArgumentException("Key not found in Dictionary");
            }

            var valueSpan = cursor.GetValueSpan(ref buf);
            fixed (void* valuePtr = valueSpan)
            {
                var reader = new MemReader(valuePtr, valueSpan.Length);
                return ByteArrayToValue(ref reader);
            }
        }
        [SkipLocalsInit]
        set
        {
            var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
            var keyBytes = KeyToByteArray(key, ref writer);
            var valueBytes = ValueToByteArray(value, ref writer);
            using var cursor = _keyValueTr.CreateCursor();
            if (cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes))
            {
                NotifyAdded();
            }
        }
    }

    [SkipLocalsInit]
    unsafe TValue DeserializeValue(IKeyValueDBCursor cursor)
    {
        Span<byte> buf = stackalloc byte[4096];
        var valueSpan = cursor.GetValueSpan(ref buf);
        fixed (void* valuePtr = valueSpan)
        {
            var reader = new MemReader(valuePtr, valueSpan.Length);
            return ByteArrayToValue(ref reader);
        }
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

    class KeysCollection : ICollection<TKey>
    {
        readonly ODBDictionary<TKey, TValue> _parent;

        public KeysCollection(ODBDictionary<TKey, TValue> parent)
        {
            _parent = parent;
        }

        public IEnumerator<TKey> GetEnumerator()
        {
            using var cursor = _parent._keyValueTr.CreateCursor();
            while (cursor.FindNextKey(_parent._prefix))
            {
                var key = _parent.CurrentToKey(cursor);
                yield return key;
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Add(TKey item)
        {
            _parent.Add(item!, default);
        }

        public void Clear()
        {
            _parent.Clear();
        }

        public bool Contains(TKey item) => _parent.ContainsKey(item!);

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

        public bool Remove(TKey item) => _parent.Remove(item!);

        public int Count => _parent.Count;

        public bool IsReadOnly => false;
    }

    public ICollection<TKey> Keys => _keysCollection ??= new KeysCollection(this);

    class ValuesCollection : ICollection<TValue>
    {
        readonly ODBDictionary<TKey, TValue> _parent;

        public ValuesCollection(ODBDictionary<TKey, TValue> parent)
        {
            _parent = parent;
        }

        public IEnumerator<TValue> GetEnumerator()
        {
            using var cursor = _parent._keyValueTr.CreateCursor();
            while (cursor.FindNextKey(_parent._prefix))
            {
                var value = _parent.DeserializeValue(cursor);
                yield return value;
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Add(TValue item)
        {
            throw new NotSupportedException();
        }

        public void Clear()
        {
            _parent.Clear();
        }

        public bool Contains(TValue item)
        {
            return this.Any(i => EqualityComparer<TValue>.Default.Equals(i, item));
        }

        public void CopyTo(TValue[] array, int arrayIndex)
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

        public bool Remove(TValue item)
        {
            throw new NotSupportedException();
        }

        public int Count => _parent.Count;

        public bool IsReadOnly => true;
    }

    public ICollection<TValue> Values => _valuesCollection ??= new ValuesCollection(this);

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        using var cursor = _keyValueTr.CreateCursor();
        while (cursor.FindNextKey(_prefix))
        {
            var key = CurrentToKey(cursor);
            var value = DeserializeValue(cursor);
            yield return new(key, value);
        }
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> GetReverseEnumerator()
    {
        using var cursor = _keyValueTr.CreateCursor();
        while (cursor.FindPreviousKey(_prefix))
        {
            var key = CurrentToKey(cursor);
            var value = DeserializeValue(cursor);
            yield return new(key, value);
        }
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> GetIncreasingEnumerator(TKey start)
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
            var value = DeserializeValue(cursor);
            yield return new(key, value);
        } while (cursor.FindNextKey(_prefix));
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> GetDecreasingEnumerator(TKey start)
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
            var value = DeserializeValue(cursor);
            yield return new(key, value);
        } while (cursor.FindPreviousKey(_prefix));
    }

    public long RemoveRange(TKey start, bool includeStart, TKey end, bool includeEnd)
    {
        var startCursor = _keyValueTr.CreateCursor();
        var result = FindKey(startCursor, start);
        if (result == FindResult.NotFound) return 0;
        if (result == FindResult.Exact)
        {
            if (!includeStart)
                if (!startCursor.FindNextKey(_prefix))
                    return 0;
        }
        else if (result == FindResult.Previous)
        {
            if (!startCursor.FindNextKey(_prefix))
                return 0;
        }

        var endCursor = _keyValueTr.CreateCursor();
        result = FindKey(endCursor, end);
        if (result == FindResult.Exact)
        {
            if (!includeEnd)
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
            yield return cursor.GetStorageSizeOfCurrentKey();
        }
    }

    [SkipLocalsInit]
    public KeyValuePair<uint, uint> QuerySizeByKey(TKey key)
    {
        using var cursor = _keyValueTr.CreateCursor();
        if (FindKey(cursor, key) != FindResult.Exact)
        {
            throw new ArgumentException("Key not found in Dictionary");
        }

        return cursor.GetStorageSizeOfCurrentKey();
    }

#pragma warning disable 693 // generic parameters named same
    class AdvancedEnumerator<TKey, TValue> : IOrderedDictionaryEnumerator<TKey, TValue>
#pragma warning restore 693
    {
        readonly ODBDictionary<TKey, TValue> _owner;
        IKeyValueDBCursor? _startCursor;
        IKeyValueDBCursor? _endCursor;
        IKeyValueDBCursor? _cursor;
        SeekState _seekState;
        readonly bool _ascending;

        public AdvancedEnumerator(ODBDictionary<TKey, TValue> owner, AdvancedEnumeratorParam<TKey> param)
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

        public uint Count => _cursor == null
            ? 0u
            : (uint)Math.Min(uint.MaxValue, _endCursor!.GetKeyIndex() - _startCursor!.GetKeyIndex() + 1);

        public TValue CurrentValue
        {
            get
            {
                if (_cursor == null) throw new IndexOutOfRangeException();
                if (_seekState == SeekState.Undefined)
                    throw new BTDBException("Invalid access to uninitialized CurrentValue.");

                return _owner.DeserializeValue(_cursor);
            }

            [SkipLocalsInit]
            set
            {
                if (_cursor == null) throw new IndexOutOfRangeException();
                if (_seekState == SeekState.Undefined)
                    throw new BTDBException("Invalid access to uninitialized CurrentValue.");
                var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
                var valueBytes = _owner.ValueToByteArray(value, ref writer);
                _cursor.SetValue(valueBytes);
            }
        }

        public uint Position
        {
            get
            {
                if (_cursor == null) return 0;
                return (uint)(_ascending
                    ? _cursor.GetKeyIndex() - _startCursor!.GetKeyIndex()
                    : _endCursor!.GetKeyIndex() - _cursor.GetKeyIndex());
            }

            set
            {
                if (value >= Count) throw new IndexOutOfRangeException();
                if (_ascending)
                {
                    _cursor!.FindKeyIndex(_startCursor!.GetKeyIndex() + value);
                }
                else
                {
                    _cursor!.FindKeyIndex(_endCursor!.GetKeyIndex() - value);
                }

                _seekState = SeekState.Undefined;
            }
        }

        public bool NextKey(out TKey key)
        {
            if (_cursor == null)
            {
                key = default;
                return false;
            }

            if (_seekState == SeekState.Ready)
            {
                if (_ascending)
                {
                    if (!_cursor.FindNextKey(_owner._prefix))
                    {
                        key = default;
                        return false;
                    }

                    if (_cursor.GetKeyIndex() > _endCursor!.GetKeyIndex())
                    {
                        key = default;
                        return false;
                    }
                }
                else
                {
                    if (!_cursor.FindPreviousKey(_owner._prefix))
                    {
                        key = default;
                        return false;
                    }

                    if (_cursor.GetKeyIndex() < _startCursor!.GetKeyIndex())
                    {
                        key = default;
                        return false;
                    }
                }
            }
            else
            {
                _seekState = SeekState.Ready;
            }

            key = _owner.CurrentToKey(_cursor);
            return true;
        }

        public void Dispose()
        {
            _startCursor?.Dispose();
            _startCursor = null;
            _endCursor?.Dispose();
            _endCursor = null;
            _cursor?.Dispose();
            _cursor = null;
        }
    }

    public IOrderedDictionaryEnumerator<TKey, TValue> GetAdvancedEnumerator(AdvancedEnumeratorParam<TKey> param)
    {
        return new AdvancedEnumerator<TKey, TValue>(this, param);
    }
}
