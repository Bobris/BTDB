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

public delegate T ReaderFun<out T>(ref SpanReader reader, IReaderCtx? ctx);
delegate void WriterFun<in T>(T value, ref SpanWriter writer, IWriterCtx? ctx);
delegate void FreeContentFun(IInternalObjectDBTransaction transaction, ref SpanReader reader, IList<ulong> dictIds);

public class ODBDictionary<TKey, TValue> : IOrderedDictionary<TKey, TValue>, IQuerySizeDictionary<TKey>
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
    int _modificationCounter;
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
        PackUnpack.UnsafePackVUInt(ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(prefix.AsSpan()), (IntPtr)ObjectDB.AllDictionariesPrefixLen), id, len);
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

    static void ThrowModifiedDuringEnum()
    {
        throw new InvalidOperationException("DB modified during iteration");
    }

    public static void DoSave(ref SpanWriter writer, IWriterCtx ctx, IDictionary<TKey, TValue>? dictionary, int cfgId)
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
        var dict = new ODBDictionary<TKey, TValue>(tr, ODBDictionaryConfiguration.Get(cfgId), id);
        dict.FreeContent(ctx, cfgId);
    }

    void FreeContent(IReaderCtx readerCtx, int cfgId)
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

        long prevProtectionCounter = 0;
        long pos = 0;
        while (true)
        {
            if (pos == 0)
            {
                if (!_keyValueTr.FindFirstKey(_prefix)) break;
            }
            else
            {
                if (_keyValueTr.CursorMovedCounter != prevProtectionCounter)
                {
                    if (!_keyValueTr.SetKeyIndex(_prefix, pos)) break;
                }
                else
                {
                    if (!_keyValueTr.FindNextKey(_prefix)) break;
                }
            }

            prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            var valueBytes = _keyValueTr.GetValue();
            var valueReader = new SpanReader(valueBytes);
            findIDictAction(ctx.GetTransaction(), ref valueReader, ctx.DictIds);

            pos++;
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
        _modificationCounter++;
        _keyValueTr.EraseAll(_prefix);
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
                _count = (int)Math.Min(_keyValueTr.GetKeyValueCount(_prefix), int.MaxValue);
            }

            return _count;
        }
    }

    public bool IsReadOnly => false;

    ReadOnlySpan<byte> KeyToByteArray(TKey key)
    {
        var writer = new SpanWriter();
        writer.WriteBlock(_prefix);
        IWriterCtx ctx = null;
        if (_keyHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr);
        _keyWriter(key, ref writer, ctx);
        return writer.GetSpan();
    }

    ReadOnlySpan<byte> ValueToByteArray(TValue value)
    {
        var writer = new SpanWriter();
        IWriterCtx ctx = null;
        if (_valueHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr);
        _valueWriter(value, ref writer, ctx);
        return writer.GetSpan();
    }

    ReadOnlySpan<byte> KeyToByteArray(TKey key, ref SpanWriter writer)
    {
        writer.WriteBlock(_prefix);
        IWriterCtx ctx = null;
        if (_keyHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr);
        _keyWriter(key, ref writer, ctx);
        return writer.GetPersistentSpanAndReset();
    }

    ReadOnlySpan<byte> ValueToByteArray(TValue value, ref SpanWriter writer)
    {
        IWriterCtx ctx = null;
        if (_valueHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr);
        _valueWriter(value, ref writer, ctx);
        return writer.GetPersistentSpanAndReset();
    }

    [SkipLocalsInit]
    TKey CurrentToKey()
    {
        Span<byte> buffer = stackalloc byte[512];
        var reader = new SpanReader(_keyValueTr.GetKey(ref MemoryMarshal.GetReference(buffer), buffer.Length).Slice(_prefix.Length));
        IReaderCtx ctx = null;
        if (_keyHandler.NeedsCtx()) ctx = new DBReaderCtx(_tr);
        return _keyReader(ref reader, ctx);
    }

    TValue ByteArrayToValue(in ReadOnlySpan<byte> data)
    {
        var reader = new SpanReader(data);
        IReaderCtx ctx = null;
        if (_valueHandler.NeedsCtx()) ctx = new DBReaderCtx(_tr);
        return _valueReader(ref reader, ctx);
    }

    [SkipLocalsInit]
    public bool ContainsKey(TKey key)
    {
        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyToByteArray(key, ref writer);
        return _keyValueTr.Find(keyBytes, 0) == FindResult.Exact;
    }

    [SkipLocalsInit]
    public void Add(TKey key, TValue value)
    {
        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyToByteArray(key, ref writer);
        var valueBytes = ValueToByteArray(value, ref writer);
        _modificationCounter++;
        if (_keyValueTr.Find(keyBytes, 0) == FindResult.Exact)
        {
            throw new ArgumentException("Cannot Add duplicate key to Dictionary");
        }

        _tr.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        NotifyAdded();
    }

    [SkipLocalsInit]
    public bool Remove(TKey key)
    {
        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyToByteArray(key, ref writer);
        _modificationCounter++;
        var found = _keyValueTr.EraseCurrent(keyBytes);
        if (found)
        {
            NotifyRemoved();
        }

        return found;
    }

    [SkipLocalsInit]
    public bool TryGetValue(TKey key, out TValue value)
    {
        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyToByteArray(key, ref writer);
        var found = _keyValueTr.FindExactKey(keyBytes);
        if (!found)
        {
            value = default;
            return false;
        }

        var valueBytes = _keyValueTr.GetValue();
        value = ByteArrayToValue(valueBytes);
        return true;
    }

    public TValue this[TKey key]
    {
        [SkipLocalsInit]
        get
        {
            Span<byte> buf = stackalloc byte[512];
            var writer = new SpanWriter(buf);
            var keyBytes = KeyToByteArray(key, ref writer);
            var found = _keyValueTr.FindExactKey(keyBytes);
            if (!found)
            {
                throw new ArgumentException("Key not found in Dictionary");
            }

            var valueBytes = _keyValueTr.GetClonedValue(ref MemoryMarshal.GetReference(writer.Buf), writer.Buf.Length);
            return ByteArrayToValue(valueBytes);
        }
        [SkipLocalsInit]
        set
        {
            Span<byte> buf = stackalloc byte[512];
            var writer = new SpanWriter(buf);
            var keyBytes = KeyToByteArray(key, ref writer);
            var valueBytes = ValueToByteArray(value, ref writer);
            if (_keyValueTr.CreateOrUpdateKeyValue(keyBytes, valueBytes))
            {
                _modificationCounter++;
                NotifyAdded();
            }
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
                _count = (int)Math.Min(_keyValueTr.GetKeyValueCount(), int.MaxValue);
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
            long prevProtectionCounter = 0;
            var prevModificationCounter = 0;
            long pos = 0;
            while (true)
            {
                if (pos == 0)
                {
                    prevModificationCounter = _parent._modificationCounter;
                    if (!_parent._keyValueTr.FindFirstKey(_parent._prefix)) break;
                }
                else
                {
                    if (_parent._keyValueTr.CursorMovedCounter != prevProtectionCounter)
                    {
                        if (prevModificationCounter != _parent._modificationCounter)
                            ThrowModifiedDuringEnum();
                        if (!_parent._keyValueTr.SetKeyIndex(_parent._prefix, pos)) break;
                    }
                    else
                    {
                        if (!_parent._keyValueTr.FindNextKey(_parent._prefix)) break;
                    }
                }

                prevProtectionCounter = _parent._keyValueTr.CursorMovedCounter;
                var key = _parent.CurrentToKey();
                yield return key;
                pos++;
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
            long prevProtectionCounter = 0;
            var prevModificationCounter = 0;
            long pos = 0;
            while (true)
            {
                if (pos == 0)
                {
                    prevModificationCounter = _parent._modificationCounter;
                    if (!_parent._keyValueTr.FindFirstKey(_parent._prefix)) break;
                }
                else
                {
                    if (_parent._keyValueTr.CursorMovedCounter != prevProtectionCounter)
                    {
                        if (prevModificationCounter != _parent._modificationCounter)
                            ThrowModifiedDuringEnum();
                        if (!_parent._keyValueTr.SetKeyIndex(_parent._prefix, pos)) break;
                    }
                    else
                    {
                        if (!_parent._keyValueTr.FindNextKey(_parent._prefix)) break;
                    }
                }

                prevProtectionCounter = _parent._keyValueTr.CursorMovedCounter;
                var valueBytes = _parent._keyValueTr.GetValue();
                var value = _parent.ByteArrayToValue(valueBytes);
                yield return value;
                pos++;
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
        long prevProtectionCounter = 0;
        var prevModificationCounter = 0;
        long pos = 0;
        while (true)
        {
            if (pos == 0)
            {
                prevModificationCounter = _modificationCounter;
                if (!_keyValueTr.FindFirstKey(_prefix)) break;
            }
            else
            {
                if (_keyValueTr.CursorMovedCounter != prevProtectionCounter)
                {
                    if (prevModificationCounter != _modificationCounter)
                        ThrowModifiedDuringEnum();
                    if (!_keyValueTr.SetKeyIndex(_prefix, pos)) break;
                }
                else
                {
                    if (!_keyValueTr.FindNextKey(_prefix)) break;
                }
            }

            prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            var valueBytes = _keyValueTr.GetValue();
            var key = CurrentToKey();
            var value = ByteArrayToValue(valueBytes);
            yield return new KeyValuePair<TKey, TValue>(key, value);
            pos++;
        }
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> GetReverseEnumerator()
    {
        long prevProtectionCounter = 0;
        var prevModificationCounter = 0;
        var pos = long.MaxValue;
        while (true)
        {
            if (pos == long.MaxValue)
            {
                prevModificationCounter = _modificationCounter;
                if (!_keyValueTr.FindFirstKey(_prefix)) break;
                pos = _keyValueTr.GetKeyIndex();
                _keyValueTr.FindLastKey(_prefix);
                pos = _keyValueTr.GetKeyIndex() - pos;
            }
            else
            {
                if (_keyValueTr.CursorMovedCounter != prevProtectionCounter)
                {
                    if (prevModificationCounter != _modificationCounter)
                        ThrowModifiedDuringEnum();
                    if (!_keyValueTr.SetKeyIndex(_prefix, pos)) break;
                }
                else
                {
                    if (!_keyValueTr.FindPreviousKey(_prefix)) break;
                }
            }

            prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            var valueBytes = _keyValueTr.GetValue();
            var key = CurrentToKey();
            var value = ByteArrayToValue(valueBytes);
            yield return new KeyValuePair<TKey, TValue>(key, value);
            pos--;
        }
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> GetIncreasingEnumerator(TKey start)
    {
        var startKeyBytes = KeyToByteArray(start).ToArray();
        long prevProtectionCounter = 0;
        var prevModificationCounter = 0;
        long pos = 0;
        while (true)
        {
            if (pos == 0)
            {
                prevModificationCounter = _modificationCounter;
                if (!_keyValueTr.FindFirstKey(_prefix))
                    break;
                pos = _keyValueTr.GetKeyIndex();

                bool startOk;
                switch (_keyValueTr.Find(startKeyBytes, (uint)_prefix.Length))
                {
                    case FindResult.Exact:
                    case FindResult.Next:
                        startOk = true;
                        break;
                    case FindResult.Previous:
                        startOk = _keyValueTr.FindNextKey(_prefix);
                        break;
                    case FindResult.NotFound:
                        startOk = false;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (!startOk) break;
                pos = _keyValueTr.GetKeyIndex() - pos;
            }
            else
            {
                if (_keyValueTr.CursorMovedCounter != prevProtectionCounter)
                {
                    if (prevModificationCounter != _modificationCounter)
                        ThrowModifiedDuringEnum();
                    if (!_keyValueTr.SetKeyIndex(_prefix, pos)) break;
                }
                else
                {
                    if (!_keyValueTr.FindNextKey(_prefix)) break;
                }
            }

            prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            var valueBytes = _keyValueTr.GetValue();
            var key = CurrentToKey();
            var value = ByteArrayToValue(valueBytes);
            yield return new KeyValuePair<TKey, TValue>(key, value);
            pos++;
        }
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> GetDecreasingEnumerator(TKey start)
    {
        var startKeyBytes = KeyToByteArray(start).ToArray();
        long prevProtectionCounter = 0;
        var prevModificationCounter = 0;
        var pos = long.MaxValue;
        while (true)
        {
            if (pos == long.MaxValue)
            {
                prevModificationCounter = _modificationCounter;
                if (!_keyValueTr.FindFirstKey(_prefix))
                    break;
                pos = _keyValueTr.GetKeyIndex();
                bool startOk;
                switch (_keyValueTr.Find(startKeyBytes, (uint)_prefix.Length))
                {
                    case FindResult.Exact:
                    case FindResult.Previous:
                        startOk = true;
                        break;
                    case FindResult.Next:
                        startOk = _keyValueTr.FindPreviousKey(_prefix);
                        break;
                    case FindResult.NotFound:
                        startOk = false;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (!startOk) break;
                pos = _keyValueTr.GetKeyIndex() - pos;
            }
            else
            {
                if (_keyValueTr.CursorMovedCounter != prevProtectionCounter)
                {
                    if (prevModificationCounter != _modificationCounter)
                        ThrowModifiedDuringEnum();
                    if (!_keyValueTr.SetKeyIndex(_prefix, pos)) break;
                }
                else
                {
                    if (!_keyValueTr.FindPreviousKey(_prefix)) break;
                }
            }

            prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            var valueBytes = _keyValueTr.GetValue();
            var key = CurrentToKey();
            var value = ByteArrayToValue(valueBytes);
            yield return new KeyValuePair<TKey, TValue>(key, value);
            pos--;
        }
    }

    public long RemoveRange(TKey start, bool includeStart, TKey end, bool includeEnd)
    {
        var startKeyBytes = KeyToByteArray(start);
        var endKeyBytes = KeyToByteArray(end);
        _modificationCounter++;
        var result = _keyValueTr.Find(startKeyBytes, (uint)_prefix.Length);
        if (result == FindResult.NotFound) return 0;
        var startIndex = _keyValueTr.GetKeyIndex();
        if (result == FindResult.Exact)
        {
            if (!includeStart) startIndex++;
        }
        else if (result == FindResult.Previous)
        {
            startIndex++;
        }

        result = _keyValueTr.Find(endKeyBytes, (uint)_prefix.Length);
        var endIndex = _keyValueTr.GetKeyIndex();
        if (result == FindResult.Exact)
        {
            if (!includeEnd) endIndex--;
        }
        else if (result == FindResult.Next)
        {
            endIndex--;
        }

        _keyValueTr.EraseRange(startIndex, endIndex);
        _count = -1;
        return Math.Max(0, endIndex - startIndex + 1);
    }

    public IEnumerable<KeyValuePair<uint, uint>> QuerySizeEnumerator()
    {
        long prevProtectionCounter = 0;
        var prevModificationCounter = 0;
        long pos = 0;
        while (true)
        {
            if (pos == 0)
            {
                prevModificationCounter = _modificationCounter;
                if (!_keyValueTr.FindFirstKey(_prefix)) break;
            }
            else
            {
                if (_keyValueTr.CursorMovedCounter != prevProtectionCounter)
                {
                    if (prevModificationCounter != _modificationCounter)
                        ThrowModifiedDuringEnum();
                    if (!_keyValueTr.SetKeyIndex(_prefix, pos)) break;
                }
                else
                {
                    if (!_keyValueTr.FindNextKey(_prefix)) break;
                }
            }

            prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            var size = _keyValueTr.GetStorageSizeOfCurrentKey();
            yield return size;
            pos++;
        }
    }

    public KeyValuePair<uint, uint> QuerySizeByKey(TKey key)
    {
        var keyBytes = KeyToByteArray(key);
        var found = _keyValueTr.Find(keyBytes, 0) == FindResult.Exact;
        if (!found)
        {
            throw new ArgumentException("Key not found in Dictionary");
        }

        var size = _keyValueTr.GetStorageSizeOfCurrentKey();
        return size;
    }

#pragma warning disable 693 // generic parameters named same
    class AdvancedEnumerator<TKey, TValue> : IOrderedDictionaryEnumerator<TKey, TValue>
#pragma warning restore 693
    {
        readonly ODBDictionary<TKey, TValue> _owner;
        readonly IKeyValueDBTransaction _keyValueTr;
        long _prevProtectionCounter;
        readonly int _prevModificationCounter;
        readonly uint _startPos;
        readonly uint _count;
        uint _pos;
        SeekState _seekState;
        readonly bool _ascending;

        public AdvancedEnumerator(ODBDictionary<TKey, TValue> owner, AdvancedEnumeratorParam<TKey> param)
        {
            _owner = owner;
            _keyValueTr = _owner._keyValueTr;
            _ascending = param.Order == EnumerationOrder.Ascending;
            _prevModificationCounter = _owner._modificationCounter;
            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            _keyValueTr.FindFirstKey(_owner._prefix);
            var prefixIndex = _keyValueTr.GetKeyIndex();
            long startIndex;
            long endIndex;
            if (param.EndProposition == KeyProposition.Ignored)
            {
                _keyValueTr.FindLastKey(_owner._prefix);
                endIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
            }
            else
            {
                var keyBytes = _owner.KeyToByteArray(param.End);
                switch (_keyValueTr.Find(keyBytes, (uint)_owner._prefix.Length))
                {
                    case FindResult.Exact:
                        endIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                        if (param.EndProposition == KeyProposition.Excluded)
                        {
                            endIndex--;
                        }

                        break;
                    case FindResult.Previous:
                        endIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                        break;
                    case FindResult.Next:
                        endIndex = _keyValueTr.GetKeyIndex() - prefixIndex - 1;
                        break;
                    case FindResult.NotFound:
                        endIndex = -1;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            if (param.StartProposition == KeyProposition.Ignored)
            {
                startIndex = 0;
            }
            else
            {
                var keyBytes = _owner.KeyToByteArray(param.Start);
                switch (_keyValueTr.Find(keyBytes, (uint)_owner._prefix.Length))
                {
                    case FindResult.Exact:
                        startIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                        if (param.StartProposition == KeyProposition.Excluded)
                        {
                            startIndex++;
                        }

                        break;
                    case FindResult.Previous:
                        startIndex = _keyValueTr.GetKeyIndex() - prefixIndex + 1;
                        break;
                    case FindResult.Next:
                        startIndex = _keyValueTr.GetKeyIndex() - prefixIndex;
                        break;
                    case FindResult.NotFound:
                        startIndex = 0;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            _count = (uint)Math.Max(0, endIndex - startIndex + 1);
            _startPos = (uint)(_ascending ? startIndex : endIndex);
            _pos = 0;
            _seekState = SeekState.Undefined;
        }

        public uint Count => _count;

        public TValue CurrentValue
        {
            get
            {
                if (_pos >= _count) throw new IndexOutOfRangeException();
                if (_seekState == SeekState.Undefined)
                    throw new BTDBException("Invalid access to uninitialized CurrentValue.");

                if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
                {
                    if (_prevModificationCounter != _owner._modificationCounter)
                        ThrowModifiedDuringEnum();
                    Seek();
                }
                else if (_seekState != SeekState.Ready)
                {
                    Seek();
                }

                _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
                var valueBytes = _keyValueTr.GetValue();
                return _owner.ByteArrayToValue(valueBytes);
            }

            set
            {
                if (_pos >= _count) throw new IndexOutOfRangeException();
                if (_seekState == SeekState.Undefined)
                    throw new BTDBException("Invalid access to uninitialized CurrentValue.");
                if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
                {
                    if (_prevModificationCounter != _owner._modificationCounter)
                        ThrowModifiedDuringEnum();
                    Seek();
                }
                else if (_seekState != SeekState.Ready)
                {
                    Seek();
                }

                _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
                var valueBytes = _owner.ValueToByteArray(value);
                _keyValueTr.SetValue(valueBytes);
            }
        }

        void Seek()
        {
            if (_ascending)
                _keyValueTr.SetKeyIndex(_owner._prefix, _startPos + _pos);
            else
                _keyValueTr.SetKeyIndex(_owner._prefix, _startPos - _pos);
            _seekState = SeekState.Ready;
        }

        public uint Position
        {
            get => _pos;

            set
            {
                _pos = value > _count ? _count : value;
                _seekState = SeekState.SeekNeeded;
            }
        }

        public bool NextKey(out TKey key)
        {
            if (_seekState == SeekState.Ready)
                _pos++;
            if (_pos >= _count)
            {
                key = default;
                return false;
            }

            if (_keyValueTr.CursorMovedCounter != _prevProtectionCounter)
            {
                if (_prevModificationCounter != _owner._modificationCounter)
                    ThrowModifiedDuringEnum();
                Seek();
            }
            else if (_seekState != SeekState.Ready)
            {
                Seek();
            }
            else
            {
                if (_ascending)
                {
                    _keyValueTr.FindNextKey(_owner._prefix);
                }
                else
                {
                    _keyValueTr.FindPreviousKey(_owner._prefix);
                }
            }

            _prevProtectionCounter = _keyValueTr.CursorMovedCounter;
            key = _owner.CurrentToKey();
            return true;
        }
    }

    public IOrderedDictionaryEnumerator<TKey, TValue> GetAdvancedEnumerator(AdvancedEnumeratorParam<TKey> param)
    {
        return new AdvancedEnumerator<TKey, TValue>(this, param);
    }
}
