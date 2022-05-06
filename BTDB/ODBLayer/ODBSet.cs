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

public class ODBSet<TKey> : IOrderedSet<TKey>, IQuerySizeDictionary<TKey>
{
    readonly IInternalObjectDBTransaction _tr;
    readonly IFieldHandler _keyHandler;
    readonly ReaderFun<TKey> _keyReader;
    readonly WriterFun<TKey> _keyWriter;
    readonly IKeyValueDBTransaction _keyValueTr;
    readonly ulong _id;
    readonly byte[] _prefix;
    int _count;
    int _modificationCounter;

    // ReSharper disable once MemberCanBePrivate.Global used by FieldHandler.Load
    public ODBSet(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config, ulong id)
    {
        _tr = tr;
        _keyHandler = config.KeyHandler;
        _id = id;
        var len = PackUnpack.LengthVUInt(id);
        var prefix = new byte[ObjectDB.AllDictionariesPrefixLen + len];
        MemoryMarshal.GetReference(prefix.AsSpan()) = ObjectDB.AllDictionariesPrefixByte;
        PackUnpack.UnsafePackVUInt(ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(prefix.AsSpan()), (IntPtr)ObjectDB.AllDictionariesPrefixLen), id, len);
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

    public static void DoSave(ref SpanWriter writer, IWriterCtx ctx, IOrderedSet<TKey>? dictionary, int cfgId)
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
            Add(key);
        }
    }

    public void Clear()
    {
        _modificationCounter++;
        _keyValueTr.EraseAll(_prefix);
        _count = 0;
    }

    public bool Contains(TKey item)
    {
        var keyBytes = KeyToByteArray(item);
        return _keyValueTr.Find(keyBytes, 0) == FindResult.Exact;
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
                _count = (int)Math.Min(_keyValueTr.GetKeyValueCount(_prefix), int.MaxValue);
            }

            return _count;
        }
    }

    public bool IsReadOnly => false;

    ReadOnlySpan<byte> KeyToByteArray(TKey key)
    {
        var writer = new SpanWriter();
        IWriterCtx ctx = null;
        if (_keyHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr);
        writer.WriteBlock(_prefix);
        _keyWriter(key, ref writer, ctx);
        return writer.GetSpan();
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

    public bool Add(TKey key)
    {
        var keyBytes = KeyToByteArray(key);
        _modificationCounter++;
        var created = _tr.CreateOrUpdateKeyValue(keyBytes, new ReadOnlySpan<byte>());
        if (created) NotifyAdded();
        return created;
    }

    public bool Remove(TKey key)
    {
        var keyBytes = KeyToByteArray(key);
        _modificationCounter++;
        if (_keyValueTr.EraseCurrent(keyBytes))
        {
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
                _count = (int)Math.Min(_keyValueTr.GetKeyValueCount(), int.MaxValue);
            }
            else
            {
                _count--;
            }
        }
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
            var key = CurrentToKey();
            yield return key;
            pos++;
        }
    }

    public IEnumerable<TKey> GetReverseEnumerator()
    {
        long prevProtectionCounter = 0;
        var prevModificationCounter = 0;
        var pos = long.MaxValue;
        while (true)
        {
            if (pos == long.MaxValue)
            {
                prevModificationCounter = _modificationCounter;
                if (!_keyValueTr.FindLastKey(_prefix)) break;
                pos = _keyValueTr.GetKeyIndex();
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
            var key = CurrentToKey();
            yield return key;
            pos--;
        }
    }

    public IEnumerable<TKey> GetIncreasingEnumerator(TKey start)
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
                pos = _keyValueTr.GetKeyIndex();
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
            var key = CurrentToKey();
            yield return key;
            pos++;
        }
    }

    public IEnumerable<TKey> GetDecreasingEnumerator(TKey start)
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
                pos = _keyValueTr.GetKeyIndex(_prefix);
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
            var key = CurrentToKey();
            yield return key;
            pos--;
        }
    }

    public long RemoveRange(AdvancedEnumeratorParam<TKey> param)
    {
        _modificationCounter++;

        _keyValueTr.FindFirstKey(_prefix);
        var prefixIndex = _keyValueTr.GetKeyIndex();
        long startIndex;
        long endIndex;
        if (param.EndProposition == KeyProposition.Ignored)
        {
            _keyValueTr.FindLastKey(_prefix);
            endIndex = _keyValueTr.GetKeyIndex() - prefixIndex - 1;
        }
        else
        {
            var keyBytes = KeyToByteArray(param.End);
            switch (_keyValueTr.Find(keyBytes, (uint)_prefix.Length))
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
            var keyBytes = KeyToByteArray(param.Start);
            switch (_keyValueTr.Find(keyBytes, (uint)_prefix.Length))
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

        _keyValueTr.EraseRange(prefixIndex + startIndex, prefixIndex + endIndex);
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
        var found = _keyValueTr.FindExactKey(keyBytes);
        if (!found)
        {
            throw new ArgumentException("Key not found in Set");
        }

        var size = _keyValueTr.GetStorageSizeOfCurrentKey();
        return size;
    }

    class AdvancedEnumerator : IEnumerable<TKey>, IEnumerator<TKey>
    {
        readonly ODBSet<TKey> _owner;
        readonly IKeyValueDBTransaction _keyValueTr;
        long _prevProtectionCounter;
        readonly int _prevModificationCounter;
        readonly uint _startPos;
        readonly uint _count;
        uint _pos;
        SeekState _seekState;
        readonly bool _ascending;

        public AdvancedEnumerator(ODBSet<TKey> owner, AdvancedEnumeratorParam<TKey> param)
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
                endIndex = _keyValueTr.GetKeyIndex() - prefixIndex - 1;
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

        void Seek()
        {
            if (_ascending)
                _keyValueTr.SetKeyIndex(_owner._prefix, _startPos + _pos);
            else
                _keyValueTr.SetKeyIndex(_owner._prefix, _startPos - _pos);
            _seekState = SeekState.Ready;
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
            if (_seekState == SeekState.Ready)
                _pos++;
            if (_pos >= _count)
            {
                Current = default;
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
            Current = _owner.CurrentToKey();
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
        }
    }

    public IEnumerable<TKey> GetAdvancedEnumerator(AdvancedEnumeratorParam<TKey> param)
    {
        return new AdvancedEnumerator(this, param);
    }
}
