using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class ODBDictionary<TKey, TValue> : IOrderedDictionary<TKey, TValue>, IQuerySizeDictionary<TKey>
    {
        readonly IInternalObjectDBTransaction _tr;
        readonly IFieldHandler _keyHandler;
        readonly IFieldHandler _valueHandler;
        readonly bool _preferInline;
        readonly Func<AbstractBufferedReader, IReaderCtx, TKey> _keyReader;
        readonly Action<TKey, AbstractBufferedWriter, IWriterCtx> _keyWriter;
        readonly Func<AbstractBufferedReader, IReaderCtx, TValue> _valueReader;
        readonly Action<TValue, AbstractBufferedWriter, IWriterCtx> _valueWriter;
        readonly IKeyValueDBTransaction _keyValueTr;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        readonly ulong _id;
        byte[] _prefix;
        int _count;
        int _modificationCounter;
        KeysCollection _keysCollection;
        ValuesCollection _valuesCollection;

        public ODBDictionary(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config, ulong id)
        {
            _tr = tr;
            _keyHandler = config.KeyHandler;
            _valueHandler = config.ValueHandler;
            _preferInline = config.PreferInline;
            _id = id;
            GeneratePrefix();
            _keyReader = (Func<AbstractBufferedReader, IReaderCtx, TKey>)config.KeyReader;
            _keyWriter = (Action<TKey, AbstractBufferedWriter, IWriterCtx>)config.KeyWriter;
            _valueReader = (Func<AbstractBufferedReader, IReaderCtx, TValue>)config.ValueReader;
            _valueWriter = (Action<TValue, AbstractBufferedWriter, IWriterCtx>)config.ValueWriter;
            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _count = -1;
        }

        public ODBDictionary(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config) : this(tr, config, tr.AllocateDictionaryId()) { }

        static void ThrowModifiedDuringEnum()
        {
            throw new InvalidOperationException("DB modified during iteration");
        }

        public static void DoSave(IWriterCtx ctx, IDictionary<TKey, TValue> dictionary, int cfgId)
        {
            var dbctx = (IDBWriterCtx)ctx;
            var goodDict = dictionary as ODBDictionary<TKey, TValue>;
            if (goodDict == null)
            {
                var tr = dbctx.GetTransaction();
                var id = tr.AllocateDictionaryId();
                goodDict = new ODBDictionary<TKey, TValue>(tr, (ODBDictionaryConfiguration)dbctx.FindInstance(cfgId), id);
                if (dictionary != null)
                    foreach (var pair in dictionary)
                        goodDict.Add(pair.Key, pair.Value);
            }
            ctx.Writer().WriteVUInt64(goodDict._id);
        }

        public static void DoFreeContent(IReaderCtx ctx, ulong id, int cfgId)
        {
            var dbctx = (DBReaderCtx)ctx;
            var tr = dbctx.GetTransaction();
            var dict = new ODBDictionary<TKey, TValue>(tr, (ODBDictionaryConfiguration)dbctx.FindInstance(cfgId), id);
            dict.FreeContent(ctx, cfgId);
        }

        void FreeContent(IReaderCtx readerCtx, int cfgId)
        {
            var config = (ODBDictionaryConfiguration)((IInstanceRegistry)readerCtx).FindInstance(cfgId);
            var ctx = (DBReaderWithFreeInfoCtx)readerCtx;

            if (config.FreeContent == null)
            {
                var method = ILBuilder.Instance
                .NewMethod<Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>>>(
                    $"IDictFinder_Cfg_{cfgId}");
                var ilGenerator = method.Generator;

                var readerLoc = ilGenerator.DeclareLocal(typeof(IReaderCtx));
                ilGenerator
                    .Ldarg(0)
                    .Ldarg(1)
                    .Ldarg(2)
                    .Newobj(() => new DBReaderWithFreeInfoCtx(null, null, null))
                    .Stloc(readerLoc);

                Action<IILGen> readerOrCtx;
                if (_valueHandler.NeedsCtx())
                    readerOrCtx = il => il.Ldloc(readerLoc);
                else
                    readerOrCtx = il => il.Ldarg(1);
                _valueHandler.FreeContent(ilGenerator, readerOrCtx);
                ilGenerator.Ret();
                config.FreeContent = method.Create();
            }

            var findIDictAction = (Action<IInternalObjectDBTransaction, AbstractBufferedReader, IList<ulong>>)config.FreeContent;

            long prevProtectionCounter = 0;
            var prevModificationCounter = 0;
            long pos = 0;
            while (true)
            {
                _keyValueTrProtector.Start();
                if (pos == 0)
                {
                    prevModificationCounter = _modificationCounter;
                    _keyValueTr.SetKeyPrefix(_prefix);
                    if (!_keyValueTr.FindFirstKey()) break;
                }
                else
                {
                    if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                    {
                        if (prevModificationCounter != _modificationCounter)
                            ThrowModifiedDuringEnum();
                        _keyValueTr.SetKeyPrefix(_prefix);
                        if (!_keyValueTr.SetKeyIndex(pos)) break;
                    }
                    else
                    {
                        if (!_keyValueTr.FindNextKey()) break;
                    }
                }

                prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var valueBytes = _keyValueTr.GetValueAsByteArray();
                var valueReader = new ByteArrayReader(valueBytes);
                findIDictAction(ctx.GetTransaction(), valueReader, ctx.DictIds);

                pos++;
            }
        }

        void GeneratePrefix()
        {
            var o = ObjectDB.AllDictionariesPrefix.Length;
            _prefix = new byte[o + PackUnpack.LengthVUInt(_id)];
            Array.Copy(ObjectDB.AllDictionariesPrefix, _prefix, o);
            PackUnpack.PackVUInt(_prefix, ref o, _id);
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
            _keyValueTrProtector.Start();
            _modificationCounter++;
            _keyValueTr.SetKeyPrefix(_prefix);
            _keyValueTr.EraseAll();
            _count = 0;
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            TValue value;
            if (!TryGetValue(item.Key, out value)) return false;
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
                    _keyValueTrProtector.Start();
                    _keyValueTr.SetKeyPrefix(_prefix);
                    _count = (int)Math.Min(_keyValueTr.GetKeyValueCount(), int.MaxValue);
                }
                return _count;
            }
        }

        public bool IsReadOnly => false;

        byte[] KeyToByteArray(TKey key)
        {
            var writer = new ByteBufferWriter();
            IWriterCtx ctx = null;
            if (_keyHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr, writer, _preferInline);
            _keyWriter(key, writer, ctx);
            return writer.Data.ToByteArray();
        }

        byte[] ValueToByteArray(TValue value)
        {
            var writer = new ByteBufferWriter();
            IWriterCtx ctx = null;
            if (_valueHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr, writer, _preferInline);
            _valueWriter(value, writer, ctx);
            return writer.Data.ToByteArray();
        }

        TKey ByteArrayToKey(byte[] data)
        {
            var reader = new ByteArrayReader(data);
            IReaderCtx ctx = null;
            if (_keyHandler.NeedsCtx()) ctx = new DBReaderCtx(_tr, reader);
            return _keyReader(reader, ctx);
        }

        TValue ByteArrayToValue(byte[] data)
        {
            var reader = new ByteArrayReader(data);
            IReaderCtx ctx = null;
            if (_valueHandler.NeedsCtx()) ctx = new DBReaderCtx(_tr, reader);
            return _valueReader(reader, ctx);
        }

        public bool ContainsKey(TKey key)
        {
            var keyBytes = KeyToByteArray(key);
            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(_prefix);
            return _keyValueTr.FindExactKey(keyBytes);
        }

        public void Add(TKey key, TValue value)
        {
            var keyBytes = KeyToByteArray(key);
            var valueBytes = ValueToByteArray(value);
            _keyValueTrProtector.Start();
            _modificationCounter++;
            _keyValueTr.SetKeyPrefix(_prefix);
            if (_keyValueTr.FindExactKey(keyBytes))
            {
                throw new ArgumentException("Cannot Add duplicate key to Dictionary");
            }
            _keyValueTr.CreateOrUpdateKeyValueUnsafe(keyBytes, valueBytes);
            NotifyAdded();
        }

        public bool Remove(TKey key)
        {
            var keyBytes = KeyToByteArray(key);
            _keyValueTrProtector.Start();
            _modificationCounter++;
            _keyValueTr.SetKeyPrefix(_prefix);
            var found = _keyValueTr.FindExactKey(keyBytes);
            if (found)
            {
                _keyValueTr.EraseCurrent();
                NotifyRemoved();
            }
            return found;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            var keyBytes = KeyToByteArray(key);
            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(_prefix);
            var found = _keyValueTr.FindExactKey(keyBytes);
            if (!found)
            {
                value = default(TValue);
                return false;
            }
            var valueBytes = _keyValueTr.GetValueAsByteArray();
            value = ByteArrayToValue(valueBytes);
            return true;
        }

        public TValue this[TKey key]
        {
            get
            {
                var keyBytes = KeyToByteArray(key);
                _keyValueTrProtector.Start();
                _keyValueTr.SetKeyPrefix(_prefix);
                var found = _keyValueTr.FindExactKey(keyBytes);
                if (!found)
                {
                    throw new ArgumentException("Key not found in Dictionary");
                }
                var valueBytes = _keyValueTr.GetValueAsByteArray();
                return ByteArrayToValue(valueBytes);
            }
            set
            {
                var keyBytes = KeyToByteArray(key);
                var valueBytes = ValueToByteArray(value);
                _keyValueTrProtector.Start();
                _keyValueTr.SetKeyPrefix(_prefix);
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
                    _parent._keyValueTrProtector.Start();
                    if (pos == 0)
                    {
                        prevModificationCounter = _parent._modificationCounter;
                        _parent._keyValueTr.SetKeyPrefix(_parent._prefix);
                        if (!_parent._keyValueTr.FindFirstKey()) break;
                    }
                    else
                    {
                        if (_parent._keyValueTrProtector.WasInterupted(prevProtectionCounter))
                        {
                            if (prevModificationCounter != _parent._modificationCounter)
                                ThrowModifiedDuringEnum();
                            _parent._keyValueTr.SetKeyPrefix(_parent._prefix);
                            if (!_parent._keyValueTr.SetKeyIndex(pos)) break;
                        }
                        else
                        {
                            if (!_parent._keyValueTr.FindNextKey()) break;
                        }
                    }
                    prevProtectionCounter = _parent._keyValueTrProtector.ProtectionCounter;
                    var keyBytes = _parent._keyValueTr.GetKeyAsByteArray();
                    var key = _parent.ByteArrayToKey(keyBytes);
                    yield return key;
                    pos++;
                }
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public void Add(TKey item)
            {
                _parent.Add(item, default(TValue));
            }

            public void Clear()
            {
                _parent.Clear();
            }

            public bool Contains(TKey item) => _parent.ContainsKey(item);

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

            public bool Remove(TKey item) => _parent.Remove(item);

            public int Count => _parent.Count;

            public bool IsReadOnly => false;
        }

        public ICollection<TKey> Keys => _keysCollection ?? (_keysCollection = new KeysCollection(this));

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
                    _parent._keyValueTrProtector.Start();
                    if (pos == 0)
                    {
                        prevModificationCounter = _parent._modificationCounter;
                        _parent._keyValueTr.SetKeyPrefix(_parent._prefix);
                        if (!_parent._keyValueTr.FindFirstKey()) break;
                    }
                    else
                    {
                        if (_parent._keyValueTrProtector.WasInterupted(prevProtectionCounter))
                        {
                            if (prevModificationCounter != _parent._modificationCounter)
                                ThrowModifiedDuringEnum();
                            _parent._keyValueTr.SetKeyPrefix(_parent._prefix);
                            if (!_parent._keyValueTr.SetKeyIndex(pos)) break;
                        }
                        else
                        {
                            if (!_parent._keyValueTr.FindNextKey()) break;
                        }
                    }
                    prevProtectionCounter = _parent._keyValueTrProtector.ProtectionCounter;
                    var valueBytes = _parent._keyValueTr.GetValueAsByteArray();
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

        public ICollection<TValue> Values => _valuesCollection ?? (_valuesCollection = new ValuesCollection(this));

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            long prevProtectionCounter = 0;
            var prevModificationCounter = 0;
            long pos = 0;
            while (true)
            {
                _keyValueTrProtector.Start();
                if (pos == 0)
                {
                    prevModificationCounter = _modificationCounter;
                    _keyValueTr.SetKeyPrefix(_prefix);
                    if (!_keyValueTr.FindFirstKey()) break;
                }
                else
                {
                    if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                    {
                        if (prevModificationCounter != _modificationCounter)
                            ThrowModifiedDuringEnum();
                        _keyValueTr.SetKeyPrefix(_prefix);
                        if (!_keyValueTr.SetKeyIndex(pos)) break;
                    }
                    else
                    {
                        if (!_keyValueTr.FindNextKey()) break;
                    }
                }
                prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var keyBytes = _keyValueTr.GetKeyAsByteArray();
                var valueBytes = _keyValueTr.GetValueAsByteArray();
                var key = ByteArrayToKey(keyBytes);
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
                _keyValueTrProtector.Start();
                if (pos == long.MaxValue)
                {
                    prevModificationCounter = _modificationCounter;
                    _keyValueTr.SetKeyPrefix(_prefix);
                    if (!_keyValueTr.FindLastKey()) break;
                    pos = _keyValueTr.GetKeyIndex();
                }
                else
                {
                    if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                    {
                        if (prevModificationCounter != _modificationCounter)
                            ThrowModifiedDuringEnum();
                        _keyValueTr.SetKeyPrefix(_prefix);
                        if (!_keyValueTr.SetKeyIndex(pos)) break;
                    }
                    else
                    {
                        if (!_keyValueTr.FindPreviousKey()) break;
                    }
                }
                prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var keyBytes = _keyValueTr.GetKeyAsByteArray();
                var valueBytes = _keyValueTr.GetValueAsByteArray();
                var key = ByteArrayToKey(keyBytes);
                var value = ByteArrayToValue(valueBytes);
                yield return new KeyValuePair<TKey, TValue>(key, value);
                pos--;
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetIncreasingEnumerator(TKey start)
        {
            var startKeyBytes = KeyToByteArray(start);
            long prevProtectionCounter = 0;
            var prevModificationCounter = 0;
            long pos = 0;
            while (true)
            {
                _keyValueTrProtector.Start();
                if (pos == 0)
                {
                    prevModificationCounter = _modificationCounter;
                    _keyValueTr.SetKeyPrefix(_prefix);
                    bool startOk;
                    switch (_keyValueTr.Find(ByteBuffer.NewSync(startKeyBytes)))
                    {
                        case FindResult.Exact:
                        case FindResult.Next:
                            startOk = true;
                            break;
                        case FindResult.Previous:
                            startOk = _keyValueTr.FindNextKey();
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
                    if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                    {
                        if (prevModificationCounter != _modificationCounter)
                            ThrowModifiedDuringEnum();
                        _keyValueTr.SetKeyPrefix(_prefix);
                        if (!_keyValueTr.SetKeyIndex(pos)) break;
                    }
                    else
                    {
                        if (!_keyValueTr.FindNextKey()) break;
                    }
                }
                prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var keyBytes = _keyValueTr.GetKeyAsByteArray();
                var valueBytes = _keyValueTr.GetValueAsByteArray();
                var key = ByteArrayToKey(keyBytes);
                var value = ByteArrayToValue(valueBytes);
                yield return new KeyValuePair<TKey, TValue>(key, value);
                pos++;
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetDecreasingEnumerator(TKey start)
        {
            var startKeyBytes = KeyToByteArray(start);
            long prevProtectionCounter = 0;
            var prevModificationCounter = 0;
            var pos = long.MaxValue;
            while (true)
            {
                _keyValueTrProtector.Start();
                if (pos == long.MaxValue)
                {
                    prevModificationCounter = _modificationCounter;
                    _keyValueTr.SetKeyPrefix(_prefix);
                    bool startOk;
                    switch (_keyValueTr.Find(ByteBuffer.NewSync(startKeyBytes)))
                    {
                        case FindResult.Exact:
                        case FindResult.Previous:
                            startOk = true;
                            break;
                        case FindResult.Next:
                            startOk = _keyValueTr.FindPreviousKey();
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
                    if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                    {
                        if (prevModificationCounter != _modificationCounter)
                            ThrowModifiedDuringEnum();
                        _keyValueTr.SetKeyPrefix(_prefix);
                        if (!_keyValueTr.SetKeyIndex(pos)) break;
                    }
                    else
                    {
                        if (!_keyValueTr.FindPreviousKey()) break;
                    }
                }
                prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var keyBytes = _keyValueTr.GetKeyAsByteArray();
                var valueBytes = _keyValueTr.GetValueAsByteArray();
                var key = ByteArrayToKey(keyBytes);
                var value = ByteArrayToValue(valueBytes);
                yield return new KeyValuePair<TKey, TValue>(key, value);
                pos--;
            }
        }

        public long RemoveRange(TKey start, bool includeStart, TKey end, bool includeEnd)
        {
            var startKeyBytes = KeyToByteArray(start);
            var endKeyBytes = KeyToByteArray(end);
            _keyValueTrProtector.Start();
            _modificationCounter++;
            _keyValueTr.SetKeyPrefix(_prefix);
            var result = _keyValueTr.Find(ByteBuffer.NewAsync(startKeyBytes));
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
            result = _keyValueTr.Find(ByteBuffer.NewAsync(endKeyBytes));
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
            return Math.Max(0, endIndex - startIndex + 1);
        }

        public IEnumerable<KeyValuePair<uint, uint>> QuerySizeEnumerator()
        {
            long prevProtectionCounter = 0;
            var prevModificationCounter = 0;
            long pos = 0;
            while (true)
            {
                _keyValueTrProtector.Start();
                if (pos == 0)
                {
                    prevModificationCounter = _modificationCounter;
                    _keyValueTr.SetKeyPrefix(_prefix);
                    if (!_keyValueTr.FindFirstKey()) break;
                }
                else
                {
                    if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                    {
                        if (prevModificationCounter != _modificationCounter)
                            ThrowModifiedDuringEnum();
                        _keyValueTr.SetKeyPrefix(_prefix);
                        if (!_keyValueTr.SetKeyIndex(pos)) break;
                    }
                    else
                    {
                        if (!_keyValueTr.FindNextKey()) break;
                    }
                }
                prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                var size = _keyValueTr.GetStorageSizeOfCurrentKey();
                yield return size;
                pos++;
            }
        }

        public KeyValuePair<uint, uint> QuerySizeByKey(TKey key)
        {
            var keyBytes = KeyToByteArray(key);
            _keyValueTrProtector.Start();
            _keyValueTr.SetKeyPrefix(_prefix);
            var found = _keyValueTr.FindExactKey(keyBytes);
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
            readonly KeyValueDBTransactionProtector _keyValueTrProtector;
            readonly IKeyValueDBTransaction _keyValueTr;
            long _prevProtectionCounter;
            readonly int _prevModificationCounter;
            readonly uint _startPos = 0;
            readonly uint _count = 0;
            uint _pos;
            bool _seekNeeded;
            readonly bool _ascending;

            public AdvancedEnumerator(ODBDictionary<TKey, TValue> owner, AdvancedEnumeratorParam<TKey> param)
            {
                _owner = owner;
                _keyValueTrProtector = _owner._keyValueTrProtector;
                _keyValueTr = _owner._keyValueTr;
                _ascending = param.Order == EnumerationOrder.Ascending;
                _keyValueTrProtector.Start();
                _prevModificationCounter = _owner._modificationCounter;
                _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                _keyValueTr.SetKeyPrefix(_owner._prefix);
                long startIndex;
                long endIndex;
                if (param.EndProposition == KeyProposition.Ignored)
                {
                    endIndex = _keyValueTr.GetKeyValueCount() - 1;
                }
                else
                {
                    var keyBytes = _owner.KeyToByteArray(param.End);
                    switch (_keyValueTr.Find(ByteBuffer.NewSync(keyBytes)))
                    {
                        case FindResult.Exact:
                            endIndex = _keyValueTr.GetKeyIndex();
                            if (param.EndProposition == KeyProposition.Excluded)
                            {
                                endIndex--;
                            }
                            break;
                        case FindResult.Previous:
                            endIndex = _keyValueTr.GetKeyIndex();
                            break;
                        case FindResult.Next:
                            endIndex = _keyValueTr.GetKeyIndex() - 1;
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
                    switch (_keyValueTr.Find(ByteBuffer.NewSync(keyBytes)))
                    {
                        case FindResult.Exact:
                            startIndex = _keyValueTr.GetKeyIndex();
                            if (param.StartProposition == KeyProposition.Excluded)
                            {
                                startIndex++;
                            }
                            break;
                        case FindResult.Previous:
                            startIndex = _keyValueTr.GetKeyIndex() + 1;
                            break;
                        case FindResult.Next:
                            startIndex = _keyValueTr.GetKeyIndex();
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
                _seekNeeded = true;
            }

            public uint Count => _count;

            public TValue CurrentValue
            {
                get
                {
                    if (_pos >= _count) throw new IndexOutOfRangeException();
                    _keyValueTrProtector.Start();
                    if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
                    {
                        if (_prevModificationCounter != _owner._modificationCounter)
                            ThrowModifiedDuringEnum();
                        _keyValueTr.SetKeyPrefix(_owner._prefix);
                        Seek();
                    }
                    else if (_seekNeeded)
                    {
                        Seek();
                        _seekNeeded = false;
                    }
                    _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                    var valueBytes = _keyValueTr.GetValueAsByteArray();
                    return _owner.ByteArrayToValue(valueBytes);
                }

                set
                {
                    if (_pos >= _count) throw new IndexOutOfRangeException();
                    _keyValueTrProtector.Start();
                    if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
                    {
                        if (_prevModificationCounter != _owner._modificationCounter)
                            ThrowModifiedDuringEnum();
                        _keyValueTr.SetKeyPrefix(_owner._prefix);
                        Seek();
                    }
                    else if (_seekNeeded)
                    {
                        Seek();
                    }
                    _prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                    var valueBytes = _owner.ValueToByteArray(value);
                    _keyValueTr.SetValue(valueBytes);
                }
            }

            void Seek()
            {
                if (_ascending)
                    _keyValueTr.SetKeyIndex(_startPos + _pos);
                else
                    _keyValueTr.SetKeyIndex(_startPos - _pos);
                _seekNeeded = false;
            }

            public uint Position
            {
                get { return _pos; }

                set
                {
                    _pos = value > _count ? _count : value;
                    _seekNeeded = true;
                }
            }

            public bool NextKey(out TKey key)
            {
                if (!_seekNeeded)
                    _pos++;
                if (_pos >= _count)
                {
                    key = default(TKey);
                    return false;
                }
                _keyValueTrProtector.Start();
                if (_keyValueTrProtector.WasInterupted(_prevProtectionCounter))
                {
                    if (_prevModificationCounter != _owner._modificationCounter)
                        ThrowModifiedDuringEnum();
                    _keyValueTr.SetKeyPrefix(_owner._prefix);
                    Seek();
                }
                else if (_seekNeeded)
                {
                    Seek();
                }
                else
                {
                    if (_ascending)
                    {
                        _keyValueTr.FindNextKey();
                    }
                    else
                    {
                        _keyValueTr.FindPreviousKey();
                    }
                }
                key = _owner.ByteArrayToKey(_keyValueTr.GetKeyAsByteArray());
                return true;
            }
        }

        public IOrderedDictionaryEnumerator<TKey, TValue> GetAdvancedEnumerator(AdvancedEnumeratorParam<TKey> param)
        {
            return new AdvancedEnumerator<TKey, TValue>(this, param);
        }
    }
}
