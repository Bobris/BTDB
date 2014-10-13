using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class ODBDictionary<TKey, TValue> : IOrderedDictionary<TKey, TValue>, IQuerySizeDictionary<TKey>
    {
        readonly IInternalObjectDBTransaction _tr;
        readonly IFieldHandler _keyHandler;
        readonly IFieldHandler _valueHandler;
        readonly Func<AbstractBufferedReader, IReaderCtx, TKey> _keyReader;
        readonly Action<TKey, AbstractBufferedWriter, IWriterCtx> _keyWriter;
        readonly Func<AbstractBufferedReader, IReaderCtx, TValue> _valueReader;
        readonly Action<TValue, AbstractBufferedWriter, IWriterCtx> _valueWriter;
        readonly IKeyValueDBTransaction _keyValueTr;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        readonly ulong _id;
        byte[] _prefix;
        int _count;
        KeysCollection _keysCollection;
        ValuesCollection _valuesCollection;

        public ODBDictionary(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config, ulong id)
        {
            _tr = tr;
            _keyHandler = config.KeyHandler;
            _valueHandler = config.ValueHandler;
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

        public ODBDictionary(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config)
        {
            _tr = tr;
            _keyHandler = config.KeyHandler;
            _valueHandler = config.ValueHandler;
            _id = tr.AllocateDictionaryId();
            GeneratePrefix();
            _keyReader = (Func<AbstractBufferedReader, IReaderCtx, TKey>)config.KeyReader;
            _keyWriter = (Action<TKey, AbstractBufferedWriter, IWriterCtx>)config.KeyWriter;
            _valueReader = (Func<AbstractBufferedReader, IReaderCtx, TValue>)config.ValueReader;
            _valueWriter = (Action<TValue, AbstractBufferedWriter, IWriterCtx>)config.ValueWriter;
            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _count = -1;
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

        void GeneratePrefix()
        {
            int o = ObjectDB.AllDictionariesPrefix.Length;
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
            var taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                _keyValueTr.EraseAll();
                _count = 0;
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            TValue value;
            if (!TryGetValue(item.Key, out value)) return false;
            return EqualityComparer<TValue>.Default.Equals(value, item.Value);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            if (array == null) throw new ArgumentNullException("array");
            if ((arrayIndex < 0) || (arrayIndex > array.Length))
            {
                throw new ArgumentOutOfRangeException("arrayIndex", arrayIndex, "Needs to be nonnegative ");
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
                    bool taken = false;
                    try
                    {
                        _keyValueTrProtector.Start(ref taken);
                        _keyValueTr.SetKeyPrefix(_prefix);
                        _count = (int)Math.Min(_keyValueTr.GetKeyValueCount(), int.MaxValue);
                    }
                    finally
                    {
                        _keyValueTrProtector.Stop(ref taken);
                    }
                }
                return _count;
            }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        byte[] KeyToByteArray(TKey key)
        {
            var writer = new ByteBufferWriter();
            IWriterCtx ctx = null;
            if (_keyHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr, writer);
            _keyWriter(key, writer, ctx);
            return writer.Data.ToByteArray();
        }

        byte[] ValueToByteArray(TValue value)
        {
            var writer = new ByteBufferWriter();
            IWriterCtx ctx = null;
            if (_valueHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr, writer);
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
            bool taken = false;
            var keyBytes = KeyToByteArray(key);
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                return _keyValueTr.FindExactKey(keyBytes);
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public void Add(TKey key, TValue value)
        {
            bool taken = false;
            var keyBytes = KeyToByteArray(key);
            var valueBytes = ValueToByteArray(value);
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                if (_keyValueTr.FindExactKey(keyBytes))
                {
                    throw new ArgumentException("Cannot Add duplicate key to Dictionary");
                }
                _keyValueTr.CreateOrUpdateKeyValueUnsafe(keyBytes, valueBytes);
                NotifyAdded();
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public bool Remove(TKey key)
        {
            bool taken = false;
            var keyBytes = KeyToByteArray(key);
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                bool found = _keyValueTr.FindExactKey(keyBytes);
                if (found)
                {
                    _keyValueTr.EraseCurrent();
                    NotifyRemoved();
                }
                return found;
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            bool taken = false;
            var keyBytes = KeyToByteArray(key);
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                bool found = _keyValueTr.FindExactKey(keyBytes);
                if (!found)
                {
                    value = default(TValue);
                    return false;
                }
                var valueBytes = _keyValueTr.GetValueAsByteArray();
                _keyValueTrProtector.Stop(ref taken);
                value = ByteArrayToValue(valueBytes);
                return true;
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public TValue this[TKey key]
        {
            get
            {
                bool taken = false;
                var keyBytes = KeyToByteArray(key);
                try
                {
                    _keyValueTrProtector.Start(ref taken);
                    _keyValueTr.SetKeyPrefix(_prefix);
                    bool found = _keyValueTr.FindExactKey(keyBytes);
                    if (!found)
                    {
                        throw new ArgumentException("Key not found in Dictionary");
                    }
                    var valueBytes = _keyValueTr.GetValueAsByteArray();
                    _keyValueTrProtector.Stop(ref taken);
                    return ByteArrayToValue(valueBytes);
                }
                finally
                {
                    _keyValueTrProtector.Stop(ref taken);
                }
            }
            set
            {
                bool taken = false;
                var keyBytes = KeyToByteArray(key);
                var valueBytes = ValueToByteArray(value);
                try
                {
                    _keyValueTrProtector.Start(ref taken);
                    _keyValueTr.SetKeyPrefix(_prefix);
                    if (_keyValueTr.CreateOrUpdateKeyValue(keyBytes, valueBytes))
                    {
                        NotifyAdded();
                    }
                }
                finally
                {
                    _keyValueTrProtector.Stop(ref taken);
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
                bool taken = false;
                try
                {
                    long prevProtectionCounter = 0;
                    long pos = 0;
                    while (true)
                    {
                        if (!taken) _parent._keyValueTrProtector.Start(ref taken);
                        if (pos == 0)
                        {
                            prevProtectionCounter = _parent._keyValueTrProtector.ProtectionCounter;
                            _parent._keyValueTr.SetKeyPrefix(_parent._prefix);
                            if (!_parent._keyValueTr.FindFirstKey()) break;
                        }
                        else
                        {
                            if (_parent._keyValueTrProtector.WasInterupted(prevProtectionCounter))
                            {
                                _parent._keyValueTr.SetKeyPrefix(_parent._prefix);
                                if (!_parent._keyValueTr.SetKeyIndex(pos)) break;
                            }
                            else
                            {
                                if (!_parent._keyValueTr.FindNextKey()) break;
                            }
                        }
                        var keyBytes = _parent._keyValueTr.GetKeyAsByteArray();
                        _parent._keyValueTrProtector.Stop(ref taken);
                        var key = _parent.ByteArrayToKey(keyBytes);
                        yield return key;
                        pos++;
                    }
                }
                finally
                {
                    _parent._keyValueTrProtector.Stop(ref taken);
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Add(TKey item)
            {
                _parent.Add(item, default(TValue));
            }

            public void Clear()
            {
                _parent.Clear();
            }

            public bool Contains(TKey item)
            {
                return _parent.ContainsKey(item);
            }

            public void CopyTo(TKey[] array, int arrayIndex)
            {
                if (array == null) throw new ArgumentNullException("array");
                if ((arrayIndex < 0) || (arrayIndex > array.Length))
                {
                    throw new ArgumentOutOfRangeException("arrayIndex", arrayIndex, "Needs to be nonnegative ");
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

            public bool Remove(TKey item)
            {
                return _parent.Remove(item);
            }

            public int Count
            {
                get { return _parent.Count; }
            }

            public bool IsReadOnly
            {
                get { return false; }
            }
        }

        public ICollection<TKey> Keys
        {
            get { return _keysCollection ?? (_keysCollection = new KeysCollection(this)); }
        }

        class ValuesCollection : ICollection<TValue>
        {
            readonly ODBDictionary<TKey, TValue> _parent;

            public ValuesCollection(ODBDictionary<TKey, TValue> parent)
            {
                _parent = parent;
            }

            public IEnumerator<TValue> GetEnumerator()
            {
                bool taken = false;
                try
                {
                    long prevProtectionCounter = 0;
                    long pos = 0;
                    while (true)
                    {
                        if (!taken) _parent._keyValueTrProtector.Start(ref taken);
                        if (pos == 0)
                        {
                            prevProtectionCounter = _parent._keyValueTrProtector.ProtectionCounter;
                            _parent._keyValueTr.SetKeyPrefix(_parent._prefix);
                            if (!_parent._keyValueTr.FindFirstKey()) break;
                        }
                        else
                        {
                            if (_parent._keyValueTrProtector.WasInterupted(prevProtectionCounter))
                            {
                                _parent._keyValueTr.SetKeyPrefix(_parent._prefix);
                                if (!_parent._keyValueTr.SetKeyIndex(pos)) break;
                            }
                            else
                            {
                                if (!_parent._keyValueTr.FindNextKey()) break;
                            }
                        }
                        var valueBytes = _parent._keyValueTr.GetValueAsByteArray();
                        _parent._keyValueTrProtector.Stop(ref taken);
                        var value = _parent.ByteArrayToValue(valueBytes);
                        yield return value;
                        pos++;
                    }
                }
                finally
                {
                    _parent._keyValueTrProtector.Stop(ref taken);
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

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
                if (array == null) throw new ArgumentNullException("array");
                if ((arrayIndex < 0) || (arrayIndex > array.Length))
                {
                    throw new ArgumentOutOfRangeException("arrayIndex", arrayIndex, "Needs to be nonnegative ");
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

            public int Count
            {
                get { return _parent.Count; }
            }

            public bool IsReadOnly
            {
                get { return true; }
            }
        }

        public ICollection<TValue> Values
        {
            get { return _valuesCollection ?? (_valuesCollection = new ValuesCollection(this)); }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            var taken = false;
            try
            {
                long prevProtectionCounter = 0;
                long pos = 0;
                while (true)
                {
                    if (!taken) _keyValueTrProtector.Start(ref taken);
                    if (pos == 0)
                    {
                        prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                        _keyValueTr.SetKeyPrefix(_prefix);
                        if (!_keyValueTr.FindFirstKey()) break;
                    }
                    else
                    {
                        if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                        {
                            _keyValueTr.SetKeyPrefix(_prefix);
                            if (!_keyValueTr.SetKeyIndex(pos)) break;
                        }
                        else
                        {
                            if (!_keyValueTr.FindNextKey()) break;
                        }
                    }
                    var keyBytes = _keyValueTr.GetKeyAsByteArray();
                    var valueBytes = _keyValueTr.GetValueAsByteArray();
                    _keyValueTrProtector.Stop(ref taken);
                    var key = ByteArrayToKey(keyBytes);
                    var value = ByteArrayToValue(valueBytes);
                    yield return new KeyValuePair<TKey, TValue>(key, value);
                    pos++;
                }
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetReverseEnumerator()
        {
            var taken = false;
            try
            {
                long prevProtectionCounter = 0;
                long pos = long.MaxValue;
                while (true)
                {
                    if (!taken) _keyValueTrProtector.Start(ref taken);
                    if (pos == long.MaxValue)
                    {
                        prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                        _keyValueTr.SetKeyPrefix(_prefix);
                        if (!_keyValueTr.FindLastKey()) break;
                        pos = _keyValueTr.GetKeyIndex();
                    }
                    else
                    {
                        if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                        {
                            _keyValueTr.SetKeyPrefix(_prefix);
                            if (!_keyValueTr.SetKeyIndex(pos)) break;
                        }
                        else
                        {
                            if (!_keyValueTr.FindPreviousKey()) break;
                        }
                    }
                    var keyBytes = _keyValueTr.GetKeyAsByteArray();
                    var valueBytes = _keyValueTr.GetValueAsByteArray();
                    _keyValueTrProtector.Stop(ref taken);
                    var key = ByteArrayToKey(keyBytes);
                    var value = ByteArrayToValue(valueBytes);
                    yield return new KeyValuePair<TKey, TValue>(key, value);
                    pos--;
                }
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetIncreasingEnumerator(TKey start)
        {
            var startKeyBytes = KeyToByteArray(start);
            var taken = false;
            try
            {
                long prevProtectionCounter = 0;
                long pos = 0;
                while (true)
                {
                    if (!taken) _keyValueTrProtector.Start(ref taken);
                    if (pos == 0)
                    {
                        prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
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
                            _keyValueTr.SetKeyPrefix(_prefix);
                            if (!_keyValueTr.SetKeyIndex(pos)) break;
                        }
                        else
                        {
                            if (!_keyValueTr.FindNextKey()) break;
                        }
                    }
                    var keyBytes = _keyValueTr.GetKeyAsByteArray();
                    var valueBytes = _keyValueTr.GetValueAsByteArray();
                    _keyValueTrProtector.Stop(ref taken);
                    var key = ByteArrayToKey(keyBytes);
                    var value = ByteArrayToValue(valueBytes);
                    yield return new KeyValuePair<TKey, TValue>(key, value);
                    pos++;
                }
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> GetDecreasingEnumerator(TKey start)
        {
            var startKeyBytes = KeyToByteArray(start);
            var taken = false;
            try
            {
                long prevProtectionCounter = 0;
                long pos = long.MaxValue;
                while (true)
                {
                    if (!taken) _keyValueTrProtector.Start(ref taken);
                    if (pos == long.MaxValue)
                    {
                        prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
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
                            _keyValueTr.SetKeyPrefix(_prefix);
                            if (!_keyValueTr.SetKeyIndex(pos)) break;
                        }
                        else
                        {
                            if (!_keyValueTr.FindPreviousKey()) break;
                        }
                    }
                    var keyBytes = _keyValueTr.GetKeyAsByteArray();
                    var valueBytes = _keyValueTr.GetValueAsByteArray();
                    _keyValueTrProtector.Stop(ref taken);
                    var key = ByteArrayToKey(keyBytes);
                    var value = ByteArrayToValue(valueBytes);
                    yield return new KeyValuePair<TKey, TValue>(key, value);
                    pos--;
                }
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public long RemoveRange(TKey start, bool includeStart, TKey end, bool includeEnd)
        {
            var taken = false;
            var startKeyBytes = KeyToByteArray(start);
            var endKeyBytes = KeyToByteArray(end);
            try
            {
                _keyValueTrProtector.Start(ref taken);
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
                _keyValueTrProtector.Stop(ref taken);
                return Math.Max(0, endIndex - startIndex + 1);
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public IEnumerable<KeyValuePair<uint, uint>> QuerySizeEnumerator()
        {
            var taken = false;
            try
            {
                long prevProtectionCounter = 0;
                long pos = 0;
                while (true)
                {
                    if (!taken) _keyValueTrProtector.Start(ref taken);
                    if (pos == 0)
                    {
                        prevProtectionCounter = _keyValueTrProtector.ProtectionCounter;
                        _keyValueTr.SetKeyPrefix(_prefix);
                        if (!_keyValueTr.FindFirstKey()) break;
                    }
                    else
                    {
                        if (_keyValueTrProtector.WasInterupted(prevProtectionCounter))
                        {
                            _keyValueTr.SetKeyPrefix(_prefix);
                            if (!_keyValueTr.SetKeyIndex(pos)) break;
                        }
                        else
                        {
                            if (!_keyValueTr.FindNextKey()) break;
                        }
                    }
                    var size = _keyValueTr.GetStorageSizeOfCurrentKey();
                    _keyValueTrProtector.Stop(ref taken);
                    yield return size;
                    pos++;
                }
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public KeyValuePair<uint, uint> QuerySizeByKey(TKey key)
        {
            bool taken = false;
            var keyBytes = KeyToByteArray(key);
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                bool found = _keyValueTr.FindExactKey(keyBytes);
                if (!found)
                {
                    throw new ArgumentException("Key not found in Dictionary");
                }
                var size = _keyValueTr.GetStorageSizeOfCurrentKey();
                _keyValueTrProtector.Stop(ref taken);
                return size;
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }
    }
}
