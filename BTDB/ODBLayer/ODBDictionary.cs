using System;
using System.Collections;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class ODBDictionaryConfiguration
    {
        readonly IFieldHandler _keyHandler;
        readonly IFieldHandler _valueHandler;

        public ODBDictionaryConfiguration(IFieldHandler keyHandler, IFieldHandler valueHandler)
        {
            _keyHandler = keyHandler;
            _valueHandler = valueHandler;
        }

        public IFieldHandler KeyHandler
        {
            get { return _keyHandler; }
        }

        public IFieldHandler ValueHandler
        {
            get { return _valueHandler; }
        }

        public object KeyReader { get; set; }
        public object KeyWriter { get; set; }
        public object ValueReader { get; set; }
        public object ValueWriter { get; set; }
    }

    public class ODBDictionary<K, V> : IDictionary<K, V>
    {
        readonly IInternalObjectDBTransaction _tr;
        readonly IFieldHandler _keyHandler;
        readonly IFieldHandler _valueHandler;
        readonly byte[] _prefix;
        readonly Func<AbstractBufferedReader, IReaderCtx, K> _keyReader;
        readonly Action<K, AbstractBufferedWriter, IWriterCtx> _keyWriter;
        readonly Func<AbstractBufferedReader, IReaderCtx, V> _valueReader;
        readonly Action<V, AbstractBufferedWriter, IWriterCtx> _valueWriter;
        readonly IKeyValueDBTransaction _keyValueTr;
        readonly KeyValueDBTransactionProtector _keyValueTrProtector;
        int _count;

        public ODBDictionary(IInternalObjectDBTransaction tr, ODBDictionaryConfiguration config, ulong id)
        {
            _tr = tr;
            _keyHandler = config.KeyHandler;
            _valueHandler = config.ValueHandler;
            int o = ObjectDB.AllDictionariesPrefix.Length;
            _prefix = new byte[o + PackUnpack.LengthVUInt(id)];
            Array.Copy(ObjectDB.AllDictionariesPrefix, _prefix, o);
            PackUnpack.PackVUInt(_prefix, ref o, id);
            _keyReader = (Func<AbstractBufferedReader, IReaderCtx, K>)config.KeyReader;
            _keyWriter = (Action<K, AbstractBufferedWriter, IWriterCtx>)config.KeyWriter;
            _valueReader = (Func<AbstractBufferedReader, IReaderCtx, V>)config.ValueReader;
            _valueWriter = (Action<V, AbstractBufferedWriter, IWriterCtx>)config.ValueWriter;
            _keyValueTr = _tr.KeyValueDBTransaction;
            _keyValueTrProtector = _tr.TransactionProtector;
            _count = -1;
        }

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            bool taken = false;
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
                    var key = ByteArrayToKey(_keyValueTr.ReadKey());
                    var value = ByteArrayToValue(_keyValueTr.ReadValue());
                    _keyValueTrProtector.Stop(ref taken);
                    yield return new KeyValuePair<K, V>(key, value);
                    pos++;
                }
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(KeyValuePair<K, V> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
            _count = 0;
        }

        public bool Contains(KeyValuePair<K, V> item)
        {
            V value;
            if (!TryGetValue(item.Key, out value)) return false;
            return EqualityComparer<V>.Default.Equals(value, item.Value);
        }

        public void CopyTo(KeyValuePair<K, V>[] array, int arrayIndex)
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

        public bool Remove(KeyValuePair<K, V> item)
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

        byte[] KeyToByteArray(K key)
        {
            var writer = new ByteArrayWriter();
            IWriterCtx ctx = null;
            if (_keyHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr, writer);
            _keyWriter(key, writer, ctx);
            return writer.Data;
        }

        byte[] ValueToByteArray(V value)
        {
            var writer = new ByteArrayWriter();
            IWriterCtx ctx = null;
            if (_valueHandler.NeedsCtx()) ctx = new DBWriterCtx(_tr, writer);
            _valueWriter(value, writer, ctx);
            return writer.Data;
        }

        K ByteArrayToKey(byte[] data)
        {
            var reader = new ByteArrayReader(data);
            IReaderCtx ctx = null;
            if (_keyHandler.NeedsCtx()) ctx = new DBReaderCtx(_tr, reader);
            return _keyReader(reader, ctx);
        }

        V ByteArrayToValue(byte[] data)
        {
            var reader = new ByteArrayReader(data);
            IReaderCtx ctx = null;
            if (_valueHandler.NeedsCtx()) ctx = new DBReaderCtx(_tr, reader);
            return _valueReader(reader, ctx);
        }

        public bool ContainsKey(K key)
        {
            bool taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                return _keyValueTr.FindExactKey(KeyToByteArray(key));
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public void Add(K key, V value)
        {
            bool taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                bool created = _keyValueTr.CreateKey(KeyToByteArray(key));
                if (!created)
                {
                    throw new ArgumentException("Cannot Add duplicate key to Dictionary");
                }
                NotifyAdded();
                _keyValueTr.SetValue(ValueToByteArray(value));
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public bool Remove(K key)
        {
            bool taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                bool found = _keyValueTr.FindExactKey(KeyToByteArray(key));
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

        public bool TryGetValue(K key, out V value)
        {
            bool taken = false;
            try
            {
                _keyValueTrProtector.Start(ref taken);
                _keyValueTr.SetKeyPrefix(_prefix);
                bool found = _keyValueTr.FindExactKey(KeyToByteArray(key));
                if (!found)
                {
                    value = default(V);
                    return false;
                }
                value = ByteArrayToValue(_keyValueTr.ReadValue());
                return true;
            }
            finally
            {
                _keyValueTrProtector.Stop(ref taken);
            }
        }

        public V this[K key]
        {
            get
            {
                bool taken = false;
                try
                {
                    _keyValueTrProtector.Start(ref taken);
                    _keyValueTr.SetKeyPrefix(_prefix);
                    bool found = _keyValueTr.FindExactKey(KeyToByteArray(key));
                    if (!found)
                    {
                        throw new ArgumentException("Key not found in Dictionary");
                    }
                    return ByteArrayToValue(_keyValueTr.ReadValue());
                }
                finally
                {
                    _keyValueTrProtector.Stop(ref taken);
                }
            }
            set
            {
                bool taken = false;
                try
                {
                    _keyValueTrProtector.Start(ref taken);
                    _keyValueTr.SetKeyPrefix(_prefix);
                    if (_keyValueTr.CreateOrUpdateKeyValue(KeyToByteArray(key), ValueToByteArray(value)))
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

        public ICollection<K> Keys
        {
            get { throw new NotImplementedException(); }
        }

        public ICollection<V> Values
        {
            get { throw new NotImplementedException(); }
        }
    }
}
