using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class RelationDBManipulator<T>
    {
        readonly IInternalObjectDBTransaction _transaction;
        readonly RelationInfo _relationInfo;

        public IInternalObjectDBTransaction Transaction => _transaction;
        public RelationInfo RelationInfo => _relationInfo;

        public RelationDBManipulator(IObjectDBTransaction transation, RelationInfo relationInfo)
        {
            _transaction = (IInternalObjectDBTransaction)transation;
            _relationInfo = relationInfo;
        }

        ByteBuffer ValueBytes(T obj)
        {
            var valueWriter = new ByteBufferWriter();
            valueWriter.WriteVUInt32(_relationInfo.ClientTypeVersion);
            _relationInfo.ValueSaver(_transaction, valueWriter, obj);
            return valueWriter.Data;
        }

        ByteBuffer KeyBytes(T obj)
        {
            var keyWriter = new ByteBufferWriter();
            keyWriter.WriteVUInt32(_relationInfo.Id);
            _relationInfo.PrimaryKeysSaver(_transaction, keyWriter, obj, this);  //this for relation interface which is same with manipulator
            return keyWriter.Data;
        }

        void StartWorkingWithPK()
        {
            _transaction.TransactionProtector.Start();
            _transaction.KeyValueDBTransaction.SetKeyPrefix(ObjectDB.AllRelationsPKPrefix);
        }

        bool HasSecondaryIndexes => _relationInfo.ClientRelationVersionInfo.HasSecondaryIndexes;

        public void Insert(T obj)
        {
            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            StartWorkingWithPK();

            if (_transaction.KeyValueDBTransaction.Find(keyBytes) == FindResult.Exact)
                throw new BTDBException("Trying to insert duplicate key.");
            _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (HasSecondaryIndexes)
            {
                var error = AddIntoSecondaryIndexes(obj);
                if (error != null)
                {
                    StartWorkingWithPK();
                    _transaction.KeyValueDBTransaction.Find(keyBytes);
                    _transaction.KeyValueDBTransaction.EraseCurrent();
                    throw new BTDBException(error);
                }
            }
        }

        public bool Upsert(T obj)
        {
            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            if (HasSecondaryIndexes)
            {
                var oldValue = FindByIdOrDefault(keyBytes, false);
                var error = oldValue != null
                    ? UpdateSecondaryIndexes(obj, oldValue)
                    : AddIntoSecondaryIndexes(obj);
                if (error != null)
                    throw new BTDBException(error);
            }
            StartWorkingWithPK();
            if (_transaction.KeyValueDBTransaction.Find(keyBytes) == FindResult.Exact)
            {
                long current = _transaction.TransactionProtector.ProtectionCounter;

                FreeContentInUpdate(_transaction.KeyValueDBTransaction.GetValue(), valueBytes);

                if (_transaction.TransactionProtector.WasInterupted(current))
                {
                    StartWorkingWithPK();
                    _transaction.KeyValueDBTransaction.Find(keyBytes);
                }
            }
            return _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }

        public void Update(T obj)
        {
            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            if (HasSecondaryIndexes)
            {
                var oldValue = FindByIdOrDefault(keyBytes, true);
                var error = UpdateSecondaryIndexes(obj, oldValue);
                if (error != null)
                    throw new BTDBException(error);
            }

            StartWorkingWithPK();

            if (_transaction.KeyValueDBTransaction.Find(keyBytes) != FindResult.Exact)
                throw new BTDBException("Not found record to update.");

            long current = _transaction.TransactionProtector.ProtectionCounter;

            FreeContentInUpdate(_transaction.KeyValueDBTransaction.GetValue(), valueBytes);

            if (_transaction.TransactionProtector.WasInterupted(current))
            {
                StartWorkingWithPK();
                _transaction.KeyValueDBTransaction.Find(keyBytes);
            }
            _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }

        void FreeContentInUpdate(ByteBuffer oldValueBytes, ByteBuffer newValueBytes)
        {
            var oldDicts = _relationInfo.FindUsedIDictionaries(_transaction, oldValueBytes);
            if (oldDicts.Count == 0)
                return;
            var newDicts = _relationInfo.FindUsedIDictionaries(_transaction, newValueBytes);
            if (newDicts.Count == 0)
            {
                foreach(var dictId in oldDicts)
                    RelationInfo.FreeIDictionary(_transaction, dictId);
            }
            else if (newDicts.Count < 10)
            {
                foreach (var dictId in oldDicts)
                {
                    if (newDicts.Contains(dictId))
                        continue;
                    RelationInfo.FreeIDictionary(_transaction, dictId);
                }
            }
            else
            {
                var newDictsDictionary = new Dictionary<ulong, object>();
                foreach(var d in newDicts)
                    newDictsDictionary[d] = null;
                foreach (var dictId in oldDicts)
                {
                    if (newDictsDictionary.ContainsKey(dictId))
                        continue;
                    RelationInfo.FreeIDictionary(_transaction, dictId);
                }
            }
        }

        public bool RemoveById(ByteBuffer keyBytes, bool throwWhenNotFound)
        {
            if (HasSecondaryIndexes)
            {
                var obj = FindByIdOrDefault(keyBytes, throwWhenNotFound);
                if (obj != null)
                    RemoveSecondaryIndexes(obj);
            }

            StartWorkingWithPK();
            if (_transaction.KeyValueDBTransaction.Find(keyBytes) != FindResult.Exact)
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            long current = _transaction.TransactionProtector.ProtectionCounter;
            var valueBytes = _transaction.KeyValueDBTransaction.GetValue();
            _relationInfo.FreeContent(_transaction, valueBytes);
            if (_transaction.TransactionProtector.WasInterupted(current))
            {
                StartWorkingWithPK();
                _transaction.KeyValueDBTransaction.Find(keyBytes);
            }

            _transaction.KeyValueDBTransaction.EraseCurrent();
            return true;
        }

        public IEnumerator<T> GetEnumerator()
        {
            var keyWriter = new ByteBufferWriter();
            keyWriter.WriteByteArrayRaw(ObjectDB.AllRelationsPKPrefix);
            keyWriter.WriteVUInt32(_relationInfo.Id);

            return new RelationEnumerator<T>(_transaction, _relationInfo, keyWriter.Data.ToAsyncSafe());
        }

        public T FindByIdOrDefault(ByteBuffer keyBytes, bool throwWhenNotFound)
        {
            StartWorkingWithPK();
            if (_transaction.KeyValueDBTransaction.Find(keyBytes) != FindResult.Exact)
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found.");
                return default(T);
            }
            var valueBytes = _transaction.KeyValueDBTransaction.GetValue();
            return (T)_relationInfo.CreateInstance(_transaction, keyBytes, valueBytes);
        }

        internal T CreateInstanceFromSK(uint secondaryKeyIndex, uint fieldInFirstBufferCount, ByteBuffer firstPart, ByteBuffer secondPart)
        {
            var pkWriter = new ByteBufferWriter();
            pkWriter.WriteVUInt32(_relationInfo.Id);
            _relationInfo.GetSKKeyValuetoPKMerger(secondaryKeyIndex, fieldInFirstBufferCount)
                                                 (firstPart.ToByteArray(), secondPart.ToByteArray(), pkWriter);
            return FindByIdOrDefault(pkWriter.Data, true);
        }

        public IEnumerator<T> FindBySecondaryKey(uint secondaryKeyIndex, uint prefixFieldCount, ByteBuffer secKeyBytes)
        {
            var keyWriter = new ByteBufferWriter();
            keyWriter.WriteBlock(secKeyBytes);

            return new RelationSecondaryKeyEnumerator<T>(_transaction, _relationInfo, keyWriter.Data.ToAsyncSafe(),
                secondaryKeyIndex, prefixFieldCount, this);
        }

        //secKeyBytes contains already AllRelationsSKPrefix
        public T FindBySecondaryKeyOrDefault(uint secondaryKeyIndex, uint parametersCount, ByteBuffer secKeyBytes,
                                             bool throwWhenNotFound)
        {
            _transaction.KeyValueDBTransaction.SetKeyPrefix(secKeyBytes);
            if (!_transaction.KeyValueDBTransaction.FindFirstKey())
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found.");
                return default(T);
            }
            var keyBytes = _transaction.KeyValueDBTransaction.GetKey();

            if (_transaction.KeyValueDBTransaction.FindNextKey())
                throw new BTDBException("Ambiguous result.");

            return CreateInstanceFromSK(secondaryKeyIndex, parametersCount, secKeyBytes, keyBytes);
        }

        //SK manipulations
        void StartWorkingWithSK()
        {
            _transaction.TransactionProtector.Start();
            _transaction.KeyValueDBTransaction.SetKeyPrefix(ObjectDB.AllRelationsSKPrefix);
        }

        ByteBuffer WriteSecodaryKeyKey(uint secondaryKeyIndex, string name, T obj)
        {
            var keyWriter = new ByteBufferWriter();
            var keySaver = _relationInfo.GetSecondaryKeysKeySaver(secondaryKeyIndex, name);
            keyWriter.WriteVUInt32(_relationInfo.Id);
            keyWriter.WriteVUInt32(secondaryKeyIndex); //secondary key index
            keySaver(_transaction, keyWriter, obj, this); //secondary key
            return keyWriter.Data;
        }

        string AddIntoSecondaryIndexes(T obj)
        {
            StartWorkingWithSK();

            foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecodaryKeyKey(sk.Key, sk.Value.Name, obj);
                _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, ByteBuffer.NewEmpty());
            }
            return null;
        }

        static bool ByteBuffersHasSameContent(ByteBuffer a, ByteBuffer b)
        {
            if (a.Length != b.Length)
                return false;
            for (int i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i])
                    return false;
            }
            return true;
        }

        string UpdateSecondaryIndexes(T newValue, T oldValue)
        {
            StartWorkingWithSK();

            var secKeys = _relationInfo.ClientRelationVersionInfo.SecondaryKeys;
            foreach (var sk in secKeys)
            {
                var newKeyBytes = WriteSecodaryKeyKey(sk.Key, sk.Value.Name, newValue);
                var oldKeyBytes = WriteSecodaryKeyKey(sk.Key, sk.Value.Name, oldValue);
                if (ByteBuffersHasSameContent(oldKeyBytes, newKeyBytes))
                    continue;
                //remove old index
                if (_transaction.KeyValueDBTransaction.Find(oldKeyBytes) != FindResult.Exact)
                    throw new BTDBException("Error in updating secondary indexes, previous index entry not found.");
                _transaction.KeyValueDBTransaction.EraseCurrent();
                //insert new value
                _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(newKeyBytes, ByteBuffer.NewEmpty());
            }
            return null;
        }

        void RemoveSecondaryIndexes(T obj)
        {
            StartWorkingWithSK();

            foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecodaryKeyKey(sk.Key, sk.Value.Name, obj);
                if (_transaction.KeyValueDBTransaction.Find(keyBytes) == FindResult.Exact)
                    _transaction.KeyValueDBTransaction.EraseCurrent();
            }
        }
    }
}