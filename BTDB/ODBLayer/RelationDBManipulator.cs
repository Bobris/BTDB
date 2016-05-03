using System;
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
            var valueBytes = valueWriter.Data; // Data from ByteBufferWriter are always fresh and not reused = AsyncSafe
            return valueBytes;
        }

        ByteBuffer KeyBytes(T obj)
        {
            var keyWriter = new ByteBufferWriter();
            keyWriter.WriteVUInt32(_relationInfo.Id);
            _relationInfo.PrimaryKeysSaver(_transaction, keyWriter, obj, this);  //this for relation interface which is same with manipulator
            var keyBytes = keyWriter.Data;
            return keyBytes;
        }

        void StartWorkingWithPK()
        {
            _transaction.TransactionProtector.Start();
            _transaction.KeyValueDBTransaction.SetKeyPrefix(ObjectDB.AllRelationsPKPrefix);
        }

        public void Insert(T obj)
        {
            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            StartWorkingWithPK();

            if (_transaction.KeyValueDBTransaction.Find(keyBytes) == FindResult.Exact)
                throw new BTDBException("Trying to insert duplicate key.");  //todo write key in message
            _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }

        public bool Upsert(T obj)
        {
            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            StartWorkingWithPK();

            return _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }

        public void Update(T obj)
        {
            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            StartWorkingWithPK();

            if (_transaction.KeyValueDBTransaction.Find(keyBytes) != FindResult.Exact)
                throw new BTDBException("Not found record to update."); //todo write key in message
            _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }

        public IEnumerator<T> GetEnumerator()
        {
            _transaction.KeyValueDBTransaction.SetKeyPrefix(ObjectDB.AllRelationsPKPrefix);
            return new RelationEnumerator<T>(_transaction, _relationInfo);
        }

        public bool RemoveById(ByteBuffer keyBytes, bool throwWhenNotFound)
        {
            StartWorkingWithPK();
            if (_transaction.KeyValueDBTransaction.Find(keyBytes) != FindResult.Exact)
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }
            if (_relationInfo.NeedsFreeContent)
            {
                long current = _transaction.TransactionProtector.ProtectionCounter;
                var valueBytes = _transaction.KeyValueDBTransaction.GetValue();
                _relationInfo.FreeContent(_transaction, valueBytes);
                if (_transaction.TransactionProtector.WasInterupted(current))
                {
                    StartWorkingWithPK();
                    _transaction.KeyValueDBTransaction.Find(keyBytes);
                }
            }
            _transaction.KeyValueDBTransaction.EraseCurrent();
            return true;
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
    }
}