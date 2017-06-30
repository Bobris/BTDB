using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.Buffer;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public interface IRelationModificationCounter
    {
        int ModificationCounter { get; }
        void CheckModifiedDuringEnum(int prevModification);
    }

    public class RelationDBManipulator<T> : IRelationModificationCounter, IReadOnlyCollection<T>
    {
        readonly IInternalObjectDBTransaction _transaction;
        readonly RelationInfo _relationInfo;

        public IInternalObjectDBTransaction Transaction => _transaction;
        public RelationInfo RelationInfo => _relationInfo;

        int _modificationCounter;
        const string AssertNotDerivedTypesMsg = "Derived types are not supported.";

        static ByteBuffer EmptyBuffer = ByteBuffer.NewEmpty();


        public RelationDBManipulator(IObjectDBTransaction transation, RelationInfo relationInfo)
        {
            _transaction = (IInternalObjectDBTransaction)transation;
            _relationInfo = relationInfo;
        }

        public int ModificationCounter => _modificationCounter;

        public void MarkModification()
        {
            _modificationCounter++;
        }

        public void CheckModifiedDuringEnum(int prevModification)
        {
            if (prevModification != _modificationCounter)
                throw new InvalidOperationException("Relation modified during iteration.");
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

        public bool Insert(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            StartWorkingWithPK();

            if (_transaction.KeyValueDBTransaction.Find(keyBytes) == FindResult.Exact)
                return false;
            _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (HasSecondaryIndexes)
                AddIntoSecondaryIndexes(obj);

            MarkModification();
            return true;
        }

        public bool Upsert(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            StartWorkingWithPK();
            if (_transaction.KeyValueDBTransaction.Find(keyBytes) == FindResult.Exact)
            {
                var oldValueBytes = _transaction.KeyValueDBTransaction.GetValue();

                _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);

                if (HasSecondaryIndexes)
                    UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes);

                FreeContentInUpdate(oldValueBytes, valueBytes);
                MarkModification();
                return false;
            }

            _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);
            if (HasSecondaryIndexes)
                AddIntoSecondaryIndexes(obj);
            MarkModification();
            return true;
        }

        public void Update(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            StartWorkingWithPK();

            if (_transaction.KeyValueDBTransaction.Find(keyBytes) != FindResult.Exact)
                throw new BTDBException("Not found record to update.");

            var oldValueBytes = _transaction.KeyValueDBTransaction.GetValue();
            _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (HasSecondaryIndexes)
                UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes);

            FreeContentInUpdate(oldValueBytes, valueBytes);
        }

        public bool Contains(ByteBuffer keyBytes)
        {
            StartWorkingWithPK();
            return _transaction.KeyValueDBTransaction.Find(keyBytes) == FindResult.Exact;
        }

        void CompareAndRelease(List<ulong> oldItems, List<ulong> newItems, Action<IInternalObjectDBTransaction, ulong> freeAction)
        {
            if (newItems.Count == 0)
            {
                foreach (var id in oldItems)
                    freeAction(_transaction, id);
            }
            else if (newItems.Count < 10)
            {
                foreach (var id in oldItems)
                {
                    if (newItems.Contains(id))
                        continue;
                    freeAction(_transaction, id);
                }
            }
            else
            {
                var newItemsDictionary = new Dictionary<ulong, object>();
                foreach (var id in newItems)
                    newItemsDictionary[id] = null;
                foreach (var id in oldItems)
                {
                    if (newItemsDictionary.ContainsKey(id))
                        continue;
                    freeAction(_transaction, id);
                }
            }
        }

        void FreeContentInUpdate(ByteBuffer oldValueBytes, ByteBuffer newValueBytes)
        {
            var oldDicts = _relationInfo.FreeContentOldDict;
            var oldOids = _relationInfo.FreeContentOldOid;
            oldDicts.Clear();
            oldOids.Clear();
            _relationInfo.FindUsedObjectsToFree(_transaction, oldValueBytes, oldDicts, oldOids);
            if (oldDicts.Count == 0 && oldOids.Count == 0)
                return;
            var newDicts = _relationInfo.FreeContentNewDict;
            var newOids = _relationInfo.FreeContentNewOid;
            newDicts.Clear();
            newOids.Clear();
            _relationInfo.FindUsedObjectsToFree(_transaction, newValueBytes, newDicts, newOids);
            CompareAndRelease(oldDicts, newDicts, RelationInfo.FreeIDictionary);
            CompareAndRelease(oldOids, newOids, RelationInfo.FreeObject);
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
            var valueBytes = _transaction.KeyValueDBTransaction.GetValue();
            _transaction.KeyValueDBTransaction.EraseCurrent();

            if (HasSecondaryIndexes)
                RemoveSecondaryIndexes(keyBytes, valueBytes);

            _relationInfo.FreeContent(_transaction, valueBytes);

            MarkModification();
            return true;
        }

        public int RemoveByPrimaryKeyPrefix(ByteBuffer keyBytesPrefix)
        {
            var removedCount = 0;
            var keysToDelete = new List<ByteBuffer>();

            var enumerator = new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix, this);
            while (enumerator.MoveNext())
            {
                keysToDelete.Add(enumerator.GetKeyBytes());
            }
            foreach (var key in keysToDelete)
            {
                RemoveById(key, true);
                removedCount++;
            }

            return removedCount;
        }


        public IEnumerator<T> GetEnumerator()
        {
            return new RelationEnumerator<T>(_transaction, _relationInfo, ByteBuffer.NewSync(_relationInfo.Prefix), this);
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

        public IEnumerator<T> FindByPrimaryKeyPrefix(ByteBuffer keyBytesPrefix)
        {
            return new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix, this);
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
        public T FindBySecondaryKeyOrDefault(uint secondaryKeyIndex, uint prefixParametersCount, ByteBuffer secKeyBytes,
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

            return CreateInstanceFromSK(secondaryKeyIndex, prefixParametersCount, secKeyBytes, keyBytes);
        }

        //SK manipulations
        void StartWorkingWithSK()
        {
            _transaction.TransactionProtector.Start();
            _transaction.KeyValueDBTransaction.SetKeyPrefix(ObjectDB.AllRelationsSKPrefix);
        }

        ByteBuffer WriteSecondaryKeyKey(uint secondaryKeyIndex, T obj)
        {
            var keyWriter = new ByteBufferWriter();
            var keySaver = _relationInfo.GetSecondaryKeysKeySaver(secondaryKeyIndex);
            keyWriter.WriteVUInt32(_relationInfo.Id);
            keyWriter.WriteVUInt32(secondaryKeyIndex); //secondary key index
            keySaver(_transaction, keyWriter, obj, this); //secondary key
            return keyWriter.Data;
        }

        ByteBuffer WriteSecondaryKeyKey(uint secondaryKeyIndex, ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            var keyWriter = new ByteBufferWriter();
            keyWriter.WriteVUInt32(_relationInfo.Id);
            keyWriter.WriteVUInt32(secondaryKeyIndex);

            var valueReader = new ByteBufferReader(valueBytes);
            var version = valueReader.ReadVUInt32();

            var keySaver = _relationInfo.GetPKValToSKMerger(version, secondaryKeyIndex);
            keySaver(_transaction, keyWriter, keyBytes.ToByteArray(), valueBytes.ToByteArray());
            return keyWriter.Data;
        }

        void AddIntoSecondaryIndexes(T obj)
        {
            StartWorkingWithSK();

            foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecondaryKeyKey(sk.Key, obj);
                _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(keyBytes, EmptyBuffer);
            }
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

        void UpdateSecondaryIndexes(T newValue, ByteBuffer oldKey, ByteBuffer oldValue)
        {
            StartWorkingWithSK();

            var secKeys = _relationInfo.ClientRelationVersionInfo.SecondaryKeys;
            foreach (var sk in secKeys)
            {
                var newKeyBytes = WriteSecondaryKeyKey(sk.Key, newValue);
                var oldKeyBytes = WriteSecondaryKeyKey(sk.Key, oldKey, oldValue);
                if (ByteBuffersHasSameContent(oldKeyBytes, newKeyBytes))
                    continue;
                //remove old index
                if (_transaction.KeyValueDBTransaction.Find(oldKeyBytes) != FindResult.Exact)
                    throw new BTDBException("Error in updating secondary indexes, previous index entry not found.");
                _transaction.KeyValueDBTransaction.EraseCurrent();
                //insert new value
                _transaction.KeyValueDBTransaction.CreateOrUpdateKeyValue(newKeyBytes, EmptyBuffer);
            }
        }

        void RemoveSecondaryIndexes(ByteBuffer oldKey, ByteBuffer oldValue)
        {
            StartWorkingWithSK();

            foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecondaryKeyKey(sk.Key, oldKey, oldValue);
                if (_transaction.KeyValueDBTransaction.Find(keyBytes) != FindResult.Exact)
                    throw new BTDBException("Error in removing secondary indexes, previous index entry not found.");
                _transaction.KeyValueDBTransaction.EraseCurrent();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count
        {
            get
            {
                _transaction.TransactionProtector.Start();
                _transaction.KeyValueDBTransaction.SetKeyPrefix(_relationInfo.Prefix);
                return (int)_transaction.KeyValueDBTransaction.GetKeyValueCount();
            }
        }
    }
}