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
        void MarkModification();
        void CheckModifiedDuringEnum(int prevModification);
    }

    public class UnforgivingRelationModificationCounter : IRelationModificationCounter
    {
        int _modificationCounter;

        public int ModificationCounter => _modificationCounter;

        public void CheckModifiedDuringEnum(int prevModification)
        {
            if (prevModification != _modificationCounter)
                throw new InvalidOperationException("Relation modified during iteration.");
        }

        public void MarkModification()
        {
            _modificationCounter++;
        }
    }

    public class RelationDBManipulator<T> : IReadOnlyCollection<T>
    {
        readonly IInternalObjectDBTransaction _transaction;
        readonly IKeyValueDBTransaction _kvtr;
        readonly RelationInfo _relationInfo;

        public IInternalObjectDBTransaction Transaction => _transaction;
        public RelationInfo RelationInfo => _relationInfo;

        const string AssertNotDerivedTypesMsg = "Derived types are not supported.";

        static readonly ByteBuffer EmptyBuffer = ByteBuffer.NewEmpty();

        readonly IRelationModificationCounter _modificationCounter;

        public RelationDBManipulator(IObjectDBTransaction transaction, RelationInfo relationInfo)
        {
            _transaction = (IInternalObjectDBTransaction)transaction;
            _kvtr = _transaction.KeyValueDBTransaction;
            _relationInfo = relationInfo;
            _modificationCounter = _transaction.GetRelationModificationCounter(relationInfo.Id);
            _hasSecondaryIndexes = _relationInfo.ClientRelationVersionInfo.HasSecondaryIndexes;
        }

        public IRelationModificationCounter ModificationCounter => _modificationCounter;

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
            WritePKPrefix(keyWriter);
            keyWriter.WriteVUInt32(_relationInfo.Id);
            _relationInfo.PrimaryKeysSaver(_transaction, keyWriter, obj, this);  //this for relation interface which is same with manipulator
            return keyWriter.Data;
        }
        readonly bool _hasSecondaryIndexes;

        public bool Insert(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            ResetKeyPrefix();

            if (_kvtr.Find(keyBytes) == FindResult.Exact)
                return false;

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
                AddIntoSecondaryIndexes(obj);

            _modificationCounter.MarkModification();
            return true;
        }

        public bool Upsert(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            ResetKeyPrefix();
            if (_kvtr.Find(keyBytes) == FindResult.Exact)
            {
                var oldValueBytes = _kvtr.GetValue();

                _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

                if (_hasSecondaryIndexes)
                    UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes);

                FreeContentInUpdate(oldValueBytes, valueBytes);
                return false;
            }

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);
            if (_hasSecondaryIndexes)
                AddIntoSecondaryIndexes(obj);
            _modificationCounter.MarkModification();
            return true;
        }

        public bool ShallowUpsert(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            ResetKeyPrefix();

            if (_hasSecondaryIndexes)
            {
                if (_kvtr.Find(keyBytes) == FindResult.Exact)
                {
                    var oldValueBytes = _kvtr.GetValue();

                    _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

                    UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes);

                    return false;
                }

                _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);
                AddIntoSecondaryIndexes(obj);
            }
            else
            {
                if (!_kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes))
                {
                    return false;
                }
            }

            _modificationCounter.MarkModification();
            return true;
        }

        public void Update(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            ResetKeyPrefix();

            if (_kvtr.Find(keyBytes) != FindResult.Exact)
                throw new BTDBException("Not found record to update.");

            var oldValueBytes = _kvtr.GetValue();
            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
                UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes);

            FreeContentInUpdate(oldValueBytes, valueBytes);
        }

        public void ShallowUpdate(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            ResetKeyPrefix();

            if (_kvtr.Find(keyBytes) != FindResult.Exact)
                throw new BTDBException("Not found record to update.");

            var oldValueBytes = _hasSecondaryIndexes
                ? _kvtr.GetValue()
                : EmptyBuffer;

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
                UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes);
        }

        public bool Contains(ByteBuffer keyBytes)
        {
            ResetKeyPrefix();
            return _kvtr.Find(keyBytes) == FindResult.Exact;
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
            oldDicts.Clear();
            _relationInfo.FindUsedObjectsToFree(_transaction, oldValueBytes, oldDicts);
            if (oldDicts.Count == 0)
                return;
            var newDicts = _relationInfo.FreeContentNewDict;
            newDicts.Clear();
            _relationInfo.FindUsedObjectsToFree(_transaction, newValueBytes, newDicts);
            CompareAndRelease(oldDicts, newDicts, RelationInfo.FreeIDictionary);
        }

        public bool RemoveById(ByteBuffer keyBytes, bool throwWhenNotFound)
        {
            ResetKeyPrefix();
            if (_kvtr.Find(keyBytes) != FindResult.Exact)
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }
            var valueBytes = _kvtr.GetValue();
            _kvtr.EraseCurrent();

            if (_hasSecondaryIndexes)
                RemoveSecondaryIndexes(keyBytes, valueBytes);

            _relationInfo.FreeContent(_transaction, valueBytes);

            _modificationCounter.MarkModification();
            return true;
        }

        public bool ShallowRemoveById(ByteBuffer keyBytes, bool throwWhenNotFound)
        {
            ResetKeyPrefix();
            if (_kvtr.Find(keyBytes) != FindResult.Exact)
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            var valueBytes = _hasSecondaryIndexes
                ? _kvtr.GetValue()
                : EmptyBuffer;

            _kvtr.EraseCurrent();

            if (_hasSecondaryIndexes)
                RemoveSecondaryIndexes(keyBytes, valueBytes);

            _modificationCounter.MarkModification();
            return true;
        }

        public int RemoveByPrimaryKeyPrefix(ByteBuffer keyBytesPrefix)
        {
            var keysToDelete = new List<ByteBuffer>();
            var enumerator = new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix, _modificationCounter);
            while (enumerator.MoveNext())
            {
                keysToDelete.Add(enumerator.GetKeyBytes());
            }

            foreach (var key in keysToDelete)
            {
                ResetKeyPrefix();
                if (_kvtr.Find(key) != FindResult.Exact)
                    throw new BTDBException("Not found record to delete.");

                var valueBytes = _kvtr.GetValue();

                if (_hasSecondaryIndexes)
                    RemoveSecondaryIndexes(key, valueBytes);

                if (_relationInfo.NeedImplementFreeContent())
                    _relationInfo.FreeContent(_transaction, valueBytes);
            }

            return RemovePrimaryKeysByPrefix(keyBytesPrefix);
        }

        public int RemoveByPrimaryKeyPrefixPartial(ByteBuffer keyBytesPrefix, int maxCount)
        {
            var enumerator = new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix, _modificationCounter);
            var keysToDelete = new List<ByteBuffer>();
            while (enumerator.MoveNext())
            {
                keysToDelete.Add(enumerator.GetKeyBytes());
                if (keysToDelete.Count == maxCount)
                    break;
            }
            foreach (var key in keysToDelete)
            {
                RemoveById(key, true);
            }
            return keysToDelete.Count;
        }

        public int RemoveByKeyPrefixWithoutIterate(ByteBuffer keyBytesPrefix)
        {
            if (_hasSecondaryIndexes)
            {
                //keyBytePrefix contains [Index Relation, Primary key prefix] we need
                //                       [Index Relation, Secondary Key Index, Primary key prefix]
                int idBytesLength = ObjectDB.AllRelationsPKPrefix.Length + PackUnpack.LengthVUInt(_relationInfo.Id);
                var writer = new ByteBufferWriter();
                foreach (var secKey in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
                {
                    WriteSKPrefix(writer);
                    writer.WriteVUInt32(_relationInfo.Id);
                    writer.WriteVUInt32(secKey.Key);
                    writer.WriteBlock(keyBytesPrefix.Buffer, idBytesLength, keyBytesPrefix.Length - idBytesLength);
                    _kvtr.SetKeyPrefix(writer.Data);
                    _kvtr.EraseAll();
                    writer.Reset();
                }
            }

            return RemovePrimaryKeysByPrefix(keyBytesPrefix);
        }

        int RemovePrimaryKeysByPrefix(ByteBuffer keyBytesPrefix)
        {
            _transaction.TransactionProtector.Start();
            _kvtr.SetKeyPrefix(keyBytesPrefix);
            int removedCount = (int)_kvtr.GetKeyValueCount();

            if (removedCount > 0)
            {
                _kvtr.EraseAll();
                _modificationCounter.MarkModification();
            }
            return removedCount;
        }

        public long CountWithPrefix(ByteBuffer keyBytesPrefix)
        {
            _transaction.TransactionProtector.Start();
            _kvtr.SetKeyPrefix(keyBytesPrefix);
            return _kvtr.GetKeyValueCount();
        }

        public long CountWithProposition(ByteBuffer prefixBytes,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes)
        {
            var keyValueTrProtector = _transaction.TransactionProtector;

            if (endKeyProposition == KeyProposition.Included)
                endKeyBytes = RelationAdvancedEnumerator<T>.FindLastKeyWithPrefix(prefixBytes, endKeyBytes, _kvtr, keyValueTrProtector);

            keyValueTrProtector.Start();
            _kvtr.SetKeyPrefix(prefixBytes);

            long startIndex;
            long endIndex;
            if (endKeyProposition == KeyProposition.Ignored)
            {
                endIndex = _kvtr.GetKeyValueCount() - 1;
            }
            else
            {
                switch (_kvtr.Find(endKeyBytes))
                {
                    case FindResult.Exact:
                        endIndex = _kvtr.GetKeyIndex();
                        if (endKeyProposition == KeyProposition.Excluded)
                        {
                            endIndex--;
                        }
                        break;
                    case FindResult.Previous:
                        endIndex = _kvtr.GetKeyIndex();
                        break;
                    case FindResult.Next:
                        endIndex = _kvtr.GetKeyIndex() - 1;
                        break;
                    case FindResult.NotFound:
                        endIndex = -1;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            if (startKeyProposition == KeyProposition.Ignored)
            {
                startIndex = 0;
            }
            else
            {
                switch (_kvtr.Find(startKeyBytes))
                {
                    case FindResult.Exact:
                        startIndex = _kvtr.GetKeyIndex();
                        if (startKeyProposition == KeyProposition.Excluded)
                        {
                            startIndex++;
                        }
                        break;
                    case FindResult.Previous:
                        startIndex = _kvtr.GetKeyIndex() + 1;
                        break;
                    case FindResult.Next:
                        startIndex = _kvtr.GetKeyIndex();
                        break;
                    case FindResult.NotFound:
                        startIndex = 0;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            return Math.Max(0, endIndex - startIndex + 1);
        }

        public int RemoveByIdAdvancedParam(RelationDBManipulator<T> manipulator, ByteBuffer prefixBytes, uint prefixFieldCount,
            EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes)
        {
            using (var enumerator = new RelationAdvancedEnumerator<T>(manipulator, prefixBytes, prefixFieldCount,
                order, startKeyProposition, startKeyBytes, endKeyProposition, endKeyBytes))
            {
                var keysToDelete = new List<ByteBuffer>();
                while (enumerator.MoveNext())
                {
                    keysToDelete.Add(enumerator.GetKeyBytes());
                }

                foreach (var key in keysToDelete)
                {
                    RemoveById(key, true);
                }

                return keysToDelete.Count;
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return new RelationEnumerator<T>(_transaction, _relationInfo, ByteBuffer.NewSync(_relationInfo.Prefix), _modificationCounter);
        }

        public T FindByIdOrDefault(ByteBuffer keyBytes, bool throwWhenNotFound)
        {
            ResetKeyPrefix();
            if (_kvtr.Find(keyBytes) != FindResult.Exact)
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found.");
                return default(T);
            }
            var valueBytes = _kvtr.GetValue();
            return (T)_relationInfo.CreateInstance(_transaction, keyBytes, valueBytes);
        }

        public IEnumerator<T> FindByPrimaryKeyPrefix(ByteBuffer keyBytesPrefix)
        {
            return new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix, _modificationCounter);
        }

        internal T CreateInstanceFromSK(uint secondaryKeyIndex, uint fieldInFirstBufferCount, ByteBuffer firstPart, ByteBuffer secondPart)
        {
            var pkWriter = new ByteBufferWriter();
            WritePKPrefix(pkWriter);
            pkWriter.WriteVUInt32(_relationInfo.Id);
            _relationInfo.GetSKKeyValuetoPKMerger(secondaryKeyIndex, fieldInFirstBufferCount)
                                                 (new ByteBufferReader(firstPart), new ByteBufferReader(secondPart), pkWriter);
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
            _transaction.TransactionProtector.Start();
            _kvtr.SetKeyPrefix(secKeyBytes);
            if (!_kvtr.FindFirstKey())
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found.");
                return default(T);
            }
            var keyBytes = _kvtr.GetKey();

            if (_kvtr.FindNextKey())
                throw new BTDBException("Ambiguous result.");

            return CreateInstanceFromSK(secondaryKeyIndex, prefixParametersCount, secKeyBytes, keyBytes);
        }

        void ResetKeyPrefix()
        {
            _transaction.TransactionProtector.Start();
            _kvtr.SetKeyPrefix(EmptyBuffer);
        }

        ByteBuffer WriteSecondaryKeyKey(uint secondaryKeyIndex, T obj)
        {
            var keyWriter = new ByteBufferWriter();
            var keySaver = _relationInfo.GetSecondaryKeysKeySaver(secondaryKeyIndex);
            WriteSKPrefix(keyWriter);
            keyWriter.WriteVUInt32(_relationInfo.Id);
            keyWriter.WriteVUInt32(secondaryKeyIndex); //secondary key index
            keySaver(_transaction, keyWriter, obj, this); //secondary key
            return keyWriter.Data;
        }

        ByteBuffer WriteSecondaryKeyKey(uint secondaryKeyIndex, ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            var keyWriter = new ByteBufferWriter();
            WriteSKPrefix(keyWriter);
            keyWriter.WriteVUInt32(_relationInfo.Id);
            keyWriter.WriteVUInt32(secondaryKeyIndex);

            var valueReader = new ByteBufferReader(valueBytes);
            var version = valueReader.ReadVUInt32();

            var keySaver = _relationInfo.GetPKValToSKMerger(version, secondaryKeyIndex);
            keySaver(_transaction, keyWriter, new ByteBufferReader(keyBytes), new ByteBufferReader(valueBytes), _relationInfo.DefaultClientObject);
            return keyWriter.Data;
        }

        void AddIntoSecondaryIndexes(T obj)
        {
            ResetKeyPrefix();

            foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecondaryKeyKey(sk.Key, obj);
                _kvtr.CreateOrUpdateKeyValue(keyBytes, EmptyBuffer);
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
            ResetKeyPrefix();

            var secKeys = _relationInfo.ClientRelationVersionInfo.SecondaryKeys;
            foreach (var sk in secKeys)
            {
                var newKeyBytes = WriteSecondaryKeyKey(sk.Key, newValue);
                var oldKeyBytes = WriteSecondaryKeyKey(sk.Key, oldKey, oldValue);
                if (ByteBuffersHasSameContent(oldKeyBytes, newKeyBytes))
                    continue;
                //remove old index
                if (_kvtr.Find(oldKeyBytes) != FindResult.Exact)
                    throw new BTDBException("Error in updating secondary indexes, previous index entry not found.");
                _kvtr.EraseCurrent();
                //insert new value
                _kvtr.CreateOrUpdateKeyValue(newKeyBytes, EmptyBuffer);
            }
        }

        void RemoveSecondaryIndexes(ByteBuffer oldKey, ByteBuffer oldValue)
        {
            ResetKeyPrefix();

            foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecondaryKeyKey(sk.Key, oldKey, oldValue);
                if (_kvtr.Find(keyBytes) != FindResult.Exact)
                    throw new BTDBException("Error in removing secondary indexes, previous index entry not found.");
                _kvtr.EraseCurrent();
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
                _kvtr.SetKeyPrefix(_relationInfo.Prefix);
                return (int)_kvtr.GetKeyValueCount();
            }
        }

        static void WritePKPrefix(AbstractBufferedWriter writer)
        {
            writer.WriteInt8(3); //ObjectDB.AllRelationsPKPrefix
        }

        static void WriteSKPrefix(AbstractBufferedWriter writer)
        {
            writer.WriteInt8(4); //ObjectDB.AllRelationsSKPrefix
        }
    }
}
