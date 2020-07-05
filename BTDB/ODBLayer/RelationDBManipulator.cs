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

    public interface IRelationDbManipulator: IRelation
    {
        public IInternalObjectDBTransaction Transaction { get; }
        public RelationInfo RelationInfo { get; }
        public IRelationModificationCounter ModificationCounter { get; }

        public object? CreateInstanceFromSecondaryKey(RelationInfo.ItemLoaderInfo itemLoader, uint secondaryKeyIndex,
            uint fieldInFirstBufferCount, ByteBuffer firstPart, ByteBuffer secondPart);
    }

    public class RelationDBManipulator<T> : IRelation<T>, IRelationDbManipulator where T : class
    {
        readonly IInternalObjectDBTransaction _transaction;
        readonly IKeyValueDBTransaction _kvtr;
        readonly RelationInfo _relationInfo;

        public IInternalObjectDBTransaction Transaction => _transaction;
        public RelationInfo RelationInfo => _relationInfo;

        const string AssertNotDerivedTypesMsg = "Derived types are not supported.";

        readonly IRelationModificationCounter _modificationCounter;

        public RelationDBManipulator(IObjectDBTransaction transaction, RelationInfo relationInfo)
        {
            _transaction = (IInternalObjectDBTransaction) transaction;
            _kvtr = _transaction.KeyValueDBTransaction;
            _relationInfo = relationInfo;
            _modificationCounter = _transaction.GetRelationModificationCounter(relationInfo.Id);
            _hasSecondaryIndexes = _relationInfo.ClientRelationVersionInfo.HasSecondaryIndexes;
        }

        public IRelationModificationCounter ModificationCounter => _modificationCounter;

        ByteBuffer ValueBytes(T obj)
        {
            var valueWriter = new SpanWriter();
            valueWriter.WriteVUInt32(_relationInfo.ClientTypeVersion);
            _relationInfo.ValueSaver(_transaction, ref valueWriter, obj);
            return valueWriter.GetByteBufferAndReset();
        }

        ByteBuffer KeyBytes(T obj)
        {
            var keyWriter = new SpanWriter();
            WriteRelationPKPrefix(ref keyWriter);
            _relationInfo.PrimaryKeysSaver(_transaction, ref keyWriter, obj,
                this); //this for relation interface which is same with manipulator
            return keyWriter.GetByteBufferAndReset();
        }

        public void WriteRelationPKPrefix(ref SpanWriter writer)
        {
            writer.WriteBlock(_relationInfo.Prefix);
        }

        public void WriteRelationSKPrefix(ref SpanWriter writer, uint secondaryKeyIndex)
        {
            writer.WriteBlock(_relationInfo.PrefixSecondary);
            writer.WriteUInt8((byte)secondaryKeyIndex);
        }

        public uint RemapPrimeSK(uint primeSecondaryKeyIndex)
        {
            return _relationInfo.PrimeSK2Real![primeSecondaryKeyIndex];
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
                : ByteBuffer.NewEmpty();

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
                UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes);
        }

        public bool Contains(ByteBuffer keyBytes)
        {
            ResetKeyPrefix();
            return _kvtr.Find(keyBytes) == FindResult.Exact;
        }

        void CompareAndRelease(List<ulong> oldItems, List<ulong> newItems,
            Action<IInternalObjectDBTransaction, ulong> freeAction)
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
                var newItemsDictionary = new HashSet<ulong>(newItems);
                foreach (var id in oldItems)
                {
                    if (newItemsDictionary.Contains(id))
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
                : ByteBuffer.NewEmpty();

            _kvtr.EraseCurrent();

            if (_hasSecondaryIndexes)
                RemoveSecondaryIndexes(keyBytes, valueBytes);

            _modificationCounter.MarkModification();
            return true;
        }

        public int RemoveByPrimaryKeyPrefix(ByteBuffer keyBytesPrefix)
        {
            var keysToDelete = new List<ByteBuffer>();
            var enumerator = new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix,
                _modificationCounter, 0);
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
            var enumerator = new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix,
                _modificationCounter, 0);
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
            if (_relationInfo.NeedImplementFreeContent())
            {
                return RemoveByPrimaryKeyPrefix(keyBytesPrefix);
            }
            if (_hasSecondaryIndexes)
            {
                //keyBytePrefix contains [3, Index Relation, Primary key prefix] we need
                //                       [4, Index Relation, Secondary Key Index, Primary key prefix]
                var idBytesLength = 1 + PackUnpack.LengthVUInt(_relationInfo.Id);
                var writer = new SpanWriter();
                foreach (var secKey in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
                {
                    WriteRelationSKPrefix(ref writer, secKey.Key);
                    writer.WriteBlock(keyBytesPrefix.Buffer!, (int)idBytesLength, keyBytesPrefix.Length - (int)idBytesLength);
                    _kvtr.SetKeyPrefix(writer.GetByteBufferAndReset());
                    _kvtr.EraseAll();
                }
            }

            return RemovePrimaryKeysByPrefix(keyBytesPrefix);
        }

        int RemovePrimaryKeysByPrefix(ByteBuffer keyBytesPrefix)
        {
            _transaction.TransactionProtector.Start();
            _kvtr.SetKeyPrefix(keyBytesPrefix);
            var removedCount = (int) _kvtr.GetKeyValueCount();

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

        public bool AnyWithPrefix(ByteBuffer keyBytesPrefix)
        {
            _transaction.TransactionProtector.Start();
            _kvtr.SetKeyPrefix(keyBytesPrefix);
            return _kvtr.FindFirstKey();
        }

        public bool AnyWithProposition(ByteBuffer prefixBytes,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes)
        {
            return 0 < CountWithProposition(prefixBytes, startKeyProposition, startKeyBytes, endKeyProposition,
                endKeyBytes);
        }

        public long CountWithProposition(ByteBuffer prefixBytes,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes)
        {
            var keyValueTrProtector = _transaction.TransactionProtector;

            if (endKeyProposition == KeyProposition.Included)
                endKeyBytes =
                    RelationAdvancedEnumerator<T>.FindLastKeyWithPrefix(prefixBytes, endKeyBytes, _kvtr,
                        keyValueTrProtector);

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

        public int RemoveByIdAdvancedParam(ByteBuffer prefixBytes, uint prefixFieldCount,
            EnumerationOrder order,
            KeyProposition startKeyProposition, ByteBuffer startKeyBytes,
            KeyProposition endKeyProposition, ByteBuffer endKeyBytes)
        {
            using var enumerator = new RelationAdvancedEnumerator<T>(this, prefixBytes, prefixFieldCount,
                order, startKeyProposition, startKeyBytes, endKeyProposition, endKeyBytes, 0);
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

        public IEnumerator<T> GetEnumerator()
        {
            return new RelationEnumerator<T>(_transaction, _relationInfo, ByteBuffer.NewSync(_relationInfo.Prefix),
                _modificationCounter, 0);
        }

        public TItem FindByIdOrDefault<TItem>(ByteBuffer keyBytes, bool throwWhenNotFound, int loaderIndex)
        {
            return (TItem) FindByIdOrDefaultInternal(_relationInfo.ItemLoaderInfos[loaderIndex], keyBytes,
                throwWhenNotFound);
        }

        object? FindByIdOrDefaultInternal(RelationInfo.ItemLoaderInfo itemLoader, ByteBuffer keyBytes,
            bool throwWhenNotFound)
        {
            ResetKeyPrefix();
            if (_kvtr.Find(keyBytes) != FindResult.Exact)
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found.");
                return default;
            }

            var valueBytes = _kvtr.GetValue();
            keyBytes = keyBytes.Slice(_relationInfo.Prefix.Length);
            return itemLoader.CreateInstance(_transaction, keyBytes, valueBytes);
        }

        public IEnumerator<TItem> FindByPrimaryKeyPrefix<TItem>(ByteBuffer keyBytesPrefix, int loaderIndex)
        {
            return new RelationPrimaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytesPrefix,
                _modificationCounter, loaderIndex);
        }

        public object? CreateInstanceFromSecondaryKey(RelationInfo.ItemLoaderInfo itemLoader, uint secondaryKeyIndex,
            uint fieldInFirstBufferCount, ByteBuffer firstPart, ByteBuffer secondPart)
        {
            var pkWriter = new SpanWriter();
            WriteRelationPKPrefix(ref pkWriter);
            var readerFirst = new SpanReader(firstPart);
            var readerSecond = new SpanReader(secondPart);
            _relationInfo.GetSKKeyValueToPKMerger(secondaryKeyIndex, fieldInFirstBufferCount)
                (ref readerFirst, ref readerSecond, ref pkWriter);
            return FindByIdOrDefaultInternal(itemLoader, pkWriter.GetByteBufferAndReset(), true);
        }

        public IEnumerator<TItem> FindBySecondaryKey<TItem>(uint secondaryKeyIndex, uint prefixFieldCount,
            ByteBuffer secKeyBytes, int loaderIndex)
        {
            return new RelationSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo, secKeyBytes.ToAsyncSafe(),
                secondaryKeyIndex, prefixFieldCount, this, loaderIndex);
        }

        //secKeyBytes contains already AllRelationsSKPrefix
        public TItem FindBySecondaryKeyOrDefault<TItem>(uint secondaryKeyIndex, uint prefixParametersCount,
            ByteBuffer secKeyBytes,
            bool throwWhenNotFound, int loaderIndex)
        {
            _transaction.TransactionProtector.Start();
            _kvtr.SetKeyPrefix(secKeyBytes);
            if (!_kvtr.FindFirstKey())
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found.");
                return default(TItem);
            }

            var keyBytes = _kvtr.GetKey();

            if (_kvtr.FindNextKey())
                throw new BTDBException("Ambiguous result.");

            return (TItem) CreateInstanceFromSecondaryKey(_relationInfo.ItemLoaderInfos[loaderIndex], secondaryKeyIndex,
                prefixParametersCount, secKeyBytes, keyBytes);
        }

        void ResetKeyPrefix()
        {
            _transaction.TransactionProtector.Start();
            _kvtr.SetKeyPrefix(ByteBuffer.NewEmpty());
        }

        ByteBuffer WriteSecondaryKeyKey(uint secondaryKeyIndex, T obj)
        {
            var keyWriter = new SpanWriter();
            var keySaver = _relationInfo.GetSecondaryKeysKeySaver(secondaryKeyIndex);
            WriteRelationSKPrefix(ref keyWriter, secondaryKeyIndex);
            keySaver(_transaction, ref keyWriter, obj, this); //secondary key
            return keyWriter.GetByteBufferAndReset();
        }

        ByteBuffer WriteSecondaryKeyKey(uint secondaryKeyIndex, ByteBuffer keyBytes, ByteBuffer valueBytes)
        {
            var keyWriter = new SpanWriter();
            WriteRelationSKPrefix(ref keyWriter, secondaryKeyIndex);

            var o = valueBytes.Offset;
            var version = (uint)PackUnpack.UnpackVUInt(valueBytes.Buffer!, ref o);

            var keySaver = _relationInfo.GetPKValToSKMerger(version, secondaryKeyIndex);
            var keyReader = new SpanReader(keyBytes);
            var valueReader = new SpanReader(valueBytes);
            keySaver(_transaction, ref keyWriter, ref keyReader, ref valueReader, _relationInfo.DefaultClientObject);
            return keyWriter.GetByteBufferAndReset();
        }

        void AddIntoSecondaryIndexes(T obj)
        {
            ResetKeyPrefix();

            foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecondaryKeyKey(sk.Key, obj);
                _kvtr.CreateOrUpdateKeyValue(keyBytes, ByteBuffer.NewEmpty());
            }
        }

        void UpdateSecondaryIndexes(T newValue, ByteBuffer oldKey, ByteBuffer oldValue)
        {
            ResetKeyPrefix();

            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var newKeyBytes = WriteSecondaryKeyKey(key, newValue);
                var oldKeyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
                if (oldKeyBytes == newKeyBytes)
                    continue;
                //remove old index
                EraseOldSecondaryKey(oldKey, oldKeyBytes, key);
                //insert new value
                _kvtr.CreateOrUpdateKeyValue(newKeyBytes, ByteBuffer.NewEmpty());
            }
        }

        void RemoveSecondaryIndexes(ByteBuffer oldKey, ByteBuffer oldValue)
        {
            ResetKeyPrefix();

            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
                EraseOldSecondaryKey(oldKey, keyBytes, key);
            }
        }

        void EraseOldSecondaryKey(in ByteBuffer primaryKey, in ByteBuffer keyBytes, uint skKey)
        {
            if (_kvtr.Find(keyBytes) != FindResult.Exact)
            {
                var sk = _relationInfo.ClientRelationVersionInfo.SecondaryKeys[skKey];
                throw new BTDBException(
                    $"Error in removing secondary indexes, previous index entry not found. {_relationInfo.Name}:{sk.Name} PK:{BitConverter.ToString(primaryKey.ToByteArray()).Replace("-", "")}");
            }
            _kvtr.EraseCurrent();
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
                return (int) _kvtr.GetKeyValueCount();
            }
        }

        public Type BtdbInternalGetRelationInterfaceType()
        {
            return _relationInfo.InterfaceType!;
        }

        public IRelation? BtdbInternalNextInChain { get; set; }
    }
}
