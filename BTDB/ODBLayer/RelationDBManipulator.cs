using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.Buffer;
using BTDB.Collections;
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

    public interface IRelationDbManipulator: IRelation, IRelationModificationCounter
    {
        public IInternalObjectDBTransaction Transaction { get; }
        public RelationInfo RelationInfo { get; }

        public object? CreateInstanceFromSecondaryKey(RelationInfo.ItemLoaderInfo itemLoader, uint secondaryKeyIndex,
            uint fieldInFirstBufferCount, in ReadOnlySpan<byte> firstPart, in ReadOnlySpan<byte> secondPart);
    }

    public class RelationDBManipulator<T> : IRelation<T>, IRelationDbManipulator where T : class
    {
        readonly IInternalObjectDBTransaction _transaction;
        readonly IKeyValueDBTransaction _kvtr;
        readonly RelationInfo _relationInfo;

        public IInternalObjectDBTransaction Transaction => _transaction;
        public RelationInfo RelationInfo => _relationInfo;

        const string AssertNotDerivedTypesMsg = "Derived types are not supported.";

        public RelationDBManipulator(IObjectDBTransaction transaction, RelationInfo relationInfo)
        {
            _transaction = (IInternalObjectDBTransaction) transaction;
            _kvtr = _transaction.KeyValueDBTransaction;
            _relationInfo = relationInfo;
            _hasSecondaryIndexes = _relationInfo.ClientRelationVersionInfo.HasSecondaryIndexes;
        }

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

        ReadOnlySpan<byte> ValueBytes(T obj)
        {
            var valueWriter = new SpanWriter();
            valueWriter.WriteVUInt32(_relationInfo.ClientTypeVersion);
            _relationInfo.ValueSaver(_transaction, ref valueWriter, obj);
            return valueWriter.GetSpan();
        }

        ReadOnlySpan<byte> KeyBytes(T obj)
        {
            var keyWriter = new SpanWriter();
            WriteRelationPKPrefix(ref keyWriter);
            _relationInfo.PrimaryKeysSaver(_transaction, ref keyWriter, obj,
                this); //this for relation interface which is same with manipulator
            return keyWriter.GetSpan();
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

            if (_kvtr.FindExactKey(keyBytes))
                return false;

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
                AddIntoSecondaryIndexes(obj);

            MarkModification();
            return true;
        }

        public bool Upsert(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            ResetKeyPrefix();
            if (_kvtr.FindExactKey(keyBytes))
            {
                var oldValueBytes = _kvtr.GetValueAsReadOnlySpan();

                _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

                if (_hasSecondaryIndexes)
                    UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes);

                FreeContentInUpdate(oldValueBytes, valueBytes);
                return false;
            }

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);
            if (_hasSecondaryIndexes)
                AddIntoSecondaryIndexes(obj);
            MarkModification();
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
                if (_kvtr.FindExactKey(keyBytes))
                {
                    var oldValueBytes = _kvtr.GetValueAsReadOnlySpan().ToArray();

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

            MarkModification();
            return true;
        }

        public void Update(T obj)
        {
            Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

            var keyBytes = KeyBytes(obj);
            var valueBytes = ValueBytes(obj);

            ResetKeyPrefix();

            if (!_kvtr.FindExactKey(keyBytes))
                throw new BTDBException("Not found record to update.");

            var oldValueBytes = _kvtr.GetValueAsReadOnlySpan().ToArray();
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

            if (!_kvtr.FindExactKey(keyBytes))
                throw new BTDBException("Not found record to update.");

            var oldValueBytes = _hasSecondaryIndexes
                ? _kvtr.GetValueAsReadOnlySpan().ToArray()
                : Array.Empty<byte>();

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
                UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes);
        }

        public bool Contains(in ReadOnlySpan<byte> keyBytes)
        {
            ResetKeyPrefix();
            return _kvtr.FindExactKey(keyBytes);
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

        void FreeContentInUpdate(in ReadOnlySpan<byte> oldValueBytes, in ReadOnlySpan<byte> newValueBytes)
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

        public bool RemoveById(in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound)
        {
            ResetKeyPrefix();
            if (!_kvtr.FindExactKey(keyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            var valueBytes = _kvtr.GetValueAsReadOnlySpan();
            _kvtr.EraseCurrent();

            if (_hasSecondaryIndexes)
                RemoveSecondaryIndexes(keyBytes, valueBytes);

            _relationInfo.FreeContent(_transaction, valueBytes);

            MarkModification();
            return true;
        }

        public bool ShallowRemoveById(in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound)
        {
            ResetKeyPrefix();
            if (!_kvtr.FindExactKey(keyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            var valueBytes = _hasSecondaryIndexes
                ? _kvtr.GetValueAsReadOnlySpan()
                : new ReadOnlySpan<byte>();

            _kvtr.EraseCurrent();

            if (_hasSecondaryIndexes)
                RemoveSecondaryIndexes(keyBytes, valueBytes);

            MarkModification();
            return true;
        }

        public int RemoveByPrimaryKeyPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
        {
            var keysToDelete = new StructList<byte[]>();
            var enumerator = new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix,
                this, 0);
            while (enumerator.MoveNext())
            {
                keysToDelete.Add(enumerator.GetKeyBytes().ToArray());
            }

            foreach (var key in keysToDelete)
            {
                ResetKeyPrefix();
                if (!_kvtr.FindExactKey(key))
                    throw new BTDBException("Not found record to delete.");

                var valueBytes = _kvtr.GetValueAsReadOnlySpan();

                if (_hasSecondaryIndexes)
                    RemoveSecondaryIndexes(key, valueBytes);

                if (_relationInfo.NeedImplementFreeContent())
                    _relationInfo.FreeContent(_transaction, valueBytes);
            }

            return RemovePrimaryKeysByPrefix(keyBytesPrefix);
        }

        public int RemoveByPrimaryKeyPrefixPartial(in ReadOnlySpan<byte> keyBytesPrefix, int maxCount)
        {
            var enumerator = new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix,
                this, 0);
            var keysToDelete = new StructList<byte[]>();
            while (enumerator.MoveNext())
            {
                keysToDelete.Add(enumerator.GetKeyBytes().ToArray());
                if (keysToDelete.Count == maxCount)
                    break;
            }

            foreach (var key in keysToDelete)
            {
                RemoveById(key, true);
            }

            return (int)keysToDelete.Count;
        }

        public int RemoveByKeyPrefixWithoutIterate(in ReadOnlySpan<byte> keyBytesPrefix)
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
                    writer.WriteBlock(keyBytesPrefix.Slice((int)idBytesLength));
                    _kvtr.EraseAll(writer.GetSpan());
                    writer.Reset();
                }
            }

            return RemovePrimaryKeysByPrefix(keyBytesPrefix);
        }

        int RemovePrimaryKeysByPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
        {
            _transaction.TransactionProtector.Start();
            MarkModification();
            return (int)_kvtr.EraseAll(keyBytesPrefix);
        }

        public long CountWithPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
        {
            _transaction.TransactionProtector.Start();
            return _kvtr.GetKeyValueCount(keyBytesPrefix);
        }

        public bool AnyWithPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
        {
            _transaction.TransactionProtector.Start();
            return _kvtr.FindFirstKey(keyBytesPrefix);
        }

        public bool AnyWithProposition(KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
            KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes)
        {
            return 0 < CountWithProposition(startKeyProposition, prefixLen, startKeyBytes, endKeyProposition, endKeyBytes);
        }

        public long CountWithProposition(KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
            KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes)
        {
            var keyValueTrProtector = _transaction.TransactionProtector;

            var realEndKeyBytes = endKeyBytes;
            if (endKeyProposition == KeyProposition.Included)
                realEndKeyBytes =
                    RelationAdvancedEnumerator<T>.FindLastKeyWithPrefix(endKeyBytes, _kvtr, keyValueTrProtector);

            keyValueTrProtector.Start();
            _kvtr.FindFirstKey(startKeyBytes.Slice(0,prefixLen));
            var prefixIndex = _kvtr.GetKeyIndex();

            long startIndex;
            long endIndex;
            if (endKeyProposition == KeyProposition.Ignored)
            {
                _kvtr.FindLastKey(startKeyBytes.Slice(0,prefixLen));

                endIndex = _kvtr.GetKeyIndex() - prefixIndex;
            }
            else
            {
                switch (_kvtr.Find(realEndKeyBytes, (uint)prefixLen))
                {
                    case FindResult.Exact:
                        endIndex = _kvtr.GetKeyIndex() - prefixIndex;
                        if (endKeyProposition == KeyProposition.Excluded)
                        {
                            endIndex--;
                        }

                        break;
                    case FindResult.Previous:
                        endIndex = _kvtr.GetKeyIndex() - prefixIndex;
                        break;
                    case FindResult.Next:
                        endIndex = _kvtr.GetKeyIndex() - prefixIndex - 1;
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
                switch (_kvtr.Find(startKeyBytes, (uint)prefixLen))
                {
                    case FindResult.Exact:
                        startIndex = _kvtr.GetKeyIndex() - prefixIndex;
                        if (startKeyProposition == KeyProposition.Excluded)
                        {
                            startIndex++;
                        }

                        break;
                    case FindResult.Previous:
                        startIndex = _kvtr.GetKeyIndex() - prefixIndex + 1;
                        break;
                    case FindResult.Next:
                        startIndex = _kvtr.GetKeyIndex() - prefixIndex;
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

        public int RemoveByIdAdvancedParam(uint prefixFieldCount,
            EnumerationOrder order,
            KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
            KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes)
        {
            using var enumerator = new RelationAdvancedEnumerator<T>(this, prefixFieldCount,
                order, startKeyProposition, prefixLen, startKeyBytes, endKeyProposition, endKeyBytes, 0);
            var keysToDelete = new StructList<byte[]>();
            while (enumerator.MoveNext())
            {
                keysToDelete.Add(enumerator.GetKeyBytes());
            }

            foreach (var key in keysToDelete)
            {
                RemoveById(key, true);
            }

            return (int)keysToDelete.Count;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return new RelationEnumerator<T>(_transaction, _relationInfo, _relationInfo.Prefix, this, 0);
        }

        public TItem FindByIdOrDefault<TItem>(in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound, int loaderIndex)
        {
            return (TItem) FindByIdOrDefaultInternal(_relationInfo.ItemLoaderInfos[loaderIndex], keyBytes,
                throwWhenNotFound);
        }

        object? FindByIdOrDefaultInternal(RelationInfo.ItemLoaderInfo itemLoader, in ReadOnlySpan<byte> keyBytes,
            bool throwWhenNotFound)
        {
            ResetKeyPrefix();
            if (!_kvtr.FindExactKey(keyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found.");
                return default;
            }

            var valueBytes = _kvtr.GetValueAsReadOnlySpan();
            return itemLoader.CreateInstance(_transaction, keyBytes, valueBytes);
        }

        public IEnumerator<TItem> FindByPrimaryKeyPrefix<TItem>(in ReadOnlySpan<byte> keyBytesPrefix, int loaderIndex)
        {
            return new RelationPrimaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytesPrefix,this, loaderIndex);
        }

        public object? CreateInstanceFromSecondaryKey(RelationInfo.ItemLoaderInfo itemLoader, uint secondaryKeyIndex,
            uint fieldInFirstBufferCount, in ReadOnlySpan<byte> firstPart, in ReadOnlySpan<byte> secondPart)
        {
            var pkWriter = new SpanWriter();
            WriteRelationPKPrefix(ref pkWriter);
            var readerFirst = new SpanReader(firstPart);
            var readerSecond = new SpanReader(secondPart);
            _relationInfo.GetSKKeyValueToPKMerger(secondaryKeyIndex, fieldInFirstBufferCount)
                (ref readerFirst, ref readerSecond, ref pkWriter);
            return FindByIdOrDefaultInternal(itemLoader, pkWriter.GetSpan(), true);
        }

        public IEnumerator<TItem> FindBySecondaryKey<TItem>(uint secondaryKeyIndex, uint prefixFieldCount,
            in ReadOnlySpan<byte> secKeyBytes, int loaderIndex)
        {
            return new RelationSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo, secKeyBytes,
                secondaryKeyIndex, prefixFieldCount, this, loaderIndex);
        }

        //secKeyBytes contains already AllRelationsSKPrefix
        public TItem FindBySecondaryKeyOrDefault<TItem>(uint secondaryKeyIndex, uint prefixParametersCount,
            in ReadOnlySpan<byte> secKeyBytes,
            bool throwWhenNotFound, int loaderIndex)
        {
            _transaction.TransactionProtector.Start();
            _kvtr.InvalidateCurrentKey();
            if (!_kvtr.FindFirstKey(secKeyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found.");
                return default;
            }

            var keyBytes = _kvtr.GetKeyAsReadOnlySpan();

            if (_kvtr.FindNextKey(secKeyBytes))
                throw new BTDBException("Ambiguous result.");

            return (TItem) CreateInstanceFromSecondaryKey(_relationInfo.ItemLoaderInfos[loaderIndex], secondaryKeyIndex,
                prefixParametersCount, secKeyBytes, keyBytes);
        }

        void ResetKeyPrefix()
        {
            _transaction.TransactionProtector.Start();
        }

        ReadOnlySpan<byte> WriteSecondaryKeyKey(uint secondaryKeyIndex, T obj)
        {
            var keyWriter = new SpanWriter();
            var keySaver = _relationInfo.GetSecondaryKeysKeySaver(secondaryKeyIndex);
            WriteRelationSKPrefix(ref keyWriter, secondaryKeyIndex);
            keySaver(_transaction, ref keyWriter, obj, this); //secondary key
            return keyWriter.GetSpan();
        }

        ReadOnlySpan<byte> WriteSecondaryKeyKey(uint secondaryKeyIndex, in ReadOnlySpan<byte> keyBytes, in ReadOnlySpan<byte> valueBytes)
        {
            var keyWriter = new SpanWriter();
            WriteRelationSKPrefix(ref keyWriter, secondaryKeyIndex);

            var version = (uint)PackUnpack.UnpackVUInt(valueBytes);

            var keySaver = _relationInfo.GetPKValToSKMerger(version, secondaryKeyIndex);
            var keyReader = new SpanReader(keyBytes);
            var valueReader = new SpanReader(valueBytes);
            keySaver(_transaction, ref keyWriter, ref keyReader, ref valueReader, _relationInfo.DefaultClientObject);
            return keyWriter.GetSpan();
        }

        void AddIntoSecondaryIndexes(T obj)
        {
            ResetKeyPrefix();

            foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecondaryKeyKey(sk.Key, obj);
                _kvtr.CreateOrUpdateKeyValue(keyBytes, new ReadOnlySpan<byte>());
            }
        }

        void UpdateSecondaryIndexes(T newValue, in ReadOnlySpan<byte> oldKey, in ReadOnlySpan<byte> oldValue)
        {
            ResetKeyPrefix();

            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var newKeyBytes = WriteSecondaryKeyKey(key, newValue);
                var oldKeyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
                if (oldKeyBytes.SequenceEqual(newKeyBytes))
                    continue;
                //remove old index
                EraseOldSecondaryKey(oldKey, oldKeyBytes, key);
                //insert new value
                _kvtr.CreateOrUpdateKeyValue(newKeyBytes, new ReadOnlySpan<byte>());
            }
        }

        void RemoveSecondaryIndexes(in ReadOnlySpan<byte> oldKey, in ReadOnlySpan<byte> oldValue)
        {
            ResetKeyPrefix();

            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
                EraseOldSecondaryKey(oldKey, keyBytes, key);
            }
        }

        void EraseOldSecondaryKey(in ReadOnlySpan<byte> primaryKey, in ReadOnlySpan<byte> keyBytes, uint skKey)
        {
            if (!_kvtr.FindExactKey(keyBytes))
            {
                var sk = _relationInfo.ClientRelationVersionInfo.SecondaryKeys[skKey];
                throw new BTDBException(
                    $"Error in removing secondary indexes, previous index entry not found. {_relationInfo.Name}:{sk.Name} PK:{BitConverter.ToString(primaryKey.ToArray()).Replace("-", "")}");
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
                return (int) _kvtr.GetKeyValueCount(_relationInfo.Prefix);
            }
        }

        public Type BtdbInternalGetRelationInterfaceType()
        {
            return _relationInfo.InterfaceType!;
        }

        public IRelation? BtdbInternalNextInChain { get; set; }
    }
}
