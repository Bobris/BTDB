using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public interface IRelationModificationCounter
{
    int ModificationCounter { get; }
    void CheckModifiedDuringEnum(int prevModification);
}

public interface IRelationDbManipulator : IRelation, IRelationModificationCounter
{
    public IInternalObjectDBTransaction Transaction { get; }
    public RelationInfo RelationInfo { get; }

    public object? CreateInstanceFromSecondaryKey(RelationInfo.ItemLoaderInfo itemLoader,
        uint remappedSecondaryKeyIndex,
        in ReadOnlySpan<byte> secondaryKey);
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
        _transaction = (IInternalObjectDBTransaction)transaction;
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

    ReadOnlySpan<byte> ValueBytes(T obj, scoped ref SpanWriter writer)
    {
        writer.WriteVUInt32(_relationInfo.ClientTypeVersion);
        _relationInfo.ValueSaver(_transaction, ref writer, obj);
        return writer.GetPersistentSpanAndReset();
    }

    ReadOnlySpan<byte> KeyBytes(T obj, scoped ref SpanWriter writer)
    {
        WriteRelationPKPrefix(ref writer);
        _relationInfo.PrimaryKeysSaver(_transaction, ref writer, obj);
        return writer.GetPersistentSpanAndReset();
    }

    public void WriteRelationPKPrefix(ref SpanWriter writer)
    {
        writer.WriteBlock(_relationInfo.Prefix);
    }

    public void WriteRelationSKPrefix(ref SpanWriter writer, uint remappedSecondaryKeyIndex)
    {
        writer.WriteBlock(_relationInfo.PrefixSecondary);
        writer.WriteUInt8((byte)remappedSecondaryKeyIndex);
    }

    public uint RemapPrimeSK(uint primeSecondaryKeyIndex)
    {
        return _relationInfo.PrimeSK2Real![primeSecondaryKeyIndex];
    }

    readonly bool _hasSecondaryIndexes;

    class SerializationCallbacks : IInternalSerializationCallbacks
    {
        public readonly ContinuousMemoryBlockWriter Metadata = new ContinuousMemoryBlockWriter();
        public readonly ContinuousMemoryBlockWriter Data = new ContinuousMemoryBlockWriter();

        public void MetadataCreateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
        {
            var writer = new SpanWriter(Metadata);
            writer.WriteByteArray(key);
            writer.WriteByteArray(value);
            writer.Sync();
        }

        public void CreateKeyValue(in ReadOnlySpan<byte> key, in ReadOnlySpan<byte> value)
        {
            var writer = new SpanWriter(Data);
            if (value.IsEmpty)
            {
                writer.WriteUInt8((byte)SerializationCommand.CreateKey);
                writer.WriteByteArray(key);
            }
            else
            {
                writer.WriteUInt8((byte)SerializationCommand.CreateKeyValue);
                writer.WriteByteArray(key);
                writer.WriteByteArray(value);
            }

            writer.Sync();
        }
    }

    SerializationCallbacks? _serializationCallbacks;

    public void SerializeInsert(ref SpanWriter writer, T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        _transaction.SetSerializationCallbacks(_serializationCallbacks ??= new SerializationCallbacks());
        writer.WriteUInt8((byte)SerializationCommand.CreateKeyValue);
        var start = writer.StartWriteByteArray();
        WriteRelationPKPrefix(ref writer);
        _relationInfo.PrimaryKeysSaver(_transaction, ref writer, obj);
        writer.FinishWriteByteArray(start);
        start = writer.StartWriteByteArray();
        writer.WriteVUInt32(_relationInfo.ClientTypeVersion);
        _relationInfo.ValueSaver(_transaction, ref writer, obj);
        writer.FinishWriteByteArray(start);

        if (_hasSecondaryIndexes)
        {
            foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                writer.WriteUInt8((byte)SerializationCommand.CreateKey);
                start = writer.StartWriteByteArray();
                WriteRelationSKPrefix(ref writer, sk.Key);
                var keySaver = _relationInfo.GetSecondaryKeysKeySaver(sk.Key);
                keySaver(_transaction, ref writer, obj);
                writer.FinishWriteByteArray(start);
            }
        }

        _transaction.SetSerializationCallbacks(null);
        if (_serializationCallbacks!.Metadata.GetCurrentPositionWithoutWriter() > 0 ||
            _serializationCallbacks.Data.GetCurrentPositionWithoutWriter() > 0)
        {
            var toCreate = _serializationCallbacks!.Metadata.GetByteBuffer();
            _serializationCallbacks!.Metadata.ResetAndFreeMemory();
            _kvtr.Owner.StartWritingTransaction().AsTask().ContinueWith(task =>
            {
                var tr = task.Result;
                var reader = new SpanReader(toCreate);
                while (!reader.Eof)
                {
                    var key = reader.ReadByteArrayAsSpan();
                    var value = reader.ReadByteArrayAsSpan();
                    tr.CreateOrUpdateKeyValue(key, value);
                }

                ((ObjectDB)_transaction.Owner).CommitLastObjIdAndDictId(tr);
                tr.Commit();
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        writer.WriteBlock(_serializationCallbacks.Data.GetSpan());
        _serializationCallbacks.Data.Reset();
    }

    [SkipLocalsInit]
    public bool Insert(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyBytes(obj, ref writer);

        if (_kvtr.FindExactKey(keyBytes))
            return false;

        var valueBytes = ValueBytes(obj, ref writer);

        _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

        if (_hasSecondaryIndexes)
        {
            writer = new SpanWriter(buf);
            AddIntoSecondaryIndexes(obj, ref writer);
        }

        MarkModification();
        return true;
    }

    [SkipLocalsInit]
    public bool Upsert(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyBytes(obj, ref writer);
        var valueBytes = ValueBytes(obj, ref writer);

        if (_kvtr.FindExactKey(keyBytes))
        {
            var oldValueBytes = _kvtr.GetClonedValue(ref MemoryMarshal.GetReference(writer.Buf), writer.Buf.Length);

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
            {
                Span<byte> buf2 = stackalloc byte[512];
                var writer2 = new SpanWriter(buf2);
                if (UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes, ref writer2))
                    MarkModification();
            }

            FreeContentInUpdate(oldValueBytes, valueBytes);
            return false;
        }

        _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        if (_hasSecondaryIndexes)
        {
            writer = new SpanWriter(buf);
            AddIntoSecondaryIndexes(obj, ref writer);
        }

        MarkModification();
        return true;
    }

    [SkipLocalsInit]
    public (bool Inserted, uint KeySize, uint OldValueSize, uint NewValueSize) ShallowUpsertWithSizes(T obj,
        bool allowInsert, bool allowUpdate)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyBytes(obj, ref writer);
        var valueBytes = ValueBytes(obj, ref writer);

        if (_kvtr.FindExactKey(keyBytes))
        {
            var oldValueSize = _kvtr.GetStorageSizeOfCurrentKey().Value;
            if (allowUpdate)
            {
                _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);
                return (false, (uint)keyBytes.Length, oldValueSize, (uint)valueBytes.Length);
            }

            return (false, (uint)keyBytes.Length, oldValueSize, oldValueSize);
        }

        if (allowInsert)
        {
            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);
            MarkModification();
            return (true, (uint)keyBytes.Length, 0, (uint)valueBytes.Length);
        }

        return (false, 0, 0, 0);
    }

    public (long Inserted, long Updated) UpsertRange(IEnumerable<T> items)
    {
        var inserted = 0L;
        var updated = 0L;
        if (_relationInfo.NeedImplementFreeContent())
        {
            foreach (var item in items)
            {
                if (Upsert(item)) inserted++;
                else updated++;
            }
        }
        else
        {
            foreach (var item in items)
            {
                if (ShallowUpsert(item)) inserted++;
                else updated++;
            }
        }

        return (inserted, updated);
    }

    [SkipLocalsInit]
    public bool ShallowUpsert(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyBytes(obj, ref writer);
        var valueBytes = ValueBytes(obj, ref writer);

        if (_hasSecondaryIndexes)
        {
            if (_kvtr.FindExactKey(keyBytes))
            {
                var oldValueBytes =
                    _kvtr.GetClonedValue(ref MemoryMarshal.GetReference(writer.Buf), writer.Buf.Length);

                _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

                Span<byte> buf2 = stackalloc byte[512];
                var writer2 = new SpanWriter(buf2);
                if (UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes, ref writer2))
                    MarkModification();

                return false;
            }

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);
            writer = new SpanWriter(buf);
            AddIntoSecondaryIndexes(obj, ref writer);
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

    [SkipLocalsInit]
    public void Update(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyBytes(obj, ref writer);
        var valueBytes = ValueBytes(obj, ref writer);

        if (!_kvtr.FindExactKey(keyBytes))
            throw new BTDBException("Not found record to update.");

        var oldValueBytes = _kvtr.GetClonedValue(ref MemoryMarshal.GetReference(writer.Buf), writer.Buf.Length);
        _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

        if (_hasSecondaryIndexes)
        {
            Span<byte> buf2 = stackalloc byte[512];
            var writer2 = new SpanWriter(buf2);
            if (UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes, ref writer2))
                MarkModification();
        }

        FreeContentInUpdate(oldValueBytes, valueBytes);
    }

    [SkipLocalsInit]
    public void ShallowUpdate(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[512];
        var writer = new SpanWriter(buf);
        var keyBytes = KeyBytes(obj, ref writer);
        var valueBytes = ValueBytes(obj, ref writer);

        if (!_kvtr.FindExactKey(keyBytes))
            throw new BTDBException("Not found record to update.");

        if (_hasSecondaryIndexes)
        {
            var oldValueBytes = _kvtr.GetClonedValue(ref MemoryMarshal.GetReference(writer.Buf), writer.Buf.Length);

            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
            {
                Span<byte> buf2 = stackalloc byte[512];
                var writer2 = new SpanWriter(buf2);
                if (UpdateSecondaryIndexes(obj, keyBytes, oldValueBytes, ref writer2))
                    MarkModification();
            }
        }
        else
        {
            _kvtr.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }
    }

    [SkipLocalsInit]
    public bool UpdateByIdStart(ReadOnlySpan<byte> keyBytes, ref SpanWriter writer, ref ReadOnlySpan<byte> valueBytes,
        bool throwIfNotFound)
    {
        if (!_kvtr.FindExactKey(keyBytes))
        {
            if (throwIfNotFound)
                throw new BTDBException("Not found record to update.");
            return false;
        }

        valueBytes = _kvtr.GetClonedValue(ref MemoryMarshal.GetReference(valueBytes), valueBytes.Length);

        var version = (uint)PackUnpack.UnpackVUInt(valueBytes);

        var currentVersion = _relationInfo.ClientTypeVersion;
        if (version != currentVersion)
        {
            var itemLoader = _relationInfo.ItemLoaderInfos[0];

            var reader = new SpanReader(keyBytes);
            reader.SkipInt8(); // 3
            reader.SkipVUInt64(); // RelationId
            var obj = (T)itemLoader._primaryKeysLoader(_transaction, ref reader);
            reader = new(valueBytes);
            reader.SkipVUInt32();
            itemLoader.GetValueLoader(version)(_transaction, ref reader, obj);
            valueBytes = ValueBytes(obj, ref writer);
        }

        writer.WriteVUInt32(currentVersion);
        return true;
    }

    [SkipLocalsInit]
    public void UpdateByIdFinish(ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> oldValueBytes,
        ReadOnlySpan<byte> newValueBytes)
    {
        _kvtr.CreateOrUpdateKeyValue(keyBytes, newValueBytes);

        if (_hasSecondaryIndexes)
        {
            if (UpdateSecondaryIndexes(keyBytes, oldValueBytes, newValueBytes))
                MarkModification();
        }
    }

    public bool Contains(in ReadOnlySpan<byte> keyBytes)
    {
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

    [SkipLocalsInit]
    public bool RemoveById(in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound)
    {
        Span<byte> valueBuffer = stackalloc byte[512];

        if (!_kvtr.EraseCurrent(keyBytes, ref MemoryMarshal.GetReference(valueBuffer), valueBuffer.Length,
                out var value))
        {
            if (throwWhenNotFound)
                throw new BTDBException("Not found record to delete.");
            return false;
        }

        if (_hasSecondaryIndexes)
        {
            RemoveSecondaryIndexes(keyBytes, value);
        }

        _relationInfo.FreeContent(_transaction, value);

        MarkModification();
        return true;
    }

    [SkipLocalsInit]
    public bool ShallowRemoveById(in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound)
    {
        if (_hasSecondaryIndexes)
        {
            Span<byte> valueBuffer = stackalloc byte[512];

            if (!_kvtr.EraseCurrent(keyBytes, ref valueBuffer.GetPinnableReference(), valueBuffer.Length,
                    out var value))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            RemoveSecondaryIndexes(keyBytes, value);
        }
        else
        {
            if (!_kvtr.EraseCurrent(keyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }
        }

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
            keysToDelete.Add(_kvtr.GetKeyToArray());
        }

        foreach (var key in keysToDelete)
        {
            if (!_kvtr.FindExactKey(key))
                throw new BTDBException("Not found record to delete.");

            var valueBytes = _kvtr.GetValue();

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
            keysToDelete.Add(_kvtr.GetKeyToArray());
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
        MarkModification();
        return (int)_kvtr.EraseAll(keyBytesPrefix);
    }

    public void RemoveAll()
    {
        MarkModification();
        if (_relationInfo.NeedImplementFreeContent())
        {
            var count = _kvtr.GetKeyValueCount(_relationInfo.Prefix);
            for (var idx = 0L; idx < count; idx++)
            {
                if (!_kvtr.SetKeyIndex(_relationInfo.Prefix, idx))
                    throw new BTDBException("Not found record in RemoveAll.");
                var valueBytes = _kvtr.GetValue();
                _relationInfo.FreeContent(_transaction, valueBytes);
            }
        }

        _kvtr.EraseAll(_relationInfo.Prefix);
        if (_hasSecondaryIndexes)
        {
            _kvtr.EraseAll(_relationInfo.PrefixSecondary);
        }
    }

    public long CountWithPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
    {
        return _kvtr.GetKeyValueCount(keyBytesPrefix);
    }

    public bool AnyWithPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
    {
        return _kvtr.FindFirstKey(keyBytesPrefix);
    }

    public bool AnyWithProposition(KeyProposition startKeyProposition, int prefixLen,
        in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes)
    {
        return 0 < CountWithProposition(startKeyProposition, prefixLen, startKeyBytes, endKeyProposition,
            endKeyBytes);
    }

    public long CountWithProposition(KeyProposition startKeyProposition, int prefixLen,
        in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes)
    {
        if (!_kvtr.FindFirstKey(startKeyBytes.Slice(0, prefixLen)))
            return 0;

        var prefixIndex = _kvtr.GetKeyIndex();

        var realEndKeyBytes = endKeyBytes;
        if (endKeyProposition == KeyProposition.Included)
            realEndKeyBytes =
                RelationAdvancedEnumerator<T>.FindLastKeyWithPrefix(endKeyBytes, _kvtr);

        long startIndex;
        long endIndex;
        if (endKeyProposition == KeyProposition.Ignored)
        {
            _kvtr.FindLastKey(startKeyBytes.Slice(0, prefixLen));

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

    public IEnumerable<TAs> As<TAs>()
    {
        if (_kvtr.GetKeyValueCount(_relationInfo.Prefix) == 0) return Enumerable.Empty<TAs>();
        var loaderInfo = new RelationInfo.ItemLoaderInfo(_relationInfo, typeof(TAs));
        return new RelationEnumerator<TAs>(_transaction, _relationInfo.Prefix, this, loaderInfo);
    }

    public TItem FindByIdOrDefault<TItem>(in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound, int loaderIndex)
    {
        return (TItem)FindByIdOrDefaultInternal(_relationInfo.ItemLoaderInfos[loaderIndex], keyBytes,
            throwWhenNotFound);
    }

    object? FindByIdOrDefaultInternal(RelationInfo.ItemLoaderInfo itemLoader, in ReadOnlySpan<byte> keyBytes,
        bool throwWhenNotFound)
    {
        if (!_kvtr.FindExactKey(keyBytes))
        {
            if (throwWhenNotFound)
                throw new BTDBException("Not found.");
            return default;
        }

        return itemLoader.CreateInstance(_transaction, keyBytes);
    }

    public IEnumerator<TItem> FindByPrimaryKeyPrefix<TItem>(in ReadOnlySpan<byte> keyBytesPrefix, int loaderIndex)
    {
        return new RelationPrimaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytesPrefix, this,
            loaderIndex);
    }

    public TItem FirstByPrimaryKey<TItem>(int loaderIndex, ConstraintInfo[] constraints, ICollection<TItem> target,
        IOrderer[]? orderers, bool hasOrDefault) where TItem : class
    {
        StructList<byte> keyBytes = new();
        keyBytes.AddRange(_relationInfo.Prefix);

        if (orderers == null || orderers.Length == 0)
        {
            var enumerator = new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes, this,
                loaderIndex, constraints);

            if (enumerator.MoveNextInGather())
            {
                return enumerator.CurrentInGather;
            }

            ThrowIfNotHasOrDefault(hasOrDefault);
            return null!;
        }
        else
        {
            var relationVersionInfo = _relationInfo.ClientRelationVersionInfo;
            var primaryKeyFields = relationVersionInfo.PrimaryKeyFields.Span;
            var ordererIdxs = PrepareOrderers(ref constraints, orderers, primaryKeyFields);

            var enumerator = new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes, this,
                loaderIndex, constraints);

            var sns = new SortNativeStorage(true);
            try
            {
                enumerator.GatherForSorting(ref sns, ordererIdxs, orderers);
                if (sns.First.IsEmpty)
                {
                    ThrowIfNotHasOrDefault(hasOrDefault);
                    return null!;
                }

                return enumerator.CurrentByKeyIndex(sns.GetFirstKeyIndex());
            }
            finally
            {
                sns.Dispose();
            }
        }
    }

    public TItem FirstBySecondaryKey<TItem>(int loaderIndex, ConstraintInfo[] constraints, uint secondaryKeyIndex,
        IOrderer[]? orderers, bool hasOrDefault) where TItem : class
    {
        StructList<byte> keyBytes = new();
        keyBytes.AddRange(_relationInfo.PrefixSecondary);
        var remappedSecondaryKeyIndex = RemapPrimeSK(secondaryKeyIndex);
        keyBytes.Add((byte)remappedSecondaryKeyIndex);

        if (orderers == null || orderers.Length == 0)
        {
            var enumerator = new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytes,
                this,
                loaderIndex, constraints, remappedSecondaryKeyIndex, this);

            if (enumerator.MoveNextInGather())
            {
                return enumerator.CurrentInGather;
            }

            ThrowIfNotHasOrDefault(hasOrDefault);
            return null!;
        }
        else
        {
            var relationVersionInfo = _relationInfo.ClientRelationVersionInfo;
            var secondaryKeyInfo = relationVersionInfo.SecondaryKeys[remappedSecondaryKeyIndex];
            var fields = secondaryKeyInfo.Fields;
            var ordererIdxs = PrepareOrderersSK(ref constraints, orderers, fields, relationVersionInfo);

            var enumerator = new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytes,
                this,
                loaderIndex, constraints, remappedSecondaryKeyIndex, this);

            var sns = new SortNativeStorage(true);
            try
            {
                enumerator.GatherForSorting(ref sns, ordererIdxs, orderers);
                if (sns.First.IsEmpty)
                {
                    ThrowIfNotHasOrDefault(hasOrDefault);
                    return null!;
                }

                return enumerator.CurrentByKeyIndex(sns.GetFirstKeyIndex());
            }
            finally
            {
                sns.Dispose();
            }
        }
    }

    static void ThrowIfNotHasOrDefault(bool hasOrDefault)
    {
        if (!hasOrDefault)
            throw new BTDBException("FirstBy didn't found item. Append OrDefault to method name if you don't care.");
    }

    int[] PrepareOrderers(ref ConstraintInfo[] constraints, IOrderer[] orderers,
        ReadOnlySpan<TableFieldInfo> primaryKeyFields)
    {
        var ordererIdxs = new int[orderers.Length];
        Array.Fill(ordererIdxs, -2);
        for (var i = 0; i < primaryKeyFields.Length; i++)
        {
            var fi = primaryKeyFields[i];
            for (var j = 0; j < orderers.Length; j++)
            {
                if (orderers[j].ColumnName == fi.Name)
                {
                    ordererIdxs[j] = i;
                    var type = orderers[j].ExpectedInput;
                    if (type != null && type != typeof(T))
                    {
                        throw new BTDBException("Orderer[" + j + "] " + orderers[j].ColumnName + " of type " +
                                                type.ToSimpleName() + " is not equal to " + typeof(T));
                    }

                    while (i >= constraints.Length)
                    {
                        var fi2 = primaryKeyFields[constraints.Length];
                        var constraintType = typeof(Constraint<>).MakeGenericType(fi2.Handler!.HandledType()!);
                        var constraintAny = (IConstraint)constraintType.GetField("Any")!.GetValue(null);
                        constraints = constraints.Append(new() { Constraint = constraintAny! }).ToArray();
                    }
                }
            }
        }

        for (var i = 0; i < orderers.Length; i++)
        {
            if (ordererIdxs[i] == -2)
            {
                throw new BTDBException("Unmatched orderer[" + i + "] " + orderers[i].ColumnName + " of " +
                                        orderers[i].ExpectedInput?.ToSimpleName() + " in relation " +
                                        _relationInfo.Name);
            }
        }

        return ordererIdxs;
    }

    public (ulong Count, ulong KeySizes, ulong ValueSizes) RemoveWithSizesByPrimaryKey(ConstraintInfo[] constraints)
    {
        if (_relationInfo.NeedImplementFreeContent())
        {
            throw new NotSupportedException("RemoveWithSizes does not support FreeContent");
        }
        SpanWriter writer = new();
        StructList<byte> helperBuffer = new();
        writer.WriteBlock(_relationInfo.Prefix);
        var completedPrefix = false;
        foreach (var constraint in constraints)
        {
            if (constraint.Constraint.IsAnyConstraint())
            {
                completedPrefix = true;
            }
            else
                switch (constraint.Constraint.Prepare(ref helperBuffer))
                {
                    case IConstraint.MatchType.NoPrefix:
                        throw new NotSupportedException("Only prefix constraints allowed");
                    case IConstraint.MatchType.Prefix:
                        if (completedPrefix)
                            goto case IConstraint.MatchType.NoPrefix;
                        completedPrefix = true;
                        constraint.Constraint.WritePrefix(ref writer, helperBuffer);
                        break;
                    case IConstraint.MatchType.Exact:
                        if (completedPrefix)
                            goto case IConstraint.MatchType.NoPrefix;
                        constraint.Constraint.WritePrefix(ref writer, helperBuffer);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
        }

        var keySizes = 0ul;
        var valueSizes = 0ul;
        var count = 0ul;
        var keyPrefix = writer.GetSpan();

        if (_kvtr.FindFirstKey(keyPrefix))
        {
            do
            {
                var p = _kvtr.GetStorageSizeOfCurrentKey();
                keySizes += p.Key;
                valueSizes += p.Value;
                count++;
            } while (_kvtr.FindNextKey(keyPrefix));
        }

        _kvtr.EraseAll(keyPrefix);
        return (count, keySizes, valueSizes);
    }

    public ulong GatherByPrimaryKey<TItem>(int loaderIndex, ConstraintInfo[] constraints, ICollection<TItem> target,
        long skip, long take, IOrderer[]? orderers)
    {
        StructList<byte> keyBytes = new();
        keyBytes.AddRange(_relationInfo.Prefix);
        if (skip < 0)
        {
            take += skip;
            skip = 0;
        }

        if (orderers == null || orderers.Length == 0)
        {
            var enumerator = new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes, this,
                loaderIndex, constraints);

            var count = 0ul;

            while (enumerator.MoveNextInGather())
            {
                count++;
                if (skip > 0)
                {
                    skip--;
                    continue;
                }

                if (take <= 0) continue;
                take--;
                target.Add(enumerator.CurrentInGather);
            }

            return count;
        }
        else
        {
            var relationVersionInfo = _relationInfo.ClientRelationVersionInfo;
            var primaryKeyFields = relationVersionInfo.PrimaryKeyFields.Span;
            var ordererIdxs = PrepareOrderers(ref constraints, orderers, primaryKeyFields);

            var enumerator = new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes, this,
                loaderIndex, constraints);

            var sns = new SortNativeStorage(false);
            try
            {
                enumerator.GatherForSorting(ref sns, ordererIdxs, orderers);
                sns.Sort();
                var count = sns.Items.Count;
                for (var i = 0; i < take; i++)
                {
                    if (skip + i >= count) break;
                    target.Add(enumerator.CurrentByKeyIndex(sns.GetKeyIndex((int)skip + i)));
                }

                return count;
            }
            finally
            {
                sns.Dispose();
            }
        }
    }

    public ulong GatherBySecondaryKey<TItem>(int loaderIndex, ConstraintInfo[] constraints, ICollection<TItem> target,
        long skip, long take, uint secondaryKeyIndex, IOrderer[]? orderers)
    {
        StructList<byte> keyBytes = new();
        keyBytes.AddRange(_relationInfo.PrefixSecondary);
        var remappedSecondaryKeyIndex = RemapPrimeSK(secondaryKeyIndex);
        keyBytes.Add((byte)remappedSecondaryKeyIndex);
        if (skip < 0)
        {
            take += skip;
            skip = 0;
        }

        if (orderers == null || orderers.Length == 0)
        {
            var enumerator = new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytes,
                this,
                loaderIndex, constraints, remappedSecondaryKeyIndex, this);

            var count = 0ul;

            while (enumerator.MoveNextInGather())
            {
                count++;
                if (skip > 0)
                {
                    skip--;
                    continue;
                }

                if (take <= 0) continue;
                take--;
                target.Add(enumerator.CurrentInGather);
            }

            return count;
        }
        else
        {
            var relationVersionInfo = _relationInfo.ClientRelationVersionInfo;
            var secondaryKeyInfo = relationVersionInfo.SecondaryKeys[remappedSecondaryKeyIndex];
            var fields = secondaryKeyInfo.Fields;
            var ordererIdxs = PrepareOrderersSK(ref constraints, orderers, fields, relationVersionInfo);

            var enumerator = new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytes,
                this,
                loaderIndex, constraints, remappedSecondaryKeyIndex, this);

            var sns = new SortNativeStorage(false);
            try
            {
                enumerator.GatherForSorting(ref sns, ordererIdxs, orderers);
                sns.Sort();
                var count = sns.Items.Count;
                for (var i = 0; i < take; i++)
                {
                    if (skip + i >= count) break;
                    target.Add(enumerator.CurrentByKeyIndex(sns.GetKeyIndex((int)skip + i)));
                }

                return count;
            }
            finally
            {
                sns.Dispose();
            }
        }
    }

    int[] PrepareOrderersSK(ref ConstraintInfo[] constraints, IOrderer[] orderers, IList<FieldId> fields,
        RelationVersionInfo relationVersionInfo)
    {
        var ordererIdxs = new int[orderers.Length];
        Array.Fill(ordererIdxs, -2);
        for (var i = 0; i < fields.Count; i++)
        {
            var fi = relationVersionInfo.GetFieldInfo(fields[i]);
            for (var j = 0; j < orderers.Length; j++)
            {
                if (orderers[j].ColumnName == fi.Name)
                {
                    ordererIdxs[j] = i;
                    var type = orderers[j].ExpectedInput;
                    if (type != null && type != typeof(T))
                    {
                        throw new BTDBException("Orderer[" + j + "] " + orderers[j].ColumnName + " of type " +
                                                type.ToSimpleName() + " is not equal to " + typeof(T));
                    }

                    while (i >= constraints.Length)
                    {
                        var fi2 = relationVersionInfo.GetFieldInfo(fields[constraints.Length]);
                        var constraintType = typeof(Constraint<>).MakeGenericType(fi2.Handler!.HandledType()!);
                        var constraintAny = (IConstraint)constraintType.GetField("Any")!.GetValue(null);
                        constraints = constraints.Append(new() { Constraint = constraintAny! }).ToArray();
                    }
                }
            }
        }

        for (var i = 0; i < orderers.Length; i++)
        {
            if (ordererIdxs[i] == -2)
            {
                throw new BTDBException("Unmatched orderer[" + i + "] " + orderers[i].ColumnName + " of " +
                                        orderers[i].ExpectedInput?.ToSimpleName() + " in relation " +
                                        _relationInfo.Name);
            }
        }

        return ordererIdxs;
    }

    public IEnumerable<TItem> ScanByPrimaryKeyPrefix<TItem>(int loaderIndex, ConstraintInfo[] constraints)
    {
        StructList<byte> keyBytes = new();
        keyBytes.AddRange(_relationInfo.Prefix);
        return new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes, this,
            loaderIndex, constraints);
    }

    public IEnumerable<TItem> ScanBySecondaryKeyPrefix<TItem>(int loaderIndex, ConstraintInfo[] constraints,
        uint secondaryKeyIndex)
    {
        StructList<byte> keyBytes = new();
        keyBytes.AddRange(_relationInfo.PrefixSecondary);
        var remappedSecondaryKeyIndex = RemapPrimeSK(secondaryKeyIndex);
        keyBytes.Add((byte)remappedSecondaryKeyIndex);
        return new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytes, this,
            loaderIndex, constraints, remappedSecondaryKeyIndex, this);
    }

    [SkipLocalsInit]
    public unsafe object CreateInstanceFromSecondaryKey(RelationInfo.ItemLoaderInfo itemLoader,
        uint remappedSecondaryKeyIndex,
        in ReadOnlySpan<byte> secondaryKey)
    {
        Span<byte> pkBuffer = stackalloc byte[512];
        var pkWriter = new SpanWriter(Unsafe.AsPointer(ref MemoryMarshal.GetReference(pkBuffer)), pkBuffer.Length);
        WriteRelationPKPrefix(ref pkWriter);
        var reader = new SpanReader(secondaryKey);
        _relationInfo.GetSKKeyValueToPKMerger(remappedSecondaryKeyIndex)
            (ref reader, ref pkWriter);
        return FindByIdOrDefaultInternal(itemLoader, pkWriter.GetSpan(), true)!;
    }

    public IEnumerator<TItem> FindBySecondaryKey<TItem>(uint remappedSecondaryKeyIndex,
        in ReadOnlySpan<byte> secKeyBytes, int loaderIndex)
    {
        return new RelationSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo, secKeyBytes,
            remappedSecondaryKeyIndex, this, loaderIndex);
    }

    //secKeyBytes contains already AllRelationsSKPrefix
    public TItem FindBySecondaryKeyOrDefault<TItem>(uint remappedSecondaryKeyIndex,
        in ReadOnlySpan<byte> secKeyBytes,
        bool throwWhenNotFound, int loaderIndex)
    {
        _kvtr.InvalidateCurrentKey();
        if (!_kvtr.FindFirstKey(secKeyBytes))
        {
            if (throwWhenNotFound)
                throw new BTDBException("Not found.");
            return default;
        }

        var keyBytes = _kvtr.GetKey();

        if (_kvtr.FindNextKey(secKeyBytes))
            throw new BTDBException("Ambiguous result.");

        return (TItem)CreateInstanceFromSecondaryKey(_relationInfo.ItemLoaderInfos[loaderIndex],
            remappedSecondaryKeyIndex,
            keyBytes);
    }

    ReadOnlySpan<byte> WriteSecondaryKeyKey(uint remappedSecondaryKeyIndex, T obj, ref SpanWriter writer)
    {
        var keySaver = _relationInfo.GetSecondaryKeysKeySaver(remappedSecondaryKeyIndex);
        WriteRelationSKPrefix(ref writer, remappedSecondaryKeyIndex);
        keySaver(_transaction, ref writer, obj); //secondary key
        return writer.GetSpan();
    }

    ReadOnlySpan<byte> WriteSecondaryKeyKey(uint remappedSecondaryKeyIndex, in ReadOnlySpan<byte> keyBytes,
        in ReadOnlySpan<byte> valueBytes)
    {
        var keyWriter = new SpanWriter();
        WriteRelationSKPrefix(ref keyWriter, remappedSecondaryKeyIndex);

        var version = (uint)PackUnpack.UnpackVUInt(valueBytes);

        var keySaver = _relationInfo.GetPKValToSKMerger(version, remappedSecondaryKeyIndex);
        var keyReader = new SpanReader(keyBytes);
        var valueReader = new SpanReader(valueBytes);
        keySaver(_transaction, ref keyWriter, ref keyReader, ref valueReader, _relationInfo.DefaultClientObject);
        return keyWriter.GetSpan();
    }

    void AddIntoSecondaryIndexes(T obj, ref SpanWriter writer)
    {
        foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
        {
            var keyBytes = WriteSecondaryKeyKey(sk.Key, obj, ref writer);
            _kvtr.CreateOrUpdateKeyValue(keyBytes, new ReadOnlySpan<byte>());
            writer.Reset();
        }
    }

    bool UpdateSecondaryIndexes(in ReadOnlySpan<byte> oldKey, in ReadOnlySpan<byte> oldValue,
        in ReadOnlySpan<byte> newValue)
    {
        var changed = false;
        foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
        {
            var newKeyBytes = WriteSecondaryKeyKey(key, oldKey, newValue);
            var oldKeyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
            if (oldKeyBytes.SequenceEqual(newKeyBytes))
                continue;
            //remove old index
            EraseOldSecondaryKey(oldKey, oldKeyBytes, key);
            //insert new value
            _kvtr.CreateOrUpdateKeyValue(newKeyBytes, new());
            changed = true;
        }

        return changed;
    }

    bool UpdateSecondaryIndexes(T newValue, in ReadOnlySpan<byte> oldKey, in ReadOnlySpan<byte> oldValue,
        ref SpanWriter writer)
    {
        var changed = false;
        foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
        {
            writer.Reset();
            var newKeyBytes = WriteSecondaryKeyKey(key, newValue, ref writer);
            var oldKeyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
            if (oldKeyBytes.SequenceEqual(newKeyBytes))
                continue;
            //remove old index
            EraseOldSecondaryKey(oldKey, oldKeyBytes, key);
            //insert new value
            _kvtr.CreateOrUpdateKeyValue(newKeyBytes, new());
            changed = true;
        }

        return changed;
    }

    void RemoveSecondaryIndexes(in ReadOnlySpan<byte> oldKey, in ReadOnlySpan<byte> oldValue)
    {
        foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
        {
            var keyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
            EraseOldSecondaryKey(oldKey, keyBytes, key);
        }
    }

    void EraseOldSecondaryKey(in ReadOnlySpan<byte> primaryKey, in ReadOnlySpan<byte> keyBytes, uint skKey)
    {
        if (!_kvtr.EraseCurrent(keyBytes))
        {
            var sk = _relationInfo.ClientRelationVersionInfo.SecondaryKeys[skKey];
            throw new BTDBException(
                $"Error in removing secondary indexes, previous index entry not found. {_relationInfo.Name}:{sk.Name} PK:{BitConverter.ToString(primaryKey.ToArray()).Replace("-", "")}");
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public int Count => (int)_kvtr.GetKeyValueCount(_relationInfo.Prefix);

    public Type BtdbInternalGetRelationInterfaceType()
    {
        return _relationInfo.InterfaceType!;
    }

    public IRelation? BtdbInternalNextInChain { get; set; }
}

ref struct SortNativeStorage
{
    internal ulong StartKeyIndex = 0;
    internal SpanWriter Writer;
    internal StructList<IntPtr> Storage;
    internal StructList<IntPtr> Items;
    internal Span<byte> FreeSpace;
    internal ReadOnlySpan<byte> First;
    internal uint AllocSize;
    internal bool OnlyFirst;

    public SortNativeStorage(bool onlyFirst)
    {
        OnlyFirst = onlyFirst;
        AllocSize = 256 * 1024u;
        Storage = new();
        Items = new();
        FreeSpace = new();
        Writer = new();
        First = new();
    }

    internal unsafe void AllocChunk()
    {
        Storage.Add(IntPtr.Zero);
        var newChunk = (IntPtr)NativeMemory.Alloc(AllocSize);
        Storage.Last = newChunk;
        FreeSpace = new((void*)newChunk, (int)AllocSize);
    }

    internal void StartNewItem()
    {
        if (OnlyFirst)
        {
            return;
        }

        if (FreeSpace.Length < 128) AllocChunk();
        Writer = new(FreeSpace);
        Writer.WriteInt32(0); // Space for length
    }

    internal unsafe void FinishNewItem(ulong keyIndex)
    {
        var endOfData = Writer.GetCurrentPosition();
        Writer.WriteVUInt64(keyIndex - StartKeyIndex);
        var lenOfKeyIndex = Writer.GetCurrentPosition() - endOfData;
        Writer.WriteUInt8((byte)lenOfKeyIndex);
        var span = Writer.GetSpan();
        if (OnlyFirst)
        {
            if (First.IsEmpty)
            {
                First = Writer.GetPersistentSpanAndReset();
            }
            else if (First.SequenceCompareTo(Writer.GetSpan()) > 0)
            {
                First = Writer.GetPersistentSpanAndReset();
            }
            else
            {
                Writer.Reset();
            }

            return;
        }

        if (Writer.HeapBuffer != null) // If it didn't fit free space in last chunk
        {
            while (span.Length >= AllocSize) AllocSize *= 2;
            if (AllocSize > int.MaxValue) AllocSize = int.MaxValue;
            AllocChunk();
            span.CopyTo(FreeSpace);
        }

        var startPtr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(FreeSpace));
        Unsafe.Write(startPtr, span.Length);
        Items.Add((IntPtr)startPtr);
        FreeSpace = FreeSpace[(int)((span.Length + 3u) & ~3u)..];
    }

    internal void Sort()
    {
        Items.AsSpan().Sort(SortNativeStorageComparator.Comparator);
    }

    internal unsafe ulong GetKeyIndex(int idx)
    {
        var ptr = Items[idx].ToPointer();
        var len = Unsafe.Read<int>(ptr);
        var lenDelta = Unsafe.Read<byte>((byte*)ptr + len - 1);
        var delta = PackUnpack.UnsafeUnpackVUInt(ref Unsafe.AsRef<byte>((byte*)ptr + len - 1 - lenDelta), lenDelta);
        return StartKeyIndex + delta;
    }

    public unsafe ulong GetFirstKeyIndex()
    {
        var ptr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(First));
        var len = First.Length;
        var lenDelta = Unsafe.Read<byte>((byte*)ptr + len - 1);
        var delta = PackUnpack.UnsafeUnpackVUInt(ref Unsafe.AsRef<byte>((byte*)ptr + len - 1 - lenDelta), lenDelta);
        return StartKeyIndex + delta;
    }

    public unsafe void Dispose()
    {
        foreach (var ptr in Storage)
        {
            NativeMemory.Free(ptr.ToPointer());
        }
    }
}

static class SortNativeStorageComparator
{
    internal static unsafe int Comparator(IntPtr a, IntPtr b)
    {
        var alen = Unsafe.Read<int>(a.ToPointer()) - 4;
        var blen = Unsafe.Read<int>(b.ToPointer()) - 4;
        var aspan = MemoryMarshal.CreateSpan(ref Unsafe.Add(ref Unsafe.AsRef<byte>(a.ToPointer()), 4), alen);
        var bspan = MemoryMarshal.CreateSpan(ref Unsafe.Add(ref Unsafe.AsRef<byte>(b.ToPointer()), 4), blen);
        return aspan.SequenceCompareTo(bspan);
    }
}
