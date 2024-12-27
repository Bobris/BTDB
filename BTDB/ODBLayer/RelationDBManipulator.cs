using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public interface IRelationDbManipulator : IRelation
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

    ReadOnlySpan<byte> ValueBytes(T obj, scoped ref MemWriter writer)
    {
        writer.WriteVUInt32(_relationInfo.ClientTypeVersion);
        _relationInfo.ValueSaver(_transaction, ref writer, obj);
        return writer.GetScopedSpanAndReset();
    }

    ReadOnlySpan<byte> KeyBytes(T obj, scoped ref MemWriter writer, out int lenOfPkWoInKeyValues)
    {
        WriteRelationPKPrefix(ref writer);
        lenOfPkWoInKeyValues = _relationInfo.PrimaryKeysSaver(_transaction, ref writer, obj);
        return writer.GetScopedSpanAndReset();
    }

    public void WriteRelationPKPrefix(ref MemWriter writer)
    {
        writer.WriteBlock(_relationInfo.Prefix);
    }

    public void WriteRelationSKPrefix(ref MemWriter writer, uint remappedSecondaryKeyIndex)
    {
        writer.WriteBlock(_relationInfo.PrefixSecondary);
        writer.WriteUInt8((byte)remappedSecondaryKeyIndex);
    }

    public uint RemapPrimeSK(uint primeSecondaryKeyIndex)
    {
        return _relationInfo.PrimeSK2Real![primeSecondaryKeyIndex];
    }

    readonly bool _hasSecondaryIndexes;

    [SkipLocalsInit]
    public bool Insert(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        var keyBytes = KeyBytes(obj, ref writer, out var lenOfPkWoInKeyValues);
        using var cursor = _kvtr.CreateCursor();

        if (lenOfPkWoInKeyValues > 0)
        {
            if (cursor.FindFirstKey(keyBytes[..lenOfPkWoInKeyValues]))
                return false;
        }
        else
        {
            if (cursor.FindExactKey(keyBytes))
                return false;
        }

        var valueBytes = ValueBytes(obj, ref writer);

        cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);

        if (_hasSecondaryIndexes)
        {
            writer = MemWriter.CreateFromStackAllocatedSpan(buf);
            AddIntoSecondaryIndexes(obj, ref writer);
        }

        return true;
    }

    [SkipLocalsInit]
    public unsafe bool Upsert(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        var keyBytes = KeyBytes(obj, ref writer, out var lenOfPkWoInKeyValues);
        var valueBytes = ValueBytes(obj, ref writer);
        using var cursor = _kvtr.CreateCursor();

        var update = false;
        if (lenOfPkWoInKeyValues > 0)
        {
            var updateSuffixResult = cursor.UpdateKeySuffix(keyBytes, (uint)lenOfPkWoInKeyValues);
            IfNotUniquePrefixThrow(updateSuffixResult);
            if (updateSuffixResult is UpdateKeySuffixResult.Updated or UpdateKeySuffixResult.NothingToDo) update = true;
        }
        else
        {
            if (cursor.FindExactKey(keyBytes)) update = true;
        }

        if (update)
        {
            var buffer = new Span<byte>((void*)writer.Current, (int)(writer.End - writer.Current));
            var oldValueBytes = cursor.GetValueSpan(ref buffer, true);

            cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
            {
                var writer2 = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[1024]);
                UpdateSecondaryIndexes(obj, cursor, keyBytes, oldValueBytes, ref writer2);
            }

            FreeContentInUpdate(oldValueBytes, valueBytes);
            return false;
        }

        cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        if (_hasSecondaryIndexes)
        {
            writer = MemWriter.CreateFromStackAllocatedSpan(buf);
            AddIntoSecondaryIndexes(obj, ref writer);
        }

        return true;
    }

    [SkipLocalsInit]
    public (bool Inserted, uint KeySize, uint OldValueSize, uint NewValueSize) ShallowUpsertWithSizes(T obj,
        bool allowInsert, bool allowUpdate)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        var keyBytes = KeyBytes(obj, ref writer, out var lenOfPkWoInKeyValues);
        using var cursor = _kvtr.CreateCursor();
        var update = false;
        if (lenOfPkWoInKeyValues > 0)
        {
            if (!allowUpdate)
            {
                if (cursor.FindFirstKey(keyBytes[..lenOfPkWoInKeyValues]))
                {
                    var oldSize = cursor.GetStorageSizeOfCurrentKey();
                    return (false, oldSize.Key, oldSize.Value, oldSize.Value);
                }
            }

            var updateSuffixResult = cursor.UpdateKeySuffix(keyBytes, (uint)lenOfPkWoInKeyValues);
            IfNotUniquePrefixThrow(updateSuffixResult);
            if (updateSuffixResult is UpdateKeySuffixResult.Updated or UpdateKeySuffixResult.NothingToDo) update = true;
        }
        else
        {
            if (cursor.FindExactKey(keyBytes)) update = true;
        }

        if (update)
        {
            var oldValueSize = cursor.GetStorageSizeOfCurrentKey().Value;
            if (allowUpdate)
            {
                var valueBytes = ValueBytes(obj, ref writer);
                cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);
                return (false, (uint)keyBytes.Length, oldValueSize, (uint)valueBytes.Length);
            }

            return (false, (uint)keyBytes.Length, oldValueSize, oldValueSize);
        }

        if (allowInsert)
        {
            var valueBytes = ValueBytes(obj, ref writer);
            cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);
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
    public unsafe bool ShallowUpsert(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        var keyBytes = KeyBytes(obj, ref writer, out var lenOfPkWoInKeyValues);
        var valueBytes = ValueBytes(obj, ref writer);
        using var cursor = _kvtr.CreateCursor();

        if (_hasSecondaryIndexes)
        {
            var update = false;
            if (lenOfPkWoInKeyValues > 0)
            {
                var updateSuffixResult = cursor.UpdateKeySuffix(keyBytes, (uint)lenOfPkWoInKeyValues);
                IfNotUniquePrefixThrow(updateSuffixResult);
                if (updateSuffixResult is UpdateKeySuffixResult.Updated or UpdateKeySuffixResult.NothingToDo)
                    update = true;
            }
            else
            {
                if (cursor.FindExactKey(keyBytes)) update = true;
            }

            if (update)
            {
                var buffer = new Span<byte>((void*)writer.Current, (int)(writer.End - writer.Current));
                var oldValueBytes = cursor.GetValueSpan(ref buffer, true);

                cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);

                Span<byte> buf2 = stackalloc byte[512];
                var writer2 = MemWriter.CreateFromStackAllocatedSpan(buf2);
                UpdateSecondaryIndexes(obj, cursor, keyBytes, oldValueBytes, ref writer2);

                return false;
            }

            cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);
            writer = MemWriter.CreateFromStackAllocatedSpan(buf);
            AddIntoSecondaryIndexes(obj, ref writer);
        }
        else
        {
            if (lenOfPkWoInKeyValues > 0)
            {
                var updateSuffixResult = cursor.UpdateKeySuffix(keyBytes, (uint)lenOfPkWoInKeyValues);
                IfNotUniquePrefixThrow(updateSuffixResult);
            }

            if (!cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes))
            {
                return false;
            }
        }

        return true;
    }

    void IfNotUniquePrefixThrow(UpdateKeySuffixResult updateSuffixResult)
    {
        if (updateSuffixResult is UpdateKeySuffixResult.NotUniquePrefix)
            throw new BTDBException("Relation " + _relationInfo.Name + " upsert failed due to not unique PK prefix");
    }

    [SkipLocalsInit]
    public unsafe void Update(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        var keyBytes = KeyBytes(obj, ref writer, out var lenOfPkWoInKeyValues);
        var valueBytes = ValueBytes(obj, ref writer);
        using var cursor = _kvtr.CreateCursor();

        if (lenOfPkWoInKeyValues > 0)
        {
            if (!cursor.FindFirstKey(keyBytes[..lenOfPkWoInKeyValues]))
                throw new BTDBException("Not found record to update.");
            var updateSuffixResult = cursor.UpdateKeySuffix(keyBytes, (uint)lenOfPkWoInKeyValues);
            IfNotUniquePrefixThrow(updateSuffixResult);
            Debug.Assert(updateSuffixResult != UpdateKeySuffixResult.NotFound);
        }
        else
        {
            if (!cursor.FindExactKey(keyBytes))
                throw new BTDBException("Not found record to update.");
        }

        var buffer = new Span<byte>((void*)writer.Current, (int)(writer.End - writer.Current));
        var oldValueBytes = cursor.GetValueSpan(ref buffer, true);
        cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);

        if (_hasSecondaryIndexes)
        {
            Span<byte> buf2 = stackalloc byte[1024];
            var writer2 = MemWriter.CreateFromStackAllocatedSpan(buf2);
            UpdateSecondaryIndexes(obj, cursor, keyBytes, oldValueBytes, ref writer2);
        }

        FreeContentInUpdate(oldValueBytes, valueBytes);
    }

    [SkipLocalsInit]
    public unsafe void ShallowUpdate(T obj)
    {
        Debug.Assert(typeof(T) == obj.GetType(), AssertNotDerivedTypesMsg);

        Span<byte> buf = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        var keyBytes = KeyBytes(obj, ref writer, out var lenOfPkWoInKeyValues);
        var valueBytes = ValueBytes(obj, ref writer);
        using var cursor = _kvtr.CreateCursor();

        if (lenOfPkWoInKeyValues > 0)
        {
            if (!cursor.FindFirstKey(keyBytes[..lenOfPkWoInKeyValues]))
                throw new BTDBException("Not found record to update.");
            var updateSuffixResult = cursor.UpdateKeySuffix(keyBytes, (uint)lenOfPkWoInKeyValues);
            IfNotUniquePrefixThrow(updateSuffixResult);
            Debug.Assert(updateSuffixResult != UpdateKeySuffixResult.NotFound);
        }
        else
        {
            if (!cursor.FindExactKey(keyBytes))
                throw new BTDBException("Not found record to update.");
        }

        if (_hasSecondaryIndexes)
        {
            var buffer = new Span<byte>((void*)writer.Current, (int)(writer.End - writer.Current));
            var oldValueBytes = cursor.GetValueSpan(ref buffer, true);
            cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);

            if (_hasSecondaryIndexes)
            {
                var writer2 = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[1024]);
                UpdateSecondaryIndexes(obj, cursor, keyBytes, oldValueBytes, ref writer2);
            }
        }
        else
        {
            cursor.CreateOrUpdateKeyValue(keyBytes, valueBytes);
        }
    }

    [SkipLocalsInit]
    public bool UpdateByIdInKeyValues(ReadOnlySpan<byte> keyBytes, int lenOfPkWoInKeyValues, bool throwIfNotFound)
    {
        using var cursor = _kvtr.CreateCursor();
        var updateKeySuffixResult = cursor.UpdateKeySuffix(keyBytes, (uint)lenOfPkWoInKeyValues);
        IfNotUniquePrefixThrow(updateKeySuffixResult);
        if (updateKeySuffixResult == UpdateKeySuffixResult.NotFound)
        {
            if (throwIfNotFound)
                throw new BTDBException("Not found record to update.");
            return false;
        }

        return true;
    }

    [SkipLocalsInit]
    public unsafe bool UpdateByIdStart(ReadOnlySpan<byte> keyBytes, ref MemWriter writer,
        ref ReadOnlySpan<byte> valueBytes,
        int lenOfPkWoInKeyValues, bool throwIfNotFound)
    {
        using var cursor = _kvtr.CreateCursor();
        if (lenOfPkWoInKeyValues > 0)
        {
            var updateKeySuffixResult = cursor.UpdateKeySuffix(keyBytes, (uint)lenOfPkWoInKeyValues);
            IfNotUniquePrefixThrow(updateKeySuffixResult);
            if (updateKeySuffixResult == UpdateKeySuffixResult.NotFound)
            {
                if (throwIfNotFound)
                    throw new BTDBException("Not found record to update.");
                return false;
            }
        }
        else
        {
            if (!cursor.FindExactKey(keyBytes))
            {
                if (throwIfNotFound)
                    throw new BTDBException("Not found record to update.");
                return false;
            }
        }

        var buffer = new Span<byte>(Unsafe.AsPointer(ref MemoryMarshal.GetReference(valueBytes)), valueBytes.Length);
        valueBytes = cursor.GetValueSpan(ref buffer, true);

        var version = (uint)PackUnpack.UnpackVUInt(valueBytes);

        var currentVersion = _relationInfo.ClientTypeVersion;
        if (version != currentVersion)
        {
            var itemLoader = _relationInfo.ItemLoaderInfos[0];
            fixed (void* _ = keyBytes)
            fixed (void* __ = valueBytes)
            {
                var reader = MemReader.CreateFromPinnedSpan(keyBytes[_relationInfo.Prefix.Length..]);
                var obj = (T)itemLoader._primaryKeysLoader(_transaction, ref reader);
                var valueReader = MemReader.CreateFromPinnedSpan(valueBytes);
                valueReader.SkipVUInt32();
                itemLoader.GetValueLoader(version)(_transaction, ref valueReader, obj);
                valueBytes = ValueBytes(obj, ref writer);
            }
        }

        writer.WriteVUInt32(currentVersion);
        return true;
    }

    [SkipLocalsInit]
    public void UpdateByIdFinish(ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> oldValueBytes,
        ReadOnlySpan<byte> newValueBytes)
    {
        using var cursor = _kvtr.CreateCursor();
        cursor.CreateOrUpdateKeyValue(keyBytes, newValueBytes);

        if (_hasSecondaryIndexes)
        {
            UpdateSecondaryIndexes(keyBytes, oldValueBytes, newValueBytes);
        }
    }

    public bool Contains(in ReadOnlySpan<byte> keyBytes)
    {
        using var cursor = _kvtr.CreateCursor();
        return cursor.FindFirstKey(keyBytes);
    }

    void CompareAndRelease(List<ulong> oldItems, List<ulong> newItems,
        Action<IInternalObjectDBTransaction, ulong> freeAction)
    {
        if (newItems.Count == 0)
        {
            foreach (var id in oldItems)
                freeAction(_transaction, id);
        }
        else if (newItems.Count < 16)
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
    // ReSharper disable once UnusedMember.Global
    public unsafe bool RemoveById(scoped in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound)
    {
        using var cursor = _kvtr.CreateCursor();
        Span<byte> keyBufferScoped = stackalloc byte[1024];
        Span<byte> keyBuffer =
            new Span<byte>(Unsafe.AsPointer(ref keyBufferScoped.GetPinnableReference()), keyBytes.Length);
        Span<byte> valueBuffer = stackalloc byte[1024];
        var fullKeyBytes = keyBytes;
        var beforeRemove = _relationInfo.BeforeRemove;
        if (beforeRemove != null)
        {
            if (!cursor.FindFirstKey(keyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            if (_relationInfo.HasInKeyValue)
            {
                fullKeyBytes = cursor.GetKeySpan(keyBuffer);
            }

            var obj = _relationInfo.ItemLoaderInfos[0].CreateInstance(_transaction, cursor, fullKeyBytes);
            if (beforeRemove(_transaction, _transaction.Owner.ActualOptions.Container!, obj))
                return false;
        }
        else
        {
            if (_relationInfo.HasInKeyValue)
            {
                if (!cursor.FindFirstKey(keyBytes))
                {
                    if (throwWhenNotFound)
                        throw new BTDBException("Not found record to delete.");
                    return false;
                }

                fullKeyBytes = cursor.GetKeySpan(keyBuffer);
            }
        }

        if (!cursor.FindExactKey(fullKeyBytes))
        {
            if (throwWhenNotFound)
                throw new BTDBException("Not found record to delete.");
            return false;
        }

        var valueSpan = cursor.GetValueSpan(ref valueBuffer, true);

        cursor.EraseCurrent();

        if (_hasSecondaryIndexes)
        {
            RemoveSecondaryIndexes(cursor, fullKeyBytes, valueSpan);
        }

        _relationInfo.FreeContent(_transaction, valueSpan);

        return true;
    }

    [SkipLocalsInit]
    public bool RemoveByIdWithFullKey(in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound)
    {
        using var cursor = _kvtr.CreateCursor();
        Span<byte> valueBuffer = stackalloc byte[512];

        var beforeRemove = _relationInfo.BeforeRemove;
        if (beforeRemove != null)
        {
            if (!cursor.FindExactKey(keyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            var obj = _relationInfo.ItemLoaderInfos[0].CreateInstance(_transaction, cursor, keyBytes);
            if (beforeRemove(_transaction, _transaction.Owner.ActualOptions.Container!, obj))
                return false;
        }

        if (!cursor.FindExactKey(keyBytes))
        {
            if (throwWhenNotFound)
                throw new BTDBException("Not found record to delete.");
            return false;
        }

        var value = cursor.GetValueSpan(ref valueBuffer, true);

        cursor.EraseCurrent();
        if (_hasSecondaryIndexes)
        {
            RemoveSecondaryIndexes(cursor, keyBytes, value);
        }

        _relationInfo.FreeContent(_transaction, value);
        return true;
    }

    [SkipLocalsInit]
    // ReSharper disable once UnusedMember.Global
    public unsafe bool ShallowRemoveById(in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound)
    {
        var beforeRemove = _relationInfo.BeforeRemove;
        ReadOnlySpan<byte> fullKeyBytes = keyBytes;
        using var cursor = _kvtr.CreateCursor();
        Span<byte> keyBufferScoped = stackalloc byte[1024];
        Span<byte> keyBuffer =
            new Span<byte>(Unsafe.AsPointer(ref keyBufferScoped.GetPinnableReference()), keyBytes.Length);
        if (beforeRemove != null)
        {
            if (!cursor.FindFirstKey(keyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            if (_relationInfo.HasInKeyValue)
            {
                fullKeyBytes = cursor.GetKeySpan(keyBuffer);
            }

            var obj = _relationInfo.ItemLoaderInfos[0].CreateInstance(_transaction, cursor, fullKeyBytes);
            if (beforeRemove(_transaction, _transaction.Owner.ActualOptions.Container!, obj))
                return false;
        }
        else
        {
            if (_relationInfo.HasInKeyValue)
            {
                if (!cursor.FindFirstKey(keyBytes))
                {
                    if (throwWhenNotFound)
                        throw new BTDBException("Not found record to delete.");
                    return false;
                }

                fullKeyBytes = cursor.GetKeySpan(keyBuffer);
            }
        }

        if (_hasSecondaryIndexes)
        {
            if (!cursor.FindExactKey(fullKeyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            Span<byte> valueBuffer = stackalloc byte[1024];
            var value = cursor.GetValueSpan(ref valueBuffer, true);

            cursor.EraseCurrent();
            RemoveSecondaryIndexes(cursor, fullKeyBytes, value);
        }
        else
        {
            if (!cursor.FindExactKey(fullKeyBytes))
            {
                if (throwWhenNotFound)
                    throw new BTDBException("Not found record to delete.");
                return false;
            }

            cursor.EraseCurrent();
        }

        return true;
    }

    [SkipLocalsInit]
    public int RemoveByPrimaryKeyPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
    {
        Span<byte> buf = stackalloc byte[4096];
        Span<byte> keyBuffer = stackalloc byte[1024];
        var needImplementFreeContent = _relationInfo.NeedImplementFreeContent();
        using var enumerator = new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix, 0)
            .GetEnumerator();
        var beforeRemove = _relationInfo.BeforeRemove;
        var removed = 0;
        while (enumerator.MoveNext())
        {
            if (beforeRemove != null)
            {
                var obj = enumerator.Current!;
                if (beforeRemove(_transaction, _transaction.Owner.ActualOptions.Container!, obj))
                    continue;
            }

            if (_hasSecondaryIndexes || needImplementFreeContent)
            {
                var valueBytes = ((RelationPrimaryKeyEnumerator<T>)enumerator).Cursor.GetValueSpan(ref buf, true);

                if (_hasSecondaryIndexes)
                {
                    using var tempCursor = _kvtr.CreateCursor();
                    RemoveSecondaryIndexes(tempCursor,
                        ((RelationPrimaryKeyEnumerator<T>)enumerator).Cursor.GetKeySpan(ref keyBuffer, true),
                        valueBytes);
                }

                if (needImplementFreeContent)
                    _relationInfo.FreeContent(_transaction, valueBytes);
            }

            if (beforeRemove != null)
            {
                ((RelationPrimaryKeyEnumerator<T>)enumerator).Cursor.EraseCurrent();
            }

            removed++;
        }

        if (beforeRemove != null)
        {
            return removed;
        }

        return RemovePrimaryKeysByPrefix(keyBytesPrefix);
    }

    [SkipLocalsInit]
    public int RemoveByPrimaryKeyPrefixPartial(in ReadOnlySpan<byte> keyBytesPrefix, int maxCount)
    {
        Span<byte> buf = stackalloc byte[4096];
        Span<byte> keyBuffer = stackalloc byte[1024];
        var needImplementFreeContent = _relationInfo.NeedImplementFreeContent();
        using var enumerator = new RelationPrimaryKeyEnumerator<T>(_transaction, _relationInfo, keyBytesPrefix, 0)
            .GetEnumerator();
        var beforeRemove = _relationInfo.BeforeRemove;
        var removed = 0;
        var idx = 0;
        while (enumerator.MoveNext())
        {
            idx++;
            if (idx > maxCount)
                break;
            if (beforeRemove != null)
            {
                var obj = enumerator.Current!;
                if (beforeRemove(_transaction, _transaction.Owner.ActualOptions.Container!, obj))
                    continue;
            }

            if (_hasSecondaryIndexes || needImplementFreeContent)
            {
                var valueBytes = ((RelationPrimaryKeyEnumerator<T>)enumerator).Cursor.GetValueSpan(ref buf, true);

                if (_hasSecondaryIndexes)
                {
                    var tempCursor = _kvtr.CreateCursor();
                    RemoveSecondaryIndexes(tempCursor,
                        ((RelationPrimaryKeyEnumerator<T>)enumerator).Cursor.GetKeySpan(ref keyBuffer), valueBytes);
                }

                if (needImplementFreeContent)
                    _relationInfo.FreeContent(_transaction, valueBytes);
            }

            ((RelationPrimaryKeyEnumerator<T>)enumerator).Cursor.EraseCurrent();
            removed++;
        }

        return removed;
    }

    [SkipLocalsInit]
    public int RemoveByKeyPrefixWithoutIterate(in ReadOnlySpan<byte> keyBytesPrefix)
    {
        if (_relationInfo.NeedImplementFreeContent())
        {
            return RemoveByPrimaryKeyPrefix(keyBytesPrefix);
        }

        if (_hasSecondaryIndexes)
        {
            using var cursor = _kvtr.CreateCursor();
            //keyBytePrefix contains [3, Index Relation, Primary key prefix] we need
            //                       [4, Index Relation, Secondary Key Index, Primary key prefix]
            var idBytesLength = 1 + PackUnpack.LengthVUInt(_relationInfo.Id);
            Span<byte> buf = stackalloc byte[1024];
            var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
            foreach (var secKey in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                WriteRelationSKPrefix(ref writer, secKey.Key);
                writer.WriteBlock(keyBytesPrefix[(int)idBytesLength..]);
                cursor.EraseAll(writer.GetSpan());
                writer.Reset();
            }
        }

        return RemovePrimaryKeysByPrefix(keyBytesPrefix);
    }

    int RemovePrimaryKeysByPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
    {
        using var cursor = _kvtr.CreateCursor();
        return (int)cursor.EraseAll(keyBytesPrefix);
    }

    [SkipLocalsInit]
    public void RemoveAll()
    {
        if (_relationInfo.BeforeRemove != null)
        {
            RemoveByPrimaryKeyPrefix(_relationInfo.Prefix);
            return;
        }

        using var cursor = _kvtr.CreateCursor();
        if (_relationInfo.NeedImplementFreeContent())
        {
            Span<byte> buf = stackalloc byte[4096];
            while (cursor.FindNextKey(_relationInfo.Prefix))
            {
                var valueBytes = cursor.GetValueSpan(ref buf);
                _relationInfo.FreeContent(_transaction, valueBytes);
            }
        }

        cursor.EraseAll(_relationInfo.Prefix);
        if (_hasSecondaryIndexes)
        {
            cursor.EraseAll(_relationInfo.PrefixSecondary);
        }
    }

    public long CountWithPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
    {
        using var cursor = _kvtr.CreateCursor();
        return cursor.GetKeyValueCount(keyBytesPrefix);
    }

    public bool AnyWithPrefix(in ReadOnlySpan<byte> keyBytesPrefix)
    {
        using var cursor = _kvtr.CreateCursor();
        return cursor.FindFirstKey(keyBytesPrefix);
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
        using var cursor = _kvtr.CreateCursor();
        if (!cursor.FindFirstKey(startKeyBytes.Slice(0, prefixLen)))
            return 0;

        var prefixIndex = cursor.GetKeyIndex();

        var realEndKeyBytes = endKeyBytes;
        if (endKeyProposition == KeyProposition.Included)
            realEndKeyBytes =
                RelationAdvancedEnumerator<T>.FindLastKeyWithPrefix(endKeyBytes, cursor);

        long startIndex;
        long endIndex;
        if (endKeyProposition == KeyProposition.Ignored)
        {
            cursor.FindLastKey(startKeyBytes.Slice(0, prefixLen));

            endIndex = cursor.GetKeyIndex() - prefixIndex;
        }
        else
        {
            switch (cursor.Find(realEndKeyBytes, (uint)prefixLen))
            {
                case FindResult.Exact:
                    endIndex = cursor.GetKeyIndex() - prefixIndex;
                    if (endKeyProposition == KeyProposition.Excluded)
                    {
                        endIndex--;
                    }

                    break;
                case FindResult.Previous:
                    endIndex = cursor.GetKeyIndex() - prefixIndex;
                    break;
                case FindResult.Next:
                    endIndex = cursor.GetKeyIndex() - prefixIndex - 1;
                    break;
                case FindResult.NotFound:
                    throw new InvalidOperationException();
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
            switch (cursor.Find(startKeyBytes, (uint)prefixLen))
            {
                case FindResult.Exact:
                    startIndex = cursor.GetKeyIndex() - prefixIndex;
                    if (startKeyProposition == KeyProposition.Excluded)
                    {
                        startIndex++;
                    }

                    break;
                case FindResult.Previous:
                    startIndex = cursor.GetKeyIndex() - prefixIndex + 1;
                    break;
                case FindResult.Next:
                    startIndex = cursor.GetKeyIndex() - prefixIndex;
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

    public unsafe int RemoveByIdAdvancedParam(EnumerationOrder order,
        KeyProposition startKeyProposition, int prefixLen, in ReadOnlySpan<byte> startKeyBytes,
        KeyProposition endKeyProposition, in ReadOnlySpan<byte> endKeyBytes)
    {
        using var enumerator = new RelationAdvancedEnumerator<T>(this,
            order, startKeyProposition, prefixLen, startKeyBytes, endKeyProposition, endKeyBytes, 0).GetEnumerator();
        var count = 0;
        var cursor = ((RelationAdvancedEnumerator<T>)enumerator).Cursor;
        Span<byte> keyBufferScoped = stackalloc byte[1024];
        Span<byte> valueBuffer = stackalloc byte[1024];
        while (enumerator.MoveNext())
        {
            var fullKeyBytes = cursor.GetKeySpan(ref keyBufferScoped);
            var beforeRemove = _relationInfo.BeforeRemove;
            if (beforeRemove != null)
            {
                var obj = _relationInfo.ItemLoaderInfos[0].CreateInstance(_transaction, cursor, fullKeyBytes);
                if (beforeRemove(_transaction, _transaction.Owner.ActualOptions.Container!, obj))
                    continue;
            }

            var valueSpan = cursor.GetValueSpan(ref valueBuffer, true);

            cursor.EraseCurrent();

            if (_hasSecondaryIndexes)
            {
                RemoveSecondaryIndexes(cursor, fullKeyBytes, valueSpan);
            }

            _relationInfo.FreeContent(_transaction, valueSpan);
            count++;
        }

        return count;
    }

    public IEnumerator<T> GetEnumerator()
    {
        return new RelationEnumerator<T>(_transaction, _relationInfo, _relationInfo.Prefix, 0).GetEnumerator();
    }

    public IEnumerable<TAs> As<TAs>()
    {
        var loaderInfo = new RelationInfo.ItemLoaderInfo(_relationInfo, typeof(TAs));
        return new RelationEnumerator<TAs>(_transaction, _relationInfo.Prefix, loaderInfo);
    }

    public TItem FindByIdOrDefault<TItem>(in ReadOnlySpan<byte> keyBytes, bool throwWhenNotFound, int loaderIndex)
    {
        return (TItem)FindByIdOrDefaultInternal(_relationInfo.ItemLoaderInfos[loaderIndex], keyBytes,
            throwWhenNotFound);
    }

    [SkipLocalsInit]
    object? FindByIdOrDefaultInternal(RelationInfo.ItemLoaderInfo itemLoader, in ReadOnlySpan<byte> keyBytes,
        bool throwWhenNotFound)
    {
        using var cursor = _kvtr.CreateCursor();
        if (!cursor.FindFirstKey(keyBytes))
        {
            if (throwWhenNotFound)
                throw new BTDBException("Not found.");
            return default;
        }

        if (_relationInfo.HasInKeyValue)
        {
            return itemLoader.CreateInstance(_transaction, cursor, cursor.GetKeySpan(stackalloc byte[1024]));
        }

        return itemLoader.CreateInstance(_transaction, cursor, keyBytes);
    }

    public IEnumerator<TItem> FindByPrimaryKeyPrefix<TItem>(in ReadOnlySpan<byte> keyBytesPrefix, int loaderIndex)
    {
        return new RelationPrimaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytesPrefix, loaderIndex);
    }

    public TItem FirstByPrimaryKey<TItem>(int loaderIndex, ConstraintInfo[] constraints, ICollection<TItem> target,
        IOrderer[]? orderers, bool hasOrDefault) where TItem : class
    {
        var keyBytes = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        keyBytes.WriteBlock(_relationInfo.Prefix);

        var relationVersionInfo = _relationInfo.ClientRelationVersionInfo;
        var primaryKeyFields = relationVersionInfo.PrimaryKeyFields.Span;

        Span<byte> buffer = stackalloc byte[4096];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buffer);
        if (orderers == null || orderers.Length == 0)
        {
            using var enumerator = new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes,
                writer,
                loaderIndex, constraints);

            if (enumerator.MoveNext())
            {
                return enumerator.Current!;
            }

            ThrowIfNotHasOrDefault(hasOrDefault);
            return null!;
        }
        else
        {
            var ordererIdxs = PrepareOrderers(ref constraints, orderers, primaryKeyFields);

            using var enumerator = new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes,
                writer,
                loaderIndex, constraints);

            var sns = new SortNativeStorage(true);
            try
            {
                enumerator.GatherForSorting(ref sns, ordererIdxs, orderers);
                if (sns.First.Start == 0)
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

    [SkipLocalsInit]
    public TItem FirstBySecondaryKey<TItem>(int loaderIndex, ConstraintInfo[] constraints, uint secondaryKeyIndex,
        IOrderer[]? orderers, bool hasOrDefault) where TItem : class
    {
        var keyBytes = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        keyBytes.WriteBlock(_relationInfo.PrefixSecondary);
        var remappedSecondaryKeyIndex = RemapPrimeSK(secondaryKeyIndex);
        keyBytes.WriteUInt8((byte)remappedSecondaryKeyIndex);
        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        if (orderers == null || orderers.Length == 0)
        {
            using var enumerator = new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo,
                keyBytes, writer,
                loaderIndex, constraints, remappedSecondaryKeyIndex, this);

            if (enumerator.MoveNextInGather())
            {
                return enumerator.Current!;
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

            using var enumerator = new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo,
                keyBytes, writer,
                loaderIndex, constraints, remappedSecondaryKeyIndex, this);

            var sns = new SortNativeStorage(true);
            try
            {
                enumerator.GatherForSorting(ref sns, ordererIdxs, orderers);
                if (sns.First.Start == 0)
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

        if (_relationInfo.BeforeRemove != null)
        {
            throw new NotSupportedException("RemoveWithSizes does not support OnBeforeRemove");
        }

        MemWriter writer = new();
        MemWriter helperBuffer = new();
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

        using var cursor = _kvtr.CreateCursor();
        while (cursor.FindNextKey(keyPrefix))
        {
            var p = cursor.GetStorageSizeOfCurrentKey();
            keySizes += p.Key;
            valueSizes += p.Value;
            count++;
        }

        cursor.EraseAll(keyPrefix);
        return (count, keySizes, valueSizes);
    }

    [SkipLocalsInit]
    public ulong GatherByPrimaryKey<TItem>(int loaderIndex, ConstraintInfo[] constraints, ICollection<TItem> target,
        long skip, long take, IOrderer[]? orderers) where TItem : class
    {
        var keyBytes = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        keyBytes.WriteBlock(_relationInfo.Prefix);
        if (skip < 0)
        {
            take += skip;
            skip = 0;
        }

        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        var relationVersionInfo = _relationInfo.ClientRelationVersionInfo;
        var primaryKeyFields = relationVersionInfo.PrimaryKeyFields.Span;

        var firstInKeyValue = FindIndexOfFirstInKeyValue(constraints, primaryKeyFields);

        var fastIteration = IsFastIterable(constraints, firstInKeyValue);

        if (orderers == null || orderers.Length == 0)
        {
            using var enumerator = new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes,
                writer,
                loaderIndex, constraints);

            if (fastIteration)
            {
                return enumerator.FastGather(skip, take, target);
            }

            return enumerator.NormalGather(skip, take, target);
        }
        else
        {
            var ordererIdxs = PrepareOrderers(ref constraints, orderers, primaryKeyFields);

            using var enumerator = new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes,
                writer,
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

    static int FindIndexOfFirstInKeyValue(ConstraintInfo[] constraints, ReadOnlySpan<TableFieldInfo> primaryKeyFields)
    {
        var firstInKeyValue = constraints.Length;
        for (var i = 0; i < primaryKeyFields.Length; i++)
        {
            if (!primaryKeyFields[i].InKeyValue) continue;
            firstInKeyValue = i;
            break;
        }

        return firstInKeyValue;
    }

    static bool IsFastIterable(ConstraintInfo[] constraints, int firstInKeyValue)
    {
        var indexOfFirstAnyConstraint = -1;
        for (var i = 0; i < firstInKeyValue; i++)
        {
            if (constraints[i].Constraint.IsAnyConstraint())
            {
                if (indexOfFirstAnyConstraint == -1)
                    indexOfFirstAnyConstraint = i;
            }
            else if (constraints[i].Constraint.IsSimpleExact())
            {
                if (indexOfFirstAnyConstraint != -1)
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        return true;
    }

    [SkipLocalsInit]
    public ulong GatherBySecondaryKey<TItem>(int loaderIndex, ConstraintInfo[] constraints, ICollection<TItem> target,
        long skip, long take, uint secondaryKeyIndex, IOrderer[]? orderers) where TItem : class
    {
        var keyBytes = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        keyBytes.WriteBlock(_relationInfo.PrefixSecondary);
        var remappedSecondaryKeyIndex = RemapPrimeSK(secondaryKeyIndex);
        keyBytes.WriteUInt8((byte)remappedSecondaryKeyIndex);
        if (skip < 0)
        {
            take += skip;
            skip = 0;
        }

        var fastIteration = IsFastIterable(constraints, constraints.Length);

        var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        if (orderers == null || orderers.Length == 0)
        {
            using var enumerator = new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo,
                keyBytes, writer,
                loaderIndex, constraints, remappedSecondaryKeyIndex, this);

            if (fastIteration)
            {
                return enumerator.FastGather(skip, take, target);
            }

            return enumerator.NormalGather(skip, take, target);
        }
        else
        {
            var relationVersionInfo = _relationInfo.ClientRelationVersionInfo;
            var secondaryKeyInfo = relationVersionInfo.SecondaryKeys[remappedSecondaryKeyIndex];
            var fields = secondaryKeyInfo.Fields;
            var ordererIdxs = PrepareOrderersSK(ref constraints, orderers, fields, relationVersionInfo);

            using var enumerator = new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo,
                keyBytes, writer,
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
        where TItem : class
    {
        MemWriter keyBytes = new();
        keyBytes.WriteBlock(_relationInfo.Prefix);
        return new RelationConstraintEnumerator<TItem>(_transaction, _relationInfo, keyBytes, new MemWriter(),
            loaderIndex, constraints);
    }

    public IEnumerable<TItem> ScanBySecondaryKeyPrefix<TItem>(int loaderIndex, ConstraintInfo[] constraints,
        uint secondaryKeyIndex) where TItem : class
    {
        var keyBytes = new MemWriter();
        keyBytes.WriteBlock(_relationInfo.PrefixSecondary);
        var remappedSecondaryKeyIndex = RemapPrimeSK(secondaryKeyIndex);
        keyBytes.WriteUInt8((byte)remappedSecondaryKeyIndex);
        return new RelationConstraintSecondaryKeyEnumerator<TItem>(_transaction, _relationInfo, keyBytes,
            new MemWriter(),
            loaderIndex, constraints, remappedSecondaryKeyIndex, this);
    }

    [SkipLocalsInit]
    public unsafe object CreateInstanceFromSecondaryKey(RelationInfo.ItemLoaderInfo itemLoader,
        uint remappedSecondaryKeyIndex,
        in ReadOnlySpan<byte> secondaryKey)
    {
        var pkWriter = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
        WriteRelationPKPrefix(ref pkWriter);
        fixed (void* _ = secondaryKey)
        {
            var reader = MemReader.CreateFromPinnedSpan(secondaryKey);
            _relationInfo.GetSKKeyValueToPKMerger(remappedSecondaryKeyIndex)
                (ref reader, ref pkWriter);
        }

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
        using var cursor = _kvtr.CreateCursor();
        if (!cursor.FindFirstKey(secKeyBytes))
        {
            if (throwWhenNotFound)
                throw new BTDBException("Not found.");
            return default;
        }

        var keyBytes = cursor.GetKeySpan(stackalloc byte[4096]);

        if (cursor.FindNextKey(secKeyBytes))
            throw new BTDBException("Ambiguous result.");

        return (TItem)CreateInstanceFromSecondaryKey(_relationInfo.ItemLoaderInfos[loaderIndex],
            remappedSecondaryKeyIndex,
            keyBytes);
    }

    ReadOnlySpan<byte> WriteSecondaryKeyKey(uint remappedSecondaryKeyIndex, T obj, ref MemWriter writer)
    {
        var keySaver = _relationInfo.GetSecondaryKeysKeySaver(remappedSecondaryKeyIndex);
        WriteRelationSKPrefix(ref writer, remappedSecondaryKeyIndex);
        keySaver(_transaction, ref writer, obj); //secondary key
        return writer.GetSpan();
    }

    unsafe ReadOnlySpan<byte> WriteSecondaryKeyKey(uint remappedSecondaryKeyIndex, in ReadOnlySpan<byte> keyBytes,
        in ReadOnlySpan<byte> valueBytes)
    {
        var keyWriter = new MemWriter();
        WriteRelationSKPrefix(ref keyWriter, remappedSecondaryKeyIndex);

        var version = (uint)PackUnpack.UnpackVUInt(valueBytes);

        var keySaver = _relationInfo.GetPKValToSKMerger(version, remappedSecondaryKeyIndex);
        fixed (void* _ = keyBytes)
        fixed (void* __ = valueBytes)
        {
            var keyReader = MemReader.CreateFromPinnedSpan(keyBytes);
            var valueReader = MemReader.CreateFromPinnedSpan(valueBytes);
            keySaver(_transaction, ref keyWriter, ref keyReader, ref valueReader, _relationInfo.DefaultClientObject);
        }

        return keyWriter.GetSpan();
    }

    void AddIntoSecondaryIndexes(T obj, ref MemWriter writer)
    {
        foreach (var sk in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
        {
            using var cursor = _kvtr.CreateCursor();
            var keyBytes = WriteSecondaryKeyKey(sk.Key, obj, ref writer);
            cursor.CreateOrUpdateKeyValue(keyBytes, new());
            writer.Reset();
        }
    }

    [SkipLocalsInit]
    bool UpdateSecondaryIndexes(in ReadOnlySpan<byte> oldKey, in ReadOnlySpan<byte> oldValue,
        in ReadOnlySpan<byte> newValue)
    {
        var changed = false;
        using var cursor = _kvtr.CreateCursor();
        if (_relationInfo.ClientRelationVersionInfo.HasComputedField)
        {
            var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
            var writerOld = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
            var objOld = _relationInfo.ItemLoaderInfos[0].CreateInstance(_transaction, oldKey, oldValue);
            var objNew = _relationInfo.ItemLoaderInfos[0].CreateInstance(_transaction, oldKey, newValue);

            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                writer.Reset();
                writerOld.Reset();
                var newKeyBytes = WriteSecondaryKeyKey(key, Unsafe.As<T>(objNew), ref writer);
                var oldKeyBytes = WriteSecondaryKeyKey(key, Unsafe.As<T>(objOld), ref writerOld);
                if (oldKeyBytes.SequenceEqual(newKeyBytes))
                    continue;
                //remove old index
                EraseOldSecondaryKey(cursor, oldKey, oldKeyBytes, key);
                //insert new value
                cursor.CreateOrUpdateKeyValue(newKeyBytes, new());
                changed = true;
            }
        }
        else
        {
            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var newKeyBytes = WriteSecondaryKeyKey(key, oldKey, newValue);
                var oldKeyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
                if (oldKeyBytes.SequenceEqual(newKeyBytes))
                    continue;
                //remove old index
                EraseOldSecondaryKey(cursor, oldKey, oldKeyBytes, key);
                //insert new value
                cursor.CreateOrUpdateKeyValue(newKeyBytes, new());
                changed = true;
            }
        }

        return changed;
    }

    [SkipLocalsInit]
    bool UpdateSecondaryIndexes(T newValue, IKeyValueDBCursor cursor, in ReadOnlySpan<byte> oldKey,
        in ReadOnlySpan<byte> oldValue,
        ref MemWriter writer)
    {
        var changed = false;
        if (_relationInfo.ClientRelationVersionInfo.HasComputedField)
        {
            var writerOld = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
            var obj = _relationInfo.ItemLoaderInfos[0].CreateInstance(_transaction, oldKey, oldValue);
            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                writer.Reset();
                writerOld.Reset();
                var newKeyBytes = WriteSecondaryKeyKey(key, newValue, ref writer);
                var oldKeyBytes = WriteSecondaryKeyKey(key, Unsafe.As<T>(obj), ref writerOld);
                if (oldKeyBytes.SequenceEqual(newKeyBytes))
                    continue;
                //remove old index
                EraseOldSecondaryKey(cursor, oldKey, oldKeyBytes, key);
                //insert new value
                cursor.CreateOrUpdateKeyValue(newKeyBytes, new());
                changed = true;
            }
        }
        else
        {
            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                writer.Reset();
                var newKeyBytes = WriteSecondaryKeyKey(key, newValue, ref writer);
                var oldKeyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
                if (oldKeyBytes.SequenceEqual(newKeyBytes))
                    continue;
                //remove old index
                EraseOldSecondaryKey(cursor, oldKey, oldKeyBytes, key);
                //insert new value
                cursor.CreateOrUpdateKeyValue(newKeyBytes, new());
                changed = true;
            }
        }

        return changed;
    }

    [SkipLocalsInit]
    void RemoveSecondaryIndexes(IKeyValueDBCursor cursor, scoped in ReadOnlySpan<byte> oldKey,
        in ReadOnlySpan<byte> oldValue)
    {
        if (_relationInfo.ClientRelationVersionInfo.HasComputedField)
        {
            var writer = MemWriter.CreateFromStackAllocatedSpan(stackalloc byte[4096]);
            var obj = _relationInfo.ItemLoaderInfos[0].CreateInstance(_transaction, oldKey, oldValue);
            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                writer.Reset();
                var keyBytes = WriteSecondaryKeyKey(key, Unsafe.As<T>(obj), ref writer);
                EraseOldSecondaryKey(cursor, oldKey, keyBytes, key);
            }
        }
        else
        {
            foreach (var (key, _) in _relationInfo.ClientRelationVersionInfo.SecondaryKeys)
            {
                var keyBytes = WriteSecondaryKeyKey(key, oldKey, oldValue);
                EraseOldSecondaryKey(cursor, oldKey, keyBytes, key);
            }
        }
    }

    void EraseOldSecondaryKey(IKeyValueDBCursor cursor, in ReadOnlySpan<byte> primaryKey,
        in ReadOnlySpan<byte> keyBytes, uint skKey)
    {
        if (!cursor.FindExactKey(keyBytes))
        {
            var sk = _relationInfo.ClientRelationVersionInfo.SecondaryKeys[skKey];
            throw new BTDBException(
                $"Error in removing secondary indexes, previous index entry not found. {_relationInfo.Name}:{sk.Name} PK:{Convert.ToHexString(primaryKey)}");
        }

        cursor.EraseCurrent();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public int Count
    {
        get
        {
            using var cursor = _kvtr.CreateCursor();
            return (int)cursor.GetKeyValueCount(_relationInfo.Prefix);
        }
    }

    public Type BtdbInternalGetRelationInterfaceType()
    {
        return _relationInfo.InterfaceType!;
    }

    public IRelation? BtdbInternalNextInChain { get; set; }
}

ref struct SortNativeStorage
{
    internal ulong StartKeyIndex = 0;
    internal MemWriter Writer;
    internal StructList<IntPtr> Storage;
    internal StructList<IntPtr> Items;
    internal Span<byte> FreeSpace;
    internal MemReader First;
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
        Writer = MemWriter.CreateFromPinnedSpan(FreeSpace);
        Writer.WriteInt32LE(0); // Space for length
    }

    internal unsafe void FinishNewItem(ulong keyIndex)
    {
        var endOfData = Writer.GetCurrentPosition();
        Writer.WriteVUInt64(keyIndex - StartKeyIndex);
        var lenOfKeyIndex = Writer.GetCurrentPosition() - endOfData;
        Writer.WriteUInt8((byte)lenOfKeyIndex);
        if (OnlyFirst)
        {
            if (First.Start == 0)
            {
                MemReader.InitFromSpan(ref First, Writer.GetSpan());
            }
            else if (First.PeekSpanTillEof().SequenceCompareTo(Writer.GetSpan()) > 0)
            {
                MemReader.InitFromSpan(ref First, Writer.GetSpan());
            }

            Writer.Reset();
            return;
        }

        var span = Writer.GetSpan();
        if (Writer.Controller != null) // If it didn't fit free space in last chunk
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
        var ptr = First.Start;
        var len = First.End - ptr;
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
