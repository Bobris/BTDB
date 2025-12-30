using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.Collections;
using BTDB.IL;
using BTDB.Locks;
using BTDB.ODBLayer;

namespace BTDB.Serialization;

public static class ReflectionMetadata
{
    static readonly RefDictionary<nint, ClassMetadata> TypeToMetadata = new();
    static readonly SpanByteNoRemoveDictionary<ClassMetadata> NameToMetadata = new();

    static readonly RefDictionary<nint, CollectionMetadata> CollectionToMetadata = new();
    static readonly RefDictionary<nint, CollectionMetadata> CollectionToMetadataByElementType = new();

    static readonly RefDictionary<nint, (Func<RelationInfo, Func<IObjectDBTransaction, IRelation>>, Type[])>
        RelationCreators =
            new();

    //value type is actually delegate*<ref byte, ref nint, delegate*<ref byte, void>, void> but C# does not support it so replaced by simple pointer
    static readonly RefDictionary<nint, nint> StackAllocators = new();

    static SeqLock _lock;

    public static ClassMetadata? FindByType(Type type)
    {
        var handle = type.TypeHandle.Value;
        return TypeToMetadata.TryGetValueSeqLock(handle, out var metadata, ref _lock) ? metadata : null;
    }

    public static ClassMetadata? FindByName(ReadOnlySpan<byte> name)
    {
        return NameToMetadata.TryGetValueSeqLock(name, out var metadata, ref _lock) ? metadata : null;
    }

    public static ClassMetadata? FindByName(ReadOnlySpan<char> name)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(name.Length, 256);
        Span<byte> buf = stackalloc byte[name.Length * 3];
        var len = Encoding.UTF8.GetBytes(name, buf);
        return NameToMetadata.TryGetValueSeqLock(buf[..len], out var metadata, ref _lock)
            ? metadata
            : null;
    }

    /// Mostly for debugging purposes, returns all registered metadata. Slow because it needs to lock for writing.
    public static IList<ClassMetadata> All()
    {
        _lock.StartWrite();
        try
        {
            return TypeToMetadata.Select(v => v.Value).ToList();
        }
        finally
        {
            _lock.EndWrite();
        }
    }

    public static void Register(ClassMetadata metadata)
    {
        _lock.StartWrite();
        try
        {
            if (TypeToMetadata.TryAdd(metadata.Type.TypeHandle.Value, metadata))
            {
                NameToMetadata.GetOrAddValueRef(Encoding.UTF8.GetBytes(metadata.TruePersistedName)) = metadata;
            }
        }
        finally
        {
            _lock.EndWrite();
        }
    }

    public static CollectionMetadata? FindCollectionByType(Type type)
    {
        var handle = type.TypeHandle.Value;
        if (CollectionToMetadata.TryGetValueSeqLock(handle, out var metadata, ref _lock)) return metadata;
        return null;
    }

    public static CollectionMetadata? FindCollectionByElementType(Type elementType)
    {
        var handle = elementType.TypeHandle.Value;
        if (CollectionToMetadataByElementType.TryGetValueSeqLock(handle, out var metadata, ref _lock))
            return metadata;
        return null;
    }

    public static void RegisterCollection(CollectionMetadata metadata)
    {
        _lock.StartWrite();
        try
        {
            CollectionToMetadata.TryAdd(metadata.Type.TypeHandle.Value, metadata);
            if (metadata.ElementValueType == null)
            {
                CollectionToMetadataByElementType.TryAdd(metadata.Type.GetGenericArguments()[0].TypeHandle.Value,
                    metadata);
            }
        }
        finally
        {
            _lock.EndWrite();
        }
    }

    public static unsafe void RegisterStackAllocator(Type type,
        delegate*<ref byte, ref nint, delegate*<ref byte, void>, void> allocator)
    {
        _lock.StartWrite();
        try
        {
            StackAllocators.TryAdd(type.TypeHandle.Value, (nint)allocator);
        }
        finally
        {
            _lock.EndWrite();
        }
    }

    public static unsafe delegate*<ref byte, ref nint, delegate*<ref byte, void>, void>
        FindStackAllocatorByType(Type type)
    {
        var handle = type.TypeHandle.Value;
        if ((*(RawData.MethodTable*)handle).IsValueType)
        {
            if (StackAllocators.TryGetValueSeqLock(handle, out var allocator, ref _lock))
                return (delegate*<ref byte, ref nint, delegate*<ref byte, void>, void>)allocator;
            if ((*(RawData.MethodTable*)handle).ContainsGCPointers)
                throw new InvalidOperationException(
                    "Value type with GC pointers is not supported without registration of stack allocator " +
                    type.ToSimpleName());
            if (RawData.GetSizeAndAlign(type).Size > Unsafe.SizeOf<UInt128>())
                throw new InvalidOperationException("Value type of size " + (*(RawData.MethodTable*)handle).BaseSize +
                                                    " is not supported without registration of stack allocator " +
                                                    type.ToSimpleName());
            return &StackAllocatorUInt128;
        }

        return &StackAllocatorObject;
    }

    static unsafe void StackAllocatorUInt128(ref byte ctx, ref nint ptr, delegate*<ref byte, void> chain)
    {
        UInt128 value;
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        ptr = (nint)(&value);
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        chain(ref ctx);
        ptr = 0;
    }

    static unsafe void StackAllocatorObject(ref byte ctx, ref nint ptr, delegate*<ref byte, void> chain)
    {
        object value;
#pragma warning disable CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        ptr = (nint)(&value);
#pragma warning restore CS8500 // This takes the address of, gets the size of, or declares a pointer to a managed type
        chain(ref ctx);
        ptr = 0;
    }

    public static void RegisterRelation(Type type,
        Func<RelationInfo, Func<IObjectDBTransaction, IRelation>> creator, Type[] loadTypes)
    {
        _lock.StartWrite();
        try
        {
            RelationCreators.TryAdd(type.TypeHandle.Value, (creator, loadTypes));
        }
        finally
        {
            _lock.EndWrite();
        }
    }

    public static (Func<RelationInfo, Func<IObjectDBTransaction, IRelation>>, Type[])?
        FindRelationCreatorByType(Type type)
    {
        var handle = type.TypeHandle.Value;
        if (RelationCreators.TryGetValueSeqLock(handle, out var creator, ref _lock))
        {
            return creator;
        }

        return null;
    }
}
