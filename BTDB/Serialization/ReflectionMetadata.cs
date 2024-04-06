using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using BTDB.Collections;
using BTDB.KVDBLayer;
using BTDB.Locks;

namespace BTDB.Serialization;

public static class ReflectionMetadata
{
    static readonly RefDictionary<nint, ClassMetadata> _typeToMetadata = new();
    static readonly SpanByteNoRemoveDictionary<ClassMetadata> _nameToMetadata = new();

    static readonly RefDictionary<nint, CollectionMetadata> _collectionToMetadata = new();

    static SeqLock _lock;

    public static ClassMetadata? FindByType(Type type)
    {
        var handle = type.TypeHandle.Value;
        return _typeToMetadata.TryGetValueSeqLock(handle, out var metadata, ref _lock) ? metadata : null;
    }

    public static ClassMetadata? FindByName(ReadOnlySpan<byte> name)
    {
        return _nameToMetadata.TryGetValueSeqLock(name, out var metadata, ref _lock) ? metadata : null;
    }


    /// Mostly for debugging purposes, returns all registered metadata. Slow because it needs to lock for writing.
    public static IList<ClassMetadata> All()
    {
        _lock.StartWrite();
        try
        {
            return _typeToMetadata.Select(v => v.Value).ToList();
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
            if (_typeToMetadata.TryAdd(metadata.Type.TypeHandle.Value, metadata))
            {
                _nameToMetadata.GetOrAddValueRef(Encoding.UTF8.GetBytes(metadata.TruePersistedName)) = metadata;
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
        if (_collectionToMetadata.TryGetValueSeqLock(handle, out var metadata, ref _lock)) return metadata;
        return null;
    }

    public static void RegisterCollection(CollectionMetadata metadata)
    {
        _lock.StartWrite();
        try
        {
            _collectionToMetadata.TryAdd(metadata.Type.TypeHandle.Value, metadata);
        }
        finally
        {
            _lock.EndWrite();
        }
    }
}
