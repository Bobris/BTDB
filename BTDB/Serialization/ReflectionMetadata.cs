using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using BTDB.Collections;
using BTDB.KVDBLayer;

namespace BTDB.Serialization;

public static class ReflectionMetadata
{
    static readonly Dictionary<nint, ClassMetadata> _typeToMetadata = new();
    static readonly SpanByteNoRemoveDictionary<ClassMetadata> _nameToMetadata = new();

    static readonly Dictionary<nint, CollectionMetadata> _collectionToMetadata = new();

    static readonly ReaderWriterLockSlim _lock = new();

    public static ClassMetadata? FindByType(Type type)
    {
        var handle = type.TypeHandle.Value;
        using (_lock.ReadLock())
        {
            return _typeToMetadata.GetValueOrDefault(handle);
        }
    }

    public static ClassMetadata? FindByName(ReadOnlySpan<byte> name)
    {
        using (_lock.ReadLock())
        {
            return _nameToMetadata.TryGetValue(name, out var metadata) ? metadata : null;
        }
    }

    public static IEnumerable<ClassMetadata> All()
    {
        using (_lock.ReadLock())
        {
            foreach (var (_, value) in _typeToMetadata)
            {
                yield return value;
            }
        }
    }

    public static void Register(ClassMetadata metadata)
    {
        using (_lock.WriteLock())
        {
            if (_typeToMetadata.TryAdd(metadata.Type.TypeHandle.Value, metadata))
                _nameToMetadata.GetOrAddValueRef(Encoding.UTF8.GetBytes(metadata.TruePersistedName)) = metadata;
        }
    }

    public static CollectionMetadata? FindCollectionByType(Type type)
    {
        var handle = type.TypeHandle.Value;
        using (_lock.ReadLock())
        {
            return _collectionToMetadata.GetValueOrDefault(handle);
        }
    }

    public static void RegisterCollection(CollectionMetadata metadata)
    {
        using (_lock.WriteLock())
        {
            _collectionToMetadata.TryAdd(metadata.Type.TypeHandle.Value, metadata);
        }
    }
}
