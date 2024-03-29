using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using BTDB.Collections;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace BTDB.Serialization;

public static class ReflectionMetadata
{
    static readonly Dictionary<Type, ClassMetadata> _typeToMetadata = new(ReferenceEqualityComparer<Type>.Instance);
    static readonly SpanByteNoRemoveDictionary<ClassMetadata> _nameToMetadata = new();

    static readonly Dictionary<Type, CollectionMetadata> _collectionToMetadata =
        new(ReferenceEqualityComparer<Type>.Instance);

    static readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

    public static ClassMetadata? FindByType(Type type)
    {
        using (_lock.ReadLock())
        {
            if (_typeToMetadata.TryGetValue(type, out var metadata))
                return metadata;
            return null;
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
            return _typeToMetadata.Values;
        }
    }

    public static void Register(ClassMetadata metadata)
    {
        using (_lock.WriteLock())
        {
            if (_typeToMetadata.TryAdd(metadata.Type, metadata))
                _nameToMetadata.GetOrAddValueRef(Encoding.UTF8.GetBytes(metadata.TruePersistedName)) = metadata;
        }
    }

    public static CollectionMetadata? FindCollectionByType(Type type)
    {
        using (_lock.ReadLock())
        {
            return _collectionToMetadata.GetValueOrDefault(type);
        }
    }

    public static void RegisterCollection(CollectionMetadata metadata)
    {
        using (_lock.WriteLock())
        {
            _collectionToMetadata.TryAdd(metadata.Type, metadata);
        }
    }
}
