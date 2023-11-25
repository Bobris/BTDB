using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

namespace BTDB.Serialization;

public static class ReflectionMetadata
{
    static readonly Dictionary<Type, ClassMetadata> _typeToMetadata = new(ReferenceEqualityComparer<Type>.Instance);
    
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
            Debug.Assert(!_typeToMetadata.ContainsKey(metadata.Type));
            _typeToMetadata[metadata.Type] = metadata;
        }
    }
}
