using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer;

/// <summary>
/// Serializes events with type IDs from a <see cref="Layer2Metadata"/> catalog. Data has the format of
/// <see cref="EventSerializer"/>; only where type IDs come from differs. Types are resolved by shape the first time a
/// CLR type is met, so a restarted process finds the types it stored before without growing the metadata.
/// Not thread-safe; the caller serializes access.
/// </summary>
public sealed class CatalogEventSerializer : ITypeDescriptorCallbacks, ITypeDescriptorFactory, ITypeBinarySerializerContext
{
    sealed class TypeInfo(ITypeDescriptor descriptor)
    {
        public readonly ITypeDescriptor Descriptor = descriptor;
        public int Id; // 0 until stored in the installed metadata
        public Layer1ComplexSaver? Saver;
    }

    readonly Dictionary<Type, TypeInfo> _types = new(ReferenceEqualityComparer.Instance);
    readonly Dictionary<Type, ITypeDescriptor> _descriptors = new(ReferenceEqualityComparer.Instance);
    readonly Dictionary<object, int> _visited = new(ReferenceEqualityComparer.Instance);
    readonly Dictionary<TypeInfo, int> _proposed = new(ReferenceEqualityComparer.Instance);
    readonly ISymmetricCipher _symmetricCipher;
    readonly bool _forbidSerializationOfLazyDBObjects;
    Layer2TypeResolver _resolver;

    public readonly ITypeConverterFactory ConverterFactory;

    public CatalogEventSerializer(Layer2Metadata metadata, ITypeNameMapper? typeNameMapper = null,
        ITypeConverterFactory? typeConverterFactory = null, ITypeConvertorGenerator? typeConvertorGenerator = null,
        ISymmetricCipher? symmetricCipher = null, bool forbidSerializationOfLazyDBObjects = false)
    {
        Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        TypeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
        ConverterFactory = typeConverterFactory ?? new DefaultTypeConverterFactory();
        ConvertorGenerator = typeConvertorGenerator ?? DefaultTypeConvertorGenerator.Instance;
        _symmetricCipher = symmetricCipher ?? new InvalidSymmetricCipher();
        _forbidSerializationOfLazyDBObjects = forbidSerializationOfLazyDBObjects;
        _resolver = new(metadata);
    }

    /// <summary>The installed metadata, whose types serialized data may use.</summary>
    public Layer2Metadata Metadata { get; private set; }

    /// <summary>Installs newer metadata: written by this serializer's proposal, or read after another writer's.
    /// It must hold everything the installed metadata holds.</summary>
    public void Update(Layer2Metadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        if (ReferenceEquals(metadata, Metadata)) return;
        if (!metadata.Extends(Metadata))
            throw new ArgumentException("New Layer2 metadata must extend the installed one.", nameof(metadata));
        Metadata = metadata;
        _resolver = new(metadata);
    }

    /// <summary>
    /// Serializes an event. When it uses types the installed metadata lacks, returns an empty buffer and
    /// <paramref name="newMetadata"/>, the installed metadata with all of them appended. Write it before any data
    /// that uses them, <see cref="Update"/> with the written (or, after a lost race, the re-read) metadata, and
    /// serialize again. Proposed types are never used before they are installed.
    /// </summary>
    public ByteBuffer Serialize(out Layer2Metadata? newMetadata, object? obj)
    {
        var writer = new MemWriter();
        try
        {
            StoreObject(ref writer, obj);
            // A proposed ID is final when the installed metadata holds it: resolution found the stored type.
            foreach (var (info, id) in _proposed)
                if (id < Layer2Metadata.FirstTypeId || Metadata.ContainsType(id))
                    info.Id = id;
            if (!_resolver.HasNewTypes)
            {
                newMetadata = null;
                return writer.GetByteBufferAndReset();
            }
            newMetadata = _resolver.Build();
            return ByteBuffer.NewEmpty();
        }
        finally
        {
            _visited.Clear();
            _proposed.Clear();
            // A failed serialization may have appended to the resolver; its proposals must not survive.
            if (_resolver.HasNewTypes) _resolver = new(Metadata);
        }
    }

    public void StoreObject(ref MemWriter writer, object? obj)
    {
        EventNesting.EnsureStack("store");
        if (obj == null)
        {
            writer.WriteUInt8(0);
            return;
        }

        if (_visited.TryGetValue(obj, out var index))
        {
            writer.WriteUInt8(1); // back reference
            writer.WriteVUInt32((uint)index);
            return;
        }

        var objType = obj.GetType();
        if (!objType.IsEnum) _visited.Add(obj, _visited.Count);
        var info = InfoOf(objType);
        var id = info.Id;
        if (id == 0 && !_proposed.TryGetValue(info, out id))
        {
            // Resolving may append; then the ID is a proposal and this serialization only collects new types.
            id = _resolver.Resolve(info.Descriptor);
            _proposed[info] = id;
        }
        info.Saver ??= Layer2Shared.BuildComplexSaver(info.Descriptor, objType, ConverterFactory);
        writer.WriteVUInt32((uint)id);
        info.Saver(ref writer, this, obj);
    }

    TypeInfo InfoOf(Type type)
    {
        if (_types.TryGetValue(type, out var info)) return info;
        var descriptor = Create(type) ??
                         throw new BTDBException("Don't know how to serialize type " + type.ToSimpleName());
        info = new(descriptor);
        _types[type] = info;
        return info;
    }

    /// <summary>The descriptor of a CLR type; descriptors refer to each other through this factory.</summary>
    public ITypeDescriptor? Create(Type type)
    {
        if (_descriptors.TryGetValue(type, out var descriptor)) return descriptor;
        if (Layer2Shared.BuiltInByType.TryGetValue(type, out descriptor)) return descriptor;
        if (_forbidSerializationOfLazyDBObjects && type.InheritsOrImplements(typeof(IAmLazyDBObject)))
            throw new BTDBException("Lazy DB object serialization is forbidden. Type: " + type.ToSimpleName());
        var kind = Layer2Shared.KindOf(type, out var alternative);
        if (kind == Layer2Shared.TypeKind.Indirect) return null;
        if (alternative != null && alternative != type && _descriptors.TryGetValue(alternative, out descriptor))
        {
            _descriptors[type] = descriptor;
            return descriptor;
        }
        descriptor = Layer2Shared.NewDescriptor(this, type, kind, alternative)!;
        // Registered before it is finished, so a type referring to itself finds it.
        _descriptors[type] = descriptor;
        if (alternative != null) _descriptors[alternative] = descriptor;
        if (descriptor.FinishBuildFromType(this)) return descriptor;
        _descriptors.Remove(type);
        if (alternative != null) _descriptors.Remove(alternative);
        return null;
    }

    public void StoreEncryptedString(ref MemWriter writer, EncryptedString value) =>
        Layer2Shared.StoreEncryptedString(ref writer, value, _symmetricCipher);

    public ITypeDescriptor? DescriptorOf(object? obj) => obj == null ? null : DescriptorOf(obj.GetType());

    public ITypeDescriptor? DescriptorOf(Type type) =>
        _descriptors.TryGetValue(type, out var descriptor) ? descriptor
        : Layer2Shared.BuiltInByType.GetValueOrDefault(type);

    public bool IsSafeToLoad(Type type) => throw new InvalidOperationException();

    public ITypeConvertorGenerator ConvertorGenerator { get; }

    public ITypeNameMapper TypeNameMapper { get; }

    public Type LoadAsType(ITypeDescriptor descriptor) =>
        descriptor.GetPreferredType() ?? TypeNameMapper.ToType(descriptor.Name!) ?? typeof(object);

    public Type LoadAsType(ITypeDescriptor descriptor, Type targetType) =>
        descriptor.GetPreferredType(targetType) ?? TypeNameMapper.ToType(descriptor.Name!) ?? typeof(object);

    public bool PreserveDescriptors => false;
}
