using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.Encrypted;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer;

/// <summary>
/// Deserializes events written by <see cref="CatalogEventSerializer"/>, building the descriptor and loader of a type
/// the first time data uses it. Types never read are never built.
/// </summary>
public sealed class CatalogEventDeserializer : ITypeDescriptorCallbacks, ITypeBinaryDeserializerContext
{
    readonly Lock _lock = new();
    readonly Layer2DescriptorDecoder _decoder;
    readonly ISymmetricCipher _symmetricCipher;
    volatile Layer2Metadata _metadata;
    Layer2Loader?[] _loaders = [];
    readonly Layer2Loader?[] _builtInLoaders = new Layer2Loader?[Layer2Metadata.FirstTypeId];

    // The serializer numbers every stored object except enums, so every loaded object gets its slot before loading.
    StructList<object?> _visited;
    int _reservedBackRef = -1;

    public readonly ITypeConverterFactory ConverterFactory;

    public CatalogEventDeserializer(Layer2Metadata metadata, ITypeNameMapper? typeNameMapper = null,
        ITypeConverterFactory? typeConverterFactory = null, ITypeConvertorGenerator? typeConvertorGenerator = null,
        ISymmetricCipher? symmetricCipher = null)
    {
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        TypeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
        ConverterFactory = typeConverterFactory ?? new DefaultTypeConverterFactory();
        ConvertorGenerator = typeConvertorGenerator ?? DefaultTypeConvertorGenerator.Instance;
        _symmetricCipher = symmetricCipher ?? new InvalidSymmetricCipher();
        _decoder = new(this);
    }

    /// <summary>The installed metadata.</summary>
    public Layer2Metadata Metadata => _metadata;

    /// <summary>Installs newer metadata, which must hold everything the installed metadata holds; types already built
    /// stay valid because type IDs never change.</summary>
    public void Update(Layer2Metadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        lock (_lock)
        {
            if (ReferenceEquals(metadata, _metadata)) return;
            if (!metadata.Extends(_metadata))
                throw new ArgumentException("New Layer2 metadata must extend the installed one.", nameof(metadata));
            _metadata = metadata;
        }
    }

    /// <summary>Deserializes an event. False when the data uses a type the installed metadata lacks: install newer
    /// metadata and try again. Metadata is written before the data that uses it, so a type still missing then means
    /// damaged data.</summary>
    public unsafe bool Deserialize(out object? @object, ByteBuffer buffer)
    {
        lock (_lock)
        {
            fixed (void* _ = buffer.Buffer)
            {
                var reader = MemReader.CreateFromPinnedSpan(buffer.AsSyncReadOnlySpan());
                @object = null;
                try
                {
                    @object = LoadObject(ref reader);
                }
                catch (MissingTypeException)
                {
                    return false;
                }
                finally
                {
                    _visited.Clear();
                    _reservedBackRef = -1;
                }

                return true;
            }
        }
    }

    sealed class MissingTypeException : Exception;

    Layer2Loader Loader(int typeId, out ITypeDescriptor descriptor)
    {
        if (typeId < Layer2Metadata.FirstTypeId)
        {
            descriptor = Layer2Encoding.BuiltIn(typeId) ?? throw new MissingTypeException();
            return _builtInLoaders[typeId] ??= LoaderFactory(descriptor);
        }
        if (!Metadata.ContainsType(typeId)) throw new MissingTypeException();
        descriptor = _decoder.Get(Metadata, typeId);
        var index = typeId - Layer2Metadata.FirstTypeId;
        if (_loaders.Length <= index) Array.Resize(ref _loaders, Metadata.TypeCount);
        return _loaders[index] ??= LoaderFactory(descriptor);
    }

    Layer2Loader LoaderFactory(ITypeDescriptor descriptor) =>
        Layer2Shared.BuildLoader(descriptor, ObjectLoadType.For(descriptor, LoadAsType(descriptor)), ConverterFactory);

    public object? LoadObject(ref MemReader reader)
    {
        EventNesting.EnsureStack("read");
        var typeId = reader.ReadVUInt32();
        if (typeId == 0) return null;
        if (typeId == 1)
        {
            var backRefId = reader.ReadVUInt32();
            // Back references point to objects read before; damaged data can name one that was not.
            if (backRefId >= _visited.Count)
                throw new InvalidDataException("Event back reference " + backRefId + " points to no object read before.");
            return _visited[(int)backRefId];
        }

        if (typeId > int.MaxValue) throw new MissingTypeException();
        var loader = Loader((int)typeId, out var descriptor);
        if (descriptor is EnumTypeDescriptor) return loader(ref reader, this, descriptor);
        var previousReservedBackRef = _reservedBackRef;
        var slot = ReserveBackRef();
        try
        {
            var result = loader(ref reader, this, descriptor);
            _visited[slot] ??= result;
            return result;
        }
        finally
        {
            _reservedBackRef = previousReservedBackRef;
        }
    }

    int ReserveBackRef()
    {
        var slot = (int)_visited.Count;
        _visited.Add(null);
        _reservedBackRef = slot;
        return slot;
    }

    public void AddBackRef(object obj)
    {
        if (_reservedBackRef >= 0)
        {
            _visited[_reservedBackRef] = obj;
            _reservedBackRef = -1;
            return;
        }

        _visited.Add(obj);
    }

    public void SkipObject(ref MemReader reader)
    {
        EventNesting.EnsureStack("read");
        var typeId = reader.ReadVUInt32();
        if (typeId == 0) return;
        if (typeId == 1)
        {
            var backRefId = reader.ReadVUInt32();
            if (backRefId >= _visited.Count) throw new InvalidDataException();
            return;
        }

        if (typeId > int.MaxValue) throw new MissingTypeException();
        var loader = Loader((int)typeId, out var descriptor);
        if (descriptor is EnumTypeDescriptor)
        {
            loader(ref reader, this, descriptor);
            return;
        }

        var previousReservedBackRef = _reservedBackRef;
        ReserveBackRef();
        try
        {
            loader(ref reader, this, descriptor);
        }
        finally
        {
            _reservedBackRef = previousReservedBackRef;
        }
    }

    public EncryptedString LoadEncryptedString(ref MemReader reader) =>
        Layer2Shared.LoadEncryptedString(ref reader, _symmetricCipher);

    public void SkipEncryptedString(ref MemReader reader) => reader.SkipByteArray();

    public ITypeDescriptor? DescriptorOf(object? obj)
    {
        if (obj == null) return null;
        if (PreserveDescriptors)
        {
            if (obj is IKnowDescriptor knowDescriptor) return knowDescriptor.GetDescriptor();
            TypeSerializers.Object2DescriptorMap.TryGetValue(obj, out var descriptor);
            if (descriptor != null) return descriptor;
        }

        return DescriptorOf(obj.GetType());
    }

    public ITypeDescriptor? DescriptorOf(Type type) => Layer2Shared.BuiltInByType.GetValueOrDefault(type);

    public bool IsSafeToLoad(Type type)
    {
        if (!type.IsGenericType) return true;
        if (type.GetGenericTypeDefinition() == typeof(IIndirect<>)) return false;
        return type.GetGenericArguments().All(IsSafeToLoad);
    }

    public ITypeConvertorGenerator ConvertorGenerator { get; }

    public ITypeNameMapper TypeNameMapper { get; }

    public Type LoadAsType(ITypeDescriptor descriptor) =>
        descriptor.GetPreferredType() ?? TypeNameMapper.ToType(descriptor.Name!) ?? typeof(object);

    public Type LoadAsType(ITypeDescriptor descriptor, Type targetType) =>
        descriptor.GetPreferredType(targetType) ?? TypeNameMapper.ToType(descriptor.Name!) ?? typeof(object);

    public bool PreserveDescriptors { get; set; } = true;
}
