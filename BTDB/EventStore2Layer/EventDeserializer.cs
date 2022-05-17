using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.Encrypted;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer;

public class EventDeserializer : IEventDeserializer, ITypeDescriptorCallbacks, ITypeBinaryDeserializerContext
{
    public const int ReservedBuildinTypes = 50;

    readonly Dictionary<object, DeserializerTypeInfo> _typeOrDescriptor2Info =
        new Dictionary<object, DeserializerTypeInfo>(ReferenceEqualityComparer<object>.Instance);

    StructList<DeserializerTypeInfo?> _id2Info;
    StructList<DeserializerTypeInfo?> _id2InfoNew;

    readonly Dictionary<ITypeDescriptor, ITypeDescriptor> _remapToOld =
        new Dictionary<ITypeDescriptor, ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

    StructList<object> _visited;
    readonly object _lock = new object();
    readonly ISymmetricCipher _symmetricCipher;

    public EventDeserializer(ITypeNameMapper? typeNameMapper = null,
        ITypeConvertorGenerator? typeConvertorGenerator = null, ISymmetricCipher? symmetricCipher = null)
    {
        TypeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
        ConvertorGenerator = typeConvertorGenerator ?? DefaultTypeConvertorGenerator.Instance;
        _symmetricCipher = symmetricCipher ?? new InvalidSymmetricCipher();
        _id2Info.Reserve(ReservedBuildinTypes + 10);
        _id2Info.Add(null); // 0 = null
        _id2Info.Add(null); // 1 = back reference
        foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
        {
            var infoForType = new DeserializerTypeInfo
            {
                Id = (int)_id2Info.Count,
                Descriptor = predefinedType
            };
            _typeOrDescriptor2Info[predefinedType] = infoForType;
            _id2Info.Add(infoForType);

            _typeOrDescriptor2Info.TryAdd(predefinedType.GetPreferredType(), infoForType);
            var descriptorMultipleNativeTypes = predefinedType as ITypeDescriptorMultipleNativeTypes;
            if (descriptorMultipleNativeTypes == null) continue;
            foreach (var type in descriptorMultipleNativeTypes.GetNativeTypes())
            {
                _typeOrDescriptor2Info[type] = infoForType;
            }
        }

        while (_id2Info.Count < ReservedBuildinTypes) _id2Info.Add(null);
    }

    public ITypeDescriptor? DescriptorOf(object obj)
    {
        if (obj == null) return null;
        var knowDescriptor = obj as IKnowDescriptor;
        if (knowDescriptor != null) return knowDescriptor.GetDescriptor();
        if (!_typeOrDescriptor2Info.TryGetValue(obj.GetType(), out var info))
            return null;
        return info.Descriptor;
    }

    public ITypeDescriptor? DescriptorOf(Type type)
    {
        return !_typeOrDescriptor2Info.TryGetValue(type, out var info) ? null : info.Descriptor;
    }

    public bool IsSafeToLoad(Type type)
    {
        if (!type.IsGenericType) return true;
        if (type.GetGenericTypeDefinition() == typeof(IIndirect<>)) return false;
        if (type.GetGenericArguments().Any(t => !IsSafeToLoad(t))) return false;
        return true;
    }

    public ITypeConvertorGenerator ConvertorGenerator { get; }

    public ITypeNameMapper TypeNameMapper { get; }

    public Type LoadAsType(ITypeDescriptor descriptor)
    {
        return descriptor.GetPreferredType() ?? TypeNameMapper.ToType(descriptor.Name!) ?? typeof(object);
    }

    public Type LoadAsType(ITypeDescriptor descriptor, Type targetType)
    {
        return descriptor.GetPreferredType(targetType) ?? TypeNameMapper.ToType(descriptor.Name) ?? typeof(object);
    }

    ITypeDescriptor NestedDescriptorReader(ref SpanReader reader)
    {
        var typeId = reader.ReadVInt32();
        if (typeId < 0 && -typeId - 1 < _id2InfoNew.Count)
        {
            var infoForType = _id2InfoNew[-typeId - 1];
            if (infoForType != null)
                return infoForType.Descriptor!;
        }
        else if (typeId > 0)
        {
            if (typeId >= _id2Info.Count)
                throw new BTDBException("Metadata corrupted");
            var infoForType = _id2Info[typeId];
            if (infoForType == null)
                throw new BTDBException("Metadata corrupted");
            return infoForType.Descriptor!;
        }

        return new PlaceHolderDescriptor(typeId);
    }

    public void ProcessMetadataLog(ByteBuffer buffer)
    {
        lock (_lock)
        {
            var reader = new SpanReader(buffer);
            var typeId = reader.ReadVInt32();
            while (typeId != 0)
            {
                var typeCategory = (TypeCategory)reader.ReadUInt8();
                ITypeDescriptor descriptor;
                switch (typeCategory)
                {
                    case TypeCategory.BuildIn:
                        throw new ArgumentOutOfRangeException();
                    case TypeCategory.Class:
                        descriptor = new ObjectTypeDescriptor(this, ref reader, NestedDescriptorReader, null);
                        break;
                    case TypeCategory.List:
                        descriptor = new ListTypeDescriptor(this, ref reader, NestedDescriptorReader);
                        break;
                    case TypeCategory.Dictionary:
                        descriptor = new DictionaryTypeDescriptor(this, ref reader, NestedDescriptorReader);
                        break;
                    case TypeCategory.Enum:
                        descriptor = new EnumTypeDescriptor(this, ref reader);
                        break;
                    case TypeCategory.Nullable:
                        descriptor = new NullableTypeDescriptor(this, ref reader, NestedDescriptorReader);
                        break;
                    case TypeCategory.Tuple:
                        descriptor = new TupleTypeDescriptor(this, ref reader, NestedDescriptorReader);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                while (-typeId - 1 >= _id2InfoNew.Count)
                    _id2InfoNew.Add(null);
                _id2InfoNew[-typeId - 1] ??= new DeserializerTypeInfo { Id = typeId, Descriptor = descriptor };
                typeId = reader.ReadVInt32();
            }

            for (var i = 0; i < _id2InfoNew.Count; i++)
            {
                _id2InfoNew[i]!.Descriptor!.MapNestedTypes(d =>
                    d is PlaceHolderDescriptor placeHolderDescriptor
                        ? _id2InfoNew[-placeHolderDescriptor.TypeId - 1]!.Descriptor
                        : d);
            }

            // This additional cycle is needed to fill names of recursive structures
            for (var i = 0; i < _id2InfoNew.Count; i++)
            {
                _id2InfoNew[i]!.Descriptor!.MapNestedTypes(d => d);
            }

            for (var i = 0; i < _id2InfoNew.Count; i++)
            {
                var infoForType = _id2InfoNew[i];
                for (var j = ReservedBuildinTypes; j < _id2Info.Count; j++)
                {
                    if (infoForType!.Descriptor.Equals(_id2Info[j]!.Descriptor))
                    {
                        _remapToOld[infoForType.Descriptor] = _id2Info[j]!.Descriptor;
                        _id2InfoNew[i] = _id2Info[j];
                        infoForType = _id2InfoNew[i];
                        break;
                    }
                }

                if (infoForType!.Id < 0)
                {
                    infoForType.Id = (int)_id2Info.Count;
                    _id2Info.Add(infoForType);
                    _typeOrDescriptor2Info[infoForType.Descriptor!] = infoForType;
                }
            }

            for (var i = 0; i < _id2InfoNew.Count; i++)
            {
                _id2InfoNew[i]!.Descriptor!.MapNestedTypes(d => _remapToOld.TryGetValue(d, out var res) ? res : d);
            }

            _id2InfoNew.Clear();
            _remapToOld.Clear();
        }
    }

    Layer2Loader LoaderFactory(ITypeDescriptor descriptor)
    {
        var loadAsType = LoadAsType(descriptor);
        var methodBuilder = ILBuilder.Instance.NewMethod<Layer2Loader>("DeserializerFor" + descriptor.Name);
        var il = methodBuilder.Generator;
        try
        {
            descriptor.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldarg(1), ilGen => ilGen.Ldarg(2),
                loadAsType);
        }
        catch (BTDBException ex)
        {
            throw new BTDBException("Deserialization of type " + loadAsType.FullName, ex);
        }

        if (loadAsType.IsValueType)
        {
            il.Box(loadAsType);
        }
        else if (loadAsType != typeof(object))
        {
            il.Castclass(typeof(object));
        }

        il.Ret();
        return methodBuilder.Create();
    }

    public bool Deserialize(out object? @object, ByteBuffer buffer)
    {
        lock (_lock)
        {
            var reader = new SpanReader(buffer);
            @object = null;
            try
            {
                @object = LoadObject(ref reader);
            }
            catch (BtdbMissingMetadataException)
            {
                return false;
            }
            finally
            {
                _visited.Clear();
            }

            return true;
        }
    }

    class BtdbMissingMetadataException : Exception
    {
    }

    public object? LoadObject(ref SpanReader reader)
    {
        var typeId = reader.ReadVUInt32();
        if (typeId == 0)
        {
            return null;
        }

        if (typeId == 1)
        {
            var backRefId = reader.ReadVUInt32();
            return _visited[(int)backRefId];
        }

        if (typeId >= _id2Info.Count)
            throw new BtdbMissingMetadataException();
        var infoForType = _id2Info[(int)typeId];
        if (infoForType!.Loader == null)
        {
            infoForType.Loader = LoaderFactory(infoForType.Descriptor!);
        }

        return infoForType.Loader(ref reader, this, infoForType.Descriptor!);
    }

    public void AddBackRef(object obj)
    {
        _visited.Add(obj);
    }

    public void SkipObject(ref SpanReader reader)
    {
        var typeId = reader.ReadVUInt32();
        if (typeId == 0)
        {
            return;
        }

        if (typeId == 1)
        {
            var backRefId = reader.ReadVUInt32();
            if (backRefId > _visited.Count) throw new InvalidDataException();
            return;
        }

        if (typeId >= _id2Info.Count)
            throw new BtdbMissingMetadataException();
        var infoForType = _id2Info[(int)typeId];
        if (infoForType!.Loader == null)
        {
            infoForType.Loader = LoaderFactory(infoForType.Descriptor!);
        }

        infoForType.Loader(ref reader, this, infoForType.Descriptor!);
    }

    public EncryptedString LoadEncryptedString(ref SpanReader reader)
    {
        var enc = reader.ReadByteArray();
        var size = _symmetricCipher.CalcPlainSizeFor(enc);
        var dec = new byte[size];
        if (!_symmetricCipher.Decrypt(enc, dec))
        {
            throw new CryptographicException();
        }

        var r = new SpanReader(dec);
        return r.ReadString();
    }

    public void SkipEncryptedString(ref SpanReader reader)
    {
        reader.SkipByteArray();
    }
}
