using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using BTDB.Collections;
using BTDB.Encrypted;

namespace BTDB.EventStore2Layer;

public class EventSerializer : IEventSerializer, ITypeDescriptorCallbacks, IDescriptorSerializerLiteContext,
    ITypeDescriptorFactory, ITypeBinarySerializerContext
{
    public const int ReservedBuildinTypes = 50;

    readonly Dictionary<object, SerializerTypeInfo> _typeOrDescriptor2Info =
        new Dictionary<object, SerializerTypeInfo>(ReferenceEqualityComparer<object>.Instance);

    readonly Dictionary<object, SerializerTypeInfo> _typeOrDescriptor2InfoNew =
        new Dictionary<object, SerializerTypeInfo>(ReferenceEqualityComparer<object>.Instance);

    StructList<SerializerTypeInfo?> _id2Info;
    StructList<SerializerTypeInfo?> _id2InfoNew;

    readonly Dictionary<ITypeDescriptor, ITypeDescriptor> _remapToOld =
        new Dictionary<ITypeDescriptor, ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

    readonly Dictionary<object, int> _visited =
        new Dictionary<object, int>(ReferenceEqualityComparer<object>.Instance);

    readonly Dictionary<Type, Action<object, IDescriptorSerializerLiteContext>> _gathererCache =
        new Dictionary<Type, Action<object, IDescriptorSerializerLiteContext>>(ReferenceEqualityComparer<Type>
            .Instance);

    readonly ISymmetricCipher _symmetricCipher;

    readonly bool _useInputDescriptors;
    bool _newTypeFound;

    public EventSerializer(ITypeNameMapper? typeNameMapper = null,
        ITypeConvertorGenerator? typeConvertorGenerator = null, ISymmetricCipher? symmetricCipher = null, bool useInputDescriptors = true)
    {
        TypeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
        ConvertorGenerator = typeConvertorGenerator ?? DefaultTypeConvertorGenerator.Instance;
        _useInputDescriptors = useInputDescriptors;
        _symmetricCipher = symmetricCipher ?? new InvalidSymmetricCipher();
        _id2Info.Reserve(ReservedBuildinTypes + 10);
        _id2Info.Add(null); // 0 = null
        _id2Info.Add(null); // 1 = back reference
        foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
        {
            var infoForType = new SerializerTypeInfo
            {
                Id = (int)_id2Info.Count,
                Descriptor = predefinedType
            };
            _typeOrDescriptor2Info[predefinedType] = infoForType;
            _id2Info.Add(infoForType);
            _typeOrDescriptor2Info.TryAdd(predefinedType.GetPreferredType()!, infoForType);
            var descriptorMultipleNativeTypes = predefinedType as ITypeDescriptorMultipleNativeTypes;
            if (descriptorMultipleNativeTypes == null) continue;
            foreach (var type in descriptorMultipleNativeTypes.GetNativeTypes())
            {
                _typeOrDescriptor2Info[type] = infoForType;
            }
        }

        while (_id2Info.Count < ReservedBuildinTypes) _id2Info.Add(null);
    }

    Layer1ComplexSaver BuildComplexSaver(ITypeDescriptor descriptor, Type type)
    {
        var method =
            ILBuilder.Instance.NewMethod<Layer1ComplexSaver>(descriptor.Name + "Saver" + type.ToSimpleName());
        var il = method.Generator;
        descriptor.GenerateSave(il, ilgen => ilgen.Ldarg(0), ilgen => ilgen.Ldarg(1), ilgen =>
        {
            ilgen.Ldarg(2);
            if (type != typeof(object))
            {
                ilgen.UnboxAny(type);
            }
        }, type);
        il.Ret();
        return method.Create();
    }

    Action<object, IDescriptorSerializerLiteContext> BuildNestedObjGatherer(ITypeDescriptor descriptor, Type type)
    {
        Action<object, IDescriptorSerializerLiteContext> res;
        if (type != typeof(object))
        {
            if (_gathererCache.TryGetValue(type, out res))
                return res;
        }

        var gen = descriptor.BuildNewDescriptorGenerator();
        if (gen == null)
        {
            res = (obj, ctx) => { };
        }
        else
        {
            var method =
                ILBuilder.Instance.NewMethod<Action<object, IDescriptorSerializerLiteContext>>(
                    "GatherAllObjectsForTypeExtraction_" + descriptor.Name + "_For_" + type.ToSimpleName());
            var il = method.Generator;
            gen.GenerateTypeIterator(il, ilgen => ilgen.Ldarg(0), ilgen => ilgen.Ldarg(1), type);
            il.Ret();
            res = method.Create();
        }

        if (type != typeof(object)) _gathererCache[type] = res;
        return res;
    }

    public ITypeDescriptor? DescriptorOf(object? obj)
    {
        if (obj == null) return null;
        if (_useInputDescriptors && obj is IKnowDescriptor knowDescriptor) return knowDescriptor.GetDescriptor();
        if (!_typeOrDescriptor2Info.TryGetValue(obj.GetType(), out var info))
            return null;
        return info.Descriptor;
    }

    public ITypeDescriptor? DescriptorOf(Type type)
    {
        if (!_typeOrDescriptor2Info.TryGetValue(type, out var info))
            return null;
        return info.Descriptor;
    }

    public bool IsSafeToLoad(Type type)
    {
        throw new InvalidOperationException();
    }

    public ITypeConvertorGenerator ConvertorGenerator { get; }

    public ITypeNameMapper TypeNameMapper { get; }

    public Type LoadAsType(ITypeDescriptor descriptor)
    {
        return descriptor.GetPreferredType() ?? TypeNameMapper.ToType(descriptor.Name!) ?? typeof(object);
    }

    public Type LoadAsType(ITypeDescriptor descriptor, Type targetType)
    {
        return descriptor.GetPreferredType(targetType) ?? TypeNameMapper.ToType(descriptor.Name!) ?? typeof(object);
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
            if (_id2InfoNew[-typeId - 1] == null)
                _id2InfoNew[-typeId - 1] = new SerializerTypeInfo { Id = typeId, Descriptor = descriptor };
            typeId = reader.ReadVInt32();
        }

        for (var i = 0; i < _id2InfoNew.Count; i++)
        {
            _id2InfoNew[i]!.Descriptor!.MapNestedTypes(d => d is PlaceHolderDescriptor placeHolderDescriptor
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
            var infoForType = _id2InfoNew[i]!;
            for (var j = ReservedBuildinTypes; j < _id2Info.Count; j++)
            {
                if (infoForType!.Descriptor!.Equals(_id2Info[j]!.Descriptor))
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

    public ByteBuffer Serialize(out bool hasMetaData, object? obj)
    {
        var writer = new SpanWriter();
        if (obj == null)
        {
            hasMetaData = false;
            writer.WriteUInt8(0); // null
            return writer.GetByteBufferAndReset();
        }

        try
        {
            _newTypeFound = false;
            StoreObject(ref writer, obj);
            _visited.Clear();
            if (!_newTypeFound)
            {
                // No unknown metadata found - to be optimistic pays off
                hasMetaData = false;
                return writer.GetByteBufferAndReset();
            }

            StoreNewDescriptors(obj);
            if (_typeOrDescriptor2InfoNew.Count > 0)
            {
                writer.Reset();
                if (MergeTypesByShapeAndStoreNew(ref writer))
                {
                    _typeOrDescriptor2InfoNew.Clear();
                    _visited.Clear();
                    hasMetaData = true;
                    return writer.GetByteBufferAndReset();
                }

                _typeOrDescriptor2InfoNew.Clear();
            }

            _visited.Clear();
            _newTypeFound = false;
            StoreObject(ref writer, obj);
            if (_newTypeFound)
            {
                throw new BTDBException("Forgot descriptor or type");
            }

            _visited.Clear();
            hasMetaData = false;
            return writer.GetByteBufferAndReset();
        }
        catch
        {
            _visited.Clear();
            _typeOrDescriptor2InfoNew.Clear();
            throw;
        }
    }

    public ITypeDescriptor? Create(Type type)
    {
        if (_typeOrDescriptor2Info.TryGetValue(type, out var result)) return result.Descriptor;
        if (_typeOrDescriptor2InfoNew.TryGetValue(type, out result)) return result.Descriptor;
        ITypeDescriptor desc = null;
        Type typeAlternative = null;
        if (!type.IsSubclassOf(typeof(Delegate)))
        {
            if (type.IsGenericType)
            {
                typeAlternative = type.SpecializationOf(typeof(IDictionary<,>));
                if (typeAlternative != null)
                {
                    if (type != typeAlternative)
                    {
                        if (_typeOrDescriptor2Info.TryGetValue(typeAlternative, out result))
                        {
                            _typeOrDescriptor2Info[type] = result;
                            return result.Descriptor;
                        }

                        if (_typeOrDescriptor2InfoNew.TryGetValue(typeAlternative, out result))
                        {
                            _typeOrDescriptor2InfoNew[type] = result;
                            return result.Descriptor;
                        }
                    }

                    desc = new DictionaryTypeDescriptor(this, typeAlternative);
                }
                else
                {
                    typeAlternative = type.SpecializationOf(typeof(IList<>)) ??
                                      type.SpecializationOf(typeof(ISet<>));
                    if (typeAlternative != null)
                    {
                        if (type != typeAlternative)
                        {
                            if (_typeOrDescriptor2Info.TryGetValue(typeAlternative, out result))
                            {
                                _typeOrDescriptor2Info[type] = result;
                                return result.Descriptor;
                            }

                            if (_typeOrDescriptor2InfoNew.TryGetValue(typeAlternative, out result))
                            {
                                _typeOrDescriptor2InfoNew[type] = result;
                                return result.Descriptor;
                            }
                        }

                        desc = new ListTypeDescriptor(this, typeAlternative);
                    }
                    else if (type.GetGenericTypeDefinition().InheritsOrImplements(typeof(IIndirect<>)))
                    {
                        return null;
                    }
                    else if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        typeAlternative = type.SpecializationOf(typeof(Nullable<>));
                        if (typeAlternative != null)
                        {
                            if (type != typeAlternative)
                            {
                                if (_typeOrDescriptor2Info.TryGetValue(typeAlternative, out result))
                                {
                                    _typeOrDescriptor2Info[type] = result;
                                    return result.Descriptor;
                                }

                                if (_typeOrDescriptor2InfoNew.TryGetValue(typeAlternative, out result))
                                {
                                    _typeOrDescriptor2InfoNew[type] = result;
                                    return result.Descriptor;
                                }
                            }

                            desc = new NullableTypeDescriptor(this, typeAlternative);
                        }
                        else if (type.InheritsOrImplements(typeof(ITuple)))
                        {
                            desc = new TupleTypeDescriptor(this, type);
                        }
                    }
                    else
                    {
                        desc = new ObjectTypeDescriptor(this, type, null);
                    }
                }
            }
            else if (type.IsArray)
            {
                typeAlternative = type.SpecializationOf(typeof(IList<>));
                Debug.Assert(typeAlternative != null && type != typeAlternative);
                if (_typeOrDescriptor2Info.TryGetValue(typeAlternative, out result))
                {
                    _typeOrDescriptor2Info[type] = result;
                    return result.Descriptor;
                }

                if (_typeOrDescriptor2InfoNew.TryGetValue(typeAlternative, out result))
                {
                    _typeOrDescriptor2InfoNew[type] = result;
                    return result.Descriptor;
                }

                desc = new ListTypeDescriptor(this, typeAlternative);
            }
            else if (type.IsEnum)
            {
                desc = new EnumTypeDescriptor(this, type);
            }
            else if (type.IsValueType)
            {
                throw new BTDBException($"Unsupported value type {type.Name}.");
            }
            else
            {
                desc = new ObjectTypeDescriptor(this, type, null);
            }
        }

        if (desc == null) throw new BTDBException("Don't know how to serialize type " + type.ToSimpleName());
        result = new SerializerTypeInfo
        {
            Id = 0,
            Descriptor = desc
        };
        _typeOrDescriptor2InfoNew[desc] = result;
        _typeOrDescriptor2InfoNew[type] = result;
        if (typeAlternative != null) _typeOrDescriptor2InfoNew[typeAlternative] = result;
        if (!desc.FinishBuildFromType(this))
        {
            _typeOrDescriptor2InfoNew.Remove(desc);
            _typeOrDescriptor2InfoNew.Remove(type);
            if (typeAlternative != null) _typeOrDescriptor2InfoNew.Remove(typeAlternative);
            return null;
        }

        return desc;
    }

    public void StoreNewDescriptors(object? obj)
    {
        if (obj == null) return;
        if (_visited.ContainsKey(obj)) return;
        _visited.Add(obj, _visited.Count);
        SerializerTypeInfo info;
        var knowDescriptor = _useInputDescriptors ? obj as IKnowDescriptor : null;
        var type = obj.GetType();
        if (knowDescriptor != null)
        {
            var origDesc = knowDescriptor.GetDescriptor();
            if (!_typeOrDescriptor2Info.TryGetValue(origDesc, out info))
            {
                var newDesc = MergeDescriptor(origDesc);
                if (!_typeOrDescriptor2Info.TryGetValue(newDesc, out info))
                {
                    info = new SerializerTypeInfo
                    {
                        Id = 0,
                        Descriptor = newDesc
                    };
                }

                _typeOrDescriptor2InfoNew[origDesc] = info;
            }
        }
        else
        {
            if (!_typeOrDescriptor2Info.TryGetValue(type, out info))
            {
                var desc = Create(type);
                if (!_typeOrDescriptor2InfoNew.TryGetValue(desc!, out info))
                {
                    // It could be already existing descriptor just unknown type
                    if (!_typeOrDescriptor2Info.TryGetValue(desc, out info))
                    {
                        // If it is not in old nor new, than fail with clearer description
                        throw new BTDBException("EventSerializer.StoreNewDescriptors bug " +
                                                type.ToSimpleName());
                    }
                }
            }
        }

        ref var gatherer = ref info.NestedObjGatherers.GetOrAddValueRef(type);
        gatherer ??= BuildNestedObjGatherer(info.Descriptor!, type);

        gatherer(obj, this);
    }

    ITypeDescriptor MergeDescriptor(ITypeDescriptor origDesc)
    {
        var visited = new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        var hasCycle = false;

        ITypeDescriptor Old2New(ITypeDescriptor old)
        {
            if (visited.Contains(old))
            {
                hasCycle = true;
                return new PlaceHolderDescriptor(old);
            }

            visited.Add(origDesc);
            if (_typeOrDescriptor2Info.TryGetValue(old, out var info))
            {
                return info.Descriptor;
            }

            if (old is ObjectTypeDescriptor || old is EnumTypeDescriptor)
            {
                var type = TypeNameMapper.ToType(old.Name!) ?? typeof(object);
                if (type != typeof(object))
                {
                    return Create(type);
                }
            }

            return old.CloneAndMapNestedTypes(this, Old2New);
        }

        var res = Old2New(origDesc);
        if (hasCycle)
        {
            visited.Clear();
            visited.Add(res);

            ITypeDescriptor FlattenPlaceHolder(ITypeDescriptor old)
            {
                if (old is PlaceHolderDescriptor)
                {
                    return ((PlaceHolderDescriptor)old).TypeDesc;
                }

                if (visited.Contains(old)) return old;
                visited.Add(old);
                old.MapNestedTypes(FlattenPlaceHolder);
                return old;
            }

            res.MapNestedTypes(FlattenPlaceHolder);
        }

        foreach (var existingTypeDescriptor in _typeOrDescriptor2Info)
        {
            if (res.Equals(existingTypeDescriptor.Value.Descriptor))
            {
                return existingTypeDescriptor.Value.Descriptor!;
            }
        }

        foreach (var existingTypeDescriptor in _typeOrDescriptor2InfoNew)
        {
            if (res.Equals(existingTypeDescriptor.Value.Descriptor))
            {
                return existingTypeDescriptor.Value.Descriptor!;
            }
        }

        return res;
    }

    bool MergeTypesByShapeAndStoreNew(ref SpanWriter writer)
    {
        var toStore = new StructList<SerializerTypeInfo>();
        foreach (var typeDescriptor in _typeOrDescriptor2InfoNew)
        {
            var info = typeDescriptor.Value;
            if (info.Id == 0)
            {
                var d = info.Descriptor;
                foreach (var existingTypeDescriptor in _typeOrDescriptor2Info)
                {
                    if (d!.Equals(existingTypeDescriptor.Value.Descriptor))
                    {
                        info.Id = existingTypeDescriptor.Value.Id;
                        break;
                    }
                }
            }

            if (info.Id == 0)
            {
                toStore.Add(info);
                info.Id = -(int)toStore.Count;
            }
            else if (info.Id > 0)
            {
                if (typeDescriptor.Key is Type)
                {
                    _typeOrDescriptor2Info[typeDescriptor.Key] = _id2Info[info.Id];
                }
            }
        }

        if (toStore.Count > 0)
        {
            for (var i = (int)toStore.Count - 1; i >= 0; i--)
            {
                writer.WriteVInt32(toStore[i].Id);
                StoreDescriptor(toStore[i].Descriptor!, ref writer);
            }

            writer.WriteVInt32(0);
            return true;
        }

        return false;
    }

    void StoreDescriptor(ITypeDescriptor descriptor, ref SpanWriter writer)
    {
        if (descriptor is ListTypeDescriptor)
        {
            writer.WriteUInt8((byte)TypeCategory.List);
        }
        else if (descriptor is DictionaryTypeDescriptor)
        {
            writer.WriteUInt8((byte)TypeCategory.Dictionary);
        }
        else if (descriptor is ObjectTypeDescriptor)
        {
            writer.WriteUInt8((byte)TypeCategory.Class);
        }
        else if (descriptor is EnumTypeDescriptor)
        {
            writer.WriteUInt8((byte)TypeCategory.Enum);
        }
        else if (descriptor is NullableTypeDescriptor)
        {
            writer.WriteUInt8((byte)TypeCategory.Nullable);
        }
        else if (descriptor is TupleTypeDescriptor)
        {
            writer.WriteUInt8((byte)TypeCategory.Tuple);
        }
        else
        {
            throw new ArgumentOutOfRangeException();
        }

        ((IPersistTypeDescriptor)descriptor).Persist(ref writer, (ref SpanWriter w, ITypeDescriptor d) =>
        {
            if (!_typeOrDescriptor2Info.TryGetValue(d, out var result))
                if (!_typeOrDescriptor2InfoNew.TryGetValue(d, out result))
                    throw new BTDBException("Invalid state unknown descriptor " + d.Name);
            w.WriteVInt32(result.Id);
        });
    }

    public void StoreObject(ref SpanWriter writer, object? obj)
    {
        if (_newTypeFound) return;
        if (obj == null)
        {
            writer.WriteUInt8(0);
            return;
        }

        var visited = _visited;
        if (visited.TryGetValue(obj, out var index))
        {
            writer.WriteUInt8(1); // backreference
            writer.WriteVUInt32((uint)index);
            return;
        }

        visited.Add(obj, visited.Count);
        var objType = obj.GetType();
        if (!_typeOrDescriptor2Info.TryGetValue(objType, out var info))
        {
            if (_useInputDescriptors && obj is IKnowDescriptor knowDescriptor)
            {
                if (!_typeOrDescriptor2Info.TryGetValue(knowDescriptor.GetDescriptor(), out info))
                {
                    _newTypeFound = true;
                    return;
                }
            }
            else
            {
                _newTypeFound = true;
                return;
            }
        }

        ref var saver = ref info.ComplexSaver.GetOrAddValueRef(objType);
        saver ??= BuildComplexSaver(info.Descriptor!, objType);
        writer.WriteVUInt32((uint)info.Id);
        saver(ref writer, this, obj);
    }

    public void StoreEncryptedString(ref SpanWriter outerWriter, EncryptedString value)
    {
        var writer = new SpanWriter();
        writer.WriteString(value);
        var plain = writer.GetSpan();
        var encSize = _symmetricCipher.CalcEncryptedSizeFor(plain);
        var enc = new byte[encSize];
        _symmetricCipher.Encrypt(plain, enc);
        outerWriter.WriteByteArray(enc);
    }
}
