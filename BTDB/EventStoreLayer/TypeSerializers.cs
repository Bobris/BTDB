using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using BTDB.Encrypted;

namespace BTDB.EventStoreLayer;

public delegate object Layer1Loader(ref SpanReader reader, ITypeBinaryDeserializerContext? ctx,
    ITypeSerializersId2LoaderMapping mapping, ITypeDescriptor descriptor);

public delegate void Layer1SimpleSaver(ref SpanWriter writer, object value);

public delegate void Layer1ComplexSaver(ref SpanWriter writer, ITypeBinarySerializerContext ctx, object value);
public class TypeSerializers : ITypeSerializers
{
    ITypeNameMapper _typeNameMapper;

    readonly
        ConcurrentDictionary<ITypeDescriptor, Layer1Loader> _loaders =
            new ConcurrentDictionary<ITypeDescriptor, Layer1Loader>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

    readonly ConcurrentDictionary<(ITypeDescriptor, Type), Action<object, IDescriptorSerializerLiteContext>>
        _newDescriptorSavers =
            new ConcurrentDictionary<(ITypeDescriptor, Type), Action<object, IDescriptorSerializerLiteContext>>();

    readonly ConcurrentDictionary<ITypeDescriptor, bool> _descriptorSet =
        new ConcurrentDictionary<ITypeDescriptor, bool>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

    ConcurrentDictionary<Type, ITypeDescriptor> _type2DescriptorMap =
        new ConcurrentDictionary<Type, ITypeDescriptor>(ReferenceEqualityComparer<Type>.Instance);

    readonly object _buildTypeLock = new object();

    readonly ConcurrentDictionary<(ITypeDescriptor, Type), Layer1SimpleSaver> _simpleSavers =
        new ConcurrentDictionary<(ITypeDescriptor, Type), Layer1SimpleSaver>();

    readonly ConcurrentDictionary<(ITypeDescriptor, Type), Layer1ComplexSaver>
        _complexSavers = new ConcurrentDictionary<(ITypeDescriptor, Type), Layer1ComplexSaver>();

    readonly Func<ITypeDescriptor, Layer1Loader> _loaderFactoryAction;

    readonly Func<Type, ITypeDescriptor> _buildFromTypeAction;
    readonly ISymmetricCipher _symmetricCipher;

    public TypeSerializers(ITypeNameMapper? typeNameMapper = null, TypeSerializersOptions? options = null)
    {
        ConvertorGenerator = options?.ConvertorGenerator ?? DefaultTypeConvertorGenerator.Instance;
        _typeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
        ForgotAllTypesAndSerializers();
        _loaderFactoryAction = LoaderFactory;
        _buildFromTypeAction = BuildFromType;
        Options = options ?? TypeSerializersOptions.Default;
        _symmetricCipher = Options.SymmetricCipher ?? new InvalidSymmetricCipher();
    }

    public TypeSerializersOptions Options { get; }

    public void SetTypeNameMapper(ITypeNameMapper? typeNameMapper)
    {
        _typeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
    }

    public ITypeDescriptor? DescriptorOf(object? obj)
    {
        if (obj == null) return null;
        if (obj is IKnowDescriptor knowDescriptor) return knowDescriptor.GetDescriptor();
        return DescriptorOf(obj.GetType());
    }

    public bool IsSafeToLoad(Type type)
    {
        return DescriptorOf(type) != null;
    }

    public ITypeConvertorGenerator ConvertorGenerator { get; private set; }

    public ITypeNameMapper TypeNameMapper => _typeNameMapper;

    public ITypeDescriptor? DescriptorOf(Type objType)
    {
        var res = _type2DescriptorMap.GetOrAdd(objType, _buildFromTypeAction);
        if (res != null) _descriptorSet.GetOrAdd(res, true);
        return res;
    }

    ITypeDescriptor? BuildFromType(Type type)
    {
        ITypeDescriptor result;
        lock (_buildTypeLock)
        {
            var buildFromTypeCtx = new BuildFromTypeCtx(this, _type2DescriptorMap, Options);
            buildFromTypeCtx.Create(type);
            buildFromTypeCtx.MergeTypesByShape();
            buildFromTypeCtx.SetNewDescriptors();
            result = buildFromTypeCtx.GetFinalDescriptor(type);
        }

        return result;
    }

    class BuildFromTypeCtx : ITypeDescriptorFactory
    {
        readonly TypeSerializers _typeSerializers;
        readonly ConcurrentDictionary<Type, ITypeDescriptor> _type2DescriptorMap;
        readonly TypeSerializersOptions _typeSerializersOptions;
        readonly Dictionary<Type, ITypeDescriptor> _temporaryMap = new Dictionary<Type, ITypeDescriptor>();

        readonly Dictionary<ITypeDescriptor, ITypeDescriptor> _remap =
            new Dictionary<ITypeDescriptor, ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

        public BuildFromTypeCtx(TypeSerializers typeSerializers,
            ConcurrentDictionary<Type, ITypeDescriptor> type2DescriptorMap, TypeSerializersOptions typeSerializersOptions)
        {
            _typeSerializers = typeSerializers;
            _type2DescriptorMap = type2DescriptorMap;
            _typeSerializersOptions = typeSerializersOptions;
        }

        public ITypeDescriptor? Create(Type type)
        {
            if (_type2DescriptorMap.TryGetValue(type, out var result)) return result;
            if (_temporaryMap.TryGetValue(type, out result)) return result;
            if (!type.IsSubclassOf(typeof(Delegate)))
            {
                if (type.IsGenericType)
                {
                    var typeAlternative = type.SpecializationOf(typeof(IDictionary<,>));
                    if (typeAlternative != null)
                    {
                        result = new DictionaryTypeDescriptor(_typeSerializers, type);
                        goto haveDescriptor;
                    }

                    typeAlternative = type.SpecializationOf(typeof(ICollection<>));
                    if (typeAlternative != null)
                    {
                        result = new ListTypeDescriptor(_typeSerializers, typeAlternative);
                        goto haveDescriptor;
                    }

                    if (_typeSerializers.Options.IgnoreIIndirect &&
                        type.InheritsOrImplements(typeof(IIndirect<>)))
                    {
                        return null;
                    }

                    if (Nullable.GetUnderlyingType(type) != null)
                    {
                        result = new NullableTypeDescriptor(_typeSerializers, type);
                        goto haveDescriptor;
                    }

                    if (type.InheritsOrImplements(typeof(ITuple)))
                    {
                        result = new TupleTypeDescriptor(_typeSerializers, type);
                        goto haveDescriptor;
                    }

                    result = new ObjectTypeDescriptor(_typeSerializers, type, _typeSerializersOptions.TypeDescriptorOptions);
                }
                else if (type.IsArray)
                {
                    var typeAlternative = type.SpecializationOf(typeof(ICollection<>));
                    result = new ListTypeDescriptor(_typeSerializers, typeAlternative!);
                }
                else if (type.IsEnum)
                {
                    result = new EnumTypeDescriptor(_typeSerializers, type);
                }
                else if (type.IsValueType)
                {
                    throw new BTDBException($"Unsupported value type {type.Name}.");
                }
                else
                {
                    result = new ObjectTypeDescriptor(_typeSerializers, type, _typeSerializersOptions.TypeDescriptorOptions);
                }
            }

        haveDescriptor:
            _temporaryMap[type] = result;
            if (result != null)
            {
                if (!result.FinishBuildFromType(this))
                {
                    _temporaryMap.Remove(type);
                    return null;
                }
            }

            return result;
        }

        public void MergeTypesByShape()
        {
            foreach (var typeDescriptor in _temporaryMap)
            {
                var d = typeDescriptor.Value;
                foreach (var existingTypeDescriptor in _type2DescriptorMap)
                {
                    if (d.Equals(existingTypeDescriptor.Value))
                    {
                        _remap[d] = existingTypeDescriptor.Value;
                        break;
                    }
                }
            }

            foreach (var typeDescriptor in _temporaryMap)
            {
                var d = typeDescriptor.Value;
                d.MapNestedTypes(desc =>
                {
                    if (_remap.TryGetValue(desc, out var res)) return res;
                    return desc;
                });
            }
        }

        public ITypeDescriptor? GetFinalDescriptor(Type type)
        {
            if (_temporaryMap.TryGetValue(type, out var result))
            {
                if (_remap.TryGetValue(result, out var result2)) return result2;
                return result;
            }

            return null;
        }

        public void SetNewDescriptors()
        {
            foreach (var typeDescriptor in _temporaryMap)
            {
                var d = typeDescriptor.Value;
                if (_remap.TryGetValue(d, out _)) continue;
                _type2DescriptorMap.TryAdd(d.GetPreferredType(), d);
            }
        }
    }

    public void ForgotAllTypesAndSerializers()
    {
        _loaders.Clear();
        _newDescriptorSavers.Clear();
        foreach (var p in _descriptorSet)
        {
            p.Key.ClearMappingToType();
        }

        _descriptorSet.Clear();
        _type2DescriptorMap =
            new ConcurrentDictionary<Type, ITypeDescriptor>(EnumDefaultTypes(),
                ReferenceEqualityComparer<Type>.Instance);
    }

    static IEnumerable<KeyValuePair<Type, ITypeDescriptor>> EnumDefaultTypes()
    {
        foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
        {
            yield return new KeyValuePair<Type, ITypeDescriptor>(predefinedType.GetPreferredType(), predefinedType);
            var descriptorMultipleNativeTypes = predefinedType as ITypeDescriptorMultipleNativeTypes;
            if (descriptorMultipleNativeTypes == null) continue;
            foreach (var type in descriptorMultipleNativeTypes.GetNativeTypes())
            {
                yield return new KeyValuePair<Type, ITypeDescriptor>(type, predefinedType);
            }
        }
    }

    public Layer1Loader GetLoader(ITypeDescriptor descriptor)
    {
        return _loaders.GetOrAdd(descriptor, _loaderFactoryAction);
    }

    Layer1Loader LoaderFactory(ITypeDescriptor descriptor)
    {
        Type loadAsType = null;
        try
        {
            loadAsType = LoadAsType(descriptor);
        }
        catch (EventSkippedException)
        {
        }

        var methodBuilder = ILBuilder.Instance.NewMethod<Layer1Loader>("DeserializerFor" + descriptor.Name);
        var il = methodBuilder.Generator;
        if (descriptor.AnyOpNeedsCtx())
        {
            var localCtx = il.DeclareLocal(typeof(ITypeBinaryDeserializerContext), "ctx");
            var haveCtx = il.DefineLabel();
            il
                .Ldarg(1)
                .Dup()
                .Stloc(localCtx)
                .Brtrue(haveCtx)
                .Ldarg(2)
                // ReSharper disable once ObjectCreationAsStatement
                .Newobj(() => new DeserializerCtx(null))
                .Castclass(typeof(ITypeBinaryDeserializerContext))
                .Stloc(localCtx)
                .Mark(haveCtx);
            if (loadAsType == null)
                descriptor.GenerateSkip(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldloc(localCtx));
            else
                descriptor.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldloc(localCtx),
                    ilGen => ilGen.Ldarg(3), loadAsType);
        }
        else
        {
            if (loadAsType == null)
                descriptor.GenerateSkip(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldarg(1));
            else
                descriptor.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldarg(1),
                    ilGen => ilGen.Ldarg(3), loadAsType);
        }

        if (loadAsType == null)
        {
            il.Ldnull();
        }
        else if (loadAsType.IsValueType)
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

    public Type LoadAsType(ITypeDescriptor descriptor)
    {
        return descriptor.GetPreferredType() ?? NameToType(descriptor.Name!) ?? typeof(object);
    }

    public Type LoadAsType(ITypeDescriptor descriptor, Type targetType)
    {
        return descriptor.GetPreferredType(targetType) ?? NameToType(descriptor.Name!) ?? typeof(object);
    }

    class DeserializerCtx : ITypeBinaryDeserializerContext
    {
        readonly ITypeSerializersId2LoaderMapping _mapping;
        readonly List<object> _backRefs = new List<object>();

        public DeserializerCtx(ITypeSerializersId2LoaderMapping mapping)
        {
            _mapping = mapping;
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
                return _backRefs[(int)backRefId];
            }

            return _mapping.Load(typeId, ref reader, this);
        }

        public void AddBackRef(object obj)
        {
            _backRefs.Add(obj);
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
                if (backRefId > _backRefs.Count) throw new InvalidDataException();
                return;
            }

            _mapping.Load(typeId, ref reader, this);
        }

        public EncryptedString LoadEncryptedString(ref SpanReader reader)
        {
            var cipher = _mapping.GetSymmetricCipher();
            var enc = reader.ReadByteArray();
            var size = cipher!.CalcPlainSizeFor(enc);
            var dec = new byte[size];
            if (!cipher.Decrypt(enc, dec))
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

    public Layer1SimpleSaver GetSimpleSaver(ITypeDescriptor descriptor, Type type)
    {
        return _simpleSavers.GetOrAdd((descriptor, type), NewSimpleSaver);
    }

    static Layer1SimpleSaver? NewSimpleSaver((ITypeDescriptor descriptor, Type type) v)
    {
        var (descriptor, type) = v;
        if (descriptor.AnyOpNeedsCtx()) return null;
        var method =
            ILBuilder.Instance.NewMethod<Layer1SimpleSaver>(descriptor.Name + "SimpleSaver");
        var il = method.Generator;
        descriptor.GenerateSave(il, ilgen => ilgen.Ldarg(0), null, ilgen =>
        {
            ilgen.Ldarg(1);
            if (type != typeof(object))
            {
                ilgen.UnboxAny(type);
            }
        }, type);
        il.Ret();
        return method.Create();
    }

    public Layer1ComplexSaver GetComplexSaver(ITypeDescriptor descriptor, Type type)
    {
        return _complexSavers.GetOrAdd((descriptor, type), NewComplexSaver);
    }

    static Layer1ComplexSaver NewComplexSaver((ITypeDescriptor descriptor, Type type) v)
    {
        var (descriptor, type) = v;
        var method = ILBuilder.Instance.NewMethod<Layer1ComplexSaver>(descriptor.Name + "ComplexSaver");
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

    public Action<object, IDescriptorSerializerLiteContext>? GetNewDescriptorSaver(ITypeDescriptor descriptor,
        Type preciseType)
    {
        return _newDescriptorSavers.GetOrAdd((descriptor, preciseType), NewDescriptorSaverFactory);
    }

    static Action<object, IDescriptorSerializerLiteContext>? NewDescriptorSaverFactory((ITypeDescriptor descriptor, Type type) pair)
    {
        var gen = pair.descriptor.BuildNewDescriptorGenerator();
        if (gen == null)
        {
            return null;
        }

        var method =
            ILBuilder.Instance.NewMethod<Action<object, IDescriptorSerializerLiteContext>>(
                "GatherAllObjectsForTypeExtraction_" + pair.descriptor.Name);
        var il = method.Generator;
        gen.GenerateTypeIterator(il, ilgen =>
        {
            ilgen.Ldarg(0);
            if (pair.type.IsValueType)
            {
                ilgen.UnboxAny(pair.type);
            }
        }, ilgen => ilgen.Ldarg(1), pair.type);
        il.Ret();
        return method.Create();
    }

    public ITypeSerializersMapping CreateMapping()
    {
        return new TypeSerializersMapping(this);
    }

    public static void StoreDescriptor(ITypeDescriptor descriptor, ref SpanWriter writer, Func<ITypeDescriptor, uint> descriptor2Id)
    {
        switch (descriptor)
        {
            case ListTypeDescriptor:
                writer.WriteUInt8((byte)TypeCategory.List);
                break;
            case DictionaryTypeDescriptor:
                writer.WriteUInt8((byte)TypeCategory.Dictionary);
                break;
            case ObjectTypeDescriptor:
                writer.WriteUInt8((byte)TypeCategory.Class);
                break;
            case EnumTypeDescriptor:
                writer.WriteUInt8((byte)TypeCategory.Enum);
                break;
            case NullableTypeDescriptor:
                writer.WriteUInt8((byte)TypeCategory.Nullable);
                break;
            case TupleTypeDescriptor:
                writer.WriteUInt8((byte)TypeCategory.Tuple);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }

        ((IPersistTypeDescriptor)descriptor).Persist(ref writer, (ref SpanWriter w, ITypeDescriptor d) => w.WriteVUInt32(descriptor2Id(d)));
    }

    public ITypeDescriptor MergeDescriptor(ITypeDescriptor descriptor)
    {
        foreach (var (typeDescriptor, _) in _descriptorSet)
        {
            if (descriptor.Equals(typeDescriptor))
            {
                return typeDescriptor;
            }
        }

        _descriptorSet.GetOrAdd(descriptor, true);
        return descriptor;
    }

    public string TypeToName(Type type)
    {
        return _typeNameMapper.ToName(type);
    }

    Type? NameToType(string name)
    {
        return _typeNameMapper.ToType(name);
    }

    public ISymmetricCipher GetSymmetricCipher()
    {
        return _symmetricCipher;
    }
}
