using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;

namespace BTDB.EventStoreLayer
{
    public class TypeSerializers : ITypeSerializers, ITypeSerializerMappingFactory
    {
        ITypeNameMapper _typeNameMapper;
        readonly TypeSerializersOptions _options;

        readonly ConcurrentDictionary<ITypeDescriptor, Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, ITypeDescriptor, object>> _loaders = new ConcurrentDictionary<ITypeDescriptor, Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, ITypeDescriptor, object>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, Action<object, IDescriptorSerializerLiteContext>> _newDescriptorSavers = new ConcurrentDictionary<ITypeDescriptor, Action<object, IDescriptorSerializerLiteContext>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, bool> _descriptorSet = new ConcurrentDictionary<ITypeDescriptor, bool>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        ConcurrentDictionary<Type, ITypeDescriptor> _type2DescriptorMap = new ConcurrentDictionary<Type, ITypeDescriptor>(ReferenceEqualityComparer<Type>.Instance);
        readonly object _buildTypeLock = new object();
        readonly ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, object>> _simpleSavers = new ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, object>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>> _complexSavers = new ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

        // created funcs just once as optimization
        readonly Func<ITypeDescriptor, Action<AbstractBufferedWriter, object>> _newSimpleSaverAction;
        readonly Func<ITypeDescriptor, Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>> _newComplexSaverAction;
        readonly Func<ITypeDescriptor, Action<object, IDescriptorSerializerLiteContext>> _newDescriptorSaverFactoryAction;
        readonly Func<ITypeDescriptor, Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, ITypeDescriptor, object>> _loaderFactoryAction;
        readonly Func<Type, ITypeDescriptor> _buildFromTypeAction;
        Type _preciseType;

        public TypeSerializers(ITypeNameMapper typeNameMapper = null, TypeSerializersOptions options = null)
        {
            ConvertorGenerator = DefaultTypeConvertorGenerator.Instance;
            SetTypeNameMapper(typeNameMapper);
            ForgotAllTypesAndSerializers();
            _newSimpleSaverAction = NewSimpleSaver;
            _newComplexSaverAction = NewComplexSaver;
            _newDescriptorSaverFactoryAction = NewDescriptorSaverFactory;
            _loaderFactoryAction = LoaderFactory;
            _buildFromTypeAction = BuildFromType;
            _options = options ?? TypeSerializersOptions.Default;
        }

        public void SetTypeNameMapper(ITypeNameMapper typeNameMapper)
        {
            _typeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
        }

        public ITypeDescriptor DescriptorOf(object obj)
        {
            if (obj == null) return null;
            var knowDescriptor = obj as IKnowDescriptor;
            if (knowDescriptor != null) return knowDescriptor.GetDescriptor();
            return DescriptorOf(obj.GetType());
        }

        public bool IsSafeToLoad(Type type)
        {
            return DescriptorOf(type) != null;
        }

        public ITypeConvertorGenerator ConvertorGenerator { get; private set; }

        public ITypeNameMapper TypeNameMapper => _typeNameMapper;

        public ITypeDescriptor DescriptorOf(Type objType)
        {
            var res = _type2DescriptorMap.GetOrAdd(objType, _buildFromTypeAction);
            if (res != null) _descriptorSet.GetOrAdd(res, true);
            return res;
        }

        ITypeDescriptor BuildFromType(Type type)
        {
            ITypeDescriptor result;
            lock (_buildTypeLock)
            {
                var buildFromTypeCtx = new BuildFromTypeCtx(this, _type2DescriptorMap);
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
            readonly Dictionary<Type, ITypeDescriptor> _temporaryMap = new Dictionary<Type, ITypeDescriptor>();
            readonly Dictionary<ITypeDescriptor, ITypeDescriptor> _remap = new Dictionary<ITypeDescriptor, ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

            public BuildFromTypeCtx(TypeSerializers typeSerializers, ConcurrentDictionary<Type, ITypeDescriptor> type2DescriptorMap)
            {
                _typeSerializers = typeSerializers;
                _type2DescriptorMap = type2DescriptorMap;
            }

            public ITypeDescriptor Create(Type type)
            {
                ITypeDescriptor result;
                if (_type2DescriptorMap.TryGetValue(type, out result)) return result;
                if (_temporaryMap.TryGetValue(type, out result)) return result;
                if (!type.IsSubclassOf(typeof(Delegate)))
                {
                    if (type.IsGenericType)
                    {
                        if (type.GetGenericTypeDefinition().InheritsOrImplements(typeof(IList<>)))
                        {
                            result = new ListTypeDescriptor(_typeSerializers, type);
                        }
                        else if (type.GetGenericTypeDefinition().InheritsOrImplements(typeof(IDictionary<,>)))
                        {
                            result = new DictionaryTypeDescriptor(_typeSerializers, type);
                        }
                        else if (_typeSerializers._options.IgnoreIIndirect &&
                            type.GetGenericTypeDefinition().InheritsOrImplements(typeof(IIndirect<>)))
                        {
                            return null;
                        }
                        else if (Nullable.GetUnderlyingType(type) != null)
                        {
                            result = new NullableTypeDescriptor(_typeSerializers, type);
                        }
                        else
                        {
                            result = new ObjectTypeDescriptor(_typeSerializers, type);
                        }
                    }
                    else if (type.IsArray)
                    {
                        result = new ListTypeDescriptor(_typeSerializers, type);
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
                        result = new ObjectTypeDescriptor(_typeSerializers, type);
                    }
                }
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
                            ITypeDescriptor res;
                            if (_remap.TryGetValue(desc, out res)) return res;
                            return desc;
                        });

                }
            }

            public ITypeDescriptor GetFinalDescriptor(Type type)
            {
                ITypeDescriptor result;
                if (_temporaryMap.TryGetValue(type, out result))
                {
                    ITypeDescriptor result2;
                    if (_remap.TryGetValue(result, out result2)) return result2;
                    return result;
                }
                return null;
            }

            public void SetNewDescriptors()
            {
                foreach (var typeDescriptor in _temporaryMap)
                {
                    var d = typeDescriptor.Value;
                    ITypeDescriptor result;
                    if (_remap.TryGetValue(d, out result)) continue;
                    _type2DescriptorMap.TryAdd(d.GetPreferedType(), d);
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
            _type2DescriptorMap = new ConcurrentDictionary<Type, ITypeDescriptor>(EnumDefaultTypes(), ReferenceEqualityComparer<Type>.Instance);
        }

        static IEnumerable<KeyValuePair<Type, ITypeDescriptor>> EnumDefaultTypes()
        {
            foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
            {
                yield return new KeyValuePair<Type, ITypeDescriptor>(predefinedType.GetPreferedType(), predefinedType);
                var descriptorMultipleNativeTypes = predefinedType as ITypeDescriptorMultipleNativeTypes;
                if (descriptorMultipleNativeTypes == null) continue;
                foreach (var type in descriptorMultipleNativeTypes.GetNativeTypes())
                {
                    yield return new KeyValuePair<Type, ITypeDescriptor>(type, predefinedType);
                }
            }
        }

        public Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, ITypeDescriptor, object> GetLoader(ITypeDescriptor descriptor)
        {
            return _loaders.GetOrAdd(descriptor, _loaderFactoryAction);
        }

        Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, ITypeDescriptor, object> LoaderFactory(ITypeDescriptor descriptor)
        {
            Type loadAsType = null;
            try
            {
                loadAsType = LoadAsType(descriptor);
            }
            catch (EventSkippedException)
            {
            }
            var methodBuilder = ILBuilder.Instance.NewMethod<Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, ITypeDescriptor, object>>("DeserializerFor" + descriptor.Name);
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
                    .Ldarg(0)
                    .Ldarg(2)
                    .Newobj(() => new DeserializerCtx(null, null))
                    .Castclass(typeof(ITypeBinaryDeserializerContext))
                    .Stloc(localCtx)
                    .Mark(haveCtx);
                if (loadAsType == null)
                    descriptor.GenerateSkip(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldloc(localCtx));
                else
                    descriptor.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldloc(localCtx), ilGen => ilGen.Ldarg(3), loadAsType);
            }
            else
            {
                if (loadAsType == null)
                    descriptor.GenerateSkip(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldarg(1));
                else
                    descriptor.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldarg(1), ilGen => ilGen.Ldarg(3), loadAsType);
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
            return descriptor.GetPreferedType() ?? NameToType(descriptor.Name) ?? typeof(object);
        }

        public Type LoadAsType(ITypeDescriptor descriptor, Type targetType)
        {
            return descriptor.GetPreferedType(targetType) ?? NameToType(descriptor.Name) ?? typeof(object);
        }

        class DeserializerCtx : ITypeBinaryDeserializerContext
        {
            readonly AbstractBufferedReader _reader;
            readonly ITypeSerializersId2LoaderMapping _mapping;
            readonly List<object> _backRefs = new List<object>();

            public DeserializerCtx(AbstractBufferedReader reader, ITypeSerializersId2LoaderMapping mapping)
            {
                _reader = reader;
                _mapping = mapping;
            }

            public object LoadObject()
            {
                var typeId = _reader.ReadVUInt32();
                if (typeId == 0)
                {
                    return null;
                }
                if (typeId == 1)
                {
                    var backRefId = _reader.ReadVUInt32();
                    return _backRefs[(int)backRefId];
                }
                return _mapping.Load(typeId, _reader, this);
            }

            public void AddBackRef(object obj)
            {
                _backRefs.Add(obj);
            }

            public void SkipObject()
            {
                var typeId = _reader.ReadVUInt32();
                if (typeId == 0)
                {
                    return;
                }
                if (typeId == 1)
                {
                    var backRefId = _reader.ReadVUInt32();
                    if (backRefId > _backRefs.Count) throw new InvalidDataException();
                    return;
                }
                _mapping.Load(typeId, _reader, this);
            }
        }

        public Action<AbstractBufferedWriter, object> GetSimpleSaver(ITypeDescriptor descriptor)
        {
            return _simpleSavers.GetOrAdd(descriptor, _newSimpleSaverAction);
        }

        Action<AbstractBufferedWriter, object> NewSimpleSaver(ITypeDescriptor descriptor)
        {
            if (descriptor.AnyOpNeedsCtx()) return null;
            var method = ILBuilder.Instance.NewMethod<Action<AbstractBufferedWriter, object>>(descriptor.Name + "SimpleSaver");
            var il = method.Generator;
            descriptor.GenerateSave(il, ilgen => ilgen.Ldarg(0), null, ilgen =>
                {
                    ilgen.Ldarg(1);
                    var type = descriptor.GetPreferedType();
                    if (type != typeof(object))
                    {
                        ilgen.UnboxAny(type);
                    }
                }, descriptor.GetPreferedType());
            il.Ret();
            return method.Create();
        }

        public Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object> GetComplexSaver(ITypeDescriptor descriptor)
        {
            return _complexSavers.GetOrAdd(descriptor, _newComplexSaverAction);
        }

        Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object> NewComplexSaver(ITypeDescriptor descriptor)
        {
            var method = ILBuilder.Instance.NewMethod<Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>>(descriptor.Name + "ComplexSaver");
            var il = method.Generator;
            descriptor.GenerateSave(il, ilgen => ilgen.Ldarg(0), ilgen => ilgen.Ldarg(1), ilgen =>
            {
                ilgen.Ldarg(2);
                var type = descriptor.GetPreferedType();
                if (type != typeof(object))
                {
                    ilgen.UnboxAny(type);
                }
            }, descriptor.GetPreferedType());
            il.Ret();
            return method.Create();
        }

        public Action<object, IDescriptorSerializerLiteContext> GetNewDescriptorSaver(ITypeDescriptor descriptor, Type preciseType)
        {
            _preciseType = preciseType;
            return _newDescriptorSavers.GetOrAdd(descriptor, _newDescriptorSaverFactoryAction);
        }

        Action<object, IDescriptorSerializerLiteContext> NewDescriptorSaverFactory(ITypeDescriptor descriptor)
        {
            var gen = descriptor.BuildNewDescriptorGenerator();
            if (gen == null)
            {
                return null;
            }
            var method = ILBuilder.Instance.NewMethod<Action<object, IDescriptorSerializerLiteContext>>("GatherAllObjectsForTypeExtraction_" + descriptor.Name);
            var il = method.Generator;
            gen.GenerateTypeIterator(il, ilgen => ilgen.Ldarg(0), ilgen => ilgen.Ldarg(1), _preciseType);
            il.Ret();
            return method.Create();
        }

        public ITypeSerializersMapping CreateMapping()
        {
            return new TypeSerializersMapping(this);
        }

        public void StoreDescriptor(ITypeDescriptor descriptor, AbstractBufferedWriter writer, Func<ITypeDescriptor, uint> descriptor2Id)
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
            else
            {
                throw new ArgumentOutOfRangeException();
            }
            var p = descriptor as IPersistTypeDescriptor;
            p.Persist(writer, (w, d) => w.WriteVUInt32(descriptor2Id(d)));
        }

        public ITypeDescriptor MergeDescriptor(ITypeDescriptor descriptor)
        {
            foreach (var existingTypeDescriptor in _descriptorSet)
            {
                if (descriptor.Equals(existingTypeDescriptor.Key))
                {
                    return existingTypeDescriptor.Key;
                }
            }
            _descriptorSet.GetOrAdd(descriptor, true);
            return descriptor;
        }

        public string TypeToName(Type type)
        {
            return _typeNameMapper.ToName(type);
        }

        Type NameToType(string name)
        {
            return _typeNameMapper.ToType(name);
        }
    }
}
