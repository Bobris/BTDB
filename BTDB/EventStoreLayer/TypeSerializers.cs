using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class TypeSerializers : ITypeSerializers
    {
        public static readonly List<ITypeDescriptor> PredefinedTypes = new List<ITypeDescriptor>();
        ITypeNameMapper _typeNameMapper;
        readonly ConcurrentDictionary<ITypeDescriptor, Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, object>> _loaders = new ConcurrentDictionary<ITypeDescriptor, Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, object>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, Action<object, IDescriptorSerializerLiteContext>> _newDescriptorSavers = new ConcurrentDictionary<ITypeDescriptor, Action<object, IDescriptorSerializerLiteContext>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, bool> _descriptorSet = new ConcurrentDictionary<ITypeDescriptor, bool>();
        ConcurrentDictionary<Type, ITypeDescriptor> _type2DescriptorMap = new ConcurrentDictionary<Type, ITypeDescriptor>(ReferenceEqualityComparer<Type>.Instance);
        readonly object _buildTypeLock = new object();
        readonly ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, object>> _simpleSavers = new ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, object>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>> _complexSavers = new ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

        static TypeSerializers()
        {
            PredefinedTypes.Add(new StringTypeDescriptor());
            PredefinedTypes.Add(new Int8TypeDescriptor());
            PredefinedTypes.Add(new UInt8TypeDescriptor());
            PredefinedTypes.Add(new VInt16TypeDescriptor());
            PredefinedTypes.Add(new VUInt16TypeDescriptor());
            PredefinedTypes.Add(new VInt32TypeDescriptor());
            PredefinedTypes.Add(new VUInt32TypeDescriptor());
            PredefinedTypes.Add(new VInt64TypeDescriptor());
            PredefinedTypes.Add(new VUInt64TypeDescriptor());
        }

        public TypeSerializers()
        {
            ForgotAllTypesAndSerializers();
        }

        public void SetTypeNameMapper(ITypeNameMapper typeNameMapper)
        {
            _typeNameMapper = typeNameMapper;
        }

        public ITypeDescriptor DescriptorOf(Type objType)
        {
            return _type2DescriptorMap.GetOrAdd(objType, BuildFromType);
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
                        if (type.GetGenericTypeDefinition() == typeof(IList<>) ||
                            type.GetGenericTypeDefinition() == typeof(List<>))
                        {
                            result = new ListTypeDescriptor(_typeSerializers, type);
                        }
                        else if (type.GetGenericTypeDefinition() == typeof(IDictionary<,>) ||
                            type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
                        {
                            result = new DictionaryTypeDescriptor(_typeSerializers, type);
                        }
                    }
                    else
                    {
                        result = new ObjectTypeDescriptor(_typeSerializers, type);
                    }
                }
                _temporaryMap[type] = result;
                if (result != null)
                    result.FinishBuildFromType(this);
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
                throw new InvalidOperationException();
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
            _type2DescriptorMap = new ConcurrentDictionary<Type, ITypeDescriptor>(EnumDefaultTypes(), ReferenceEqualityComparer<Type>.Instance);
        }

        static IEnumerable<KeyValuePair<Type, ITypeDescriptor>> EnumDefaultTypes()
        {
            foreach (var predefinedType in PredefinedTypes)
            {
                yield return new KeyValuePair<Type, ITypeDescriptor>(predefinedType.GetPreferedType(), predefinedType);
            }
        }

        public Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, object> GetLoader(ITypeDescriptor descriptor)
        {
            return _loaders.GetOrAdd(descriptor, LoaderFactory);
        }

        Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, object> LoaderFactory(ITypeDescriptor descriptor)
        {
            var loadAsType = LoadAsType(descriptor);
            var loadDeserializer = descriptor.BuildBinaryDeserializerGenerator(loadAsType);
            var methodBuilder = ILBuilder.Instance.NewMethod<Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, object>>("DeserializerFor" + descriptor.Name);
            var il = methodBuilder.Generator;
            if (loadDeserializer.LoadNeedsCtx())
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
                loadDeserializer.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldloc(localCtx));
            }
            else
            {
                loadDeserializer.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldarg(1));
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

        public Type LoadAsType(ITypeDescriptor descriptor)
        {
            return descriptor.GetPreferedType() ??
                   (_typeNameMapper != null ? _typeNameMapper.ToType(descriptor.Name) : typeof(object))
                   ?? typeof(object);
        }

        public class DeserializerCtx : ITypeBinaryDeserializerContext
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
                return _mapping.GetLoader(typeId)(_reader, this, _mapping);
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
                _mapping.GetLoader(typeId)(_reader, this, _mapping);
            }
        }

        public Action<AbstractBufferedWriter, object> GetSimpleSaver(ITypeDescriptor descriptor)
        {
            return _simpleSavers.GetOrAdd(descriptor, NewSimpleSaver);
        }

        Action<AbstractBufferedWriter, object> NewSimpleSaver(ITypeDescriptor descriptor)
        {
            var generator = descriptor.BuildBinarySerializerGenerator();
            if (generator.SaveNeedsCtx()) return null;
            var method = ILBuilder.Instance.NewMethod<Action<AbstractBufferedWriter, object>>(descriptor.Name + "SimpleSaver");
            var il = method.Generator;
            generator.GenerateSave(il, ilgen => ilgen.Ldarg(0), null, ilgen =>
                {
                    ilgen.Ldarg(1);
                    var type = descriptor.GetPreferedType();
                    if (type != typeof(object))
                    {
                        ilgen.UnboxAny(type);
                    }
                });
            il.Ret();
            return method.Create();
        }

        public Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object> GetComplexSaver(ITypeDescriptor descriptor)
        {
            return _complexSavers.GetOrAdd(descriptor, NewComplexSaver);
        }

        Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object> NewComplexSaver(ITypeDescriptor descriptor)
        {
            var generator = descriptor.BuildBinarySerializerGenerator();
            var method = ILBuilder.Instance.NewMethod<Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>>(descriptor.Name + "ComplexSaver");
            var il = method.Generator;
            generator.GenerateSave(il, ilgen => ilgen.Ldarg(0), ilgen => ilgen.Ldarg(1), ilgen =>
            {
                ilgen.Ldarg(2);
                var type = descriptor.GetPreferedType();
                if (type != typeof(object))
                {
                    ilgen.UnboxAny(type);
                }
            });
            il.Ret();
            return method.Create();
        }

        public Action<object, IDescriptorSerializerLiteContext> GetNewDescriptorSaver(ITypeDescriptor descriptor)
        {
            return _newDescriptorSavers.GetOrAdd(descriptor, NewDescriptorSaverFactory);
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
            gen.GenerateTypeIterator(il, ilgen => ilgen.Ldarg(0), ilgen => ilgen.Ldarg(1));
            il.Ret();
            return method.Create();
        }

        public TypeCategory GetTypeCategory(ITypeDescriptor descriptor)
        {
            return TypeCategory.BuildIn;
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
            else
            {
                throw new ArgumentOutOfRangeException();
            }
            var p = descriptor as IPersistTypeDescriptor;
            p.Persist(writer, (w, d) => w.WriteVUInt32(descriptor2Id(d)));
        }

        public ITypeDescriptor MergeDescriptor(ITypeDescriptor descriptor)
        {
            foreach (var existingTypeDescriptor in _type2DescriptorMap)
            {
                if (descriptor.Equals(existingTypeDescriptor.Value))
                {
                    return existingTypeDescriptor.Value;
                }
            }
            return descriptor;
        }

        public string TypeToName(Type type)
        {
            var typeNameMapper = _typeNameMapper;
            if (typeNameMapper == null) return type.FullName;
            return typeNameMapper.ToName(type);
        }

        public Type NameToType(string name)
        {
            var typeNameMapper = _typeNameMapper;
            if (typeNameMapper == null) return Type.GetType(name, false);
            return typeNameMapper.ToType(name);
        }
    }
}