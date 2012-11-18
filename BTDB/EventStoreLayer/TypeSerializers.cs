using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal class TypeSerializers : ITypeSerializers
    {
        public static readonly List<ITypeDescriptor> PredefinedTypes = new List<ITypeDescriptor>();
        static readonly Dictionary<Type, int> PredefinedAdditionalTypes = new Dictionary<Type, int>();
        ITypeNameMapper _typeNameMapper;
        readonly ConcurrentDictionary<ITypeDescriptor, Func<AbstractBufferedReader, object>> _loaders = new ConcurrentDictionary<ITypeDescriptor, Func<AbstractBufferedReader, object>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, Action<IDescriptorSerializerContext>> _newDescriptorSavers = new ConcurrentDictionary<ITypeDescriptor, Action<IDescriptorSerializerContext>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, bool> _descriptorSet = new ConcurrentDictionary<ITypeDescriptor, bool>();
        ConcurrentDictionary<Type, ITypeDescriptor> _type2DescriptorMap = new ConcurrentDictionary<Type, ITypeDescriptor>(ReferenceEqualityComparer<Type>.Instance);
        readonly object _buildTypeLock = new object();
        readonly ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, object>> _simpleSavers = new ConcurrentDictionary<ITypeDescriptor, Action<AbstractBufferedWriter, object>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

        static TypeSerializers()
        {
            PredefinedTypes.Add(new StringTypeDescriptor());
            PredefinedTypes.Add(new Uint8TypeDescriptor());
            PredefinedTypes.Add(new Int8TypeDescriptor());
            PredefinedAdditionalTypes.Add(typeof(short), PredefinedTypes.Count);
            PredefinedAdditionalTypes.Add(typeof(int), PredefinedTypes.Count);
            PredefinedTypes.Add(new VIntTypeDescriptor());
            PredefinedAdditionalTypes.Add(typeof(ushort), PredefinedTypes.Count);
            PredefinedAdditionalTypes.Add(typeof(uint), PredefinedTypes.Count);
            PredefinedTypes.Add(new VuIntTypeDescriptor());
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
                var buildFromTypeCtx = new BuildFromTypeCtx(_type2DescriptorMap);
                buildFromTypeCtx.Create(type);
                buildFromTypeCtx.MergeTypesByShape();
                result = buildFromTypeCtx.GetFinalDescriptor(type);
            }
            return result;
        }

        class BuildFromTypeCtx : ITypeDescriptorFactory
        {
            readonly ConcurrentDictionary<Type, ITypeDescriptor> _type2DescriptorMap;
            readonly Dictionary<Type, ITypeDescriptor> _temporaryMap = new Dictionary<Type, ITypeDescriptor>();

            public BuildFromTypeCtx(ConcurrentDictionary<Type, ITypeDescriptor> type2DescriptorMap)
            {
                _type2DescriptorMap = type2DescriptorMap;
            }

            public ITypeDescriptor Create(Type type)
            {
                ITypeDescriptor result;
                if (_type2DescriptorMap.TryGetValue(type, out result)) return result;
                if (_temporaryMap.TryGetValue(type, out result)) return result;
                if (type.IsSubclassOf(typeof(Delegate)))
                {
                    result = null;
                }
                else
                {
                    result = new ObjectTypeDescriptor(type);
                }
                _temporaryMap[type] = result;
                if (result != null)
                    result.FinishBuildFromType(this);
                return result;
            }

            public void MergeTypesByShape()
            {
                throw new NotImplementedException();
            }

            public ITypeDescriptor GetFinalDescriptor(Type type)
            {
                ITypeDescriptor result;
                if (_temporaryMap.TryGetValue(type, out result))
                {
                    return result;
                }
                throw new InvalidOperationException();
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
            foreach (var predefinedAdditionalType in PredefinedAdditionalTypes)
            {
                yield return new KeyValuePair<Type, ITypeDescriptor>(predefinedAdditionalType.Key, PredefinedTypes[predefinedAdditionalType.Value]);
            }
        }

        public Func<AbstractBufferedReader, object> GetLoader(ITypeDescriptor descriptor)
        {
            return _loaders.GetOrAdd(descriptor, LoaderFactory);
        }

        Func<AbstractBufferedReader, object> LoaderFactory(ITypeDescriptor descriptor)
        {
            var loadAsType = descriptor.GetPreferedType() ??
                             (_typeNameMapper != null ? _typeNameMapper.ToType(descriptor.Name) : typeof(object))
                             ?? typeof(object);
            var loadDeserializer = descriptor.BuildBinaryDeserializerGenerator(loadAsType);
            var methodBuilder = ILBuilder.Instance.NewMethod<Func<AbstractBufferedReader, object>>("DeserializerFor" + descriptor.Name);
            var il = methodBuilder.Generator;
            if (loadDeserializer.LoadNeedsCtx())
            {
                var localCtx = il.DeclareLocal(typeof(List<object>), "ctx");
                il
                    .Newobj(() => new List<object>())
                    .Dup()
                    .Stloc(localCtx)
                    .Ldnull()
                    .Callvirt(() => ((List<object>)null).Add(null));
                loadDeserializer.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldloc(localCtx));
            }
            else
            {
                loadDeserializer.GenerateLoad(il, ilGen => ilGen.Ldarg(0), null);
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
                    if (type.IsValueType)
                    {
                        ilgen.Unbox(type);
                    }
                    else if (type != typeof(object))
                    {
                        ilgen.Castclass(type);
                    }
                });
            il.Ret();
            return method.Create();
        }

        public Action<IDescriptorSerializerContext> GetNewDescriptorSaver(ITypeDescriptor descriptor)
        {
            return _newDescriptorSavers.GetOrAdd(descriptor, NewDescriptorSaverFactory);
        }

        Action<IDescriptorSerializerContext> NewDescriptorSaverFactory(ITypeDescriptor descriptor)
        {
            var gen = descriptor.BuildNewDescriptorGenerator();
            if (gen == null)
            {
                return null;
            }
            throw new NotImplementedException();
        }

        public TypeCategory GetTypeCategory(ITypeDescriptor descriptor)
        {
            return TypeCategory.BuildIn;
        }

        public ITypeSerializersMapping CreateMapping()
        {
            return new TypeSerializersMapping(this);
        }
    }
}