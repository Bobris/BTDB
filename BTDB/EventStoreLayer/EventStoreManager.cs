using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    public class EventStoreManager : IEventStoreManager
    {
        ITypeNameMapper _mapper;
        readonly TypeSerializers _typeSerializers = new TypeSerializers();

        public void SetNewTypeNameMapper(ITypeNameMapper mapper)
        {
            _mapper = mapper;
            _typeSerializers.SetTypeNameMapper(mapper);
        }

        public void ForgotAllTypesAndSerializers()
        {
            _typeSerializers.ForgotAllTypesAndSerializers();
        }

        public IReadEventStore OpenReadOnlyStore(IEventFileStorage file)
        {
            return new ReadOnlyEventStore(this, file);
        }

        public IWriteEventStore AppendToStore(IEventFileStorage file)
        {
            throw new System.NotImplementedException();
        }
    }

    internal class TypeSerializers
    {
        static readonly List<ITypeDescriptor> PredefinedTypes = new List<ITypeDescriptor>();
        static readonly Dictionary<Type, int> PredefinedAdditionalTypes = new Dictionary<Type, int>();
        ITypeNameMapper _typeNameMapper;
        readonly ConcurrentDictionary<ITypeDescriptor, Func<AbstractBufferedReader, object>> _loaders = new ConcurrentDictionary<ITypeDescriptor, Func<AbstractBufferedReader, object>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, Action<IDescriptorSerializerContext>> _newDescriptorSavers = new ConcurrentDictionary<ITypeDescriptor, Action<IDescriptorSerializerContext>>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly ConcurrentDictionary<ITypeDescriptor, bool> _descriptorSet = new ConcurrentDictionary<ITypeDescriptor, bool>();
        ConcurrentDictionary<Type, ITypeDescriptor> _type2DescriptorMap = new ConcurrentDictionary<Type, ITypeDescriptor>(ReferenceEqualityComparer<Type>.Instance);

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
            return null;
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
            throw new NotImplementedException();
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

        public Action<AbstractBufferedWriter> GetSaver(ITypeDescriptor descriptor)
        {
            throw new NotImplementedException();
        }

        public Action<IDescriptorSerializerContext> GetNewDescriptorSaver(ITypeDescriptor descriptor)
        {
            return _newDescriptorSavers.GetOrAdd(descriptor, NewDescriptorSaverFactory);
        }

        Action<IDescriptorSerializerContext> NewDescriptorSaverFactory(ITypeDescriptor descriptor)
        {

            throw new NotImplementedException();
        }

        public TypeCategory GetTypeCategory(ITypeDescriptor descriptor)
        {
            return TypeCategory.BuildIn;
        }
    }

    public interface IDescriptorSerializerContext
    {

    }

    public enum TypeCategory : byte
    {
        BuildIn,
        Class,
        List,
        Dictionary
    }

    internal class TypeSerializersMapping
    {
        readonly List<ITypeDescriptor> _id2DescriptorMap = new List<ITypeDescriptor>();
        readonly Dictionary<ITypeDescriptor, int> _descriptor2IdMap = new Dictionary<ITypeDescriptor, int>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly TypeSerializers _typeSerializers;

        public TypeSerializersMapping(TypeSerializers typeSerializers)
        {
            _typeSerializers = typeSerializers;
        }

        public void LoadTypeDescriptors(AbstractBufferedReader reader)
        {
            var typeId = reader.ReadVUInt32();
            var typeCategory = (TypeCategory)reader.ReadUInt8();
            switch (typeCategory)
            {
                case TypeCategory.BuildIn:
                    throw new ArgumentOutOfRangeException();
                case TypeCategory.Class:
                    break;
                case TypeCategory.List:
                    break;
                case TypeCategory.Dictionary:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public object LoadObject(AbstractBufferedReader reader)
        {
            var typeId = reader.ReadVUInt32();
            if (typeId == 0)
            {
                return null;
            }
            var descriptor = _id2DescriptorMap[(int)typeId];
            return _typeSerializers.GetLoader(descriptor)(reader);
        }

        public IDescriptorSerializerContext StoreNewDescriptors(AbstractBufferedWriter writer, object obj)
        {
            if (obj == null) return null;
            int typeId;
            var objType = obj.GetType();
            Action<IDescriptorSerializerContext> action;
            DescriptorSerializerContext ctx = null;
            var descriptor = _typeSerializers.DescriptorOf(objType);
            if (_descriptor2IdMap.TryGetValue(descriptor, out typeId))
            {
                action = _typeSerializers.GetNewDescriptorSaver(descriptor);
            }
            else
            {
                ctx = new DescriptorSerializerContext(this);
                ctx.AddDescriptor(descriptor);
                action = _typeSerializers.GetNewDescriptorSaver(descriptor);
            }
            if (action != null)
            {
                if (ctx == null) ctx = new DescriptorSerializerContext(this);
                action(ctx);
            }
            return ctx;
        }

        public void CommitNewDescriptors(IDescriptorSerializerContext context)
        {

        }

        internal class DescriptorSerializerContext : IDescriptorSerializerContext
        {
            readonly TypeSerializersMapping _typeSerializersMapping;

            public DescriptorSerializerContext(TypeSerializersMapping typeSerializersMapping)
            {
                _typeSerializersMapping = typeSerializersMapping;
            }

            public void AddDescriptor(ITypeDescriptor descriptor)
            {
                throw new NotImplementedException();
            }
        }

        public void StoreObject(AbstractBufferedWriter writer, object obj)
        {
            if (obj == null)
            {
                writer.WriteUInt8(0);
                return;
            }
            int typeId;
            var descriptor = _typeSerializers.DescriptorOf(obj.GetType());
            if (_descriptor2IdMap.TryGetValue(descriptor, out typeId))
            {
                writer.WriteVUInt32((uint)typeId);
                _typeSerializers.GetSaver(descriptor)(writer);
            }
        }
    }
}