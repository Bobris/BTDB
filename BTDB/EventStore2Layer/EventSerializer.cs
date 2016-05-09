using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer
{
    public class EventSerializer : IEventSerializer, ITypeDescriptorCallbacks
    {
        public const int ReservedBuildinTypes = 50;
        readonly ITypeNameMapper _typeNameMapper;
        readonly Dictionary<object, SerializerTypeInfo> _typeOrDescriptor2Info = new Dictionary<object, SerializerTypeInfo>(ReferenceEqualityComparer<object>.Instance);
        readonly List<SerializerTypeInfo> _id2Info = new List<SerializerTypeInfo>();

        public EventSerializer(ITypeNameMapper typeNameMapper = null, ITypeConvertorGenerator typeConvertorGenerator = null)
        {
            _typeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
            ConvertorGenerator = typeConvertorGenerator ?? new DefaultTypeConvertorGenerator();
            _id2Info.Add(null); // 0 = null
            _id2Info.Add(null); // 1 = back reference
            foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
            {
                var infoForType = new SerializerTypeInfo { Id = (uint)_id2Info.Count, Descriptor = predefinedType, SimpleSaver = BuildSimpleSaver(predefinedType), ComplexSaver = BuildComplexSaver(predefinedType), NewTypeDiscoverer = null };
                _typeOrDescriptor2Info[predefinedType] = infoForType;
                _id2Info.Add(infoForType);
                _typeOrDescriptor2Info[predefinedType.GetPreferedType()] = infoForType;
                var descriptorMultipleNativeTypes = predefinedType as ITypeDescriptorMultipleNativeTypes;
                if (descriptorMultipleNativeTypes == null) continue;
                foreach (var type in descriptorMultipleNativeTypes.GetNativeTypes())
                {
                    _typeOrDescriptor2Info[type] = infoForType;
                }
            }
            while (_id2Info.Count < ReservedBuildinTypes) _id2Info.Add(null);
        }

        Action<AbstractBufferedWriter, object> BuildSimpleSaver(ITypeDescriptor descriptor)
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

        Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object> BuildComplexSaver(ITypeDescriptor descriptor)
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

        public ITypeDescriptor DescriptorOf(object obj)
        {
            if (obj == null) return null;
            var knowDescriptor = obj as IKnowDescriptor;
            if (knowDescriptor != null) return knowDescriptor.GetDescriptor();
            SerializerTypeInfo info;
            if (!_typeOrDescriptor2Info.TryGetValue(obj.GetType(), out info))
                return null;
            return info.Descriptor;
        }

        public ITypeDescriptor DescriptorOf(Type type)
        {
            SerializerTypeInfo info;
            if (!_typeOrDescriptor2Info.TryGetValue(type, out info))
                return null;
            return info.Descriptor;
        }

        public ITypeConvertorGenerator ConvertorGenerator { get; }

        public ITypeNameMapper TypeNameMapper => _typeNameMapper;

        public Type LoadAsType(ITypeDescriptor descriptor)
        {
            return descriptor.GetPreferedType() ?? _typeNameMapper.ToType(descriptor.Name) ?? typeof(object);
        }

        public void ProcessMetadataLog(ByteBuffer buffer)
        {
            throw new NotImplementedException();
        }

        public bool Serialize(AbstractBufferedWriter writer, object obj)
        {
            if (obj == null)
            {
                writer.WriteUInt8(0); // null
                return false;
            }
            SerializerTypeInfo info;
            var knowDescriptor = obj as IKnowDescriptor;
            if (knowDescriptor != null)
            {
                if (!_typeOrDescriptor2Info.TryGetValue(knowDescriptor.GetDescriptor(), out info))
                {
                    info = new SerializerTypeInfo
                    {
                        Id = 0,
                        Descriptor = knowDescriptor.GetDescriptor()
                    };
                }
            }
            else
            {
                if (!_typeOrDescriptor2Info.TryGetValue(obj.GetType(), out info))
                {
                    info = new SerializerTypeInfo
                    {
                        Id = 0,
                        Descriptor = knowDescriptor.GetDescriptor()
                    };
                }
            }
            throw new NotImplementedException();
        }
    }
}