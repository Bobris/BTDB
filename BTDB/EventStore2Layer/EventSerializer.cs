using System;
using System.Collections.Generic;
using BTDB.Buffer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer
{
    public class EventSerializer : IEventSerializer, ITypeDescriptorCallbacks, IDescriptorSerializerLiteContext, ITypeDescriptorFactory
    {
        public const int ReservedBuildinTypes = 50;
        readonly ITypeNameMapper _typeNameMapper;
        readonly Dictionary<object, SerializerTypeInfo> _typeOrDescriptor2Info = new Dictionary<object, SerializerTypeInfo>(ReferenceEqualityComparer<object>.Instance);
        readonly Dictionary<object, SerializerTypeInfo> _typeOrDescriptor2InfoNew = new Dictionary<object, SerializerTypeInfo>(ReferenceEqualityComparer<object>.Instance);
        readonly List<SerializerTypeInfo> _id2Info = new List<SerializerTypeInfo>();

        public EventSerializer(ITypeNameMapper typeNameMapper = null, ITypeConvertorGenerator typeConvertorGenerator = null)
        {
            _typeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
            ConvertorGenerator = typeConvertorGenerator ?? new DefaultTypeConvertorGenerator();
            _id2Info.Add(null); // 0 = null
            _id2Info.Add(null); // 1 = back reference
            foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
            {
                var infoForType = new SerializerTypeInfo
                {
                    Id = _id2Info.Count,
                    Descriptor = predefinedType,
                    SimpleSaver = BuildSimpleSaver(predefinedType),
                    ComplexSaver = BuildComplexSaver(predefinedType),
                    NestedObjGatherer = BuildNestedObjGatherer(predefinedType)
                };
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

        Action<object, IDescriptorSerializerLiteContext> BuildNestedObjGatherer(ITypeDescriptor descriptor)
        {
            var gen = descriptor.BuildNewDescriptorGenerator();
            if (gen == null)
            {
                return (obj, ctx) => { };
            }
            var method = ILBuilder.Instance.NewMethod<Action<object, IDescriptorSerializerLiteContext>>("GatherAllObjectsForTypeExtraction_" + descriptor.Name);
            var il = method.Generator;
            gen.GenerateTypeIterator(il, ilgen => ilgen.Ldarg(0), ilgen => ilgen.Ldarg(1));
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
            StoreNewDescriptors(obj);
            if (_typeOrDescriptor2InfoNew.Count > 0)
            {
                if (MergeTypesByShapeAndStoreNew(writer))
                    return true;
            }
            throw new NotImplementedException();
        }

        public ITypeDescriptor Create(Type type)
        {
            SerializerTypeInfo result;
            if (_typeOrDescriptor2Info.TryGetValue(type, out result)) return result.Descriptor;
            if (_typeOrDescriptor2InfoNew.TryGetValue(type, out result)) return result.Descriptor;
            ITypeDescriptor desc = null;
            if (!type.IsSubclassOf(typeof(Delegate)))
            {
                if (type.IsGenericType)
                {
                    if (type.GetGenericTypeDefinition().InheritsOrImplements(typeof(IList<>)))
                    {
                        desc = new ListTypeDescriptor(this, type);
                    }
                    else if (type.GetGenericTypeDefinition().InheritsOrImplements(typeof(IDictionary<,>)))
                    {
                        desc = new DictionaryTypeDescriptor(this, type);
                    }
                }
                else if (type.IsArray)
                {
                    desc = new ListTypeDescriptor(this, type);
                }
                else if (type.IsEnum)
                {
                    desc = new EnumTypeDescriptor(this, type);
                }
                else
                {
                    desc = new ObjectTypeDescriptor(this, type);
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
            desc.FinishBuildFromType(this);
            return desc;
        }

        public void StoreNewDescriptors(object obj)
        {
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
                    _typeOrDescriptor2InfoNew[knowDescriptor.GetDescriptor()] = info;
                }
            }
            else
            {
                if (!_typeOrDescriptor2Info.TryGetValue(obj.GetType(), out info))
                {
                    info = new SerializerTypeInfo
                    {
                        Id = 0,
                        Descriptor = Create(obj.GetType())
                    };
                    _typeOrDescriptor2InfoNew[info.Descriptor] = info;
                    _typeOrDescriptor2InfoNew[obj.GetType()] = info;
                }
            }
            if (info.NestedObjGatherer == null)
            {
                info.NestedObjGatherer = BuildNestedObjGatherer(info.Descriptor);
            }
            info.NestedObjGatherer(obj, this);
        }

        bool MergeTypesByShapeAndStoreNew(AbstractBufferedWriter writer)
        {
            List<SerializerTypeInfo> toStore = null;
            foreach (var typeDescriptor in _typeOrDescriptor2InfoNew)
            {
                var info = typeDescriptor.Value;
                if (info.Id == 0)
                {
                    var d = info.Descriptor;
                    foreach (var existingTypeDescriptor in _typeOrDescriptor2Info)
                    {
                        if (d.Equals(existingTypeDescriptor.Value.Descriptor))
                        {
                            info.Id = existingTypeDescriptor.Value.Id;
                            break;
                        }
                    }
                }
                if (info.Id == 0)
                {
                    if (toStore == null) toStore = new List<SerializerTypeInfo>();
                    toStore.Add(info);
                    info.Id = -toStore.Count;
                }
                else if (info.Id > 0)
                {
                    if (typeDescriptor.Key.GetType() == typeof(Type))
                    {
                        _typeOrDescriptor2Info[typeDescriptor.Key] = _id2Info[info.Id];
                    }
                }
            }
            if (toStore != null)
            {
                for (int i = toStore.Count - 1; i >= 0; i--)
                {
                    writer.WriteVInt32(toStore[i].Id);
                    StoreDescriptor(toStore[i].Descriptor, writer);
                }
                writer.WriteVInt32(0);
                return true;
            }
            return false;
        }

        public void StoreDescriptor(ITypeDescriptor descriptor, AbstractBufferedWriter writer)
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
            else
            {
                throw new ArgumentOutOfRangeException();
            }
            ((IPersistTypeDescriptor)descriptor).Persist(writer, (w, d) =>
            {
                SerializerTypeInfo result;
                if (!_typeOrDescriptor2Info.TryGetValue(d, out result))
                    if (!_typeOrDescriptor2InfoNew.TryGetValue(d, out result))
                        throw new BTDBException("Invalid state unknown descriptor " + d.Name);
                w.WriteVInt32(result.Id);
            });
        }

    }
}
