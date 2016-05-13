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
    public class EventSerializer : IEventSerializer, ITypeDescriptorCallbacks, IDescriptorSerializerLiteContext, ITypeDescriptorFactory, ITypeBinarySerializerContext
    {
        public const int ReservedBuildinTypes = 50;
        readonly Dictionary<object, SerializerTypeInfo> _typeOrDescriptor2Info = new Dictionary<object, SerializerTypeInfo>(ReferenceEqualityComparer<object>.Instance);
        readonly Dictionary<object, SerializerTypeInfo> _typeOrDescriptor2InfoNew = new Dictionary<object, SerializerTypeInfo>(ReferenceEqualityComparer<object>.Instance);
        readonly List<SerializerTypeInfo> _id2Info = new List<SerializerTypeInfo>();
        readonly List<SerializerTypeInfo> _id2InfoNew = new List<SerializerTypeInfo>();
        readonly Dictionary<ITypeDescriptor, ITypeDescriptor> _remapToOld = new Dictionary<ITypeDescriptor, ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly List<object> _visited = new List<object>();
        AbstractBufferedWriter _writer;

        public EventSerializer(ITypeNameMapper typeNameMapper = null, ITypeConvertorGenerator typeConvertorGenerator = null)
        {
            TypeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
            ConvertorGenerator = typeConvertorGenerator ?? new DefaultTypeConvertorGenerator();
            _id2Info.Add(null); // 0 = null
            _id2Info.Add(null); // 1 = back reference
            foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
            {
                var infoForType = new SerializerTypeInfo
                {
                    Id = _id2Info.Count,
                    Descriptor = predefinedType
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

        Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object> BuildComplexSaver(ITypeDescriptor descriptor)
        {
            var method = ILBuilder.Instance.NewMethod<Action<AbstractBufferedWriter, ITypeBinarySerializerContext, object>>(descriptor.Name + "Saver");
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
            // TODO: Add cache here to do this only once instead of 3 times for every type
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

        public ITypeNameMapper TypeNameMapper { get; }

        public Type LoadAsType(ITypeDescriptor descriptor)
        {
            return descriptor.GetPreferedType() ?? TypeNameMapper.ToType(descriptor.Name) ?? typeof(object);
        }

        ITypeDescriptor NestedDescriptorReader(AbstractBufferedReader reader)
        {
            var typeId = reader.ReadVInt32();
            if (typeId < 0 && -typeId - 1 < _id2InfoNew.Count)
            {
                var infoForType = _id2InfoNew[-typeId - 1];
                if (infoForType != null)
                    return infoForType.Descriptor;
            }
            else if (typeId > 0)
            {
                if (typeId >= _id2Info.Count)
                    throw new BTDBException("Metadata corrupted");
                var infoForType = _id2Info[typeId];
                if (infoForType == null)
                    throw new BTDBException("Metadata corrupted");
                return infoForType.Descriptor;
            }
            return new PlaceHolderDescriptor(typeId);
        }

        public void ProcessMetadataLog(ByteBuffer buffer)
        {
            var reader = new ByteBufferReader(buffer);
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
                        descriptor = new ObjectTypeDescriptor(this, reader, NestedDescriptorReader);
                        break;
                    case TypeCategory.List:
                        descriptor = new ListTypeDescriptor(this, reader, NestedDescriptorReader);
                        break;
                    case TypeCategory.Dictionary:
                        descriptor = new DictionaryTypeDescriptor(this, reader, NestedDescriptorReader);
                        break;
                    case TypeCategory.Enum:
                        descriptor = new EnumTypeDescriptor(this, reader);
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
                _id2InfoNew[i].Descriptor.MapNestedTypes(d =>
                {
                    var placeHolderDescriptor = d as PlaceHolderDescriptor;
                    return placeHolderDescriptor != null ? _id2InfoNew[-placeHolderDescriptor.TypeId - 1].Descriptor : d;
                });
            }
            // This additional cycle is needed to fill names of recursive structures
            for (var i = 0; i < _id2InfoNew.Count; i++)
            {
                _id2InfoNew[i].Descriptor.MapNestedTypes(d => d);
            }
            for (var i = 0; i < _id2InfoNew.Count; i++)
            {
                var infoForType = _id2InfoNew[i];
                for (var j = ReservedBuildinTypes; j < _id2Info.Count; j++)
                {
                    if (infoForType.Descriptor.Equals(_id2Info[j].Descriptor))
                    {
                        _remapToOld[infoForType.Descriptor] = _id2Info[j].Descriptor;
                        _id2InfoNew[i] = _id2Info[j];
                        infoForType = _id2InfoNew[i];
                        break;
                    }
                }
                if (infoForType.Id < 0)
                {
                    infoForType.Id = _id2Info.Count;
                    _id2Info.Add(infoForType);
                    _typeOrDescriptor2Info[infoForType.Descriptor] = infoForType;
                }
            }
            for (var i = 0; i < _id2InfoNew.Count; i++)
            {
                _id2InfoNew[i].Descriptor.MapNestedTypes(d =>
                {
                    ITypeDescriptor res;
                    return _remapToOld.TryGetValue(d, out res) ? res : d;
                });
            }
            _id2InfoNew.Clear();
            _remapToOld.Clear();
        }

        public bool Serialize(AbstractBufferedWriter writer, object obj)
        {
            if (obj == null)
            {
                writer.WriteUInt8(0); // null
                return false;
            }
            var info=StoreNewDescriptorsAndReturnInfo(obj);
            if (_typeOrDescriptor2InfoNew.Count > 0)
            {
                if (MergeTypesByShapeAndStoreNew(writer))
                {
                    _typeOrDescriptor2InfoNew.Clear();
                    _visited.Clear();
                    return true;
                }
                _typeOrDescriptor2InfoNew.Clear();
                var knowDescriptor = obj as IKnowDescriptor;
                if (knowDescriptor != null)
                {
                    if (!_typeOrDescriptor2Info.TryGetValue(knowDescriptor.GetDescriptor(), out info))
                    {
                        throw new BTDBException("Forgot descriptor");
                    }
                }
                else
                {
                    if (!_typeOrDescriptor2Info.TryGetValue(obj.GetType(), out info))
                    {
                        throw new BTDBException("Forgot type");
                    }
                }
            }
            _visited.Clear();
            if (info.ComplexSaver == null) info.ComplexSaver = BuildComplexSaver(info.Descriptor);
            _visited.Add(obj); // first backreference
            writer.WriteVUInt32((uint) info.Id);
            _writer = writer;
            info.ComplexSaver(writer, this, obj);
            _writer = null;
            _visited.Clear();
            return false;
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
            StoreNewDescriptorsAndReturnInfo(obj);
        }

        SerializerTypeInfo StoreNewDescriptorsAndReturnInfo(object obj)
        {
            if (obj == null) return null;
            for (var i = 0; i < _visited.Count; i++)
            {
                if (_visited[i] == obj) return null;
            }
            _visited.Add(obj);
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
                    var desc=Create(obj.GetType());
                    info = _typeOrDescriptor2InfoNew[desc];
                }
            }
            if (info.NestedObjGatherer == null)
            {
                info.NestedObjGatherer = BuildNestedObjGatherer(info.Descriptor);
            }
            info.NestedObjGatherer(obj, this);
            return info;
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
                    if (typeDescriptor.Key is Type)
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

        public void StoreObject(object obj)
        {
            if (obj == null)
            {
                _writer.WriteUInt8(0);
                return;
            }
            for (int i = 0; i < _visited.Count; i++)
            {
                if (_visited[i] == obj)
                {
                    _writer.WriteUInt8(1); // backreference
                    _writer.WriteVUInt32((uint) i);
                    return;
                }
            }
            _visited.Add(obj);
            SerializerTypeInfo info;
            var knowDescriptor = obj as IKnowDescriptor;
            if (knowDescriptor != null)
            {
                if (!_typeOrDescriptor2Info.TryGetValue(knowDescriptor.GetDescriptor(), out info))
                {
                    throw new BTDBException("Forgot descriptor");
                }
            }
            else
            {
                if (!_typeOrDescriptor2Info.TryGetValue(obj.GetType(), out info))
                {
                    throw new BTDBException("Forgot type");
                }
            }
            if (info.ComplexSaver == null) info.ComplexSaver = BuildComplexSaver(info.Descriptor);
            _writer.WriteVUInt32((uint)info.Id);
            info.ComplexSaver(_writer, this, obj);
        }
    }
}
