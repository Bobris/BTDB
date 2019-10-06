using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BTDB.Buffer;
using BTDB.EventStoreLayer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStore2Layer
{
    public class EventDeserializer : IEventDeserializer, ITypeDescriptorCallbacks, ITypeBinaryDeserializerContext
    {
        public const int ReservedBuildinTypes = 50;
        readonly Dictionary<object, DeserializerTypeInfo> _typeOrDescriptor2Info = new Dictionary<object, DeserializerTypeInfo>(ReferenceEqualityComparer<object>.Instance);
        readonly List<DeserializerTypeInfo?> _id2Info = new List<DeserializerTypeInfo>();
        readonly List<DeserializerTypeInfo?> _id2InfoNew = new List<DeserializerTypeInfo>();
        readonly Dictionary<ITypeDescriptor, ITypeDescriptor> _remapToOld = new Dictionary<ITypeDescriptor, ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
        readonly List<object> _visited = new List<object>();
        readonly ByteBufferReader _reader = new ByteBufferReader(ByteBuffer.NewEmpty());
        readonly object _lock = new object();

        public EventDeserializer(ITypeNameMapper? typeNameMapper = null, ITypeConvertorGenerator? typeConvertorGenerator = null)
        {
            TypeNameMapper = typeNameMapper ?? new FullNameTypeMapper();
            ConvertorGenerator = typeConvertorGenerator ?? new DefaultTypeConvertorGenerator();
            _id2Info.Add(null); // 0 = null
            _id2Info.Add(null); // 1 = back reference
            foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
            {
                var infoForType = new DeserializerTypeInfo
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

        public ITypeDescriptor? DescriptorOf(object obj)
        {
            if (obj == null) return null;
            var knowDescriptor = obj as IKnowDescriptor;
            if (knowDescriptor != null) return knowDescriptor.GetDescriptor();
            DeserializerTypeInfo info;
            if (!_typeOrDescriptor2Info.TryGetValue(obj.GetType(), out info))
                return null;
            return info.Descriptor;
        }

        public ITypeDescriptor? DescriptorOf(Type type)
        {
            DeserializerTypeInfo info;
            if (!_typeOrDescriptor2Info.TryGetValue(type, out info))
                return null;
            return info.Descriptor;
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
            return descriptor.GetPreferedType() ?? TypeNameMapper.ToType(descriptor.Name) ?? typeof(object);
        }

        public Type LoadAsType(ITypeDescriptor descriptor, Type targetType)
        {
            return descriptor.GetPreferedType(targetType) ?? TypeNameMapper.ToType(descriptor.Name) ?? typeof(object);
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
            lock(_lock)
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
                        case TypeCategory.Nullable:
                            descriptor = new NullableTypeDescriptor(this, reader, NestedDescriptorReader);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                    while (-typeId - 1 >= _id2InfoNew.Count)
                        _id2InfoNew.Add(null);
                    if (_id2InfoNew[-typeId - 1] == null)
                        _id2InfoNew[-typeId - 1] = new DeserializerTypeInfo { Id = typeId, Descriptor = descriptor };
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
        }

        Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeDescriptor, object> LoaderFactory(ITypeDescriptor descriptor)
        {
            var loadAsType = LoadAsType(descriptor);
            var methodBuilder = ILBuilder.Instance.NewMethod<Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeDescriptor, object>>("DeserializerFor" + descriptor.Name);
            var il = methodBuilder.Generator;
            try
            {
                descriptor.GenerateLoad(il, ilGen => ilGen.Ldarg(0), ilGen => ilGen.Ldarg(1), ilGen => ilGen.Ldarg(2), loadAsType);
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

        public bool Deserialize(out object @object, ByteBuffer buffer)
        {
            lock(_lock)
            {
                _reader.Restart(buffer);
                @object = null;
                try
                {
                    @object = LoadObject();
                }
                catch (BtdbMissingMetadataException)
                {
                    return false;
                }
                finally
                {
                    _visited.Clear();
                }
                _reader.Restart(ByteBuffer.NewEmpty());
                return true;
            }
        }

        class BtdbMissingMetadataException : Exception
        {
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
                return _visited[(int)backRefId];
            }
            if (typeId >= _id2Info.Count)
                throw new BtdbMissingMetadataException();
            var infoForType = _id2Info[(int)typeId];
            if (infoForType.Loader == null)
            {
                infoForType.Loader = LoaderFactory(infoForType.Descriptor);
            }
            return infoForType.Loader(_reader, this, infoForType.Descriptor);
        }

        public void AddBackRef(object obj)
        {
            _visited.Add(obj);
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
                if (backRefId > _visited.Count) throw new InvalidDataException();
                return;
            }
            if (typeId >= _id2Info.Count)
                throw new BtdbMissingMetadataException();
            var infoForType = _id2Info[(int)typeId];
            if (infoForType.Loader == null)
            {
                infoForType.Loader = LoaderFactory(infoForType.Descriptor);
            }
            infoForType.Loader(_reader, this, infoForType.Descriptor);
        }
    }
}
