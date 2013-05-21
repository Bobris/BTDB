using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal class TypeSerializersMapping : ITypeSerializersMapping, ITypeSerializersLightMapping, ITypeSerializersId2LoaderMapping
    {
        const int ReservedBuildinTypes = 50;
        readonly List<InfoForType> _id2DescriptorMap = new List<InfoForType>();
        readonly Dictionary<object, InfoForType> _typeOrDescriptor2Info = new Dictionary<object, InfoForType>(ReferenceEqualityComparer<object>.Instance);
        readonly TypeSerializers _typeSerializers;

        public TypeSerializersMapping(TypeSerializers typeSerializers)
        {
            _typeSerializers = typeSerializers;
            AddBuildInTypes();
        }

        void AddBuildInTypes()
        {
            _id2DescriptorMap.Add(null); // 0 = null
            _id2DescriptorMap.Add(null); // 1 = back reference
            foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
            {
                var infoForType = new InfoForType { Id = _id2DescriptorMap.Count, Descriptor = predefinedType };
                _typeOrDescriptor2Info[predefinedType] = infoForType;
                _id2DescriptorMap.Add(infoForType);
            }
            while (_id2DescriptorMap.Count < ReservedBuildinTypes) _id2DescriptorMap.Add(null);
        }

        public void LoadTypeDescriptors(AbstractBufferedReader reader)
        {
            var typeId = reader.ReadVUInt32();
            var firstTypeId = typeId;
            while (typeId != 0)
            {
                if (typeId < firstTypeId) firstTypeId = typeId;
                var typeCategory = (TypeCategory)reader.ReadUInt8();
                ITypeDescriptor descriptor;
                switch (typeCategory)
                {
                    case TypeCategory.BuildIn:
                        throw new ArgumentOutOfRangeException();
                    case TypeCategory.Class:
                        descriptor = new ObjectTypeDescriptor(reader, NestedDescriptorReader);
                        break;
                    case TypeCategory.List:
                        descriptor = new ListTypeDescriptor(_typeSerializers, reader, NestedDescriptorReader);
                        break;
                    case TypeCategory.Dictionary:
                        descriptor = new DictionaryTypeDescriptor(_typeSerializers, reader, NestedDescriptorReader);
                        break;
                    case TypeCategory.Enum:
                        descriptor = new EnumTypeDescriptor(_typeSerializers, reader);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                while (typeId >= _id2DescriptorMap.Count)
                    _id2DescriptorMap.Add(null);
                if (_id2DescriptorMap[(int)typeId] == null)
                    _id2DescriptorMap[(int)typeId] = new InfoForType { Id = (int)typeId, Descriptor = descriptor };
                typeId = reader.ReadVUInt32();
            }
            for (var i = firstTypeId; i < _id2DescriptorMap.Count; i++)
            {
                _id2DescriptorMap[(int)i].Descriptor.MapNestedTypes(d =>
                    {
                        var placeHolderDescriptor = d as PlaceHolderDescriptor;
                        return placeHolderDescriptor != null ? _id2DescriptorMap[(int)placeHolderDescriptor.TypeId].Descriptor : d;
                    });
            }
            for (var i = firstTypeId; i < _id2DescriptorMap.Count; i++)
            {
                var infoForType = _id2DescriptorMap[(int)i];
                var descriptor = _typeSerializers.MergeDescriptor(infoForType.Descriptor);
                infoForType.Descriptor = descriptor;
                _typeOrDescriptor2Info[descriptor] = infoForType;
            }
        }

        ITypeDescriptor NestedDescriptorReader(AbstractBufferedReader reader)
        {
            var typeId = reader.ReadVUInt32();
            if (typeId < _id2DescriptorMap.Count)
            {
                var infoForType = _id2DescriptorMap[(int)typeId];
                if (infoForType != null)
                    return infoForType.Descriptor;
            }
            return new PlaceHolderDescriptor(typeId);
        }

        class PlaceHolderDescriptor : ITypeDescriptor
        {
            readonly uint _typeId;

            internal uint TypeId
            {
                get { return _typeId; }
            }

            public PlaceHolderDescriptor(uint typeId)
            {
                _typeId = typeId;
            }

            bool IEquatable<ITypeDescriptor>.Equals(ITypeDescriptor other)
            {
                throw new InvalidOperationException();
            }

            string ITypeDescriptor.Name
            {
                get { return ""; }
            }

            void ITypeDescriptor.FinishBuildFromType(ITypeDescriptorFactory factory)
            {
                throw new InvalidOperationException();
            }

            void ITypeDescriptor.BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
            {
                throw new InvalidOperationException();
            }

            bool ITypeDescriptor.Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
            {
                throw new InvalidOperationException();
            }

            Type ITypeDescriptor.GetPreferedType()
            {
                return null;
            }

            ITypeBinaryDeserializerGenerator ITypeDescriptor.BuildBinaryDeserializerGenerator(Type target)
            {
                throw new InvalidOperationException();
            }

            ITypeBinarySkipperGenerator ITypeDescriptor.BuildBinarySkipperGenerator()
            {
                throw new InvalidOperationException();
            }

            ITypeBinarySerializerGenerator ITypeDescriptor.BuildBinarySerializerGenerator()
            {
                throw new InvalidOperationException();
            }

            ITypeNewDescriptorGenerator ITypeDescriptor.BuildNewDescriptorGenerator()
            {
                throw new InvalidOperationException();
            }

            public ITypeDescriptor NestedType(int index)
            {
                throw new InvalidOperationException();
            }

            void ITypeDescriptor.MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
            {
                throw new InvalidOperationException();
            }

            bool ITypeDescriptor.Sealed
            {
                get { return false; }
            }

            bool ITypeDescriptor.StoredInline
            {
                get { return false; }
            }

            void ITypeDescriptor.ClearMappingToType()
            {
                throw new InvalidOperationException();
            }
        }

        public object LoadObject(AbstractBufferedReader reader)
        {
            var typeId = reader.ReadVUInt32();
            if (typeId == 0)
            {
                return null;
            }
            if (typeId == 1)
            {
                throw new InvalidDataException("Backreference cannot be first object");
            }
            return Load(typeId, reader, null);
        }

        public object Load(uint typeId, AbstractBufferedReader reader, ITypeBinaryDeserializerContext context)
        {
            var infoForType = _id2DescriptorMap[(int)typeId];
            if (infoForType.Loader == null)
            {
                infoForType.Loader = _typeSerializers.GetLoader(infoForType.Descriptor);
            }
            return infoForType.Loader(reader, context, this, infoForType.Descriptor);
        }

        public bool SomeTypeStored { get { return false; } }

        public IDescriptorSerializerContext StoreNewDescriptors(AbstractBufferedWriter writer, object obj)
        {
            if (obj == null) return this;
            InfoForType infoForType;
            if (obj is IKnowDescriptor)
            {
                var descriptor = ((IKnowDescriptor)obj).GetDescriptor();
                if (!_typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                {
                    infoForType = new InfoForType { Id = 0, Descriptor = descriptor };
                }
            }
            else
            {
                var objType = obj.GetType();
                if (!_typeOrDescriptor2Info.TryGetValue(objType, out infoForType))
                {
                    var descriptor = _typeSerializers.DescriptorOf(objType);
                    if (!_typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                    {
                        infoForType = new InfoForType { Id = 0, Descriptor = descriptor };
                    }
                    else
                    {
                        _typeOrDescriptor2Info[objType] = infoForType;
                    }
                }
            }
            DescriptorSerializerContext ctx = null;
            if (infoForType.Id == 0)
            {
                ctx = new DescriptorSerializerContext(this, writer);
                ctx.AddDescriptor(infoForType);
            }
            if (!infoForType.KnownNewTypeDiscoverer)
            {
                infoForType.NewTypeDiscoverer = _typeSerializers.GetNewDescriptorSaver(infoForType.Descriptor);
                infoForType.KnownNewTypeDiscoverer = true;
            }
            var action = infoForType.NewTypeDiscoverer;
            if (action != null)
            {
                if (ctx == null) ctx = new DescriptorSerializerContext(this, writer);
                action(obj, ctx);
            }
            if (ctx != null && ctx.SomeTypeStored)
            {
                return ctx;
            }
            return this;
        }

        public void CommitNewDescriptors()
        {
        }

        class DescriptorSerializerContext : IDescriptorSerializerContext, IDescriptorSerializerLiteContext, ITypeSerializersLightMapping
        {
            readonly TypeSerializersMapping _typeSerializersMapping;
            readonly AbstractBufferedWriter _writer;
            readonly TypeSerializers _typeSerializers;
            readonly List<InfoForType> _id2InfoMap = new List<InfoForType>();
            readonly Dictionary<object, InfoForType> _typeOrDescriptor2InfoMap = new Dictionary<object, InfoForType>(ReferenceEqualityComparer<object>.Instance);

            public DescriptorSerializerContext(TypeSerializersMapping typeSerializersMapping, AbstractBufferedWriter writer)
            {
                _typeSerializersMapping = typeSerializersMapping;
                _writer = writer;
                _typeSerializers = _typeSerializersMapping._typeSerializers;
            }

            public void AddDescriptor(InfoForType infoForType)
            {
                infoForType.Id = _typeSerializersMapping._id2DescriptorMap.Count + _id2InfoMap.Count;
                _typeOrDescriptor2InfoMap.Add(infoForType.Descriptor, infoForType);
                _id2InfoMap.Add(infoForType);
                var idx = 0;
                ITypeDescriptor nestedDescriptor;
                while ((nestedDescriptor = infoForType.Descriptor.NestedType(idx)) != null)
                {
                    int _;
                    if (!TryDescriptor2Id(nestedDescriptor, out _))
                        AddDescriptor(new InfoForType { Descriptor = nestedDescriptor });
                    idx++;
                }
            }

            uint Descriptor2Id(ITypeDescriptor descriptor)
            {
                if (descriptor == null) return 0;
                InfoForType infoForType;
                if (_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out infoForType))
                    return (uint)infoForType.Id;
                if (_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                    return (uint)infoForType.Id;
                throw new InvalidOperationException();
            }

            bool TryDescriptor2Id(ITypeDescriptor descriptor, out int typeId)
            {
                InfoForType infoForType;
                if (_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out infoForType) ||
                    _typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                {
                    typeId = infoForType.Id;
                    return true;
                }
                typeId = 0;
                return false;
            }

            public bool SomeTypeStored
            {
                get { return _id2InfoMap.Count != 0; }
            }

            public IDescriptorSerializerContext StoreNewDescriptors(AbstractBufferedWriter writer, object obj)
            {
                if (obj == null) return this;
                InfoForType infoForType;
                if (obj is IKnowDescriptor)
                {
                    var descriptor = ((IKnowDescriptor)obj).GetDescriptor();
                    if (!_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out infoForType) &&
                        !_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                    {
                        infoForType = new InfoForType { Id = 0, Descriptor = descriptor };
                    }
                }
                else
                {
                    var objType = obj.GetType();
                    if (!_typeOrDescriptor2InfoMap.TryGetValue(objType, out infoForType) &&
                        !_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(objType, out infoForType))
                    {
                        var descriptor = _typeSerializers.DescriptorOf(objType);
                        if (_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out infoForType))
                        {
                            _typeOrDescriptor2InfoMap[objType] = infoForType;
                        }
                        else if (_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                        {
                            _typeSerializersMapping._typeOrDescriptor2Info[objType] = infoForType;
                        }
                        else
                        {
                            infoForType = new InfoForType { Id = 0, Descriptor = descriptor };
                        }
                    }
                }
                if (infoForType.Id == 0)
                {
                    AddDescriptor(infoForType);
                }
                if (!infoForType.KnownNewTypeDiscoverer)
                {
                    infoForType.NewTypeDiscoverer = _typeSerializers.GetNewDescriptorSaver(infoForType.Descriptor);
                    infoForType.KnownNewTypeDiscoverer = true;
                }
                var action = infoForType.NewTypeDiscoverer;
                if (action != null)
                {
                    action(obj, this);
                }
                return this;
            }

            public void CommitNewDescriptors()
            {
                _typeSerializersMapping._id2DescriptorMap.AddRange(_id2InfoMap);
                var ownerTypeOrDescriptor2Info = _typeSerializersMapping._typeOrDescriptor2Info;
                foreach (var d2IPair in _typeOrDescriptor2InfoMap)
                {
                    ownerTypeOrDescriptor2Info[d2IPair.Key] = d2IPair.Value;
                }
            }

            public void StoreObject(AbstractBufferedWriter writer, object obj)
            {
                if (obj == null)
                {
                    writer.WriteUInt8(0);
                    return;
                }
                TypeSerializers typeSerializers;
                var infoForType = GetInfoFromObject(obj, out typeSerializers);
                StoreObjectCore(_typeSerializers, writer, obj, infoForType, this);
            }

            public void FinishNewDescriptors(AbstractBufferedWriter writer)
            {
                if (SomeTypeStored)
                {
                    for (int i = _id2InfoMap.Count - 1; i >= 0; i--)
                    {
                        writer.WriteVUInt32((uint)(i + _typeSerializersMapping._id2DescriptorMap.Count));
                        _typeSerializers.StoreDescriptor(_id2InfoMap[i].Descriptor, writer, Descriptor2Id);
                    }
                    writer.WriteUInt8(0);
                }
            }

            public void StoreNewDescriptors(object obj)
            {
                StoreNewDescriptors(_writer, obj);
            }

            public InfoForType GetInfoFromObject(object obj, out TypeSerializers typeSerializers)
            {
                InfoForType infoForType;
                if (obj is IKnowDescriptor)
                {
                    var descriptor = ((IKnowDescriptor)obj).GetDescriptor();
                    if (!_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out infoForType))
                        _typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType);
                }
                else
                {
                    var objType = obj.GetType();
                    if (!_typeOrDescriptor2InfoMap.TryGetValue(objType, out infoForType) && !_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(objType, out infoForType))
                    {
                        var descriptor = _typeSerializers.DescriptorOf(objType);
                        if (_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out infoForType))
                        {
                            _typeOrDescriptor2InfoMap[objType] = infoForType;
                        }
                        else if (_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                        {
                            _typeSerializersMapping._typeOrDescriptor2Info[objType] = infoForType;
                        }
                    }
                }
                if (infoForType == null)
                {
                    throw new InvalidOperationException(String.Format("Type {0} was not registered using StoreNewDescriptors", obj.GetType().FullName));
                }
                typeSerializers = _typeSerializers;
                return infoForType;
            }
        }

        public void StoreObject(AbstractBufferedWriter writer, object obj)
        {
            if (obj == null)
            {
                writer.WriteUInt8(0);
                return;
            }
            TypeSerializers typeSerializers;
            var infoForType = GetInfoFromObject(obj, out typeSerializers);
            StoreObjectCore(typeSerializers, writer, obj, infoForType, this);
        }

        static void StoreObjectCore(TypeSerializers typeSerializers, AbstractBufferedWriter writer, object obj, InfoForType infoForType, ITypeSerializersLightMapping mapping)
        {
            writer.WriteVUInt32((uint)infoForType.Id);
            if (!infoForType.KnownSimpleSaver)
            {
                infoForType.SimpleSaver = typeSerializers.GetSimpleSaver(infoForType.Descriptor);
                infoForType.KnownSimpleSaver = true;
            }
            var simpleSaver = infoForType.SimpleSaver;
            if (simpleSaver != null)
            {
                simpleSaver(writer, obj);
                return;
            }
            if (!infoForType.KnownComplexSaver)
            {
                infoForType.ComplexSaver = typeSerializers.GetComplexSaver(infoForType.Descriptor);
                infoForType.KnownComplexSaver = true;
            }
            var complexSaver = infoForType.ComplexSaver;
            ITypeBinarySerializerContext ctx = new TypeBinarySerializerContext(mapping, writer, obj);
            complexSaver(writer, ctx, obj);
        }

        class TypeBinarySerializerContext : ITypeBinarySerializerContext
        {
            readonly ITypeSerializersLightMapping _mapping;
            readonly AbstractBufferedWriter _writer;
            readonly Dictionary<object, uint> _backrefs = new Dictionary<object, uint>(ReferenceEqualityComparer<object>.Instance);

            public TypeBinarySerializerContext(ITypeSerializersLightMapping mapping, AbstractBufferedWriter writer, object obj)
            {
                _mapping = mapping;
                _writer = writer;
                _backrefs.Add(obj, 0);
            }

            public void StoreObject(object obj)
            {
                if (obj == null)
                {
                    _writer.WriteUInt8(0);
                    return;
                }
                uint backRefId;
                if (_backrefs.TryGetValue(obj, out backRefId))
                {
                    _writer.WriteUInt8(1);
                    _writer.WriteVUInt32(backRefId);
                    return;
                }
                _backrefs.Add(obj, (uint)_backrefs.Count);
                TypeSerializers typeSerializers;
                var infoForType = _mapping.GetInfoFromObject(obj, out typeSerializers);
                _writer.WriteVUInt32((uint)infoForType.Id);
                if (!infoForType.KnownSimpleSaver)
                {
                    infoForType.SimpleSaver = typeSerializers.GetSimpleSaver(infoForType.Descriptor);
                    infoForType.KnownSimpleSaver = true;
                }
                var simpleSaver = infoForType.SimpleSaver;
                if (simpleSaver != null)
                {
                    simpleSaver(_writer, obj);
                    return;
                }
                if (!infoForType.KnownComplexSaver)
                {
                    infoForType.ComplexSaver = typeSerializers.GetComplexSaver(infoForType.Descriptor);
                    infoForType.KnownComplexSaver = true;
                }
                var complexSaver = infoForType.ComplexSaver;
                complexSaver(_writer, this, obj);
            }
        }

        public void FinishNewDescriptors(AbstractBufferedWriter writer)
        {
        }

        public InfoForType GetInfoFromObject(object obj, out TypeSerializers typeSerializers)
        {
            InfoForType infoForType;
            if (obj is IKnowDescriptor)
            {
                var descriptor = ((IKnowDescriptor)obj).GetDescriptor();
                _typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType);
            }
            else
            {
                var objType = obj.GetType();
                if (!_typeOrDescriptor2Info.TryGetValue(objType, out infoForType))
                {
                    var descriptor = _typeSerializers.DescriptorOf(objType);
                    if (_typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                    {
                        _typeOrDescriptor2Info[objType] = infoForType;
                    }
                }
            }
            typeSerializers = _typeSerializers;
            if (infoForType == null)
            {
                throw new InvalidOperationException(String.Format("Type {0} was not registered using StoreNewDescriptors", obj.GetType().FullName));
            }
            return infoForType;
        }
    }
}