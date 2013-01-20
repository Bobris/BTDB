using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer
{
    internal class TypeSerializersMapping : ITypeSerializersMapping, ITypeSerializersLightMapping, ITypeSerializersId2LoaderMapping
    {
        readonly List<ITypeDescriptor> _id2DescriptorMap = new List<ITypeDescriptor>();
        readonly Dictionary<ITypeDescriptor, int> _descriptor2IdMap = new Dictionary<ITypeDescriptor, int>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);
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
            foreach (var predefinedType in TypeSerializers.PredefinedTypes)
            {
                _descriptor2IdMap[predefinedType] = _id2DescriptorMap.Count;
                _id2DescriptorMap.Add(predefinedType);
            }
        }

        public void LoadTypeDescriptors(AbstractBufferedReader reader)
        {
            var typeId = reader.ReadVUInt32();
            var firstTypeId = typeId;
            while (typeId != 0)
            {
                if (typeId < firstTypeId) firstTypeId = typeId;
                var typeCategory = (TypeCategory)reader.ReadUInt8();
                ITypeDescriptor descriptor = null;
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
                    default:
                        throw new ArgumentOutOfRangeException();
                }
                while (typeId >= _id2DescriptorMap.Count)
                    _id2DescriptorMap.Add(null);
                if (_id2DescriptorMap[(int)typeId] == null)
                    _id2DescriptorMap[(int)typeId] = descriptor;
                typeId = reader.ReadVUInt32();
            }
            for (var i = firstTypeId; i < _id2DescriptorMap.Count; i++)
            {
                _id2DescriptorMap[(int)i].MapNestedTypes(d =>
                    {
                        var placeHolderDescriptor = d as PlaceHolderDescriptor;
                        return placeHolderDescriptor != null ? _id2DescriptorMap[(int)placeHolderDescriptor.TypeId] : d;
                    });
            }
            for (var i = firstTypeId; i < _id2DescriptorMap.Count; i++)
            {
                var descriptor = _typeSerializers.MergeDescriptor(_id2DescriptorMap[(int)i]);
                _id2DescriptorMap[(int)i] = descriptor;
                _descriptor2IdMap[descriptor] = (int)i;
            }
        }

        ITypeDescriptor NestedDescriptorReader(AbstractBufferedReader reader)
        {
            var typeId = reader.ReadVUInt32();
            if (typeId < _id2DescriptorMap.Count)
                return _id2DescriptorMap[(int)typeId];
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

            void ITypeDescriptor.BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack)
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

            IEnumerable<ITypeDescriptor> ITypeDescriptor.NestedTypes()
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
            return GetLoader(typeId)(reader, null, this);
        }

        public Func<AbstractBufferedReader, ITypeBinaryDeserializerContext, ITypeSerializersId2LoaderMapping, object> GetLoader(uint typeId)
        {
            var descriptor = _id2DescriptorMap[(int)typeId];
            return _typeSerializers.GetLoader(descriptor);
        }

        public bool SomeTypeStored { get { return false; } }

        public IDescriptorSerializerContext StoreNewDescriptors(AbstractBufferedWriter writer, object obj)
        {
            if (obj == null) return this;
            int typeId;
            var objType = obj.GetType();
            Action<object, IDescriptorSerializerLiteContext> action;
            DescriptorSerializerContext ctx = null;
            var descriptor = _typeSerializers.DescriptorOf(objType);
            if (_descriptor2IdMap.TryGetValue(descriptor, out typeId))
            {
                action = _typeSerializers.GetNewDescriptorSaver(descriptor);
            }
            else
            {
                ctx = new DescriptorSerializerContext(this, writer);
                ctx.AddDescriptor(descriptor);
                foreach (var nestedDescriptor in descriptor.NestedTypes())
                {
                    int _;
                    if (!ctx.TryDescriptor2Id(nestedDescriptor, out _))
                        ctx.AddDescriptor(nestedDescriptor);
                }
                action = _typeSerializers.GetNewDescriptorSaver(descriptor);
            }
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
            readonly List<ITypeDescriptor> _id2DescriptorMap = new List<ITypeDescriptor>();
            internal readonly Dictionary<ITypeDescriptor, int> Descriptor2IdMap = new Dictionary<ITypeDescriptor, int>(ReferenceEqualityComparer<ITypeDescriptor>.Instance);

            public DescriptorSerializerContext(TypeSerializersMapping typeSerializersMapping, AbstractBufferedWriter writer)
            {
                _typeSerializersMapping = typeSerializersMapping;
                _writer = writer;
                _typeSerializers = _typeSerializersMapping._typeSerializers;
            }

            public void AddDescriptor(ITypeDescriptor descriptor)
            {
                Descriptor2IdMap.Add(descriptor, _typeSerializersMapping._id2DescriptorMap.Count + _id2DescriptorMap.Count);
                _id2DescriptorMap.Add(descriptor);
            }

            public uint Descriptor2Id(ITypeDescriptor descriptor)
            {
                if (descriptor == null) return 0;
                int id;
                if (Descriptor2IdMap.TryGetValue(descriptor, out id))
                    return (uint)id;
                if (_typeSerializersMapping._descriptor2IdMap.TryGetValue(descriptor, out id))
                    return (uint)id;
                throw new InvalidOperationException();
            }

            internal bool TryDescriptor2Id(ITypeDescriptor descriptor, out int typeId)
            {
                if (Descriptor2IdMap.TryGetValue(descriptor, out typeId))
                    return true;
                if (_typeSerializersMapping._descriptor2IdMap.TryGetValue(descriptor, out typeId))
                    return true;
                return false;
            }

            public bool SomeTypeStored
            {
                get { return _id2DescriptorMap.Count != 0; }
            }

            public IDescriptorSerializerContext StoreNewDescriptors(AbstractBufferedWriter writer, object obj)
            {
                if (obj == null) return this;
                int typeId;
                var objType = obj.GetType();
                Action<object, IDescriptorSerializerLiteContext> action;
                var descriptor = _typeSerializers.DescriptorOf(objType);
                if (TryDescriptor2Id(descriptor, out typeId))
                {
                    action = _typeSerializers.GetNewDescriptorSaver(descriptor);
                }
                else
                {
                    AddDescriptor(descriptor);
                    foreach (var nestedDescriptor in descriptor.NestedTypes())
                    {
                        int _;
                        if (!TryDescriptor2Id(nestedDescriptor, out _))
                            AddDescriptor(nestedDescriptor);
                    }
                    action = _typeSerializers.GetNewDescriptorSaver(descriptor);
                }
                if (action != null)
                {
                    action(obj, this);
                }
                return this;
            }

            public void CommitNewDescriptors()
            {
                _typeSerializersMapping._id2DescriptorMap.AddRange(_id2DescriptorMap);
                var ownerDescriptor2IdMap = _typeSerializersMapping._descriptor2IdMap;
                foreach (var d2IPair in Descriptor2IdMap)
                {
                    ownerDescriptor2IdMap[d2IPair.Key] = d2IPair.Value;
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
                ITypeDescriptor descriptor;
                TypeSerializers typeSerializers;
                GetDescriptorAndTypeId(obj.GetType(), out typeSerializers, out descriptor, out typeId);
                StoreObjectCore(_typeSerializers, writer, obj, typeId, descriptor, this);
            }

            public void FinishNewDescriptors(AbstractBufferedWriter writer)
            {
                if (SomeTypeStored)
                {
                    for (int i = _id2DescriptorMap.Count - 1; i >= 0; i--)
                    {
                        writer.WriteVUInt32((uint)(i + _typeSerializersMapping._id2DescriptorMap.Count));
                        _typeSerializers.StoreDescriptor(_id2DescriptorMap[i], writer, Descriptor2Id);
                    }
                    writer.WriteUInt8(0);
                }
            }

            public void StoreNewDescriptors(object obj)
            {
                StoreNewDescriptors(_writer, obj);
            }

            public void GetDescriptorAndTypeId(Type type, out TypeSerializers typeSerializers, out ITypeDescriptor descriptor, out int typeId)
            {
                descriptor = _typeSerializers.DescriptorOf(type);
                if (Descriptor2IdMap.TryGetValue(descriptor, out typeId))
                {
                    typeSerializers = _typeSerializers;
                    return;
                }
                if (_typeSerializersMapping._descriptor2IdMap.TryGetValue(descriptor, out typeId))
                {
                    typeSerializers = _typeSerializers;
                    return;
                }
                throw new InvalidOperationException(String.Format("Type {0} was not registered using StoreNewDescriptors", type.FullName));
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
            ITypeDescriptor descriptor;
            TypeSerializers typeSerializers;
            GetDescriptorAndTypeId(obj.GetType(), out typeSerializers, out descriptor, out typeId);
            StoreObjectCore(typeSerializers, writer, obj, typeId, descriptor, this);
        }

        static void StoreObjectCore(TypeSerializers typeSerializers, AbstractBufferedWriter writer, object obj, int typeId, ITypeDescriptor descriptor, ITypeSerializersLightMapping mapping)
        {
            writer.WriteVUInt32((uint)typeId);
            var simpleSaver = typeSerializers.GetSimpleSaver(descriptor);
            if (simpleSaver != null)
            {
                simpleSaver(writer, obj);
                return;
            }
            var complexSaver = typeSerializers.GetComplexSaver(descriptor);
            ITypeBinarySerializerContext ctx = new TypeBinarySerializerContext(mapping, writer, obj);
            complexSaver(writer, ctx, obj);
        }

        internal class TypeBinarySerializerContext : ITypeBinarySerializerContext
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
                ITypeDescriptor descriptor;
                int typeId;
                _mapping.GetDescriptorAndTypeId(obj.GetType(), out typeSerializers, out descriptor, out typeId);
                _writer.WriteVUInt32((uint)typeId);
                var simpleSaver = typeSerializers.GetSimpleSaver(descriptor);
                if (simpleSaver != null)
                {
                    simpleSaver(_writer, obj);
                    return;
                }
                var complexSaver = typeSerializers.GetComplexSaver(descriptor);
                complexSaver(_writer, this, obj);
            }
        }

        public void FinishNewDescriptors(AbstractBufferedWriter writer)
        {
        }

        public void GetDescriptorAndTypeId(Type type, out TypeSerializers typeSerializers, out ITypeDescriptor descriptor, out int typeId)
        {
            descriptor = _typeSerializers.DescriptorOf(type);
            if (_descriptor2IdMap.TryGetValue(descriptor, out typeId))
            {
                typeSerializers = _typeSerializers;
                return;
            }
            throw new InvalidOperationException(String.Format("Type {0} was not registered using StoreNewDescriptors", type.FullName));
        }
    }
}