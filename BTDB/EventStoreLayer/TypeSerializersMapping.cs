using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using BTDB.Collections;
using BTDB.Encrypted;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

class TypeSerializersMapping : ITypeSerializersMapping, ITypeSerializersLightMapping, ITypeSerializersId2LoaderMapping
{
    const int ReservedBuiltInTypes = 50;
    StructList<InfoForType?> _id2DescriptorMap;
    readonly Dictionary<object, InfoForType> _typeOrDescriptor2Info = new Dictionary<object, InfoForType>(ReferenceEqualityComparer<object>.Instance);
    readonly TypeSerializers _typeSerializers;
    readonly ISymmetricCipher _symmetricCipher;

    public TypeSerializersMapping(TypeSerializers typeSerializers)
    {
        _typeSerializers = typeSerializers;
        _symmetricCipher = _typeSerializers.GetSymmetricCipher();
        AddBuildInTypes();
    }

    public void Reset()
    {
        _id2DescriptorMap.Clear();
        _typeOrDescriptor2Info.Clear();
        AddBuildInTypes();
    }

    void AddBuildInTypes()
    {
        _id2DescriptorMap.Add(null); // 0 = null
        _id2DescriptorMap.Add(null); // 1 = back reference
        foreach (var predefinedType in BasicSerializersFactory.TypeDescriptors)
        {
            var infoForType = new InfoForType { Id = (int)_id2DescriptorMap.Count, Descriptor = predefinedType };
            _typeOrDescriptor2Info[predefinedType] = infoForType;
            _id2DescriptorMap.Add(infoForType);
        }
        while (_id2DescriptorMap.Count < ReservedBuiltInTypes) _id2DescriptorMap.Add(null);
    }

    public void LoadTypeDescriptors(ref SpanReader reader)
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
                    descriptor = new ObjectTypeDescriptor(_typeSerializers, ref reader, NestedDescriptorReader,
                        _typeSerializers.Options.TypeDescriptorOptions);
                    break;
                case TypeCategory.List:
                    descriptor = new ListTypeDescriptor(_typeSerializers, ref reader, NestedDescriptorReader);
                    break;
                case TypeCategory.Dictionary:
                    descriptor = new DictionaryTypeDescriptor(_typeSerializers, ref reader, NestedDescriptorReader);
                    break;
                case TypeCategory.Enum:
                    descriptor = new EnumTypeDescriptor(_typeSerializers, ref reader);
                    break;
                case TypeCategory.Nullable:
                    descriptor = new NullableTypeDescriptor(_typeSerializers, ref reader, NestedDescriptorReader);
                    break;
                case TypeCategory.Tuple:
                    descriptor = new TupleTypeDescriptor(_typeSerializers, ref reader, NestedDescriptorReader);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            while (typeId >= _id2DescriptorMap.Count)
                _id2DescriptorMap.Add(null);
            _id2DescriptorMap[(int)typeId] ??= new InfoForType { Id = (int)typeId, Descriptor = descriptor };
            typeId = reader.ReadVUInt32();
        }
        for (var i = firstTypeId; i < _id2DescriptorMap.Count; i++)
        {
            _id2DescriptorMap[i]!.Descriptor.MapNestedTypes(d => d is PlaceHolderDescriptor placeHolderDescriptor ? _id2DescriptorMap[(int)placeHolderDescriptor.TypeId]!.Descriptor : d);
        }
        // This additional cycle is needed to fill names of recursive structures
        for (var i = firstTypeId; i < _id2DescriptorMap.Count; i++)
        {
            _id2DescriptorMap[i]!.Descriptor.MapNestedTypes(d => d);
        }
        for (var i = firstTypeId; i < _id2DescriptorMap.Count; i++)
        {
            var infoForType = _id2DescriptorMap[(int)i];
            var descriptor = _typeSerializers.MergeDescriptor(infoForType!.Descriptor);
            infoForType.Descriptor = descriptor;
            _typeOrDescriptor2Info[descriptor] = infoForType;
        }
    }

    ITypeDescriptor NestedDescriptorReader(ref SpanReader reader)
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
        internal uint TypeId { get; }

        public PlaceHolderDescriptor(uint typeId)
        {
            TypeId = typeId;
        }

        bool IEquatable<ITypeDescriptor>.Equals(ITypeDescriptor other)
        {
            throw new InvalidOperationException();
        }

        string ITypeDescriptor.Name => "";

        bool ITypeDescriptor.FinishBuildFromType(ITypeDescriptorFactory factory)
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

        Type? ITypeDescriptor.GetPreferredType()
        {
            return null;
        }

        public Type? GetPreferredType(Type targetType)
        {
            return null;
        }

        public bool AnyOpNeedsCtx()
        {
            throw new InvalidOperationException();
        }

        public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType)
        {
            throw new InvalidOperationException();
        }

        public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            throw new InvalidOperationException();
        }

        public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue, Type valueType)
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

        bool ITypeDescriptor.Sealed => false;

        bool ITypeDescriptor.StoredInline => false;

        public bool LoadNeedsHelpWithConversion => false;

        void ITypeDescriptor.ClearMappingToType()
        {
            throw new InvalidOperationException();
        }

        public bool ContainsField(string name)
        {
            throw new InvalidOperationException();
        }

        public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields => Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

        public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers, Func<ITypeDescriptor, ITypeDescriptor> map)
        {
            throw new InvalidOperationException();
        }
    }

    public object? LoadObject(ref SpanReader reader)
    {
        var typeId = reader.ReadVUInt32();
        if (typeId == 0)
        {
            return null;
        }
        if (typeId == 1)
        {
            throw new InvalidDataException("Back reference cannot be first object");
        }
        return Load(typeId, ref reader, null);
    }

    public object Load(uint typeId, ref SpanReader reader, ITypeBinaryDeserializerContext? context)
    {
        var infoForType = _id2DescriptorMap[typeId];
        if (infoForType!.Loader == null)
        {
            infoForType.Loader = _typeSerializers.GetLoader(infoForType.Descriptor);
        }
        return infoForType.Loader(ref reader, context, this, infoForType.Descriptor);
    }

    public ISymmetricCipher GetSymmetricCipher() => _symmetricCipher;

    public bool SomeTypeStored => false;

    public IDescriptorSerializerContext StoreNewDescriptors(object? obj)
    {
        if (obj == null) return this;
        InfoForType infoForType;
        var objType = obj.GetType();
        if (obj is IKnowDescriptor iKnowDescriptor)
        {
            var descriptor = iKnowDescriptor.GetDescriptor();
            if (!_typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
            {
                infoForType = new InfoForType { Id = 0, Descriptor = descriptor };
            }
        }
        else
        {
            if (!_typeOrDescriptor2Info.TryGetValue(objType, out infoForType))
            {
                var descriptor = _typeSerializers.DescriptorOf(objType);
                if (!_typeOrDescriptor2Info.TryGetValue(descriptor!, out infoForType))
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
            ctx = new DescriptorSerializerContext(this);
            ctx.AddDescriptor(infoForType);
        }

        ref var actions = ref infoForType.Type2Actions.GetOrAddValueRef(objType);
        if (!actions.KnownNewTypeDiscoverer)
        {
            actions.NewTypeDiscoverer = _typeSerializers.GetNewDescriptorSaver(infoForType.Descriptor, objType);
            actions.KnownNewTypeDiscoverer = true;
        }
        var action = actions.NewTypeDiscoverer;
        if (action != null)
        {
            ctx ??= new DescriptorSerializerContext(this);
            try
            {
                action(obj, ctx);
            }
            catch (Exception e)
            {
                throw new BTDBException(
                    $"Failed store new descriptors for {objType.ToSimpleName()} with descriptor {infoForType.Descriptor.Describe()}", e);
            }
        }
        if (ctx is { SomeTypeStored: true })
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
        readonly TypeSerializers _typeSerializers;
        StructList<InfoForType> _id2InfoMap;
        readonly Dictionary<object, InfoForType> _typeOrDescriptor2InfoMap = new Dictionary<object, InfoForType>(ReferenceEqualityComparer<object>.Instance);

        public DescriptorSerializerContext(TypeSerializersMapping typeSerializersMapping)
        {
            _typeSerializersMapping = typeSerializersMapping;
            _typeSerializers = _typeSerializersMapping._typeSerializers;
        }

        public void AddDescriptor(InfoForType infoForType)
        {
            infoForType.Id = (int)(_typeSerializersMapping._id2DescriptorMap.Count + _id2InfoMap.Count);
            _typeOrDescriptor2InfoMap.Add(infoForType.Descriptor, infoForType);
            _id2InfoMap.Add(infoForType);
            var idx = 0;
            ITypeDescriptor nestedDescriptor;
            while ((nestedDescriptor = infoForType.Descriptor.NestedType(idx)) != null)
            {
                if (!TryDescriptor2Id(nestedDescriptor, out _))
                    AddDescriptor(new InfoForType { Descriptor = nestedDescriptor });
                idx++;
            }
        }

        uint Descriptor2Id(ITypeDescriptor? descriptor)
        {
            if (descriptor == null) return 0;
            if (_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out var infoForType))
                return (uint)infoForType.Id;
            if (_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                return (uint)infoForType.Id;
            throw new InvalidOperationException();
        }

        bool TryDescriptor2Id(ITypeDescriptor descriptor, out int typeId)
        {
            if (_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out var infoForType) ||
                _typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
            {
                typeId = infoForType.Id;
                return true;
            }
            typeId = 0;
            return false;
        }

        public bool SomeTypeStored => _id2InfoMap.Count != 0;

        public IDescriptorSerializerContext StoreNewDescriptors(object? obj)
        {
            if (obj == null) return this;
            InfoForType infoForType;
            var objType = obj.GetType();
            if (obj is IKnowDescriptor iKnowDescriptor)
            {
                var descriptor = iKnowDescriptor.GetDescriptor();
                if (!_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out infoForType) &&
                    !_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType))
                {
                    infoForType = new InfoForType { Id = 0, Descriptor = descriptor };
                }
            }
            else
            {
                if (!_typeOrDescriptor2InfoMap.TryGetValue(objType, out infoForType) &&
                    !_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(objType, out infoForType))
                {
                    var descriptor = _typeSerializers.DescriptorOf(objType);
                    if (_typeOrDescriptor2InfoMap.TryGetValue(descriptor!, out infoForType))
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

            ref var actions = ref infoForType.Type2Actions.GetOrAddValueRef(objType);
            if (!actions.KnownNewTypeDiscoverer)
            {
                actions.NewTypeDiscoverer = _typeSerializers.GetNewDescriptorSaver(infoForType.Descriptor, objType);
                actions.KnownNewTypeDiscoverer = true;
            }
            var action = actions.NewTypeDiscoverer;
            action?.Invoke(obj, this);
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

        public void StoreObject(ref SpanWriter writer, object? obj)
        {
            if (obj == null)
            {
                writer.WriteUInt8(0);
                return;
            }

            var infoForType = GetInfoFromObject(obj, out _);
            StoreObjectCore(_typeSerializers, ref writer, obj, infoForType, this);
        }

        public void FinishNewDescriptors(ref SpanWriter writer)
        {
            if (SomeTypeStored)
            {
                for (var i = (int)_id2InfoMap.Count - 1; i >= 0; i--)
                {
                    writer.WriteVUInt32((uint)(i + _typeSerializersMapping._id2DescriptorMap.Count));
                    TypeSerializers.StoreDescriptor(_id2InfoMap[i].Descriptor, ref writer, Descriptor2Id);
                }
                writer.WriteUInt8(0);
            }
        }

        public InfoForType GetInfoFromObject(object obj, out TypeSerializers typeSerializers)
        {
            InfoForType infoForType;
            if (obj is IKnowDescriptor iKnowDescriptor)
            {
                var descriptor = iKnowDescriptor.GetDescriptor();
                if (!_typeOrDescriptor2InfoMap.TryGetValue(descriptor, out infoForType))
                    _typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType);
            }
            else
            {
                var objType = obj.GetType();
                if (!_typeOrDescriptor2InfoMap.TryGetValue(objType, out infoForType) && !_typeSerializersMapping._typeOrDescriptor2Info.TryGetValue(objType, out infoForType))
                {
                    var descriptor = _typeSerializers.DescriptorOf(objType);
                    if (_typeOrDescriptor2InfoMap.TryGetValue(descriptor!, out infoForType))
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
                throw new InvalidOperationException(
                    $"Type {obj.GetType().FullName} was not registered using StoreNewDescriptors");
            }
            typeSerializers = _typeSerializers;
            return infoForType;
        }

        public ISymmetricCipher GetSymmetricCipher() => _typeSerializers.GetSymmetricCipher();

        void IDescriptorSerializerLiteContext.StoreNewDescriptors(object obj)
        {
            StoreNewDescriptors(obj);
        }
    }

    public void StoreObject(ref SpanWriter writer, object? obj)
    {
        if (obj == null)
        {
            writer.WriteUInt8(0);
            return;
        }

        var infoForType = GetInfoFromObject(obj, out var typeSerializers);
        StoreObjectCore(typeSerializers, ref writer, obj, infoForType, this);
    }

    static void StoreObjectCore(TypeSerializers typeSerializers, ref SpanWriter writer, object obj, InfoForType infoForType, ITypeSerializersLightMapping mapping)
    {
        writer.WriteVUInt32((uint)infoForType.Id);
        var objType = obj.GetType();
        ref var actions = ref infoForType.Type2Actions.GetOrAddValueRef(objType);
        if (!actions.KnownSimpleSaver)
        {
            try
            {
                actions.SimpleSaver = typeSerializers.GetSimpleSaver(infoForType.Descriptor, objType);
            }
            catch (Exception e)
            {
                throw new BTDBException(
                    $"Failed creating SimpleSaver for {objType.ToSimpleName()} with descriptor {infoForType.Descriptor.Describe()}", e);
            }
            actions.KnownSimpleSaver = true;
        }
        var simpleSaver = actions.SimpleSaver;
        if (simpleSaver != null)
        {
            simpleSaver(ref writer, obj);
            return;
        }
        if (!actions.KnownComplexSaver)
        {
            try
            {
                actions.ComplexSaver = typeSerializers.GetComplexSaver(infoForType.Descriptor, objType);
            }
            catch (Exception e)
            {
                throw new BTDBException(
                    $"Failed creating SimpleSaver for {objType.ToSimpleName()} with descriptor {infoForType.Descriptor.Describe()}", e);
            }
            actions.KnownComplexSaver = true;
        }
        var complexSaver = actions.ComplexSaver;
        var ctx = new TypeBinarySerializerContext(mapping, obj);
        complexSaver(ref writer, ctx, obj);
    }

    class TypeBinarySerializerContext : ITypeBinarySerializerContext
    {
        readonly ITypeSerializersLightMapping _mapping;
        readonly Dictionary<object, uint> _backRefs = new Dictionary<object, uint>(ReferenceEqualityComparer<object>.Instance);

        public TypeBinarySerializerContext(ITypeSerializersLightMapping mapping, object obj)
        {
            _mapping = mapping;
            _backRefs.Add(obj, 0);
        }

        public void StoreObject(ref SpanWriter writer, object? obj)
        {
            if (obj == null)
            {
                writer.WriteByteZero();
                return;
            }

            if (_backRefs.TryGetValue(obj, out var backRefId))
            {
                writer.WriteUInt8(1);
                writer.WriteVUInt32(backRefId);
                return;
            }
            _backRefs.Add(obj, (uint)_backRefs.Count);
            var infoForType = _mapping.GetInfoFromObject(obj, out var typeSerializers);
            writer.WriteVUInt32((uint)infoForType.Id);
            var objType = obj.GetType();
            ref var actions = ref infoForType.Type2Actions.GetOrAddValueRef(objType);
            if (!actions.KnownSimpleSaver)
            {
                actions.SimpleSaver = typeSerializers.GetSimpleSaver(infoForType.Descriptor, objType);
                actions.KnownSimpleSaver = true;
            }
            var simpleSaver = actions.SimpleSaver;
            if (simpleSaver != null)
            {
                simpleSaver(ref writer, obj);
                return;
            }
            if (!actions.KnownComplexSaver)
            {
                actions.ComplexSaver = typeSerializers.GetComplexSaver(infoForType.Descriptor, objType);
                actions.KnownComplexSaver = true;
            }
            var complexSaver = actions.ComplexSaver;
            complexSaver(ref writer, this, obj);
        }

        public void StoreEncryptedString(ref SpanWriter outsideWriter, EncryptedString value)
        {
            var writer = new SpanWriter();
            writer.WriteString(value);
            var cipher = _mapping.GetSymmetricCipher();
            var plain = writer.GetSpan();
            var encSize = cipher.CalcEncryptedSizeFor(plain);
            var enc = new byte[encSize];
            cipher.Encrypt(plain, enc);
            outsideWriter.WriteByteArray(enc);
        }
    }

    public void FinishNewDescriptors(ref SpanWriter writer)
    {
    }

    public InfoForType GetInfoFromObject(object obj, out TypeSerializers typeSerializers)
    {
        InfoForType infoForType;
        if (obj is IKnowDescriptor iKnowDescriptor)
        {
            var descriptor = iKnowDescriptor.GetDescriptor();
            _typeOrDescriptor2Info.TryGetValue(descriptor, out infoForType);
        }
        else
        {
            var objType = obj.GetType();
            if (!_typeOrDescriptor2Info.TryGetValue(objType, out infoForType))
            {
                var descriptor = _typeSerializers.DescriptorOf(objType);
                if (_typeOrDescriptor2Info.TryGetValue(descriptor!, out infoForType))
                {
                    _typeOrDescriptor2Info[objType] = infoForType;
                }
            }
        }
        typeSerializers = _typeSerializers;
        if (infoForType == null)
        {
            throw new InvalidOperationException(
                $"Type {obj.GetType().FullName} was not registered using StoreNewDescriptors");
        }
        return infoForType;
    }
}
