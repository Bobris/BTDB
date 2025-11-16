using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.EventStoreLayer;

class ListTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor
{
    readonly ITypeDescriptorCallbacks _typeSerializers;
    Type? _type;
    Type? _itemType;
    ITypeDescriptor? _itemDescriptor;
    string? _name;
    readonly ITypeConvertorGenerator _convertGenerator;

    public ListTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, Type type)
    {
        _convertGenerator = typeSerializers.ConvertorGenerator;
        _typeSerializers = typeSerializers;
        _type = type;
        _itemType = GetItemType(type);
    }

    public ListTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, ref MemReader reader,
        DescriptorReader nestedDescriptorReader)
        : this(typeSerializers, nestedDescriptorReader(ref reader))
    {
    }

    ListTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, ITypeDescriptor itemDesc)
    {
        _convertGenerator = typeSerializers.ConvertorGenerator;
        _typeSerializers = typeSerializers;
        InitFromItemDescriptor(itemDesc);
    }

    void InitFromItemDescriptor(ITypeDescriptor descriptor)
    {
        if (descriptor == _itemDescriptor && _name != null) return;
        _itemDescriptor = descriptor;
        if ((descriptor.Name?.Length ?? 0) == 0) return;
        Sealed = _itemDescriptor.Sealed;
        Name = $"List<{_itemDescriptor.Name}>";
    }

    public bool Equals(ITypeDescriptor? other)
    {
        return Equals(other, null);
    }

    public override int GetHashCode()
    {
#pragma warning disable RECS0025 // Non-readonly field referenced in 'GetHashCode()'
        // ReSharper disable once NonReadonlyMemberInGetHashCode
        return 33 * _itemDescriptor!.GetHashCode();
#pragma warning restore RECS0025 // Non-readonly field referenced in 'GetHashCode()'
    }

    public string Name
    {
        get
        {
            if (_name == null) InitFromItemDescriptor(_itemDescriptor!);
            return _name!;
        }
        private set => _name = value;
    }

    public bool FinishBuildFromType(ITypeDescriptorFactory factory)
    {
        var descriptor = factory.Create(_itemType!);
        if (descriptor == null) return false;
        InitFromItemDescriptor(descriptor);
        return true;
    }

    public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
    {
        text.Append("List<");
        _itemDescriptor!.BuildHumanReadableFullName(text, stack, indent);
        text.Append('>');
    }

    public bool Equals(ITypeDescriptor? other, Dictionary<ITypeDescriptor, ITypeDescriptor>? equalities)
    {
        if (ReferenceEquals(this, other)) return true;
        if (other is not ListTypeDescriptor o) return false;
        return _itemDescriptor!.Equals(o._itemDescriptor!, equalities);
    }

    public Type GetPreferredType()
    {
        if (_type == null)
        {
            _itemType = _typeSerializers.LoadAsType(_itemDescriptor!);
            _type = typeof(ICollection<>).MakeGenericType(_itemType);
        }

        return _type;
    }

    public Type GetPreferredType(Type targetType)
    {
        if (_type == targetType) return _type;
        var targetICollection = targetType.GetInterface("ICollection`1") ?? targetType;
        var targetTypeArguments = targetICollection.GetGenericArguments();
        var itemType = _typeSerializers.LoadAsType(_itemDescriptor!, targetTypeArguments[0]);
        return targetType.GetGenericTypeDefinition().MakeGenericType(itemType);
    }

    public bool AnyOpNeedsCtx()
    {
        return !_itemDescriptor!.StoredInline || _itemDescriptor.AnyOpNeedsCtx();
    }

    ref struct ListItemLoaderCtx
    {
        internal nint StoragePtr;
        internal object Object;
        internal Layer2Loader ItemLoader;
        internal unsafe delegate*<object, ref byte, void> Adder;
        internal ITypeBinaryDeserializerContext? Ctx;
        internal ref MemReader Reader;
    }

    public unsafe Layer2Loader GenerateLoad(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (targetType == typeof(object))
        {
            var genericItemLoad = _itemDescriptor!.GenerateLoadEx(typeof(object), typeConverterFactory);
            return (ref MemReader reader, ITypeBinaryDeserializerContext? ctx, ref byte value) =>
            {
                var count = reader.ReadVUInt32();
                if (count == 0)
                {
                    Unsafe.As<byte, object?>(ref value) = null;
                    return;
                }

                count--;
                var obj = new ListWithDescriptor<object?>((int)count, this);
                while (count-- != 0)
                {
                    object? item = null;
                    genericItemLoad(ref reader, ctx,
                        ref Unsafe.As<object?, byte>(ref item));
                    obj.Add(item);
                }

                Unsafe.As<byte, object?>(ref value) = obj;
            };
        }

        var collectionMetadata = ReflectionMetadata.FindCollectionByType(targetType!);
        if (collectionMetadata == null)
        {
            var itemType = targetType.IsArray ? targetType.GetElementType()! : targetType.GenericTypeArguments[0];
            var ienumerableType = typeof(IEnumerable<>).MakeGenericType(itemType);
            collectionMetadata = ReflectionMetadata.FindCollectionByType(ienumerableType);
            if (collectionMetadata == null)
            {
                var ilistType = typeof(IList<>).MakeGenericType(itemType);
                collectionMetadata = ReflectionMetadata.FindCollectionByType(ilistType);
                if (collectionMetadata == null)
                {
                    var listType = typeof(List<>).MakeGenericType(itemType);
                    collectionMetadata = ReflectionMetadata.FindCollectionByType(listType);
                    if (collectionMetadata == null)
                    {
                        var isetType = typeof(ISet<>).MakeGenericType(itemType);
                        collectionMetadata = ReflectionMetadata.FindCollectionByType(isetType);
                        if (collectionMetadata == null)
                        {
                            var hashsetType = typeof(HashSet<>).MakeGenericType(itemType);
                            collectionMetadata = ReflectionMetadata.FindCollectionByType(hashsetType);
                        }
                    }
                }
            }
        }

        if (collectionMetadata == null)
            throw new BTDBException("Cannot find collection metadata for " + targetType.ToSimpleName());

        var itemLoad = _itemDescriptor!.GenerateLoadEx(collectionMetadata.ElementKeyType, typeConverterFactory);
        var itemStackAllocator = ReflectionMetadata.FindStackAllocatorByType(collectionMetadata.ElementKeyType);

        return (ref MemReader reader, ITypeBinaryDeserializerContext? ctx, ref byte value) =>
        {
            var count = reader.ReadVUInt32();
            if (count == 0)
            {
                Unsafe.As<byte, object?>(ref value) = null;
                return;
            }

            count--;
            var obj = collectionMetadata!.Creator(count);
            var loaderCtx = new ListItemLoaderCtx
            {
                Adder = collectionMetadata.Adder,
                ItemLoader = itemLoad,
                Object = obj,
                Ctx = ctx,
                Reader = ref reader,
            };
            for (var i = 0; i != count; i++)
            {
                itemStackAllocator(ref Unsafe.As<ListItemLoaderCtx, byte>(ref loaderCtx), ref loaderCtx.StoragePtr,
                    &Nested);

                static void Nested(ref byte value)
                {
                    ref var context = ref Unsafe.As<byte, ListItemLoaderCtx>(ref value);
                    context.ItemLoader(ref context.Reader, context.Ctx,
                        ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                    context.Adder(context.Object, ref Unsafe.AsRef<byte>((void*)context.StoragePtr));
                }
            }

            Unsafe.As<byte, object?>(ref value) = obj;
        };
    }

    public void Skip(ref MemReader reader, ITypeBinaryDeserializerContext? ctx)
    {
        var count = reader.ReadVUInt32();
        if (count == 0) return;
        count--;
        for (var i = 0u; i != count; i++)
        {
            _itemDescriptor!.SkipEx(ref reader, ctx);
        }
    }

    public Layer2Saver GenerateSave(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        var itemType = targetType.IsSZArray ? targetType.GetElementType()! : targetType.GenericTypeArguments[0];
        var hashSetType = typeof(HashSet<>).MakeGenericType(itemType);
        var listType = typeof(List<>).MakeGenericType(itemType);
        var arrayType = itemType.MakeArrayType();
        var saveItem = _itemDescriptor!.GenerateSaveEx(itemType, typeConverterFactory);
        var layout = RawData.GetHashSetEntriesLayout(itemType);
        return (ref MemWriter writer, ITypeBinarySerializerContext? ctx, ref byte value) =>
        {
            var obj = Unsafe.As<byte, object>(ref value);
            if (obj == null)
            {
                writer.WriteByteZero();
                return;
            }

            var objType = obj.GetType();
            if (arrayType.IsAssignableFrom(objType))
            {
                var count = (uint)RawData.GetArrayLength(ref value);
                writer.WriteVUInt32(count + 1);
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    saveItem(ref writer, ctx, ref RawData.Ref(obj, offset));
                }
            }
            else if (listType.IsAssignableFrom(objType))
            {
                var count = (uint)Unsafe.As<ICollection>(obj).Count;
                writer.WriteVUInt32(count + 1);
                obj = RawData.ListItems(Unsafe.As<List<object>>(obj));
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    saveItem(ref writer, ctx, ref RawData.Ref(obj, offset));
                }
            }
            else if (hashSetType.IsAssignableFrom(objType))
            {
                var count = Unsafe.As<byte, uint>(ref RawData.Ref(obj,
                    RawData.Align(8 + 4 * (uint)Unsafe.SizeOf<nint>(), 8)));
                writer.WriteVUInt32(count + 1);
                if (count != 0)
                {
                    obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(obj));
                    ref readonly var mt = ref RawData.MethodTableRef(obj);
                    var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                    var offsetDelta = mt.ComponentSize;
                    if (offsetDelta != layout.Size)
                        throw new BTDBException("Invalid HashSet layout " + offsetDelta + " != " + layout.Size);
                    for (var i = 0; i < count; i++, offset += offsetDelta)
                    {
                        if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + layout.OffsetNext)) < -1)
                        {
                            continue;
                        }

                        saveItem(ref writer, ctx, ref RawData.Ref(obj, offset + layout.Offset));
                    }
                }
            }
            else if (objType.IsGenericType && objType.Name == "<>z__ReadOnlyArray`1" &&
                     objType.GenericTypeArguments[0] == itemType)
            {
                obj = RawData.ListItems(Unsafe.As<List<object>>(obj));
                var count = (uint)RawData.GetArrayLength(ref Unsafe.As<object, byte>(ref obj));
                writer.WriteVUInt32(count + 1);
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    saveItem(ref writer, ctx, ref RawData.Ref(obj, offset));
                }
            }
            else throw new BTDBException("Cannot save type " + objType.ToSimpleName());
        };
    }

    public Layer2NewDescriptor? GenerateNewDescriptor(Type targetType, ITypeConverterFactory typeConverterFactory,
        bool forbidSerializationOfLazyDBObjects)
    {
        if (_itemDescriptor!.Sealed) return null;
        var itemType = targetType.IsSZArray ? targetType.GetElementType()! : targetType.GenericTypeArguments[0];
        var nestedNewDescriptor =
            _itemDescriptor!.GenerateNewDescriptorEx(itemType, typeConverterFactory,
                forbidSerializationOfLazyDBObjects);
        if (nestedNewDescriptor == null) return null;
        var arrayType = itemType.MakeArrayType();
        var hashSetType = typeof(HashSet<>).MakeGenericType(itemType);
        var listType = typeof(List<>).MakeGenericType(itemType);
        var layout = RawData.GetHashSetEntriesLayout(itemType);
        return (ctx, ref value) =>
        {
            var obj = Unsafe.As<byte, object?>(ref value);
            if (obj == null)
            {
                return;
            }

            var objType = obj.GetType();
            if (arrayType.IsAssignableFrom(objType))
            {
                var count = (uint)RawData.GetArrayLength(ref value);
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    nestedNewDescriptor(ctx, ref RawData.Ref(obj, offset));
                }
            }
            else if (listType.IsAssignableFrom(objType))
            {
                var count = (uint)Unsafe.As<ICollection>(obj).Count;
                obj = RawData.ListItems(Unsafe.As<List<object>>(obj));
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    nestedNewDescriptor(ctx, ref RawData.Ref(obj, offset));
                }
            }
            else if (hashSetType.IsAssignableFrom(objType))
            {
                var count = Unsafe.As<byte, uint>(ref RawData.Ref(obj,
                    RawData.Align(8 + 4 * (uint)Unsafe.SizeOf<nint>(), 8)));
                if (count != 0)
                {
                    obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(obj));
                    ref readonly var mt = ref RawData.MethodTableRef(obj);
                    var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                    var offsetDelta = mt.ComponentSize;
                    if (offsetDelta != layout.Size)
                        throw new BTDBException("Invalid HashSet layout " + offsetDelta + " != " + layout.Size);
                    for (var i = 0; i < count; i++, offset += offsetDelta)
                    {
                        if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + layout.OffsetNext)) < -1)
                        {
                            continue;
                        }

                        nestedNewDescriptor(ctx, ref RawData.Ref(obj, offset + layout.Offset));
                    }
                }
            }
            else if (objType.IsGenericType && objType.Name == "<>z__ReadOnlyArray`1" &&
                     objType.GenericTypeArguments[0] == itemType)
            {
                obj = RawData.ListItems(Unsafe.As<List<object>>(obj));
                var count = (uint)RawData.GetArrayLength(ref Unsafe.As<object, byte>(ref obj));
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    nestedNewDescriptor(ctx, ref RawData.Ref(obj, offset));
                }
            }
            else throw new BTDBException("Cannot iterate new descriptors for type " + objType.ToSimpleName());
        };
    }

    static Type GetInterface(Type type) => type.GetInterface("ICollection`1") ?? type;

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx,
        Action<IILGen> pushDescriptor, Type targetType)
    {
        if (targetType == typeof(object))
            targetType = GetPreferredType();
        var localCount = ilGenerator.DeclareLocal(typeof(int));
        var targetICollection = GetInterface(targetType);
        var targetTypeArguments = targetICollection.GetGenericArguments();
        var itemType = _typeSerializers.LoadAsType(_itemDescriptor!, targetTypeArguments[0]);
        if (targetType.IsArray)
        {
            var localArray = ilGenerator.DeclareLocal(targetType);
            var loadFinished = ilGenerator.DefineLabel();
            var localIndex = ilGenerator.DeclareLocal(typeof(int));
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Ldnull()
                .Stloc(localArray)
                .LdcI4(0)
                .Stloc(localIndex)
                .Do(pushReader)
                .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
                .ConvI4()
                .Dup()
                .Stloc(localCount)
                .Brfalse(loadFinished)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .Dup()
                .Stloc(localCount)
                .Newarr(itemType)
                .Stloc(localArray)
                .Mark(next)
                .Ldloc(localCount)
                .Ldloc(localIndex)
                .Sub()
                .Brfalse(loadFinished)
                .Ldloc(localArray)
                .Ldloc(localIndex);
            _itemDescriptor!.GenerateLoadEx(ilGenerator, pushReader, pushCtx,
                il => il.Do(pushDescriptor).LdcI4(0).Callvirt(() => default(ITypeDescriptor)!.NestedType(0)), itemType,
                _convertGenerator);
            ilGenerator
                .Stelem(itemType)
                .Ldloc(localIndex)
                .LdcI4(1)
                .Add()
                .Stloc(localIndex)
                .Br(next)
                .Mark(loadFinished)
                .Ldloc(localArray)
                .Castclass(targetType);
        }
        else
        {
            var isSet = targetType.InheritsOrImplements(typeof(ISet<>));
            var listType =
                (isSet ? typeof(HashSetWithDescriptor<>) : typeof(ListWithDescriptor<>)).MakeGenericType(itemType);

            if (!targetType.IsAssignableFrom(listType))
                throw new NotSupportedException(
                    $"List type {listType.ToSimpleName()} is not assignable to {targetType.ToSimpleName()}.");
            var localList = ilGenerator.DeclareLocal(listType);
            var loadFinished = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Ldnull()
                .Stloc(localList)
                .Do(pushReader)
                .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
                .ConvI4()
                .Dup()
                .Stloc(localCount)
                .Brfalse(loadFinished)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .Dup()
                .Stloc(localCount)
                .Do(pushDescriptor)
                .Newobj(listType.GetConstructor(new[] { typeof(int), typeof(ITypeDescriptor) })!)
                .Stloc(localList)
                .Mark(next)
                .Ldloc(localCount)
                .Brfalse(loadFinished)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .Stloc(localCount)
                .Ldloc(localList);
            _itemDescriptor!.GenerateLoadEx(ilGenerator, pushReader, pushCtx,
                il => il.Do(pushDescriptor).LdcI4(0).Callvirt(() => default(ITypeDescriptor)!.NestedType(0)), itemType,
                _convertGenerator);
            ilGenerator
                .Callvirt(listType.GetInterface("ICollection`1")!.GetMethod("Add")!)
                .Br(next)
                .Mark(loadFinished)
                .Ldloc(localList)
                .Castclass(targetType);
        }
    }

    public ITypeNewDescriptorGenerator? BuildNewDescriptorGenerator()
    {
        if (_itemDescriptor!.Sealed) return null;
        return new TypeNewDescriptorGenerator(this);
    }

    class TypeNewDescriptorGenerator : ITypeNewDescriptorGenerator
    {
        readonly ListTypeDescriptor _listTypeDescriptor;

        public TypeNewDescriptorGenerator(ListTypeDescriptor listTypeDescriptor)
        {
            _listTypeDescriptor = listTypeDescriptor;
        }

        public void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushObj, Action<IILGen> pushCtx, Type type)
        {
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();

            if (type == typeof(object))
                type = _listTypeDescriptor.GetPreferredType();
            var targetInterface = GetInterface(type);
            var targetTypeArguments = targetInterface.GetGenericArguments();
            var itemType =
                _listTypeDescriptor._typeSerializers.LoadAsType(_listTypeDescriptor._itemDescriptor!,
                    targetTypeArguments[0]);
            if (_listTypeDescriptor._type == null) _listTypeDescriptor._type = type;
            var isConcreteImplementation = type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(List<>) ||
                                                                  type.GetGenericTypeDefinition() == typeof(HashSet<>));
            var typeAsICollection = isConcreteImplementation ? type : typeof(ICollection<>).MakeGenericType(itemType);
            var getEnumeratorMethod = isConcreteImplementation
                ? typeAsICollection.GetMethods()
                    .Single(m => m.Name == nameof(IEnumerable.GetEnumerator) && m.ReturnType.IsValueType &&
                                 m.GetParameters().Length == 0)
                : typeAsICollection.GetInterface("IEnumerable`1")!.GetMethod(nameof(IEnumerable.GetEnumerator));
            var typeAsIEnumerator = getEnumeratorMethod!.ReturnType;
            var currentGetter = typeAsIEnumerator.GetProperty(nameof(IEnumerator.Current))!.GetGetMethod();
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            ilGenerator
                .Do(pushObj)
                .Castclass(typeAsICollection)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Do(il =>
                {
                    if (isConcreteImplementation)
                    {
                        il
                            .Ldloca(localEnumerator)
                            .Call(typeAsIEnumerator.GetMethod(nameof(IEnumerator.MoveNext))!);
                    }
                    else
                    {
                        il
                            .Ldloc(localEnumerator)
                            .Callvirt(() => default(IEnumerator)!.MoveNext());
                    }
                })
                .Brfalse(finish)
                .Do(pushCtx)
                .Do(il =>
                {
                    if (isConcreteImplementation)
                    {
                        il
                            .Ldloca(localEnumerator)
                            .Call(currentGetter!);
                    }
                    else
                    {
                        il
                            .Ldloc(localEnumerator)
                            .Callvirt(currentGetter!);
                    }

                    if (itemType.IsValueType)
                    {
                        il.Box(itemType);
                    }
                })
                .Callvirt(typeof(IDescriptorSerializerLiteContext).GetMethod(nameof(IDescriptorSerializerLiteContext
                    .StoreNewDescriptors))!)
                .Br(next)
                .Mark(finish)
                .Finally()
                .Do(il =>
                {
                    if (isConcreteImplementation)
                    {
                        il
                            .Ldloca(localEnumerator)
                            .Constrained(typeAsIEnumerator);
                    }
                    else
                    {
                        il.Ldloc(localEnumerator);
                    }
                })
                .Callvirt(() => default(IDisposable)!.Dispose())
                .EndTry();
        }
    }

    public ITypeDescriptor? NestedType(int index)
    {
        return index == 0 ? _itemDescriptor : null;
    }

    public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        InitFromItemDescriptor(map(_itemDescriptor!));
    }

    public bool Sealed { get; private set; }
    public bool StoredInline => true;
    public bool LoadNeedsHelpWithConversion => false;

    public void ClearMappingToType()
    {
        _type = null;
        _itemType = null;
    }

    public bool ContainsField(string name)
    {
        return false;
    }

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields =>
        Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

    public void Persist(ref MemWriter writer, DescriptorWriter nestedDescriptorWriter)
    {
        nestedDescriptorWriter(ref writer, _itemDescriptor!);
    }

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx,
        Action<IILGen> pushValue, Type valueType)
    {
        var notnull = ilGenerator.DefineLabel();
        var completeFinish = ilGenerator.DefineLabel();
        var notList = ilGenerator.DefineLabel();
        var notHashSet = ilGenerator.DefineLabel();
        var itemType = GetItemType(valueType);
        var typeAsICollection = typeof(ICollection<>).MakeGenericType(itemType);
        var localCollection = ilGenerator.DeclareLocal(typeAsICollection);
        ilGenerator
            .Do(pushValue)
            .Castclass(typeAsICollection)
            .Stloc(localCollection)
            .Ldloc(localCollection)
            .Brtrue(notnull)
            .Do(pushWriter)
            .Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteByteZero))!)
            .Br(completeFinish)
            .Mark(notnull)
            .Do(pushWriter)
            .Ldloc(localCollection)
            .Callvirt(typeAsICollection.GetProperty(nameof(ICollection.Count))!.GetGetMethod()!)
            .LdcI4(1)
            .Add()
            .Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt32))!);
        {
            var typeAsList = typeof(List<>).MakeGenericType(itemType);
            var getEnumeratorMethod = typeAsList.GetMethods()
                .Single(m =>
                    m.Name == nameof(IEnumerable.GetEnumerator) && m.ReturnType.IsValueType &&
                    m.GetParameters().Length == 0);
            var typeAsIEnumerator = getEnumeratorMethod.ReturnType;
            var currentGetter = typeAsIEnumerator.GetProperty(nameof(IEnumerator.Current))!.GetGetMethod();
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Ldloc(localCollection)
                .Isinst(typeAsList)
                .Brfalse(notList)
                .Ldloc(localCollection)
                .Castclass(typeAsList)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Ldloca(localEnumerator)
                .Call(typeAsIEnumerator.GetMethod(nameof(IEnumerator.MoveNext))!)
                .Brfalse(finish);
            _itemDescriptor!.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloca(localEnumerator).Callvirt(currentGetter!), itemType);
            ilGenerator
                .Br(next)
                .Mark(finish)
                .Finally()
                .Ldloca(localEnumerator)
                .Constrained(typeAsIEnumerator)
                .Callvirt(() => default(IDisposable)!.Dispose())
                .EndTry()
                .Br(completeFinish);
        }
        {
            var typeAsHashSet = typeof(HashSet<>).MakeGenericType(itemType);
            var getEnumeratorMethod = typeAsHashSet.GetMethods()
                .Single(m =>
                    m.Name == nameof(IEnumerable.GetEnumerator) && m.ReturnType.IsValueType &&
                    m.GetParameters().Length == 0);
            var typeAsIEnumerator = getEnumeratorMethod.ReturnType;
            var currentGetter = typeAsIEnumerator.GetProperty(nameof(IEnumerator.Current))!.GetGetMethod();
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Mark(notList)
                .Ldloc(localCollection)
                .Isinst(typeAsHashSet)
                .Brfalse(notHashSet)
                .Ldloc(localCollection)
                .Castclass(typeAsHashSet)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Ldloca(localEnumerator)
                .Call(typeAsIEnumerator.GetMethod(nameof(IEnumerator.MoveNext))!)
                .Brfalse(finish);
            _itemDescriptor!.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloca(localEnumerator).Callvirt(currentGetter!), itemType);
            ilGenerator
                .Br(next)
                .Mark(finish)
                .Finally()
                .Ldloca(localEnumerator)
                .Constrained(typeAsIEnumerator)
                .Callvirt(() => default(IDisposable).Dispose())
                .EndTry()
                .Br(completeFinish);
        }
        {
            var getEnumeratorMethod =
                typeAsICollection.GetInterface("IEnumerable`1")!.GetMethod(nameof(IEnumerable.GetEnumerator));
            var typeAsIEnumerator = getEnumeratorMethod!.ReturnType;
            var currentGetter = typeAsIEnumerator.GetProperty(nameof(IEnumerator.Current))!.GetGetMethod();
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Mark(notHashSet)
                .Ldloc(localCollection)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Ldloc(localEnumerator)
                .Callvirt(() => default(IEnumerator).MoveNext())
                .Brfalse(finish);
            _itemDescriptor!.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloc(localEnumerator)
                    .Callvirt(currentGetter!), itemType);
            ilGenerator
                .Br(next)
                .Mark(finish)
                .Finally()
                .Ldloc(localEnumerator)
                .Callvirt(() => default(IDisposable).Dispose())
                .EndTry()
                .Mark(completeFinish);
        }
    }

    static Type GetItemType(Type valueType)
    {
        if (valueType.IsArray)
        {
            return valueType.GetElementType()!;
        }

        return valueType.GetGenericArguments()[0];
    }

    public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var localCount = ilGenerator.DeclareLocal(typeof(int));
        var skipFinished = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        ilGenerator
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
            .ConvI4()
            .Dup()
            .Stloc(localCount)
            .Brfalse(skipFinished)
            .Ldloc(localCount)
            .LdcI4(1)
            .Sub()
            .Stloc(localCount)
            .Mark(next)
            .Ldloc(localCount)
            .Brfalse(skipFinished)
            .Ldloc(localCount)
            .LdcI4(1)
            .Sub()
            .Stloc(localCount);
        _itemDescriptor!.GenerateSkipEx(ilGenerator, pushReader, pushCtx);
        ilGenerator
            .Br(next)
            .Mark(skipFinished);
    }

    public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers,
        Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        var itemDesc = map(_itemDescriptor);
        if (_typeSerializers == typeSerializers && itemDesc == _itemDescriptor)
            return this;
        return new ListTypeDescriptor(typeSerializers, itemDesc);
    }
}
