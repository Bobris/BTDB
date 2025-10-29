using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using BTDB.KVDBLayer;
using BTDB.Serialization;

namespace BTDB.EventStoreLayer;

class DictionaryTypeDescriptor : ITypeDescriptor, IPersistTypeDescriptor
{
    readonly ITypeDescriptorCallbacks _typeSerializers;
    Type? _type;
    Type? _keyType;
    Type? _valueType;
    ITypeDescriptor? _keyDescriptor;
    ITypeDescriptor? _valueDescriptor;
    string? _name;
    readonly ITypeConvertorGenerator _convertorGenerator;

    public DictionaryTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, Type type)
    {
        _convertorGenerator = typeSerializers.ConvertorGenerator;
        _typeSerializers = typeSerializers;
        _type = type;
        var genericArguments = type.GetGenericArguments();
        _keyType = genericArguments[0];
        _valueType = genericArguments[1];
    }

    public DictionaryTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, ref MemReader reader,
        DescriptorReader nestedDescriptorReader)
        : this(typeSerializers, nestedDescriptorReader(ref reader), nestedDescriptorReader(ref reader))
    {
    }

    DictionaryTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, ITypeDescriptor keyDesc,
        ITypeDescriptor valueDesc)
    {
        _convertorGenerator = typeSerializers.ConvertorGenerator;
        _typeSerializers = typeSerializers;
        InitFromKeyValueDescriptors(keyDesc, valueDesc);
    }

    void InitFromKeyValueDescriptors(ITypeDescriptor keyDescriptor, ITypeDescriptor valueDescriptor)
    {
        if (_keyDescriptor == keyDescriptor && _valueDescriptor == valueDescriptor && _name != null) return;
        _keyDescriptor = keyDescriptor;
        _valueDescriptor = valueDescriptor;
        if ((_keyDescriptor.Name?.Length ?? 0) == 0 || (_valueDescriptor.Name?.Length ?? 0) == 0) return;
        Sealed = _keyDescriptor.Sealed && _valueDescriptor.Sealed;
        Name = $"Dictionary<{_keyDescriptor.Name}, {_valueDescriptor.Name}>";
    }

    public bool Equals(ITypeDescriptor other)
    {
        return Equals(other, null);
    }

    public string Name
    {
        get
        {
            if (_name == null) InitFromKeyValueDescriptors(_keyDescriptor!, _valueDescriptor!);
            return _name!;
        }
        private set => _name = value;
    }

    public bool FinishBuildFromType(ITypeDescriptorFactory factory)
    {
        var keyDescriptor = factory.Create(_keyType!);
        if (keyDescriptor == null) return false;
        var valueDescriptor = factory.Create(_valueType!);
        if (valueDescriptor == null) return false;
        InitFromKeyValueDescriptors(keyDescriptor, valueDescriptor);
        return true;
    }

    public void BuildHumanReadableFullName(StringBuilder text, HashSet<ITypeDescriptor> stack, uint indent)
    {
        text.Append("Dictionary<");
        _keyDescriptor!.BuildHumanReadableFullName(text, stack, indent);
        text.Append(", ");
        _valueDescriptor!.BuildHumanReadableFullName(text, stack, indent);
        text.Append(">");
    }

    public bool Equals(ITypeDescriptor other, Dictionary<ITypeDescriptor, ITypeDescriptor>? equalities)
    {
        if (other is not DictionaryTypeDescriptor o) return false;
        return _keyDescriptor!.Equals(o._keyDescriptor!, equalities) &&
               _valueDescriptor!.Equals(o._valueDescriptor!, equalities);
    }

    public Type GetPreferredType()
    {
        if (_type == null)
        {
            _keyType = _typeSerializers.LoadAsType(_keyDescriptor!);
            _valueType = _typeSerializers.LoadAsType(_valueDescriptor!);
            _type = typeof(IDictionary<,>).MakeGenericType(_keyType, _valueType);
        }

        return _type;
    }

    static Type GetInterface(Type type) =>
        type.GetInterface("IOrderedDictionary`2") ?? type.GetInterface("IDictionary`2") ?? type;

    public Type GetPreferredType(Type targetType)
    {
        if (_type == targetType) return _type;
        var targetIDictionary = GetInterface(targetType);
        var targetTypeArguments = targetIDictionary.GetGenericArguments();
        var keyType = _typeSerializers.LoadAsType(_keyDescriptor!, targetTypeArguments[0]);
        var valueType = _typeSerializers.LoadAsType(_valueDescriptor!, targetTypeArguments[1]);
        return targetType.GetGenericTypeDefinition().MakeGenericType(keyType, valueType);
    }

    public bool AnyOpNeedsCtx()
    {
        return !_keyDescriptor!.StoredInline || !_valueDescriptor!.StoredInline
                                             || _keyDescriptor.AnyOpNeedsCtx()
                                             || _valueDescriptor.AnyOpNeedsCtx();
    }

    ref struct DictionaryKeyValueLoaderCtx
    {
        internal nint KeyStoragePtr;
        internal nint ValueStoragePtr;
        internal object Object;
        internal Layer2Loader KeyLoader;
        internal Layer2Loader ValueLoader;
        internal unsafe delegate*<object, ref byte, ref byte, void> Adder;
        internal ITypeBinaryDeserializerContext? Ctx;
        internal ref MemReader Reader;
        internal unsafe delegate*<ref byte, ref IntPtr, delegate*<ref byte, void>, void> ValueStackAllocator;
    }

    public unsafe Layer2Loader GenerateLoad(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        var collectionMetadata = ReflectionMetadata.FindCollectionByType(targetType);
        if (collectionMetadata == null)
            throw new BTDBException("Cannot find collection metadata for " + _type.ToSimpleName());
        var keyLoad = _keyDescriptor!.GenerateLoadEx(collectionMetadata.ElementKeyType, typeConverterFactory);
        var keyStackAllocator = ReflectionMetadata.FindStackAllocatorByType(collectionMetadata.ElementKeyType);
        var valueLoad = _valueDescriptor!.GenerateLoadEx(collectionMetadata.ElementValueType!, typeConverterFactory);
        var valueStackAllocator =
            ReflectionMetadata.FindStackAllocatorByType(collectionMetadata.ElementValueType!);

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
            var loaderCtx = new DictionaryKeyValueLoaderCtx
            {
                Adder = collectionMetadata.AdderKeyValue,
                KeyLoader = keyLoad,
                ValueLoader = valueLoad,
                Object = obj,
                Ctx = ctx,
                Reader = ref reader,
                ValueStackAllocator = valueStackAllocator,
            };
            for (var i = 0; i != count; i++)
            {
                keyStackAllocator(ref Unsafe.As<DictionaryKeyValueLoaderCtx, byte>(ref loaderCtx),
                    ref loaderCtx.KeyStoragePtr,
                    &NestedValue);

                static void NestedValue(ref byte value)
                {
                    ref var context = ref Unsafe.As<byte, DictionaryKeyValueLoaderCtx>(ref value);
                    context.ValueStackAllocator(ref value, ref context.ValueStoragePtr, &Nested);
                }

                static void Nested(ref byte value)
                {
                    ref var context = ref Unsafe.As<byte, DictionaryKeyValueLoaderCtx>(ref value);
                    context.KeyLoader(ref context.Reader, context.Ctx,
                        ref Unsafe.AsRef<byte>((void*)context.KeyStoragePtr));
                    context.ValueLoader(ref context.Reader, context.Ctx,
                        ref Unsafe.AsRef<byte>((void*)context.ValueStoragePtr));
                    context.Adder(context.Object, ref Unsafe.AsRef<byte>((void*)context.KeyStoragePtr),
                        ref Unsafe.AsRef<byte>((void*)context.ValueStoragePtr));
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
            _keyDescriptor!.Skip(ref reader, ctx);
            _valueDescriptor!.Skip(ref reader, ctx);
        }
    }

    public Layer2Saver GenerateSave(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        var keyType = targetType.GenericTypeArguments[0];
        var valueType = targetType.GenericTypeArguments[1];
        var dictType = typeof(Dictionary<,>).MakeGenericType(targetType.GenericTypeArguments);
        var saveKey = _keyDescriptor!.GenerateSaveEx(keyType, typeConverterFactory);
        var saveValue = _valueDescriptor!.GenerateSaveEx(valueType, typeConverterFactory);
        var layout = RawData.GetDictionaryEntriesLayout(keyType, valueType);
        return (ref MemWriter writer, ITypeBinarySerializerContext? ctx, ref byte value) =>
        {
            var obj = Unsafe.As<byte, object>(ref value);
            if (obj == null)
            {
                writer.WriteByteZero();
                return;
            }

            var objType = obj.GetType();
            if (dictType.IsAssignableFrom(objType))
            {
                var count = Unsafe.As<byte, int>(ref RawData.Ref(obj,
                    RawData.Align(8 + 6 * (uint)Unsafe.SizeOf<nint>(), 8)));
                writer.WriteVUInt32((uint)count + 1);
                if (count == 0) return;
                obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(obj));
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + layout.OffsetNext)) < -1)
                    {
                        continue;
                    }

                    saveKey(ref writer, ctx, ref RawData.Ref(obj, offset + layout.OffsetKey));
                    saveValue(ref writer, ctx, ref RawData.Ref(obj, offset + layout.OffsetValue));
                }
            }
            else throw new BTDBException("Cannot save " + objType.ToSimpleName() + " as " + dictType.ToSimpleName());
        };
    }

    public Layer2NewDescriptor? GenerateNewDescriptor(Type targetType, ITypeConverterFactory typeConverterFactory)
    {
        if (_keyDescriptor!.Sealed && _valueDescriptor!.Sealed) return null;
        var keyType = targetType.GenericTypeArguments[0];
        var valueType = targetType.GenericTypeArguments[1];
        var dictType = typeof(Dictionary<,>).MakeGenericType(targetType.GenericTypeArguments);
        var saveKey = _keyDescriptor!.GenerateNewDescriptorEx(keyType, typeConverterFactory);
        var saveValue = _valueDescriptor!.GenerateNewDescriptorEx(valueType, typeConverterFactory);
        var layout = RawData.GetDictionaryEntriesLayout(keyType, valueType);
        return (IDescriptorSerializerLiteContext ctx, ref byte value) =>
        {
            var obj = Unsafe.As<byte, object>(ref value);
            if (obj == null)
            {
                return;
            }

            var objType = obj.GetType();
            if (dictType.IsAssignableFrom(objType))
            {
                var count = Unsafe.As<byte, int>(ref RawData.Ref(obj,
                    RawData.Align(8 + 6 * (uint)Unsafe.SizeOf<nint>(), 8)));
                if (count == 0) return;
                obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(obj));
                ref readonly var mt = ref RawData.MethodTableRef(obj);
                var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var offsetDelta = mt.ComponentSize;
                for (var i = 0; i < count; i++, offset += offsetDelta)
                {
                    if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + layout.OffsetNext)) < -1)
                    {
                        continue;
                    }

                    saveKey?.Invoke(ctx, ref RawData.Ref(obj, offset + layout.OffsetKey));
                    saveValue?.Invoke(ctx, ref RawData.Ref(obj, offset + layout.OffsetValue));
                }
            }
            else throw new BTDBException("Cannot save " + objType.ToSimpleName() + " as " + dictType.ToSimpleName());
        };
    }

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx,
        Action<IILGen> pushDescriptor, Type targetType)
    {
        if (targetType == typeof(object))
            targetType = GetPreferredType();
        var localCount = ilGenerator.DeclareLocal(typeof(int));
        var targetIDictionary = GetInterface(targetType);
        var targetTypeArguments = targetIDictionary.GetGenericArguments();
        var keyType = _typeSerializers.LoadAsType(_keyDescriptor!, targetTypeArguments[0]);
        var valueType = _typeSerializers.LoadAsType(_valueDescriptor!, targetTypeArguments[1]);
        var dictionaryTypeGenericDefinition = targetType.InheritsOrImplements(typeof(IOrderedDictionary<,>))
            ? typeof(OrderedDictionaryWithDescriptor<,>)
            : typeof(DictionaryWithDescriptor<,>);
        var dictionaryType = dictionaryTypeGenericDefinition.MakeGenericType(keyType, valueType);
        if (!targetType.IsAssignableFrom(dictionaryType)) throw new InvalidOperationException();
        var localDict = ilGenerator.DeclareLocal(dictionaryType);
        var loadFinished = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        ilGenerator
            .Ldnull()
            .Stloc(localDict)
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
            .ConvI4()
            .Dup()
            .LdcI4(1)
            .Sub()
            .Stloc(localCount)
            .Brfalse(loadFinished)
            .Ldloc(localCount)
            .Do(pushDescriptor)
            .Newobj(dictionaryType.GetConstructor(new[] { typeof(int), typeof(ITypeDescriptor) })!)
            .Stloc(localDict)
            .Mark(next)
            .Ldloc(localCount)
            .Brfalse(loadFinished)
            .Ldloc(localCount)
            .LdcI4(1)
            .Sub()
            .Stloc(localCount)
            .Ldloc(localDict);
        _keyDescriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx,
            il => il.Do(pushDescriptor).LdcI4(0).Callvirt(() => default(ITypeDescriptor)!.NestedType(0)), keyType,
            _convertorGenerator);
        _valueDescriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx,
            il => il.Do(pushDescriptor).LdcI4(1).Callvirt(() => default(ITypeDescriptor)!.NestedType(0)), valueType,
            _convertorGenerator);
        ilGenerator
            .Callvirt(dictionaryType.GetMethod(nameof(IDictionary.Add))!)
            .Br(next)
            .Mark(loadFinished)
            .Ldloc(localDict)
            .Castclass(targetType);
    }

    public ITypeNewDescriptorGenerator? BuildNewDescriptorGenerator()
    {
        if (_keyDescriptor!.Sealed && _valueDescriptor!.Sealed) return null;
        return new TypeNewDescriptorGenerator(this);
    }

    class TypeNewDescriptorGenerator : ITypeNewDescriptorGenerator
    {
        readonly DictionaryTypeDescriptor _owner;

        public TypeNewDescriptorGenerator(DictionaryTypeDescriptor owner)
        {
            _owner = owner;
        }

        public void GenerateTypeIterator(IILGen ilGenerator, Action<IILGen> pushObj, Action<IILGen> pushCtx, Type type)
        {
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();

            if (type == typeof(object))
                type = _owner.GetPreferredType();
            var targetIDictionary = GetInterface(type);
            var targetTypeArguments = targetIDictionary.GetGenericArguments();
            var keyType = _owner._typeSerializers.LoadAsType(_owner._keyDescriptor!, targetTypeArguments[0]);
            var valueType = _owner._typeSerializers.LoadAsType(_owner._valueDescriptor!, targetTypeArguments[1]);
            if (_owner._type == null) _owner._type = type;
            var isDict = type.GetGenericTypeDefinition() == typeof(Dictionary<,>);
            var typeAsIDictionary = isDict ? type : typeof(IDictionary<,>).MakeGenericType(keyType, valueType);
            var getEnumeratorMethod = isDict
                ? typeAsIDictionary.GetMethods()
                    .Single(m => m.Name == nameof(IEnumerable.GetEnumerator) && m.ReturnType.IsValueType &&
                                 m.GetParameters().Length == 0)
                : typeAsIDictionary.GetInterface("IEnumerable`1")!.GetMethod(nameof(IEnumerable.GetEnumerator));
            var typeAsIEnumerator = getEnumeratorMethod!.ReturnType;
            var currentGetter = typeAsIEnumerator.GetProperty(nameof(IEnumerator.Current))!.GetGetMethod();
            var typeKeyValuePair = currentGetter!.ReturnType;
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            var localPair = ilGenerator.DeclareLocal(typeKeyValuePair);
            ilGenerator
                .Do(pushObj)
                .Castclass(typeAsIDictionary)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Do(il =>
                {
                    if (isDict)
                    {
                        il
                            .Ldloca(localEnumerator)
                            .Call(typeAsIEnumerator.GetMethod(nameof(IEnumerator.MoveNext))!);
                    }
                    else
                    {
                        il
                            .Ldloc(localEnumerator)
                            .Callvirt(() => default(IEnumerator).MoveNext());
                    }
                })
                .Brfalse(finish)
                .Do(il =>
                {
                    if (isDict)
                    {
                        il
                            .Ldloca(localEnumerator)
                            .Call(currentGetter);
                    }
                    else
                    {
                        il
                            .Ldloc(localEnumerator)
                            .Callvirt(currentGetter);
                    }
                })
                .Stloc(localPair);
            if (!_owner._keyDescriptor.Sealed)
            {
                ilGenerator
                    .Do(pushCtx)
                    .Ldloca(localPair)
                    .Call(typeKeyValuePair.GetProperty("Key")!.GetGetMethod()!)
                    .Do(il =>
                    {
                        if (keyType.IsValueType)
                        {
                            il.Box(keyType);
                        }
                    })
                    .Callvirt(typeof(IDescriptorSerializerLiteContext).GetMethod(
                        nameof(IDescriptorSerializerLiteContext.StoreNewDescriptors))!);
            }

            if (!_owner._valueDescriptor.Sealed)
            {
                ilGenerator
                    .Do(pushCtx)
                    .Ldloca(localPair)
                    .Call(typeKeyValuePair.GetProperty("Value")!.GetGetMethod()!)
                    .Do(il =>
                    {
                        if (valueType.IsValueType)
                        {
                            il.Box(valueType);
                        }
                    })
                    .Callvirt(typeof(IDescriptorSerializerLiteContext).GetMethod(
                        nameof(IDescriptorSerializerLiteContext.StoreNewDescriptors))!);
            }

            ilGenerator
                .Br(next)
                .Mark(finish)
                .Finally()
                .Do(il =>
                {
                    if (isDict)
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
                .Callvirt(() => default(IDisposable).Dispose())
                .EndTry();
        }
    }

    public ITypeDescriptor? NestedType(int index)
    {
        return index switch
        {
            0 => _keyDescriptor,
            1 => _valueDescriptor,
            _ => null
        };
    }

    public void MapNestedTypes(Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        InitFromKeyValueDescriptors(map(_keyDescriptor), map(_valueDescriptor));
    }

    public bool Sealed { get; private set; }
    public bool StoredInline => true;
    public bool LoadNeedsHelpWithConversion => false;

    public void ClearMappingToType()
    {
        _type = null;
        _keyType = null;
        _valueType = null;
    }

    public bool ContainsField(string name)
    {
        return false;
    }

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields =>
        Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

    public void Persist(ref MemWriter writer, DescriptorWriter nestedDescriptorWriter)
    {
        nestedDescriptorWriter(ref writer, _keyDescriptor!);
        nestedDescriptorWriter(ref writer, _valueDescriptor!);
    }

    public void GenerateSave(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
        Action<IILGen> pushValue, Type saveType)
    {
        var notnull = ilGenerator.DefineLabel();
        var completeFinish = ilGenerator.DefineLabel();
        var notDictionary = ilGenerator.DefineLabel();
        var keyType = saveType.GetGenericArguments()[0];
        var valueType = saveType.GetGenericArguments()[1];
        var typeAsIDictionary = typeof(IDictionary<,>).MakeGenericType(keyType, valueType);
        var typeAsICollection = typeAsIDictionary.GetInterface("ICollection`1");
        var localDict = ilGenerator.DeclareLocal(typeAsIDictionary);
        ilGenerator
            .Do(pushValue)
            .Castclass(typeAsIDictionary)
            .Stloc(localDict)
            .Ldloc(localDict)
            .Brtrue(notnull)
            .Do(pushWriter)
            .Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteByteZero))!)
            .Br(completeFinish)
            .Mark(notnull)
            .Do(pushWriter)
            .Ldloc(localDict)
            .Callvirt(typeAsICollection!.GetProperty(nameof(ICollection.Count))!.GetGetMethod()!)
            .LdcI4(1)
            .Add()
            .Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt32))!);
        {
            var typeAsDictionary = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
            var getEnumeratorMethod = typeAsDictionary.GetMethods()
                .Single(m =>
                    m.Name == nameof(IEnumerable.GetEnumerator) && m.ReturnType.IsValueType &&
                    m.GetParameters().Length == 0);
            var typeAsIEnumerator = getEnumeratorMethod.ReturnType;
            var currentGetter = typeAsIEnumerator.GetProperty(nameof(IEnumerator.Current))!.GetGetMethod();
            var typeKeyValuePair = currentGetter!.ReturnType;
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            var localPair = ilGenerator.DeclareLocal(typeKeyValuePair);
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Ldloc(localDict)
                .Isinst(typeAsDictionary)
                .Brfalse(notDictionary)
                .Ldloc(localDict)
                .Castclass(typeAsDictionary)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Ldloca(localEnumerator)
                .Call(typeAsIEnumerator.GetMethod(nameof(IEnumerator.MoveNext))!)
                .Brfalse(finish)
                .Ldloca(localEnumerator)
                .Call(currentGetter)
                .Stloc(localPair);
            _keyDescriptor!.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloca(localPair).Call(typeKeyValuePair.GetProperty("Key")!.GetGetMethod()!), keyType);
            _valueDescriptor!.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloca(localPair).Call(typeKeyValuePair.GetProperty("Value")!.GetGetMethod()!), valueType);
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
                typeAsIDictionary.GetInterface("IEnumerable`1")!.GetMethod(nameof(IEnumerable.GetEnumerator));
            var typeAsIEnumerator = getEnumeratorMethod!.ReturnType;
            var currentGetter = typeAsIEnumerator.GetProperty(nameof(IEnumerator.Current))!.GetGetMethod();
            var typeKeyValuePair = currentGetter!.ReturnType;
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            var localPair = ilGenerator.DeclareLocal(typeKeyValuePair);
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Mark(notDictionary)
                .Ldloc(localDict)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Ldloc(localEnumerator)
                .Callvirt(() => default(IEnumerator).MoveNext())
                .Brfalse(finish)
                .Ldloc(localEnumerator)
                .Callvirt(currentGetter)
                .Stloc(localPair);
            _keyDescriptor.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloca(localPair).Call(typeKeyValuePair.GetProperty("Key")!.GetGetMethod()!), keyType);
            _valueDescriptor.GenerateSaveEx(ilGenerator, pushWriter, pushCtx,
                il => il.Ldloca(localPair).Call(typeKeyValuePair.GetProperty("Value")!.GetGetMethod()!), valueType);
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

    public void GenerateSkip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var localCount = ilGenerator.DeclareLocal(typeof(uint));
        var skipFinished = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        ilGenerator
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
            .Stloc(localCount)
            .Ldloc(localCount)
            .Brfalse(skipFinished)
            .Mark(next)
            .Ldloc(localCount)
            .LdcI4(1)
            .Sub()
            .Stloc(localCount)
            .Ldloc(localCount)
            .Brfalse(skipFinished);
        _keyDescriptor!.GenerateSkipEx(ilGenerator, pushReader, pushCtx);
        _valueDescriptor!.GenerateSkipEx(ilGenerator, pushReader, pushCtx);
        ilGenerator
            .Br(next)
            .Mark(skipFinished);
    }

    public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers,
        Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        var keyDesc = map(_keyDescriptor);
        var valueDesc = map(_valueDescriptor);
        if (_typeSerializers == typeSerializers && keyDesc == _keyDescriptor && valueDesc == _valueDescriptor)
            return this;
        return new DictionaryTypeDescriptor(typeSerializers, keyDesc, valueDesc);
    }
}
