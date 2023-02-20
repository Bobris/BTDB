using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

    public DictionaryTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, ref SpanReader reader, DescriptorReader nestedDescriptorReader)
        : this(typeSerializers, nestedDescriptorReader(ref reader), nestedDescriptorReader(ref reader))
    {
    }

    DictionaryTypeDescriptor(ITypeDescriptorCallbacks typeSerializers, ITypeDescriptor keyDesc, ITypeDescriptor valueDesc)
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
        return Equals(other, new HashSet<ITypeDescriptor>(ReferenceEqualityComparer<ITypeDescriptor>.Instance));
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

    public bool Equals(ITypeDescriptor other, HashSet<ITypeDescriptor> stack)
    {
        var o = other as DictionaryTypeDescriptor;
        if (o == null) return false;
        return _keyDescriptor!.Equals(o._keyDescriptor!, stack) && _valueDescriptor!.Equals(o._valueDescriptor!, stack);
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

    static Type GetInterface(Type type) => type.GetInterface("IOrderedDictionary`2") ?? type.GetInterface("IDictionary`2") ?? type;

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

    public void GenerateLoad(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx, Action<IILGen> pushDescriptor, Type targetType)
    {
        if (targetType == typeof(object))
            targetType = GetPreferredType();
        var localCount = ilGenerator.DeclareLocal(typeof(int));
        var targetIDictionary = GetInterface(targetType);
        var targetTypeArguments = targetIDictionary.GetGenericArguments();
        var keyType = _typeSerializers.LoadAsType(_keyDescriptor!, targetTypeArguments[0]);
        var valueType = _typeSerializers.LoadAsType(_valueDescriptor!, targetTypeArguments[1]);
        var dictionaryTypeGenericDefinition = targetType.InheritsOrImplements(typeof(IOrderedDictionary<,>)) ? typeof(OrderedDictionaryWithDescriptor<,>) : typeof(DictionaryWithDescriptor<,>);
        var dictionaryType = dictionaryTypeGenericDefinition.MakeGenericType(keyType, valueType);
        if (!targetType.IsAssignableFrom(dictionaryType)) throw new InvalidOperationException();
        var localDict = ilGenerator.DeclareLocal(dictionaryType);
        var loadFinished = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        ilGenerator
            .Ldnull()
            .Stloc(localDict)
            .Do(pushReader)
            .Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt32))!)
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
        _keyDescriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx, il => il.Do(pushDescriptor).LdcI4(0).Callvirt(() => default(ITypeDescriptor).NestedType(0)), keyType, _convertorGenerator);
        _valueDescriptor.GenerateLoadEx(ilGenerator, pushReader, pushCtx, il => il.Do(pushDescriptor).LdcI4(1).Callvirt(() => default(ITypeDescriptor).NestedType(0)), valueType, _convertorGenerator);
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
                    .Single(
                        m => m.Name == nameof(IEnumerable.GetEnumerator) && m.ReturnType.IsValueType && m.GetParameters().Length == 0)
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
                    .Callvirt(typeof(IDescriptorSerializerLiteContext).GetMethod(nameof(IDescriptorSerializerLiteContext.StoreNewDescriptors))!);
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
                    .Callvirt(typeof(IDescriptorSerializerLiteContext).GetMethod(nameof(IDescriptorSerializerLiteContext.StoreNewDescriptors))!);
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

    public IEnumerable<KeyValuePair<string, ITypeDescriptor>> Fields => Array.Empty<KeyValuePair<string, ITypeDescriptor>>();

    public void Persist(ref SpanWriter writer, DescriptorWriter nestedDescriptorWriter)
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
            .Call(typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteByteZero))!)
            .Br(completeFinish)
            .Mark(notnull)
            .Do(pushWriter)
            .Ldloc(localDict)
            .Callvirt(typeAsICollection!.GetProperty(nameof(ICollection.Count))!.GetGetMethod()!)
            .LdcI4(1)
            .Add()
            .Call(typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVUInt32))!);
        {
            var typeAsDictionary = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
            var getEnumeratorMethod = typeAsDictionary.GetMethods()
                    .Single(m => m.Name == nameof(IEnumerable.GetEnumerator) && m.ReturnType.IsValueType && m.GetParameters().Length == 0);
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
            var getEnumeratorMethod = typeAsIDictionary.GetInterface("IEnumerable`1")!.GetMethod(nameof(IEnumerable.GetEnumerator));
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
            _keyDescriptor.GenerateSaveEx(ilGenerator, pushWriter, pushCtx, il => il.Ldloca(localPair).Call(typeKeyValuePair.GetProperty("Key")!.GetGetMethod()!), keyType);
            _valueDescriptor.GenerateSaveEx(ilGenerator, pushWriter, pushCtx, il => il.Ldloca(localPair).Call(typeKeyValuePair.GetProperty("Value")!.GetGetMethod()!), valueType);
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
            .Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt32))!)
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

    public ITypeDescriptor CloneAndMapNestedTypes(ITypeDescriptorCallbacks typeSerializers, Func<ITypeDescriptor, ITypeDescriptor> map)
    {
        var keyDesc = map(_keyDescriptor);
        var valueDesc = map(_valueDescriptor);
        if (_typeSerializers == typeSerializers && keyDesc == _keyDescriptor && valueDesc == _valueDescriptor)
            return this;
        return new DictionaryTypeDescriptor(typeSerializers, keyDesc, valueDesc);
    }
}
