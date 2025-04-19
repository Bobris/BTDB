using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class DictionaryFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers
{
    readonly IFieldHandlerFactory _fieldHandlerFactory;
    readonly ITypeConvertorGenerator _typeConvertGenerator;
    readonly IFieldHandler _keysHandler;
    readonly IFieldHandler _valuesHandler;
    Type? _type;

    [SkipLocalsInit]
    public DictionaryFieldHandler(IFieldHandlerFactory fieldHandlerFactory,
        ITypeConvertorGenerator typeConvertGenerator, Type type)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        _type = type;
        _keysHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0], FieldHandlerOptions.None);
        _valuesHandler =
            _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[1], FieldHandlerOptions.None);
        Span<byte> buf = stackalloc byte[1024];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteFieldHandler(_keysHandler);
        writer.WriteFieldHandler(_valuesHandler);
        Configuration = writer.GetSpan().ToArray();
    }

    public unsafe DictionaryFieldHandler(IFieldHandlerFactory fieldHandlerFactory,
        ITypeConvertorGenerator typeConvertGenerator, byte[] configuration)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        Configuration = configuration;
        fixed (void* confPtr = configuration)
        {
            var reader = new MemReader(confPtr, configuration.Length);
            _keysHandler = _fieldHandlerFactory.CreateFromReader(ref reader, FieldHandlerOptions.None);
            _valuesHandler = _fieldHandlerFactory.CreateFromReader(ref reader, FieldHandlerOptions.None);
        }
    }

    DictionaryFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertGenerator,
        Type type, IFieldHandler keySpecialized, IFieldHandler valueSpecialized)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        _type = type;
        _keysHandler = keySpecialized;
        _valuesHandler = valueSpecialized;
        Configuration = Array.Empty<byte>();
    }

    public static string HandlerName => "Dictionary";

    public string Name => HandlerName;

    public byte[] Configuration { get; }

    public static bool IsCompatibleWith(Type type)
    {
        if (!type.IsGenericType) return false;
        return type.GetGenericTypeDefinition() == typeof(IDictionary<,>) ||
               type.GetGenericTypeDefinition() == typeof(Dictionary<,>);
    }

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return IsCompatibleWith(type);
    }

    public Type HandledType()
    {
        return _type ??=
            typeof(IDictionary<,>).MakeGenericType(_keysHandler.HandledType(), _valuesHandler.HandledType());
    }

    public bool NeedsCtx()
    {
        return true;
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var localCount = ilGenerator.DeclareLocal(typeof(uint));
        var localResultOfObject = ilGenerator.DeclareLocal(typeof(object));
        var localResult = ilGenerator.DeclareLocal(HandledType());
        var loadSkipped = ilGenerator.DefineLabel();
        var finish = ilGenerator.DefineLabel();
        var readFinish = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        var genericArguments = _type!.GetGenericArguments();
        ilGenerator
            .Do(pushCtx)
            .Do(pushReader)
            .Ldloca(localResultOfObject)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.ReadObject))!)
            .Brfalse(loadSkipped)
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
            .Stloc(localCount)
            .Ldloc(localCount)
            .Newobj(typeof(Dictionary<,>).MakeGenericType(genericArguments).GetConstructor(new[] { typeof(int) })!)
            .Stloc(localResult)
            .Do(pushCtx)
            .Ldloc(localResult)
            .Castclass(typeof(object))
            .Callvirt(() => default(IReaderCtx).RegisterObject(null))
            .Mark(next)
            .Ldloc(localCount)
            .Brfalse(readFinish)
            .Ldloc(localCount)
            .LdcI4(1)
            .Sub()
            .ConvU4()
            .Stloc(localCount)
            .Ldloc(localResult)
            .GenerateLoad(_keysHandler, genericArguments[0], pushReader, pushCtx, _typeConvertGenerator)
            .GenerateLoad(_valuesHandler, genericArguments[1], pushReader, pushCtx, _typeConvertGenerator)
            .Callvirt(_type.GetMethod("Add")!)
            .Br(next)
            .Mark(readFinish)
            .Do(pushCtx)
            .Do(pushReader)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.ReadObjectDone))!)
            .Br(finish)
            .Mark(loadSkipped)
            .Ldloc(localResultOfObject)
            .Isinst(_type)
            .Stloc(localResult)
            .Mark(finish)
            .Ldloc(localResult);
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var localCount = ilGenerator.DeclareLocal(typeof(uint));
        var finish = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        ilGenerator
            .Do(pushCtx)
            .Do(pushReader)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.SkipObject))!)
            .Brfalse(finish)
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
            .Stloc(localCount)
            .Mark(next)
            .Ldloc(localCount)
            .Brfalse(finish)
            .Ldloc(localCount)
            .LdcI4(1)
            .Sub()
            .ConvU4()
            .Stloc(localCount)
            .GenerateSkip(_keysHandler, pushReader, pushCtx)
            .GenerateSkip(_valuesHandler, pushReader, pushCtx)
            .Br(next)
            .Mark(finish);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
        Action<IILGen> pushValue)
    {
        var realFinish = ilGenerator.DefineLabel();
        var finish = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        var localValue = ilGenerator.DeclareLocal(_type!);
        var typeAsICollection = _type.GetInterface("ICollection`1");
        var typeAsIEnumerable = _type.GetInterface("IEnumerable`1");
        var getEnumeratorMethod = typeAsIEnumerable!.GetMethod("GetEnumerator");
        var typeAsIEnumerator = getEnumeratorMethod!.ReturnType;
        var typeKeyValuePair = typeAsICollection!.GetGenericArguments()[0];
        var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
        var localPair = ilGenerator.DeclareLocal(typeKeyValuePair);
        ilGenerator
            .Do(pushValue)
            .Stloc(localValue)
            .Do(pushCtx)
            .Do(pushWriter)
            .Ldloc(localValue)
            .Castclass(typeof(object))
            .Callvirt(typeof(IWriterCtx).GetMethod(nameof(IWriterCtx.WriteObject))!)
            .Brfalse(realFinish)
            .Do(pushWriter)
            .Ldloc(localValue)
            .Callvirt(typeAsICollection.GetProperty("Count")!.GetGetMethod()!)
            .ConvU4()
            .Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt32))!)
            .Ldloc(localValue)
            .Callvirt(getEnumeratorMethod)
            .Stloc(localEnumerator)
            .Try()
            .Mark(next)
            .Ldloc(localEnumerator)
            .Callvirt(() => default(IEnumerator).MoveNext())
            .Brfalse(finish)
            .Ldloc(localEnumerator)
            .Callvirt(typeAsIEnumerator.GetProperty("Current")!.GetGetMethod()!)
            .Stloc(localPair);
        var keyAndValueTypes = _type.GetGenericArguments();
        var keysHandler = _keysHandler.SpecializeSaveForType(keyAndValueTypes[0]);
        keysHandler.Save(ilGenerator, pushWriter, pushCtx, il => il
            .Ldloca(localPair)
            .Call(typeKeyValuePair.GetProperty("Key")!.GetGetMethod()!)
            .Do(_typeConvertGenerator.GenerateConversion(keyAndValueTypes[0], keysHandler.HandledType()!)!));
        var valuesHandler = _valuesHandler.SpecializeSaveForType(keyAndValueTypes[1]);
        valuesHandler.Save(ilGenerator, pushWriter, pushCtx, il => il
            .Ldloca(localPair)
            .Call(typeKeyValuePair.GetProperty("Value")!.GetGetMethod()!)
            .Do(_typeConvertGenerator.GenerateConversion(keyAndValueTypes[1], valuesHandler.HandledType()!)!));
        ilGenerator
            .Br(next)
            .Mark(finish)
            .Finally()
            .Ldloc(localEnumerator)
            .Callvirt(() => default(IDisposable).Dispose())
            .EndTry()
            .Mark(realFinish);
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (_type == type) return this;
        if (!IsCompatibleWith(type))
        {
            return this;
        }

        var wantedKeyType = type.GetGenericArguments()[0];
        var wantedValueType = type.GetGenericArguments()[1];
        var wantedKeyHandler = default(IFieldHandler);
        var wantedValueHandler = default(IFieldHandler);
        if (typeHandler is DictionaryFieldHandler dictTypeHandler)
        {
            wantedKeyHandler = dictTypeHandler._keysHandler;
            wantedValueHandler = dictTypeHandler._valuesHandler;
        }

        var keySpecialized = _keysHandler.SpecializeLoadForType(wantedKeyType, wantedKeyHandler, logger);
        if (_typeConvertGenerator.GenerateConversion(keySpecialized.HandledType(), wantedKeyType) == null)
        {
            return this;
        }

        var valueSpecialized = _valuesHandler.SpecializeLoadForType(wantedValueType, wantedValueHandler, logger);
        if (_typeConvertGenerator.GenerateConversion(valueSpecialized.HandledType(), wantedValueType) == null)
        {
            return this;
        }

        if (wantedKeyHandler == keySpecialized && wantedValueHandler == valueSpecialized)
        {
            return typeHandler;
        }

        return new DictionaryFieldHandler(_fieldHandlerFactory, _typeConvertGenerator, type, keySpecialized,
            valueSpecialized);
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        if (_type == type) return this;
        if (!IsCompatibleWith(type))
        {
            Debug.Fail("strange");
            return this;
        }

        var wantedKeyType = type.GetGenericArguments()[0];
        var wantedValueType = type.GetGenericArguments()[1];
        var keySpecialized = _keysHandler.SpecializeSaveForType(wantedKeyType);
        if (_typeConvertGenerator.GenerateConversion(wantedKeyType, keySpecialized.HandledType()) == null)
        {
            Debug.Fail("even more strange key");
            return this;
        }

        var valueSpecialized = _valuesHandler.SpecializeSaveForType(wantedValueType);
        if (_typeConvertGenerator.GenerateConversion(wantedValueType, valueSpecialized.HandledType()) == null)
        {
            Debug.Fail("even more strange value");
            return this;
        }

        return new DictionaryFieldHandler(_fieldHandlerFactory, _typeConvertGenerator, type, keySpecialized,
            valueSpecialized);
    }

    public IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers()
    {
        yield return _keysHandler;
        yield return _valuesHandler;
    }

    public void FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var localCount = ilGenerator.DeclareLocal(typeof(uint));
        var finish = ilGenerator.DefineLabel();
        var next = ilGenerator.DefineLabel();
        ilGenerator
            .Do(pushCtx)
            .Do(pushReader)
            .Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.SkipObject))!)
            .Brfalse(finish)
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt32))!)
            .Stloc(localCount)
            .Mark(next)
            .Ldloc(localCount)
            .Brfalse(finish)
            .Ldloc(localCount)
            .LdcI4(1)
            .Sub()
            .ConvU4()
            .Stloc(localCount)
            .GenerateFreeContent(_keysHandler, pushReader, pushCtx)
            .GenerateFreeContent(_valuesHandler, pushReader, pushCtx)
            .Br(next)
            .Mark(finish);
    }

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes)
    {
        return _keysHandler.DoesNeedFreeContent(visitedTypes) || _valuesHandler.DoesNeedFreeContent(visitedTypes);
    }
}
