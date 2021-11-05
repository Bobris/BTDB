using System;
using System.Collections.Generic;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public class ODBDictionaryFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers, IFieldHandlerWithInit
{
    readonly IObjectDB _odb;
    readonly ITypeConvertorGenerator _typeConvertGenerator;
    readonly byte[] _configuration;
    readonly IFieldHandler _keysHandler;
    readonly IFieldHandler _valuesHandler;
    int _configurationId;
    Type? _type;

    public ODBDictionaryFieldHandler(IObjectDB odb, Type type, IFieldHandlerFactory fieldHandlerFactory)
    {
        _odb = odb;
        _typeConvertGenerator = odb.TypeConvertorGenerator;
        _type = type;
        _keysHandler = fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0],
            FieldHandlerOptions.Orderable | FieldHandlerOptions.AtEndOfStream);
        _valuesHandler =
            fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[1], FieldHandlerOptions.None);
        var writer = new SpanWriter();
        writer.WriteFieldHandler(_keysHandler);
        writer.WriteFieldHandler(_valuesHandler);
        _configuration = writer.GetSpan().ToArray();
        CreateConfiguration();
    }

    public ODBDictionaryFieldHandler(IObjectDB odb, byte[] configuration)
    {
        _odb = odb;
        var fieldHandlerFactory = odb.FieldHandlerFactory;
        _typeConvertGenerator = odb.TypeConvertorGenerator;
        _configuration = configuration;
        var reader = new SpanReader(configuration);
        _keysHandler = fieldHandlerFactory.CreateFromReader(ref reader,
            FieldHandlerOptions.Orderable | FieldHandlerOptions.AtEndOfStream);
        _valuesHandler = fieldHandlerFactory.CreateFromReader(ref reader, FieldHandlerOptions.None);
        CreateConfiguration();
    }

    ODBDictionaryFieldHandler(IObjectDB odb, byte[] configuration, IFieldHandler specializedKeyHandler,
        IFieldHandler specializedValueHandler)
    {
        _odb = odb;
        _typeConvertGenerator = odb.TypeConvertorGenerator;
        _configuration = configuration;
        _keysHandler = specializedKeyHandler;
        _valuesHandler = specializedValueHandler;
        CreateConfiguration();
    }

    void CreateConfiguration()
    {
        HandledType();
        _configurationId = GetConfigurationId(_type!);
    }

    int GetConfigurationId(Type type)
    {
        var keyAndValueTypes = type.GetGenericArguments();
        var configurationId = ODBDictionaryConfiguration.Register(_keysHandler, keyAndValueTypes[0], _valuesHandler, keyAndValueTypes[1]);
        var cfg = ODBDictionaryConfiguration.Get(configurationId);
        lock (cfg)
        {
            cfg.KeyReader ??= CreateReader(_keysHandler, keyAndValueTypes[0]);
            cfg.KeyWriter ??= CreateWriter(_keysHandler, keyAndValueTypes[0]);
            cfg.ValueReader ??= CreateReader(_valuesHandler, keyAndValueTypes[1]);
            cfg.ValueWriter ??= CreateWriter(_valuesHandler, keyAndValueTypes[1]);
        }

        return configurationId;
    }

    object CreateWriter(IFieldHandler fieldHandler, Type realType)
    {
        //Action<T, ref SpanWriter, IWriterCtx>
        var delegateType = typeof(WriterFun<>).MakeGenericType(realType);
        var dm = ILBuilder.Instance.NewMethod(fieldHandler.Name + "Writer", delegateType);
        var ilGenerator = dm.Generator;
        fieldHandler.Save(ilGenerator, il => il.Ldarg(1), il => il.Ldarg(2),
            il => il.Ldarg(0).Do(_typeConvertGenerator.GenerateConversion(realType, fieldHandler.HandledType())!));
        ilGenerator.Ret();
        return dm.Create();
    }

    object CreateReader(IFieldHandler fieldHandler, Type realType)
    {
        //Func<ref SpanReader, IReaderCtx, T>
        var delegateType = typeof(ReaderFun<>).MakeGenericType(realType);
        var dm = ILBuilder.Instance.NewMethod(fieldHandler.Name + "Reader", delegateType);
        var ilGenerator = dm.Generator;
        fieldHandler.Load(ilGenerator, il => il.Ldarg(0), il => il.Ldarg(1));
        ilGenerator
            .Do(_typeConvertGenerator.GenerateConversion(fieldHandler.HandledType(), realType)!)
            .Ret();
        return dm.Create();
    }

    public static string HandlerName => "ODBDictionary";

    public string Name => HandlerName;

    public byte[] Configuration => _configuration;

    public static bool IsCompatibleWithStatic(Type type, FieldHandlerOptions options)
    {
        if ((options & FieldHandlerOptions.Orderable) != 0) return false;
        return type.IsGenericType && IsCompatibleWithCore(type);
    }

    static bool IsCompatibleWithCore(Type type)
    {
        var genericTypeDefinition = type.GetGenericTypeDefinition();
        return genericTypeDefinition == typeof(IDictionary<,>) ||
               genericTypeDefinition == typeof(IOrderedDictionary<,>);
    }

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return IsCompatibleWithStatic(type, options);
    }

    public Type HandledType()
    {
        return _type ?? GenerateType(null);
    }

    public bool NeedsCtx()
    {
        return true;
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var genericArguments = _type!.GetGenericArguments();
        var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
        var constructorInfo = instanceType.GetConstructor(
            new[] { typeof(IInternalObjectDBTransaction), typeof(ODBDictionaryConfiguration), typeof(ulong) });
        ilGenerator
            .Do(pushCtx)
            .Castclass(typeof(IDBReaderCtx))
            .Callvirt(() => default(IDBReaderCtx).GetTransaction())
            .LdcI4(_configurationId)
            .Call(() => ODBDictionaryConfiguration.Get(0))
            .Do(pushReader)
            .Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt64))!)
            .Newobj(constructorInfo!)
            .Castclass(_type);
    }

    public bool NeedInit()
    {
        return true;
    }

    public void Init(IILGen ilGenerator, Action<IILGen> pushReaderCtx)
    {
        var genericArguments = _type!.GetGenericArguments();
        var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
        var constructorInfo = instanceType.GetConstructor(
            new[] { typeof(IInternalObjectDBTransaction), typeof(ODBDictionaryConfiguration) });
        ilGenerator
            .Do(pushReaderCtx)
            .Castclass(typeof(IDBReaderCtx))
            .Callvirt(() => default(IDBReaderCtx).GetTransaction())
            .LdcI4(_configurationId)
            .Call(() => ODBDictionaryConfiguration.Get(0))
            .Newobj(constructorInfo!)
            .Castclass(_type);
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        ilGenerator
            .Do(pushReader)
            .Call(typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVUInt64))!);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
    {
        var genericArguments = _type!.GetGenericArguments();
        var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
        ilGenerator
            .Do(pushWriter)
            .Do(pushCtx)
            .Do(pushValue)
            .LdcI4(_configurationId)
            .Call(instanceType.GetMethod(nameof(ODBDictionary<int, int>.DoSave))!);
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (_type != type)
            GenerateType(type);
        if (_type == type) return this;
        if (!IsCompatibleWithCore(type)) return this;
        var arguments = type.GetGenericArguments();
        var wantedKeyHandler = default(IFieldHandler);
        var wantedValueHandler = default(IFieldHandler);
        if (typeHandler is ODBDictionaryFieldHandler dictTypeHandler)
        {
            wantedKeyHandler = dictTypeHandler._keysHandler;
            wantedValueHandler = dictTypeHandler._valuesHandler;
        }

        var specializedKeyHandler = _keysHandler.SpecializeLoadForType(arguments[0], wantedKeyHandler, logger);
        var specializedValueHandler = _valuesHandler.SpecializeLoadForType(arguments[1], wantedValueHandler, logger);
        if (wantedKeyHandler == specializedKeyHandler &&
            (wantedValueHandler == specializedValueHandler ||
             wantedValueHandler.HandledType() == specializedValueHandler.HandledType()))
        {
            return typeHandler;
        }

        var res = new ODBDictionaryFieldHandler(_odb, _configuration, specializedKeyHandler,
            specializedValueHandler);
        res.GenerateType(type);
        return res;
    }

    Type GenerateType(Type? compatibleWith)
    {
        if (compatibleWith != null && compatibleWith.GetGenericTypeDefinition() == typeof(IOrderedDictionary<,>))
        {
            return _type =
                typeof(IOrderedDictionary<,>).MakeGenericType(_keysHandler.HandledType(),
                    _valuesHandler.HandledType());
        }

        return _type =
            typeof(IDictionary<,>).MakeGenericType(_keysHandler.HandledType(), _valuesHandler.HandledType());
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        if (_type != type)
            GenerateType(type);
        return this;
    }

    public IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers()
    {
        yield return _keysHandler;
        yield return _valuesHandler;
    }

    public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var fakeMethod = ILBuilder.Instance.NewMethod<Action>("Relation_fake");
        var fakeGenerator = fakeMethod.Generator;
        if (_keysHandler.FreeContent(fakeGenerator, _ => { }, _ => { }) == NeedsFreeContent.Yes)
            throw new BTDBException("Not supported 'free content' in IDictionary key");
        if (_valuesHandler.FreeContent(fakeGenerator, _ => { }, _ => { }) == NeedsFreeContent.No)
        {
            ilGenerator
                .Do(pushCtx)
                .Castclass(typeof(IDBReaderCtx))
                .Do(pushReader)
                .Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt64))!)
                .Callvirt(() => default(IDBReaderCtx).RegisterDict(0ul));
        }
        else
        {
            var genericArguments = _type!.GetGenericArguments();
            var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);

            var dictId = ilGenerator.DeclareLocal(typeof(ulong));
            ilGenerator
                .Do(pushCtx)
                .Castclass(typeof(IDBReaderCtx))
                .Do(pushReader)
                .Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt64))!)
                .Stloc(dictId)
                .Ldloc(dictId)
                .Callvirt(() => default(IDBReaderCtx).RegisterDict(0ul))
                .Do(pushCtx)
                .Ldloc(dictId)
                .LdcI4(GetConfigurationId(_type))
                //ODBDictionary.DoFreeContent(IReaderCtx ctx, ulong id, int cfgId)
                .Call(instanceType.GetMethod(nameof(ODBDictionary<int, int>.DoFreeContent))!);
        }

        return NeedsFreeContent.Yes;
    }
}
