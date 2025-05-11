using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public class ODBSetFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers, IFieldHandlerWithInit
{
    readonly IObjectDB _odb;
    readonly ITypeConvertorGenerator _typeConvertGenerator;
    readonly byte[] _configuration;
    readonly IFieldHandler _keysHandler;
    int _configurationId;
    Type? _type;

    [SkipLocalsInit]
    public ODBSetFieldHandler(IObjectDB odb, Type type, IFieldHandlerFactory fieldHandlerFactory)
    {
        _odb = odb;
        _typeConvertGenerator = odb.TypeConvertorGenerator;
        _type = type;
        _keysHandler = fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0],
            FieldHandlerOptions.Orderable | FieldHandlerOptions.AtEndOfStream);
        Span<byte> buf = stackalloc byte[1024];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteFieldHandler(_keysHandler);
        _configuration = writer.GetSpan().ToArray();
        CreateConfiguration();
    }

    public unsafe ODBSetFieldHandler(IObjectDB odb, byte[] configuration)
    {
        _odb = odb;
        var fieldHandlerFactory = odb.FieldHandlerFactory;
        _typeConvertGenerator = odb.TypeConvertorGenerator;
        _configuration = configuration;
        fixed (void* confPtr = configuration)
        {
            var reader = new MemReader(confPtr, configuration.Length);
            _keysHandler = fieldHandlerFactory.CreateFromReader(ref reader,
                FieldHandlerOptions.Orderable | FieldHandlerOptions.AtEndOfStream);
        }

        CreateConfiguration();
    }

    ODBSetFieldHandler(IObjectDB odb, byte[] configuration, IFieldHandler specializedKeyHandler)
    {
        _odb = odb;
        _typeConvertGenerator = odb.TypeConvertorGenerator;
        _configuration = configuration;
        _keysHandler = specializedKeyHandler;
        CreateConfiguration();
    }

    void CreateConfiguration()
    {
        HandledType();
        _configurationId = GetConfigurationId(_type!, null);
    }

    int GetConfigurationId(Type type, ITypeConverterFactory? typeConverterFactory)
    {
        var keyAndValueTypes = type.GetGenericArguments();
        var configurationId =
            ODBDictionaryConfiguration.Register(_keysHandler, keyAndValueTypes[0], null, null);
        var cfg = ODBDictionaryConfiguration.Get(configurationId);
        lock (cfg)
        {
            cfg.KeyReader ??= ODBDictionaryFieldHandler.CreateReader(_keysHandler, keyAndValueTypes[0],
                typeConverterFactory, _typeConvertGenerator);
            cfg.KeyWriter ??= ODBDictionaryFieldHandler.CreateWriter(_keysHandler, keyAndValueTypes[0],
                typeConverterFactory, _typeConvertGenerator);
        }

        return configurationId;
    }

    public static string HandlerName => "ODBSet";

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
        return genericTypeDefinition == typeof(IOrderedSet<>);
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
        var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
        var constructorInfo = instanceType.GetConstructor(
            new[] { typeof(IInternalObjectDBTransaction), typeof(ODBDictionaryConfiguration), typeof(ulong) });
        ilGenerator
            .Do(pushCtx)
            .Castclass(typeof(IDBReaderCtx))
            .Callvirt(() => default(IDBReaderCtx).GetTransaction())
            .LdcI4(_configurationId)
            .Call(() => ODBDictionaryConfiguration.Get(0))
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt64))!)
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
        var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
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

    public FieldHandlerInit Init()
    {
        var genericArguments = _type!.GetGenericArguments();
        var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
        var configuration = ODBDictionaryConfiguration.Get(_configurationId);
        return (IReaderCtx? ctx, ref byte value) =>
        {
            // TODO: Create source generator for this
            Unsafe.As<byte, object>(ref value) =
                Activator.CreateInstance(instanceType, ((IDBReaderCtx)ctx!).GetTransaction(), configuration);
        };
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        ilGenerator
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt64))!);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
    {
        var genericArguments = _type!.GetGenericArguments();
        var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
        ilGenerator
            .Do(pushWriter)
            .Do(pushCtx)
            .Do(pushValue)
            .LdcI4(_configurationId)
            .Call(instanceType.GetMethod(nameof(ODBSet<int>.DoSave))!);
    }

    public FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (IsCompatibleWithStatic(asType, FieldHandlerOptions.None))
        {
            var genericArguments = asType!.GetGenericArguments();
            var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
            var configurationId = GetConfigurationId(asType, typeConverterFactory);
            var configuration = ODBDictionaryConfiguration.Get(configurationId);
            return (ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
            {
                var dictId = reader.ReadVUInt64();
                // TODO: Create source generator for this
                Unsafe.As<byte, object>(ref value) =
                    Activator.CreateInstance(instanceType, ((IDBReaderCtx)ctx!).GetTransaction(), configuration,
                        dictId);
            };
        }

        if (asType.IsGenericType && asType.GetGenericTypeDefinition() == typeof(HashSet<>))
        {
            var genericArguments = asType!.GetGenericArguments();
            return this.BuildConvertingLoader(typeof(IOrderedSet<>).MakeGenericType(genericArguments), asType,
                typeConverterFactory);
        }

        return this.BuildConvertingLoader(
            typeof(IOrderedSet<>).MakeGenericType(_keysHandler.HandledType()!), asType,
            typeConverterFactory);
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        reader.SkipVUInt64();
    }

    public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (!IsCompatibleWithCore(asType))
            throw new BTDBException("Type " + asType.ToSimpleName() +
                                    " is not compatible with ODBSetFieldHandler.Save");
        var genericArguments = _type!.GetGenericArguments();
        var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
        var configurationId = GetConfigurationId(asType, typeConverterFactory);
        var configuration = ODBDictionaryConfiguration.Get(configurationId);
        return (ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
        {
            var writerCtx = (IDBWriterCtx)ctx!;
            var list = Unsafe.As<byte, IEnumerable>(ref value);
            if (list is IInternalODBSet goodDict)
            {
                writer.WriteVUInt64(goodDict.DictId);
                return;
            }

            var dictId = writerCtx.GetTransaction().AllocateDictionaryId();
            // TODO: Create source generator for this
            goodDict = (IInternalODBSet)Activator.CreateInstance(instanceType, writerCtx.GetTransaction(),
                configuration, dictId)!;
            goodDict.Upsert(list);
            writer.WriteVUInt64(dictId);
        };
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (_type != type)
            GenerateType(type);
        if (_type == type) return this;
        if (!IsCompatibleWithCore(type)) return this;
        var arguments = type.GetGenericArguments();
        var wantedKeyHandler = default(IFieldHandler);
        if (typeHandler is ODBSetFieldHandler dictTypeHandler)
        {
            wantedKeyHandler = dictTypeHandler._keysHandler;
        }

        var specializedKeyHandler = _keysHandler.SpecializeLoadForType(arguments[0], wantedKeyHandler, logger);
        if (wantedKeyHandler == specializedKeyHandler)
        {
            return typeHandler;
        }

        var res = new ODBSetFieldHandler(_odb, _configuration, specializedKeyHandler);
        res.GenerateType(type);
        return res;
    }

    Type GenerateType(Type? compatibleWith)
    {
        if (compatibleWith != null && compatibleWith.GetGenericTypeDefinition() == typeof(IOrderedSet<>))
        {
            return _type = typeof(IOrderedSet<>).MakeGenericType(_keysHandler.HandledType()!);
        }

        return _type = typeof(ISet<>).MakeGenericType(_keysHandler.HandledType()!);
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
    }

    public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        ctx!.RegisterDict(reader.ReadVUInt64());
    }

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes)
    {
        if (_keysHandler.DoesNeedFreeContent(visitedTypes))
            throw new BTDBException("Not supported 'free content' in IOrderedSet");
        return true;
    }
}
