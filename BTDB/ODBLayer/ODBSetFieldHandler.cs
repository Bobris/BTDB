using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
        _configurationId = GetConfigurationId(_type!);
    }

    int GetConfigurationId(Type type)
    {
        var keyAndValueTypes = type.GetGenericArguments();
        var configurationId =
            ODBDictionaryConfiguration.Register(_keysHandler, keyAndValueTypes[0], null, null);
        var cfg = ODBDictionaryConfiguration.Get(configurationId);
        lock (cfg)
        {
            cfg.KeyReader ??= CreateReader(_keysHandler, keyAndValueTypes[0]);
            cfg.KeyWriter ??= CreateWriter(_keysHandler, keyAndValueTypes[0]);
        }

        return configurationId;
    }

    RefWriterFun CreateWriter(IFieldHandler fieldHandler, Type realType)
    {
        var needsCtx = fieldHandler.NeedsCtx();
        var dm = ILBuilder.Instance.NewMethod<RefWriterFun>(fieldHandler.Name + "Writer");
        var ilGenerator = dm.Generator;
        var miAs = typeof(Unsafe)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m =>
                m.Name == nameof(Unsafe.As)
                && m.IsGenericMethodDefinition
                && m.GetGenericArguments().Length == 2).MakeGenericMethod(typeof(byte), realType);
        var generateConversion = _typeConvertGenerator.GenerateConversion(realType, fieldHandler.HandledType()!)!;
        if (needsCtx)
        {
            var localWriterCtx = ilGenerator.DeclareLocal(typeof(IWriterCtx));
            ilGenerator
                .Ldarg(1)
                .Newobj(() => new DBWriterCtx(null!))
                .Stloc(localWriterCtx);
            fieldHandler.Save(ilGenerator, il => il.Ldarg(0), il => il.Ldloc(localWriterCtx),
                il => il.Ldarg(2).Call(miAs).Ldind(realType).Do(generateConversion));
        }
        else
        {
            fieldHandler.Save(ilGenerator, il => il.Ldarg(0), il => il.Ldnull(),
                il => il.Ldarg(2).Call(miAs).Ldind(realType).Do(generateConversion));
        }

        ilGenerator.Ret();
        return dm.Create();
    }

    RefReaderFun CreateReader(IFieldHandler fieldHandler, Type realType)
    {
        var needsCtx = fieldHandler.NeedsCtx();
        var dm = ILBuilder.Instance.NewMethod<RefReaderFun>(fieldHandler.Name + "Reader");
        var ilGenerator = dm.Generator;
        var localValue = ilGenerator.DeclareLocal(realType);
        if (needsCtx)
        {
            var localReaderCtx = ilGenerator.DeclareLocal(typeof(IReaderCtx));
            ilGenerator
                .Ldarg(1)
                .Newobj(() => new DBReaderCtx(null!))
                .Stloc(localReaderCtx);
            fieldHandler.Load(ilGenerator, il => il.Ldarg(0), il => il.Ldloc(localReaderCtx));
        }
        else
        {
            fieldHandler.Load(ilGenerator, il => il.Ldarg(0), il => il.Ldnull());
        }

        var miAs = typeof(Unsafe)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m =>
                m.Name == nameof(Unsafe.As)
                && m.IsGenericMethodDefinition
                && m.GetGenericArguments().Length == 2).MakeGenericMethod(typeof(byte), realType);
        ilGenerator
            .Do(_typeConvertGenerator.GenerateConversion(fieldHandler.HandledType()!, realType)!)
            .Stloc(localValue)
            .Ldarg(2)
            .Call(miAs)
            .Ldloc(localValue)
            .Stind(realType)
            .Ret();
        return dm.Create();
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
        if (!IsCompatibleWithStatic(asType, FieldHandlerOptions.None))
            throw new BTDBException("Type " + asType.ToSimpleName() +
                                    " is not compatible with ODBSetFieldHandler");
        var genericArguments = asType!.GetGenericArguments();
        var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
        var configurationId = GetConfigurationId(asType);
        var configuration = ODBDictionaryConfiguration.Get(configurationId);
        return (ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
        {
            var dictId = reader.ReadVUInt64();
            // TODO: Create source generator for this
            Unsafe.As<byte, object>(ref value) =
                Activator.CreateInstance(instanceType, ((IDBReaderCtx)ctx!).GetTransaction(), configuration, dictId);
        };
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        reader.SkipVUInt64();
    }

    public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        throw new NotImplementedException();
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
