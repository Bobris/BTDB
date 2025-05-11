using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.Buffer;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer;

public class ODBDictionaryFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers, IFieldHandlerWithInit
{
    readonly IObjectDB _odb;
    readonly ITypeConvertorGenerator _typeConvertGenerator;
    readonly byte[] _configuration;
    readonly IFieldHandler _keysHandler;
    readonly IFieldHandler _valuesHandler;
    int _configurationId = -1;
    Type? _type;

    [SkipLocalsInit]
    public ODBDictionaryFieldHandler(IObjectDB odb, Type type, IFieldHandlerFactory fieldHandlerFactory)
    {
        _odb = odb;
        _typeConvertGenerator = odb.TypeConvertorGenerator;
        _type = type;
        _keysHandler = fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0],
            FieldHandlerOptions.Orderable | FieldHandlerOptions.AtEndOfStream);
        _valuesHandler =
            fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[1], FieldHandlerOptions.None);
        Span<byte> buf = stackalloc byte[2048];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteFieldHandler(_keysHandler);
        writer.WriteFieldHandler(_valuesHandler);
        _configuration = writer.GetSpan().ToArray();
        HandledType();
    }

    public unsafe ODBDictionaryFieldHandler(IObjectDB odb, byte[] configuration)
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
            _valuesHandler = fieldHandlerFactory.CreateFromReader(ref reader, FieldHandlerOptions.None);
        }

        HandledType();
    }

    ODBDictionaryFieldHandler(IObjectDB odb, byte[] configuration, IFieldHandler specializedKeyHandler,
        IFieldHandler specializedValueHandler)
    {
        _odb = odb;
        _typeConvertGenerator = odb.TypeConvertorGenerator;
        _configuration = configuration;
        _keysHandler = specializedKeyHandler;
        _valuesHandler = specializedValueHandler;
        HandledType();
    }

    int GetOrCreateConfigurationId()
    {
        if (_configurationId == -1)
        {
            _configurationId = GetConfigurationId(_type!, null);
        }

        return _configurationId;
    }

    int GetConfigurationId(Type type, ITypeConverterFactory? typeConverterFactory)
    {
        var keyAndValueTypes = type.GetGenericArguments();
        var configurationId =
            ODBDictionaryConfiguration.Register(_keysHandler, keyAndValueTypes[0], _valuesHandler, keyAndValueTypes[1]);
        var cfg = ODBDictionaryConfiguration.Get(configurationId);
        lock (cfg)
        {
            cfg.KeyReader ??= CreateReader(_keysHandler, keyAndValueTypes[0], typeConverterFactory,
                _typeConvertGenerator);
            cfg.KeyWriter ??= CreateWriter(_keysHandler, keyAndValueTypes[0], typeConverterFactory,
                _typeConvertGenerator);
            cfg.ValueReader ??= CreateReader(_valuesHandler, keyAndValueTypes[1], typeConverterFactory,
                _typeConvertGenerator);
            cfg.ValueWriter ??= CreateWriter(_valuesHandler, keyAndValueTypes[1], typeConverterFactory,
                _typeConvertGenerator);
        }

        return configurationId;
    }

    internal static RefWriterFun CreateWriter(IFieldHandler fieldHandler, Type realType,
        ITypeConverterFactory? typeConverterFactory, ITypeConvertorGenerator typeConvertorGenerator)
    {
        var needsCtx = fieldHandler.NeedsCtx();
#pragma warning disable CS0162 // Unreachable code detected
        if (IFieldHandler.UseNoEmitForKeyValue)
        {
            var saver = fieldHandler.Save(realType, typeConverterFactory ?? new DefaultTypeConverterFactory());
            if (needsCtx)
            {
                return (ref MemWriter writer, IInternalObjectDBTransaction transaction, ref byte value) =>
                {
                    var ctx = new DBWriterCtx(transaction);
                    saver(ref writer, ctx, ref value);
                };
            }

            return (ref MemWriter writer, IInternalObjectDBTransaction _, ref byte value) =>
            {
                saver(ref writer, null, ref value);
            };
        }
        else
        {
            var dm = ILBuilder.Instance.NewMethod<RefWriterFun>(fieldHandler.Name + "Writer");
            var ilGenerator = dm.Generator;
            var miAs = typeof(Unsafe)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(m =>
                    m.Name == nameof(Unsafe.As)
                    && m.IsGenericMethodDefinition
                    && m.GetGenericArguments().Length == 2).MakeGenericMethod(typeof(byte), realType);
            var generateConversion = typeConvertorGenerator.GenerateConversion(realType, fieldHandler.HandledType()!)!;
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
#pragma warning restore CS0162 // Unreachable code detected
    }

    internal static RefReaderFun CreateReader(IFieldHandler fieldHandler, Type realType,
        ITypeConverterFactory? typeConverterFactory, ITypeConvertorGenerator typeConvertorGenerator)
    {
        var needsCtx = fieldHandler.NeedsCtx();
#pragma warning disable CS0162 // Unreachable code detected
        if (IFieldHandler.UseNoEmitForKeyValue)
        {
            var loader = fieldHandler.Load(realType, typeConverterFactory ?? new DefaultTypeConverterFactory());
            if (needsCtx)
            {
                return (ref MemReader reader, IInternalObjectDBTransaction transaction, ref byte value) =>
                {
                    var ctx = new DBReaderCtx(transaction);
                    loader(ref reader, ctx, ref value);
                };
            }

            return (ref MemReader reader, IInternalObjectDBTransaction _, ref byte value) =>
            {
                loader(ref reader, null, ref value);
            };
        }
        else
        {
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
                .Do(typeConvertorGenerator.GenerateConversion(fieldHandler.HandledType()!, realType)!)
                .Stloc(localValue)
                .Ldarg(2)
                .Call(miAs)
                .Ldloc(localValue)
                .Stind(realType)
                .Ret();
            return dm.Create();
        }
#pragma warning restore CS0162 // Unreachable code detected
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
            .LdcI4(GetOrCreateConfigurationId())
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
        var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
        var constructorInfo = instanceType.GetConstructor(
            new[] { typeof(IInternalObjectDBTransaction), typeof(ODBDictionaryConfiguration) });
        ilGenerator
            .Do(pushReaderCtx)
            .Castclass(typeof(IDBReaderCtx))
            .Callvirt(() => default(IDBReaderCtx).GetTransaction())
            .LdcI4(GetOrCreateConfigurationId())
            .Call(() => ODBDictionaryConfiguration.Get(0))
            .Newobj(constructorInfo!)
            .Castclass(_type);
    }

    public FieldHandlerInit Init()
    {
        var genericArguments = _type!.GetGenericArguments();
        var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
        var configuration = ODBDictionaryConfiguration.Get(GetOrCreateConfigurationId());
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
        var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
        ilGenerator
            .Do(pushWriter)
            .Do(pushCtx)
            .Do(pushValue)
            .LdcI4(GetOrCreateConfigurationId())
            .Call(instanceType.GetMethod(nameof(ODBDictionary<int, int>.DoSave))!);
    }

    public FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (IsCompatibleWithStatic(asType, FieldHandlerOptions.None))
        {
            var genericArguments = asType!.GetGenericArguments();
            var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
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

        if (asType.IsGenericType && asType.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var genericArguments = asType!.GetGenericArguments();
            return this.BuildConvertingLoader(typeof(IDictionary<,>).MakeGenericType(genericArguments), asType,
                typeConverterFactory);
        }

        return this.BuildConvertingLoader(
            typeof(IDictionary<,>).MakeGenericType(_keysHandler.HandledType()!, _valuesHandler.HandledType()!), asType,
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
                                    " is not compatible with ODBDictionaryFieldHandler.Save");
        var genericArguments = _type!.GetGenericArguments();
        var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
        var configurationId = GetConfigurationId(asType, typeConverterFactory);
        var configuration = ODBDictionaryConfiguration.Get(configurationId);
        return (ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
        {
            var writerCtx = (IDBWriterCtx)ctx!;
            var dictionary = Unsafe.As<byte, IDictionary>(ref value);
            if (dictionary is IInternalODBDictionary goodDict)
            {
                writer.WriteVUInt64(goodDict.DictId);
                return;
            }

            var dictId = writerCtx.GetTransaction().AllocateDictionaryId();
            // TODO: Create source generator for this
            goodDict = (IInternalODBDictionary)Activator.CreateInstance(instanceType, writerCtx.GetTransaction(),
                configuration, dictId)!;
            goodDict.Upsert(dictionary);
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

    [SkipLocalsInit]
    public unsafe void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        if (_valuesHandlerDoesNeedFreeContent == null) throw new BTDBException("FreeContent not initialized");

        var dictId = reader.ReadVUInt64();
        ctx!.RegisterDict(dictId);
        if (_valuesHandlerDoesNeedFreeContent == true)
        {
            var len = PackUnpack.LengthVUInt(dictId);
            Span<byte> prefix = stackalloc byte[ObjectDB.AllDictionariesPrefixLen + (int)len];
            MemoryMarshal.GetReference(prefix) = ObjectDB.AllDictionariesPrefixByte;
            PackUnpack.UnsafePackVUInt(
                ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(prefix),
                    ObjectDB.AllDictionariesPrefixLen), dictId, len);

            Span<byte> buffer = stackalloc byte[4096];
            using var cursor = ((DBReaderCtx)ctx).GetTransaction()!.KeyValueDBTransaction.CreateCursor();
            while (cursor.FindNextKey(prefix))
            {
                var valueSpan = cursor.GetValueSpan(ref buffer);
                fixed (void* _ = valueSpan)
                {
                    var valueReader = MemReader.CreateFromPinnedSpan(valueSpan);
                    _valuesHandler.FreeContent(ref valueReader, ctx);
                }
            }
        }
    }

    bool? _valuesHandlerDoesNeedFreeContent;

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes)
    {
        if (_keysHandler.DoesNeedFreeContent(visitedTypes))
            throw new BTDBException("Not supported 'free content' in IDictionary key");
        _valuesHandlerDoesNeedFreeContent = _valuesHandler.DoesNeedFreeContent(visitedTypes);
        return true;
    }
}
