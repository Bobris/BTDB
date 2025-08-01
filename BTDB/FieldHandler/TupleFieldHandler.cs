using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using BTDB.Collections;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class TupleFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers
{
    readonly IFieldHandlerFactory _fieldHandlerFactory;
    readonly ITypeConvertorGenerator _typeConvertGenerator;
    readonly StructList<IFieldHandler> _fieldHandlers;
    Type? _type;
    readonly FieldHandlerOptions _options;

    [SkipLocalsInit]
    public TupleFieldHandler(IFieldHandlerFactory fieldHandlerFactory,
        ITypeConvertorGenerator typeConvertGenerator, Type type, FieldHandlerOptions options)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        _type = type;
        _options = options;
        var args = type.GetGenericArguments();
        for (var i = 0; i < args.Length; i++)
        {
            var t = args[i];
            var opts = _options;
            if (i != args.Length - 1)
            {
                opts &= ~FieldHandlerOptions.AtEndOfStream;
            }

            _fieldHandlers.Add(_fieldHandlerFactory.CreateFromType(t, opts));
        }

        Span<byte> buf = stackalloc byte[2048];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteVUInt32(_fieldHandlers.Count);
        foreach (var f in _fieldHandlers)
        {
            writer.WriteFieldHandler(f);
        }

        Configuration = writer.GetSpan().ToArray();
    }

    public unsafe TupleFieldHandler(IFieldHandlerFactory fieldHandlerFactory,
        ITypeConvertorGenerator typeConvertGenerator, byte[] configuration, FieldHandlerOptions options)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        _options = options;
        Configuration = configuration;
        fixed (void* confPtr = configuration)
        {
            var reader = new MemReader(confPtr, configuration.Length);
            var count = reader.ReadVUInt32();
            for (var i = 0; i < count; i++)
            {
                var opts = _options;
                if (i != count - 1)
                {
                    opts &= ~FieldHandlerOptions.AtEndOfStream;
                }

                _fieldHandlers.Add(_fieldHandlerFactory.CreateFromReader(ref reader, opts));
            }
        }
    }

    TupleFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertGenerator,
        Type type, in StructList<IFieldHandler> specialized, FieldHandlerOptions options)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        _type = type;
        _options = options;
        _fieldHandlers.AddRange(specialized);
        Configuration = Array.Empty<byte>();
    }

    public static string HandlerName => "Tuple";

    public string Name => HandlerName;

    public byte[] Configuration { get; }

    public static bool IsCompatibleWith(Type type)
    {
        if (!type.IsGenericType) return false;
        return type.InheritsOrImplements(typeof(ITuple));
    }

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return IsCompatibleWith(type);
    }

    public static readonly Type[] TupleTypes = new[]
    {
        typeof(Tuple<>), typeof(Tuple<,>), typeof(Tuple<,,>), typeof(Tuple<,,,>), typeof(Tuple<,,,,>),
        typeof(Tuple<,,,,,>), typeof(Tuple<,,,,,,>)
    };

    public static readonly Type[] ValueTupleTypes = new[]
    {
        typeof(ValueTuple<>), typeof(ValueTuple<,>), typeof(ValueTuple<,,>), typeof(ValueTuple<,,,>),
        typeof(ValueTuple<,,,,>), typeof(ValueTuple<,,,,,>), typeof(ValueTuple<,,,,,,>)
    };

    public static readonly string[] TupleFieldName = new[]
    {
        "Item1", "Item2", "Item3", "Item4", "Item5", "Item6", "Item7"
    };

    public Type HandledType()
    {
        return _type ??=
            TupleTypes[_fieldHandlers.Count].MakeGenericType(_fieldHandlers.Select(t => t.HandledType()).ToArray());
    }

    public bool NeedsCtx()
    {
        return _fieldHandlers.Any(h => h.NeedsCtx());
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var type = HandledType();
        var genericArguments = type.GetGenericArguments();
        var valueType = ValueTupleTypes[genericArguments.Length - 1].MakeGenericType(genericArguments);
        var localResult = ilGenerator.DeclareLocal(valueType);
        ilGenerator
            .Ldloca(localResult)
            .InitObj(valueType);
        for (var i = 0; i < genericArguments.Length; i++)
        {
            if (i >= _fieldHandlers.Count) break;
            ilGenerator
                .Ldloca(localResult)
                .GenerateLoad(_fieldHandlers[i], genericArguments[i], pushReader, pushCtx,
                    _typeConvertGenerator)
                .Stfld(valueType.GetField(TupleFieldName[i])!);
        }

        for (var i = genericArguments.Length; i < _fieldHandlers.Count; i++)
        {
            ilGenerator
                .GenerateSkip(_fieldHandlers[i], pushReader, pushCtx);
        }

        if (type.IsValueType)
        {
            ilGenerator.Ldloc(localResult);
        }
        else
        {
            for (var i = 0; i < genericArguments.Length; i++)
            {
                ilGenerator.Ldloca(localResult).Ldfld(valueType.GetField(TupleFieldName[i])!);
            }

            ilGenerator.Newobj(type.GetConstructor(genericArguments)!);
        }
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        foreach (var f in _fieldHandlers)
        {
            ilGenerator.GenerateSkip(f, pushReader, pushCtx);
        }
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
        Action<IILGen> pushValue)
    {
        var type = HandledType();
        var genericArguments = type.GetGenericArguments();
        var localValue = ilGenerator.DeclareLocal(_type!);
        ilGenerator
            .Do(pushValue)
            .Stloc(localValue);
        if (type.IsValueType)
        {
            for (var i = 0; i < genericArguments.Length; i++)
            {
                var fieldInfo = type.GetField(TupleFieldName[i]);
                var conversion = _typeConvertGenerator.GenerateConversion(genericArguments[i],
                    _fieldHandlers[i].HandledType()!);
                _fieldHandlers[i].Save(ilGenerator, pushWriter, pushCtx, il =>
                {
                    il
                        .Ldloc(localValue)
                        .Ldfld(fieldInfo!)
                        .Do(conversion!);
                });
            }
        }
        else
        {
            for (var i = 0; i < genericArguments.Length; i++)
            {
                var methodInfo = type.GetProperty(TupleFieldName[i])!.GetGetMethod();
                var conversion = _typeConvertGenerator.GenerateConversion(genericArguments[i],
                    _fieldHandlers[i].HandledType()!);
                _fieldHandlers[i].Save(ilGenerator, pushWriter, pushCtx, il =>
                {
                    il
                        .Ldloc(localValue)
                        .Callvirt(methodInfo!)
                        .Do(conversion!);
                });
            }
        }
    }

    public unsafe FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (IsCompatibleWith(asType))
        {
            var metadata = ReflectionMetadata.FindByType(asType);
            if (metadata == null)
            {
                throw new BTDBException("Cannot load " + asType.ToSimpleName() +
                                        " as it is not registered in ReflectionMetadata");
            }

            if (asType.IsValueType)
            {
                var loaders = new StructList<FieldHandlerLoad>();
                for (var i = 0; i < _fieldHandlers.Count; i++)
                {
                    var fieldHandler = _fieldHandlers[i];
                    if (i >= metadata.Fields.Length)
                    {
                        loaders.Add((ref MemReader reader, IReaderCtx? ctx, ref byte _) =>
                        {
                            fieldHandler.Skip(ref reader, ctx);
                        });
                        continue;
                    }

                    var field = metadata.Fields[i];
                    var loader = fieldHandler.Load(field.Type, typeConverterFactory);
                    var offset = field.ByteOffset!.Value;
                    loaders.Add((ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
                    {
                        loader(ref reader, ctx,
                            ref Unsafe.AddByteOffset(ref value, offset));
                    });
                }

                var loadersArray = loaders.ToArray();
                return (ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
                {
                    foreach (var fieldHandlerLoad in loadersArray)
                    {
                        fieldHandlerLoad(ref reader, ctx, ref value);
                    }
                };
            }
            else
            {
                var creator = metadata.Creator;
                var loaders = new StructList<FieldHandlerLoad>();
                for (var i = 0; i < _fieldHandlers.Count; i++)
                {
                    var fieldHandler = _fieldHandlers[i];
                    if (i >= metadata.Fields.Length)
                    {
                        loaders.Add((ref MemReader reader, IReaderCtx? ctx, ref byte _) =>
                        {
                            fieldHandler.Skip(ref reader, ctx);
                        });
                        continue;
                    }

                    var field = metadata.Fields[i];
                    var loader = fieldHandler.Load(field.Type, typeConverterFactory);
                    var offset = field.ByteOffset!.Value;
                    loaders.Add((ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
                    {
                        loader(ref reader, ctx,
                            ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset));
                    });
                }

                var loadersArray = loaders.ToArray();
                return (ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
                {
                    var tuple = creator();
                    Unsafe.As<byte, object>(ref value) = tuple;
                    foreach (var fieldHandlerLoad in loadersArray)
                    {
                        fieldHandlerLoad(ref reader, ctx, ref value);
                    }
                };
            }
        }

        return this.BuildConvertingLoader(HandledType(), asType, typeConverterFactory);
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        foreach (var f in _fieldHandlers)
        {
            f.Skip(ref reader, ctx);
        }
    }

    public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (HandledType() == asType)
        {
            var metadata = ReflectionMetadata.FindByType(asType);
            if (metadata == null)
            {
                throw new BTDBException("Cannot save " + asType.ToSimpleName() +
                                        " as it is not registered in ReflectionMetadata");
            }

            if (asType.IsValueType)
            {
                var savers = new StructList<FieldHandlerSave>();
                for (var i = 0; i < _fieldHandlers.Count; i++)
                {
                    var fieldHandler = _fieldHandlers[i];
                    var field = metadata.Fields[i];
                    var saver = fieldHandler.Save(field.Type, typeConverterFactory);
                    var offset = field.ByteOffset!.Value;
                    savers.Add((ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
                    {
                        saver(ref writer, ctx,
                            ref Unsafe.AddByteOffset(ref value, offset));
                    });
                }

                var saversArray = savers.ToArray();
                return (ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
                {
                    foreach (var fieldHandlerSave in saversArray)
                    {
                        fieldHandlerSave(ref writer, ctx, ref value);
                    }
                };
            }
            else
            {
                var savers = new StructList<FieldHandlerSave>();
                for (var i = 0; i < _fieldHandlers.Count; i++)
                {
                    var fieldHandler = _fieldHandlers[i];
                    var field = metadata.Fields[i];
                    var saver = fieldHandler.Save(field.Type, typeConverterFactory);
                    var offset = field.ByteOffset!.Value;
                    savers.Add((ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
                    {
                        saver(ref writer, ctx,
                            ref RawData.Ref(Unsafe.As<byte, object>(ref value), offset));
                    });
                }

                var saversArray = savers.ToArray();
                return (ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
                {
                    foreach (var fieldHandlerSave in saversArray)
                    {
                        fieldHandlerSave(ref writer, ctx, ref value);
                    }
                };
            }
        }

        return this.BuildConvertingSaver(asType, HandledType(), typeConverterFactory);
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (_type == type) return this;
        if (!IsCompatibleWith(type))
        {
            return this;
        }

        var wantedTypes = type.GetGenericArguments();
        var wantedHandlers = new StructList<IFieldHandler>();
        if (typeHandler is TupleFieldHandler tupleFieldHandler)
        {
            foreach (var fieldHandler in tupleFieldHandler._fieldHandlers)
            {
                wantedHandlers.Add(fieldHandler);
            }
        }

        wantedHandlers.RepeatAdd(default,
            (uint)Math.Max(wantedTypes.Length, _fieldHandlers.Count) - wantedHandlers.Count);

        StructList<IFieldHandler> specializedHandlers = new();
        for (var i = 0; i < wantedHandlers.Count; i++)
        {
            if (i < _fieldHandlers.Count)
            {
                var sourceHandler = _fieldHandlers[i];
                if (i >= wantedTypes.Length)
                {
                    specializedHandlers.Add(sourceHandler);
                    continue;
                }

                var specialized = sourceHandler.SpecializeLoadForType(wantedTypes[i], wantedHandlers[i], logger);
                if (_typeConvertGenerator.GenerateConversion(specialized.HandledType()!, wantedTypes[i]) == null)
                {
                    return this;
                }

                specializedHandlers.Add(specialized);
            }
            else
            {
                break;
            }
        }

        return new TupleFieldHandler(_fieldHandlerFactory, _typeConvertGenerator, type, specializedHandlers,
            _options);
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        if (_type == type) return this;
        if (!IsCompatibleWith(type))
        {
            Debug.Fail("strange");
            return this;
        }

        var wantedTypes = type.GetGenericArguments();
        var specializedHandlers = new StructList<IFieldHandler>();
        for (var i = 0; i < wantedTypes.Length; i++)
        {
            var specialized = _fieldHandlers[i].SpecializeSaveForType(wantedTypes[i]);
            if (_typeConvertGenerator.GenerateConversion(wantedTypes[i], specialized.HandledType()!) == null)
            {
                Debug.Fail("even more strange");
                return this;
            }

            specializedHandlers.Add(specialized);
        }

        return new TupleFieldHandler(_fieldHandlerFactory, _typeConvertGenerator, type, specializedHandlers,
            _options);
    }

    public IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers()
    {
        return _fieldHandlers;
    }

    public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        foreach (var f in _fieldHandlers)
        {
            f.FreeContent(ref reader, ctx);
        }
    }

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes)
    {
        return _fieldHandlers.Any(handler => handler.DoesNeedFreeContent(visitedTypes));
    }
}
