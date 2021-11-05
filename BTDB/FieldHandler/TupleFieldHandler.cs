using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using BTDB.Collections;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class TupleFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers
{
    readonly IFieldHandlerFactory _fieldHandlerFactory;
    readonly ITypeConvertorGenerator _typeConvertGenerator;
    readonly StructList<IFieldHandler> _fieldHandlers;
    Type? _type;
    readonly FieldHandlerOptions _options;

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

        var writer = new SpanWriter();
        writer.WriteVUInt32(_fieldHandlers.Count);
        foreach (var f in _fieldHandlers)
        {
            writer.WriteFieldHandler(f);
        }

        Configuration = writer.GetSpan().ToArray();
    }

    public TupleFieldHandler(IFieldHandlerFactory fieldHandlerFactory,
        ITypeConvertorGenerator typeConvertGenerator, byte[] configuration, FieldHandlerOptions options)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertGenerator = typeConvertGenerator;
        _options = options;
        Configuration = configuration;
        var reader = new SpanReader(configuration);
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

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (_type == type) return this;
        if (!IsCompatibleWith(type))
        {
            logger?.ReportTypeIncompatibility(_type, this, type, typeHandler);
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
                    logger?.ReportTypeIncompatibility(specialized.HandledType(), specialized, wantedTypes[i],
                        wantedHandlers[i]);
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

    public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        var needsFreeContent = NeedsFreeContent.No;
        foreach (var f in _fieldHandlers)
        {
            ilGenerator
                .GenerateFreeContent(f, pushReader, pushCtx, ref needsFreeContent);
        }

        return needsFreeContent;
    }
}
