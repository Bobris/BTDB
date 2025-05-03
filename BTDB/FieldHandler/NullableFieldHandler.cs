using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using BTDB.IL;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class NullableFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers
{
    readonly IFieldHandlerFactory _fieldHandlerFactory;
    readonly ITypeConvertorGenerator _typeConvertorGenerator;
    readonly IFieldHandler _itemHandler;
    Type? _type;

    [SkipLocalsInit]
    public NullableFieldHandler(IFieldHandlerFactory fieldHandlerFactory,
        ITypeConvertorGenerator typeConvertorGenerator, Type type)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertorGenerator = typeConvertorGenerator;
        _type = type;
        _itemHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0], FieldHandlerOptions.None);
        if (_itemHandler.NeedsCtx())
            throw new NotSupportedException("Nullable complex types are not supported.");
        Span<byte> buf = stackalloc byte[512];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buf);
        writer.WriteFieldHandler(_itemHandler);
        Configuration = writer.GetSpan().ToArray();
    }

    public unsafe NullableFieldHandler(IFieldHandlerFactory fieldHandlerFactory,
        ITypeConvertorGenerator typeConvertorGenerator, byte[] configuration)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertorGenerator = typeConvertorGenerator;
        Configuration = configuration;
        fixed (void* confPtr = configuration)
        {
            var reader = new MemReader(confPtr, configuration.Length);
            _itemHandler = _fieldHandlerFactory.CreateFromReader(ref reader, FieldHandlerOptions.None);
        }
    }

    NullableFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator,
        Type type, IFieldHandler itemSpecialized)
    {
        _fieldHandlerFactory = fieldHandlerFactory;
        _typeConvertorGenerator = typeConvertorGenerator;
        _type = type;
        _itemHandler = itemSpecialized;
    }

    public static string HandlerName => "Nullable";

    public string Name => HandlerName;

    public byte[]? Configuration { get; }

    public static bool IsCompatibleWith(Type type)
    {
        if (!type.IsGenericType) return false;
        return type.GetGenericTypeDefinition() == typeof(Nullable<>);
    }

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return IsCompatibleWith(type);
    }

    public Type HandledType()
    {
        return _type ??= typeof(Nullable<>).MakeGenericType(_itemHandler.HandledType());
    }

    public bool NeedsCtx()
    {
        return _itemHandler.NeedsCtx();
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        var localResult = ilGenerator.DeclareLocal(HandledType());
        var finish = ilGenerator.DefineLabel();
        var noValue = ilGenerator.DefineLabel();
        var itemType = _type!.GetGenericArguments()[0];
        var nullableType = typeof(Nullable<>).MakeGenericType(itemType);

        ilGenerator
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadBool))!)
            .Brfalse(noValue);
        _itemHandler.Load(ilGenerator, pushReader, pushCtx);
        _typeConvertorGenerator.GenerateConversion(_itemHandler.HandledType(), itemType)!(ilGenerator);
        ilGenerator
            .Newobj(nullableType.GetConstructor(new[] { itemType })!)
            .Stloc(localResult)
            .BrS(finish)
            .Mark(noValue)
            .Ldloca(localResult)
            .InitObj(nullableType)
            .Mark(finish)
            .Ldloc(localResult);
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        var finish = ilGenerator.DefineLabel();
        ilGenerator
            .Do(pushReader)
            .Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadBool))!)
            .Brfalse(finish)
            .GenerateSkip(_itemHandler, pushReader, pushCtx)
            .Mark(finish);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx, Action<IILGen> pushValue)
    {
        var nullableType = typeof(Nullable<>).MakeGenericType(_type!.GetGenericArguments()[0]);
        var localValue = ilGenerator.DeclareLocal(nullableType);
        var localHasValue = ilGenerator.DeclareLocal(typeof(bool));
        var finish = ilGenerator.DefineLabel();
        ilGenerator
            .Do(pushValue)
            .Stloc(localValue)
            .Do(pushWriter)
            .Ldloca(localValue) //ref for struct!
            .Call(nullableType.GetMethod("get_HasValue")!)
            .Dup()
            .Stloc(localHasValue)
            .Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteBool))!)
            .Ldloc(localHasValue)
            .Brfalse(finish);
        _itemHandler.Save(ilGenerator, pushWriter, pushCtx,
            il => il
                .Ldloca(localValue).Call(_type.GetMethod("get_Value")!)
                .Do(_typeConvertorGenerator.GenerateConversion(_type.GetGenericArguments()[0],
                    _itemHandler.HandledType())!));
        ilGenerator.Mark(finish);
    }

    public FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        var underlyingType = Nullable.GetUnderlyingType(asType);
        if (underlyingType == null)
        {
            return this.BuildConvertingLoader(HandledType(), asType, typeConverterFactory);
        }

        var itemLoader = _itemHandler.Load(underlyingType, typeConverterFactory);
        var offset = RawData.Align(1, RawData.GetSizeAndAlign(underlyingType).Align);

        return (ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
        {
            if (reader.ReadBool())
            {
                value = 1;
                itemLoader(ref reader, ctx, ref Unsafe.AddByteOffset(ref value, offset));
            }
            else
            {
                value = 0;
            }
        };
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        if (reader.ReadBool())
        {
            _itemHandler.Skip(ref reader, ctx);
        }
    }

    public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        var underlyingType = Nullable.GetUnderlyingType(asType);
        if (underlyingType == null)
        {
            return this.BuildConvertingSaver(asType, HandledType(), typeConverterFactory);
        }

        var itemSaver = _itemHandler.Save(underlyingType, typeConverterFactory);
        var offset = RawData.Align(1, RawData.GetSizeAndAlign(underlyingType).Align);
        return (ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
        {
            if (value != 0)
            {
                writer.WriteBool(true);
                itemSaver(ref writer, ctx, ref Unsafe.AddByteOffset(ref value, offset));
            }
            else
            {
                writer.WriteBool(false);
            }
        };
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (_type == type) return this;
        if (!IsCompatibleWith(type))
        {
            return this;
        }

        var wantedItemType = type.GetGenericArguments()[0];
        var wantedItemHandler = default(IFieldHandler);
        if (typeHandler is NullableFieldHandler nullableFieldHandler)
        {
            wantedItemHandler = nullableFieldHandler._itemHandler;
        }

        var itemSpecialized = _itemHandler.SpecializeLoadForType(wantedItemType, wantedItemHandler, logger);
        if (itemSpecialized == wantedItemHandler)
        {
            return typeHandler;
        }

        if (_typeConvertorGenerator.GenerateConversion(itemSpecialized.HandledType(), wantedItemType) == null)
        {
            Debug.Fail("even more strange");
            return this;
        }

        return new NullableFieldHandler(_fieldHandlerFactory, _typeConvertorGenerator, type, itemSpecialized);
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        if (_type == type) return this;
        if (!IsCompatibleWith(type))
        {
            Debug.Fail("strange");
            return this;
        }

        var wantedItemType = type.GetGenericArguments()[0];
        var itemSpecialized = _itemHandler.SpecializeSaveForType(wantedItemType);
        if (_typeConvertorGenerator.GenerateConversion(wantedItemType, itemSpecialized.HandledType()) == null)
        {
            Debug.Fail("even more strange");
            return this;
        }

        return new NullableFieldHandler(_fieldHandlerFactory, _typeConvertorGenerator, type, itemSpecialized);
    }

    public IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers()
    {
        yield return _itemHandler;
    }

    public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        if (reader.ReadBool())
        {
            _itemHandler.Skip(ref reader, ctx);
        }
    }

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes)
    {
        return false;
    }
}
