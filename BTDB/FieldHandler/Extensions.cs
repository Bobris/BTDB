using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using BTDB.IL;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public static class Extensions
{
    public static IEnumerable<T> Flatten<T>(this IEnumerable<T> enumerable,
        Func<T, IEnumerable<T>> childrenSelector)
    {
        foreach (var item in enumerable)
        {
            yield return item;
            var nested = childrenSelector(item);
            if (nested == null) continue;
            foreach (var nestedItem in nested.Flatten(childrenSelector))
            {
                yield return nestedItem;
            }
        }
    }

    public static void WriteFieldHandler(this ref MemWriter writer, IFieldHandler handler)
    {
        writer.WriteString(handler.Name);
        writer.WriteByteArray(handler.Configuration);
    }

    public static IFieldHandler CreateFromReader(this IFieldHandlerFactory factory, ref MemReader reader,
        FieldHandlerOptions options)
    {
        var handlerName = reader.ReadString();
        var handlerConfiguration = reader.ReadByteArray();
        return factory.CreateFromName(handlerName!, handlerConfiguration!, options);
    }

    public static IILGen GenerateLoad(this IILGen ilGenerator, IFieldHandler fieldHandler, Type typeWanted,
        Action<IILGen> pushReader, Action<IILGen>? pushCtx, ITypeConvertorGenerator typeConvertorGenerator)
    {
        var betterFieldHandler = fieldHandler.SpecializeLoadForType(typeWanted, null, null);
        betterFieldHandler.Load(ilGenerator, pushReader, pushCtx);
        typeConvertorGenerator.GenerateConversion(betterFieldHandler.HandledType()!, typeWanted)!(ilGenerator);
        return ilGenerator;
    }

    public static IILGen GenerateSkip(this IILGen ilGenerator, IFieldHandler fieldHandler,
        Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        fieldHandler.Skip(ilGenerator, pushReader, pushCtx);
        return ilGenerator;
    }

    public static void RegisterFieldHandlers(IEnumerable<IFieldHandler?> fieldHandlers, object owner)
    {
        var visited = new HashSet<IFieldHandler>();
        var queue = new Queue<IFieldHandler>(fieldHandlers.Where(f => f != null));
        while (queue.TryDequeue(out var fieldHandler))
        {
            if (!visited.Add(fieldHandler)) continue;
            if (fieldHandler is IFieldHandlerWithRegister reg)
            {
                reg.Register(owner);
            }

            if (fieldHandler is IFieldHandlerWithNestedFieldHandlers nested)
            {
                foreach (var nestedFieldHandler in nested.EnumerateNestedFieldHandlers())
                {
                    queue.Enqueue(nestedFieldHandler);
                }
            }
        }
    }

    public static FieldHandlerLoad BuildConvertingLoader(this IFieldHandler handler, Type fromType, Type toType,
        ITypeConverterFactory typeConverterFactory)
    {
        var converter = typeConverterFactory.GetConverter(fromType, toType);
        if (converter == null)
        {
            throw new NotSupportedException(
                $"Cannot load {handler.Name} and convert {fromType.ToSimpleName()} to {toType.ToSimpleName()}");
        }

        var loader = handler.Load(fromType, typeConverterFactory);
        if (fromType.IsValueType && !RawData.MethodTableOf(fromType).ContainsGCPointers &&
            RawData.GetSizeAndAlign(fromType).Size <= 16)
        {
            return (ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
            {
                Int128 temp = 0;
                loader(ref reader, ctx, ref Unsafe.As<Int128, byte>(ref temp));
                converter(ref Unsafe.As<Int128, byte>(ref temp), ref value);
            };
        }

        if (!fromType.IsValueType)
        {
            return (ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
            {
                object temp = null!;
                loader(ref reader, ctx, ref Unsafe.As<object, byte>(ref temp));
                converter(ref Unsafe.As<object, byte>(ref temp), ref value);
            };
        }

        throw new NotImplementedException("TODO loading convertor from " + fromType.ToSimpleName() + " to " +
                                          toType.ToSimpleName());
    }

    public static FieldHandlerSave BuildConvertingSaver(this IFieldHandler fieldHandler, Type from, Type to,
        ITypeConverterFactory typeConverterFactory)
    {
        var converter = typeConverterFactory.GetConverter(from, to);
        if (converter == null)
        {
            throw new NotSupportedException(
                $"Cannot save {fieldHandler.Name} and convert {from.ToSimpleName()} to {to.ToSimpleName()}");
        }

        var saver = fieldHandler.Save(to, typeConverterFactory);
        if (from.IsValueType && !RawData.MethodTableOf(to).ContainsGCPointers &&
            RawData.GetSizeAndAlign(to).Size <= 16)
        {
            return (ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
            {
                Int128 temp = 0;
                converter(ref value, ref Unsafe.As<Int128, byte>(ref temp));
                saver(ref writer, ctx, ref Unsafe.As<Int128, byte>(ref temp));
            };
        }

        if (!from.IsValueType)
        {
            return (ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
            {
                object temp = null!;
                converter(ref value, ref Unsafe.As<object, byte>(ref temp));
                saver(ref writer, ctx, ref Unsafe.As<object, byte>(ref temp));
            };
        }

        throw new NotImplementedException("TODO saving convertor from " + from.ToSimpleName() + " to " +
                                          to.ToSimpleName());
    }
}
