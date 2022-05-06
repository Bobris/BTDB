using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.IL;
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

    public static void WriteFieldHandler(this ref SpanWriter writer, IFieldHandler handler)
    {
        writer.WriteString(handler.Name);
        writer.WriteByteArray(handler.Configuration);
    }

    public static IFieldHandler CreateFromReader(this IFieldHandlerFactory factory, ref SpanReader reader,
        FieldHandlerOptions options)
    {
        var handlerName = reader.ReadString();
        var handlerConfiguration = reader.ReadByteArray();
        return factory.CreateFromName(handlerName!, handlerConfiguration!, options);
    }

    public static IILGen GenerateLoad(this IILGen ilGenerator, IFieldHandler fieldHandler, Type typeWanted,
        Action<IILGen> pushReader, Action<IILGen>? pushCtx, ITypeConvertorGenerator typeConvertorGenerator)
    {
        fieldHandler.Load(ilGenerator, pushReader, pushCtx);
        typeConvertorGenerator.GenerateConversion(fieldHandler.HandledType(), typeWanted)!(ilGenerator);
        return ilGenerator;
    }

    public static IILGen GenerateSkip(this IILGen ilGenerator, IFieldHandler fieldHandler,
        Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        fieldHandler.Skip(ilGenerator, pushReader, pushCtx);
        return ilGenerator;
    }

    public static void UpdateNeedsFreeContent(NeedsFreeContent partial, ref NeedsFreeContent accumulatedValue)
    {
        if ((int)partial > (int)accumulatedValue)
            accumulatedValue = partial;
    }

    public static IILGen GenerateFreeContent(this IILGen ilGenerator, IFieldHandler fieldHandler,
        Action<IILGen> pushReader, Action<IILGen>? pushCtx, ref NeedsFreeContent needsFreeContent)
    {
        UpdateNeedsFreeContent(fieldHandler.FreeContent(ilGenerator, pushReader, pushCtx), ref needsFreeContent);
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
}
