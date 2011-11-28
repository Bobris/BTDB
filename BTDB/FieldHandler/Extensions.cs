using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public static class Extensions
    {
        public static IEnumerable<T> Flatten<T>(this IEnumerable<T> enumerable, Func<T, IEnumerable<T>> childrenSelector)
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

        public static void WriteFieldHandler(this AbstractBufferedWriter writer, IFieldHandler handler)
        {
            writer.WriteString(handler.Name);
            writer.WriteByteArray(handler.Configuration);
        }

        public static IFieldHandler CreateFromReader(this IFieldHandlerFactory factory, AbstractBufferedReader reader)
        {
            var handlerName = reader.ReadString();
            var handlerConfiguration = reader.ReadByteArray();
            return factory.CreateFromName(handlerName, handlerConfiguration);
        }

        public static IILGen GenerateLoad(this IILGen ilGenerator, IFieldHandler fieldHandler, Type typeWanted, Action<IILGen> pushReaderOrCtx, ITypeConvertorGenerator typeConvertorGenerator)
        {
            fieldHandler.Load(ilGenerator,
                              fieldHandler.NeedsCtx() ? pushReaderOrCtx : PushReaderFromCtx(pushReaderOrCtx));
            typeConvertorGenerator.GenerateConversion(fieldHandler.HandledType(), typeWanted)(ilGenerator);
            return ilGenerator;
        }

        public static IILGen GenerateSkip(this IILGen ilGenerator, IFieldHandler fieldHandler, Action<IILGen> pushReaderOrCtx)
        {
            fieldHandler.Skip(ilGenerator,
                                  fieldHandler.NeedsCtx() ? pushReaderOrCtx : PushReaderFromCtx(pushReaderOrCtx));
            return ilGenerator;
        }

        public static Action<IILGen> PushReaderFromCtx(Action<IILGen> pushReaderOrCtx)
        {
            return il => { pushReaderOrCtx(il); il.Callvirt(() => default(IReaderCtx).Reader()); };
        }

        public static Action<IILGen> PushWriterOrCtxAsNeeded(Action<IILGen> pushWriterOrCtx, bool noConversion)
        {
            return noConversion ? pushWriterOrCtx : PushWriterFromCtx(pushWriterOrCtx);
        }

        public static Action<IILGen> PushWriterFromCtx(Action<IILGen> pushWriterOrCtx)
        {
            return il => { pushWriterOrCtx(il); il.Callvirt(() => default(IWriterCtx).Writer()); };
        }
    }
}