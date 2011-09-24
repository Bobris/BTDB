using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class DictionaryFieldHandler : IFieldHandler
    {
        readonly IFieldHandlerFactory _fieldHandlerFactory;
        readonly ITypeConvertorGenerator _typeConvertorGenerator;
        readonly byte[] _configuration;
        readonly IFieldHandler _keysHandler;
        readonly IFieldHandler _valuesHandler;
        Type _type;

        public DictionaryFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator, Type type)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertorGenerator = typeConvertorGenerator;
            _type = type;
            _keysHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0]);
            _valuesHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[1]);
            var writer = new ByteArrayWriter();
            writer.WriteFieldHandler(_keysHandler);
            writer.WriteFieldHandler(_valuesHandler);
            _configuration = writer.Data;
        }

        public DictionaryFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator, byte[] configuration)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertorGenerator = typeConvertorGenerator;
            _configuration = configuration;
            var reader = new ByteArrayReader(configuration);
            _keysHandler = _fieldHandlerFactory.CreateFromReader(reader);
            _valuesHandler = _fieldHandlerFactory.CreateFromReader(reader);
        }

        public static string HandlerName
        {
            get { return "Dictionary"; }
        }

        public string Name
        {
            get { return HandlerName; }
        }

        public byte[] Configuration
        {
            get { return _configuration; }
        }

        public static bool IsCompatibleWith(Type type)
        {
            if (!type.IsGenericType) return false;
            return type.GetGenericTypeDefinition() == typeof(IDictionary<,>);
        }

        bool IFieldHandler.IsCompatibleWith(Type type)
        {
            return IsCompatibleWith(type);
        }

        public Type HandledType()
        {
            return _type ?? (_type = typeof(IDictionary<,>).MakeGenericType(_keysHandler.HandledType(),_valuesHandler.HandledType()));
        }

        public bool NeedsCtx()
        {
            return _keysHandler.NeedsCtx() || _valuesHandler.NeedsCtx();
        }

        public static IList<T> LoadCtx<T>(IReaderCtx ctx) where T : class
        {
            var count = ctx.Reader().ReadVUInt32();
            if (count == 0) return null;
            count--;
            var result = new List<T>((int)count);
            while (count-- > 0)
                result.Add(ctx.ReadObject() as T);
            return result;
        }

        public void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            bool hasCtx = NeedsCtx();
            var localCount = ilGenerator.DeclareLocal(typeof(uint));
            var localResult = ilGenerator.DeclareLocal(_type);
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            var genericArguments = _type.GetGenericArguments();
            ilGenerator
                .Callvirt(() => ((AbstractBufferedReader)null).ReadVUInt32())
                .Stloc(localCount)
                .Ldloc(localCount)
                .Brfalse(finish)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .ConvU4()
                .Stloc(localCount)
                .Ldloc(localCount)
                .Newobj(
                    typeof(Dictionary<,>).MakeGenericType(genericArguments).GetConstructor(new[] { typeof(int) }))
                .Stloc(localResult)
                .Mark(next)
                .Ldloc(localCount)
                .Brfalse(finish)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .ConvU4()
                .Stloc(localCount)
                .Ldloc(localResult);
            _keysHandler.Load(ilGenerator, pushReaderOrCtx);
            _typeConvertorGenerator.GenerateConversion(_keysHandler.HandledType(), genericArguments[0])(ilGenerator);
            _valuesHandler.Load(ilGenerator, pushReaderOrCtx);
            _typeConvertorGenerator.GenerateConversion(_valuesHandler.HandledType(), genericArguments[1])(ilGenerator);
            ilGenerator
                .Callvirt(_type.GetMethod("Add"))
                .Br(next)
                .Mark(finish)
                .Ldloc(localResult);
        }

        public static void SkipLoadCtx(IReaderCtx ctx)
        {
            var reader = ctx.Reader();
            var count = reader.ReadVUInt32();
            if (count <= 1) return;
            count--;
            while (count-- > 0)
            {
                ctx.SkipObject();
            }
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            bool hasCtx = NeedsCtx();
            var localCount = ilGenerator.DeclareLocal(typeof(uint));
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Callvirt(() => ((AbstractBufferedReader) null).ReadVUInt32())
                .Stloc(localCount)
                .Ldloc(localCount)
                .Brfalse(finish)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .ConvU4()
                .Stloc(localCount)
                .Mark(next)
                .Ldloc(localCount)
                .Brfalse(finish)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .ConvU4()
                .Stloc(localCount);
            _keysHandler.SkipLoad(ilGenerator, pushReaderOrCtx);
            _valuesHandler.SkipLoad(ilGenerator, pushReaderOrCtx);
            ilGenerator
                .Br(next)
                .Mark(finish);
        }

        public static void SaveCtx<T>(IWriterCtx ctx, IList<T> list) where T : class
        {
            var writer = ctx.Writer();
            if (list == null)
            {
                writer.WriteVUInt32(0);
                return;
            }
            writer.WriteVUInt32((uint)list.Count + 1);
            foreach (var t in list)
            {
                ctx.WriteObject(t);
            }
        }

        public void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriterOrCtx, Action<ILGenerator> pushValue)
        {
            bool hasCtx = NeedsCtx();
            var realfinish = ilGenerator.DefineLabel();
            var finish = ilGenerator.DefineLabel();
            var isnull = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            var localValue = ilGenerator.DeclareLocal(_type);
            var typeAsICollection = _type.GetInterface("ICollection`1");
            var typeAsIEnumerable = _type.GetInterface("IEnumerable`1");
            var getEnumeratorMethod = typeAsIEnumerable.GetMethod("GetEnumerator");
            var typeAsIEnumerator = getEnumeratorMethod.ReturnType;
            var typeKeyValuePair = typeAsICollection.GetGenericArguments()[0];
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            var localPair = ilGenerator.DeclareLocal(typeKeyValuePair);
            ilGenerator
                .Do(pushValue)
                .Stloc(localValue)
                .Ldloc(localValue)
                .Brfalse(isnull)
                .Do(pushWriterOrCtx)
                .Ldloc(localValue)
                .Callvirt(typeAsICollection.GetProperty("Count").GetGetMethod())
                .LdcI4(1)
                .Add()
                .ConvU4()
                .Callvirt(() => ((AbstractBufferedWriter) null).WriteVUInt32(0))
                .Ldloc(localValue)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Ldloc(localEnumerator)
                .Callvirt(() => ((IEnumerator) null).MoveNext())
                .Brfalse(finish)
                .Ldloc(localEnumerator)
                .Callvirt(typeAsIEnumerator.GetProperty("Current").GetGetMethod())
                .Stloc(localPair);
            _keysHandler.Save(ilGenerator, pushWriterOrCtx, il => il
                .Ldloca(localPair)
                .Call(typeKeyValuePair.GetProperty("Key").GetGetMethod())
                .Do(_typeConvertorGenerator.GenerateConversion(_type.GetGenericArguments()[0], _keysHandler.HandledType())));
            _valuesHandler.Save(ilGenerator, pushWriterOrCtx, il => il
                .Ldloca(localPair)
                .Call(typeKeyValuePair.GetProperty("Value").GetGetMethod())
                .Do(_typeConvertorGenerator.GenerateConversion(_type.GetGenericArguments()[1], _valuesHandler.HandledType())));
            ilGenerator
                .Br(next)
                .Mark(finish)
                .Finally()
                .Ldloc(localEnumerator)
                .Callvirt(() => ((IDisposable) null).Dispose())
                .EndTry()
                .BrS(realfinish)
                .Mark(isnull)
                .Do(pushWriterOrCtx)
                .Callvirt(() => ((AbstractBufferedWriter) null).WriteByteZero())
                .Mark(realfinish);
        }

        public void InformAboutDestinationHandler(IFieldHandler dstHandler)
        {
            if (_type != null) return;
            if ((dstHandler is DictionaryFieldHandler) == false) return;
            _keysHandler.InformAboutDestinationHandler(((DictionaryFieldHandler)dstHandler)._keysHandler);
            _valuesHandler.InformAboutDestinationHandler(((DictionaryFieldHandler)dstHandler)._valuesHandler);
        }
    }
}