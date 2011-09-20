using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class ListFieldHandler : IFieldHandler
    {
        readonly IFieldHandlerFactory _fieldHandlerFactory;
        readonly ITypeConvertorGenerator _typeConvertorGenerator;
        readonly byte[] _configuration;
        readonly IFieldHandler _itemsHandler;
        Type _type;

        public ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator, Type type)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertorGenerator = typeConvertorGenerator;
            _type = type;
            _itemsHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0]);
            var writer = new ByteArrayWriter();
            writer.WriteString(_itemsHandler.Name);
            writer.WriteByteArray(_itemsHandler.Configuration);
            _configuration = writer.Data;
        }

        public ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator, byte[] configuration)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertorGenerator = typeConvertorGenerator;
            _configuration = configuration;
            var reader = new ByteArrayReader(configuration);
            var itemsHandlerName = reader.ReadString();
            var itemsHandlerConfiguration = reader.ReadByteArray();
            _itemsHandler = _fieldHandlerFactory.CreateFromName(itemsHandlerName, itemsHandlerConfiguration);
        }

        public static string HandlerName
        {
            get { return "List"; }
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
            return type.GetGenericTypeDefinition() == typeof(IList<>);
        }

        bool IFieldHandler.IsCompatibleWith(Type type)
        {
            return IsCompatibleWith(type);
        }

        public Type HandledType()
        {
            return _type ?? (_type = typeof(IList<>).MakeGenericType(_itemsHandler.HandledType()));
        }

        public bool NeedsCtx()
        {
            return _itemsHandler.NeedsCtx();
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
            if (NeedsCtx())
            {
                ilGenerator.Call(
                    typeof(ListFieldHandler).GetMethod("LoadCtx").MakeGenericMethod(_type.GetGenericArguments()[0]));
                return;
            }
            var localCount = ilGenerator.DeclareLocal(typeof(uint));
            var localResult = ilGenerator.DeclareLocal(_type);
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
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
                    typeof(List<>).MakeGenericType(_type.GetGenericArguments()[0]).GetConstructor(new[] { typeof(int) }))
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
            _itemsHandler.Load(ilGenerator, pushReaderOrCtx);
            _typeConvertorGenerator.GenerateConversion(_itemsHandler.HandledType(), _type.GetGenericArguments()[0])(ilGenerator);
            ilGenerator
                .Callvirt(_type.GetInterface("ICollection`1").GetMethod("Add"))
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
            if (NeedsCtx())
            {
                ilGenerator.Call(() => SkipLoadCtx(null));
                return;
            }
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
            _itemsHandler.SkipLoad(ilGenerator, pushReaderOrCtx);
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
            if (NeedsCtx())
            {
                pushWriterOrCtx(ilGenerator);
                pushValue(ilGenerator);
                ilGenerator.Call(
                    typeof(ListFieldHandler).GetMethod("SaveCtx").MakeGenericMethod(_type.GetGenericArguments()[0]));
                return;
            }
            var finish = ilGenerator.DefineLabel();
            var isnull = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            var localValue = ilGenerator.DeclareLocal(_type);
            var localIndex = ilGenerator.DeclareLocal(typeof(int));
            var localCount = ilGenerator.DeclareLocal(typeof(int));
            ilGenerator
                .Do(pushValue)
                .Stloc(localValue)
                .Ldloc(localValue)
                .Brfalse(isnull)
                .Ldloc(localValue)
                .Callvirt(_type.GetInterface("ICollection`1").GetProperty("Count").GetGetMethod())
                .Stloc(localCount)
                .Do(pushWriterOrCtx)
                .Ldloc(localCount)
                .LdcI4(1)
                .Add()
                .ConvU4()
                .Callvirt(() => ((AbstractBufferedWriter)null).WriteVUInt32(0))
                .Mark(next)
                .Ldloc(localIndex)
                .Ldloc(localCount)
                .BgeUn(finish);
            _itemsHandler.Save(ilGenerator, pushWriterOrCtx, il => il
                .Ldloc(localValue)
                .Ldloc(localIndex)
                .Callvirt(_type.GetMethod("get_Item"))
                .Do(_typeConvertorGenerator.GenerateConversion(_type.GetGenericArguments()[0], _itemsHandler.HandledType())));
            ilGenerator
                .Ldloc(localIndex)
                .LdcI4(1)
                .Add()
                .Stloc(localIndex)
                .Br(next)
                .Mark(isnull)
                .Do(pushWriterOrCtx)
                .Callvirt(() => ((AbstractBufferedWriter)null).WriteByteZero())
                .Mark(finish);
        }

        public void InformAboutDestinationHandler(IFieldHandler dstHandler)
        {
            if (_type != null) return;
            if ((dstHandler is ListFieldHandler) == false) return;
            _itemsHandler.InformAboutDestinationHandler(((ListFieldHandler)dstHandler)._itemsHandler);
        }
    }
}