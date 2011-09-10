using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class ListFieldHandler : IFieldHandler
    {
        readonly IFieldHandlerFactory _fieldHandlerFactory;
        readonly byte[] _configuration;
        readonly IFieldHandler _itemsHandler;
        Type _type;

        public ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, Type type)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _type = type;
            _itemsHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0]);
            var writer = new ByteArrayWriter();
            writer.WriteString(_itemsHandler.Name);
            writer.WriteByteArray(_itemsHandler.Configuration);
        }

        public ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, byte[] configuration)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
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
            return _type ?? (_type = typeof (IList<>).MakeGenericType(_itemsHandler.HandledType()));
        }

        public bool NeedsCtx()
        {
            return _itemsHandler.NeedsCtx();
        }

        public static IList<T> LoadCtx<T>(IReaderCtx ctx) where T : class
        {
            var count = ctx.Reader().ReadVUInt32();
            var result = new List<T>((int) count);
            while (count-->0)
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
            throw new NotImplementedException();
        }

        public static void SkipLoadCtx(IReaderCtx ctx)
        {
            var reader = ctx.Reader();
            var count = reader.ReadVUInt32();
            while (count-->0)
            {
                ctx.SkipObject();
            }
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            if (NeedsCtx())
            {
                ilGenerator.Call(()=>SkipLoadCtx(null));
                return;
            }
            throw new NotImplementedException();
        }

        public static void SaveCtx<T>(IWriterCtx ctx, IList<T> list) where T : class
        {
            ctx.Writer().WriteVUInt32((uint) list.Count);
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
                    typeof (ListFieldHandler).GetMethod("SaveCtx").MakeGenericMethod(_type.GetGenericArguments()[0]));
                return;
            }
            throw new NotImplementedException();
        }

        public void InformAboutDestinationHandler(IFieldHandler dstHandler)
        {
            if (_type != null) return;
            if ((dstHandler is ListFieldHandler) == false) return;
            _itemsHandler.InformAboutDestinationHandler(((ListFieldHandler)dstHandler)._itemsHandler);
        }
    }
}