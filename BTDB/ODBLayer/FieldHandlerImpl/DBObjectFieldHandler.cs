using System;
using System.Linq;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer.ReaderWriters;
using BTDB.ODBLayer.FieldHandlerIface;

namespace BTDB.ODBLayer.FieldHandlerImpl
{
    public class DBObjectFieldHandler : IFieldHandler
    {
        Type _type;
        byte[] _configuration;

        public DBObjectFieldHandler(Type type)
        {
            _type = type;
            _configuration = new byte[0];
        }

        public DBObjectFieldHandler(byte[] configuration)
        {
            _type = typeof(object);
            _configuration = configuration;
        }

        public static string HandlerName
        {
            get { return "Object"; }
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
            return (!type.IsInterface && !type.IsValueType && !type.IsGenericType);
        }

        bool IFieldHandler.IsCompatibleWith(Type type)
        {
            return IsCompatibleWith(type);
        }

        public Type HandledType()
        {
            return _type;
        }

        public bool NeedsCtx()
        {
            return true;
        }

        public void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Callvirt(() => ((IReaderCtx)null).ReadObject());
            if (_type != typeof(object)) ilGenerator.Isinst(_type);
        }

        public void SkipLoad(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Callvirt(() => ((IReaderCtx)null).SkipObject());
        }

        public void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriterOrCtx, Action<ILGenerator> pushValue)
        {
            pushWriterOrCtx(ilGenerator);
            pushValue(ilGenerator);
            ilGenerator.Callvirt(() => ((IWriterCtx)null).WriteObject(null));
        }

        public void InformAboutDestinationHandler(IFieldHandler dstHandler)
        {
            if (_type != null) return;
            if ((dstHandler is DBObjectFieldHandler) == false) return;
            if (dstHandler.Configuration.SequenceEqual(Configuration))
            {
                _type = dstHandler.HandledType();
            }
        }
    }
}