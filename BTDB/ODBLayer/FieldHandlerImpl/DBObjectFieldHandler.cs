using System;
using System.Linq;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class DBObjectFieldHandler : IFieldHandler
    {
        readonly IObjectDB _objectDB;
        readonly byte[] _configuration;
        readonly string _typeName;
        Type _type;

        public DBObjectFieldHandler(IObjectDB objectDB, Type type)
        {
            _objectDB = objectDB;
            _type = type;
            _typeName = _objectDB.RegisterType(type);
            var writer = new ByteArrayWriter();
            writer.WriteString(_typeName);
            _configuration = writer.Data;
        }

        public DBObjectFieldHandler(IObjectDB objectDB, byte[] configuration)
        {
            _objectDB = objectDB;
            _configuration = configuration;
            _typeName = new ByteArrayReader(configuration).ReadString();
            _type = _objectDB.TypeByName(_typeName);
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
            return _type ?? typeof(object);
        }

        public bool NeedsCtx()
        {
            return true;
        }

        public void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            pushReaderOrCtx(ilGenerator);
            ilGenerator.Callvirt(() => ((IReaderCtx)null).ReadObject());
            var type = HandledType();
            if (type != typeof(object)) ilGenerator.Isinst(type);
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