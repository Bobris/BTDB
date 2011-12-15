using System;
using System.Reflection.Emit;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
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
            _typeName = string.Intern(new ByteArrayReader(configuration).ReadString());
            CreateType();
        }

        Type CreateType()
        {
            return _type = _objectDB.TypeByName(_typeName);
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
            return (!type.IsValueType && !type.IsArray && !type.IsSubclassOf(typeof(Delegate)));
        }

        bool IFieldHandler.IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            if (options.HasFlag(FieldHandlerOptions.Orderable)) return false;
            return IsCompatibleWith(type);
        }

        public Type HandledType()
        {
            return _type ?? CreateType() ?? typeof(object);
        }

        public bool NeedsCtx()
        {
            return true;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            ilGenerator
                .Do(pushReaderOrCtx)
                .Callvirt(() => default(IReaderCtx).ReadNativeObject());
            var type = HandledType();
            ilGenerator.Do(_objectDB.TypeConvertorGenerator.GenerateConversion(typeof(object), type));
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            ilGenerator
                .Do(pushReaderOrCtx)
                .Callvirt(() => default(IReaderCtx).SkipNativeObject());
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            ilGenerator
                .Do(pushWriterOrCtx)
                .Do(pushValue)
                .Do(_objectDB.TypeConvertorGenerator.GenerateConversion(HandledType(), typeof(object)))
                .Callvirt(() => default(IWriterCtx).WriteNativeObject(null));
        }

        public IFieldHandler SpecializeLoadForType(Type type)
        {
            return this;
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            return this;
        }
    }
}