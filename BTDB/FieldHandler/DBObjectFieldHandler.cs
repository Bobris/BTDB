using System;
using System.Linq;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class DBObjectFieldHandler : IFieldHandler, IFieldHandlerWithInit
    {
        readonly IObjectDB _objectDB;
        readonly byte[] _configuration;
        readonly string _typeName;
        readonly bool _indirect;
        Type _type;

        public DBObjectFieldHandler(IObjectDB objectDB, Type type)
        {
            _objectDB = objectDB;
            _type = Unwrap(type);
            _indirect = _type != type;
            _typeName = _objectDB.RegisterType(_type);
            var writer = new ByteBufferWriter();
            writer.WriteString(_typeName);
            _configuration = writer.Data.ToByteArray();
        }

        static Type Unwrap(Type type)
        {
            if (IsIIndirect(type))
            {
                return type.GetGenericArguments()[0];
            }
            var indType = type.GetInterfaces().FirstOrDefault(IsIIndirect);
            if (indType == null) return type;
            return indType.GetGenericArguments()[0];
        }

        static bool IsIIndirect(Type ti)
        {
            return ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IIndirect<>);
        }

        public DBObjectFieldHandler(IObjectDB objectDB, byte[] configuration)
        {
            _objectDB = objectDB;
            _configuration = configuration;
            _typeName = string.Intern(new ByteArrayReader(configuration).ReadString());
            _indirect = false;
            CreateType();
        }

        Type CreateType()
        {
            return _type = _objectDB.TypeByName(_typeName);
        }

        public static string HandlerName => "Object";

        public string Name => HandlerName;

        public byte[] Configuration => _configuration;

        public static bool IsCompatibleWith(Type type)
        {
            type = Unwrap(type);
            return (!type.IsValueType && !type.IsArray && !type.IsSubclassOf(typeof(Delegate)));
        }

        public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            if (options.HasFlag(FieldHandlerOptions.Orderable)) return false;
            return IsCompatibleWith(type);
        }

        public Type HandledType()
        {
            if (_indirect) return typeof(IIndirect<>).MakeGenericType(_type);
            return _type ?? CreateType() ?? typeof(object);
        }

        public bool NeedsCtx()
        {
            return true;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            if (_indirect)
            {
                ilGenerator
                    .Do(pushReaderOrCtx)
                    .Call(typeof (DBIndirect<>).MakeGenericType(_type).GetMethod("LoadImpl"));
                return;
            }
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
            if (_indirect)
            {
                ilGenerator
                    .Do(pushWriterOrCtx)
                    .Do(pushValue)
                    .Call(typeof(DBIndirect<>).MakeGenericType(_type).GetMethod("SaveImpl"));
                return;
            }
            ilGenerator
                .Do(pushWriterOrCtx)
                .Do(pushValue)
                .Do(_objectDB.TypeConvertorGenerator.GenerateConversion(HandledType(), typeof(object)))
                .Callvirt(() => default(IWriterCtx).WriteNativeObject(null));
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler typeHandler)
        {
            return this;
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            return this;
        }

        public bool NeedInit()
        {
            return _indirect;
        }

        public void Init(IILGen ilGenerator, Action<IILGen> pushReaderCtx)
        {
            ilGenerator.Newobj(typeof(DBIndirect<>).MakeGenericType(_type).GetConstructor(Type.EmptyTypes));
        }

        public void FreeContent(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            ilGenerator
                .Do(pushReaderOrCtx)
                .Callvirt(() => default(IReaderCtx).FreeContentInNativeObject());
        }
    }
}