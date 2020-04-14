using System;
using System.Linq;
using BTDB.IL;
using BTDB.ODBLayer;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class DBObjectFieldHandler : IFieldHandler, IFieldHandlerWithInit
    {
        readonly IObjectDB _objectDb;
        readonly byte[] _configuration;
        readonly string? _typeName;
        readonly bool _indirect;
        Type? _type;

        public DBObjectFieldHandler(IObjectDB objectDb, Type type)
        {
            _objectDb = objectDb;
            _type = Unwrap(type);
            _indirect = _type != type;
            if (_type.IsInterface || _type.IsAbstract)
            {
                _type = typeof(object);
                _typeName = null;
                _configuration = Array.Empty<byte>();
            }
            else
            {
                _typeName = (_objectDb as ObjectDB)?.RegisterType(_type, false);
                var writer = new ByteBufferWriter();
                writer.WriteString(_typeName);
                _configuration = writer.Data.ToByteArray();
            }
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

        public DBObjectFieldHandler(IObjectDB objectDb, byte[] configuration)
        {
            _objectDb = objectDb;
            _configuration = configuration;
            if (configuration.Length == 0)
            {
                _typeName = null;
            }
            else
            {
                _typeName = string.Intern(new ByteArrayReader(configuration).ReadString()!);
                _indirect = false;
            }
            CreateType();
        }

        public DBObjectFieldHandler(IObjectDB objectDb, Type type, bool indirect) : this(objectDb, type)
        {
            _objectDb = objectDb;
            _type = type;
            _typeName = null;
            _indirect = indirect;
        }

        Type? CreateType()
        {
            if (_typeName == null)
            {
                return _type = typeof(object);
            }
            return _type = _objectDb.TypeByName(_typeName);
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
            if ((options & FieldHandlerOptions.Orderable) != 0) return false;
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
                    .Call(typeof(DBIndirect<>).MakeGenericType(_type).GetMethod("LoadImpl"));
                return;
            }
            ilGenerator
                .Do(pushReaderOrCtx)
                .Callvirt(() => default(IReaderCtx).ReadNativeObject());
            var type = HandledType();
            ilGenerator.Do(_objectDb.TypeConvertorGenerator.GenerateConversion(typeof(object), type)!);
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
                .Do(_objectDb.TypeConvertorGenerator.GenerateConversion(HandledType(), typeof(object))!)
                .Callvirt(() => default(IWriterCtx).WriteNativeObject(null));
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler typeHandler)
        {
            var needType = Unwrap(type);
            if (needType.IsInterface || needType.IsAbstract || type != needType && needType == HandledType())
            {
                return new DBObjectFieldHandler(_objectDb, needType, needType != type);
            }
            return this;
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            var needType = Unwrap(type);
            if (needType.IsInterface)
            {
                return new DBObjectFieldHandler(_objectDb, needType, needType != type);
            }
            return this;
        }

        public bool NeedInit()
        {
            return _indirect;
        }

        public void Init(IILGen ilGenerator, Action<IILGen> pushReaderCtx)
        {
            ilGenerator.Newobj(typeof(DBIndirect<>).MakeGenericType(_type!).GetConstructor(Type.EmptyTypes)!);
        }

        public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            var needsFreeContent = NeedsFreeContent.No;
            var type = HandledType();
            foreach (var st in _objectDb.GetPolymorphicTypes(type))
            {
                UpdateNeedsFreeContent(st, ref needsFreeContent);
            }
            if (!type.IsInterface && !type.IsAbstract)
                UpdateNeedsFreeContent(type, ref needsFreeContent);

            ilGenerator
                .Do(pushReaderOrCtx)
                .Callvirt(() => default(IReaderCtx).FreeContentInNativeObject());
            return needsFreeContent;
        }

        void UpdateNeedsFreeContent(Type type, ref NeedsFreeContent needsFreeContent)
        {
            //decides upon current version  (null for object types never stored in DB)
            var tableInfo = ((ObjectDB)_objectDb).TablesInfo.FindByType(type);
            var needsContentPartial = tableInfo?.IsFreeContentNeeded(tableInfo.ClientTypeVersion) ?? NeedsFreeContent.Unknown;
            Extensions.UpdateNeedsFreeContent(needsContentPartial, ref needsFreeContent);
        }
    }
}
