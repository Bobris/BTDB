using System;
using System.Collections.Generic;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;

namespace BTDB.ODBLayer
{
    public class ODBSetFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers, IFieldHandlerWithInit
    {
        readonly IObjectDB _odb;
        readonly ITypeConvertorGenerator _typeConvertGenerator;
        readonly byte[] _configuration;
        readonly IFieldHandler _keysHandler;
        int _configurationId;
        Type? _type;

        public ODBSetFieldHandler(IObjectDB odb, Type type, IFieldHandlerFactory fieldHandlerFactory)
        {
            _odb = odb;
            _typeConvertGenerator = odb.TypeConvertorGenerator;
            _type = type;
            _keysHandler = fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0],
                FieldHandlerOptions.Orderable | FieldHandlerOptions.AtEndOfStream);
            var writer = new ByteBufferWriter();
            writer.WriteFieldHandler(_keysHandler);
            _configuration = writer.Data.ToByteArray();
            CreateConfiguration();
        }

        public ODBSetFieldHandler(IObjectDB odb, byte[] configuration)
        {
            _odb = odb;
            var fieldHandlerFactory = odb.FieldHandlerFactory;
            _typeConvertGenerator = odb.TypeConvertorGenerator;
            _configuration = configuration;
            var reader = new ByteArrayReader(configuration);
            _keysHandler = fieldHandlerFactory.CreateFromReader(reader,
                FieldHandlerOptions.Orderable | FieldHandlerOptions.AtEndOfStream);
            CreateConfiguration();
        }

        ODBSetFieldHandler(IObjectDB odb, byte[] configuration, IFieldHandler specializedKeyHandler)
        {
            _odb = odb;
            _typeConvertGenerator = odb.TypeConvertorGenerator;
            _configuration = configuration;
            _keysHandler = specializedKeyHandler;
            CreateConfiguration();
        }

        void CreateConfiguration()
        {
            HandledType();
            var keyAndValueTypes = _type!.GetGenericArguments();
            _configurationId = ODBDictionaryConfiguration.Register(_keysHandler, keyAndValueTypes[0], null, null);
            var cfg = ODBDictionaryConfiguration.Get(_configurationId);
            lock (cfg)
            {
                cfg.KeyReader ??= CreateReader(_keysHandler, keyAndValueTypes[0]);
                cfg.KeyWriter ??= CreateWriter(_keysHandler, keyAndValueTypes[0]);
            }
        }

        object CreateWriter(IFieldHandler fieldHandler, Type realType)
        {
            //Action<T, AbstractBufferedWriter, IWriterCtx>
            var delegateType =
                typeof(Action<,,>).MakeGenericType(realType, typeof(AbstractBufferedWriter), typeof(IWriterCtx));
            var dm = ILBuilder.Instance.NewMethod(fieldHandler.Name + "Writer", delegateType);
            var ilGenerator = dm.Generator;
            void PushWriterOrCtx(IILGen il) => il.Ldarg((ushort) (1 + (fieldHandler.NeedsCtx() ? 1 : 0)));
            fieldHandler.Save(ilGenerator, PushWriterOrCtx,
                il => il.Ldarg(0).Do(_typeConvertGenerator.GenerateConversion(realType, fieldHandler.HandledType())!));
            ilGenerator.Ret();
            return dm.Create();
        }

        object CreateReader(IFieldHandler fieldHandler, Type realType)
        {
            //Func<AbstractBufferedReader, IReaderCtx, T>
            var delegateType = typeof(Func<,,>).MakeGenericType(typeof(AbstractBufferedReader), typeof(IReaderCtx),
                realType);
            var dm = ILBuilder.Instance.NewMethod(fieldHandler.Name + "Reader", delegateType);
            var ilGenerator = dm.Generator;
            void PushReaderOrCtx(IILGen il) => il.Ldarg((ushort) (fieldHandler.NeedsCtx() ? 1 : 0));
            fieldHandler.Load(ilGenerator, PushReaderOrCtx);
            ilGenerator
                .Do(_typeConvertGenerator.GenerateConversion(fieldHandler.HandledType(), realType)!)
                .Ret();
            return dm.Create();
        }

        public static string HandlerName => "ODBSet";

        public string Name => HandlerName;

        public byte[] Configuration => _configuration;

        public static bool IsCompatibleWithStatic(Type type, FieldHandlerOptions options)
        {
            if ((options & FieldHandlerOptions.Orderable) != 0) return false;
            return type.IsGenericType && IsCompatibleWithCore(type);
        }

        static bool IsCompatibleWithCore(Type type)
        {
            var genericTypeDefinition = type.GetGenericTypeDefinition();
            return genericTypeDefinition == typeof(IOrderedSet<>);
        }

        public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return IsCompatibleWithStatic(type, options);
        }

        public Type HandledType()
        {
            return _type ?? GenerateType(null);
        }

        public bool NeedsCtx()
        {
            return true;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            var genericArguments = _type!.GetGenericArguments();
            var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
            var constructorInfo = instanceType.GetConstructor(
                new[] {typeof(IInternalObjectDBTransaction), typeof(ODBDictionaryConfiguration), typeof(ulong)});
            ilGenerator
                .Do(pushReaderOrCtx)
                .Castclass(typeof(IDBReaderCtx))
                .Callvirt(() => default(IDBReaderCtx).GetTransaction())
                .LdcI4(_configurationId)
                .Call(() => ODBDictionaryConfiguration.Get(0))
                .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                .Callvirt(() => default(AbstractBufferedReader).ReadVUInt64())
                .Newobj(constructorInfo!)
                .Castclass(_type);
        }

        public bool NeedInit()
        {
            return true;
        }

        public void Init(IILGen ilGenerator, Action<IILGen> pushReaderCtx)
        {
            var genericArguments = _type!.GetGenericArguments();
            var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
            var constructorInfo = instanceType.GetConstructor(
                new[] {typeof(IInternalObjectDBTransaction), typeof(ODBDictionaryConfiguration)});
            ilGenerator
                .Do(pushReaderCtx)
                .Castclass(typeof(IDBReaderCtx))
                .Callvirt(() => default(IDBReaderCtx).GetTransaction())
                .LdcI4(_configurationId)
                .Call(() => ODBDictionaryConfiguration.Get(0))
                .Newobj(constructorInfo!)
                .Castclass(_type);
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            ilGenerator
                .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                .Callvirt(() => default(AbstractBufferedReader).SkipVUInt64());
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            var genericArguments = _type!.GetGenericArguments();
            var instanceType = typeof(ODBSet<>).MakeGenericType(genericArguments);
            ilGenerator
                .Do(pushWriterOrCtx)
                .Do(pushValue)
                .LdcI4(_configurationId)
                .Call(instanceType.GetMethod(nameof(ODBDictionary<int, int>.DoSave))!);
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler)
        {
            if (_type != type)
                GenerateType(type);
            if (_type == type) return this;
            if (!IsCompatibleWithCore(type)) return this;
            var arguments = type.GetGenericArguments();
            var wantedKeyHandler = default(IFieldHandler);
            if (typeHandler is ODBSetFieldHandler dictTypeHandler)
            {
                wantedKeyHandler = dictTypeHandler._keysHandler;
            }

            var specializedKeyHandler = _keysHandler.SpecializeLoadForType(arguments[0], wantedKeyHandler);
            if (wantedKeyHandler == specializedKeyHandler)
            {
                return typeHandler;
            }

            var res = new ODBSetFieldHandler(_odb, _configuration, specializedKeyHandler);
            res.GenerateType(type);
            return res;
        }

        Type GenerateType(Type? compatibleWith)
        {
            if (compatibleWith != null && compatibleWith.GetGenericTypeDefinition() == typeof(IOrderedSet<>))
            {
                return _type = typeof(IOrderedSet<>).MakeGenericType(_keysHandler.HandledType());
            }

            return _type = typeof(ISet<>).MakeGenericType(_keysHandler.HandledType());
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            if (_type != type)
                GenerateType(type);
            return this;
        }

        public IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers()
        {
            yield return _keysHandler;
        }

        public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            var fakeMethod = ILBuilder.Instance.NewMethod<Action>("Relation_fake");
            var fakeGenerator = fakeMethod.Generator;
            if (_keysHandler.FreeContent(fakeGenerator, _ => { }) == NeedsFreeContent.Yes)
                throw new BTDBException("Not supported 'free content' in IOrderedSet");
            ilGenerator
                .Do(pushReaderOrCtx)
                .Castclass(typeof(IDBReaderCtx))
                .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                .Callvirt(() => default(AbstractBufferedReader).ReadVUInt64())
                .Callvirt(() => default(IDBReaderCtx).RegisterDict(0ul));
            return NeedsFreeContent.Yes;
        }
    }
}
