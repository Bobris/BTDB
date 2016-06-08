using System;
using System.Collections.Generic;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class ODBDictionaryFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers, IFieldHandlerWithInit
    {
        readonly IObjectDB _odb;
        readonly IFieldHandlerFactory _fieldHandlerFactory;
        readonly ITypeConvertorGenerator _typeConvertorGenerator;
        readonly byte[] _configuration;
        readonly IFieldHandler _keysHandler;
        readonly IFieldHandler _valuesHandler;
        int _configurationId;
        Type _type;

        public ODBDictionaryFieldHandler(IObjectDB odb, Type type)
        {
            _odb = odb;
            _fieldHandlerFactory = odb.FieldHandlerFactory;
            _typeConvertorGenerator = odb.TypeConvertorGenerator;
            _type = type;
            _keysHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0], FieldHandlerOptions.Orderable | FieldHandlerOptions.AtEndOfStream);
            _valuesHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[1], FieldHandlerOptions.None);
            var writer = new ByteBufferWriter();
            writer.WriteFieldHandler(_keysHandler);
            writer.WriteFieldHandler(_valuesHandler);
            _configuration = writer.Data.ToByteArray();
            CreateConfiguration();
        }

        public ODBDictionaryFieldHandler(IObjectDB odb, byte[] configuration)
        {
            _odb = odb;
            _fieldHandlerFactory = odb.FieldHandlerFactory;
            _typeConvertorGenerator = odb.TypeConvertorGenerator;
            _configuration = configuration;
            var reader = new ByteArrayReader(configuration);
            _keysHandler = _fieldHandlerFactory.CreateFromReader(reader, FieldHandlerOptions.Orderable | FieldHandlerOptions.AtEndOfStream);
            _valuesHandler = _fieldHandlerFactory.CreateFromReader(reader, FieldHandlerOptions.None);
            CreateConfiguration();
        }

        ODBDictionaryFieldHandler(IObjectDB odb, byte[] configuration, int configurationId, IFieldHandler specializedKeyHandler, IFieldHandler specializedValueHandler)
        {
            _odb = odb;
            _fieldHandlerFactory = odb.FieldHandlerFactory;
            _typeConvertorGenerator = odb.TypeConvertorGenerator;
            _configuration = configuration;
            _keysHandler = specializedKeyHandler;
            _valuesHandler = specializedValueHandler;
            CreateConfiguration();
        }

        void CreateConfiguration()
        {
            HandledType();
            var cfg = new ODBDictionaryConfiguration(_odb, _keysHandler, _valuesHandler)
                {
                    KeyReader = CreateReader(_keysHandler, _type.GetGenericArguments()[0]),
                    KeyWriter = CreateWriter(_keysHandler, _type.GetGenericArguments()[0]),
                    ValueReader = CreateReader(_valuesHandler, _type.GetGenericArguments()[1]),
                    ValueWriter = CreateWriter(_valuesHandler, _type.GetGenericArguments()[1])
                };
            _configurationId = ((IInstanceRegistry)_odb).RegisterInstance(cfg);
        }

        object CreateWriter(IFieldHandler fieldHandler, Type realType)
        {
            //Action<T, AbstractBufferedWriter, IWriterCtx>
            var delegateType = typeof(Action<,,>).MakeGenericType(realType, typeof(AbstractBufferedWriter), typeof(IWriterCtx));
            var dm = ILBuilder.Instance.NewMethod(fieldHandler.Name + "Writer", delegateType);
            var ilGenerator = dm.Generator;
            Action<IILGen> pushWriterOrCtx = il => il.Ldarg((ushort)(1 + (fieldHandler.NeedsCtx() ? 1 : 0)));
            fieldHandler.Save(ilGenerator, pushWriterOrCtx,
                il => il.Ldarg(0).Do(_typeConvertorGenerator.GenerateConversion(realType, fieldHandler.HandledType())));
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
            Action<IILGen> pushReaderOrCtx = il => il.Ldarg((ushort)(fieldHandler.NeedsCtx() ? 1 : 0));
            fieldHandler.Load(ilGenerator, pushReaderOrCtx);
            ilGenerator
                .Do(_typeConvertorGenerator.GenerateConversion(fieldHandler.HandledType(), realType))
                .Ret();
            return dm.Create();
        }

        public static string HandlerName => "ODBDictionary";

        public string Name => HandlerName;

        public byte[] Configuration => _configuration;

        public static bool IsCompatibleWithStatic(Type type, FieldHandlerOptions options)
        {
            if ((options & FieldHandlerOptions.Orderable) != 0) return false;
            if (!type.IsGenericType) return false;
            return IsCompatibleWithCore(type);
        }

        static bool IsCompatibleWithCore(Type type)
        {
            var genericTypeDefinition = type.GetGenericTypeDefinition();
            return genericTypeDefinition == typeof(IDictionary<,>) || genericTypeDefinition == typeof(IOrderedDictionary<,>);
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
            var genericArguments = _type.GetGenericArguments();
            var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
            var constructorInfo = instanceType.GetConstructor(
                new[] { typeof(IInternalObjectDBTransaction), typeof(ODBDictionaryConfiguration), typeof(ulong) });
            ilGenerator
                .Do(pushReaderOrCtx)
                .Castclass(typeof(IDBReaderCtx))
                .Callvirt(() => default(IDBReaderCtx).GetTransaction())
                .Do(pushReaderOrCtx)
                .Castclass(typeof(IDBReaderCtx))
                .LdcI4(_configurationId)
                .Callvirt(() => default(IDBReaderCtx).FindInstance(0))
                .Castclass(typeof(ODBDictionaryConfiguration))
                .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                .Callvirt(() => default(AbstractBufferedReader).ReadVUInt64())
                .Newobj(constructorInfo)
                .Castclass(_type);
        }

        public bool NeedInit()
        {
            return true;
        }

        public void Init(IILGen ilGenerator, Action<IILGen> pushReaderCtx)
        {
            var genericArguments = _type.GetGenericArguments();
            var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
            var constructorInfo = instanceType.GetConstructor(
                new[] { typeof(IInternalObjectDBTransaction), typeof(ODBDictionaryConfiguration) });
            ilGenerator
                .Do(pushReaderCtx)
                .Castclass(typeof(IDBReaderCtx))
                .Callvirt(() => default(IDBReaderCtx).GetTransaction())
                .Do(pushReaderCtx)
                .Castclass(typeof(IDBReaderCtx))
                .LdcI4(_configurationId)
                .Callvirt(() => default(IDBReaderCtx).FindInstance(0))
                .Castclass(typeof(ODBDictionaryConfiguration))
                .Newobj(constructorInfo)
                .Castclass(_type);
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            // TODO register dict id for deletion
            ilGenerator
                .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                .Callvirt(() => default(AbstractBufferedReader).SkipVUInt64());
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            var genericArguments = _type.GetGenericArguments();
            var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
            ilGenerator
                .Do(pushWriterOrCtx)
                .Do(pushValue)
                .LdcI4(_configurationId)
                .Call(instanceType.GetMethod("DoSave"));
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler typeHandler)
        {
            if (_type != type)
                GenerateType(type);
            if (_type == type) return this;
            if (!IsCompatibleWithCore(type)) return this;
            var arguments = type.GetGenericArguments();
            var wantedKeyHandler = default(IFieldHandler);
            var wantedValueHandler = default(IFieldHandler);
            var dictTypeHandler = typeHandler as ODBDictionaryFieldHandler;
            if (dictTypeHandler != null)
            {
                wantedKeyHandler = dictTypeHandler._keysHandler;
                wantedValueHandler = dictTypeHandler._valuesHandler;
            }
            var specializedKeyHandler = _keysHandler.SpecializeLoadForType(arguments[0], wantedKeyHandler);
            var specializedValueHandler = _valuesHandler.SpecializeLoadForType(arguments[1], wantedValueHandler);
            if (wantedKeyHandler == specializedKeyHandler && (wantedValueHandler == specializedValueHandler || wantedValueHandler.HandledType() == specializedValueHandler.HandledType()))
            {
                return typeHandler;
            }
            return new ODBDictionaryFieldHandler(_odb, _configuration, _configurationId, specializedKeyHandler, specializedValueHandler);
        }

        Type GenerateType(Type compatibleWith)
        {
            if (compatibleWith != null && compatibleWith.GetGenericTypeDefinition() == typeof(IOrderedDictionary<,>))
            {
                return _type = typeof(IOrderedDictionary<,>).MakeGenericType(_keysHandler.HandledType(), _valuesHandler.HandledType());
            }
            return _type = typeof(IDictionary<,>).MakeGenericType(_keysHandler.HandledType(), _valuesHandler.HandledType());
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
            yield return _valuesHandler;
        }

        public bool FreeContent(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            var fakeMethod = ILBuilder.Instance.NewMethod<Action>("Relation_fake");
            var fakeGenerator = fakeMethod.Generator;
            if (_keysHandler.FreeContent(fakeGenerator, _ => { }))
                throw new BTDBException("Not supported IDictionary in IDictionary key");
            var containsNestedIDictionaries = _valuesHandler.FreeContent(fakeGenerator, _ => { });
            if (!containsNestedIDictionaries)
            {
                ilGenerator
                    .Do(pushReaderOrCtx)
                    .Castclass(typeof (IDBReaderCtx))
                    .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                    .Callvirt(() => default(AbstractBufferedReader).ReadVUInt64())
                    .Callvirt(() => default(IDBReaderCtx).RegisterDict(0ul));
            }
            else
            {
                var genericArguments = _type.GetGenericArguments();
                var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);

                var dictId = ilGenerator.DeclareLocal(typeof (ulong));
                ilGenerator
                    .Do(pushReaderOrCtx)
                    .Castclass(typeof(IDBReaderCtx))
                    .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                    .Callvirt(() => default(AbstractBufferedReader).ReadVUInt64())
                    .Stloc(dictId)
                    .Ldloc(dictId)
                    .Callvirt(() => default(IDBReaderCtx).RegisterDict(0ul))
                    .Do(pushReaderOrCtx)
                    .Ldloc(dictId)
                    .LdcI4(_configurationId)
                    //ODBDictionary.DoFreeContent(IReaderCtx ctx, ulong id, int cfgId)
                    .Call(instanceType.GetMethod("DoFreeContent"));
            }
            return true;
        }
    }
}
