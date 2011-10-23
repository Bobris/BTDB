using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection.Emit;
using BTDB.FieldHandler;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.ODBLayer
{
    public class ODBDictionaryFieldHandler : IFieldHandler
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
            _keysHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0]);
            _valuesHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[1]);
            var writer = new ByteArrayWriter();
            writer.WriteFieldHandler(_keysHandler);
            writer.WriteFieldHandler(_valuesHandler);
            _configuration = writer.Data;
            CreateConfiguration();
        }

        public ODBDictionaryFieldHandler(IObjectDB odb, byte[] configuration)
        {
            _odb = odb;
            _fieldHandlerFactory = odb.FieldHandlerFactory;
            _typeConvertorGenerator = odb.TypeConvertorGenerator;
            _configuration = configuration;
            var reader = new ByteArrayReader(configuration);
            _keysHandler = _fieldHandlerFactory.CreateFromReader(reader);
            _valuesHandler = _fieldHandlerFactory.CreateFromReader(reader);
            CreateConfiguration();
        }

        void CreateConfiguration()
        {
            HandledType();
            var cfg = new ODBDictionaryConfiguration(_odb, _keysHandler, _valuesHandler);
            cfg.KeyReader = CreateReader(_keysHandler, _type.GetGenericArguments()[0], true);
            cfg.KeyWriter = CreateWriter(_keysHandler, _type.GetGenericArguments()[0], true);
            cfg.ValueReader = CreateReader(_valuesHandler, _type.GetGenericArguments()[1], false);
            cfg.ValueWriter = CreateWriter(_valuesHandler, _type.GetGenericArguments()[1], false);
            _configurationId = ((IInstanceRegistry)_odb).RegisterInstance(cfg);
        }

        object CreateWriter(IFieldHandler fieldHandler, Type realType, bool ordered)
        {
            //Action<T, AbstractBufferedWriter, IWriterCtx>
            var delegateType = typeof(Action<,,>).MakeGenericType(realType, typeof(AbstractBufferedWriter), typeof(IWriterCtx));
            var dm = new DynamicMethodSpecific(fieldHandler.Name + (ordered ? "Ordered" : "") + "Writer", delegateType);
            var ilGenerator = dm.GetILGenerator();
            Action<ILGenerator> pushWriterOrCtx;
            if (fieldHandler.NeedsCtx())
            {
                pushWriterOrCtx = il => il.Ldarg(2);
            }
            else
            {
                pushWriterOrCtx = il => il.Ldarg(1);
            }
            if (ordered && fieldHandler is IFieldHandleOrderable)
            {
                ((IFieldHandleOrderable)fieldHandler).SaveOrdered(ilGenerator, pushWriterOrCtx,
                    il => il.Ldarg(0).Do(_typeConvertorGenerator.GenerateConversion(realType, fieldHandler.HandledType())));
            }
            else
            {
                fieldHandler.Save(ilGenerator, pushWriterOrCtx,
                    il => il.Ldarg(0).Do(_typeConvertorGenerator.GenerateConversion(realType, fieldHandler.HandledType())));
            }
            ilGenerator.Ret();
            return dm.Create();
        }

        object CreateReader(IFieldHandler fieldHandler, Type realType, bool ordered)
        {
            //Func<AbstractBufferedReader, IReaderCtx, T>
            var delegateType = typeof(Func<,,>).MakeGenericType(typeof(AbstractBufferedReader), typeof(IReaderCtx),
                                                                 realType);
            var dm = new DynamicMethodSpecific(fieldHandler.Name + (ordered ? "Ordered" : "") + "Reader", delegateType);
            var ilGenerator = dm.GetILGenerator();
            Action<ILGenerator> pushReaderOrCtx;
            if (fieldHandler.NeedsCtx())
            {
                pushReaderOrCtx = il => il.Ldarg(1);
            }
            else
            {
                pushReaderOrCtx = il => il.Ldarg(0);
            }
            if (ordered && fieldHandler is IFieldHandleOrderable)
            {
                ((IFieldHandleOrderable)fieldHandler).LoadOrdered(ilGenerator, pushReaderOrCtx);
            }
            else
            {
                fieldHandler.Load(ilGenerator, pushReaderOrCtx);
            }
            ilGenerator
                .Do(_typeConvertorGenerator.GenerateConversion(fieldHandler.HandledType(), realType))
                .Ret();
            return dm.Create();
        }

        public static string HandlerName
        {
            get { return "ODBDictionary"; }
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
            return _type ?? (_type = typeof(IDictionary<,>).MakeGenericType(_keysHandler.HandledType(), _valuesHandler.HandledType()));
        }

        public bool NeedsCtx()
        {
            return true;
        }

        public void Load(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            var genericArguments = _type.GetGenericArguments();
            var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
            var constructorInfo = instanceType.GetConstructor(
                new[] { typeof(IInternalObjectDBTransaction), typeof(ODBDictionaryConfiguration), typeof(ulong) });
            ilGenerator
                .Do(pushReaderOrCtx)
                .Castclass(typeof(IDBReaderCtx))
                .Callvirt(() => ((IDBReaderCtx)null).GetTransaction())
                .Do(pushReaderOrCtx)
                .Castclass(typeof(IDBReaderCtx))
                .LdcI4(_configurationId)
                .Callvirt(() => ((IDBReaderCtx)null).FindInstance(0))
                .Castclass(typeof(ODBDictionaryConfiguration))
                .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                .Callvirt(() => ((AbstractBufferedReader)null).ReadVUInt64())
                .Newobj(constructorInfo)
                .Castclass(_type);
        }

        public void Skip(ILGenerator ilGenerator, Action<ILGenerator> pushReaderOrCtx)
        {
            // TODO register dict id for deletion
            ilGenerator
                .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                .Callvirt(() => ((AbstractBufferedReader)null).SkipVUInt64());
        }

        public void Save(ILGenerator ilGenerator, Action<ILGenerator> pushWriterOrCtx, Action<ILGenerator> pushValue)
        {
            var genericArguments = _type.GetGenericArguments();
            var instanceType = typeof(ODBDictionary<,>).MakeGenericType(genericArguments);
            ilGenerator
                .Do(pushWriterOrCtx)
                .Do(pushValue)
                .LdcI4(_configurationId)
                .Call(instanceType.GetMethod("DoSave"));
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
