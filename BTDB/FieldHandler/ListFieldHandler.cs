using System;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class ListFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers
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
            _itemsHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0], FieldHandlerOptions.None);
            var writer = new ByteBufferWriter();
            writer.WriteFieldHandler(_itemsHandler);
            _configuration = writer.Data.ToByteArray();
        }

        public ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator, byte[] configuration)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertorGenerator = typeConvertorGenerator;
            _configuration = configuration;
            var reader = new ByteArrayReader(configuration);
            _itemsHandler = _fieldHandlerFactory.CreateFromReader(reader);
        }

        ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator, Type type, IFieldHandler itemSpecialized)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertorGenerator = typeConvertorGenerator;
            _type = type;
            _itemsHandler = itemSpecialized;
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
            return type.GetGenericTypeDefinition() == typeof(IList<>) || type.GetGenericTypeDefinition() == typeof(List<>);
        }

        bool IFieldHandler.IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return IsCompatibleWith(type);
        }

        public Type HandledType()
        {
            return _type ?? (_type = typeof(IList<>).MakeGenericType(_itemsHandler.HandledType()));
        }

        public bool NeedsCtx()
        {
            return true;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            var localCount = ilGenerator.DeclareLocal(typeof(uint));
            var localResultOfObject = ilGenerator.DeclareLocal(typeof(object));
            var localResult = ilGenerator.DeclareLocal(HandledType());
            var loadSkipped = ilGenerator.DefineLabel();
            var loadFinished = ilGenerator.DefineLabel();
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            object fake;
            ilGenerator
                .Do(pushReaderOrCtx)
                .Ldloca(localResultOfObject)
                .Callvirt(() => default(IReaderCtx).ReadObject(out fake))
                .Brfalse(loadSkipped)
                .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                .Callvirt(() => default(AbstractBufferedReader).ReadVUInt32())
                .Stloc(localCount)
                .Ldloc(localCount)
                .Newobj(typeof(List<>).MakeGenericType(_type.GetGenericArguments()[0]).GetConstructor(new[] { typeof(int) }))
                .Stloc(localResult)
                .Do(pushReaderOrCtx)
                .Ldloc(localResult)
                .Castclass(typeof(object))
                .Callvirt(() => default(IReaderCtx).RegisterObject(null))
                .Mark(next)
                .Ldloc(localCount)
                .Brfalse(loadFinished)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .ConvU4()
                .Stloc(localCount)
                .Ldloc(localResult)
                .GenerateLoad(_itemsHandler, _type.GetGenericArguments()[0], pushReaderOrCtx, _typeConvertorGenerator)
                .Callvirt(_type.GetInterface("ICollection`1").GetMethod("Add"))
                .Br(next)
                .Mark(loadFinished)
                .Do(pushReaderOrCtx)
                .Callvirt(() => default(IReaderCtx).ReadObjectDone())
                .Br(finish)
                .Mark(loadSkipped)
                .Ldloc(localResultOfObject)
                .Isinst(_type)
                .Stloc(localResult)
                .Mark(finish)
                .Ldloc(localResult);
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            var localCount = ilGenerator.DeclareLocal(typeof(uint));
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            ilGenerator
                .Do(pushReaderOrCtx)
                .Callvirt(() => default(IReaderCtx).SkipObject())
                .Brfalse(finish)
                .Do(Extensions.PushReaderFromCtx(pushReaderOrCtx))
                .Callvirt(() => default(AbstractBufferedReader).ReadVUInt32())
                .Stloc(localCount)
                .Mark(next)
                .Ldloc(localCount)
                .Brfalse(finish)
                .Ldloc(localCount)
                .LdcI4(1)
                .Sub()
                .ConvU4()
                .Stloc(localCount)
                .GenerateSkip(_itemsHandler, pushReaderOrCtx)
                .Br(next)
                .Mark(finish);
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            var localValue = ilGenerator.DeclareLocal(_type);
            var localIndex = ilGenerator.DeclareLocal(typeof(int));
            var localCount = ilGenerator.DeclareLocal(typeof(int));
            ilGenerator
                .Do(pushValue)
                .Stloc(localValue)
                .Do(pushWriterOrCtx)
                .Ldloc(localValue)
                .Castclass(typeof(object))
                .Callvirt(() => default(IWriterCtx).WriteObject(null))
                .Brfalse(finish)
                .Ldloc(localValue)
                .Callvirt(_type.GetInterface("ICollection`1").GetProperty("Count").GetGetMethod())
                .Stloc(localCount)
                .Do(Extensions.PushWriterFromCtx(pushWriterOrCtx))
                .Ldloc(localCount)
                .ConvU4()
                .Callvirt(() => default(AbstractBufferedWriter).WriteVUInt32(0))
                .Mark(next)
                .Ldloc(localIndex)
                .Ldloc(localCount)
                .BgeUn(finish);
            _itemsHandler.Save(ilGenerator, Extensions.PushWriterOrCtxAsNeeded(pushWriterOrCtx, _itemsHandler.NeedsCtx()), il => il
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
                .Mark(finish);
        }

        public IFieldHandler SpecializeLoadForType(Type type)
        {
            if (_type == type) return this;
            if (!IsCompatibleWith(type))
            {
                Debug.Fail("strange");
                return this;
            }
            var wantedItemType = type.GetGenericArguments()[0];
            var itemSpecialized = _itemsHandler.SpecializeLoadForType(wantedItemType);
            if (_typeConvertorGenerator.GenerateConversion(itemSpecialized.HandledType(), wantedItemType) == null)
            {
                Debug.Fail("even more strange");
                return this;
            }
            return new ListFieldHandler(_fieldHandlerFactory, _typeConvertorGenerator, type, itemSpecialized);
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            if (_type == type) return this;
            if (!IsCompatibleWith(type))
            {
                Debug.Fail("strange");
                return this;
            }
            var wantedItemType = type.GetGenericArguments()[0];
            var itemSpecialized = _itemsHandler.SpecializeSaveForType(wantedItemType);
            if (_typeConvertorGenerator.GenerateConversion(wantedItemType, itemSpecialized.HandledType()) == null)
            {
                Debug.Fail("even more strange");
                return this;
            }
            return new ListFieldHandler(_fieldHandlerFactory, _typeConvertorGenerator, type, itemSpecialized);
        }

        public IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers()
        {
            yield return _itemsHandler;
        }
    }
}