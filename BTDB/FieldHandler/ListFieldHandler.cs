using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class ListFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers
    {
        readonly IFieldHandlerFactory _fieldHandlerFactory;
        readonly ITypeConvertorGenerator _typeConvertGenerator;
        readonly IFieldHandler _itemsHandler;
        Type? _type;
        readonly bool _isSet;

        public ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertGenerator, Type type)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertGenerator = typeConvertGenerator;
            _type = type;
            _isSet = type.InheritsOrImplements(typeof(ISet<>));
            _itemsHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0], FieldHandlerOptions.None);
            var writer = new ByteBufferWriter();
            writer.WriteFieldHandler(_itemsHandler);
            Configuration = writer.Data.ToByteArray();
        }

        public ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertGenerator, byte[] configuration)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertGenerator = typeConvertGenerator;
            Configuration = configuration;
            var reader = new ByteArrayReader(configuration);
            _itemsHandler = _fieldHandlerFactory.CreateFromReader(reader, FieldHandlerOptions.None);
        }

        ListFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertGenerator, Type type, IFieldHandler itemSpecialized)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertGenerator = typeConvertGenerator;
            _type = type;
            _isSet = type.InheritsOrImplements(typeof(ISet<>));
            _itemsHandler = itemSpecialized;
            Configuration = Array.Empty<byte>();
        }

        public static string HandlerName => "List";

        public string Name => HandlerName;

        public byte[] Configuration { get; }

        public static bool IsCompatibleWith(Type type)
        {
            if (!type.IsGenericType) return false;
            return type.InheritsOrImplements(typeof(IList<>)) || type.InheritsOrImplements( typeof(ISet<>));
        }

        public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return IsCompatibleWith(type);
        }

        public Type HandledType()
        {
            if (_isSet)
                return _type ??= typeof(ISet<>).MakeGenericType(_itemsHandler.HandledType());
            return _type ??= typeof(IList<>).MakeGenericType(_itemsHandler.HandledType());
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
            var collectionInterface = _type!.SpecializationOf(typeof(ICollection<>));
            var itemType = collectionInterface!.GetGenericArguments()[0];
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
                .Newobj((_isSet?typeof(HashSet<>):typeof(List<>)).MakeGenericType(itemType).GetConstructor(new[] { typeof(int) })!)
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
                .GenerateLoad(_itemsHandler, itemType, pushReaderOrCtx, _typeConvertGenerator)
                .Callvirt(collectionInterface!.GetMethod("Add")!)
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
            var realFinish = ilGenerator.DefineLabel();
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            var localValue = ilGenerator.DeclareLocal(_type!);
            var typeAsICollection = _type.GetInterface("ICollection`1");
            var typeAsIEnumerable = _type.GetInterface("IEnumerable`1");
            var getEnumeratorMethod = typeAsIEnumerable!.GetMethod("GetEnumerator");
            var typeAsIEnumerator = getEnumeratorMethod!.ReturnType;
            var localEnumerator = ilGenerator.DeclareLocal(typeAsIEnumerator);
            ilGenerator
                .Do(pushValue)
                .Stloc(localValue)
                .Do(pushWriterOrCtx)
                .Ldloc(localValue)
                .Castclass(typeof(object))
                .Callvirt(() => default(IWriterCtx).WriteObject(null))
                .Brfalse(realFinish)
                .Do(Extensions.PushWriterFromCtx(pushWriterOrCtx))
                .Ldloc(localValue)
                .Callvirt(typeAsICollection!.GetProperty("Count")!.GetGetMethod()!)
                .ConvU4()
                .Callvirt(() => default(AbstractBufferedWriter).WriteVUInt32(0))
                .Ldloc(localValue)
                .Callvirt(getEnumeratorMethod)
                .Stloc(localEnumerator)
                .Try()
                .Mark(next)
                .Ldloc(localEnumerator)
                .Callvirt(() => default(IEnumerator).MoveNext())
                .Brfalse(finish);
            _itemsHandler.Save(ilGenerator, Extensions.PushWriterOrCtxAsNeeded(pushWriterOrCtx, _itemsHandler.NeedsCtx()), il => il
                .Ldloc(localEnumerator)
                .Callvirt(typeAsIEnumerator.GetProperty("Current")!.GetGetMethod()!)
                .Do(_typeConvertGenerator.GenerateConversion(_type.GetGenericArguments()[0], _itemsHandler.HandledType())!));
            ilGenerator
                .Br(next)
                .Mark(finish)
                .Finally()
                .Ldloc(localEnumerator)
                .Callvirt(() => default(IDisposable).Dispose())
                .EndTry()
                .Mark(realFinish);
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler)
        {
            if (_type == type) return this;
            if (!IsCompatibleWith(type))
            {
                Debug.Fail("strange");
                return this;
            }
            var wantedItemType = type.GetGenericArguments()[0];
            var wantedItemHandler = default(IFieldHandler);
            if (typeHandler is ListFieldHandler listFieldHandler)
            {
                wantedItemHandler = listFieldHandler._itemsHandler;
            }
            var itemSpecialized = _itemsHandler.SpecializeLoadForType(wantedItemType, wantedItemHandler);
            if (itemSpecialized == wantedItemHandler)
            {
                return typeHandler;
            }
            if (_typeConvertGenerator.GenerateConversion(itemSpecialized.HandledType(), wantedItemType) == null)
            {
                Debug.Fail("even more strange");
                return this;
            }
            return new ListFieldHandler(_fieldHandlerFactory, _typeConvertGenerator, type, itemSpecialized);
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
            if (_typeConvertGenerator.GenerateConversion(wantedItemType, itemSpecialized.HandledType()) == null)
            {
                Debug.Fail("even more strange");
                return this;
            }
            return new ListFieldHandler(_fieldHandlerFactory, _typeConvertGenerator, type, itemSpecialized);
        }

        public IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers()
        {
            yield return _itemsHandler;
        }

        public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            var localCount = ilGenerator.DeclareLocal(typeof(uint));
            var finish = ilGenerator.DefineLabel();
            var next = ilGenerator.DefineLabel();
            var needsFreeContent = NeedsFreeContent.No;
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
                .GenerateFreeContent(_itemsHandler, pushReaderOrCtx, ref needsFreeContent)
                .Br(next)
                .Mark(finish);
            return needsFreeContent;
        }
    }
}
