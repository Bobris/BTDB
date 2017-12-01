using System;
using System.Collections.Generic;
using System.Diagnostics;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler
{
    public class NullableFieldHandler : IFieldHandler, IFieldHandlerWithNestedFieldHandlers
    {
        readonly IFieldHandlerFactory _fieldHandlerFactory;
        readonly ITypeConvertorGenerator _typeConvertorGenerator;
        readonly byte[] _configuration;
        readonly IFieldHandler _itemHandler;
        Type _type;

        public NullableFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator, Type type)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertorGenerator = typeConvertorGenerator;
            _type = type;
            _itemHandler = _fieldHandlerFactory.CreateFromType(type.GetGenericArguments()[0], FieldHandlerOptions.None);
            if (_itemHandler.NeedsCtx())
                throw new NotSupportedException("Nullable complex types are not supported.");
            var writer = new ByteBufferWriter();
            writer.WriteFieldHandler(_itemHandler);
            _configuration = writer.Data.ToByteArray();
        }

        public NullableFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator, byte[] configuration)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertorGenerator = typeConvertorGenerator;
            _configuration = configuration;
            var reader = new ByteArrayReader(configuration);
            _itemHandler = _fieldHandlerFactory.CreateFromReader(reader, FieldHandlerOptions.None);
        }

        NullableFieldHandler(IFieldHandlerFactory fieldHandlerFactory, ITypeConvertorGenerator typeConvertorGenerator, Type type, IFieldHandler itemSpecialized)
        {
            _fieldHandlerFactory = fieldHandlerFactory;
            _typeConvertorGenerator = typeConvertorGenerator;
            _type = type;
            _itemHandler = itemSpecialized;
        }

        public static string HandlerName => "Nullable";

        public string Name => HandlerName;

        public byte[] Configuration => _configuration;

        public static bool IsCompatibleWith(Type type)
        {
            if (!type.IsGenericType) return false;
            return type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return IsCompatibleWith(type);
        }

        public Type HandledType()
        {
            return _type ?? (_type = typeof(Nullable<>).MakeGenericType(_itemHandler.HandledType()));
        }

        public bool NeedsCtx()
        {
            return _itemHandler.NeedsCtx();
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            var localResult = ilGenerator.DeclareLocal(HandledType());
            var finish = ilGenerator.DefineLabel();
            var noValue = ilGenerator.DefineLabel();
            var itemType = _type.GetGenericArguments()[0];
            var nullableType = typeof(Nullable<>).MakeGenericType(itemType);

            ilGenerator
                .Do(pushReaderOrCtx)
                .Callvirt(() => default(AbstractBufferedReader).ReadBool())
                .Brfalse(noValue);
            _itemHandler.Load(ilGenerator, pushReaderOrCtx);
            _typeConvertorGenerator.GenerateConversion(_itemHandler.HandledType(), itemType)(ilGenerator);
            ilGenerator
                .Newobj(nullableType.GetConstructor(new[] { itemType }))
                .Stloc(localResult)
                .BrS(finish)
                .Mark(noValue)
                .Ldloca(localResult)
                .InitObj(nullableType)
                .Mark(finish)
                .Ldloc(localResult);
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            var finish = ilGenerator.DefineLabel();
            ilGenerator
                .Do(pushReaderOrCtx)
                .Callvirt(() => default(AbstractBufferedReader).ReadBool())
                .Brfalse(finish);
            _itemHandler.Skip(ilGenerator, pushReaderOrCtx);
            ilGenerator
                .Mark(finish);
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriterOrCtx, Action<IILGen> pushValue)
        {
            var nullableType = typeof(Nullable<>).MakeGenericType(_type.GetGenericArguments()[0]);
            var localValue = ilGenerator.DeclareLocal(nullableType);
            var localHasValue = ilGenerator.DeclareLocal(typeof(bool));
            var finish = ilGenerator.DefineLabel();
            ilGenerator
                .Do(pushValue)
                .Stloc(localValue)
                .Do(pushWriterOrCtx)
                .Ldloca(localValue) //Ldloca for struct!
                .Call(nullableType.GetMethod("get_HasValue"))
                .Dup()
                .Stloc(localHasValue)
                .Callvirt(() => default(AbstractBufferedWriter).WriteBool(false))
                .Ldloc(localHasValue)
                .Brfalse(finish);
            _itemHandler.Save(ilGenerator, pushWriterOrCtx,
                il => il
                    .Ldloca(localValue).Call(_type.GetMethod("get_Value"))
                    .Do(_typeConvertorGenerator.GenerateConversion(_type.GetGenericArguments()[0], _itemHandler.HandledType())));
            ilGenerator.Mark(finish);
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler typeHandler)
        {
            if (_type == type) return this;
            if (!IsCompatibleWith(type))
            {
                Debug.Fail("strange");
                return this;
            }
            var wantedItemType = type.GetGenericArguments()[0];
            var wantedItemHandler = default(IFieldHandler);
            var nullableFieldHandler = typeHandler as NullableFieldHandler;
            if (nullableFieldHandler != null)
            {
                wantedItemHandler = nullableFieldHandler._itemHandler;
            }
            var itemSpecialized = _itemHandler.SpecializeLoadForType(wantedItemType, wantedItemHandler);
            if (itemSpecialized == wantedItemHandler)
            {
                return typeHandler;
            }
            if (_typeConvertorGenerator.GenerateConversion(itemSpecialized.HandledType(), wantedItemType) == null)
            {
                Debug.Fail("even more strange");
                return this;
            }
            return new NullableFieldHandler(_fieldHandlerFactory, _typeConvertorGenerator, type, itemSpecialized);
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
            var itemSpecialized = _itemHandler.SpecializeSaveForType(wantedItemType);
            if (_typeConvertorGenerator.GenerateConversion(wantedItemType, itemSpecialized.HandledType()) == null)
            {
                Debug.Fail("even more strange");
                return this;
            }
            return new NullableFieldHandler(_fieldHandlerFactory, _typeConvertorGenerator, type, itemSpecialized);
        }

        public IEnumerable<IFieldHandler> EnumerateNestedFieldHandlers()
        {
            yield return _itemHandler;
        }

        public bool FreeContent(IILGen ilGenerator, Action<IILGen> pushReaderOrCtx)
        {
            Skip(ilGenerator, pushReaderOrCtx);
            return false;
        }
    }
}