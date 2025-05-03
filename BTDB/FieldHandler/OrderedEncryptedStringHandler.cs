using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB.Encrypted;
using BTDB.IL;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class OrderedEncryptedStringHandler : IFieldHandler
{
    public string Name => "OrderedEncryptedString";

    public byte[]? Configuration => null;

    public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return options.HasFlag(FieldHandlerOptions.Orderable) && typeof(EncryptedString) == type;
    }

    public Type HandledType()
    {
        return typeof(EncryptedString);
    }

    public bool NeedsCtx()
    {
        return true;
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        pushCtx(ilGenerator);
        pushReader(ilGenerator);
        ilGenerator.Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.ReadOrderedEncryptedString))!);
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        pushCtx(ilGenerator);
        pushReader(ilGenerator);
        ilGenerator.Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.SkipOrderedEncryptedString))!);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
    {
        pushCtx(ilGenerator);
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Callvirt(typeof(IWriterCtx).GetMethod(nameof(IWriterCtx.WriteOrderedEncryptedString))!);
    }

    public FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (asType == typeof(EncryptedString) || asType == typeof(string))
        {
            return static (ref MemReader reader, IReaderCtx? ctx, ref byte value) =>
            {
                Unsafe.As<byte, EncryptedString>(ref value) = ctx!.ReadOrderedEncryptedString(ref reader);
            };
        }

        return this.BuildConvertingLoader(typeof(EncryptedString), asType, typeConverterFactory);
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        ctx!.SkipOrderedEncryptedString(ref reader);
    }

    public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (asType == typeof(EncryptedString) || asType == typeof(string))
        {
            return static (ref MemWriter writer, IWriterCtx? ctx, ref byte value) =>
            {
                ctx!.WriteOrderedEncryptedString(ref writer, Unsafe.As<byte, EncryptedString>(ref value));
            };
        }

        return this.BuildConvertingSaver(asType, typeof(EncryptedString), typeConverterFactory);
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (HandledType() == type ||
            DefaultTypeConvertorGenerator.Instance.GenerateConversion(typeof(EncryptedString), type) == null)
        {
            return this;
        }

        return new ConvertingHandler(this, type);
    }

    public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        ctx!.SkipOrderedEncryptedString(ref reader);
    }

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes) => false;

    public class ConvertingHandler : IFieldHandler
    {
        readonly IFieldHandler _fieldHandler;
        readonly Type _type;

        public ConvertingHandler(IFieldHandler fieldHandler, Type type)
        {
            _fieldHandler = fieldHandler;
            _type = type;
        }

        public string Name => _fieldHandler.Name;
        public byte[]? Configuration => _fieldHandler.Configuration;

        public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return _type == type;
        }

        public Type HandledType()
        {
            return _type;
        }

        public bool NeedsCtx() => true;

        public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            _fieldHandler.Load(ilGenerator, pushReader, pushCtx);
            DefaultTypeConvertorGenerator.Instance.GenerateConversion(_fieldHandler.HandledType(), _type)!(ilGenerator);
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            _fieldHandler.Skip(ilGenerator, pushReader, pushCtx);
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx,
            Action<IILGen> pushValue)
        {
            _fieldHandler.Save(ilGenerator, pushWriter, pushCtx,
                il => il.Do(pushValue)
                    .Do(DefaultTypeConvertorGenerator.Instance.GenerateConversion(_type,
                        _fieldHandler.HandledType())!));
        }

        public FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
        {
            throw new InvalidOperationException();
        }

        public void Skip(ref MemReader reader, IReaderCtx? ctx)
        {
            _fieldHandler.Skip(ref reader, ctx);
        }

        public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
        {
            throw new InvalidOperationException();
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
        {
            throw new InvalidOperationException();
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            throw new InvalidOperationException();
        }

        public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
        {
            ctx!.SkipOrderedEncryptedString(ref reader);
        }

        public bool DoesNeedFreeContent(HashSet<Type> visitedTypes) => false;
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        if (HandledType() == type ||
            DefaultTypeConvertorGenerator.Instance.GenerateConversion(type, typeof(EncryptedString)) == null)
        {
            return this;
        }

        return new ConvertingHandler(this, type);
    }
}
