using System;
using System.Collections.Generic;
using BTDB.Encrypted;
using BTDB.IL;

namespace BTDB.FieldHandler;

public class EncryptedStringHandler : IFieldHandler
{
    public string Name => "EncryptedString";

    public byte[]? Configuration => null;

    public virtual bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return typeof(EncryptedString) == type;
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
        ilGenerator.Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.ReadEncryptedString))!);
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        pushCtx(ilGenerator);
        pushReader(ilGenerator);
        ilGenerator.Callvirt(typeof(IReaderCtx).GetMethod(nameof(IReaderCtx.SkipEncryptedString))!);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushCtx, Action<IILGen> pushValue)
    {
        pushCtx(ilGenerator);
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Callvirt(typeof(IWriterCtx).GetMethod(nameof(IWriterCtx.WriteEncryptedString))!);
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

    public void FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
    {
        Skip(ilGenerator, pushReader, pushCtx);
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

        public bool NeedsCtx()
        {
            return true;
        }

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

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
        {
            throw new InvalidOperationException();
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            throw new InvalidOperationException();
        }

        public void FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen> pushCtx)
        {
            _fieldHandler.Skip(ilGenerator, pushReader, pushCtx);
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
