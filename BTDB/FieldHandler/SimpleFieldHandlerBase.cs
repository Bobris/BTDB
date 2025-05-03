using System;
using System.Collections.Generic;
using System.Reflection;
using BTDB.IL;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public delegate void SkipReaderCtxFunc(ref MemReader reader, IReaderCtx? ctx);

public class SimpleFieldHandlerBase : IFieldHandler
{
    readonly string _name;
    readonly MethodInfo _loader;
    readonly MethodInfo _skipper;
    readonly MethodInfo _saver;
    readonly SkipReaderCtxFunc _skipReader;
    readonly FieldHandlerLoad _loaderReader;
    readonly FieldHandlerSave _saverWriter;

    public SimpleFieldHandlerBase(string name, MethodInfo loader, MethodInfo skipper, MethodInfo saver,
        SkipReaderCtxFunc skipReader, FieldHandlerLoad loaderReader, FieldHandlerSave saverWriter)
    {
        _name = name;
        _loader = loader;
        _skipper = skipper;
        _saver = saver;
        _skipReader = skipReader;
        _loaderReader = loaderReader;
        _saverWriter = saverWriter;
    }

    public string Name => _name;

    public byte[]? Configuration => null;

    public virtual bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return _loader.ReturnType == type;
    }

    public Type HandledType()
    {
        return _loader.ReturnType;
    }

    public bool NeedsCtx()
    {
        return false;
    }

    public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(_loader);
    }

    public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(_skipper);
    }

    public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx, Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(_saver);
    }

    public FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (asType == HandledType())
        {
            return _loaderReader;
        }

        return this.BuildConvertingLoader(HandledType(), asType, typeConverterFactory);
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        _skipReader(ref reader, ctx);
    }

    public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (asType == HandledType())
        {
            return _saverWriter;
        }

        return this.BuildConvertingSaver(asType, HandledType(), typeConverterFactory);
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (HandledType() == type || !IsCompatibleWith(type, FieldHandlerOptions.None))
        {
            return this;
        }

        return new ConvertingHandler(this, type);
    }

    public void FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        Skip(ilGenerator, pushReader, pushCtx);
    }

    public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        _skipReader(ref reader, ctx);
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
            return false;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
        {
            _fieldHandler.Load(ilGenerator, pushReader, pushCtx);
            DefaultTypeConvertorGenerator.Instance.GenerateConversion(_fieldHandler.HandledType(), _type)!(ilGenerator);
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
        {
            _fieldHandler.Skip(ilGenerator, pushReader, pushCtx);
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx,
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
            return this;
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            return this;
        }

        public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
        {
            _fieldHandler.Skip(ref reader, ctx);
        }

        public bool DoesNeedFreeContent(HashSet<Type> visitedTypes) => false;
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        if (HandledType() == type || !IsCompatibleWith(type, FieldHandlerOptions.None))
        {
            return this;
        }

        return new ConvertingHandler(this, type);
    }
}
