using System;
using BTDB.Buffer;
using BTDB.IL;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class ByteArrayFieldHandler : IFieldHandler
{
    public virtual string Name => "Byte[]";

    public byte[]? Configuration => null;

    public virtual bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return typeof(byte[]) == type || typeof(ByteBuffer) == type;
    }

    public Type HandledType()
    {
        return typeof(byte[]);
    }

    public bool NeedsCtx()
    {
        return false;
    }

    public virtual void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(typeof(SpanReader).GetMethod(nameof(SpanReader.ReadByteArray))!);
    }

    public virtual void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(typeof(SpanReader).GetMethod(nameof(SpanReader.SkipByteArray))!);
    }

    public virtual void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx,
        Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteByteArray), new[] { typeof(byte[]) })!);
    }

    protected virtual void SaveByteBuffer(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(
            typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteByteArray), new[] { typeof(ByteBuffer) })!);
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (typeof(ByteBuffer) == type)
        {
            return new ByteBufferHandler(this);
        }

        return this;
    }

    public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        Skip(ilGenerator, pushReader, pushCtx);
        return NeedsFreeContent.No;
    }

    class ByteBufferHandler : IFieldHandler
    {
        readonly ByteArrayFieldHandler _fieldHandler;

        public ByteBufferHandler(ByteArrayFieldHandler fieldHandler)
        {
            _fieldHandler = fieldHandler;
        }

        public string Name => _fieldHandler.Name;
        public byte[]? Configuration => _fieldHandler.Configuration;

        public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return typeof(ByteBuffer) == type;
        }

        public Type HandledType()
        {
            return typeof(ByteBuffer);
        }

        public bool NeedsCtx()
        {
            return false;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
        {
            _fieldHandler.Load(ilGenerator, pushReader, pushCtx);
            ilGenerator.Call(() => ByteBuffer.NewAsync(null));
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
        {
            _fieldHandler.Skip(ilGenerator, pushReader, pushCtx);
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx,
            Action<IILGen> pushValue)
        {
            _fieldHandler.SaveByteBuffer(ilGenerator, pushWriter, pushValue);
        }

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
        {
            throw new InvalidOperationException();
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            throw new InvalidOperationException();
        }

        public NeedsFreeContent FreeContent(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
        {
            _fieldHandler.Skip(ilGenerator, pushReader, pushCtx);
            return NeedsFreeContent.No;
        }
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        if (typeof(ByteBuffer) == type)
        {
            return new ByteBufferHandler(this);
        }

        return this;
    }
}
