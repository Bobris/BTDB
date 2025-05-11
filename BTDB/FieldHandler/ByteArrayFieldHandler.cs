using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB.Buffer;
using BTDB.IL;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class ByteArrayFieldHandler : IFieldHandler
{
    public virtual string Name => "Byte[]";

    public byte[]? Configuration => null;

    public virtual bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return typeof(byte[]) == type || typeof(ByteBuffer) == type || typeof(ReadOnlyMemory<byte>) == type;
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
        ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadByteArray))!);
    }

    public virtual void LoadByteBuffer(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadByteArray))!);
        ilGenerator.Call(() => ByteBuffer.NewAsync(null));
    }

    public virtual void LoadReadOnlyMemory(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadByteArrayAsMemory))!);
    }

    public virtual void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.SkipByteArray))!);
    }

    public virtual void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx,
        Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteByteArray), new[] { typeof(byte[]) })!);
    }

    public virtual FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (asType == typeof(byte[]))
        {
            return static (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, byte[]>(ref value) = reader.ReadByteArray();
            };
        }

        if (asType == typeof(ByteBuffer))
        {
            return static (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, ByteBuffer>(ref value) = ByteBuffer.NewAsync(reader.ReadByteArray()!);
            };
        }

        if (asType == typeof(ReadOnlyMemory<byte>))
        {
            return static (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, ReadOnlyMemory<byte>>(ref value) = reader.ReadByteArrayAsMemory();
            };
        }

        return this.BuildConvertingLoader(typeof(byte[]), asType, typeConverterFactory);
    }

    public void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        reader.SkipByteArray();
    }

    public virtual FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (asType == typeof(byte[]))
        {
            return static (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteByteArray(Unsafe.As<byte, byte[]>(ref value));
            };
        }

        if (asType == typeof(ByteBuffer))
        {
            return static (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteByteArray(Unsafe.As<byte, ByteBuffer>(ref value));
            };
        }

        if (asType == typeof(ReadOnlyMemory<byte>))
        {
            return static (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteByteArray(Unsafe.As<byte, ReadOnlyMemory<byte>>(ref value));
            };
        }

        return this.BuildConvertingSaver(typeof(byte[]), asType, typeConverterFactory);
    }

    protected virtual void SaveByteBuffer(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteByteArray), new[] { typeof(ByteBuffer) })!);
    }

    protected virtual void SaveReadOnlyMemory(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(
            typeof(MemWriter).GetMethod(nameof(MemWriter.WriteByteArray), new[] { typeof(ReadOnlyMemory<byte>) })!);
    }

    public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
    {
        if (typeof(ByteBuffer) == type)
        {
            return new ByteBufferHandler(this);
        }

        if (typeof(ReadOnlyMemory<byte>) == type)
        {
            return new ReadOnlyMemoryHandler(this);
        }

        return this;
    }

    public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
    {
        reader.SkipByteArray();
    }

    public bool DoesNeedFreeContent(HashSet<Type> visitedTypes) => false;

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
            _fieldHandler.LoadByteBuffer(ilGenerator, pushReader, pushCtx);
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

        public FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
        {
            throw new InvalidOperationException();
        }

        public void Skip(ref MemReader reader, IReaderCtx? ctx)
        {
            reader.SkipByteArray();
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
            reader.SkipByteArray();
        }

        public bool DoesNeedFreeContent(HashSet<Type> visitedTypes) => false;
    }

    class ReadOnlyMemoryHandler : IFieldHandler
    {
        readonly ByteArrayFieldHandler _fieldHandler;

        public ReadOnlyMemoryHandler(ByteArrayFieldHandler fieldHandler)
        {
            _fieldHandler = fieldHandler;
        }

        public string Name => _fieldHandler.Name;
        public byte[]? Configuration => _fieldHandler.Configuration;

        public bool IsCompatibleWith(Type type, FieldHandlerOptions options)
        {
            return typeof(ReadOnlyMemory<byte>) == type;
        }

        public Type HandledType()
        {
            return typeof(ReadOnlyMemory<byte>);
        }

        public bool NeedsCtx()
        {
            return false;
        }

        public void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
        {
            _fieldHandler.LoadReadOnlyMemory(ilGenerator, pushReader, pushCtx);
        }

        public void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
        {
            _fieldHandler.Skip(ilGenerator, pushReader, pushCtx);
        }

        public void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx,
            Action<IILGen> pushValue)
        {
            _fieldHandler.SaveReadOnlyMemory(ilGenerator, pushWriter, pushValue);
        }

        public FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
        {
            throw new InvalidOperationException();
        }

        public void Skip(ref MemReader reader, IReaderCtx? ctx)
        {
            reader.SkipByteArray();
        }

        public FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
        {
            throw new InvalidOperationException();
        }

        public void FreeContent(ref MemReader reader, IReaderCtx? ctx)
        {
            reader.SkipByteArray();
        }

        public bool DoesNeedFreeContent(HashSet<Type> visitedTypes) => false;

        public IFieldHandler SpecializeLoadForType(Type type, IFieldHandler? typeHandler, IFieldHandlerLogger? logger)
        {
            throw new InvalidOperationException();
        }

        public IFieldHandler SpecializeSaveForType(Type type)
        {
            throw new InvalidOperationException();
        }

        public bool DoesPreferLoadAsMemory() => true;
    }

    public IFieldHandler SpecializeSaveForType(Type type)
    {
        if (typeof(ByteBuffer) == type)
        {
            return new ByteBufferHandler(this);
        }

        if (typeof(ReadOnlyMemory<byte>) == type)
        {
            return new ReadOnlyMemoryHandler(this);
        }

        return this;
    }
}
