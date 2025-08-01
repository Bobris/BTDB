using System;
using System.Runtime.CompilerServices;
using BTDB.Buffer;
using BTDB.IL;
using BTDB.Serialization;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class ByteArrayLastFieldHandler : ByteArrayFieldHandler
{
    public override string Name => "Byte[]Last";

    public override void Load(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadByteArrayRawTillEof))!);
    }

    public override void LoadByteBuffer(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadByteArrayRawTillEof))!);
        ilGenerator.Call(() => ByteBuffer.NewAsync(null));
    }

    public override void LoadReadOnlyMemory(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        pushReader(ilGenerator);
        ilGenerator.Call(typeof(MemReader).GetMethod(nameof(MemReader.ReadByteArrayRawTillEofAsMemory))!);
    }

    public override void Skip(IILGen ilGenerator, Action<IILGen> pushReader, Action<IILGen>? pushCtx)
    {
        // Nothing needed it is last field anyway
    }

    public override void Skip(ref MemReader reader, IReaderCtx? ctx)
    {
        // Nothing needed it is last field anyway
    }

    public override void Save(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen>? pushCtx,
        Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteByteArrayRaw))!);
    }

    protected override void SaveByteBuffer(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteBlock), new[] { typeof(ByteBuffer) })!);
    }

    protected override void SaveReadOnlyMemory(IILGen ilGenerator, Action<IILGen> pushWriter, Action<IILGen> pushValue)
    {
        pushWriter(ilGenerator);
        pushValue(ilGenerator);
        ilGenerator.Call(typeof(MemWriter).GetMethod(nameof(MemWriter.WriteBlock),
            new[] { typeof(ReadOnlyMemory<byte>) })!);
    }

    public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        if ((options & FieldHandlerOptions.AtEndOfStream) == 0) return false;
        return base.IsCompatibleWith(type, options);
    }

    public override FieldHandlerLoad Load(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (asType == typeof(byte[]))
        {
            return static (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, byte[]>(ref value) = reader.ReadByteArrayRawTillEof();
            };
        }

        if (asType == typeof(ByteBuffer))
        {
            return static (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, ByteBuffer>(ref value) = ByteBuffer.NewAsync(reader.ReadByteArrayRawTillEof()!);
            };
        }

        if (asType == typeof(ReadOnlyMemory<byte>))
        {
            return static (ref MemReader reader, IReaderCtx? _, ref byte value) =>
            {
                Unsafe.As<byte, ReadOnlyMemory<byte>>(ref value) = reader.ReadByteArrayRawTillEofAsMemory();
            };
        }

        return this.BuildConvertingLoader(typeof(byte[]), asType, typeConverterFactory);
    }

    public override FieldHandlerSave Save(Type asType, ITypeConverterFactory typeConverterFactory)
    {
        if (asType == typeof(byte[]))
        {
            return static (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteByteArrayRaw(Unsafe.As<byte, byte[]>(ref value));
            };
        }

        if (asType == typeof(ByteBuffer))
        {
            return static (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteBlock(Unsafe.As<byte, ByteBuffer>(ref value));
            };
        }

        if (asType == typeof(ReadOnlyMemory<byte>))
        {
            return static (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
            {
                writer.WriteBlock(Unsafe.As<byte, ReadOnlyMemory<byte>>(ref value));
            };
        }

        return this.BuildConvertingSaver(typeof(byte[]), asType, typeConverterFactory);
    }
}
