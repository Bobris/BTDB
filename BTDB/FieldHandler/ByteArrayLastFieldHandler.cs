using System;
using BTDB.Buffer;
using BTDB.IL;
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
}
