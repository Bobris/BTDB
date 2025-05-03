using System;
using System.Runtime.CompilerServices;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class SignedFieldHandler : SimpleFieldHandlerBase
{
    public SignedFieldHandler() : base("Signed",
        typeof(MemReader).GetMethod(nameof(MemReader.ReadVInt64))!,
        typeof(MemReader).GetMethod(nameof(MemReader.SkipVInt64))!,
        typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVInt64))!,
        (ref MemReader reader, IReaderCtx? _) => reader.SkipVInt64(),
        (ref MemReader reader, IReaderCtx? _, ref byte value) =>
        {
            Unsafe.As<byte, long>(ref value) = reader.ReadVInt64();
        },
        (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
        {
            writer.WriteVInt64(Unsafe.As<byte, long>(ref value));
        })
    {
    }

    public static bool IsCompatibleWith(Type type)
    {
        if (type == typeof(sbyte)) return true;
        if (type == typeof(short)) return true;
        if (type == typeof(int)) return true;
        if (type == typeof(long)) return true;
        return false;
    }

    public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return IsCompatibleWith(type);
    }
}
