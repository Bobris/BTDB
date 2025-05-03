using System;
using System.Runtime.CompilerServices;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class UnsignedFieldHandler : SimpleFieldHandlerBase
{
    public UnsignedFieldHandler() : base("Unsigned",
        typeof(MemReader).GetMethod(nameof(MemReader.ReadVUInt64))!,
        typeof(MemReader).GetMethod(nameof(MemReader.SkipVUInt64))!,
        typeof(MemWriter).GetMethod(nameof(MemWriter.WriteVUInt64))!,
        (ref MemReader reader, IReaderCtx? _) => reader.SkipVUInt64(),
        (ref MemReader reader, IReaderCtx? _, ref byte value) =>
        {
            Unsafe.As<byte, ulong>(ref value) = reader.ReadVUInt64();
        },
        (ref MemWriter writer, IWriterCtx? _, ref byte value) =>
        {
            writer.WriteVUInt64(Unsafe.As<byte, ulong>(ref value));
        })
    {
    }

    public static bool IsCompatibleWith(Type type)
    {
        if (type == typeof(byte)) return true;
        if (type == typeof(ushort)) return true;
        if (type == typeof(uint)) return true;
        if (type == typeof(ulong)) return true;
        return false;
    }

    public override bool IsCompatibleWith(Type type, FieldHandlerOptions options)
    {
        return IsCompatibleWith(type);
    }
}
