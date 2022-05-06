using System;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class UnsignedFieldHandler : SimpleFieldHandlerBase
{
    public UnsignedFieldHandler() : base("Unsigned",
        typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVUInt64))!,
        typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVUInt64))!,
        typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVUInt64))!)
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
