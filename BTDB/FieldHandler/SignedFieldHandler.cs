using System;
using BTDB.StreamLayer;

namespace BTDB.FieldHandler;

public class SignedFieldHandler : SimpleFieldHandlerBase
{
    public SignedFieldHandler() : base("Signed",
        typeof(SpanReader).GetMethod(nameof(SpanReader.ReadVInt64))!,
        typeof(SpanReader).GetMethod(nameof(SpanReader.SkipVInt64))!,
        typeof(SpanWriter).GetMethod(nameof(SpanWriter.WriteVInt64))!)
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
