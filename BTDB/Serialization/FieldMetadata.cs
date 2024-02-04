using System;

namespace BTDB.Serialization;

public class FieldMetadata
{
    public string Name;
    public Type Type;
    public uint? ByteOffset;
    public unsafe delegate*<object, ref byte, void> PropRefGetter;
    public unsafe delegate*<object, ref byte, void> PropRefSetter;
}
