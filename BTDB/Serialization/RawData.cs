using System.Runtime.CompilerServices;

namespace BTDB.Serialization;

// Helper class for getting offset of field in object
public sealed class RawData
{
    public byte Data;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ref byte Ref(object? @object)
    {
        return ref Unsafe.SubtractByteOffset(ref Unsafe.As<RawData>(@object)!.Data, Unsafe.SizeOf<object>());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ref byte Ref(object? @object, uint offset)
    {
        return ref Unsafe.AddByteOffset(ref Ref(@object), offset);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint CalcOffset<T>(object @object, ref T field)
    {
        return (uint)Unsafe.ByteOffset(ref Ref(@object), ref Unsafe.As<T, byte>(ref field));
    }
}
