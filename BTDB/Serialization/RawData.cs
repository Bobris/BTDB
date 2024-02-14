using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.IL;

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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SetMethodTable(object @object, Type type)
    {
        Unsafe.As<byte, nint>(ref Ref(@object)) = type.TypeHandle.Value;
    }

    [StructLayout(LayoutKind.Explicit)]
    public struct MethodTable
    {
        [FieldOffset(0)] public ushort ComponentSize;

        [FieldOffset(0)] public uint Flags;

        [FieldOffset(4)] public uint BaseSize;
    }

    public static unsafe ref readonly MethodTable MethodTableRef(object @object)
    {
        return ref *(MethodTable*)Unsafe.As<byte, nint>(ref Ref(@object));
    }

    public static unsafe ref readonly MethodTable MethodTableOf(Type type)
    {
        return ref *(MethodTable*)type.TypeHandle.Value;
    }

    // Array returned could have more items than are actually in the list
    public static ref object[] ListItems(List<object> @this)
    {
        return ref Unsafe.As<byte, object[]>(ref Ref(@this, (uint)Unsafe.SizeOf<object>()));
    }

    public static ref HashSetEntry[] HashSetEntries(HashSet<object> @this)
    {
        return ref Unsafe.As<byte, HashSetEntry[]>(ref Ref(@this, 2 * (uint)Unsafe.SizeOf<object>()));
    }

    public struct HashSetEntry
    {
        public int HashCode;
        public int Next;
        public byte Value;
    }

    public static (uint Offset, uint Size) GetHashSetEntriesLayout(Type memberType)
    {
        var sa = Combine((8, 4), GetSizeAndAlign(memberType));
        return (Align(8, sa.Align), sa.Size);
    }

    public static uint CombineAlign(uint align1, uint align2)
    {
        return align1 > align2 ? align1 : align2;
    }

    public static uint Align(uint size, uint align)
    {
        return (size + align - 1) & ~(align - 1);
    }

    public static (uint Size, uint Align) GetSizeAndAlign(Type type)
    {
        if (!type.IsValueType)
        {
            return ((uint)Unsafe.SizeOf<object>(), (uint)Unsafe.SizeOf<object>());
        }

        if (type.IsEnum)
        {
            type = Enum.GetUnderlyingType(type);
        }

        if (Nullable.GetUnderlyingType(type) is { } underlyingType)
        {
            var res = GetSizeAndAlign(underlyingType);
            return (Align(1, res.Align) + res.Size, res.Align);
        }

        if (type.SpecializationOf(typeof(KeyValuePair<,>)) is { } kvpType)
        {
            var arguments = kvpType.GetGenericArguments();
            var keySizeAlign = GetSizeAndAlign(arguments[0]);
            var valueSizeAlign = GetSizeAndAlign(arguments[1]);
            return Combine(keySizeAlign, valueSizeAlign);
        }

        if (type.SpecializationOf(typeof(ValueTuple<,>)) is { } vt2Type)
        {
            var arguments = vt2Type.GetGenericArguments();
            return Combine(GetSizeAndAlign(arguments[0]), GetSizeAndAlign(arguments[1]));
        }

        if (type.SpecializationOf(typeof(ValueTuple<,,>)) is { } vt3Type)
        {
            var arguments = vt3Type.GetGenericArguments();
            return Combine(GetSizeAndAlign(arguments[0]), GetSizeAndAlign(arguments[1]), GetSizeAndAlign(arguments[2]));
        }

        var size = MethodTableOf(type).BaseSize;
        return (size, size);
    }

    static (uint Size, uint Align) Combine((uint Size, uint Align) f1, (uint Size, uint Align) f2)
    {
        var align = CombineAlign(f1.Align, f2.Align);
        return (Align(Align(f1.Size, f2.Align) + f2.Size, align), align);
    }

    static (uint Size, uint Align) Combine((uint Size, uint Align) f1, (uint Size, uint Align) f2,
        (uint Size, uint Align) f3)
    {
        var align = CombineAlign(CombineAlign(f1.Align, f2.Align), f3.Align);
        return (Align(Align(Align(f1.Size, f2.Align) + f2.Size, f3.Align) + f3.Size, align), align);
    }
}
