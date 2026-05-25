using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BTDB.IL;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;

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
        [FieldOffset(14)] public ushort InterfaceCount;
        [FieldOffset(16)] public unsafe MethodTable* ParentMethodTable;
        [FieldOffset(48)] public unsafe void* ElementType;
        [FieldOffset(56)] public unsafe MethodTable** InterfaceMap;

        public bool HasComponentSize => (this.Flags & 2147483648U) > 0U;

        public bool ContainsGCPointers => (this.Flags & 16777216U) > 0U;

        public bool NonTrivialInterfaceCast => (this.Flags & 1080819712U) > 0U;

        public bool HasTypeEquivalence => (this.Flags & 33554432U) > 0U;

        public bool HasDefaultConstructor => ((int)this.Flags & -2147483136) == 512;

        public bool IsMultiDimensionalArray
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => BaseSize > 3 * 8;
        }

        public int MultiDimensionalArrayRank
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)((BaseSize - 3 * 8) / 8U);
        }

        public bool IsValueType => ((int)Flags & 786432) == 262144;

        public bool IsNullable => ((int)Flags & 983040) == 327680;

        public bool HasInstantiation
        {
            get => ((int)this.Flags & int.MinValue) == 0 && (this.Flags & 48U) > 0U;
        }

        public bool IsGenericTypeDefinition => ((int)this.Flags & -2147483600) == 48;

        public bool IsConstructedGenericType
        {
            get
            {
                uint num = this.Flags & 2147483696U;
                return num is 16U or 32U;
            }
        }
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

    public static (uint OffsetNext, uint Offset, uint Size) GetHashSetEntriesLayout(Type memberType)
    {
        if (!memberType.IsValueType)
        {
            return (12, 0, 16);
        }

        if (ReflectionMetadata.FindCollectionByElementType(memberType) is { } metadata)
        {
            return (metadata.OffsetNext, metadata.OffsetKey, metadata.SizeOfEntry);
        }

        throw new BTDBException("Cannot find metadata for Collection<" + memberType.ToSimpleName() + ">");
    }

    public static (uint OffsetNext, uint OffsetKey, uint OffsetValue, uint Size) GetDictionaryEntriesLayout(
        Type keyType, Type valueType)
    {
        if (!keyType.IsValueType && !valueType.IsValueType)
        {
            return (20, 0, 8, 24);
        }

        var type1 = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
        if (ReflectionMetadata.FindCollectionByType(type1) is { } metadata)
        {
            return (metadata.OffsetNext, metadata.OffsetKey, metadata.OffsetValue, metadata.SizeOfEntry);
        }

        var type2 = typeof(IDictionary<,>).MakeGenericType(keyType, valueType);
        if (ReflectionMetadata.FindCollectionByType(type2) is { } metadata2)
        {
            return (metadata2.OffsetNext, metadata2.OffsetKey, metadata2.OffsetValue, metadata2.SizeOfEntry);
        }

        var type3 = typeof(IOrderedDictionary<,>).MakeGenericType(keyType, valueType);
        if (ReflectionMetadata.FindCollectionByType(type3) is { } metadata3)
        {
            return (metadata3.OffsetNext, metadata3.OffsetKey, metadata3.OffsetValue, metadata3.SizeOfEntry);
        }

        throw new BTDBException("Cannot find metadata for Dictionary<" + keyType.ToSimpleName() + ", " +
                                valueType.ToSimpleName() + ">");
    }

    public static bool IsRefOrContainsRef(Type type)
    {
        return !type.IsValueType || MethodTableOf(type).ContainsGCPointers;
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

        if (type == typeof(decimal))
        {
            return (16, 8);
        }

        if (type == typeof(long) || type == typeof(ulong) || type == typeof(double))
        {
            return (8, 8);
        }

        if (type == typeof(int) || type == typeof(uint) || type == typeof(float))
        {
            return (4, 4);
        }

        if (type == typeof(short) || type == typeof(ushort) || type == typeof(char) || type == typeof(Half))
        {
            return (2, 2);
        }

        if (type == typeof(byte) || type == typeof(sbyte) || type == typeof(bool))
        {
            return (1, 1);
        }

        if (type == typeof(Guid))
        {
            return (16, 4);
        }

        // from type make Array<type> because it has always ComponentSize
        var size = MethodTableOf(type.MakeArrayType()).ComponentSize;
        var alignment = size;
        if (alignment > 16) alignment = 16;
        if (size == 16 && MethodTableOf(type).ContainsGCPointers)
        {
            alignment = 8;
        }

        return (size, alignment);
    }

    public static int GetArrayLength(ref byte array)
    {
        return Unsafe.As<byte, Array>(ref array).Length;
    }

    public static void BuildListOutOfArray(ref byte array, ref byte list, Type listType)
    {
        var res = new List<object>();
        SetMethodTable(res, listType);
        var length = GetArrayLength(ref array);
        Unsafe.As<byte, object>(ref Ref(res, (uint)Unsafe.SizeOf<object>())) = Unsafe.As<byte, object>(ref array);
        Unsafe.As<byte, int>(ref Ref(res, (uint)Unsafe.SizeOf<object>() * 2)) = length;
        Unsafe.As<byte, object>(ref list) = res;
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

    public static (uint Item1, uint Item2) GetOffsets(Type t1, Type t2)
    {
        var offsets = GetOffsets([t1, t2]);
        return (offsets[0], offsets[1]);
    }

    public static uint[] GetOffsets(params Type[] types)
    {
        var fields = new OffsetField[types.Length];
        for (var i = 0; i < types.Length; i++)
        {
            var sizeAndAlign = GetSizeAndAlign(types[i]);
            fields[i] = new()
            {
                Index = i,
                Size = sizeAndAlign.Size,
                Align = sizeAndAlign.Align,
                IsReference = !types[i].IsValueType
            };
        }

        Array.Sort(fields, static (a, b) =>
        {
            if (a.IsReference != b.IsReference) return a.IsReference ? -1 : 1;
            var alignCompare = b.Align.CompareTo(a.Align);
            if (alignCompare != 0) return alignCompare;
            return a.Index.CompareTo(b.Index);
        });

        var offsets = new uint[types.Length];
        var offset = 0u;
        foreach (var field in fields)
        {
            offset = Align(offset, field.Align);
            offsets[field.Index] = offset;
            offset += field.Size;
        }

        return offsets;
    }

    struct OffsetField
    {
        public int Index;
        public uint Size;
        public uint Align;
        public bool IsReference;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void BulkMoveWithWriteBarrier(ref byte destination, ref byte source, nuint byteCount)
    {
        BufferBulkMoveWithWriteBarrier(null, ref destination, ref source, byteCount);
    }

    [UnsafeAccessor(UnsafeAccessorKind.StaticMethod, Name = "BulkMoveWithWriteBarrier")]
    static extern void BufferBulkMoveWithWriteBarrier(
        [UnsafeAccessorType("System.Buffer")] object? buffer,
        ref byte destination,
        ref byte source,
        nuint byteCount);

    public static bool FitsInInt128(Type type)
    {
        return !RawData.MethodTableOf(type).ContainsGCPointers &&
               RawData.GetSizeAndAlign(type).Size <= 16;
    }
}
