using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.IL;
using BTDB.ODBLayer;

namespace BTDB.Serialization;

public class DefaultTypeConverterFactory : ITypeConverterFactory
{
    readonly Dictionary<(Type From, Type To), Converter> _converters = new();

    public virtual Converter? GetConverter(Type from, Type to)
    {
        if (from == to) return CreateAssign(from);
        if (_converters.TryGetValue((from, to), out var converter))
            return converter;
        if (from.IsEnum)
        {
            return GetConverter(from.GetEnumUnderlyingType(), to);
        }

        if (to.IsEnum)
        {
            return GetConverter(from, Enum.GetUnderlyingType(to));
        }

        if (to == typeof(object) && !from.IsValueType) return CreateAssign(to);

        if (to == typeof(object))
        {
            return CreateBoxing(from);
        }

        if (from == typeof(object))
        {
            if (!to.IsValueType)
            {
                return (ref byte fromI, ref byte toI) =>
                {
                    var r = Unsafe.As<byte, object>(ref fromI);
                    if (r != null)
                    {
                        if (!to.IsAssignableFrom(r.GetType())) r = null;
                    }

                    Unsafe.As<byte, object>(ref toI) = r;
                };
            }

            return CreateUnboxing(to);
        }

        if (Nullable.GetUnderlyingType(to) == from)
        {
            var offset = RawData.Align(1, RawData.GetSizeAndAlign(from).Align);
            var assigner = CreateAssign(from);
            if (assigner == null) return null;
            return (ref byte fromI, ref byte toI) =>
            {
                toI = 1;
                assigner(ref fromI, ref Unsafe.AddByteOffset(ref toI, (int)offset));
            };
        }

        if (Nullable.GetUnderlyingType(to) is { } underTo && GetConverter(from, underTo) is { } conv)
        {
            var offset = RawData.Align(1, RawData.GetSizeAndAlign(underTo).Align);
            return (ref byte fromI, ref byte toI) =>
            {
                toI = 1;
                conv(ref fromI, ref Unsafe.AddByteOffset(ref toI, (int)offset));
            };
        }

        if (Nullable.GetUnderlyingType(from) == to)
        {
            var offset = RawData.Align(1, RawData.GetSizeAndAlign(to).Align);
            var assigner = CreateAssign(to);
            if (assigner == null) return null;
            return (ref byte fromI, ref byte toI) =>
            {
                if (fromI == 0) return;
                assigner(ref Unsafe.AddByteOffset(ref fromI, (int)offset), ref toI);
            };
        }

        if (Nullable.GetUnderlyingType(from) is { } underFrom && GetConverter(underFrom, to) is { } conv2)
        {
            var offset = RawData.Align(1, RawData.GetSizeAndAlign(underFrom).Align);
            return (ref byte fromI, ref byte toI) =>
            {
                if (fromI == 0) return;
                conv2(ref Unsafe.AddByteOffset(ref fromI, (int)offset), ref toI);
            };
        }

        if (to.IsArray && to.IsSZArray)
        {
            var toItemType = to.GetElementType()!;
            if (from.IsArray && from.IsSZArray)
            {
                var converter1 = CreateArrayToArrayConverter(from, to, toItemType);
                if (converter1 != null) return converter1;
            }

            if (from.IsArray) return null;
            ref readonly var mt = ref RawData.MethodTableOf(to);
            var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
            if (GetConverter(from, toItemType) is { } itemConversion)
            {
                return (ref byte fromI, ref byte toI) =>
                {
                    var res = Array.CreateInstance(toItemType, 1);
                    itemConversion(ref fromI, ref RawData.Ref(res, offset));
                    Unsafe.As<byte, object>(ref toI) = res;
                };
            }
        }

        if (to.IsGenericType && (to.GetGenericTypeDefinition() == typeof(List<>) ||
                                 to.GetGenericTypeDefinition() == typeof(IList<>)))
        {
            var itemType = to.GenericTypeArguments[0];
            var arrayType = itemType.MakeArrayType();
            var listType = typeof(List<>).MakeGenericType(itemType);
            var convertToArray = GetConverter(from, arrayType);
            if (convertToArray == null)
                return null;
            return (ref byte fromI, ref byte toI) =>
            {
                object? array = null;
                convertToArray(ref fromI, ref Unsafe.As<object, byte>(ref array));
                RawData.BuildListOutOfArray(ref Unsafe.As<object, byte>(ref array),
                    ref Unsafe.As<byte, byte>(ref toI), listType);
            };
        }

        if (from.IsGenericType && to.IsGenericType && from.GetGenericTypeDefinition() == typeof(IDictionary<,>) &&
            to.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            return GetIDictionary2DictionaryConverter(from, to);
        }

        if (from == typeof(byte))
        {
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, sbyte>(ref to) = (sbyte)from; };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, ushort>(ref to) = from; };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, short>(ref to) = from; };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, uint>(ref to) = from; };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, int>(ref to) = from; };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, ulong>(ref to) = from; };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, long>(ref to) = from; };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, Half>(ref to) = from; };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, float>(ref to) = from; };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) => { Unsafe.As<byte, double>(ref to) = from; };
        }

        if (from == typeof(sbyte))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) => { to = (byte)Unsafe.As<byte, sbyte>(ref from); };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ushort>(ref to) = (ushort)Unsafe.As<byte, sbyte>(ref from);
                };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, short>(ref to) = Unsafe.As<byte, sbyte>(ref from);
                };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, uint>(ref to) = (uint)Unsafe.As<byte, sbyte>(ref from);
                };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, int>(ref to) = Unsafe.As<byte, sbyte>(ref from);
                };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ulong>(ref to) = (ulong)Unsafe.As<byte, sbyte>(ref from);
                };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, long>(ref to) = Unsafe.As<byte, sbyte>(ref from);
                };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, Half>(ref to) = Unsafe.As<byte, sbyte>(ref from);
                };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, float>(ref to) = Unsafe.As<byte, sbyte>(ref from);
                };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, double>(ref to) = Unsafe.As<byte, sbyte>(ref from);
                };
        }

        if (from == typeof(ushort))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, byte>(ref to) = (byte)Unsafe.As<byte, ushort>(ref from);
                };
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, sbyte>(ref to) = (sbyte)Unsafe.As<byte, ushort>(ref from);
                };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, short>(ref to) = (short)Unsafe.As<byte, ushort>(ref from);
                };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, int>(ref to) = Unsafe.As<byte, ushort>(ref from);
                };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, uint>(ref to) = Unsafe.As<byte, ushort>(ref from);
                };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, long>(ref to) = Unsafe.As<byte, ushort>(ref from);
                };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ulong>(ref to) = Unsafe.As<byte, ushort>(ref from);
                };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, Half>(ref to) = (Half)Unsafe.As<byte, ushort>(ref from);
                };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, float>(ref to) = Unsafe.As<byte, ushort>(ref from);
                };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, double>(ref to) = Unsafe.As<byte, ushort>(ref from);
                };
        }

        if (from == typeof(short))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, byte>(ref to) = (byte)Unsafe.As<byte, short>(ref from);
                };
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, sbyte>(ref to) = (sbyte)Unsafe.As<byte, short>(ref from);
                };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ushort>(ref to) = (ushort)Unsafe.As<byte, short>(ref from);
                };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, int>(ref to) = Unsafe.As<byte, short>(ref from);
                };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, uint>(ref to) = (uint)Unsafe.As<byte, short>(ref from);
                };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, long>(ref to) = Unsafe.As<byte, short>(ref from);
                };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ulong>(ref to) = (ulong)Unsafe.As<byte, short>(ref from);
                };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, Half>(ref to) = (Half)Unsafe.As<byte, short>(ref from);
                };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, float>(ref to) = Unsafe.As<byte, short>(ref from);
                };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, double>(ref to) = Unsafe.As<byte, short>(ref from);
                };
        }

        if (from == typeof(int))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, byte>(ref to) = (byte)Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, sbyte>(ref to) = (sbyte)Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ushort>(ref to) = (ushort)Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, short>(ref to) = (short)Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, uint>(ref to) = (uint)Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, long>(ref to) = Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ulong>(ref to) = (ulong)Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, Half>(ref to) = (Half)Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, float>(ref to) = Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, double>(ref to) = Unsafe.As<byte, int>(ref from);
                };
            if (to == typeof(decimal))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, decimal>(ref to) = new(Unsafe.As<byte, int>(ref from));
                };
        }

        if (from == typeof(uint))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, byte>(ref to) = (byte)Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, sbyte>(ref to) = (sbyte)Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ushort>(ref to) = (ushort)Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, short>(ref to) = (short)Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, int>(ref to) = (int)Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, long>(ref to) = Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ulong>(ref to) = Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, Half>(ref to) = (Half)Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, float>(ref to) = Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, double>(ref to) = Unsafe.As<byte, uint>(ref from);
                };
            if (to == typeof(decimal))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, decimal>(ref to) = new(Unsafe.As<byte, uint>(ref from));
                };
        }

        if (from == typeof(long))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, byte>(ref to) = (byte)Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, sbyte>(ref to) = (sbyte)Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ushort>(ref to) = (ushort)Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, short>(ref to) = (short)Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, uint>(ref to) = (uint)Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, int>(ref to) = (int)Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ulong>(ref to) = (ulong)Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, Half>(ref to) = (Half)Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, float>(ref to) = Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, double>(ref to) = Unsafe.As<byte, long>(ref from);
                };
            if (to == typeof(decimal))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, decimal>(ref to) = new(Unsafe.As<byte, long>(ref from));
                };
        }

        if (from == typeof(ulong))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, byte>(ref to) = (byte)Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, sbyte>(ref to) = (sbyte)Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ushort>(ref to) = (ushort)Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, short>(ref to) = (short)Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, uint>(ref to) = (uint)Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, int>(ref to) = (int)Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, long>(ref to) = (long)Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, Half>(ref to) = (Half)Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, float>(ref to) = Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, double>(ref to) = Unsafe.As<byte, ulong>(ref from);
                };
            if (to == typeof(decimal))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, decimal>(ref to) = new(Unsafe.As<byte, ulong>(ref from));
                };
        }

        if (from == typeof(Half))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, byte>(ref to) = (byte)Unsafe.As<byte, Half>(ref from);
                };
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, sbyte>(ref to) = (sbyte)Unsafe.As<byte, Half>(ref from);
                };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ushort>(ref to) = (ushort)Unsafe.As<byte, Half>(ref from);
                };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, short>(ref to) = (short)Unsafe.As<byte, Half>(ref from);
                };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, uint>(ref to) = (uint)Unsafe.As<byte, Half>(ref from);
                };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, int>(ref to) = (int)Unsafe.As<byte, Half>(ref from);
                };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, long>(ref to) = (long)Unsafe.As<byte, Half>(ref from);
                };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ulong>(ref to) = (ulong)Unsafe.As<byte, Half>(ref from);
                };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, float>(ref to) = (float)Unsafe.As<byte, Half>(ref from);
                };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, double>(ref to) = (double)Unsafe.As<byte, Half>(ref from);
                };
        }

        if (from == typeof(float))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, byte>(ref to) = (byte)Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, sbyte>(ref to) = (sbyte)Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ushort>(ref to) = (ushort)Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, short>(ref to) = (short)Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, uint>(ref to) = (uint)Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, int>(ref to) = (int)Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, long>(ref to) = (long)Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ulong>(ref to) = (ulong)Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, Half>(ref to) = (Half)Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, double>(ref to) = Unsafe.As<byte, float>(ref from);
                };
            if (to == typeof(decimal))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, decimal>(ref to) = new(Unsafe.As<byte, float>(ref from));
                };
        }

        if (from == typeof(double))
        {
            if (to == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, byte>(ref to) = (byte)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, sbyte>(ref to) = (sbyte)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ushort>(ref to) = (ushort)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, short>(ref to) = (short)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, uint>(ref to) = (uint)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, int>(ref to) = (int)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, long>(ref to) = (long)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, ulong>(ref to) = (ulong)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, Half>(ref to) = (Half)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, float>(ref to) = (float)Unsafe.As<byte, double>(ref from);
                };
            if (to == typeof(decimal))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, decimal>(ref to) = new(Unsafe.As<byte, double>(ref from));
                };
        }

        if (from == typeof(object) && !to.IsValueType)
        {
            return (ref byte fromI, ref byte toI) =>
            {
                var r = Unsafe.As<byte, object>(ref fromI);
                if (r != null)
                {
                    if (!to.IsAssignableFrom(r.GetType())) r = null;
                }

                Unsafe.As<byte, object>(ref toI) = r;
            };
        }

        if (to == typeof(bool))
        {
            if (from == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, bool>(ref to) = Unsafe.As<byte, int>(ref from) != 0;
                };
            if (from == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, bool>(ref to) = Unsafe.As<byte, uint>(ref from) != 0;
                };
        }

        if (to == typeof(string))
        {
            if (from == typeof(byte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) = Unsafe.As<byte, byte>(ref from).ToString();
                };
            if (from == typeof(sbyte))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, sbyte>(ref from).ToString(CultureInfo.InvariantCulture);
                };
            if (from == typeof(ushort))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, ushort>(ref from).ToString();
                };
            if (from == typeof(short))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, short>(ref from).ToString(CultureInfo.InvariantCulture);
                };
            if (from == typeof(uint))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, uint>(ref from).ToString();
                };
            if (from == typeof(int))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, int>(ref from).ToString(CultureInfo.InvariantCulture);
                };
            if (from == typeof(ulong))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, ulong>(ref from).ToString();
                };
            if (from == typeof(long))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, long>(ref from).ToString(CultureInfo.InvariantCulture);
                };
            if (from == typeof(Half))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, Half>(ref from).ToString(CultureInfo.InvariantCulture);
                };
            if (from == typeof(float))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, float>(ref from).ToString(CultureInfo.InvariantCulture);
                };
            if (from == typeof(double))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, double>(ref from).ToString(CultureInfo.InvariantCulture);
                };
            if (from == typeof(decimal))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, decimal>(ref from).ToString(CultureInfo.InvariantCulture);
                };
            if (from == typeof(bool))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, bool>(ref from) ? "1" : "0";
                };
            if (from == typeof(Version))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, Version>(ref from)?.ToString();
                };
            if (from == typeof(Guid))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, Guid>(ref from).ToString();
                };
            if (from == typeof(EncryptedString))
                return static (ref byte from, ref byte to) =>
                {
                    Unsafe.As<byte, string>(ref to) =
                        Unsafe.As<byte, EncryptedString>(ref from);
                };
        }

        if (to == typeof(EncryptedString) && from == typeof(string))
        {
            return static (ref byte from, ref byte to) =>
            {
                Unsafe.As<byte, EncryptedString>(ref to) =
                    Unsafe.As<byte, string>(ref from);
            };
        }

        if (to == typeof(Version) && from == typeof(string))
        {
            return static (ref byte from, ref byte to) =>
            {
#pragma warning disable CA1806 // Result could be ignored because null result is wanted anyway
                Version.TryParse(Unsafe.As<byte, string>(ref from), out var result);
#pragma warning restore CA1806
                Unsafe.As<byte, Version>(ref to) = result;
            };
        }

        if (to == typeof(byte[]) && from == typeof(ByteBuffer))
        {
            return static (ref byte from, ref byte to) =>
            {
                Unsafe.As<byte, byte[]>(ref to) = Unsafe.As<byte, ByteBuffer>(ref from).ToByteArray();
            };
        }

        if (from == typeof(byte[]) && to == typeof(ByteBuffer))
        {
            return static (ref byte from, ref byte to) =>
            {
                Unsafe.As<byte, ByteBuffer>(ref to) = ByteBuffer.NewAsync(Unsafe.As<byte, byte[]>(ref from));
            };
        }

        return null;
    }

    unsafe Converter? GetIDictionary2DictionaryConverter(Type from, Type to)
    {
        var fromKVType = from.GenericTypeArguments;
        var toKVType = to.GenericTypeArguments;
        var toCollectionMetadata = ReflectionMetadata.FindCollectionByType(to);
        if (toCollectionMetadata == null) return null;
        var fromAsDictionaryType = typeof(Dictionary<,>).MakeGenericType(fromKVType);
        var layout = RawData.GetDictionaryEntriesLayout(fromKVType[0], fromKVType[1]);
        if (fromKVType[0] == toKVType[0] && fromKVType[1] == toKVType[1])
        {
            return (ref byte fromI, ref byte toI) =>
            {
                var fromObject = Unsafe.As<byte, object>(ref fromI);
                if (fromObject == null)
                {
                    Unsafe.As<byte, object>(ref toI) = null;
                    return;
                }

                if (fromObject is IInternalODBDictionary fromODBDictionary)
                {
                    var result = toCollectionMetadata.Creator((uint)fromODBDictionary.Count);
                    fromODBDictionary.Iterate((ref byte key, ref byte value) =>
                    {
                        toCollectionMetadata.AdderKeyValue(result, ref key, ref value);
                    });
                    Unsafe.As<byte, object>(ref toI) = result;
                }
                else if (fromObject.GetType().IsAssignableTo(fromAsDictionaryType))
                {
                    var count = Unsafe.As<byte, int>(ref RawData.Ref(fromObject,
                        RawData.Align(8 + 6 * (uint)Unsafe.SizeOf<nint>(), 8)));
                    var result = toCollectionMetadata.Creator((uint)count);
                    var obj = RawData.HashSetEntries(Unsafe.As<HashSet<object>>(fromObject));
                    ref readonly var mt = ref RawData.MethodTableRef(obj);
                    var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                    var offsetDelta = mt.ComponentSize;
                    for (var i = 0; i < count; i++, offset += offsetDelta)
                    {
                        if (Unsafe.As<byte, int>(ref RawData.Ref(obj, offset + layout.OffsetNext)) < -1)
                        {
                            continue;
                        }

                        toCollectionMetadata.AdderKeyValue(result,
                            ref RawData.Ref(obj, offset + layout.OffsetKey),
                            ref RawData.Ref(obj, offset + layout.OffsetValue));
                    }

                    Unsafe.As<byte, object>(ref toI) = result;
                }
            };
        }

        // Todo: Key/Value type converting converter
        return null;
    }

    Converter? CreateUnboxing(Type to)
    {
        if (Nullable.GetUnderlyingType(to) is { } underTo)
        {
            var offset = RawData.Align(1, RawData.GetSizeAndAlign(underTo).Align);
            var assigner = CreateAssign(underTo);
            if (assigner == null)
            {
                return null;
            }

            return (ref byte fromI, ref byte toI) =>
            {
                var o = Unsafe.As<byte, object>(ref fromI);
                if (o == null)
                {
                    toI = 0; // this should not be needed because it should be already initialized to null
                    return;
                }

                if (underTo != o.GetType())
                {
                    throw new InvalidCastException(
                        $"Cannot unbox {o.GetType().ToSimpleName()} to {underTo.ToSimpleName()}");
                }

                assigner(ref RawData.Ref(o, (uint)Unsafe.SizeOf<nuint>()), ref Unsafe.AddByteOffset(ref toI, offset));
                toI = 1;
            };
        }
        else
        {
            var assigner = CreateAssign(to);
            if (assigner == null)
            {
                return null;
            }

            return (ref byte fromI, ref byte toI) =>
            {
                var o = Unsafe.As<byte, object>(ref fromI);
                if (o == null)
                {
                    return;
                }

                if (to != o.GetType())
                {
                    throw new InvalidCastException($"Cannot unbox {o.GetType().ToSimpleName()} to {to.ToSimpleName()}");
                }

                assigner(ref RawData.Ref(o, (uint)Unsafe.SizeOf<nuint>()), ref toI);
            };
        }
    }

    static Converter? CreateBoxing(Type from)
    {
        if (Nullable.GetUnderlyingType(from) is { } underFrom)
        {
            var offset = RawData.Align(1, RawData.GetSizeAndAlign(underFrom).Align);
            var assigner = CreateAssign(underFrom);
            if (assigner == null)
            {
                return null;
            }

            return (ref byte fromI, ref byte toI) =>
            {
                if (fromI == 0)
                {
                    Unsafe.As<byte, object>(ref toI) = null;
                    return;
                }

                var o = Activator.CreateInstance(underFrom);
                assigner(ref Unsafe.AddByteOffset(ref fromI, offset), ref RawData.Ref(o, (uint)Unsafe.SizeOf<nuint>()));
                Unsafe.As<byte, object>(ref toI) = o;
            };
        }
        else
        {
            var assigner = CreateAssign(from);
            if (assigner == null)
            {
                return null;
            }

            return (ref byte fromI, ref byte toI) =>
            {
                var o = Activator.CreateInstance(from);
                assigner(ref fromI, ref RawData.Ref(o, (uint)Unsafe.SizeOf<nuint>()));
                Unsafe.As<byte, object>(ref toI) = o;
            };
        }
    }

    Converter? CreateArrayToArrayConverter(Type from, Type to, Type toItemType)
    {
        var fromItemType = from.GetElementType()!;
        if (GetConverter(fromItemType, toItemType) is { } conv)
        {
            return (ref byte fromI, ref byte toI) =>
            {
                var fromArray = Unsafe.As<byte, Array>(ref fromI);
                if (fromArray == null)
                {
                    Unsafe.As<byte, object>(ref toI) = null;
                    return;
                }

                var len = fromArray.Length;
                var toArray = Array.CreateInstance(toItemType, len);
                ref readonly var fromMt = ref RawData.MethodTableOf(from);
                ref readonly var toMt = ref RawData.MethodTableOf(to);
                var fromOffset = fromMt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                var toOffset = toMt.BaseSize - (uint)Unsafe.SizeOf<nint>();
                for (var i = 0; i < len; i++)
                {
                    conv(ref RawData.Ref(fromArray, fromOffset),
                        ref RawData.Ref(toArray, toOffset));
                    fromOffset += fromMt.ComponentSize;
                    toOffset += toMt.ComponentSize;
                }

                Unsafe.As<byte, object>(ref toI) = toArray;
            };
        }

        return null;
    }

    public void RegisterConverter<TFrom, TTo>(ActionConverter<TFrom, TTo> converter)
    {
        _converters[(typeof(TFrom), typeof(TTo))] = (ref byte from, ref byte to) =>
        {
            converter(in Unsafe.As<byte, TFrom>(ref from), out Unsafe.As<byte, TTo>(ref to));
        };
    }

    public static Converter? CreateAssign(Type type)
    {
        ref readonly var methodTable = ref RawData.MethodTableOf(type);
        if (!methodTable.IsValueType)
        {
            return static (ref byte from, ref byte to) =>
            {
                Unsafe.As<byte, object>(ref to) = Unsafe.As<byte, object>(ref from);
            };
        }

        var size = RawData.GetSizeAndAlign(type).Size;
        if (!methodTable.ContainsGCPointers)
        {
            switch (size)
            {
                case 1:
                    return static (ref byte from, ref byte to) => { to = from; };
                case 2:
                    return static (ref byte from, ref byte to) =>
                    {
                        Unsafe.As<byte, ushort>(ref to) = Unsafe.As<byte, ushort>(ref from);
                    };
                case 4:
                    return static (ref byte from, ref byte to) =>
                    {
                        Unsafe.As<byte, uint>(ref to) = Unsafe.As<byte, uint>(ref from);
                    };
                case 8:
                    return static (ref byte from, ref byte to) =>
                    {
                        Unsafe.As<byte, ulong>(ref to) = Unsafe.As<byte, ulong>(ref from);
                    };
                case 16:
                    return (ref byte from, ref byte to) => { Unsafe.CopyBlock(ref to, ref from, 16); };
                default:
                {
                    var size2 = size;
                    return (ref byte from, ref byte to) => { Unsafe.CopyBlock(ref to, ref from, size2); };
                }
            }
        }

        var size3 = size;
        return (ref byte from, ref byte to) => { RawData.BulkMoveWithWriteBarrier(ref to, ref from, size3); };
    }
}
