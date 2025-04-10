using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.IL;

namespace BTDB.Serialization;

public class DefaultTypeConverterFactory : ITypeConverterFactory
{
    readonly Dictionary<(Type From, Type To), Converter> _converters = new();

    public Converter? GetConverter(Type from, Type to)
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
            var elementType = to.GetElementType()!;
            ref readonly var mt = ref RawData.MethodTableOf(to);
            var offset = mt.BaseSize - (uint)Unsafe.SizeOf<nint>();
            if (GetConverter(from, elementType) is { } itemConversion)
            {
                var itemSize = RawData.GetSizeAndAlign(elementType).Size;
                var itemAlign = RawData.GetSizeAndAlign(elementType).Align;
                var offsetDelta = RawData.Align(1, itemAlign);
                return (ref byte fromI, ref byte toI) =>
                {
                    var res = Array.CreateInstance(elementType, 1);
                    itemConversion(ref fromI, ref RawData.Ref(res, offset));
                    Unsafe.As<byte, object>(ref toI) = res;
                };
            }
        }

        if (to.IsGenericType && (to.GetGenericTypeDefinition() == typeof(List<>) ||
                                 to.GetGenericTypeDefinition() == typeof(IList<>)))
        {
            var toList = to.GenericTypeArguments[0];
            var collectionMetadata = ReflectionMetadata.FindCollectionByType(to);
            if (collectionMetadata == null)
                throw new NotSupportedException(
                    $"Type {to} is not supported for conversion. Use [assembly: GenerateFor(typeof(...))] to generate code for it.");
            if (from == toList)
            {
                if (toList.IsValueType)
                {
                    return (ref byte fromI, ref byte toI) =>
                    {
                        unsafe
                        {
                            var listResult = collectionMetadata.Creator(1);
                            collectionMetadata.Adder(listResult, ref fromI);
                            Unsafe.As<byte, object>(ref toI) = listResult;
                        }
                    };
                }

                return (ref byte fromI, ref byte toI) =>
                {
                    unsafe
                    {
                        if (Unsafe.As<byte, object>(ref fromI) == null)
                        {
                            var listResult = collectionMetadata.Creator(0);
                            Unsafe.As<byte, object>(ref toI) = listResult;
                        }
                        else
                        {
                            var listResult = collectionMetadata.Creator(1);
                            collectionMetadata.Adder(listResult, ref fromI);
                            Unsafe.As<byte, object>(ref toI) = listResult;
                        }
                    }
                };
            }

            if (GetConverter(from, toList) is { } itemConversion)
            {
                if (!toList.IsValueType)
                {
                    return (ref byte fromI, ref byte toI) =>
                    {
                        unsafe
                        {
                            object? toItem = null;
                            itemConversion(ref fromI, ref Unsafe.As<object, byte>(ref toItem));
                            if (toItem == null)
                            {
                                var listResult = collectionMetadata.Creator(0);
                                Unsafe.As<byte, object>(ref toI) = listResult;
                            }
                            else
                            {
                                var listResult = collectionMetadata.Creator(1);
                                collectionMetadata.Adder(listResult, ref Unsafe.As<object, byte>(ref toItem));
                                Unsafe.As<byte, object>(ref toI) = listResult;
                            }
                        }
                    };
                }

                if (!RawData.MethodTableOf(toList).ContainsGCPointers)
                {
                    return (ref byte fromI, ref byte toI) =>
                    {
                        unsafe
                        {
                            Int128 toItem = default;
                            itemConversion(ref fromI, ref Unsafe.As<Int128, byte>(ref toItem));
                            var listResult = collectionMetadata.Creator(1);
                            collectionMetadata.Adder(listResult, ref Unsafe.As<Int128, byte>(ref toItem));
                            Unsafe.As<byte, object>(ref toI) = listResult;
                        }
                    };
                }

                throw new NotSupportedException("Cannot convert from " + from.ToSimpleName() + " to " +
                                                to.ToSimpleName());
            }
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

        if (to == typeof(object) && !from.IsValueType) return CreateAssign(to);

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

    public void RegisterConverter<TFrom, TTo>(ActionConverter<TFrom, TTo> converter)
    {
        _converters[(typeof(TFrom), typeof(TTo))] = (ref byte from, ref byte to) =>
        {
            converter(in Unsafe.As<byte, TFrom>(ref from), out Unsafe.As<byte, TTo>(ref to));
        };
    }

    Converter? CreateAssign(Type type)
    {
        ref readonly var methodTable = ref RawData.MethodTableOf(type);
        if (!methodTable.IsValueType)
        {
            return static (ref byte from, ref byte to) =>
            {
                Unsafe.As<byte, object>(ref to) = Unsafe.As<byte, object>(ref from);
            };
        }

        if (!methodTable.ContainsGCPointers)
        {
            switch (methodTable.BaseSize)
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
                default:
                {
                    var size = methodTable.BaseSize;
                    return (ref byte from, ref byte to) => { Unsafe.CopyBlock(ref to, ref from, size); };
                }
            }
        }

        if (type.SpecializationOf(typeof(ValueTuple<,>)) is { } valueTuple2)
        {
            var typeParams = valueTuple2.GetGenericArguments();
            var offsets = RawData.GetOffsets(typeParams[0], typeParams[1]);
            var convertor1 = CreateAssign(typeParams[0]);
            if (convertor1 == null) return null;
            var convertor2 = CreateAssign(typeParams[1]);
            if (convertor2 == null) return null;
            return (ref byte from, ref byte to) =>
            {
                convertor1(ref Unsafe.AddByteOffset(ref from, offsets.Item1),
                    ref Unsafe.AddByteOffset(ref to, offsets.Item1));
                convertor2(ref Unsafe.AddByteOffset(ref from, offsets.Item2),
                    ref Unsafe.AddByteOffset(ref to, offsets.Item2));
            };
        }

        return null;
    }

    private static void ConvertEnumToInt(ref byte from, ref byte to)
    {
        to = from;
    }

    private static void ConvertIntToEnum(ref byte from, ref byte to)
    {
        to = from;
    }
}
