using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BTDB;
using BTDB.Buffer;
using BTDB.Encrypted;
using BTDB.Serialization;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace BTDBTest.SerializationTests;

public class DefaultTypeConverterFactoryTest
{
    [Fact]
    public void AssignForScalarTypesWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        var converter = factory.GetConverter(typeof(int), typeof(int));
        int from = 42;
        int to = 0;
        converter!.Invoke(ref Unsafe.As<int, byte>(ref from), ref Unsafe.As<int, byte>(ref to));
        Assert.Equal(42, to);
    }

    [Fact]
    public void AssignForObjectTypesWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        var converter = factory.GetConverter(typeof(string), typeof(string));
        string from = "Hello";
        string to = null!;
        converter!.Invoke(ref Unsafe.As<string, byte>(ref from), ref Unsafe.As<string, byte>(ref to));
        Assert.Equal("Hello", to);
    }

    static void CheckConverter<TFrom, TTo>(ITypeConverterFactory factory, TFrom from, TTo to)
    {
        Assert.Equal((uint)Unsafe.SizeOf<TFrom>(), RawData.GetSizeAndAlign(typeof(TFrom)).Size);
        Assert.Equal((uint)Unsafe.SizeOf<TTo>(), RawData.GetSizeAndAlign(typeof(TTo)).Size);
        var converter = factory.GetConverter(typeof(TFrom), typeof(TTo));
        var protection = 0x1234567890abcdef;
        TTo to2 = default!;
        var protection2 = 0x1234567890abcdef;
        if (converter == null) Assert.Fail($"Converter for {typeof(TFrom).Name} to {typeof(TTo).Name} not found");
        //var hex = ToHex(ref protection, ref protection2);
        converter.Invoke(ref Unsafe.As<TFrom, byte>(ref from), ref Unsafe.As<TTo, byte>(ref to2));
        //var hex2 = ToHex(ref protection, ref protection2);
        CheckProtection(ref protection, ref protection2);
        Assert.Equal(to, to2);
    }

    static string ToHex(ref long protection, ref long protection2)
    {
        ref var start = ref Unsafe.As<long, byte>(ref protection);
        ref var end = ref Unsafe.As<long, byte>(ref protection2);
        if (Unsafe.ByteOffset(in start, in end) < 0)
        {
            start = ref Unsafe.As<long, byte>(ref protection2);
            end = ref Unsafe.As<long, byte>(ref protection);
        }

        var length = Unsafe.ByteOffset(in start, in end) + 8;
        var bytes = new byte[length];
        Unsafe.CopyBlockUnaligned(ref bytes[0], ref start, (uint)length);
        var hex = BitConverter.ToString(bytes).Replace("-", "");
        return hex;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    static void CheckProtection(ref long protection, ref long protection2)
    {
        Assert.Equal(0x1234567890abcdef, protection);
        Assert.Equal(0x1234567890abcdef, protection2);
    }

    [Fact]
    public void NumberConversionsWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, (byte)42, (sbyte)42);
        CheckConverter(factory, (byte)42, (short)42);
        CheckConverter(factory, (byte)42, (ushort)42);
        CheckConverter(factory, (byte)42, (int)42);
        CheckConverter(factory, (byte)42, (uint)42);
        CheckConverter(factory, (byte)42, (long)42);
        CheckConverter(factory, (byte)42, (ulong)42);
        CheckConverter(factory, (byte)42, (Half)42);
        CheckConverter(factory, (byte)42, (float)42);
        CheckConverter(factory, (byte)42, (double)42);
        CheckConverter(factory, (sbyte)42, (byte)42);
        CheckConverter(factory, (sbyte)42, (short)42);
        CheckConverter(factory, (sbyte)42, (ushort)42);
        CheckConverter(factory, (sbyte)42, (int)42);
        CheckConverter(factory, (sbyte)42, (uint)42);
        CheckConverter(factory, (sbyte)42, (long)42);
        CheckConverter(factory, (sbyte)42, (ulong)42);
        CheckConverter(factory, (sbyte)42, (Half)42);
        CheckConverter(factory, (sbyte)42, (float)42);
        CheckConverter(factory, (sbyte)42, (double)42);
        CheckConverter(factory, (short)42, (byte)42);
        CheckConverter(factory, (short)42, (sbyte)42);
        CheckConverter(factory, (short)42, (ushort)42);
        CheckConverter(factory, (short)42, (int)42);
        CheckConverter(factory, (short)42, (uint)42);
        CheckConverter(factory, (short)42, (long)42);
        CheckConverter(factory, (short)42, (ulong)42);
        CheckConverter(factory, (short)42, (Half)42);
        CheckConverter(factory, (short)42, (float)42);
        CheckConverter(factory, (short)42, (double)42);
        CheckConverter(factory, (ushort)42, (byte)42);
        CheckConverter(factory, (ushort)42, (sbyte)42);
        CheckConverter(factory, (ushort)42, (short)42);
        CheckConverter(factory, (ushort)42, (int)42);
        CheckConverter(factory, (ushort)42, (uint)42);
        CheckConverter(factory, (ushort)42, (long)42);
        CheckConverter(factory, (ushort)42, (ulong)42);
        CheckConverter(factory, (ushort)42, (Half)42);
        CheckConverter(factory, (ushort)42, (float)42);
        CheckConverter(factory, (ushort)42, (double)42);
        CheckConverter(factory, (int)42, (byte)42);
        CheckConverter(factory, (int)42, (sbyte)42);
        CheckConverter(factory, (int)42, (short)42);
        CheckConverter(factory, (int)42, (ushort)42);
        CheckConverter(factory, (int)42, (uint)42);
        CheckConverter(factory, (int)42, (long)42);
        CheckConverter(factory, (int)42, (ulong)42);
        CheckConverter(factory, (int)42, (Half)42);
        CheckConverter(factory, (int)42, (float)42);
        CheckConverter(factory, (int)42, (double)42);
        CheckConverter(factory, (int)42, (decimal)42);
        CheckConverter(factory, (uint)42, (byte)42);
        CheckConverter(factory, (uint)42, (sbyte)42);
        CheckConverter(factory, (uint)42, (short)42);
        CheckConverter(factory, (uint)42, (ushort)42);
        CheckConverter(factory, (uint)42, (int)42);
        CheckConverter(factory, (uint)42, (long)42);
        CheckConverter(factory, (uint)42, (ulong)42);
        CheckConverter(factory, (uint)42, (Half)42);
        CheckConverter(factory, (uint)42, (float)42);
        CheckConverter(factory, (uint)42, (double)42);
        CheckConverter(factory, (uint)42, (decimal)42);
        CheckConverter(factory, (long)42, (byte)42);
        CheckConverter(factory, (long)42, (sbyte)42);
        CheckConverter(factory, (long)42, (short)42);
        CheckConverter(factory, (long)42, (ushort)42);
        CheckConverter(factory, (long)42, (int)42);
        CheckConverter(factory, (long)42, (uint)42);
        CheckConverter(factory, (long)42, (ulong)42);
        CheckConverter(factory, (long)42, (Half)42);
        CheckConverter(factory, (long)42, (float)42);
        CheckConverter(factory, (long)42, (double)42);
        CheckConverter(factory, (long)42, (decimal)42);
        CheckConverter(factory, (ulong)42, (byte)42);
        CheckConverter(factory, (ulong)42, (sbyte)42);
        CheckConverter(factory, (ulong)42, (short)42);
        CheckConverter(factory, (ulong)42, (ushort)42);
        CheckConverter(factory, (ulong)42, (int)42);
        CheckConverter(factory, (ulong)42, (uint)42);
        CheckConverter(factory, (ulong)42, (long)42);
        CheckConverter(factory, (ulong)42, (Half)42);
        CheckConverter(factory, (ulong)42, (float)42);
        CheckConverter(factory, (ulong)42, (double)42);
        CheckConverter(factory, (ulong)42, (decimal)42);
        CheckConverter(factory, (Half)42, (byte)42);
        CheckConverter(factory, (Half)42, (sbyte)42);
        CheckConverter(factory, (Half)42, (short)42);
        CheckConverter(factory, (Half)42, (ushort)42);
        CheckConverter(factory, (Half)42, (int)42);
        CheckConverter(factory, (Half)42, (uint)42);
        CheckConverter(factory, (Half)42, (long)42);
        CheckConverter(factory, (Half)42, (ulong)42);
        CheckConverter(factory, (Half)42, (float)42);
        CheckConverter(factory, (Half)42, (double)42);
        CheckConverter(factory, (float)42, (byte)42);
        CheckConverter(factory, (float)42, (sbyte)42);
        CheckConverter(factory, (float)42, (short)42);
        CheckConverter(factory, (float)42, (ushort)42);
        CheckConverter(factory, (float)42, (int)42);
        CheckConverter(factory, (float)42, (uint)42);
        CheckConverter(factory, (float)42, (long)42);
        CheckConverter(factory, (float)42, (ulong)42);
        CheckConverter(factory, (float)42, (Half)42);
        CheckConverter(factory, (float)42, (double)42);
        CheckConverter(factory, (float)42, (decimal)42);
        CheckConverter(factory, (double)42, (byte)42);
        CheckConverter(factory, (double)42, (sbyte)42);
        CheckConverter(factory, (double)42, (short)42);
        CheckConverter(factory, (double)42, (ushort)42);
        CheckConverter(factory, (double)42, (int)42);
        CheckConverter(factory, (double)42, (uint)42);
        CheckConverter(factory, (double)42, (long)42);
        CheckConverter(factory, (double)42, (ulong)42);
        CheckConverter(factory, (double)42, (Half)42);
        CheckConverter(factory, (double)42, (float)42);
        CheckConverter(factory, (double)42, (decimal)42);
    }

    [Fact]
    public void ToStringWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, (byte)42, "42");
        CheckConverter(factory, (sbyte)42, "42");
        CheckConverter(factory, (ushort)42, "42");
        CheckConverter(factory, (short)42, "42");
        CheckConverter(factory, (uint)42, "42");
        CheckConverter(factory, (int)42, "42");
        CheckConverter(factory, (ulong)42, "42");
        CheckConverter(factory, (long)42, "42");
        CheckConverter(factory, (Half)42, "42");
        CheckConverter(factory, (float)42, "42");
        CheckConverter(factory, (double)42, "42");
        CheckConverter(factory, (decimal)42, "42");
        CheckConverter(factory, false, "0");
        CheckConverter(factory, true, "1");
        CheckConverter(factory, new Version(1, 2, 3, 4), "1.2.3.4");
        CheckConverter(factory, Guid.Parse("9e251065-0873-49bc-8fd9-266cc9aa39d3"),
            "9e251065-0873-49bc-8fd9-266cc9aa39d3");
    }

    [Fact]
    public void FromNullableWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, (byte?)42, (sbyte)42);
        CheckConverter(factory, (long?)123456, 123456);
    }

    [Fact]
    public void ToNullableWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, (byte)42, (int?)42);
        CheckConverter(factory, (float)42, (double?)42);
        CheckConverter(factory, (double)42, (double?)42);
        CheckConverter(factory, (decimal)42, (decimal?)42);
    }

    [Fact]
    public void ToObjectWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, "ABC", (object)"ABC");
        CheckConverter(factory, new int[] { 1, 2, 3 }, (object)new int[] { 1, 2, 3 });
    }

    [Fact]
    public void ToArrayWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, 42, new[] { 42 });
        CheckConverter(factory, 42u, new[] { 42u });
    }

    [Fact]
    public void ToListWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, 42, new List<int> { 42 });
        CheckConverter(factory, 42u, new List<uint> { 42u });
        CheckConverter(factory, 42u, new List<ulong> { 42u });
        CheckConverter(factory, 42, new List<Half> { (Half)42 });
        CheckConverter(factory, "Hello", new List<string> { "Hello" });
        CheckConverter(factory, "Hello", new List<object> { "Hello" });
    }

    [Fact]
    public void ToIListWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, 42, (IList<int>)new List<int> { 42 });
        CheckConverter(factory, 42u, (IList<uint>)new List<uint> { 42u });
        CheckConverter(factory, 42u, (IList<ulong>)new List<ulong> { 42u });
        CheckConverter(factory, 42, (IList<Half>)new List<Half> { (Half)42 });
        CheckConverter(factory, "Hello", (IList<string>)new List<string> { "Hello" });
        CheckConverter(factory, "Hello", (IList<object>)new List<object> { "Hello" });
    }

    [Fact]
    public void ToEncryptedStringWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, "Hello", (EncryptedString)"Hello");
        CheckConverter(factory, (EncryptedString)"Hello", "Hello");
    }

    [Fact]
    public void FromByteArrayToByteBufferWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, new byte[] { 1, 2, 3 }, ByteBuffer.NewAsync(new byte[] { 1, 2, 3 }));
        CheckConverter(factory, ByteBuffer.NewAsync(new byte[] { 1, 2, 3 }), new byte[] { 1, 2, 3 });
    }

    [Fact]
    public void AssignStructWithRefsWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, ("A", 42, "B"), ("A", 42, "B"));
    }

    [Fact]
    public void RegisterCustomConvertorWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        factory.RegisterConverter((in StringValues from, out string to) => { to = from.ToString(); });
        CheckConverter(factory, new StringValues(["Hello", "World"]), "Hello,World");
    }

    [Fact]
    public void ArrayToArrayWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, new[] { 1, 2, 3 }, new[] { 1u, 2u, 3u });
        CheckConverter(factory, new[] { (Half)1.0f, (Half)2.0f, (Half)3.0f }, new[] { "1", "2", "3" });
        CheckConverter(factory, (byte[])null, (int[])null);
    }

    [Fact(Skip = "Not implemented yet")]
    public void BoxingScalarValuesWorks()
    {
        var factory = new DefaultTypeConverterFactory();
        CheckConverter(factory, (byte)42, (object)(byte)42);
        CheckConverter(factory, (sbyte)42, (object)(sbyte)42);
        CheckConverter(factory, (short)42, (object)(short)42);
        CheckConverter(factory, (ushort)42, (object)(ushort)42);
        CheckConverter(factory, (int)42, (object)(int)42);
        CheckConverter(factory, (uint)42, (object)(uint)42);
        CheckConverter(factory, (long)42, (object)(long)42);
        CheckConverter(factory, (ulong)42, (object)(ulong)42);
        CheckConverter(factory, (Half)42, (object)(Half)42);
        CheckConverter(factory, (float)42, (object)(float)42);
        CheckConverter(factory, (double)42, (object)(double)42);
        CheckConverter(factory, (decimal)42, (object)(decimal)42);
        CheckConverter(factory, true, (object)true);
        CheckConverter(factory, false, (object)false);
    }
}
