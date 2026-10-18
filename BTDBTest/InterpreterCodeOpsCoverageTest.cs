using System;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using BTDB.Buffer;
using BTDB.Interpreter;
using BTDB.Serialization;
using BTDB.StreamLayer;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace BTDBTest;

public class InterpreterCodeOpsCoverageTest
{
    static InterpreterProgram SingleCodeOp(CodeOp codeOp)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(codeOp);
        builder.AddInstruction(OpCode.Stop);
        return builder.Materialize();
    }

    static byte[] RunWrite<T>(CodeOp codeOp, T value)
    {
        var program = SingleCodeOp(codeOp);
        var writer = new MemWriter();
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref Unsafe.As<MemWriter, byte>(ref writer),
            ref Unsafe.As<T, byte>(ref value), stack, program);
        Interpreter.Run(ref ctx);
        return writer.GetSpan().ToArray();
    }

    delegate void WriteAction(ref MemWriter writer);

    static byte[] Direct(WriteAction write)
    {
        var writer = new MemWriter();
        write(ref writer);
        return writer.GetSpan().ToArray();
    }

    static T RunRead<T>(CodeOp codeOp, byte[] data, out long consumed, uint param2 = 0)
    {
        var program = SingleCodeOp(codeOp);
        var pinned = GC.AllocateArray<byte>(data.Length + 1, true);
        data.CopyTo(pinned, 0);
        var reader = MemReader.CreateFromPinnedSpan(pinned.AsSpan(0, data.Length));
        T result = default!;
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref Unsafe.As<T, byte>(ref result), ref Unsafe.As<MemReader, byte>(ref reader),
            ref Unsafe.As<uint, byte>(ref param2), stack, program);
        Interpreter.Run(ref ctx);
        consumed = reader.GetCurrentPosition();
        return result;
    }

    static long RunSkip(CodeOp codeOp, byte[] data, uint param2 = 0)
    {
        RunRead<long>(codeOp, data, out var consumed, param2);
        return consumed;
    }

    static void RoundTrip<T>(CodeOp write, WriteAction direct, CodeOp read, CodeOp? skip, T value)
    {
        var bytes = RunWrite(write, value);
        Assert.Equal(Direct(direct), bytes);
        Assert.Equal(value, RunRead<T>(read, bytes, out var consumed));
        Assert.Equal(bytes.Length, consumed);
        if (skip is { } skipOp) Assert.Equal(bytes.Length, RunSkip(skipOp, bytes));
    }

    [Fact]
    public void IntegerCodeOpsRoundTrip()
    {
        RoundTrip(CodeOp.WriteBool, (ref MemWriter w) => w.WriteBool(true), CodeOp.ReadBool, CodeOp.Skip1Byte, true);
        RoundTrip(CodeOp.WriteUInt8, (ref MemWriter w) => w.WriteUInt8(200), CodeOp.ReadUInt8, CodeOp.Skip1Byte, (byte)200);
        RoundTrip(CodeOp.WriteInt8, (ref MemWriter w) => w.WriteInt8(-5), CodeOp.ReadInt8, CodeOp.Skip1Byte, (sbyte)-5);
        RoundTrip(CodeOp.WriteInt8Ordered, (ref MemWriter w) => w.WriteInt8Ordered(-5), CodeOp.ReadInt8Ordered, CodeOp.Skip1Byte,
            (sbyte)-5);
        RoundTrip(CodeOp.WriteVInt16, (ref MemWriter w) => w.WriteVInt16(-1234), CodeOp.ReadVInt16, CodeOp.SkipVInt16, (short)-1234);
        RoundTrip(CodeOp.WriteVUInt16, (ref MemWriter w) => w.WriteVUInt16(60000), CodeOp.ReadVUInt16, CodeOp.SkipVUInt16,
            (ushort)60000);
        RoundTrip(CodeOp.WriteVInt32, (ref MemWriter w) => w.WriteVInt32(-123456789), CodeOp.ReadVInt32, CodeOp.SkipVInt32,
            -123456789);
        RoundTrip(CodeOp.WriteVUInt32, (ref MemWriter w) => w.WriteVUInt32(4000000000), CodeOp.ReadVUInt32, CodeOp.SkipVUInt32,
            4000000000u);
        RoundTrip(CodeOp.WriteVInt64, (ref MemWriter w) => w.WriteVInt64(long.MinValue), CodeOp.ReadVInt64, CodeOp.SkipVInt64,
            long.MinValue);
        RoundTrip(CodeOp.WriteVUInt64, (ref MemWriter w) => w.WriteVUInt64(ulong.MaxValue), CodeOp.ReadVUInt64, CodeOp.SkipVUInt64,
            ulong.MaxValue);
        RoundTrip(CodeOp.WriteInt64BE, (ref MemWriter w) => w.WriteInt64BE(-2), CodeOp.ReadInt64BE, CodeOp.Skip8Bytes, -2L);
        RoundTrip(CodeOp.WriteInt64LE, (ref MemWriter w) => w.WriteInt64LE(-3), CodeOp.ReadInt64LE, CodeOp.Skip8Bytes, -3L);
        RoundTrip(CodeOp.WriteUInt16LE, (ref MemWriter w) => w.WriteUInt16LE(0xBEEF), CodeOp.ReadUInt16LE, CodeOp.Skip2Bytes,
            (ushort)0xBEEF);
        RoundTrip(CodeOp.WriteInt32BE, (ref MemWriter w) => w.WriteInt32BE(-77), CodeOp.ReadInt32BE, CodeOp.Skip4Bytes, -77);
        RoundTrip(CodeOp.WriteInt16BE, (ref MemWriter w) => w.WriteInt16BE(-300), CodeOp.ReadInt16BE, CodeOp.Skip2Bytes, (short)-300);
        RoundTrip(CodeOp.WriteInt32LE, (ref MemWriter w) => w.WriteInt32LE(-78), CodeOp.ReadInt32LE, CodeOp.Skip4Bytes, -78);
        RoundTrip(CodeOp.WriteUInt32LE, (ref MemWriter w) => w.WriteUInt32LE(0xDEADBEEF), CodeOp.ReadUInt32LE, CodeOp.Skip4Bytes,
            0xDEADBEEF);
        RoundTrip(CodeOp.WriteUInt64LE, (ref MemWriter w) => w.WriteUInt64LE(0x0102030405060708), CodeOp.ReadUInt64LE,
            CodeOp.Skip8Bytes, 0x0102030405060708ul);
        RoundTrip(CodeOp.WriteUInt64BE, (ref MemWriter w) => w.WriteUInt64BE(0x0102030405060708), CodeOp.ReadUInt64BE,
            CodeOp.Skip8Bytes, 0x0102030405060708ul);
        RoundTrip(CodeOp.WritePointer, (ref MemWriter w) => w.WritePointer(12345), CodeOp.ReadPointer, null, (nint)12345);
    }

    [Fact]
    public void ReadOnlyIntegerCodeOpsDecodeEndianness()
    {
        Assert.Equal((ushort)0x1234, RunRead<ushort>(CodeOp.ReadUInt16BE, [0x12, 0x34], out _));
        Assert.Equal(0x12345678u, RunRead<uint>(CodeOp.ReadUInt32BE, [0x12, 0x34, 0x56, 0x78], out _));
        Assert.Equal((short)0x3412, RunRead<short>(CodeOp.ReadInt16LE, [0x12, 0x34], out _));
        Assert.Equal(0, RunWrite(CodeOp.WriteByteZero, 0).Single());
    }

    [Fact]
    public void FloatingPointCodeOpsRoundTrip()
    {
        RoundTrip(CodeOp.WriteSingle, (ref MemWriter w) => w.WriteSingle(1.5f), CodeOp.ReadSingle, CodeOp.Skip4Bytes, 1.5f);
        RoundTrip(CodeOp.WriteDouble, (ref MemWriter w) => w.WriteDouble(-2.25), CodeOp.ReadDouble, CodeOp.Skip8Bytes, -2.25);
        RoundTrip(CodeOp.WriteDoubleOrdered, (ref MemWriter w) => w.WriteDoubleOrdered(-2.25), CodeOp.ReadDoubleOrdered,
            CodeOp.Skip8Bytes, -2.25);
        RoundTrip(CodeOp.WriteHalf, (ref MemWriter w) => w.WriteHalf((Half)0.5f), CodeOp.ReadHalf, CodeOp.Skip2Bytes, (Half)0.5f);
        RoundTrip(CodeOp.WriteDecimal, (ref MemWriter w) => w.WriteDecimal(-12.345m), CodeOp.ReadDecimal, CodeOp.SkipDecimal,
            -12.345m);
    }

    [Fact]
    public void TimeCodeOpsRoundTrip()
    {
        var dt = new DateTime(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);
        RoundTrip(CodeOp.WriteDateTime, (ref MemWriter w) => w.WriteDateTime(dt), CodeOp.ReadDateTime, null, dt);
        RoundTrip(CodeOp.WriteDateTimeForbidUnspecifiedKind, (ref MemWriter w) => w.WriteDateTimeForbidUnspecifiedKind(dt),
            CodeOp.ReadDateTime, null, dt);
        Assert.ThrowsAny<Exception>(() =>
            RunWrite(CodeOp.WriteDateTimeForbidUnspecifiedKind, new DateTime(2024, 1, 1)));
        var dto = new DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.FromHours(2));
        RoundTrip(CodeOp.WriteDateTimeOffset, (ref MemWriter w) => w.WriteDateTimeOffset(dto), CodeOp.ReadDateTimeOffset,
            CodeOp.SkipDateTimeOffset, dto);
        var ts = TimeSpan.FromTicks(-123456789);
        RoundTrip(CodeOp.WriteTimeSpan, (ref MemWriter w) => w.WriteTimeSpan(ts), CodeOp.ReadTimeSpan, null, ts);
        RoundTrip(CodeOp.WriteGuid, (ref MemWriter w) => w.WriteGuid(Guid.Parse("01234567-89ab-cdef-0123-456789abcdef")),
            CodeOp.ReadGuid, CodeOp.SkipGuid, Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("žluťoučký kůň")]
    [InlineData(null)]
    public void StringCodeOpsRoundTrip(string? value)
    {
        RoundTrip(CodeOp.WriteString, (ref MemWriter w) => w.WriteString(value), CodeOp.ReadString, CodeOp.SkipString, value);
        RoundTrip(CodeOp.WriteStringOrdered, (ref MemWriter w) => w.WriteStringOrdered(value), CodeOp.ReadStringOrdered,
            CodeOp.SkipStringOrdered, value);
        if (value == null) return;
        RoundTrip(CodeOp.WriteStringInUtf8, (ref MemWriter w) => w.WriteStringInUtf8(value), CodeOp.ReadStringInUtf8,
            CodeOp.SkipStringInUtf8, value);
        Assert.Equal(Direct((ref MemWriter w) => w.WriteStringOrderedPrefix(value)), RunWrite(CodeOp.WriteStringOrderedPrefix, value));
    }

    [Fact]
    public void ByteArrayCodeOpsRoundTrip()
    {
        byte[] data = [1, 2, 3, 250];
        RoundTrip(CodeOp.WriteByteArray, (ref MemWriter w) => w.WriteByteArray(data), CodeOp.ReadByteArray, CodeOp.SkipByteArray,
            data);
        RoundTrip<byte[]?>(CodeOp.WriteByteArray, (ref MemWriter w) => w.WriteByteArray((byte[]?)null), CodeOp.ReadByteArray,
            CodeOp.SkipByteArray, null);
        var withLength = RunWrite(CodeOp.WriteByteArray, data);
        Assert.Equal(withLength, RunWrite(CodeOp.WriteByteArrayFromByteBuffer, ByteBuffer.NewAsync(data)));
        Assert.Equal(withLength, RunWrite(CodeOp.WriteByteArrayFromMemory, new ReadOnlyMemory<byte>(data)));
        Assert.Equal(withLength,
            RunWrite(CodeOp.WriteByteArrayLength, new ReadOnlyMemory<byte>(data))
                .Concat(RunWrite(CodeOp.WriteByteArrayRaw, data)).ToArray());
        Assert.Equal(data, RunRead<ReadOnlyMemory<byte>>(CodeOp.ReadByteArrayAsMemory, withLength, out _).ToArray());

        Assert.Equal(data, RunWrite(CodeOp.WriteBlockFromByteArray, data));
        Assert.Equal(data, RunWrite(CodeOp.WriteBlockFromByteBuffer, ByteBuffer.NewAsync(data)));
        Assert.Equal(data, RunWrite(CodeOp.WriteBlockFromMemory, new ReadOnlyMemory<byte>(data)));
        Assert.Equal(data, RunWrite<byte[]?>(CodeOp.WriteByteArrayRaw, data));
        Assert.Empty(RunWrite<byte[]?>(CodeOp.WriteByteArrayRaw, null));
        Assert.Equal(data, RunRead<byte[]>(CodeOp.ReadByteArrayRawTillEof, data, out var consumed));
        Assert.Equal(data.Length, consumed);
        Assert.Equal(data,
            RunRead<ReadOnlyMemory<byte>>(CodeOp.ReadByteArrayRawTillEofAsMemory, data, out consumed).ToArray());
        Assert.Equal(data.Length, consumed);
        Assert.Equal(data[..3], RunRead<byte[]>(CodeOp.ReadByteArrayRaw, data, out consumed, 3));
        Assert.Equal(3, consumed);
        Assert.Equal(2, RunSkip(CodeOp.SkipBlock, data, 2));
    }

    [Fact]
    public void NetworkAndMiscCodeOpsRoundTrip()
    {
        foreach (var address in new[] { IPAddress.Parse("10.1.2.3"), IPAddress.Parse("fe80::1%3"), null })
        {
            RoundTrip(CodeOp.WriteIPAddress, (ref MemWriter w) => w.WriteIPAddress(address), CodeOp.ReadIPAddress,
                CodeOp.SkipIPAddress, address);
        }

        foreach (var version in new[] { new Version(1, 2), new Version(1, 2, 3, 4), null })
        {
            RoundTrip(CodeOp.WriteVersion, (ref MemWriter w) => w.WriteVersion(version), CodeOp.ReadVersion, CodeOp.SkipVersion,
                version);
        }

        foreach (var values in new[] { StringValues.Empty, new StringValues("a"), new StringValues(["a", null, "c"]) })
        {
            var bytes = RunWrite(CodeOp.WriteStringValues, values);
            Assert.Equal(Direct((ref MemWriter w) => w.WriteStringValues(values)), bytes);
            Assert.Equal(values.ToArray(), RunRead<StringValues>(CodeOp.ReadStringValues, bytes, out _).ToArray());
            Assert.Equal(bytes.Length, RunSkip(CodeOp.SkipStringValues, bytes));
        }
    }

    [Theory]
    [InlineData((byte)CodeOp.Invalid, "Invalid interpreter code opcode.")]
    [InlineData((byte)250, "Unknown interpreter code opcode.")]
    public void InvalidCodeOpsThrow(byte codeOp, string message)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => RunSkip((CodeOp)codeOp, [1, 2, 3]));
        Assert.Equal(message, ex.Message);
    }

    static InterpreterProgram Build(Action<InterpreterBuilder> build)
    {
        var builder = new InterpreterBuilder();
        build(builder);
        return builder.Materialize();
    }

    static void RunProgram(InterpreterProgram program)
    {
        Span<byte> stack = stackalloc byte[8];
        object? param1 = null;
        object? param2 = null;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref Unsafe.As<object?, byte>(ref param1),
            ref Unsafe.As<object?, byte>(ref param2), stack, program);
        Interpreter.Run(ref ctx);
    }

    [Fact]
    public void InvalidInstructionsThrow()
    {
        var unknown = Build(b =>
        {
            b.AddInstruction((OpCode)250);
            b.AddInstruction(OpCode.Stop);
        });
        Assert.Equal("Unknown interpreter opcode.",
            Assert.Throws<InvalidOperationException>(() => RunProgram(unknown)).Message);
        var badFinally = Build(b =>
        {
            b.AddInstruction(OpCode.JmpFinally);
            b.AddVUInt64(5);
            b.AddInstruction(OpCode.Stop);
        });
        Assert.Equal("Invalid try block index.",
            Assert.Throws<InvalidOperationException>(() => RunProgram(badFinally)).Message);
        var rethrow = Build(b =>
        {
            b.AddInstruction(OpCode.Rethrow);
            b.AddInstruction(OpCode.Stop);
        });
        Assert.Contains("Cannot rethrow",
            Assert.Throws<InvalidOperationException>(() => RunProgram(rethrow)).Message);
        var catched = Build(b =>
        {
            b.AddInstruction(OpCode.CatchedException);
            b.AddInstruction(OpCode.Stop);
        });
        Assert.Contains("inside catch block",
            Assert.Throws<InvalidOperationException>(() => RunProgram(catched)).Message);
        var throwNull = Build(b =>
        {
            b.AddInstruction(OpCode.ThrowException);
            b.AddInstruction(OpCode.Stop);
        });
        Assert.Throws<NullReferenceException>(() => RunProgram(throwNull));
        var hugeAlloc = Build(b =>
        {
            b.AddInstruction(OpCode.StackBytesAlloc);
            b.AddVUInt64((ulong)int.MaxValue + 1);
            b.AddInstruction(OpCode.Stop);
            b.AddInstruction(OpCode.Stop);
        });
        Assert.Throws<ArgumentOutOfRangeException>(() => RunProgram(hugeAlloc));
    }

    [Fact]
    public void ExceptionInsideTryWithoutCatchRunsFinallyAndPropagates()
    {
        var builder = new InterpreterBuilder();
        var message = builder.AddObjectConstant(new InvalidOperationException("inner"));
        builder.AddTry();
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)message);
        builder.AddInstruction(OpCode.ThrowException);
        builder.AddFinally();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[8];
        object? param1 = null;
        object? param2 = null;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref Unsafe.As<object?, byte>(ref param1),
            ref Unsafe.As<object?, byte>(ref param2), stack, program);
        var thrown = false;
        try
        {
            Interpreter.Run(ref ctx);
        }
        catch (InvalidOperationException ex)
        {
            thrown = true;
            Assert.Equal("inner", ex.Message);
        }

        Assert.True(thrown);
        Assert.True(ctx.BoolResult);
    }

    [Fact]
    public void FinallyWithoutExceptionFallsThroughEndOfFinally()
    {
        var program = Build(b =>
        {
            b.AddTry();
            b.AddInstruction(OpCode.SetBoolResultFalse);
            b.AddFinally();
            b.AddInstruction(OpCode.NegateBoolResult);
            b.AddInstruction(OpCode.EndOfFinally);
            b.AddTryEnd();
            b.AddInstruction(OpCode.NegateBoolResult);
            b.AddInstruction(OpCode.NegateBoolResult);
            b.AddInstruction(OpCode.Stop);
        });
        Span<byte> stack = stackalloc byte[8];
        byte p1 = 0, p2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref p1, ref p2, stack, program);
        Interpreter.Run(ref ctx);
        Assert.True(ctx.BoolResult);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(10)]
    [InlineData(11)]
    [InlineData(12)]
    [InlineData(13)]
    [InlineData(14)]
    [InlineData(15)]
    [InlineData(16)]
    public void StackAllocObjectProvidesRequestedNumberOfSlots(int count)
    {
        var factory = new DefaultTypeConverterFactory();
        factory.RegisterConverter<string, object>((in string from, out object to) => to = from + "!");
        var builder = new InterpreterBuilder();
        var value = builder.AddObjectConstant("slot");
        builder.AddInstruction(OpCode.StackAllocObject);
        builder.AddVUInt64((ulong)count);
        builder.AddInstruction(OpCode.IncrementRefParam2);
        builder.AddVUInt64((ulong)((count - 1) * IntPtr.Size));
        builder.AddInstruction(OpCode.SetParam2ObjectConst);
        builder.AddVUInt64((ulong)value);
        builder.AddInstruction(OpCode.AssignRefParam1Param2);
        builder.AddInstruction(OpCode.ConvertParam1ToResult);
        builder.AddConverterParameter(factory, typeof(string), typeof(object));
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[8];
        object? result = null;
        object? param1 = null;
        object? param2 = null;
        var ctx = new InterpreterCtx(ref Unsafe.As<object?, byte>(ref result), ref Unsafe.As<object?, byte>(ref param1),
            ref Unsafe.As<object?, byte>(ref param2), stack, program);
        Interpreter.Run(ref ctx);
        Assert.Equal("slot!", result);
    }

    class NoMetadataProbe
    {
        public int Value { get; set; }
    }

    [Fact]
    public void BuilderValidatesParameters()
    {
        var builder = new InterpreterBuilder();
        var offset = builder.AllocAlignedConst(4);
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.ConstSpan<long>(offset));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.AllocAlignedConst(0));
        Assert.Throws<ArgumentException>(() => builder.AddStackStructTypeParameter(typeof(string)));
        Assert.Throws<ArgumentException>(() => builder.AddTypeParameter(typeof(NoMetadataProbe)));
        Assert.Throws<ArgumentException>(() => builder.AddGetterParameter(typeof(NoMetadataProbe), "Value"));
        Assert.Throws<ArgumentException>(() =>
            builder.AddConverterParameter(new DefaultTypeConverterFactory(), typeof(Version), typeof(Guid)));
        Assert.Throws<InvalidOperationException>(() => builder.AddCatch());
        Assert.Throws<InvalidOperationException>(() => builder.AddFinally());
        Assert.Throws<InvalidOperationException>(() => builder.AddTryEnd());
    }

    [Fact]
    public void ConstSpanRetypingIsReflectedInDump()
    {
        var builder = new InterpreterBuilder();
        var offset = builder.AllocAlignedConst(8);
        builder.ConstSpan<long>(offset) = 5;
        builder.ConstSpan<double>(offset) = 1.5;
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();
        builder.DumpAsm(sb);
        Assert.Contains("0000: Double " + 1.5, sb.ToString());
        Assert.DoesNotContain("Int64", sb.ToString());
    }

    [Fact]
    public void DumpAsmFallsBackToRawParameters()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.AllocObjectToResult);
        builder.AddVUInt64(3);
        builder.AddInstruction(OpCode.StackStructAlloc);
        builder.AddVUInt64(77);
        builder.AddInstruction(OpCode.CallGetter);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.ConvertParam1ToResult);
        builder.AddVUInt64(9);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();
        builder.DumpAsm(sb);
        var dump = sb.ToString();
        Assert.Contains("AllocObjectToResult 3\n", dump);
        Assert.Contains("StackStructAlloc 77\n", dump);
        Assert.Contains("CallGetter 16\n", dump);
        Assert.Contains("ConvertParam1ToResult 9\n", dump);
    }
}
