using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using BTDB.Interpreter;
using BTDB.Serialization;
using BTDB.StreamLayer;
using Microsoft.CSharp.RuntimeBinder;
using Xunit;

namespace BTDBTest;

/// Checks the frame/query opcodes of the interpreter against equivalent C# evaluation (operators bound by the C#
/// runtime binder, string methods, LINQ) over boundary values of every supported type.
public class InterpreterFrameOpsCoverageTest
{
    // Byte offsets inside the frame; deliberately non-zero to catch operand offset mix-ups.
    const ushort LeftSlot = 16;
    const ushort RightSlot = 40;
    const ushort ResultSlot = 64;
    const int FrameLongs = 12;

    static readonly DateTime BaseUtc = new(2025, 3, 1, 12, 0, 0, DateTimeKind.Utc);

    static readonly Dictionary<QueryValueType, object[]> Values = new()
    {
        [QueryValueType.UInt8] = [(byte)0, (byte)1, (byte)127, (byte)128, byte.MaxValue],
        [QueryValueType.Int8] = [sbyte.MinValue, (sbyte)-1, (sbyte)0, (sbyte)1, sbyte.MaxValue],
        [QueryValueType.UInt16] = [(ushort)0, (ushort)1, (ushort)255, (ushort)256, (ushort)0x8000, ushort.MaxValue],
        [QueryValueType.Int16] = [short.MinValue, (short)-129, (short)-1, (short)0, (short)1, (short)255, short.MaxValue],
        [QueryValueType.UInt32] = [0u, 1u, 65535u, 65536u, (uint)int.MaxValue, 0x80000000u, uint.MaxValue],
        [QueryValueType.Int32] = [int.MinValue, -129, -1, 0, 1, 256, 65536, int.MaxValue],
        [QueryValueType.UInt64] =
            [0ul, 1ul, (ulong)uint.MaxValue, (ulong)uint.MaxValue + 1, (ulong)long.MaxValue, 1ul << 63, ulong.MaxValue],
        [QueryValueType.Int64] =
            [long.MinValue, (long)int.MinValue - 1, -1L, 0L, 1L, (long)uint.MaxValue + 1, long.MaxValue],
        [QueryValueType.Half] =
        [
            Half.NegativeInfinity, Half.MinValue, (Half)(-1.5f), Half.NegativeZero, (Half)0f, Half.Epsilon,
            (Half)0.5f, (Half)2.5f, (Half)255.9f, Half.MaxValue, Half.PositiveInfinity, Half.NaN
        ],
        [QueryValueType.Single] =
        [
            float.NegativeInfinity, float.MinValue, -1e10f, -1.5f, -0f, 0f, float.Epsilon, 2.5f, 255.9f, 65504f, 1e10f,
            float.MaxValue, float.PositiveInfinity, float.NaN
        ],
        [QueryValueType.Double] =
        [
            double.NegativeInfinity, double.MinValue, -1e20, -1.5, -0d, 0d, double.Epsilon, 2.5, 255.9, 1e10, 1e20,
            double.MaxValue, double.PositiveInfinity, double.NaN
        ],
        [QueryValueType.Decimal] =
        [
            decimal.MinValue, -1e20m, -1.5m, 0m, 1.0m, 1.00m, 2.5m, 255.9m, 1e20m, decimal.MaxValue
        ],
        [QueryValueType.DateTime] =
        [
            DateTime.MinValue, BaseUtc, new DateTime(BaseUtc.Ticks, DateTimeKind.Local),
            new DateTime(BaseUtc.Ticks, DateTimeKind.Unspecified), BaseUtc.AddTicks(1), DateTime.MaxValue
        ],
        [QueryValueType.Guid] =
        [
            Guid.Empty, new Guid("00000000-0000-0000-0000-000000000001"),
            new Guid("6f9619ff-8b86-d011-b42d-00cf4fc964ff"), new Guid("ffffffff-0000-0000-0000-000000000000"),
            new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff")
        ],
        [QueryValueType.TimeSpan] =
            [TimeSpan.MinValue, TimeSpan.FromTicks(-1), TimeSpan.Zero, TimeSpan.FromTicks(1), TimeSpan.MaxValue],
        [QueryValueType.DateTimeOffset] =
        [
            DateTimeOffset.MinValue, new DateTimeOffset(BaseUtc), new DateTimeOffset(BaseUtc).ToOffset(TimeSpan.FromHours(2)),
            new DateTimeOffset(BaseUtc).ToOffset(TimeSpan.FromHours(-5)).AddTicks(1), DateTimeOffset.MaxValue
        ]
    };

    static readonly QueryCompare[] Compares = Enum.GetValues<QueryCompare>();

    static readonly QueryValueType[] NumericTypes =
    [
        QueryValueType.UInt8, QueryValueType.Int8, QueryValueType.UInt16, QueryValueType.Int16, QueryValueType.UInt32,
        QueryValueType.Int32, QueryValueType.UInt64, QueryValueType.Int64, QueryValueType.Half, QueryValueType.Single,
        QueryValueType.Double, QueryValueType.Decimal
    ];

    public static IEnumerable<object[]> AllValueTypes => Enum.GetValues<QueryValueType>().Select(t => new object[] { t });
    public static IEnumerable<object[]> NumericValueTypes => NumericTypes.Select(t => new object[] { t });

    // ---------------------------------------------------------------------------------------------------------------
    // Running programs

    static unsafe bool Run(InterpreterProgram program, long[] frame, object?[] objects, ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value, out long keyPosition, out long valuePosition)
    {
        if (objects.Length == 0) objects = new object?[1];
        fixed (byte* keyPointer = key)
        fixed (byte* valuePointer = value)
        {
            var keyReader = new MemReader(keyPointer, key.Length);
            var valueReader = new MemReader(valuePointer, value.Length);
            var ctx = new InterpreterCtx(program,
                ref Unsafe.As<long, byte>(ref MemoryMarshal.GetArrayDataReference(frame)),
                ref MemoryMarshal.GetArrayDataReference(objects), ref keyReader, ref valueReader);
            Interpreter.Run(ref ctx);
            keyPosition = keyReader.GetCurrentPosition();
            valuePosition = valueReader.GetCurrentPosition();
            return ctx.BoolResult;
        }
    }

    static bool Run(InterpreterProgram program, long[] frame, object?[]? objects = null)
    {
        return Run(program, frame, objects ?? [], [], [], out _, out _);
    }

    static ref byte FrameRef(long[] frame, int offset)
    {
        return ref Unsafe.AddByteOffset(ref Unsafe.As<long, byte>(ref frame[0]), (nint)offset);
    }

    static void EmitBoolResultEnd(InterpreterBuilder builder, uint trueLabel)
    {
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddInstruction(OpCode.Stop);
        builder.MarkLabel(trueLabel);
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
    }

    static int SizeOf(object value) => value switch
    {
        byte or sbyte or bool => 1,
        ushort or short or Half => 2,
        uint or int or float => 4,
        ulong or long or double or DateTime or TimeSpan => 8,
        Guid or decimal => 16,
        DateTimeOffset => Unsafe.SizeOf<DateTimeOffset>(),
        _ => throw new ArgumentException(value.GetType().Name)
    };

    static void Write(ref byte destination, object value)
    {
        switch (value)
        {
            case bool v: Unsafe.WriteUnaligned(ref destination, v); break;
            case byte v: Unsafe.WriteUnaligned(ref destination, v); break;
            case sbyte v: Unsafe.WriteUnaligned(ref destination, v); break;
            case ushort v: Unsafe.WriteUnaligned(ref destination, v); break;
            case short v: Unsafe.WriteUnaligned(ref destination, v); break;
            case uint v: Unsafe.WriteUnaligned(ref destination, v); break;
            case int v: Unsafe.WriteUnaligned(ref destination, v); break;
            case ulong v: Unsafe.WriteUnaligned(ref destination, v); break;
            case long v: Unsafe.WriteUnaligned(ref destination, v); break;
            case Half v: Unsafe.WriteUnaligned(ref destination, v); break;
            case float v: Unsafe.WriteUnaligned(ref destination, v); break;
            case double v: Unsafe.WriteUnaligned(ref destination, v); break;
            case decimal v: Unsafe.WriteUnaligned(ref destination, v); break;
            case DateTime v: Unsafe.WriteUnaligned(ref destination, v); break;
            case Guid v: Unsafe.WriteUnaligned(ref destination, v); break;
            case TimeSpan v: Unsafe.WriteUnaligned(ref destination, v); break;
            case DateTimeOffset v: Unsafe.WriteUnaligned(ref destination, v); break;
            default: throw new ArgumentException(value.GetType().Name);
        }
    }

    static object Read(ref byte source, Type type)
    {
        if (type == typeof(bool)) return Unsafe.ReadUnaligned<bool>(ref source);
        if (type == typeof(byte)) return Unsafe.ReadUnaligned<byte>(ref source);
        if (type == typeof(sbyte)) return Unsafe.ReadUnaligned<sbyte>(ref source);
        if (type == typeof(ushort)) return Unsafe.ReadUnaligned<ushort>(ref source);
        if (type == typeof(short)) return Unsafe.ReadUnaligned<short>(ref source);
        if (type == typeof(uint)) return Unsafe.ReadUnaligned<uint>(ref source);
        if (type == typeof(int)) return Unsafe.ReadUnaligned<int>(ref source);
        if (type == typeof(ulong)) return Unsafe.ReadUnaligned<ulong>(ref source);
        if (type == typeof(long)) return Unsafe.ReadUnaligned<long>(ref source);
        if (type == typeof(Half)) return Unsafe.ReadUnaligned<Half>(ref source);
        if (type == typeof(float)) return Unsafe.ReadUnaligned<float>(ref source);
        if (type == typeof(double)) return Unsafe.ReadUnaligned<double>(ref source);
        if (type == typeof(decimal)) return Unsafe.ReadUnaligned<decimal>(ref source);
        if (type == typeof(DateTime)) return Unsafe.ReadUnaligned<DateTime>(ref source);
        if (type == typeof(Guid)) return Unsafe.ReadUnaligned<Guid>(ref source);
        if (type == typeof(TimeSpan)) return Unsafe.ReadUnaligned<TimeSpan>(ref source);
        if (type == typeof(DateTimeOffset)) return Unsafe.ReadUnaligned<DateTimeOffset>(ref source);
        throw new ArgumentException(type.Name);
    }

    static Type ClrType(QueryValueType type) => type switch
    {
        QueryValueType.UInt8 => typeof(byte),
        QueryValueType.Int8 => typeof(sbyte),
        QueryValueType.UInt16 => typeof(ushort),
        QueryValueType.Int16 => typeof(short),
        QueryValueType.UInt32 => typeof(uint),
        QueryValueType.Int32 => typeof(int),
        QueryValueType.UInt64 => typeof(ulong),
        QueryValueType.Int64 => typeof(long),
        QueryValueType.Half => typeof(Half),
        QueryValueType.Single => typeof(float),
        QueryValueType.Double => typeof(double),
        QueryValueType.DateTime => typeof(DateTime),
        QueryValueType.Guid => typeof(Guid),
        QueryValueType.Decimal => typeof(decimal),
        QueryValueType.TimeSpan => typeof(TimeSpan),
        QueryValueType.DateTimeOffset => typeof(DateTimeOffset),
        _ => throw new ArgumentOutOfRangeException(nameof(type))
    };

    static uint AddConstant(InterpreterBuilder builder, object value)
    {
        var offset = builder.AllocAlignedConst((uint)SizeOf(value), 8);
        Write(ref builder.ConstSpan<byte>(offset), value);
        return offset;
    }

    static string Show(object? value) => value switch
    {
        null => "null",
        string s => "\"" + string.Concat(s.Select(c => c < 128 && !char.IsControl(c) ? c.ToString() : $"\\u{(int)c:x4}")) + "\"",
        char c => $"'\\u{(int)c:x4}'",
        DateTime d => d.ToString("O", CultureInfo.InvariantCulture) + d.Kind,
        DateTimeOffset d => d.ToString("O", CultureInfo.InvariantCulture),
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture) + ":" + value.GetType().Name,
        _ => value.ToString()!
    };

    // ---------------------------------------------------------------------------------------------------------------
    // C# reference semantics

    static bool ReferenceCompare(QueryCompare compare, object left, object right)
    {
        dynamic l = left, r = right;
        return compare switch
        {
            QueryCompare.Equal => (bool)(l == r),
            QueryCompare.NotEqual => (bool)(l != r),
            QueryCompare.Less => (bool)(l < r),
            QueryCompare.LessOrEqual => (bool)(l <= r),
            QueryCompare.Greater => (bool)(l > r),
            QueryCompare.GreaterOrEqual => (bool)(l >= r),
            _ => throw new ArgumentOutOfRangeException(nameof(compare))
        };
    }

    static object CastTo(Type type, dynamic value, bool isChecked)
    {
        if (isChecked)
        {
            if (type == typeof(byte)) return checked((byte)value);
            if (type == typeof(sbyte)) return checked((sbyte)value);
            if (type == typeof(ushort)) return checked((ushort)value);
            if (type == typeof(short)) return checked((short)value);
            if (type == typeof(uint)) return checked((uint)value);
            if (type == typeof(int)) return checked((int)value);
            if (type == typeof(ulong)) return checked((ulong)value);
            if (type == typeof(long)) return checked((long)value);
            if (type == typeof(Half)) return checked((Half)value);
            if (type == typeof(float)) return checked((float)value);
            if (type == typeof(double)) return checked((double)value);
            if (type == typeof(decimal)) return checked((decimal)value);
        }
        else
        {
            if (type == typeof(byte)) return unchecked((byte)value);
            if (type == typeof(sbyte)) return unchecked((sbyte)value);
            if (type == typeof(ushort)) return unchecked((ushort)value);
            if (type == typeof(short)) return unchecked((short)value);
            if (type == typeof(uint)) return unchecked((uint)value);
            if (type == typeof(int)) return unchecked((int)value);
            if (type == typeof(ulong)) return unchecked((ulong)value);
            if (type == typeof(long)) return unchecked((long)value);
            if (type == typeof(Half)) return unchecked((Half)value);
            if (type == typeof(float)) return unchecked((float)value);
            if (type == typeof(double)) return unchecked((double)value);
            if (type == typeof(decimal)) return unchecked((decimal)value);
        }

        throw new ArgumentOutOfRangeException(nameof(type));
    }

    static object ReferenceArithmetic(QueryArithmetic operation, object left, object right)
    {
        dynamic l = left, r = right;
        var isChecked = operation is QueryArithmetic.AddChecked or QueryArithmetic.SubtractChecked
            or QueryArithmetic.MultiplyChecked or QueryArithmetic.NegateChecked;
        // C# promotes small integers to int; the typed result is the cast of the promoted one in the same context.
        dynamic result = operation switch
        {
            QueryArithmetic.Add => unchecked(l + r),
            QueryArithmetic.Subtract => unchecked(l - r),
            QueryArithmetic.Multiply => unchecked(l * r),
            QueryArithmetic.Divide => unchecked(l / r),
            QueryArithmetic.Modulo => unchecked(l % r),
            QueryArithmetic.AddChecked => checked(l + r),
            QueryArithmetic.SubtractChecked => checked(l - r),
            QueryArithmetic.MultiplyChecked => checked(l * r),
            QueryArithmetic.Negate => unchecked(-l),
            QueryArithmetic.NegateChecked => checked(-l),
            _ => throw new ArgumentOutOfRangeException(nameof(operation))
        };
        return CastTo(left.GetType(), result, isChecked);
    }

    // Returns the result or the type of the thrown exception.
    static object Outcome(Func<object> action)
    {
        try
        {
            return action();
        }
        catch (Exception exception) when (exception is not RuntimeBinderException)
        {
            return exception.GetType();
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // JmpIfFrameConst, JmpIfNotFrameConst, JmpIfFrameFrame, JmpIfNotFrameFrame

    static InterpreterProgram FrameConstProgram(QueryValueType type, QueryCompare compare, object constant,
        bool jumpIfTrue)
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("jumped");
        var constantOffset = AddConstant(builder, constant);
        builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfFrameConst : OpCode.JmpIfNotFrameConst);
        builder.AddUInt8((byte)type);
        builder.AddUInt8((byte)compare);
        builder.AddUInt16(LeftSlot);
        builder.AddUInt32(constantOffset);
        builder.AddLabelTarget(label);
        EmitBoolResultEnd(builder, label);
        return builder.Materialize();
    }

    static InterpreterProgram FrameFrameProgram(QueryValueType type, QueryCompare compare, bool jumpIfTrue)
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("jumped");
        builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfFrameFrame : OpCode.JmpIfNotFrameFrame);
        builder.AddUInt8((byte)type);
        builder.AddUInt8((byte)compare);
        builder.AddUInt16(LeftSlot);
        builder.AddUInt16(RightSlot);
        builder.AddLabelTarget(label);
        EmitBoolResultEnd(builder, label);
        return builder.Materialize();
    }

    static long[] FrameWith(object left, object? right = null)
    {
        var frame = new long[FrameLongs];
        // Garbage around the slots must not leak into narrow values.
        frame.AsSpan().Fill(unchecked((long)0xA5A5A5A5A5A5A5A5));
        Write(ref FrameRef(frame, LeftSlot), left);
        if (right != null) Write(ref FrameRef(frame, RightSlot), right);
        return frame;
    }

    [Theory]
    [MemberData(nameof(AllValueTypes))]
    public void FrameComparisonsMatchCSharpOperators(QueryValueType type)
    {
        var values = Values[type];
        var failures = new List<string>();
        foreach (var compare in Compares)
        {
            var frameFrameJump = FrameFrameProgram(type, compare, true);
            var frameFrameNotJump = FrameFrameProgram(type, compare, false);
            foreach (var right in values)
            {
                var constJump = FrameConstProgram(type, compare, right, true);
                var constNotJump = FrameConstProgram(type, compare, right, false);
                foreach (var left in values)
                {
                    var expected = ReferenceCompare(compare, left, right);
                    var actual = new[]
                    {
                        Run(constJump, FrameWith(left)), !Run(constNotJump, FrameWith(left)),
                        Run(frameFrameJump, FrameWith(left, right)), !Run(frameFrameNotJump, FrameWith(left, right))
                    };
                    if (actual.Any(a => a != expected))
                        failures.Add($"{Show(left)} {compare} {Show(right)}: expected {expected}, got {string.Join(",", actual)}");
                }
            }
        }

        Assert.Empty(failures);
    }

    [Fact]
    public void FrameComparisonBoundaryExamples()
    {
        // Spot checks independent of the reference evaluation.
        Assert.False(Run(FrameConstProgram(QueryValueType.Double, QueryCompare.Equal, double.NaN, true),
            FrameWith(double.NaN)));
        Assert.True(Run(FrameConstProgram(QueryValueType.Half, QueryCompare.NotEqual, Half.NaN, true),
            FrameWith(Half.NaN)));
        Assert.False(Run(FrameConstProgram(QueryValueType.Single, QueryCompare.GreaterOrEqual, float.NaN, true),
            FrameWith(float.PositiveInfinity)));
        Assert.True(Run(FrameConstProgram(QueryValueType.Decimal, QueryCompare.Equal, 1.00m, true), FrameWith(1.0m)));
        Assert.True(Run(FrameConstProgram(QueryValueType.UInt8, QueryCompare.Greater, (byte)127, true),
            FrameWith((byte)128)));
        Assert.True(Run(FrameConstProgram(QueryValueType.Int8, QueryCompare.Less, (sbyte)127, true),
            FrameWith(sbyte.MinValue)));
        var instant = new DateTimeOffset(BaseUtc);
        Assert.True(Run(FrameConstProgram(QueryValueType.DateTimeOffset, QueryCompare.Equal, instant, true),
            FrameWith(instant.ToOffset(TimeSpan.FromHours(3)))));
        Assert.True(Run(FrameConstProgram(QueryValueType.Guid, QueryCompare.Less,
            new Guid("ffffffff-0000-0000-0000-000000000000"), true), FrameWith(new Guid("6f9619ff-8b86-d011-b42d-00cf4fc964ff"))));
        Assert.True(Run(FrameConstProgram(QueryValueType.TimeSpan, QueryCompare.LessOrEqual, TimeSpan.MinValue, true),
            FrameWith(TimeSpan.MinValue)));
    }

    [Theory]
    [InlineData(QueryValueType.Int32)]
    [InlineData(QueryValueType.DateTime)]
    [InlineData(QueryValueType.Guid)]
    [InlineData(QueryValueType.TimeSpan)]
    [InlineData(QueryValueType.DateTimeOffset)]
    public void FrameComparisonRejectsUnknownCompare(QueryValueType type)
    {
        var value = Values[type][0];
        var program = FrameConstProgram(type, (QueryCompare)99, value, true);
        Assert.Contains("Unknown query comparison 99",
            Assert.Throws<InvalidOperationException>(() => Run(program, FrameWith(value))).Message);
    }

    [Fact]
    public void FrameComparisonRejectsUnknownValueType()
    {
        var program = FrameFrameProgram((QueryValueType)99, QueryCompare.Equal, true);
        Assert.Contains("Unknown query value type 99",
            Assert.Throws<InvalidOperationException>(() => Run(program, FrameWith(1L, 1L))).Message);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // JmpIfFrameInSet, JmpIfNotFrameInSet

    static object MakeSet<T>(object[] items) where T : unmanaged
    {
        return new HashSet<T>(items.Cast<T>());
    }

    static InterpreterProgram FrameInSetProgram(QueryValueType type, object set, bool jumpIfTrue)
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("in");
        builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfFrameInSet : OpCode.JmpIfNotFrameInSet);
        builder.AddUInt8((byte)type);
        builder.AddUInt16(LeftSlot);
        builder.AddUInt32((uint)builder.AddObjectConstant(set));
        builder.AddLabelTarget(label);
        EmitBoolResultEnd(builder, label);
        return builder.Materialize();
    }

    [Theory]
    [MemberData(nameof(AllValueTypes))]
    public void FrameInSetMatchesSetContainsOfEveryType(QueryValueType type)
    {
        var values = Values[type];
        var members = values.Where((_, i) => i % 2 == 0).ToArray();
        var set = typeof(InterpreterFrameOpsCoverageTest)
            .GetMethod(nameof(MakeSet), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(ClrType(type)).Invoke(null, [members])!;
        var inSet = FrameInSetProgram(type, set, true);
        var notInSet = FrameInSetProgram(type, set, false);
        foreach (var value in values)
        {
            // Sets use Equals, so NaN is found and 1.0m equals 1.00m, unlike some == operators.
            var expected = members.Any(m => m.Equals(value));
            Assert.True(expected == Run(inSet, FrameWith(value)), $"{Show(value)} in set");
            Assert.True(expected != Run(notInSet, FrameWith(value)), $"{Show(value)} not in set");
        }
    }

    [Fact]
    public void FrameInSetRejectsUnknownValueType()
    {
        var program = FrameInSetProgram((QueryValueType)99, new HashSet<int>(), true);
        Assert.Throws<InvalidOperationException>(() => Run(program, FrameWith(1)));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Arithmetic

    static InterpreterProgram ArithmeticProgram(QueryValueType type, QueryArithmetic operation,
        QueryOperandKind leftKind, uint leftOffset, QueryOperandKind rightKind, uint rightOffset, ushort destination,
        InterpreterBuilder? builder = null)
    {
        builder ??= new InterpreterBuilder();
        builder.AddInstruction(OpCode.Arithmetic);
        builder.AddUInt8((byte)type);
        builder.AddUInt8((byte)operation);
        builder.AddUInt16(destination);
        builder.AddUInt8((byte)leftKind);
        builder.AddUInt32(leftOffset);
        builder.AddUInt8((byte)rightKind);
        builder.AddUInt32(rightOffset);
        builder.AddInstruction(OpCode.Stop);
        return builder.Materialize();
    }

    static object RunArithmetic(QueryValueType type, QueryArithmetic operation, object left, object right)
    {
        var builder = new InterpreterBuilder();
        var constant = AddConstant(builder, right);
        var program = ArithmeticProgram(type, operation, QueryOperandKind.Frame, LeftSlot, QueryOperandKind.Constant,
            constant, ResultSlot, builder);
        var frame = FrameWith(left);
        Run(program, frame);
        return Read(ref FrameRef(frame, ResultSlot), left.GetType());
    }

    [Theory]
    [MemberData(nameof(NumericValueTypes))]
    public void ArithmeticMatchesCSharpOperators(QueryValueType type)
    {
        var values = Values[type];
        var failures = new List<string>();
        var compared = 0;
        foreach (var operation in Enum.GetValues<QueryArithmetic>())
        {
            foreach (var left in values)
            {
                foreach (var right in values)
                {
                    object expected;
                    try
                    {
                        expected = Outcome(() => ReferenceArithmetic(operation, left, right));
                    }
                    catch (RuntimeBinderException)
                    {
                        // Unary minus of ulong does not compile in C#.
                        Assert.Equal(QueryValueType.UInt64, type);
                        continue;
                    }

                    var actual = Outcome(() => RunArithmetic(type, operation, left, right));
                    compared++;
                    if (!Equals(expected, actual))
                        failures.Add($"{operation} {Show(left)}, {Show(right)}: expected {Show(expected)}, got {Show(actual)}");
                }
            }
        }

        Assert.Empty(failures);
        Assert.True(compared >= values.Length * values.Length * 8);
    }

    [Fact]
    public void ArithmeticReadsConstantLeftOperandAndWritesInPlace()
    {
        var builder = new InterpreterBuilder();
        var constant = AddConstant(builder, 100L);
        var program = ArithmeticProgram(QueryValueType.Int64, QueryArithmetic.Subtract, QueryOperandKind.Constant,
            constant, QueryOperandKind.Frame, LeftSlot, LeftSlot, builder);
        var frame = FrameWith(30L);

        Run(program, frame);

        Assert.Equal(70L, Unsafe.ReadUnaligned<long>(ref FrameRef(frame, LeftSlot)));
        // A negation ignores the right operand, which may point anywhere.
        program = ArithmeticProgram(QueryValueType.Int16, QueryArithmetic.Negate, QueryOperandKind.Frame, LeftSlot,
            QueryOperandKind.Constant, uint.MaxValue, ResultSlot);
        frame = FrameWith((short)-7);
        Run(program, frame);
        Assert.Equal((short)7, Unsafe.ReadUnaligned<short>(ref FrameRef(frame, ResultSlot)));
        // Only the typed width is written.
        Assert.Equal(unchecked((long)0xA5A5A5A5A5A50007), frame[ResultSlot / 8]);
    }

    [Fact]
    public void ArithmeticRejectsUnsupportedTypeAndOperation()
    {
        var program = ArithmeticProgram(QueryValueType.DateTime, QueryArithmetic.Add, QueryOperandKind.Frame, LeftSlot,
            QueryOperandKind.Frame, LeftSlot, ResultSlot);
        Assert.Contains("does not support DateTime",
            Assert.Throws<InvalidOperationException>(() => Run(program, FrameWith(1L))).Message);
        program = ArithmeticProgram(QueryValueType.Int32, (QueryArithmetic)99, QueryOperandKind.Frame, LeftSlot,
            QueryOperandKind.Frame, LeftSlot, ResultSlot);
        Assert.Contains("Unknown query arithmetic 99",
            Assert.Throws<InvalidOperationException>(() => Run(program, FrameWith(1))).Message);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // ConvertNumber

    static InterpreterProgram ConvertProgram(QueryValueType from, QueryValueType to, bool isChecked,
        QueryOperandKind kind, uint source, InterpreterBuilder? builder = null)
    {
        builder ??= new InterpreterBuilder();
        builder.AddInstruction(OpCode.ConvertNumber);
        builder.AddUInt8((byte)from);
        builder.AddUInt8((byte)to);
        builder.AddUInt8(isChecked ? (byte)1 : (byte)0);
        builder.AddUInt16(ResultSlot);
        builder.AddUInt8((byte)kind);
        builder.AddUInt32(source);
        builder.AddInstruction(OpCode.Stop);
        return builder.Materialize();
    }

    [Theory]
    [MemberData(nameof(NumericValueTypes))]
    public void ConvertNumberMatchesCSharpCasts(QueryValueType from)
    {
        var failures = new List<string>();
        var skipped = new List<string>();
        foreach (var to in NumericTypes)
        {
            foreach (var isChecked in new[] { false, true })
            {
                var program = ConvertProgram(from, to, isChecked, QueryOperandKind.Frame, LeftSlot);
                foreach (var value in Values[from])
                {
                    // The runtime binder ignores the checked conversion operators of Half, which C# uses; they
                    // convert through float.
                    var source = from == QueryValueType.Half && to != QueryValueType.Half ? (float)(Half)value : value;
                    object expected;
                    try
                    {
                        expected = Outcome(() => CastTo(ClrType(to), source, isChecked));
                    }
                    catch (RuntimeBinderException)
                    {
                        skipped.Add($"{from}->{to}");
                        continue;
                    }

                    // C# leaves unchecked out of range floating point to integer conversions unspecified (and the
                    // JIT truncates small types after converting to int); the interpreter saturates.
                    if (!isChecked && SaturatedIntegerCast(value, ClrType(to)) is { } saturated) expected = saturated;

                    var actual = Outcome(() =>
                    {
                        var frame = FrameWith(value);
                        Run(program, frame);
                        return Read(ref FrameRef(frame, ResultSlot), ClrType(to));
                    });
                    if (!Equals(expected, actual))
                        failures.Add(
                            $"{(isChecked ? "checked" : "unchecked")} ({to}){Show(value)}: expected {Show(expected)}, got {Show(actual)}");
                }
            }
        }

        Assert.Empty(failures);
        // C# has explicit conversions between all these types except Half and decimal.
        Assert.All(skipped, s => Assert.True(s is "Half->Decimal" or "Decimal->Half", s));
    }

    static readonly Dictionary<Type, (double Low, double High, object Min, object Max)> IntegerRanges = new()
    {
        [typeof(byte)] = (0, 256, byte.MinValue, byte.MaxValue),
        [typeof(sbyte)] = (-128, 128, sbyte.MinValue, sbyte.MaxValue),
        [typeof(ushort)] = (0, 65536, ushort.MinValue, ushort.MaxValue),
        [typeof(short)] = (-32768, 32768, short.MinValue, short.MaxValue),
        [typeof(uint)] = (0, 4294967296.0, uint.MinValue, uint.MaxValue),
        [typeof(int)] = (-2147483648.0, 2147483648.0, int.MinValue, int.MaxValue),
        [typeof(ulong)] = (0, 18446744073709551616.0, ulong.MinValue, ulong.MaxValue),
        [typeof(long)] = (-9223372036854775808.0, 9223372036854775808.0, long.MinValue, long.MaxValue)
    };

    // Returns the saturated result of a floating point to integer conversion whose value is out of range, else null.
    static object? SaturatedIntegerCast(object value, Type to)
    {
        double d;
        switch (value)
        {
            case Half h: d = (double)h; break;
            case float f: d = f; break;
            case double x: d = x; break;
            default: return null;
        }

        if (!IntegerRanges.TryGetValue(to, out var range)) return null;
        if (double.IsNaN(d)) return CastTo(to, 0, false);
        var truncated = Math.Truncate(d);
        if (truncated < range.Low) return range.Min;
        if (truncated >= range.High) return range.Max;
        return null;
    }

    [Fact]
    public void UncheckedOutOfRangeFloatingConversionsSaturate()
    {
        Assert.Equal((byte)0, RunConvert(QueryValueType.Double, QueryValueType.UInt8, false, -1.5));
        Assert.Equal(byte.MaxValue, RunConvert(QueryValueType.Single, QueryValueType.UInt8, false, 65504f));
        Assert.Equal(sbyte.MaxValue, RunConvert(QueryValueType.Double, QueryValueType.Int8, false, 255.9));
        Assert.Equal(0, RunConvert(QueryValueType.Double, QueryValueType.Int32, false, double.NaN));
        Assert.Equal(long.MinValue, RunConvert(QueryValueType.Half, QueryValueType.Int64, false, Half.NegativeInfinity));
        Assert.Throws<OverflowException>(() => RunConvert(QueryValueType.Double, QueryValueType.UInt8, true, -1.5));
    }

    [Fact]
    public void ConvertNumberBetweenHalfAndDecimalIsChecked()
    {
        Assert.Equal(2.5m, RunConvert(QueryValueType.Half, QueryValueType.Decimal, false, (Half)2.5f));
        Assert.Equal((Half)2.5f, RunConvert(QueryValueType.Decimal, QueryValueType.Half, false, 2.5m));
        Assert.Throws<OverflowException>(() =>
            RunConvert(QueryValueType.Half, QueryValueType.Decimal, false, Half.NaN));
    }

    static object RunConvert(QueryValueType from, QueryValueType to, bool isChecked, object value)
    {
        var frame = FrameWith(value);
        Run(ConvertProgram(from, to, isChecked, QueryOperandKind.Frame, LeftSlot), frame);
        return Read(ref FrameRef(frame, ResultSlot), ClrType(to));
    }

    [Fact]
    public void ConvertNumberReadsConstantSource()
    {
        var builder = new InterpreterBuilder();
        var constant = AddConstant(builder, -1.9);
        var program = ConvertProgram(QueryValueType.Double, QueryValueType.Int8, false, QueryOperandKind.Constant,
            constant, builder);
        var frame = new long[FrameLongs];
        Run(program, frame);
        Assert.Equal((sbyte)-1, Unsafe.ReadUnaligned<sbyte>(ref FrameRef(frame, ResultSlot)));
    }

    [Fact]
    public void ConvertNumberRejectsUnsupportedTypes()
    {
        Assert.Contains("does not support Guid", Assert.Throws<InvalidOperationException>(() =>
            Run(ConvertProgram(QueryValueType.Guid, QueryValueType.Int32, false, QueryOperandKind.Frame, LeftSlot),
                FrameWith(1))).Message);
        Assert.Contains("does not support TimeSpan", Assert.Throws<InvalidOperationException>(() =>
            Run(ConvertProgram(QueryValueType.Int32, QueryValueType.TimeSpan, false, QueryOperandKind.Frame, LeftSlot),
                FrameWith(1))).Message);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // JmpIfStringConst, JmpIfNotStringConst

    static readonly string?[] OrdinalStrings =
    [
        null, "", "\0", "a", "a\0", "A", "B", "ab", "abc", "z", "ž", "Ž", "😀", "￿", "\uD800", "􏿿", "ß",
        "ss"
    ];

    static InterpreterProgram StringConstProgram(QueryCompare compare, string? constant, bool jumpIfTrue)
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("jumped");
        builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfStringConst : OpCode.JmpIfNotStringConst);
        builder.AddUInt8((byte)compare);
        builder.AddUInt16(2);
        builder.AddUInt32((uint)builder.AddObjectConstant(constant));
        builder.AddLabelTarget(label);
        EmitBoolResultEnd(builder, label);
        return builder.Materialize();
    }

    static bool ReferenceStringCompare(QueryCompare compare, string? left, string? right) => compare switch
    {
        QueryCompare.Equal => left == right,
        QueryCompare.NotEqual => left != right,
        QueryCompare.Less => string.CompareOrdinal(left, right) < 0,
        QueryCompare.LessOrEqual => string.CompareOrdinal(left, right) <= 0,
        QueryCompare.Greater => string.CompareOrdinal(left, right) > 0,
        QueryCompare.GreaterOrEqual => string.CompareOrdinal(left, right) >= 0,
        _ => throw new ArgumentOutOfRangeException(nameof(compare))
    };

    [Fact]
    public void StringConstComparisonsMatchOrdinalCSharpSemantics()
    {
        var failures = new List<string>();
        foreach (var compare in Compares)
        {
            foreach (var constant in OrdinalStrings)
            {
                var jump = StringConstProgram(compare, constant, true);
                var notJump = StringConstProgram(compare, constant, false);
                foreach (var value in OrdinalStrings)
                {
                    var expected = ReferenceStringCompare(compare, value, constant);
                    // Copies make sure no reference equality shortcut is needed.
                    var copy = value == null ? null : new string(value.AsSpan());
                    var actual1 = Run(jump, new long[1], [null, "other", copy]);
                    var actual2 = !Run(notJump, new long[1], [null, "other", copy]);
                    if (actual1 != expected || actual2 != expected)
                        failures.Add($"{Show(value)} {compare} {Show(constant)}: expected {expected}");
                }
            }
        }

        Assert.Empty(failures);
    }

    [Fact]
    public void StringConstComparisonRejectsUnknownCompare()
    {
        Assert.Contains("Unknown query comparison 99", Assert.Throws<InvalidOperationException>(() =>
            Run(StringConstProgram((QueryCompare)99, "a", true), new long[1], [null, null, "a"])).Message);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // CaptureValue + DecodeString + JmpIfStringOp / JmpIfNotStringOp

    static byte[] Encode(QueryStringEncoding encoding, string? value)
    {
        var writer = new MemWriter();
        if (encoding == QueryStringEncoding.String) writer.WriteString(value);
        else writer.WriteStringOrdered(value);
        writer.WriteVUInt64(777);
        return writer.GetSpan().ToArray();
    }

    static InterpreterProgram StringOpProgram(QueryStringEncoding encoding, QueryStringOp operation, byte argument,
        object? constant, bool jumpIfTrue)
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("true");
        builder.AddInstruction(OpCode.CaptureValue);
        builder.AddUInt8((byte)(encoding == QueryStringEncoding.String ? CodeOp.SkipString : CodeOp.SkipStringOrdered));
        builder.AddUInt16(LeftSlot);
        builder.AddInstruction(OpCode.DecodeString);
        builder.AddUInt8((byte)encoding);
        builder.AddUInt16(LeftSlot);
        builder.AddUInt16(1);
        builder.AddUInt16(ResultSlot);
        builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfStringOp : OpCode.JmpIfNotStringOp);
        builder.AddUInt8((byte)operation);
        builder.AddUInt8(argument);
        builder.AddUInt16(1);
        builder.AddUInt16(ResultSlot);
        builder.AddUInt32((uint)builder.AddObjectConstant(constant));
        builder.AddLabelTarget(label);
        EmitBoolResultEnd(builder, label);
        return builder.Materialize();
    }

    static object RunStringOp(InterpreterProgram program, QueryStringEncoding encoding, string? value,
        object?[] objects)
    {
        return Outcome(() =>
        {
            var result = Run(program, new long[FrameLongs], objects, [], Encode(encoding, value), out _,
                out var position);
            Assert.Equal(Encode(encoding, value).Length - 2, position);
            return result;
        });
    }

    static readonly string?[] StringOpValues =
        [null, "", "abc", "ABC", "aBc", "a\0bc", "😀x", "x😀", "žluť", "ŽLUŤ", "Straße", "STRASSE", "\uD800"];

    static readonly object?[] StringOpConstants =
        [null, "", "a", "A", "B", "bc", "BC", "abc", "ABC", "😀", "ž", "Ž", "ss", "ß", 'a', 'A', 'c', "😀"[0], 'ž', 'Ž'];

    static readonly StringComparison[] Comparisons =
    [
        StringComparison.Ordinal, StringComparison.OrdinalIgnoreCase, StringComparison.InvariantCulture,
        StringComparison.InvariantCultureIgnoreCase, StringComparison.CurrentCulture
    ];

    static object ReferenceStringOp(QueryStringOp operation, StringComparison comparison, string? value,
        object? constant)
    {
        return Outcome(() => operation switch
        {
            QueryStringOp.Equals => string.Equals(value, (string?)constant, comparison),
            QueryStringOp.InstanceEquals => value!.Equals((string?)constant, comparison),
            QueryStringOp.StartsWith => constant is char c ? value!.StartsWith(c) : value!.StartsWith((string)constant!, comparison),
            QueryStringOp.EndsWith => constant is char c ? value!.EndsWith(c) : value!.EndsWith((string)constant!, comparison),
            QueryStringOp.Contains => constant is char c ? value!.Contains(c, comparison) : value!.Contains((string)constant!, comparison),
            _ => throw new ArgumentOutOfRangeException(nameof(operation))
        });
    }

    public static IEnumerable<object[]> StringOperations =>
        from operation in new[]
        {
            QueryStringOp.Equals, QueryStringOp.InstanceEquals, QueryStringOp.StartsWith, QueryStringOp.EndsWith,
            QueryStringOp.Contains
        }
        from encoding in new[] { QueryStringEncoding.String, QueryStringEncoding.StringOrdered }
        select new object[] { operation, encoding };

    [Theory]
    [MemberData(nameof(StringOperations))]
    public void StringOpsMatchCSharpStringMethods(QueryStringOp operation, QueryStringEncoding encoding)
    {
        var failures = new List<string>();
        foreach (var comparison in Comparisons)
        {
            foreach (var constant in StringOpConstants)
            {
                // C# has char overloads only for StartsWith, EndsWith (ordinal) and Contains (any comparison).
                if (constant is char && (operation is QueryStringOp.Equals or QueryStringOp.InstanceEquals ||
                                         operation != QueryStringOp.Contains && comparison != StringComparison.Ordinal))
                    continue;
                var jump = StringOpProgram(encoding, operation, (byte)comparison, constant, true);
                var notJump = StringOpProgram(encoding, operation, (byte)comparison, constant, false);
                var objects = new object?[2];
                foreach (var value in StringOpValues)
                {
                    var expected = ReferenceStringOp(operation, comparison, value, constant);
                    var actualJump = RunStringOp(jump, encoding, value, objects);
                    var actualNotJump = RunStringOp(notJump, encoding, value, objects);
                    if (actualNotJump is bool b) actualNotJump = !b;
                    if (!Equals(expected, actualJump) || !Equals(expected, actualNotJump))
                        failures.Add(
                            $"{Show(value)}.{operation}({Show(constant)}, {comparison}): expected {Show(expected)}, got {Show(actualJump)}/{Show(actualNotJump)}");
                }
            }
        }

        Assert.Empty(failures);
    }

    [Theory]
    [InlineData(QueryStringEncoding.String)]
    [InlineData(QueryStringEncoding.StringOrdered)]
    public void StringOpCompareOrdinalMatchesStringCompareOrdinal(QueryStringEncoding encoding)
    {
        var failures = new List<string>();
        var objects = new object?[2];
        foreach (var compare in Compares)
        {
            foreach (var constant in OrdinalStrings)
            {
                var jump = StringOpProgram(encoding, QueryStringOp.CompareOrdinal, (byte)compare, constant, true);
                var notJump = StringOpProgram(encoding, QueryStringOp.CompareOrdinal, (byte)compare, constant, false);
                foreach (var value in OrdinalStrings)
                {
                    var expected = ReferenceStringCompare(compare, value, constant);
                    if (!Equals(expected, RunStringOp(jump, encoding, value, objects)) ||
                        !Equals(!expected, RunStringOp(notJump, encoding, value, objects)))
                        failures.Add($"{Show(value)} {compare} {Show(constant)}: expected {expected}");
                }
            }
        }

        Assert.Empty(failures);
    }

    [Theory]
    [InlineData(QueryStringEncoding.String)]
    [InlineData(QueryStringEncoding.StringOrdered)]
    public void StringOpInSetMatchesSetMembership(QueryStringEncoding encoding)
    {
        string?[] members = ["", "abc", "žluť", "😀x", "\uD800"];
        var sets = new[] { new QueryStringSet(members), new QueryStringSet(members.Append(null)) };
        var objects = new object?[2];
        foreach (var set in sets)
        {
            var jump = StringOpProgram(encoding, QueryStringOp.InSet, 0, set, true);
            var notJump = StringOpProgram(encoding, QueryStringOp.InSet, 0, set, false);
            foreach (var value in StringOpValues.Concat(members))
            {
                var expected = value == null ? set.ContainsNull : members.Contains(value, StringComparer.Ordinal);
                Assert.True(Equals(expected, RunStringOp(jump, encoding, value, objects)), Show(value));
                Assert.True(Equals(!expected, RunStringOp(notJump, encoding, value, objects)), Show(value));
            }
        }

        Assert.Equal("[a, null]", new QueryStringSet(["a", null, "a"]).ToString());
        Assert.Equal("[]", new QueryStringSet([]).ToString());
    }

    [Fact]
    public void StringOpRejectsUnknownOperationAndCompare()
    {
        var objects = new object?[2];
        Assert.Equal(typeof(InvalidOperationException),
            RunStringOp(StringOpProgram(QueryStringEncoding.String, (QueryStringOp)99, 0, "a", true),
                QueryStringEncoding.String, "a", objects));
        Assert.Equal(typeof(InvalidOperationException),
            RunStringOp(StringOpProgram(QueryStringEncoding.String, QueryStringOp.CompareOrdinal, 99, "a", true),
                QueryStringEncoding.String, "a", objects));
    }

    [Theory]
    [InlineData(QueryStringEncoding.String)]
    [InlineData(QueryStringEncoding.StringOrdered)]
    public void DecodeStringGrowsAndReusesBufferAcrossRows(QueryStringEncoding encoding)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.CaptureValue);
        builder.AddUInt8((byte)(encoding == QueryStringEncoding.String ? CodeOp.SkipString : CodeOp.SkipStringOrdered));
        builder.AddUInt16(0);
        builder.AddInstruction(OpCode.DecodeString);
        builder.AddUInt8((byte)encoding);
        builder.AddUInt16(0);
        builder.AddUInt16(0);
        builder.AddUInt16(ResultSlot);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        var objects = new object?[1];
        var previousLength = 0;
        foreach (var value in new[]
                 {
                     "", "a", new string('x', 15) + "😀", string.Concat(Enumerable.Repeat("😀ž", 40)), "short",
                     new string('￿', 300), "\uD800 lone", "\uDC00"
                 })
        {
            var frame = new long[FrameLongs];
            Run(program, frame, objects, [], Encode(encoding, value), out _, out var position);
            var length = Unsafe.ReadUnaligned<int>(ref FrameRef(frame, ResultSlot));
            var buffer = (char[])objects[0]!;
            Assert.Equal(value, new string(buffer, 0, length));
            Assert.Equal(Encode(encoding, value).Length - 2, position);
            // Buffer only grows.
            Assert.True(buffer.Length >= previousLength);
            previousLength = buffer.Length;
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Reading and skipping key/value fields

    delegate void WriteField(ref MemWriter writer);

    sealed record FieldCase(CodeOp Read, CodeOp? Skip, WriteField Writer, object? Expected, bool IsObject);

    static readonly Dictionary<string, FieldCase> FieldCases = new()
    {
        ["UInt8"] = new(CodeOp.ReadUInt8, CodeOp.Skip1Byte, (ref MemWriter w) => w.WriteUInt8(200), (byte)200, false),
        ["Int8"] = new(CodeOp.ReadInt8, CodeOp.Skip1Byte, (ref MemWriter w) => w.WriteInt8(-128), sbyte.MinValue, false),
        ["Int8Ordered"] = new(CodeOp.ReadInt8Ordered, CodeOp.Skip1Byte, (ref MemWriter w) => w.WriteInt8Ordered(-5),
            (sbyte)-5, false),
        ["Bool"] = new(CodeOp.ReadBool, CodeOp.Skip1Byte, (ref MemWriter w) => w.WriteBool(true), true, false),
        ["VInt16"] = new(CodeOp.ReadVInt16, CodeOp.SkipVInt16, (ref MemWriter w) => w.WriteVInt16(short.MinValue),
            short.MinValue, false),
        ["VUInt16"] = new(CodeOp.ReadVUInt16, CodeOp.SkipVUInt16, (ref MemWriter w) => w.WriteVUInt16(ushort.MaxValue),
            ushort.MaxValue, false),
        ["VInt32"] = new(CodeOp.ReadVInt32, CodeOp.SkipVInt32, (ref MemWriter w) => w.WriteVInt32(int.MinValue),
            int.MinValue, false),
        ["VUInt32"] = new(CodeOp.ReadVUInt32, CodeOp.SkipVUInt32, (ref MemWriter w) => w.WriteVUInt32(uint.MaxValue),
            uint.MaxValue, false),
        ["VInt64"] = new(CodeOp.ReadVInt64, CodeOp.SkipVInt64, (ref MemWriter w) => w.WriteVInt64(long.MinValue),
            long.MinValue, false),
        ["VUInt64"] = new(CodeOp.ReadVUInt64, CodeOp.SkipVUInt64, (ref MemWriter w) => w.WriteVUInt64(ulong.MaxValue),
            ulong.MaxValue, false),
        ["Half"] = new(CodeOp.ReadHalf, CodeOp.Skip2Bytes, (ref MemWriter w) => w.WriteHalf(Half.MinValue),
            Half.MinValue, false),
        ["Single"] = new(CodeOp.ReadSingle, CodeOp.Skip4Bytes, (ref MemWriter w) => w.WriteSingle(-1.5f), -1.5f, false),
        ["Double"] = new(CodeOp.ReadDouble, CodeOp.Skip8Bytes, (ref MemWriter w) => w.WriteDouble(double.MaxValue),
            double.MaxValue, false),
        ["DoubleOrdered"] = new(CodeOp.ReadDoubleOrdered, CodeOp.Skip8Bytes,
            (ref MemWriter w) => w.WriteDoubleOrdered(-1e-300), -1e-300, false),
        ["Int64BE"] = new(CodeOp.ReadInt64BE, CodeOp.Skip8Bytes, (ref MemWriter w) => w.WriteInt64BE(-2), -2L, false),
        ["DateTime"] = new(CodeOp.ReadDateTime, CodeOp.Skip8Bytes, (ref MemWriter w) => w.WriteDateTime(BaseUtc),
            BaseUtc, false),
        ["DateTimeOffset"] = new(CodeOp.ReadDateTimeOffset, CodeOp.SkipDateTimeOffset,
            (ref MemWriter w) => w.WriteDateTimeOffset(new DateTimeOffset(BaseUtc).ToOffset(TimeSpan.FromHours(-3))),
            new DateTimeOffset(BaseUtc).ToOffset(TimeSpan.FromHours(-3)), false),
        ["TimeSpan"] = new(CodeOp.ReadTimeSpan, CodeOp.SkipVInt64, (ref MemWriter w) => w.WriteTimeSpan(TimeSpan.MinValue),
            TimeSpan.MinValue, false),
        ["Guid"] = new(CodeOp.ReadGuid, CodeOp.SkipGuid,
            (ref MemWriter w) => w.WriteGuid(new Guid("6f9619ff-8b86-d011-b42d-00cf4fc964ff")),
            new Guid("6f9619ff-8b86-d011-b42d-00cf4fc964ff"), false),
        ["Decimal"] = new(CodeOp.ReadDecimal, CodeOp.SkipDecimal, (ref MemWriter w) => w.WriteDecimal(-1.00m), -1.00m,
            false),
        ["String"] = new(CodeOp.ReadString, CodeOp.SkipString, (ref MemWriter w) => w.WriteString("a😀ž"), "a😀ž", true),
        ["NullString"] = new(CodeOp.ReadString, CodeOp.SkipString, (ref MemWriter w) => w.WriteString(null), null, true),
        ["StringOrdered"] = new(CodeOp.ReadStringOrdered, CodeOp.SkipStringOrdered,
            (ref MemWriter w) => w.WriteStringOrdered("žluť😀"), "žluť😀", true),
        ["StringInUtf8"] = new(CodeOp.ReadStringInUtf8, CodeOp.SkipStringInUtf8,
            (ref MemWriter w) => w.WriteStringInUtf8("utf8 ž"), "utf8 ž", true),
        ["ByteArray"] = new(CodeOp.ReadByteArray, CodeOp.SkipByteArray, (ref MemWriter w) => w.WriteByteArray([1, 2, 3]),
            new byte[] { 1, 2, 3 }, true),
        ["IPAddress"] = new(CodeOp.ReadIPAddress, CodeOp.SkipIPAddress,
            (ref MemWriter w) => w.WriteIPAddress(IPAddress.IPv6Loopback), IPAddress.IPv6Loopback, true),
        ["Version"] = new(CodeOp.ReadVersion, CodeOp.SkipVersion, (ref MemWriter w) => w.WriteVersion(new(1, 2, 3)),
            new Version(1, 2, 3), true)
    };

    public static IEnumerable<object[]> FieldCaseNames =>
        from name in FieldCases.Keys
        from inKey in new[] { false, true }
        select new object[] { name, inKey };

    static byte[] FieldData(FieldCase fieldCase, int repeat)
    {
        var writer = new MemWriter();
        for (var i = 0; i < repeat; i++) fieldCase.Writer(ref writer);
        writer.WriteVUInt64(4242);
        return writer.GetSpan().ToArray();
    }

    [Theory]
    [MemberData(nameof(FieldCaseNames))]
    public void ReadsAndSkipsFieldsFromKeyAndValue(string name, bool inKey)
    {
        var fieldCase = FieldCases[name];
        var data = FieldData(fieldCase, 1);
        // Read the field, then a trailing VUInt64 proves the reader advanced exactly over the field.
        var builder = new InterpreterBuilder();
        builder.AddInstruction(fieldCase.IsObject
            ? inKey ? OpCode.ReadKeyToObject : OpCode.ReadValueToObject
            : inKey ? OpCode.ReadKeyToFrame : OpCode.ReadValueToFrame);
        builder.AddUInt8((byte)fieldCase.Read);
        builder.AddUInt16(fieldCase.IsObject ? (ushort)1 : LeftSlot);
        builder.AddInstruction(inKey ? OpCode.ReadKeyToFrame : OpCode.ReadValueToFrame);
        builder.AddUInt8((byte)CodeOp.ReadVUInt64);
        builder.AddUInt16(ResultSlot);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[FrameLongs];
        var objects = new object?[] { "untouched", "previous", "untouched" };

        Run(builder.Materialize(), frame, objects, inKey ? data : [], inKey ? [] : data, out var keyPosition,
            out var valuePosition);

        Assert.Equal(4242L, frame[ResultSlot / 8]);
        Assert.Equal(inKey ? data.Length : 0, keyPosition);
        Assert.Equal(inKey ? 0 : data.Length, valuePosition);
        var actual = fieldCase.IsObject ? objects[1] : Read(ref FrameRef(frame, LeftSlot), fieldCase.Expected!.GetType());
        Assert.Equal(fieldCase.Expected, actual);
        Assert.Equal("untouched", objects[0]);
        Assert.Equal("untouched", objects[2]);

        if (fieldCase.Skip is not { } skip) return;
        foreach (var count in new ushort[] { 0, 1, 3 })
        {
            data = FieldData(fieldCase, count);
            builder = new();
            builder.AddInstruction(inKey ? OpCode.SkipKey : OpCode.SkipValue);
            builder.AddUInt8((byte)skip);
            builder.AddUInt16(count);
            builder.AddInstruction(inKey ? OpCode.ReadKeyToFrame : OpCode.ReadValueToFrame);
            builder.AddUInt8((byte)CodeOp.ReadVUInt64);
            builder.AddUInt16(ResultSlot);
            builder.AddInstruction(OpCode.Stop);
            frame = new long[FrameLongs];
            Run(builder.Materialize(), frame, [], inKey ? data : [], inKey ? [] : data, out keyPosition,
                out valuePosition);
            Assert.Equal(4242L, frame[ResultSlot / 8]);
            Assert.Equal(data.Length, inKey ? keyPosition : valuePosition);
        }
    }

    [Fact]
    public void CaptureKeyAndSkipKeyByHandlerWorkOnKeyReader()
    {
        var handler = Array.Find(BTDB.FieldHandler.BasicSerializersFactory.FieldHandlers, h => h.Name == "String")!;
        var key = new MemWriter();
        key.WriteString("skipped by handler");
        key.WriteString("captured");
        key.WriteVInt64(-9);
        var keyBytes = key.GetSpan().ToArray();
        var expected = new MemWriter();
        expected.WriteString("captured");
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("equal");
        builder.AddInstruction(OpCode.SkipKeyByHandler);
        builder.AddUInt32((uint)builder.AddObjectConstant(handler));
        builder.AddInstruction(OpCode.CaptureKey);
        builder.AddUInt8((byte)CodeOp.SkipString);
        builder.AddUInt16(LeftSlot);
        builder.AddInstruction(OpCode.StringFromCaptured);
        builder.AddUInt8((byte)QueryStringEncoding.String);
        builder.AddUInt16(LeftSlot);
        builder.AddUInt16(0);
        builder.AddInstruction(OpCode.StringLength);
        builder.AddUInt16(LeftSlot);
        builder.AddUInt16(ResultSlot);
        builder.AddInstruction(OpCode.ReadKeyToFrame);
        builder.AddUInt8((byte)CodeOp.ReadVInt64);
        builder.AddUInt16(RightSlot);
        builder.AddInstruction(OpCode.JmpIfNotEncodedEqual);
        builder.AddUInt16(LeftSlot);
        var bytes = expected.GetSpan();
        builder.AddUInt32(builder.AddConstantBytes(bytes));
        builder.AddUInt32((uint)bytes.Length);
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
        builder.MarkLabel(label);
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[FrameLongs];
        var objects = new object?[1];

        var result = Run(builder.Materialize(), frame, objects, keyBytes, [], out var keyPosition, out _);

        Assert.True(result);
        Assert.Equal("captured", objects[0]);
        Assert.Equal(8, Unsafe.ReadUnaligned<int>(ref FrameRef(frame, ResultSlot)));
        Assert.Equal(-9L, frame[RightSlot / 8]);
        Assert.Equal(keyBytes.Length, keyPosition);
        Assert.Equal(bytes.Length, Unsafe.ReadUnaligned<int>(ref FrameRef(frame, LeftSlot + 8)));
    }

    [Theory]
    [InlineData(QueryStringEncoding.String, "hello", "he", true)]
    [InlineData(QueryStringEncoding.String, "hello", "hello", true)]
    [InlineData(QueryStringEncoding.String, "he", "hello", false)]
    [InlineData(QueryStringEncoding.String, "", "", true)]
    [InlineData(QueryStringEncoding.String, "žluť", "žl", true)]
    [InlineData(QueryStringEncoding.String, "žluť", "zl", false)]
    [InlineData(QueryStringEncoding.StringOrdered, "", "", true)]
    [InlineData(QueryStringEncoding.StringOrdered, "😀x", "😀", true)]
    [InlineData(QueryStringEncoding.StringOrdered, "abc", "abd", false)]
    public void JmpIfNotEncodedStartsWithIsNegationOfStartsWith(QueryStringEncoding encoding, string value,
        string prefix, bool expected)
    {
        Assert.Equal(value.StartsWith(prefix, StringComparison.Ordinal), expected);
        var writer = new MemWriter();
        var skip = 0;
        if (encoding == QueryStringEncoding.String)
        {
            writer.WriteString(prefix);
            skip = (int)BTDB.Buffer.PackUnpack.LengthVUInt((uint)prefix.Length + 1);
        }
        else
        {
            writer.WriteStringOrderedPrefix(prefix);
        }

        var bytes = writer.GetSpan()[skip..].ToArray();
        foreach (var jumpIfTrue in new[] { true, false })
        {
            var builder = new InterpreterBuilder();
            var label = builder.DeclareLabel("jumped");
            builder.AddInstruction(OpCode.CaptureValue);
            builder.AddUInt8((byte)(encoding == QueryStringEncoding.String ? CodeOp.SkipString : CodeOp.SkipStringOrdered));
            builder.AddUInt16(0);
            builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfEncodedStartsWith : OpCode.JmpIfNotEncodedStartsWith);
            builder.AddUInt8((byte)encoding);
            builder.AddUInt16(0);
            builder.AddUInt32(builder.AddConstantBytes(bytes));
            builder.AddUInt32((uint)bytes.Length);
            builder.AddLabelTarget(label);
            EmitBoolResultEnd(builder, label);
            var result = Run(builder.Materialize(), new long[2], [], [], Encode(encoding, value), out _, out _);
            Assert.Equal(expected, jumpIfTrue ? result : !result);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // ConvertSlot between frame and object slots

    sealed class StringLengthConverterFactory : ITypeConverterFactory
    {
        public Converter? GetConverter(Type from, Type to)
        {
            if (from == typeof(string) && to == typeof(int))
                return static (ref byte f, ref byte t) =>
                    Unsafe.As<byte, int>(ref t) = Unsafe.As<byte, string?>(ref f)?.Length ?? -1;
            return null;
        }

        public void RegisterConverter<TFrom, TTo>(ActionConverter<TFrom, TTo> converter) =>
            throw new NotSupportedException();
    }

    static InterpreterProgram ConvertSlotProgram(QuerySlotKind fromKind, ushort from, QuerySlotKind toKind, ushort to,
        ITypeConverterFactory factory, Type fromType, Type toType)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.ConvertSlot);
        builder.AddUInt8((byte)fromKind);
        builder.AddUInt16(from);
        builder.AddUInt8((byte)toKind);
        builder.AddUInt16(to);
        builder.AddConverterIndex(factory, fromType, toType);
        builder.AddInstruction(OpCode.Stop);
        return builder.Materialize();
    }

    [Fact]
    public void ConvertSlotSupportsAllSlotKindCombinations()
    {
        // Frame -> Object
        var frame = FrameWith((byte)200);
        var objects = new object?[3];
        Run(ConvertSlotProgram(QuerySlotKind.Frame, LeftSlot, QuerySlotKind.Object, 2,
            DefaultTypeConverterFactory.Instance, typeof(byte), typeof(string)), frame, objects);
        Assert.Equal([null, null, "200"], objects);

        // Object -> Object
        objects = ["1.2.3", null];
        Run(ConvertSlotProgram(QuerySlotKind.Object, 0, QuerySlotKind.Object, 1, DefaultTypeConverterFactory.Instance,
            typeof(string), typeof(Version)), new long[1], objects);
        Assert.Equal(new Version(1, 2, 3), objects[1]);

        // Object -> Frame
        var program = ConvertSlotProgram(QuerySlotKind.Object, 1, QuerySlotKind.Frame, ResultSlot,
            new StringLengthConverterFactory(), typeof(string), typeof(int));
        frame = new long[FrameLongs];
        Run(program, frame, [null, "žluť😀"]);
        Assert.Equal(6, Unsafe.ReadUnaligned<int>(ref FrameRef(frame, ResultSlot)));
        Run(program, frame, [null, null]);
        Assert.Equal(-1, Unsafe.ReadUnaligned<int>(ref FrameRef(frame, ResultSlot)));

        // Frame -> Frame in place
        frame = FrameWith(-3);
        Run(ConvertSlotProgram(QuerySlotKind.Frame, LeftSlot, QuerySlotKind.Frame, LeftSlot,
            DefaultTypeConverterFactory.Instance, typeof(int), typeof(long)), frame);
        Assert.Equal(-3L, frame[LeftSlot / 8]);
    }

    [Fact]
    public void AddConverterIndexRejectsMissingConverter()
    {
        var builder = new InterpreterBuilder();
        Assert.Throws<ArgumentException>(() =>
            builder.AddConverterIndex(new StringLengthConverterFactory(), typeof(int), typeof(string)));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Misc frame ops

    [Fact]
    public void CopyToFrameCopiesExactlyTheGivenSize()
    {
        foreach (var size in new byte[] { 1, 2, 3, 4, 8, 12, 16 })
        {
            var builder = new InterpreterBuilder();
            var constant = AddConstant(builder, new Guid("00112233-4455-6677-8899-aabbccddeeff"));
            builder.AddInstruction(OpCode.CopyToFrame);
            builder.AddUInt8(size);
            builder.AddUInt16((ushort)(ResultSlot + 1));
            builder.AddUInt8((byte)QueryOperandKind.Constant);
            builder.AddUInt32(constant);
            builder.AddInstruction(OpCode.Stop);
            var frame = new long[FrameLongs];
            Run(builder.Materialize(), frame);
            var bytes = MemoryMarshal.AsBytes(frame.AsSpan()).ToArray();
            var source = new Guid("00112233-4455-6677-8899-aabbccddeeff").ToByteArray();
            for (var i = 0; i < bytes.Length; i++)
            {
                var expected = i > ResultSlot && i <= ResultSlot + size ? source[i - ResultSlot - 1] : 0;
                Assert.Equal(expected, bytes[i]);
            }
        }
    }

    [Fact]
    public void ThrowQueryExceptionThrowsMappedExceptions()
    {
        static InterpreterProgram Program(QueryException exception)
        {
            var builder = new InterpreterBuilder();
            builder.AddInstruction(OpCode.ThrowQueryException);
            builder.AddUInt8((byte)exception);
            builder.AddInstruction(OpCode.Stop);
            return builder.Materialize();
        }

        Assert.Throws<NullReferenceException>(() => Run(Program(QueryException.NullReference), new long[1]));
        Assert.Equal("Nullable object must have a value.", Assert.Throws<InvalidOperationException>(() =>
            Run(Program(QueryException.NullableHasNoValue), new long[1])).Message);
        Assert.Equal("Unknown query exception 99.", Assert.Throws<InvalidOperationException>(() =>
            Run(Program((QueryException)99), new long[1])).Message);
    }

    [Fact]
    public void EqualDateTimeComparesTicksIgnoringKind()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.EqualDateTime);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        foreach (var left in Values[QueryValueType.DateTime].Cast<DateTime>())
        {
            foreach (var right in Values[QueryValueType.DateTime].Cast<DateTime>())
            {
                var l = left;
                var r = right;
                var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
                    ref Unsafe.As<DateTime, byte>(ref l), ref Unsafe.As<DateTime, byte>(ref r), stack, program);
                Interpreter.Run(ref ctx);
                Assert.Equal(left == right, ctx.BoolResult);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Try/finally interaction

    [Fact]
    public void StopInsideNestedTryRunsAllFinallyBlocksInnerFirst()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddTry();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(0);
        builder.AddUInt8(1);
        builder.AddInstruction(OpCode.Stop);
        builder.AddFinally();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(1);
        builder.AddUInt8(2);
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(5);
        builder.AddUInt8(9);
        builder.AddFinally();
        // Records whether the inner finally already ran.
        builder.AddInstruction(OpCode.CopyToFrame);
        builder.AddUInt8(1);
        builder.AddUInt16(3);
        builder.AddUInt8((byte)QueryOperandKind.Frame);
        builder.AddUInt32(1);
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(2);
        builder.AddUInt8(3);
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(4);
        builder.AddUInt8(9);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[1];

        Run(builder.Materialize(), frame);

        Assert.Equal(new byte[] { 1, 2, 3, 2, 0, 0, 0, 0 }, BitConverter.GetBytes(frame[0]));
    }

    [Fact]
    public void ExceptionOutsideOfTryBlocksPropagatesFromProgramWithTryBlocks()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddCatch();
        builder.AddInstruction(OpCode.CatchedException);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.ThrowQueryException);
        builder.AddUInt8((byte)QueryException.NullReference);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        var frame = new long[1];
        var objects = new object?[1];
        unsafe
        {
            var keyReader = new MemReader(null, 0);
            var valueReader = new MemReader(null, 0);
            var ctx = new InterpreterCtx(program, ref Unsafe.As<long, byte>(ref frame[0]), ref objects[0],
                ref keyReader, ref valueReader);
            var thrown = false;
            try
            {
                Interpreter.Run(ref ctx);
            }
            catch (NullReferenceException exception)
            {
                thrown = true;
                Assert.Same(exception, ctx.CurrentException);
            }

            Assert.True(thrown);
            Assert.True(ctx.BoolResult);
        }
    }

    [Fact]
    public void RethrowInsideFinallyPropagatesCurrentException()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddInstruction(OpCode.ThrowQueryException);
        builder.AddUInt8((byte)QueryException.NullableHasNoValue);
        builder.AddFinally();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(0);
        builder.AddUInt8(1);
        builder.AddInstruction(OpCode.Rethrow);
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(1);
        builder.AddUInt8(1);
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[1];

        Assert.Contains("Nullable object", Assert.Throws<InvalidOperationException>(() =>
            Run(builder.Materialize(), frame)).Message);
        Assert.Equal(1, frame[0]);
    }

    [Fact]
    public void ExceptionInsideCatchRunsFinallyAndPropagates()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddInstruction(OpCode.ThrowQueryException);
        builder.AddUInt8((byte)QueryException.NullableHasNoValue);
        builder.AddCatch();
        builder.AddInstruction(OpCode.ThrowQueryException);
        builder.AddUInt8((byte)QueryException.NullReference);
        builder.AddFinally();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(0);
        builder.AddUInt8(1);
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(1);
        builder.AddUInt8(1);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[1];

        Assert.Throws<NullReferenceException>(() => Run(builder.Materialize(), frame));
        Assert.Equal(1, frame[0]);
    }

    [Fact]
    public void ExceptionInsideCatchWithoutFinallyIsCaughtByOuterCatch()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddTry();
        builder.AddInstruction(OpCode.ThrowQueryException);
        builder.AddUInt8((byte)QueryException.NullableHasNoValue);
        builder.AddCatch();
        builder.AddInstruction(OpCode.ThrowQueryException);
        builder.AddUInt8((byte)QueryException.NullReference);
        builder.AddTryEnd();
        builder.AddCatch();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(0);
        builder.AddUInt8(1);
        builder.AddInstruction(OpCode.CatchedException);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(1);
        builder.AddUInt8(1);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[1];
        var objects = new object?[1];
        unsafe
        {
            var keyReader = new MemReader(null, 0);
            var valueReader = new MemReader(null, 0);
            var ctx = new InterpreterCtx(builder.Materialize(), ref Unsafe.As<long, byte>(ref frame[0]),
                ref objects[0], ref keyReader, ref valueReader);
            Interpreter.Run(ref ctx);
            Assert.Null(ctx.CurrentException);
        }

        Assert.Equal(0x0101, frame[0]);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Builder errors and DumpAsm

    class NoAccessorObject
    {
        public int Value;
    }

    static unsafe void RegisterNoAccessorObjectMetadata()
    {
        if (ReflectionMetadata.FindByType(typeof(NoAccessorObject)) != null) return;
        ReflectionMetadata.Register(new()
        {
            Type = typeof(NoAccessorObject),
            Name = nameof(NoAccessorObject),
            Namespace = typeof(NoAccessorObject).Namespace ?? "",
            Implements = [],
            Creator = &CreateNoAccessorObject,
            Fields = [new FieldMetadata { Name = nameof(NoAccessorObject.Value), Type = typeof(int), ByteOffset = 0 }]
        });
    }

    static object CreateNoAccessorObject() => new NoAccessorObject();

    [Fact]
    public void AccessorParametersRequireRegisteredFieldWithAccessor()
    {
        RegisterNoAccessorObjectMetadata();
        var builder = new InterpreterBuilder();
        Assert.Contains("does not have getter metadata", Assert.Throws<ArgumentException>(() =>
            builder.AddGetterParameter(typeof(NoAccessorObject), nameof(NoAccessorObject.Value))).Message);
        Assert.Contains("does not have setter metadata", Assert.Throws<ArgumentException>(() =>
            builder.AddSetterParameter(typeof(NoAccessorObject), nameof(NoAccessorObject.Value))).Message);
        Assert.Contains("Missing was not found", Assert.Throws<ArgumentException>(() =>
            builder.AddGetterParameter(typeof(NoAccessorObject), "Missing")).Message);
        Assert.Contains("is not registered", Assert.Throws<ArgumentException>(() =>
            builder.AddSetterParameter(typeof(InterpreterFrameOpsCoverageTest), "Missing")).Message);
    }

    [Fact]
    public void DumpAsmPrintsAllFrameOps()
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("target");
        var intConstant = builder.AllocAlignedConst(4);
        builder.ConstSpan<int>(intConstant) = 7;
        var handler = Array.Find(BTDB.FieldHandler.BasicSerializersFactory.FieldHandlers, h => h.Name == "String")!;

        builder.AddInstruction(OpCode.ReadKeyToFrame);
        builder.AddUInt8((byte)CodeOp.ReadVUInt64);
        builder.AddUInt16(8);
        builder.AddInstruction(OpCode.ReadKeyToObject);
        builder.AddUInt8((byte)CodeOp.ReadString);
        builder.AddUInt16(1);
        builder.AddInstruction(OpCode.SkipKey);
        builder.AddUInt8((byte)CodeOp.SkipVInt32);
        builder.AddUInt16(3);
        builder.AddInstruction(OpCode.JmpIfFrameFrame);
        builder.AddUInt8((byte)QueryValueType.Double);
        builder.AddUInt8((byte)QueryCompare.LessOrEqual);
        builder.AddUInt16(8);
        builder.AddUInt16(16);
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.JmpIfNotFrameFrame);
        builder.AddUInt8((byte)QueryValueType.Guid);
        builder.AddUInt8((byte)QueryCompare.NotEqual);
        builder.AddUInt16(24);
        builder.AddUInt16(40);
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.ConvertNumber);
        builder.AddUInt8((byte)QueryValueType.Int32);
        builder.AddUInt8((byte)QueryValueType.UInt8);
        builder.AddUInt8(1);
        builder.AddUInt16(32);
        builder.AddUInt8((byte)QueryOperandKind.Constant);
        builder.AddUInt32(intConstant);
        builder.AddInstruction(OpCode.ConvertNumber);
        builder.AddUInt8((byte)QueryValueType.Half);
        builder.AddUInt8((byte)QueryValueType.Double);
        builder.AddUInt8(0);
        builder.AddUInt16(32);
        builder.AddUInt8((byte)QueryOperandKind.Frame);
        builder.AddUInt32(8);
        builder.AddInstruction(OpCode.CaptureKey);
        builder.AddUInt8((byte)CodeOp.SkipStringOrdered);
        builder.AddUInt16(48);
        builder.AddInstruction(OpCode.JmpIfEncodedStartsWith);
        builder.AddUInt8((byte)QueryStringEncoding.StringOrdered);
        builder.AddUInt16(48);
        builder.AddUInt32(builder.AddConstantBytes([0x62, 0x63]));
        builder.AddUInt32(2);
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.JmpIfNotEncodedStartsWith);
        builder.AddUInt8((byte)QueryStringEncoding.String);
        builder.AddUInt16(48);
        builder.AddUInt32(builder.AddConstantBytes([0x63]));
        builder.AddUInt32(1);
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.DecodeString);
        builder.AddUInt8((byte)QueryStringEncoding.String);
        builder.AddUInt16(48);
        builder.AddUInt16(2);
        builder.AddUInt16(64);
        builder.AddInstruction(OpCode.JmpIfNotStringOp);
        builder.AddUInt8((byte)QueryStringOp.CompareOrdinal);
        builder.AddUInt8((byte)QueryCompare.Greater);
        builder.AddUInt16(2);
        builder.AddUInt16(64);
        builder.AddUInt32((uint)builder.AddObjectConstant("abc"));
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.JmpIfStringOp);
        builder.AddUInt8((byte)QueryStringOp.InSet);
        builder.AddUInt8(0);
        builder.AddUInt16(2);
        builder.AddUInt16(64);
        builder.AddUInt32((uint)builder.AddObjectConstant(new QueryStringSet(["x", null])));
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.JmpIfStringOp);
        builder.AddUInt8((byte)QueryStringOp.Equals);
        builder.AddUInt8((byte)StringComparison.OrdinalIgnoreCase);
        builder.AddUInt16(2);
        builder.AddUInt16(64);
        builder.AddUInt32((uint)builder.AddObjectConstant(null));
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.StringLength);
        builder.AddUInt16(48);
        builder.AddUInt16(72);
        builder.AddInstruction(OpCode.JmpIfFrameInSet);
        builder.AddUInt8((byte)QueryValueType.Int64);
        builder.AddUInt16(8);
        builder.AddUInt32((uint)builder.AddObjectConstant(new HashSet<long> { 1 }));
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.JmpIfNotFrameInSet);
        builder.AddUInt8((byte)QueryValueType.Int64);
        builder.AddUInt16(8);
        builder.AddUInt32(0);
        // A jump target written without a label is printed as unknown.
        builder.AddUInt32(0);
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(80);
        builder.AddUInt8(42);
        builder.AddInstruction(OpCode.ThrowQueryException);
        builder.AddUInt8((byte)QueryException.NullableHasNoValue);
        builder.AddInstruction(OpCode.CopyToFrame);
        builder.AddUInt8(16);
        builder.AddUInt16(88);
        builder.AddUInt8((byte)QueryOperandKind.Frame);
        builder.AddUInt32(8);
        builder.AddInstruction(OpCode.SkipKeyByHandler);
        builder.AddUInt32((uint)builder.AddObjectConstant(handler));
        builder.AddInstruction(OpCode.SkipValueByHandler);
        builder.AddUInt32((uint)builder.AddObjectConstant(handler));
        builder.AddInstruction(OpCode.ConvertSlot);
        builder.AddUInt8((byte)QuerySlotKind.Frame);
        builder.AddUInt16(8);
        builder.AddUInt8((byte)QuerySlotKind.Object);
        builder.AddUInt16(3);
        builder.AddConverterIndex(DefaultTypeConverterFactory.Instance, typeof(byte), typeof(string));
        builder.AddInstruction(OpCode.ConvertSlot);
        builder.AddUInt8((byte)QuerySlotKind.Object);
        builder.AddUInt16(1);
        builder.AddUInt8((byte)QuerySlotKind.Frame);
        builder.AddUInt16(8);
        // A raw converter index without resolved types is printed as a number.
        builder.AddUInt32(5);
        builder.AddInstruction(OpCode.Jmp);
        // A label index which was never declared gets a generated name.
        builder.AddVUInt64(1000);
        builder.MarkLabel(label);
        builder.AddInstruction(OpCode.Stop);

        var sb = new StringBuilder();
        builder.DumpAsm(sb);
        var lines = sb.ToString().Split('\n').Select(l => l.Trim()).Where(l => l.Length > 0)
            .Select(l => l.Length > 6 && l[4] == ':' ? l[6..] : l).ToList();

        Assert.Contains("ReadKeyToFrame ReadVUInt64 frame[8]", lines);
        Assert.Contains("ReadKeyToObject ReadString objects[1]", lines);
        Assert.Contains("SkipKey SkipVInt32 x3", lines);
        Assert.Contains("JmpIfFrameFrame Double LessOrEqual frame[8] frame[16] -> target", lines);
        Assert.Contains("JmpIfNotFrameFrame Guid NotEqual frame[24] frame[40] -> target", lines);
        Assert.Contains($"ConvertNumber Int32 -> UInt8 checked frame[32] = const[{intConstant}] ; 7", lines);
        Assert.Contains("ConvertNumber Half -> Double frame[32] = frame[8]", lines);
        Assert.Contains("CaptureKey SkipStringOrdered frame[48]", lines);
        Assert.Contains(lines, l => l.StartsWith("JmpIfEncodedStartsWith StringOrdered frame[48] const[") &&
                                    l.EndsWith("] 0x6263 -> target"));
        Assert.Contains(lines, l => l.StartsWith("JmpIfNotEncodedStartsWith String frame[48] const[") &&
                                    l.EndsWith("] 0x63 -> target"));
        Assert.Contains("DecodeString String frame[48] -> objects[2] length frame[64]", lines);
        Assert.Contains("JmpIfNotStringOp CompareOrdinal Greater objects[2] length frame[64] 0 ; abc -> target", lines);
        Assert.Contains("JmpIfStringOp InSet  objects[2] length frame[64] 1 ; [x, null] -> target", lines);
        Assert.Contains("JmpIfStringOp Equals OrdinalIgnoreCase objects[2] length frame[64] 2 ; null -> target", lines);
        Assert.Contains("StringLength frame[48] -> frame[72]", lines);
        Assert.Contains("JmpIfFrameInSet Int64 frame[8] 3 -> target", lines);
        Assert.Contains("JmpIfNotFrameInSet Int64 frame[8] 0 -> ?", lines);
        Assert.Contains("SetFrameByte frame[80] = 42", lines);
        Assert.Contains("ThrowQueryException NullableHasNoValue", lines);
        Assert.Contains("CopyToFrame 16 bytes frame[88] = frame[8]", lines);
        Assert.Contains("SkipKeyByHandler 4 ; String", lines);
        Assert.Contains("SkipValueByHandler 4 ; String", lines);
        Assert.Contains("ConvertSlot Frame[8] -> Object[3] System.Byte -> System.String", lines);
        Assert.Contains("ConvertSlot Object[1] -> Frame[8] 5", lines);
        Assert.Contains("Jmp L1000", lines);
        Assert.Contains("target:", lines);
    }
}
