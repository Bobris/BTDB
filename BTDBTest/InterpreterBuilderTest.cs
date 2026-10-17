using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using BTDB.Interpreter;
using BTDB.Serialization;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class InterpreterBuilderTest
{
    [Fact]
    public void ObjectConstantsAreDeduplicatedByValue()
    {
        var builder = new InterpreterBuilder();

        Assert.Equal(0, builder.AddObjectConstant(null));
        Assert.Equal(0, builder.AddObjectConstant(null));
        Assert.Equal(1, builder.AddObjectConstant(new string("same")));
        Assert.Equal(1, builder.AddObjectConstant("same"));
        Assert.Equal(2, builder.AddObjectConstant("other"));

        var program = builder.Materialize();
        Assert.Equal([null, "same", "other"], program.ObjectConstants);
    }

    [Fact]
    public void AddInstructionWritesOpCode()
    {
        var builder = new InterpreterBuilder();

        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);

        var program = builder.Materialize();
        Assert.Equal(2, program.Program.Length);
        Assert.Equal((byte)OpCode.SetBoolResultTrue, program.Program[0]);
        Assert.Equal((byte)OpCode.Stop, program.Program[1]);
    }

    [Fact]
    public void MaterializeRejectsProgramWithoutStop()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.SetBoolResultTrue);

        var thrown = false;
        try
        {
            builder.Materialize();
        }
        catch (InvalidOperationException ex)
        {
            thrown = true;
            Assert.Contains("Stop", ex.Message);
        }

        Assert.True(thrown);
    }

    [Fact]
    public void MaterializeRejectsProgramWithoutTryEnd()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddInstruction(OpCode.Stop);

        var thrown = false;
        try
        {
            builder.Materialize();
        }
        catch (InvalidOperationException ex)
        {
            thrown = true;
            Assert.Contains("Try block was not closed", ex.Message);
        }

        Assert.True(thrown);
    }

    [Theory]
    [InlineData(OpCode.StackAllocObject)]
    [InlineData(OpCode.StackBytesAlloc)]
    public void MaterializeRejectsStackAllocWithoutNestedStop(OpCode opCode)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(opCode);
        builder.AddVUInt64(1);
        builder.AddInstruction(OpCode.Stop);

        var thrown = false;
        try
        {
            builder.Materialize();
        }
        catch (InvalidOperationException ex)
        {
            thrown = true;
            Assert.Contains("Stack allocation", ex.Message);
        }

        Assert.True(thrown);
    }

    [Fact]
    public void MaterializeRejectsStackStructAllocWithoutNestedStop()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.StackStructAlloc);
        builder.AddStackStructTypeParameter(typeof(int));
        builder.AddInstruction(OpCode.Stop);

        var thrown = false;
        try
        {
            builder.Materialize();
        }
        catch (InvalidOperationException ex)
        {
            thrown = true;
            Assert.Contains("Stack allocation", ex.Message);
        }

        Assert.True(thrown);
    }

    [Fact]
    public void AllocAlignedConstUsesDefaultAlignmentClampedToEight()
    {
        var builder = new InterpreterBuilder();

        Assert.Equal((uint)0, builder.AllocAlignedConst(3));
        Assert.Equal((uint)8, builder.AllocAlignedConst(16));

        var program = builder.Materialize();
        Assert.Equal(24, program.Constants.Length);
        Assert.Equal(24u, program.ProgramOffset);
        Assert.Equal(24, program.Image.Length);
    }

    [Fact]
    public void ConstSpanReadsAndWritesUnmanagedConstants()
    {
        var builder = new InterpreterBuilder();

        Assert.Equal((uint)0, builder.AllocAlignedConst(1));
        var offset = builder.AllocAlignedConst(sizeof(int));
        builder.ConstSpan<int>(offset) = 0x04030201;
        builder.AddInstruction(OpCode.Stop);

        var program = builder.Materialize();
        Assert.Equal((uint)4, offset);
        Assert.Equal((uint)8, program.ProgramOffset);
        Assert.Equal(9, program.Image.Length);
        Assert.Equal([1, 2, 3, 4], program.Constants.Slice((int)offset, sizeof(int)).ToArray());
        Assert.Equal((byte)OpCode.Stop, program.Program[0]);

        Span<byte> stack = stackalloc byte[1];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);
        Assert.Equal(0x04030201, Unsafe.As<byte, int>(ref Unsafe.Add(ref ctx.BeginConstants, (nint)offset)));
    }

    [Fact]
    public void LabelsStoreMarkedProgramOffset()
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("target");
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.MarkLabel(label);
        builder.AddInstruction(OpCode.Stop);

        var program = builder.Materialize();

        Assert.Equal((uint)0, label);
        Assert.Equal((uint)1, MemoryMarshal.Read<uint>(program.Constants));
    }

    [Fact]
    public void AddLabelParameterStoresLabelOffsetDividedByFour()
    {
        var builder = new InterpreterBuilder();
        builder.DeclareLabel();
        var label = builder.DeclareLabel();
        builder.AddInstruction(OpCode.Jmp);
        builder.AddLabelParameter(label);
        builder.AddInstruction(OpCode.Stop);

        var program = builder.Materialize();

        Assert.Equal((uint)4, label);
        Assert.Equal([(byte)OpCode.Jmp, 1, (byte)OpCode.Stop], program.Program.ToArray());
    }

    [Fact]
    public void DumpAsmIncludesOffsetsLabelsAndParameters()
    {
        var builder = new InterpreterBuilder();
        var objectIndex = builder.AddObjectConstant("value");
        var constOffset = builder.AllocAlignedConst(sizeof(int));
        builder.ConstSpan<int>(constOffset) = 123;
        var label = builder.DeclareLabel("target");
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)objectIndex);
        builder.AddInstruction(OpCode.SetParam2Const);
        builder.AddVUInt64(constOffset);
        builder.AddInstruction(OpCode.Jmp);
        builder.AddLabelParameter(label);
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.MarkLabel(label);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
constants:
  0000: Int32 123
  0004: UInt32 7
program:
  0000: SetParam1ObjectConst 0 ; value
  0002: SetParam2Const 0 ; 123
  0004: Jmp target
  0006: SetBoolResultFalse
target:
  0007: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void DumpAsmSupportsGuidConstants()
    {
        var builder = new InterpreterBuilder();
        var offset = builder.AllocAlignedConst((uint)Unsafe.SizeOf<Guid>());
        var guid = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        builder.ConstSpan<Guid>(offset) = guid;
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
constants:
  0000: Guid 00112233-4455-6677-8899-aabbccddeeff
program:
""" + "\n", sb.ToString());
    }

    [Fact]
    public void TryCatchFinallyMarkersMaterializeCatchSkipJump()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddCatch();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);

        var program = builder.Materialize();

        Assert.Equal([(byte)OpCode.SetBoolResultFalse, (byte)OpCode.JmpFinally, 0,
            (byte)OpCode.SetBoolResultTrue, (byte)OpCode.Stop], program.Program.ToArray());
        Assert.Equal([(uint)0, (uint)3, (uint)4, (uint)4], program.TryCatchFinallyBlocks);
    }

    [Fact]
    public void DumpAsmIncludesVirtualTryInstructionsAndIndent()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddTry();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddFinally();
        builder.AddInstruction(OpCode.NegateBoolResult);
        builder.AddTryEnd();
        builder.AddCatch();
        builder.AddInstruction(OpCode.CatchedException);
        builder.AddFinally();
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
Try
    0000: SetBoolResultFalse
  Try
      0001: SetBoolResultTrue
  Finally
      0002: NegateBoolResult
  TryEnd
    0003: JmpFinally 0
Catch
    0005: CatchedException
Finally
    0006: EndOfFinally
TryEnd
  0007: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void InterpreterRunsOpCodesUntilStop()
    {
        var builder = new InterpreterBuilder();
        var param1Index = builder.AddObjectConstant("value");
        var param2Index = builder.AddObjectConstant("other");
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)param1Index);
        builder.AddInstruction(OpCode.SetParam2ObjectConst);
        builder.AddVUInt64((ulong)param2Index);
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        object? param2 = null;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<object?, byte>(ref param1), ref Unsafe.As<object?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.False(ctx.BoolResult);
        Assert.Same("value", param1);
        Assert.Same("other", param2);
        Assert.Equal((uint)0, ctx.SP);
        Assert.Equal((uint)0, ctx.BP);
    }

    [Fact]
    public void SetParamConstLoadsReferenceToUnmanagedConstant()
    {
        var builder = new InterpreterBuilder();
        var offset1 = builder.AllocAlignedConst(sizeof(int));
        var offset2 = builder.AllocAlignedConst(sizeof(int));
        builder.ConstSpan<int>(offset1) = 123;
        builder.ConstSpan<int>(offset2) = 456;
        builder.AddInstruction(OpCode.SetParam1Const);
        builder.AddVUInt64(offset1);
        builder.AddInstruction(OpCode.SetParam2Const);
        builder.AddVUInt64(offset2);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Assert.Equal([(byte)OpCode.SetParam1Const, 0, (byte)OpCode.SetParam2Const, 4, (byte)OpCode.Stop],
            program.Program.ToArray());
        Span<byte> stack = stackalloc byte[1];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal(123, Unsafe.As<byte, int>(ref ctx.Param1));
        Assert.Equal(456, Unsafe.As<byte, int>(ref ctx.Param2));
    }

    [Fact]
    public void PushStackAndPopStackChangeStackPointer()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.PushStack);
        builder.AddVUInt64(24);
        builder.AddInstruction(OpCode.PopStack);
        builder.AddVUInt64(8);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[32];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal((uint)16, ctx.SP);
    }

    [Fact]
    public void PushStackDoublesStackWhenThereIsNotEnoughSpace()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.PushStack);
        builder.AddVUInt64(20);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[4];
        stack[0] = 42;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal((uint)20, ctx.SP);
        Assert.Equal(32, ctx.Stack.Length);
        Assert.Equal(42, ctx.Stack[0]);
    }

    [Fact]
    public void DumpAsmIncludesStackParameters()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.PushStack);
        builder.AddVUInt64(24);
        builder.AddInstruction(OpCode.PopStack);
        builder.AddVUInt64(8);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: PushStack 24
  0002: PopStack 8
  0004: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void SetBySPPointsRefsIntoStack()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.PushStack);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.SetResultBySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.SetParam1BySP);
        builder.AddVUInt64(8);
        builder.AddInstruction(OpCode.SetParam2BySP);
        builder.AddVUInt64(4);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[16];
        stack.Clear();
        byte result = 0;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref result, ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        ctx.Result = 11;
        ctx.Param1 = 22;
        ctx.Param2 = 33;
        Assert.Equal(11, stack[0]);
        Assert.Equal(22, stack[8]);
        Assert.Equal(33, stack[12]);
    }

    [Fact]
    public void DumpAsmIncludesSetBySPParameters()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.SetResultBySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.SetParam1BySP);
        builder.AddVUInt64(8);
        builder.AddInstruction(OpCode.SetParam2BySP);
        builder.AddVUInt64(4);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: SetResultBySP 16
  0002: SetParam1BySP 8
  0004: SetParam2BySP 4
  0006: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void StackAllocObjectRunsNestedProgramUntilStop()
    {
        var builder = new InterpreterBuilder();
        var nestedIndex = builder.AddObjectConstant("nested");
        var afterIndex = builder.AddObjectConstant("after");
        builder.AddInstruction(OpCode.StackAllocObject);
        builder.AddVUInt64(2);
        builder.AddInstruction(OpCode.SetParam2ObjectConst);
        builder.AddVUInt64((ulong)nestedIndex);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)afterIndex);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        object? param2 = null;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<object?, byte>(ref param1), ref Unsafe.As<object?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.Same("after", param1);
    }

    [Fact]
    public void StackStructAllocRunsNestedProgramWithParam2PointingToStructStorage()
    {
        var builder = new InterpreterBuilder();
        var valueOffset = builder.AllocAlignedConst(sizeof(int));
        builder.ConstSpan<int>(valueOffset) = 42;
        builder.AddInstruction(OpCode.StackStructAlloc);
        builder.AddStackStructTypeParameter(typeof(int));
        builder.AddInstruction(OpCode.AssignRefResultParam2);
        builder.AddInstruction(OpCode.SetParam1Const);
        builder.AddVUInt64(valueOffset);
        builder.AddInstruction(OpCode.AddInt32);
        builder.AddInstruction(OpCode.SetParam1Const);
        builder.AddVUInt64(valueOffset);
        builder.AddInstruction(OpCode.EqualInt32);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte result = 0;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref result, ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
    }

    [Fact]
    public void StackBytesAllocRunsNestedProgramWithParam2PointingToBytes()
    {
        var builder = new InterpreterBuilder();
        var valueOffset = builder.AllocAlignedConst(sizeof(uint));
        builder.ConstSpan<uint>(valueOffset) = 0x12345678;
        builder.AddInstruction(OpCode.StackBytesAlloc);
        builder.AddVUInt64(sizeof(uint));
        builder.AddInstruction(OpCode.AssignRefResultParam2);
        builder.AddInstruction(OpCode.SetParam1Const);
        builder.AddVUInt64(valueOffset);
        builder.AddInstruction(OpCode.AddUInt32);
        builder.AddInstruction(OpCode.SetParam1Const);
        builder.AddVUInt64(valueOffset);
        builder.AddInstruction(OpCode.EqualUInt32);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte result = 0;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref result, ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
    }

    [Fact]
    public void ExceptionInsideStackAllocationPropagates()
    {
        var builder = new InterpreterBuilder();
        var exceptionIndex = builder.AddObjectConstant(new InvalidOperationException("inside"));
        builder.AddInstruction(OpCode.StackAllocObject);
        builder.AddVUInt64(1);
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)exceptionIndex);
        builder.AddInstruction(OpCode.ThrowException);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();

        var exception = Assert.Throws<InvalidOperationException>(() => RunWithObjectParams(program, out _));
        Assert.Equal("inside", exception.Message);
    }

    [Fact]
    public void ExceptionInsideStackAllocationIsCaughtByOuterCatch()
    {
        var builder = new InterpreterBuilder();
        var exceptionIndex = builder.AddObjectConstant(new InvalidOperationException("inside"));
        builder.AddTry();
        builder.AddInstruction(OpCode.StackAllocObject);
        builder.AddVUInt64(1);
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)exceptionIndex);
        builder.AddInstruction(OpCode.ThrowException);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddCatch();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.CatchedException);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();

        RunWithObjectParams(program, out var boolResult);
        Assert.True(boolResult);
    }

    static void RunWithObjectParams(InterpreterProgram program, out bool boolResult)
    {
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        object? param2 = null;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<object?, byte>(ref param1), ref Unsafe.As<object?, byte>(ref param2), stack, program);
        Interpreter.Run(ref ctx);
        boolResult = ctx.BoolResult;
    }

    [Fact]
    public void NestedStackAllocationsUnwindOneScopeAtATime()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.StackBytesAlloc);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.StackAllocObject);
        builder.AddVUInt64(2);
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte result = 0;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref result, ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
    }

    [Fact]
    public unsafe void CallGetterAndSetterUseResolvedFieldAccessors()
    {
        RegisterAccessorTestObjectMetadata();
        var builder = new InterpreterBuilder();
        var target = new AccessorTestObject { Value = 41 };
        var targetIndex = builder.AddObjectConstant(target);
        var oneOffset = builder.AllocAlignedConst(sizeof(int));
        builder.ConstSpan<int>(oneOffset) = 1;
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)targetIndex);
        builder.AddInstruction(OpCode.CallGetter);
        builder.AddGetterParameter(typeof(AccessorTestObject), nameof(AccessorTestObject.Value));
        builder.AddInstruction(OpCode.AssignRefParam2Result);
        builder.AddInstruction(OpCode.SetParam1Const);
        builder.AddVUInt64(oneOffset);
        builder.AddInstruction(OpCode.AddInt32);
        builder.AddInstruction(OpCode.AssignRefParam2Result);
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)targetIndex);
        builder.AddInstruction(OpCode.CallSetter);
        builder.AddSetterParameter(typeof(AccessorTestObject), nameof(AccessorTestObject.Value));
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        int result = 0;
        object? param1 = null;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref Unsafe.As<int, byte>(ref result), ref Unsafe.As<object?, byte>(ref param1),
            ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal(42, target.Value);
    }

    [Fact]
    public unsafe void DumpAsmIncludesGetterAndSetterFieldParameters()
    {
        RegisterAccessorTestObjectMetadata();
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.CallGetter);
        builder.AddGetterParameter(typeof(AccessorTestObject), nameof(AccessorTestObject.Value));
        builder.AddInstruction(OpCode.CallSetter);
        builder.AddSetterParameter(typeof(AccessorTestObject), nameof(AccessorTestObject.Value));
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Contains("CallGetter BTDBTest.InterpreterBuilderTest+AccessorTestObject.Value", sb.ToString());
        Assert.Contains("CallSetter BTDBTest.InterpreterBuilderTest+AccessorTestObject.Value", sb.ToString());
    }

    [Fact]
    public void ConvertParam1ToResultUsesResolvedConverter()
    {
        var factory = new DefaultTypeConverterFactory();
        factory.RegisterConverter<int, long>((in int from, out long to) => to = from + 10);
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.ConvertParam1ToResult);
        builder.AddConverterParameter(factory, typeof(int), typeof(long));
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        long result = 0;
        int param1 = 32;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref Unsafe.As<long, byte>(ref result), ref Unsafe.As<int, byte>(ref param1),
            ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal(42, result);
    }

    [Fact]
    public void DumpAsmIncludesConverterTypesAndNonDefaultFactory()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.ConvertParam1ToResult);
        builder.AddConverterParameter(typeof(int), typeof(long));
        var factory = new TestTypeConverterFactory();
        builder.AddInstruction(OpCode.ConvertParam1ToResult);
        builder.AddConverterParameter(factory, typeof(int), typeof(long));
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Contains("  0000: ConvertParam1ToResult System.Int32 -> System.Int64\n", sb.ToString());
        Assert.Contains(
            "  0002: ConvertParam1ToResult System.Int32 -> System.Int64 ; BTDBTest.InterpreterBuilderTest+TestTypeConverterFactory\n",
            sb.ToString());
    }

    [Fact]
    public void AllocObjectToResultCreatesObjectUsingClassMetadata()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.AllocObjectToResult);
        builder.AddTypeParameter(typeof(object));
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? result = null;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref Unsafe.As<object?, byte>(ref result), ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.NotNull(result);
        Assert.IsType<object>(result);
    }

    [Fact]
    public void AllocObjectToParam1AndParam2CreateObjectsUsingClassMetadata()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.AllocObjectToParam1);
        builder.AddTypeParameter(typeof(object));
        builder.AddInstruction(OpCode.AllocObjectToParam2);
        builder.AddTypeParameter(typeof(object));
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte result = 0;
        object? param1 = null;
        object? param2 = null;
        var ctx = new InterpreterCtx(ref result, ref Unsafe.As<object?, byte>(ref param1),
            ref Unsafe.As<object?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.NotNull(param1);
        Assert.NotNull(param2);
        Assert.IsType<object>(param1);
        Assert.IsType<object>(param2);
        Assert.NotSame(param1, param2);
    }

    [Fact]
    public void DumpAsmIncludesAllocObjectTypeParameters()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.AllocObjectToResult);
        builder.AddTypeParameter(typeof(object));
        builder.AddInstruction(OpCode.AllocObjectToParam1);
        builder.AddTypeParameter(typeof(object));
        builder.AddInstruction(OpCode.AllocObjectToParam2);
        builder.AddTypeParameter(typeof(object));
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Contains("  0000: AllocObjectToResult System.Object", sb.ToString());
        Assert.Contains("  0002: AllocObjectToParam1 System.Object", sb.ToString());
        Assert.Contains("  0004: AllocObjectToParam2 System.Object", sb.ToString());
        Assert.Contains("  0006: Stop\n", sb.ToString());
    }

    [Fact]
    public void DerefObjectOpsPointRefsToRawObjectData()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.DerefObjectResult);
        builder.AddInstruction(OpCode.DerefObjectParam1);
        builder.AddInstruction(OpCode.DerefObjectParam2);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? result = new object();
        object? param1 = new object();
        object? param2 = new object();
        var ctx = new InterpreterCtx(ref Unsafe.As<object?, byte>(ref result), ref Unsafe.As<object?, byte>(ref param1),
            ref Unsafe.As<object?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(Unsafe.AreSame(ref RawData.Ref(result), ref ctx.Result));
        Assert.True(Unsafe.AreSame(ref RawData.Ref(param1), ref ctx.Param1));
        Assert.True(Unsafe.AreSame(ref RawData.Ref(param2), ref ctx.Param2));
    }

    [Fact]
    public void DumpAsmIncludesDerefObjectOps()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.DerefObjectResult);
        builder.AddInstruction(OpCode.DerefObjectParam1);
        builder.AddInstruction(OpCode.DerefObjectParam2);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: DerefObjectResult
  0001: DerefObjectParam1
  0002: DerefObjectParam2
  0003: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void AssignRefOpsAssignRegisterRefs()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.PushStack);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.SetResultBySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.SetParam1BySP);
        builder.AddVUInt64(12);
        builder.AddInstruction(OpCode.SetParam2BySP);
        builder.AddVUInt64(8);
        builder.AddInstruction(OpCode.AssignRefResultParam1);
        builder.AddInstruction(OpCode.AssignRefParam2Result);
        builder.AddInstruction(OpCode.AssignRefParam1Param2);
        builder.AddInstruction(OpCode.AssignRefResultParam2);
        builder.AddInstruction(OpCode.SetParam2BySP);
        builder.AddVUInt64(4);
        builder.AddInstruction(OpCode.AssignRefParam1Result);
        builder.AddInstruction(OpCode.AssignRefResultParam2);
        builder.AddInstruction(OpCode.AssignRefParam2Param1);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[16];
        stack.Clear();
        byte result = 0;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref result, ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        ctx.Result = 11;
        ctx.Param1 = 22;
        ctx.Param2 = 33;
        Assert.Equal(11, stack[12]);
        Assert.Equal(33, stack[4]);
        Assert.Equal(0, stack[8]);
        Assert.Equal(0, stack[0]);
    }

    [Fact]
    public void DumpAsmIncludesAssignRefOps()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.AssignRefResultParam1);
        builder.AddInstruction(OpCode.AssignRefResultParam2);
        builder.AddInstruction(OpCode.AssignRefParam1Result);
        builder.AddInstruction(OpCode.AssignRefParam1Param2);
        builder.AddInstruction(OpCode.AssignRefParam2Result);
        builder.AddInstruction(OpCode.AssignRefParam2Param1);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: AssignRefResultParam1
  0001: AssignRefResultParam2
  0002: AssignRefParam1Result
  0003: AssignRefParam1Param2
  0004: AssignRefParam2Result
  0005: AssignRefParam2Param1
  0006: Stop
""" + "\n", sb.ToString());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(17)]
    public void StackAllocObjectSupportsOnlyOneToSixteenReferences(ulong count)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.StackAllocObject);
        builder.AddVUInt64(count);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);

        var thrown = false;
        try
        {
            Interpreter.Run(ref ctx);
        }
        catch (NotSupportedException)
        {
            thrown = true;
        }
        Assert.True(thrown);
    }

    [Fact]
    public void DumpAsmIncludesStackAllocObjectParameter()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.StackAllocObject);
        builder.AddVUInt64(2);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: StackAllocObject 2
Try
    0002: Stop
Finally
    0003: Stop
TryEnd
  0004: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void DumpAsmIncludesStackStructAllocTypeParameter()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.StackStructAlloc);
        builder.AddStackStructTypeParameter(typeof(int));
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Contains("  0000: StackStructAlloc System.Int32\n", sb.ToString());
    }

    [Fact]
    public void DumpAsmIncludesStackBytesAllocParameter()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.StackBytesAlloc);
        builder.AddVUInt64(12);
        builder.AddInstruction(OpCode.Stop);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: StackBytesAlloc 12
Try
    0002: Stop
Finally
    0003: Stop
TryEnd
  0004: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void StoreAndLoadBySPPreservesStackRefs()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.PushStack);
        builder.AddVUInt64(64);
        builder.AddInstruction(OpCode.SetResultBySP);
        builder.AddVUInt64(64);
        builder.AddInstruction(OpCode.SetParam1BySP);
        builder.AddVUInt64(60);
        builder.AddInstruction(OpCode.SetParam2BySP);
        builder.AddVUInt64(56);
        builder.AddInstruction(OpCode.StoreResultBySP);
        builder.AddVUInt64(32);
        builder.AddInstruction(OpCode.StoreParam1BySP);
        builder.AddVUInt64(24);
        builder.AddInstruction(OpCode.StoreParam2BySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.SetResultBySP);
        builder.AddVUInt64(52);
        builder.AddInstruction(OpCode.SetParam1BySP);
        builder.AddVUInt64(48);
        builder.AddInstruction(OpCode.SetParam2BySP);
        builder.AddVUInt64(44);
        builder.AddInstruction(OpCode.LoadResultBySP);
        builder.AddVUInt64(32);
        builder.AddInstruction(OpCode.LoadParam1BySP);
        builder.AddVUInt64(24);
        builder.AddInstruction(OpCode.LoadParam2BySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[64];
        stack.Clear();
        byte result = 0;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref result, ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        ctx.Result = 11;
        ctx.Param1 = 22;
        ctx.Param2 = 33;
        Assert.Equal(11, stack[0]);
        Assert.Equal(22, stack[4]);
        Assert.Equal(33, stack[8]);
        Assert.Equal(0, stack[12]);
        Assert.Equal(0, stack[16]);
        Assert.Equal(0, stack[20]);
    }

    [Fact]
    public void DumpAsmIncludesStoreAndLoadBySPParameters()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.StoreResultBySP);
        builder.AddVUInt64(32);
        builder.AddInstruction(OpCode.StoreParam1BySP);
        builder.AddVUInt64(24);
        builder.AddInstruction(OpCode.StoreParam2BySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.LoadResultBySP);
        builder.AddVUInt64(32);
        builder.AddInstruction(OpCode.LoadParam1BySP);
        builder.AddVUInt64(24);
        builder.AddInstruction(OpCode.LoadParam2BySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: StoreResultBySP 32
  0002: StoreParam1BySP 24
  0004: StoreParam2BySP 16
  0006: LoadResultBySP 32
  0008: LoadParam1BySP 24
  0010: LoadParam2BySP 16
  0012: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void IncrementAndDecrementRefOpsMoveRefsByBytes()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.PushStack);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.SetResultBySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.IncrementRefResult);
        builder.AddVUInt64(3);
        builder.AddInstruction(OpCode.DecrementRefResult);
        builder.AddVUInt64(1);
        builder.AddInstruction(OpCode.SetParam1BySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.IncrementRefParam1);
        builder.AddVUInt64(7);
        builder.AddInstruction(OpCode.DecrementRefParam1);
        builder.AddVUInt64(2);
        builder.AddInstruction(OpCode.SetParam2BySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.IncrementRefParam2);
        builder.AddVUInt64(11);
        builder.AddInstruction(OpCode.DecrementRefParam2);
        builder.AddVUInt64(3);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[16];
        stack.Clear();
        byte result = 0;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref result, ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        ctx.Result = 11;
        ctx.Param1 = 22;
        ctx.Param2 = 33;
        Assert.Equal(11, stack[2]);
        Assert.Equal(22, stack[5]);
        Assert.Equal(33, stack[8]);
    }

    [Fact]
    public void DumpAsmIncludesIncrementAndDecrementRefParameters()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.IncrementRefResult);
        builder.AddVUInt64(3);
        builder.AddInstruction(OpCode.DecrementRefResult);
        builder.AddVUInt64(1);
        builder.AddInstruction(OpCode.IncrementRefParam1);
        builder.AddVUInt64(7);
        builder.AddInstruction(OpCode.DecrementRefParam1);
        builder.AddVUInt64(2);
        builder.AddInstruction(OpCode.IncrementRefParam2);
        builder.AddVUInt64(11);
        builder.AddInstruction(OpCode.DecrementRefParam2);
        builder.AddVUInt64(3);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: IncrementRefResult 3
  0002: DecrementRefResult 1
  0004: IncrementRefParam1 7
  0006: DecrementRefParam1 2
  0008: IncrementRefParam2 11
  0010: DecrementRefParam2 3
  0012: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void SwapRefOpsSwapRegisterRefs()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.PushStack);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.SetResultBySP);
        builder.AddVUInt64(16);
        builder.AddInstruction(OpCode.SetParam1BySP);
        builder.AddVUInt64(12);
        builder.AddInstruction(OpCode.SetParam2BySP);
        builder.AddVUInt64(8);
        builder.AddInstruction(OpCode.SwapRefResultParam1);
        builder.AddInstruction(OpCode.SwapRefParam1Param2);
        builder.AddInstruction(OpCode.SwapRefResultParam2);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[16];
        stack.Clear();
        byte result = 0;
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref result, ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        ctx.Result = 11;
        ctx.Param1 = 22;
        ctx.Param2 = 33;
        Assert.Equal(11, stack[0]);
        Assert.Equal(33, stack[4]);
        Assert.Equal(22, stack[8]);
    }

    [Fact]
    public void DumpAsmIncludesSwapRefOps()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.SwapRefResultParam1);
        builder.AddInstruction(OpCode.SwapRefParam1Param2);
        builder.AddInstruction(OpCode.SwapRefResultParam2);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: SwapRefResultParam1
  0001: SwapRefParam1Param2
  0002: SwapRefResultParam2
  0003: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void JmpSkipsInstructionsUntilMarkedLabel()
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel();
        builder.AddInstruction(OpCode.Jmp);
        builder.AddLabelParameter(label);
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.MarkLabel(label);
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
    }

    [Fact]
    public void ConditionalJumpsUseBoolResult()
    {
        var trueBuilder = new InterpreterBuilder();
        var trueLabel = trueBuilder.DeclareLabel();
        trueBuilder.AddInstruction(OpCode.SetBoolResultTrue);
        trueBuilder.AddInstruction(OpCode.JmpIfTrue);
        trueBuilder.AddLabelParameter(trueLabel);
        trueBuilder.AddInstruction(OpCode.SetBoolResultFalse);
        trueBuilder.MarkLabel(trueLabel);
        trueBuilder.AddInstruction(OpCode.Stop);
        var trueProgram = trueBuilder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack,
            trueProgram);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);

        var falseBuilder = new InterpreterBuilder();
        var falseLabel = falseBuilder.DeclareLabel();
        falseBuilder.AddInstruction(OpCode.SetBoolResultFalse);
        falseBuilder.AddInstruction(OpCode.JmpIfFalse);
        falseBuilder.AddLabelParameter(falseLabel);
        falseBuilder.AddInstruction(OpCode.SetBoolResultTrue);
        falseBuilder.MarkLabel(falseLabel);
        falseBuilder.AddInstruction(OpCode.Stop);
        var falseProgram = falseBuilder.Materialize();
        ctx = new(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, falseProgram);

        Interpreter.Run(ref ctx);

        Assert.False(ctx.BoolResult);
    }

    [Fact]
    public void InvalidOpCodeThrows()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.Invalid);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<object?, byte>(ref param1), ref param2, stack, program);

        var thrown = false;
        try
        {
            Interpreter.Run(ref ctx);
        }
        catch (InvalidOperationException)
        {
            thrown = true;
        }
        Assert.True(thrown);
    }

    [Fact]
    public void ExceptionInTryJumpsToCatchAndParam1ReferencesCurrentException()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddInstruction(OpCode.DivInt32);
        builder.AddCatch();
        builder.AddInstruction(OpCode.Stop);
        builder.AddTryEnd();
        var program = builder.Materialize();
        var result = 0;
        var param1 = 1;
        var param2 = 0;
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref Unsafe.As<int, byte>(ref result), ref Unsafe.As<int, byte>(ref param1),
            ref Unsafe.As<int, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.IsType<DivideByZeroException>(ctx.CurrentException);
        Assert.Same(ctx.CurrentException, Unsafe.As<byte, Exception?>(ref ctx.Param1));
    }

    [Fact]
    public void CatchedExceptionClearsExceptionAndContinuesFinally()
    {
        var builder = new InterpreterBuilder();
        var exceptionIndex = builder.AddObjectConstant(new InvalidOperationException("fail"));
        builder.AddTry();
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)exceptionIndex);
        builder.AddInstruction(OpCode.ThrowException);
        builder.AddCatch();
        builder.AddInstruction(OpCode.CatchedException);
        builder.AddFinally();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref Unsafe.As<object?, byte>(ref param1),
            ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
        Assert.Null(ctx.CurrentException);
    }

    [Fact]
    public void JmpFinallySkipsCatchBlockAndRunsFinally()
    {
        var builder = new InterpreterBuilder();
        builder.AddTry();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddCatch();
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddFinally();
        builder.AddInstruction(OpCode.NegateBoolResult);
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.False(ctx.BoolResult);
    }

    [Fact]
    public void RethrowRunsFinallyAndPropagatesToOuterCatch()
    {
        var builder = new InterpreterBuilder();
        var exceptionIndex = builder.AddObjectConstant(new InvalidOperationException("fail"));
        builder.AddTry();
        builder.AddTry();
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)exceptionIndex);
        builder.AddInstruction(OpCode.ThrowException);
        builder.AddCatch();
        builder.AddInstruction(OpCode.Rethrow);
        builder.AddFinally();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddCatch();
        builder.AddInstruction(OpCode.CatchedException);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref Unsafe.As<object?, byte>(ref param1),
            ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
        Assert.Null(ctx.CurrentException);
    }

    [Fact]
    public void RethrowWithoutFinallyPropagatesAsEndOfFinally()
    {
        var builder = new InterpreterBuilder();
        var exceptionIndex = builder.AddObjectConstant(new InvalidOperationException("fail"));
        builder.AddTry();
        builder.AddTry();
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)exceptionIndex);
        builder.AddInstruction(OpCode.ThrowException);
        builder.AddCatch();
        builder.AddInstruction(OpCode.Rethrow);
        builder.AddTryEnd();
        builder.AddCatch();
        builder.AddInstruction(OpCode.CatchedException);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref Unsafe.As<object?, byte>(ref param1),
            ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.Null(ctx.CurrentException);
    }

    [Fact]
    public void EmptyFinallyWithoutCatchContinuesUnwindingToOuterCatch()
    {
        var builder = new InterpreterBuilder();
        var exceptionIndex = builder.AddObjectConstant(new InvalidOperationException("fail"));
        builder.AddTry();
        builder.AddTry();
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)exceptionIndex);
        builder.AddInstruction(OpCode.ThrowException);
        builder.AddFinally();
        builder.AddTryEnd();
        builder.AddCatch();
        builder.AddInstruction(OpCode.CatchedException);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref Unsafe.As<object?, byte>(ref param1),
            ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.Null(ctx.CurrentException);
    }

    [Fact]
    public void EndOfFinallyRethrowsUnhandledException()
    {
        var builder = new InterpreterBuilder();
        var exceptionIndex = builder.AddObjectConstant(new InvalidOperationException("fail"));
        builder.AddTry();
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)exceptionIndex);
        builder.AddInstruction(OpCode.ThrowException);
        builder.AddFinally();
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref Unsafe.As<object?, byte>(ref param1),
            ref param2, stack, program);

        var thrown = false;
        try
        {
            Interpreter.Run(ref ctx);
        }
        catch (InvalidOperationException)
        {
            thrown = true;
        }

        Assert.True(thrown);
    }

    [Fact]
    public void StackAllocObjectStopRunsFinallyBeforeReturning()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.StackAllocObject);
        builder.AddVUInt64(1);
        builder.AddTry();
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddInstruction(OpCode.Stop);
        builder.AddFinally();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.EndOfFinally);
        builder.AddTryEnd();
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
    }

    [Fact]
    public void EqualStringComparesParam1AndParam2()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.EqualString);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        string? param1 = new string("same");
        string? param2 = new string("same");
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<string?, byte>(ref param1), ref Unsafe.As<string?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
    }

    [Fact]
    public void SetParam2ObjectConstLoadsObjectConstant()
    {
        var builder = new InterpreterBuilder();
        var param1Index = builder.AddObjectConstant("same");
        var param2Index = builder.AddObjectConstant(new string("same"));
        builder.AddInstruction(OpCode.SetParam1ObjectConst);
        builder.AddVUInt64((ulong)param1Index);
        builder.AddInstruction(OpCode.SetParam2ObjectConst);
        builder.AddVUInt64((ulong)param2Index);
        builder.AddInstruction(OpCode.EqualString);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        object? param1 = null;
        object? param2 = null;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<object?, byte>(ref param1), ref Unsafe.As<object?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
        Assert.Same(program.ObjectConstants[param1Index], param1);
        Assert.Same(program.ObjectConstants[param2Index], param2);
    }

    [Fact]
    public void EqualStringHandlesDifferentValuesAndNulls()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.EqualString);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        string? param1 = "left";
        string? param2 = "right";
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<string?, byte>(ref param1), ref Unsafe.As<string?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.False(ctx.BoolResult);

        param1 = null;
        param2 = null;
        ctx = new(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<string?, byte>(ref param1), ref Unsafe.As<string?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
    }

    [Fact]
    public void LessStringComparesParam1AndParam2()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.LessString);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        string? param1 = "abc";
        string? param2 = "abd";
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<string?, byte>(ref param1), ref Unsafe.As<string?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);

        param1 = "abd";
        param2 = "abc";
        ctx = new(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<string?, byte>(ref param1), ref Unsafe.As<string?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.False(ctx.BoolResult);
    }

    [Fact]
    public void LessStringHandlesNulls()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.LessString);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        string? param1 = null;
        string? param2 = "value";
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<string?, byte>(ref param1), ref Unsafe.As<string?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);

        param1 = null;
        param2 = null;
        ctx = new(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<string?, byte>(ref param1), ref Unsafe.As<string?, byte>(ref param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.False(ctx.BoolResult);
    }

    static readonly (OpCode EqualOp, OpCode LessOp, Type Type)[] NumericComparisonOps =
    {
        (OpCode.EqualByte, OpCode.LessByte, typeof(byte)),
        (OpCode.EqualSByte, OpCode.LessSByte, typeof(sbyte)),
        (OpCode.EqualUInt16, OpCode.LessUInt16, typeof(ushort)),
        (OpCode.EqualInt16, OpCode.LessInt16, typeof(short)),
        (OpCode.EqualUInt32, OpCode.LessUInt32, typeof(uint)),
        (OpCode.EqualInt32, OpCode.LessInt32, typeof(int)),
        (OpCode.EqualUInt64, OpCode.LessUInt64, typeof(ulong)),
        (OpCode.EqualInt64, OpCode.LessInt64, typeof(long)),
        (OpCode.EqualHalf, OpCode.LessHalf, typeof(Half)),
        (OpCode.EqualFloat, OpCode.LessFloat, typeof(float)),
        (OpCode.EqualDouble, OpCode.LessDouble, typeof(double))
    };

    static readonly (OpCode AddOp, OpCode SubOp, OpCode MulOp, OpCode DivOp, Type Type)[] NumericArithmeticOps =
    {
        (OpCode.AddByte, OpCode.SubByte, OpCode.MulByte, OpCode.DivByte, typeof(byte)),
        (OpCode.AddSByte, OpCode.SubSByte, OpCode.MulSByte, OpCode.DivSByte, typeof(sbyte)),
        (OpCode.AddUInt16, OpCode.SubUInt16, OpCode.MulUInt16, OpCode.DivUInt16, typeof(ushort)),
        (OpCode.AddInt16, OpCode.SubInt16, OpCode.MulInt16, OpCode.DivInt16, typeof(short)),
        (OpCode.AddUInt32, OpCode.SubUInt32, OpCode.MulUInt32, OpCode.DivUInt32, typeof(uint)),
        (OpCode.AddInt32, OpCode.SubInt32, OpCode.MulInt32, OpCode.DivInt32, typeof(int)),
        (OpCode.AddUInt64, OpCode.SubUInt64, OpCode.MulUInt64, OpCode.DivUInt64, typeof(ulong)),
        (OpCode.AddInt64, OpCode.SubInt64, OpCode.MulInt64, OpCode.DivInt64, typeof(long)),
        (OpCode.AddHalf, OpCode.SubHalf, OpCode.MulHalf, OpCode.DivHalf, typeof(Half)),
        (OpCode.AddFloat, OpCode.SubFloat, OpCode.MulFloat, OpCode.DivFloat, typeof(float)),
        (OpCode.AddDouble, OpCode.SubDouble, OpCode.MulDouble, OpCode.DivDouble, typeof(double))
    };

    static readonly (OpCode NegOp, Type Type, double Expected)[] NumericNegOps =
    {
        (OpCode.NegByte, typeof(byte), 244),
        (OpCode.NegSByte, typeof(sbyte), -12),
        (OpCode.NegUInt16, typeof(ushort), 65524),
        (OpCode.NegInt16, typeof(short), -12),
        (OpCode.NegUInt32, typeof(uint), 4294967284),
        (OpCode.NegInt32, typeof(int), -12),
        (OpCode.NegUInt64, typeof(ulong), 18446744073709551604d),
        (OpCode.NegInt64, typeof(long), -12),
        (OpCode.NegHalf, typeof(Half), -12),
        (OpCode.NegFloat, typeof(float), -12),
        (OpCode.NegDouble, typeof(double), -12)
    };

    static readonly (OpCode AndOp, OpCode OrOp, OpCode XorOp, OpCode NotOp, Type Type, ulong Mask)[] UnsignedBinaryOps =
    {
        (OpCode.BinaryAndByte, OpCode.BinaryOrByte, OpCode.BinaryXorByte, OpCode.BinaryNotByte, typeof(byte),
            byte.MaxValue),
        (OpCode.BinaryAndUInt16, OpCode.BinaryOrUInt16, OpCode.BinaryXorUInt16, OpCode.BinaryNotUInt16,
            typeof(ushort), ushort.MaxValue),
        (OpCode.BinaryAndUInt32, OpCode.BinaryOrUInt32, OpCode.BinaryXorUInt32, OpCode.BinaryNotUInt32,
            typeof(uint), uint.MaxValue),
        (OpCode.BinaryAndUInt64, OpCode.BinaryOrUInt64, OpCode.BinaryXorUInt64, OpCode.BinaryNotUInt64,
            typeof(ulong), ulong.MaxValue)
    };

    [Fact]
    public void NumericEqualOpsCompareParam1AndParam2()
    {
        foreach (var (equalOp, _, type) in NumericComparisonOps)
            NumericEqualOpComparesParam1AndParam2(equalOp, type);
    }

    static void NumericEqualOpComparesParam1AndParam2(OpCode equalOp, Type type)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(equalOp);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> param1 = stackalloc byte[8];
        Span<byte> param2 = stackalloc byte[8];
        WriteNumericValue(param1, type, 123);
        WriteNumericValue(param2, type, 123);
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref MemoryMarshal.GetReference(param1),
            ref MemoryMarshal.GetReference(param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);

        WriteNumericValue(param2, type, 124);
        ctx = new(ref MemoryMarshal.GetReference(stack), ref MemoryMarshal.GetReference(param1),
            ref MemoryMarshal.GetReference(param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.False(ctx.BoolResult);
    }

    [Fact]
    public void NumericLessOpsCompareParam1AndParam2()
    {
        foreach (var (_, lessOp, type) in NumericComparisonOps)
            NumericLessOpComparesParam1AndParam2(lessOp, type);
    }

    static void NumericLessOpComparesParam1AndParam2(OpCode lessOp, Type type)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(lessOp);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> param1 = stackalloc byte[8];
        Span<byte> param2 = stackalloc byte[8];
        WriteNumericValue(param1, type, 123);
        WriteNumericValue(param2, type, 124);
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref MemoryMarshal.GetReference(param1),
            ref MemoryMarshal.GetReference(param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);

        WriteNumericValue(param1, type, 124);
        WriteNumericValue(param2, type, 123);
        ctx = new(ref MemoryMarshal.GetReference(stack), ref MemoryMarshal.GetReference(param1),
            ref MemoryMarshal.GetReference(param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.False(ctx.BoolResult);
    }

    [Fact]
    public void NumericArithmeticOpsStoreResultFromParam1AndParam2()
    {
        foreach (var (addOp, subOp, mulOp, divOp, type) in NumericArithmeticOps)
        {
            NumericArithmeticOpStoresResult(addOp, type, 15);
            NumericArithmeticOpStoresResult(subOp, type, 9);
            NumericArithmeticOpStoresResult(mulOp, type, 36);
            NumericArithmeticOpStoresResult(divOp, type, 4);
        }
    }

    static void NumericArithmeticOpStoresResult(OpCode opCode, Type type, double expected)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(opCode);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> result = stackalloc byte[8];
        Span<byte> param1 = stackalloc byte[8];
        Span<byte> param2 = stackalloc byte[8];
        WriteNumericValue(param1, type, 12);
        WriteNumericValue(param2, type, 3);
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(result), ref MemoryMarshal.GetReference(param1),
            ref MemoryMarshal.GetReference(param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal(expected, ReadNumericValue(result, type));
    }

    [Fact]
    public void NumericNegOpsStoreNegatedResult()
    {
        foreach (var (negOp, type, expected) in NumericNegOps)
            NumericNegOpStoresNegatedResult(negOp, type, expected);
    }

    static void NumericNegOpStoresNegatedResult(OpCode opCode, Type type, double expected)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(opCode);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> result = stackalloc byte[8];
        Span<byte> param = stackalloc byte[1];
        Span<byte> stack = stackalloc byte[1];
        WriteNumericValue(result, type, 12);
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(result), ref MemoryMarshal.GetReference(param),
            ref MemoryMarshal.GetReference(param), stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal(expected, ReadNumericValue(result, type));
    }

    [Fact]
    public void UnsignedBinaryOpsStoreResultFromParams()
    {
        foreach (var (andOp, orOp, xorOp, notOp, type, mask) in UnsignedBinaryOps)
        {
            UnsignedBinaryOpStoresResult(andOp, type, 0b1010, 0b1100, 0b1000);
            UnsignedBinaryOpStoresResult(orOp, type, 0b1010, 0b1100, 0b1110);
            UnsignedBinaryOpStoresResult(xorOp, type, 0b1010, 0b1100, 0b0110);
            UnsignedBinaryOpStoresResult(notOp, type, 0b1010, 0, mask ^ 0b1010UL);
        }
    }

    static void UnsignedBinaryOpStoresResult(OpCode opCode, Type type, ulong param1Value, ulong param2Value,
        ulong expected)
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(opCode);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> result = stackalloc byte[8];
        Span<byte> param1 = stackalloc byte[8];
        Span<byte> param2 = stackalloc byte[8];
        WriteNumericValue(param1, type, (long)param1Value);
        WriteNumericValue(param2, type, (long)param2Value);
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(result), ref MemoryMarshal.GetReference(param1),
            ref MemoryMarshal.GetReference(param2), stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal(expected, ReadUnsignedNumericValue(result, type));
    }

    [Fact]
    public void DumpAsmIncludesNumericComparisonOps()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.EqualInt32);
        builder.AddInstruction(OpCode.LessUInt64);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: EqualInt32
  0001: LessUInt64
  0002: Stop
""" + "\n", sb.ToString());
    }

    [Fact]
    public void CodeOpsReadFromMemReaderInParam1IntoResult()
    {
        Span<byte> buffer = stackalloc byte[64];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buffer);
        writer.WriteUInt8(0x7f);
        writer.WriteUInt32LE(0x12345678);
        writer.WriteVInt64(-123);
        var reader = MemReader.CreateFromPinnedSpan(writer.GetSpan());
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.ReadUInt8);
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.Skip4Bytes);
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.ReadVInt64);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        long result = 0;
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref Unsafe.As<long, byte>(ref result),
            ref Unsafe.As<MemReader, byte>(ref reader), ref MemoryMarshal.GetReference(stack), stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal(-123, result);
        Assert.True(reader.Eof);
    }

    [Fact]
    public void CodeOpsUseParam2UInt32ForReaderLength()
    {
        var source = new byte[] { 1, 2, 3, 4, 5, 6 };
        var reader = MemReader.CreateFromPinnedSpan(source);
        Span<byte> destination = stackalloc byte[2];
        uint length = 2;
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.SkipBlock);
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.ReadBlock);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(destination),
            ref Unsafe.As<MemReader, byte>(ref reader), ref Unsafe.As<uint, byte>(ref length), stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal([3, 4], destination.ToArray());

        reader = MemReader.CreateFromPinnedSpan(source);
        builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.SkipBlock);
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.ReadByteArrayRaw);
        builder.AddInstruction(OpCode.Stop);
        program = builder.Materialize();
        byte[]? result = null;
        ctx = new(ref Unsafe.As<byte[]?, byte>(ref result), ref Unsafe.As<MemReader, byte>(ref reader),
            ref Unsafe.As<uint, byte>(ref length), stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal([3, 4], result);
    }

    [Fact]
    public void CodeOpsWriteToMemWriterFromParam2()
    {
        Span<byte> buffer = stackalloc byte[64];
        var writer = MemWriter.CreateFromStackAllocatedSpan(buffer);
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.WriteUInt8);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        byte value = 0x7f;
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack),
            ref Unsafe.As<MemWriter, byte>(ref writer), ref Unsafe.As<byte, byte>(ref value), stack, program);

        Interpreter.Run(ref ctx);

        Assert.Equal([0x7f], writer.GetSpan().ToArray());

        writer = MemWriter.CreateFromStackAllocatedSpan(buffer);
        var text = "abc";
        builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.WriteString);
        builder.AddInstruction(OpCode.Stop);
        program = builder.Materialize();
        ctx = new(ref MemoryMarshal.GetReference(stack), ref Unsafe.As<MemWriter, byte>(ref writer),
            ref Unsafe.As<string, byte>(ref text), stack, program);

        Interpreter.Run(ref ctx);

        var reader = MemReader.CreateFromPinnedSpan(writer.GetSpan());
        Assert.Equal("abc", reader.ReadString());
    }

    [Fact]
    public void CodeOpsDumpAsmIncludesNestedCodeOp()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.SkipBlock);
        builder.AddInstruction(OpCode.CodeOps);
        builder.AddCodeOp(CodeOp.ReadString);
        builder.AddInstruction(OpCode.Stop);
        var sb = new StringBuilder();

        builder.DumpAsm(sb);

        Assert.Equal("""
  0000: CodeOps SkipBlock
  0002: CodeOps ReadString
  0004: Stop
""" + "\n", sb.ToString());
    }

    static void WriteNumericValue(Span<byte> span, Type type, long value)
    {
        if (type == typeof(byte)) MemoryMarshal.Write(span, (byte)value);
        else if (type == typeof(sbyte)) MemoryMarshal.Write(span, (sbyte)value);
        else if (type == typeof(ushort)) MemoryMarshal.Write(span, (ushort)value);
        else if (type == typeof(short)) MemoryMarshal.Write(span, (short)value);
        else if (type == typeof(uint)) MemoryMarshal.Write(span, (uint)value);
        else if (type == typeof(int)) MemoryMarshal.Write(span, (int)value);
        else if (type == typeof(ulong)) MemoryMarshal.Write(span, (ulong)value);
        else if (type == typeof(long)) MemoryMarshal.Write(span, value);
        else if (type == typeof(Half)) MemoryMarshal.Write(span, (Half)(float)value);
        else if (type == typeof(float)) MemoryMarshal.Write(span, (float)value);
        else if (type == typeof(double)) MemoryMarshal.Write(span, (double)value);
        else throw new ArgumentOutOfRangeException(nameof(type));
    }

    static double ReadNumericValue(ReadOnlySpan<byte> span, Type type)
    {
        if (type == typeof(byte)) return MemoryMarshal.Read<byte>(span);
        if (type == typeof(sbyte)) return MemoryMarshal.Read<sbyte>(span);
        if (type == typeof(ushort)) return MemoryMarshal.Read<ushort>(span);
        if (type == typeof(short)) return MemoryMarshal.Read<short>(span);
        if (type == typeof(uint)) return MemoryMarshal.Read<uint>(span);
        if (type == typeof(int)) return MemoryMarshal.Read<int>(span);
        if (type == typeof(ulong)) return MemoryMarshal.Read<ulong>(span);
        if (type == typeof(long)) return MemoryMarshal.Read<long>(span);
        if (type == typeof(Half)) return (double)MemoryMarshal.Read<Half>(span);
        if (type == typeof(float)) return MemoryMarshal.Read<float>(span);
        if (type == typeof(double)) return MemoryMarshal.Read<double>(span);
        throw new ArgumentOutOfRangeException(nameof(type));
    }

    static ulong ReadUnsignedNumericValue(ReadOnlySpan<byte> span, Type type)
    {
        if (type == typeof(byte)) return MemoryMarshal.Read<byte>(span);
        if (type == typeof(ushort)) return MemoryMarshal.Read<ushort>(span);
        if (type == typeof(uint)) return MemoryMarshal.Read<uint>(span);
        if (type == typeof(ulong)) return MemoryMarshal.Read<ulong>(span);
        throw new ArgumentOutOfRangeException(nameof(type));
    }

    [Fact]
    public void NegateBoolResultFlipsCurrentValue()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.NegateBoolResult);
        builder.AddInstruction(OpCode.NegateBoolResult);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
    }

    [Fact]
    public void NegateBoolResultFlipsDefaultFalseToTrue()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.NegateBoolResult);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Span<byte> stack = stackalloc byte[1];
        byte param1 = 0;
        byte param2 = 0;
        var ctx = new InterpreterCtx(ref MemoryMarshal.GetReference(stack), ref param1, ref param2, stack, program);

        Interpreter.Run(ref ctx);

        Assert.True(ctx.BoolResult);
    }

    class AccessorTestObject
    {
        public int Value { get; set; }
    }

    static unsafe void RegisterAccessorTestObjectMetadata()
    {
        if (ReflectionMetadata.FindByType(typeof(AccessorTestObject)) != null) return;
        ReflectionMetadata.Register(new()
        {
            Type = typeof(AccessorTestObject),
            Name = nameof(AccessorTestObject),
            Namespace = typeof(AccessorTestObject).Namespace ?? "",
            Implements = [],
            Creator = &CreateAccessorTestObject,
            Fields =
            [
                new FieldMetadata
                {
                    Name = nameof(AccessorTestObject.Value),
                    Type = typeof(int),
                    PropRefGetter = &GetAccessorTestObjectValue,
                    PropRefSetter = &SetAccessorTestObjectValue
                }
            ]
        });
    }

    static object CreateAccessorTestObject()
    {
        return RuntimeHelpers.GetUninitializedObject(typeof(AccessorTestObject));
    }

    static void GetAccessorTestObjectValue(object obj, ref byte value)
    {
        Unsafe.As<byte, int>(ref value) = Unsafe.As<AccessorTestObject>(obj).Value;
    }

    static void SetAccessorTestObjectValue(object obj, ref byte value)
    {
        Unsafe.As<AccessorTestObject>(obj).Value = Unsafe.As<byte, int>(ref value);
    }

    [Fact]
    public void FrameOpsReadKeyAndValueFieldsIntoFrameAndObjectSlots()
    {
        var keyWriter = new MemWriter();
        keyWriter.WriteVUInt64(1234);
        keyWriter.WriteString("skipped");
        var valueWriter = new MemWriter();
        valueWriter.WriteString("name");
        valueWriter.WriteVInt64(-1);
        valueWriter.WriteVInt64(-2);
        valueWriter.WriteBool(true);
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.ReadKeyToFrame);
        builder.AddUInt8((byte)CodeOp.ReadVUInt64);
        builder.AddUInt16(0);
        builder.AddInstruction(OpCode.SkipKey);
        builder.AddUInt8((byte)CodeOp.SkipString);
        builder.AddUInt16(1);
        builder.AddInstruction(OpCode.ReadValueToObject);
        builder.AddUInt8((byte)CodeOp.ReadString);
        builder.AddUInt16(1);
        builder.AddInstruction(OpCode.SkipValue);
        builder.AddUInt8((byte)CodeOp.SkipVInt64);
        builder.AddUInt16(2);
        builder.AddInstruction(OpCode.ReadValueToFrame);
        builder.AddUInt8((byte)CodeOp.ReadBool);
        builder.AddUInt16(8);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();

        var frame = new long[2];
        var objects = new object?[2];
        RunFrameProgram(program, frame, objects, keyWriter.GetSpan(), valueWriter.GetSpan(), out var keyEof,
            out var valueEof);

        Assert.Equal(1234, frame[0]);
        Assert.Equal(1, frame[1]);
        Assert.Equal([null, "name"], objects);
        Assert.True(keyEof);
        Assert.True(valueEof);
    }

    [Theory]
    [InlineData(QueryCompare.Equal, 5, 5, true)]
    [InlineData(QueryCompare.Equal, 5, 7, false)]
    [InlineData(QueryCompare.NotEqual, 5, 7, true)]
    [InlineData(QueryCompare.Less, -1, 7, true)]
    [InlineData(QueryCompare.Less, 7, 7, false)]
    [InlineData(QueryCompare.LessOrEqual, 7, 7, true)]
    [InlineData(QueryCompare.Greater, 8, 7, true)]
    [InlineData(QueryCompare.GreaterOrEqual, 6, 7, false)]
    public void JmpIfFrameConstComparesInt32(QueryCompare compare, int value, int constant, bool expected)
    {
        Assert.Equal(expected, RunFrameCompare(QueryValueType.Int32, compare, value, constant, true));
        Assert.Equal(!expected, RunFrameCompare(QueryValueType.Int32, compare, value, constant, false));
    }

    [Fact]
    public void JmpIfFrameConstUsesCSharpSemanticsOfFloatsAndDateTimes()
    {
        Assert.False(RunFrameCompare(QueryValueType.Double, QueryCompare.Equal, double.NaN, double.NaN, true));
        Assert.True(RunFrameCompare(QueryValueType.Double, QueryCompare.NotEqual, double.NaN, double.NaN, true));
        Assert.True(RunFrameCompare(QueryValueType.Single, QueryCompare.Equal, -0f, 0f, true));
        var utc = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var local = new DateTime(utc.Ticks, DateTimeKind.Local);
        Assert.True(RunFrameCompare(QueryValueType.DateTime, QueryCompare.Equal, utc, local, true));
        Assert.True(RunFrameCompare(QueryValueType.DateTime, QueryCompare.Less, utc, local.AddTicks(1), true));
        // Unsigned comparison of the same bits differs from the signed one.
        Assert.True(RunFrameCompare(QueryValueType.UInt64, QueryCompare.Greater, ulong.MaxValue, 1ul, true));
        Assert.True(RunFrameCompare(QueryValueType.Int64, QueryCompare.Less, -1L, 1L, true));
    }

    [Theory]
    [InlineData(QueryCompare.Equal, "a", "a", true)]
    [InlineData(QueryCompare.Equal, null, null, true)]
    [InlineData(QueryCompare.Equal, "a", null, false)]
    [InlineData(QueryCompare.NotEqual, "a", "b", true)]
    [InlineData(QueryCompare.Less, "B", "a", true)]
    [InlineData(QueryCompare.Less, null, "a", true)]
    [InlineData(QueryCompare.GreaterOrEqual, "b", "b", true)]
    public void JmpIfStringConstComparesOrdinally(QueryCompare compare, string? value, string? constant,
        bool expected)
    {
        Assert.Equal(expected, RunStringCompare(compare, value, constant, true));
        Assert.Equal(!expected, RunStringCompare(compare, value, constant, false));
    }

    [Fact]
    public void ConvertSlotConvertsBetweenFrameSlotsWithResolvedConverter()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.ConvertSlot);
        builder.AddUInt8((byte)QuerySlotKind.Frame);
        builder.AddUInt16(0);
        builder.AddUInt8((byte)QuerySlotKind.Frame);
        builder.AddUInt16(8);
        builder.AddConverterIndex(DefaultTypeConverterFactory.Instance, typeof(float), typeof(double));
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        var frame = new long[2];
        Unsafe.As<long, float>(ref frame[0]) = 1.5f;

        RunFrameProgram(program, frame, [], [], [], out _, out _);

        Assert.Equal(1.5, BitConverter.Int64BitsToDouble(frame[1]));
    }

    [Fact]
    public void MaterializeRejectsLabelTargetOfUnmarkedLabel()
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("missing");
        builder.AddInstruction(OpCode.JmpIfFrameConst);
        builder.AddUInt8((byte)QueryValueType.Int32);
        builder.AddUInt8((byte)QueryCompare.Equal);
        builder.AddUInt16(0);
        builder.AddUInt32(0);
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.Stop);

        Assert.Contains("missing", Assert.Throws<InvalidOperationException>(() => builder.Materialize()).Message);
    }

    [Fact]
    public void DumpAsmIncludesFrameOps()
    {
        var builder = new InterpreterBuilder();
        var falseLabel = builder.DeclareLabel("false");
        var constant = builder.AllocAlignedConst(sizeof(int));
        builder.ConstSpan<int>(constant) = 35;
        builder.AddInstruction(OpCode.ReadValueToFrame);
        builder.AddUInt8((byte)CodeOp.ReadVInt64);
        builder.AddUInt16(8);
        builder.AddInstruction(OpCode.SkipValue);
        builder.AddUInt8((byte)CodeOp.SkipString);
        builder.AddUInt16(2);
        builder.AddInstruction(OpCode.JmpIfNotFrameConst);
        builder.AddUInt8((byte)QueryValueType.Int32);
        builder.AddUInt8((byte)QueryCompare.Equal);
        builder.AddUInt16(8);
        builder.AddUInt32(constant);
        builder.AddLabelTarget(falseLabel);
        builder.AddInstruction(OpCode.ReadValueToObject);
        builder.AddUInt8((byte)CodeOp.ReadString);
        builder.AddUInt16(0);
        builder.AddInstruction(OpCode.JmpIfNotStringConst);
        builder.AddUInt8((byte)QueryCompare.Equal);
        builder.AddUInt16(0);
        builder.AddUInt32((uint)builder.AddObjectConstant("Prague"));
        builder.AddLabelTarget(falseLabel);
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
        builder.MarkLabel(falseLabel);
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddInstruction(OpCode.Stop);

        var sb = new StringBuilder();
        builder.DumpAsm(sb);
        var asm = sb.ToString();

        Assert.Contains("ReadValueToFrame ReadVInt64 frame[8]", asm);
        Assert.Contains("SkipValue SkipString x2", asm);
        Assert.Contains("JmpIfNotFrameConst Int32 Equal frame[8] const[" + constant + "] ; 35 -> false", asm);
        Assert.Contains("ReadValueToObject ReadString objects[0]", asm);
        Assert.Contains("JmpIfNotStringConst Equal objects[0] 0 ; Prague -> false", asm);
    }

    [Fact]
    public void ExceptionInProgramWithoutTryBlocksIsStoredAndPropagates()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.DivInt32);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        var result = 0;
        var param1 = 1;
        var param2 = 0;
        Span<byte> stack = stackalloc byte[1];
        var ctx = new InterpreterCtx(ref Unsafe.As<int, byte>(ref result), ref Unsafe.As<int, byte>(ref param1),
            ref Unsafe.As<int, byte>(ref param2), stack, program);

        var thrown = false;
        try
        {
            Interpreter.Run(ref ctx);
        }
        catch (DivideByZeroException exception)
        {
            thrown = true;
            Assert.Same(exception, ctx.CurrentException);
        }

        Assert.True(thrown);
    }

    static bool RunFrameCompare<T>(QueryValueType type, QueryCompare compare, T value, T constant, bool jumpIfTrue)
        where T : unmanaged
    {
        var builder = new InterpreterBuilder();
        var jumpedLabel = builder.DeclareLabel("jumped");
        var constantOffset = builder.AllocAlignedConst((uint)Unsafe.SizeOf<T>());
        builder.ConstSpan<T>(constantOffset) = constant;
        builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfFrameConst : OpCode.JmpIfNotFrameConst);
        builder.AddUInt8((byte)type);
        builder.AddUInt8((byte)compare);
        builder.AddUInt16(0);
        builder.AddUInt32(constantOffset);
        builder.AddLabelTarget(jumpedLabel);
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddInstruction(OpCode.Stop);
        builder.MarkLabel(jumpedLabel);
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[1];
        Unsafe.As<long, T>(ref frame[0]) = value;
        return RunFrameProgram(builder.Materialize(), frame, [], [], [], out _, out _);
    }

    static bool RunStringCompare(QueryCompare compare, string? value, string? constant, bool jumpIfTrue)
    {
        var builder = new InterpreterBuilder();
        var jumpedLabel = builder.DeclareLabel("jumped");
        builder.AddInstruction(jumpIfTrue ? OpCode.JmpIfStringConst : OpCode.JmpIfNotStringConst);
        builder.AddUInt8((byte)compare);
        builder.AddUInt16(1);
        builder.AddUInt32((uint)builder.AddObjectConstant(constant));
        builder.AddLabelTarget(jumpedLabel);
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddInstruction(OpCode.Stop);
        builder.MarkLabel(jumpedLabel);
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
        return RunFrameProgram(builder.Materialize(), [], [null, value], [], [], out _, out _);
    }

    static unsafe bool RunFrameProgram(InterpreterProgram program, long[] frame, object?[] objects,
        ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, out bool keyEof, out bool valueEof)
    {
        fixed (byte* keyPointer = key)
        fixed (byte* valuePointer = value)
        {
            var keyReader = new MemReader(keyPointer, key.Length);
            var valueReader = new MemReader(valuePointer, value.Length);
            var ctx = new InterpreterCtx(program,
                ref Unsafe.As<long, byte>(ref MemoryMarshal.GetArrayDataReference(frame)),
                ref MemoryMarshal.GetArrayDataReference(objects), ref keyReader, ref valueReader);
            Interpreter.Run(ref ctx);
            keyEof = keyReader.Eof;
            valueEof = valueReader.Eof;
            return ctx.BoolResult;
        }
    }

    [Fact]
    public void JmpIfFrameFrameComparesTwoFrameValues()
    {
        Assert.True(RunFrameFrameCompare(QueryCompare.Less, -5L, 3L));
        Assert.False(RunFrameFrameCompare(QueryCompare.Greater, -5L, 3L));
        Assert.True(RunFrameFrameCompare(QueryCompare.Equal, 7L, 7L));
    }

    [Fact]
    public void ArithmeticComputesFromFrameAndConstantOperands()
    {
        Assert.Equal(42, RunArithmetic(QueryValueType.Int32, QueryArithmetic.Add, 40, 2));
        Assert.Equal(-38, RunArithmetic(QueryValueType.Int32, QueryArithmetic.Subtract, 2, 40));
        Assert.Equal(int.MinValue, RunArithmetic(QueryValueType.Int32, QueryArithmetic.Add, int.MaxValue, 1));
        Assert.Equal(-40, RunArithmetic(QueryValueType.Int32, QueryArithmetic.Negate, 40, 0));
        Assert.Equal(2.5m, RunArithmetic(QueryValueType.Decimal, QueryArithmetic.Multiply, 1.25m, 2m));
        Assert.Equal(3.5, RunArithmetic(QueryValueType.Double, QueryArithmetic.Divide, 7.0, 2.0));
        Assert.Throws<DivideByZeroException>(() => RunArithmetic(QueryValueType.Int32, QueryArithmetic.Divide, 1, 0));
        Assert.Throws<OverflowException>(() =>
            RunArithmetic(QueryValueType.Int64, QueryArithmetic.AddChecked, long.MaxValue, 1L));
    }

    [Fact]
    public void ConvertNumberConvertsLikeCSharpCasts()
    {
        Assert.Equal(250, RunConvert<byte, int>(QueryValueType.UInt8, QueryValueType.Int32, 250, false));
        Assert.Equal(unchecked((byte)300), RunConvert<int, byte>(QueryValueType.Int32, QueryValueType.UInt8, 300, false));
        Assert.Equal(-7, RunConvert<double, int>(QueryValueType.Double, QueryValueType.Int32, -7.9, false));
        Assert.Equal(1.5m, RunConvert<double, decimal>(QueryValueType.Double, QueryValueType.Decimal, 1.5, false));
        Assert.Throws<OverflowException>(() =>
            RunConvert<int, byte>(QueryValueType.Int32, QueryValueType.UInt8, 300, true));
        // Conversions from decimal are checked even in unchecked context.
        Assert.Throws<OverflowException>(() =>
            RunConvert<decimal, int>(QueryValueType.Decimal, QueryValueType.Int32, 1e20m, false));
    }

    [Theory]
    [InlineData(QueryStringEncoding.String, "abc", "abc", true)]
    [InlineData(QueryStringEncoding.String, "abc", "abd", false)]
    [InlineData(QueryStringEncoding.String, null, null, true)]
    [InlineData(QueryStringEncoding.String, "", null, false)]
    [InlineData(QueryStringEncoding.StringOrdered, "ž😀", "ž😀", true)]
    [InlineData(QueryStringEncoding.StringOrdered, null, "", false)]
    public void JmpIfEncodedEqualComparesCapturedEncoding(QueryStringEncoding encoding, string? value,
        string? constant, bool expected)
    {
        var builder = new InterpreterBuilder();
        var equalLabel = builder.DeclareLabel("equal");
        EmitCaptureValue(builder, encoding, 0);
        var bytes = Encode(encoding, constant);
        builder.AddInstruction(OpCode.JmpIfEncodedEqual);
        builder.AddUInt16(0);
        builder.AddUInt32(builder.AddConstantBytes(bytes));
        builder.AddUInt32((uint)bytes.Length);
        builder.AddLabelTarget(equalLabel);
        EmitBoolResultEnd(builder, equalLabel);

        Assert.Equal(expected, RunFrameProgram(builder.Materialize(), new long[2], [], [], Encode(encoding, value),
            out _, out var valueEof));
        Assert.True(valueEof);
    }

    [Theory]
    [InlineData(QueryStringEncoding.String, "hello", "he", true)]
    [InlineData(QueryStringEncoding.String, "hello", "", true)]
    [InlineData(QueryStringEncoding.String, "hello", "hel😀", false)]
    [InlineData(QueryStringEncoding.String, "😀x", "😀", true)]
    [InlineData(QueryStringEncoding.StringOrdered, "hello", "he", true)]
    [InlineData(QueryStringEncoding.StringOrdered, "hello", "hello!", false)]
    [InlineData(QueryStringEncoding.StringOrdered, "žluť", "žl", true)]
    public void JmpIfEncodedStartsWithComparesEncodedPrefix(QueryStringEncoding encoding, string value, string prefix,
        bool expected)
    {
        Assert.Equal(expected, RunEncodedStartsWith(encoding, value, prefix));
    }

    [Theory]
    [InlineData(QueryStringEncoding.String)]
    [InlineData(QueryStringEncoding.StringOrdered)]
    public void JmpIfEncodedStartsWithThrowsForNull(QueryStringEncoding encoding)
    {
        Assert.Throws<NullReferenceException>(() => RunEncodedStartsWith(encoding, null, "a"));
    }

    [Theory]
    [InlineData(QueryStringEncoding.String)]
    [InlineData(QueryStringEncoding.StringOrdered)]
    public void DecodeStringDecodesIntoReusedCharBuffer(QueryStringEncoding encoding)
    {
        var builder = new InterpreterBuilder();
        EmitCaptureValue(builder, encoding, 0);
        builder.AddInstruction(OpCode.DecodeString);
        builder.AddUInt8((byte)encoding);
        builder.AddUInt16(0);
        builder.AddUInt16(0);
        builder.AddUInt16(16);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        var frame = new long[3];
        var objects = new object?[1];

        RunFrameProgram(program, frame, objects, [], Encode(encoding, "a😀ž"), out _, out _);
        var buffer = (char[])objects[0]!;
        Assert.Equal("a😀ž", new string(buffer, 0, (int)frame[2]));

        RunFrameProgram(program, frame, objects, [], Encode(encoding, "xy"), out _, out _);
        Assert.Same(buffer, objects[0]);
        Assert.Equal("xy", new string(buffer, 0, (int)frame[2]));

        RunFrameProgram(program, frame, objects, [], Encode(encoding, null), out _, out _);
        Assert.Equal(-1, (int)frame[2]);
    }

    [Theory]
    [InlineData(QueryStringOp.Equals, (byte)StringComparison.Ordinal, "abc", "abc", true)]
    [InlineData(QueryStringOp.Equals, (byte)StringComparison.Ordinal, null, null, true)]
    [InlineData(QueryStringOp.Equals, (byte)StringComparison.OrdinalIgnoreCase, "ABC", "abc", true)]
    [InlineData(QueryStringOp.InstanceEquals, (byte)StringComparison.Ordinal, "abc", null, false)]
    [InlineData(QueryStringOp.StartsWith, (byte)StringComparison.Ordinal, "abc", "ab", true)]
    [InlineData(QueryStringOp.EndsWith, (byte)StringComparison.OrdinalIgnoreCase, "abc", "BC", true)]
    [InlineData(QueryStringOp.Contains, (byte)StringComparison.Ordinal, "abc", "x", false)]
    [InlineData(QueryStringOp.CompareOrdinal, (byte)QueryCompare.Less, "B", "a", true)]
    [InlineData(QueryStringOp.CompareOrdinal, (byte)QueryCompare.Greater, "a", null, true)]
    [InlineData(QueryStringOp.CompareOrdinal, (byte)QueryCompare.Equal, null, null, true)]
    public void JmpIfStringOpEvaluatesStringOperations(QueryStringOp operation, byte argument, string? value,
        string? constant, bool expected)
    {
        Assert.Equal(expected, RunStringOp(operation, argument, value, constant));
    }

    [Fact]
    public void JmpIfStringOpSupportsCharsSetsAndThrowsForNullReceiver()
    {
        Assert.True(RunStringOp(QueryStringOp.StartsWith, (byte)StringComparison.Ordinal, "abc", 'a'));
        Assert.True(RunStringOp(QueryStringOp.Contains, (byte)StringComparison.Ordinal, "a😀c", "😀"[1]));
        var set = new QueryStringSet(["x", null]);
        Assert.True(RunStringOp(QueryStringOp.InSet, 0, "x", set));
        Assert.True(RunStringOp(QueryStringOp.InSet, 0, null, set));
        Assert.False(RunStringOp(QueryStringOp.InSet, 0, "y", set));
        Assert.Throws<NullReferenceException>(() =>
            RunStringOp(QueryStringOp.StartsWith, (byte)StringComparison.Ordinal, null, "a"));
        Assert.Throws<ArgumentNullException>(() =>
            RunStringOp(QueryStringOp.Contains, (byte)StringComparison.Ordinal, "a", null));
    }

    [Fact]
    public void StringLengthReadsLengthFromEncodingHeader()
    {
        var builder = new InterpreterBuilder();
        EmitCaptureValue(builder, QueryStringEncoding.String, 0);
        builder.AddInstruction(OpCode.StringLength);
        builder.AddUInt16(0);
        builder.AddUInt16(16);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        var frame = new long[3];

        RunFrameProgram(program, frame, [], [], Encode(QueryStringEncoding.String, "a😀"), out _, out _);
        Assert.Equal(3, (int)frame[2]);
        RunFrameProgram(program, frame, [], [], Encode(QueryStringEncoding.String, null), out _, out _);
        Assert.Equal(-1, (int)frame[2]);
    }

    [Fact]
    public void JmpIfFrameInSetUsesDefaultEqualityOfTheSet()
    {
        Assert.True(RunFrameInSet(QueryValueType.Int32, 5, System.Collections.Frozen.FrozenSet.ToFrozenSet([1, 5])));
        Assert.False(RunFrameInSet(QueryValueType.Int32, 4, System.Collections.Frozen.FrozenSet.ToFrozenSet([1, 5])));
        // Enumerable.Contains finds NaN, unlike the == operator.
        Assert.True(RunFrameInSet(QueryValueType.Double, double.NaN,
            System.Collections.Frozen.FrozenSet.ToFrozenSet([double.NaN])));
    }

    [Fact]
    public void SetFrameByteAndThrowQueryException()
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.SetFrameByte);
        builder.AddUInt16(3);
        builder.AddUInt8(7);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[1];
        RunFrameProgram(builder.Materialize(), frame, [], [], [], out _, out _);
        Assert.Equal(7L << 24, frame[0]);

        builder = new();
        builder.AddInstruction(OpCode.ThrowQueryException);
        builder.AddUInt8((byte)QueryException.NullableHasNoValue);
        builder.AddInstruction(OpCode.Stop);
        var program = builder.Materialize();
        Assert.Contains("Nullable object must have a value",
            Assert.Throws<InvalidOperationException>(() => RunFrameProgram(program, [], [], [], [], out _, out _))
                .Message);
    }

    [Fact]
    public void SkipValueByHandlerSkipsFieldWithFieldHandler()
    {
        var handler = Array.Find(BTDB.FieldHandler.BasicSerializersFactory.FieldHandlers, h => h.Name == "String")!;
        var writer = new MemWriter();
        writer.WriteString("skipped");
        writer.WriteVInt64(-3);
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.SkipValueByHandler);
        builder.AddUInt32((uint)builder.AddObjectConstant(handler));
        builder.AddInstruction(OpCode.ReadValueToFrame);
        builder.AddUInt8((byte)CodeOp.ReadVInt64);
        builder.AddUInt16(0);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[1];

        RunFrameProgram(builder.Materialize(), frame, [], [], writer.GetSpan(), out _, out var valueEof);

        Assert.Equal(-3, frame[0]);
        Assert.True(valueEof);
    }

    [Fact]
    public void DumpAsmIncludesExpressionOps()
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("target");
        var constant = builder.AllocAlignedConst(sizeof(int));
        builder.ConstSpan<int>(constant) = 5;
        builder.AddInstruction(OpCode.Arithmetic);
        builder.AddUInt8((byte)QueryValueType.Int32);
        builder.AddUInt8((byte)QueryArithmetic.Add);
        builder.AddUInt16(8);
        builder.AddUInt8((byte)QueryOperandKind.Frame);
        builder.AddUInt32(0);
        builder.AddUInt8((byte)QueryOperandKind.Constant);
        builder.AddUInt32(constant);
        builder.AddInstruction(OpCode.CaptureValue);
        builder.AddUInt8((byte)CodeOp.SkipString);
        builder.AddUInt16(16);
        builder.AddInstruction(OpCode.JmpIfNotEncodedEqual);
        builder.AddUInt16(16);
        builder.AddUInt32(builder.AddConstantBytes([2, 0x61]));
        builder.AddUInt32(2);
        builder.AddLabelTarget(label);
        builder.AddInstruction(OpCode.JmpIfStringOp);
        builder.AddUInt8((byte)QueryStringOp.Contains);
        builder.AddUInt8((byte)StringComparison.Ordinal);
        builder.AddUInt16(0);
        builder.AddUInt16(32);
        builder.AddUInt32((uint)builder.AddObjectConstant("x"));
        builder.AddLabelTarget(label);
        builder.MarkLabel(label);
        builder.AddInstruction(OpCode.Stop);

        var sb = new StringBuilder();
        builder.DumpAsm(sb);
        var asm = sb.ToString();

        Assert.Contains("Arithmetic Int32 Add frame[8] = frame[0], const[" + constant + "] ; 5", asm);
        Assert.Contains("CaptureValue SkipString frame[16]", asm);
        Assert.Contains("JmpIfNotEncodedEqual frame[16] const[", asm);
        Assert.Contains("] 0x0261 -> target", asm);
        Assert.Contains("JmpIfStringOp Contains Ordinal objects[0] length frame[32]", asm);
    }

    static byte[] Encode(QueryStringEncoding encoding, string? value)
    {
        var writer = new MemWriter();
        if (encoding == QueryStringEncoding.String) writer.WriteString(value);
        else writer.WriteStringOrdered(value);
        return writer.GetSpan().ToArray();
    }

    static void EmitCaptureValue(InterpreterBuilder builder, QueryStringEncoding encoding, ushort slot)
    {
        builder.AddInstruction(OpCode.CaptureValue);
        builder.AddUInt8((byte)(encoding == QueryStringEncoding.String ? CodeOp.SkipString : CodeOp.SkipStringOrdered));
        builder.AddUInt16(slot);
    }

    static void EmitBoolResultEnd(InterpreterBuilder builder, uint trueLabel)
    {
        builder.AddInstruction(OpCode.SetBoolResultFalse);
        builder.AddInstruction(OpCode.Stop);
        builder.MarkLabel(trueLabel);
        builder.AddInstruction(OpCode.SetBoolResultTrue);
        builder.AddInstruction(OpCode.Stop);
    }

    static bool RunFrameFrameCompare(QueryCompare compare, long left, long right)
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("true");
        builder.AddInstruction(OpCode.JmpIfFrameFrame);
        builder.AddUInt8((byte)QueryValueType.Int64);
        builder.AddUInt8((byte)compare);
        builder.AddUInt16(0);
        builder.AddUInt16(8);
        builder.AddLabelTarget(label);
        EmitBoolResultEnd(builder, label);
        return RunFrameProgram(builder.Materialize(), [left, right], [], [], [], out _, out _);
    }

    static T RunArithmetic<T>(QueryValueType type, QueryArithmetic operation, T left, T right) where T : unmanaged
    {
        var builder = new InterpreterBuilder();
        var constant = builder.AllocAlignedConst((uint)Unsafe.SizeOf<T>());
        builder.ConstSpan<T>(constant) = right;
        builder.AddInstruction(OpCode.Arithmetic);
        builder.AddUInt8((byte)type);
        builder.AddUInt8((byte)operation);
        builder.AddUInt16(16);
        builder.AddUInt8((byte)QueryOperandKind.Frame);
        builder.AddUInt32(0);
        builder.AddUInt8((byte)QueryOperandKind.Constant);
        builder.AddUInt32(constant);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[4];
        Unsafe.As<long, T>(ref frame[0]) = left;
        RunFrameProgram(builder.Materialize(), frame, [], [], [], out _, out _);
        return Unsafe.As<long, T>(ref frame[2]);
    }

    static TTo RunConvert<TFrom, TTo>(QueryValueType from, QueryValueType to, TFrom value, bool isChecked)
        where TFrom : unmanaged where TTo : unmanaged
    {
        var builder = new InterpreterBuilder();
        builder.AddInstruction(OpCode.ConvertNumber);
        builder.AddUInt8((byte)from);
        builder.AddUInt8((byte)to);
        builder.AddUInt8(isChecked ? (byte)1 : (byte)0);
        builder.AddUInt16(16);
        builder.AddUInt8((byte)QueryOperandKind.Frame);
        builder.AddUInt32(0);
        builder.AddInstruction(OpCode.Stop);
        var frame = new long[4];
        Unsafe.As<long, TFrom>(ref frame[0]) = value;
        RunFrameProgram(builder.Materialize(), frame, [], [], [], out _, out _);
        return Unsafe.As<long, TTo>(ref frame[2]);
    }

    static bool RunEncodedStartsWith(QueryStringEncoding encoding, string? value, string prefix)
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("true");
        EmitCaptureValue(builder, encoding, 0);
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

        var bytes = writer.GetSpan()[skip..];
        builder.AddInstruction(OpCode.JmpIfEncodedStartsWith);
        builder.AddUInt8((byte)encoding);
        builder.AddUInt16(0);
        builder.AddUInt32(builder.AddConstantBytes(bytes));
        builder.AddUInt32((uint)bytes.Length);
        builder.AddLabelTarget(label);
        EmitBoolResultEnd(builder, label);
        return RunFrameProgram(builder.Materialize(), new long[2], [], [], Encode(encoding, value), out _, out _);
    }

    static bool RunStringOp(QueryStringOp operation, byte argument, string? value, object? constant)
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("true");
        EmitCaptureValue(builder, QueryStringEncoding.String, 0);
        builder.AddInstruction(OpCode.DecodeString);
        builder.AddUInt8((byte)QueryStringEncoding.String);
        builder.AddUInt16(0);
        builder.AddUInt16(0);
        builder.AddUInt16(16);
        builder.AddInstruction(OpCode.JmpIfStringOp);
        builder.AddUInt8((byte)operation);
        builder.AddUInt8(argument);
        builder.AddUInt16(0);
        builder.AddUInt16(16);
        builder.AddUInt32((uint)builder.AddObjectConstant(constant));
        builder.AddLabelTarget(label);
        EmitBoolResultEnd(builder, label);
        return RunFrameProgram(builder.Materialize(), new long[3], new object?[1], [],
            Encode(QueryStringEncoding.String, value), out _, out _);
    }

    static bool RunFrameInSet<T>(QueryValueType type, T value, object set) where T : unmanaged
    {
        var builder = new InterpreterBuilder();
        var label = builder.DeclareLabel("true");
        builder.AddInstruction(OpCode.JmpIfFrameInSet);
        builder.AddUInt8((byte)type);
        builder.AddUInt16(0);
        builder.AddUInt32((uint)builder.AddObjectConstant(set));
        builder.AddLabelTarget(label);
        EmitBoolResultEnd(builder, label);
        var frame = new long[1];
        Unsafe.As<long, T>(ref frame[0]) = value;
        return RunFrameProgram(builder.Materialize(), frame, [], [], [], out _, out _);
    }

    class TestTypeConverterFactory : DefaultTypeConverterFactory
    {
    }
}
