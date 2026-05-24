using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Text;
using BTDB.Buffer;
using BTDB.Serialization;
using BTDB.StreamLayer;
using Microsoft.Extensions.Primitives;

namespace BTDB.Interpreter;

public enum OpCode : byte
{
    Invalid = 0,
    Stop = 1,
    SetBoolResultFalse = 2,
    SetBoolResultTrue = 3,
    NegateBoolResult = 4,
    SetParam1ObjectConst = 5,
    SetParam2ObjectConst = 6,
    SetParam1Const = 7,
    SetParam2Const = 8,
    EqualString = 9,
    LessString = 10,
    Jmp = 11,
    JmpIfTrue = 12,
    JmpIfFalse = 13,
    PushStack = 14,
    PopStack = 15,
    SetResultBySP = 16,
    SetParam1BySP = 17,
    SetParam2BySP = 18,
    StoreResultBySP = 19,
    StoreParam1BySP = 20,
    StoreParam2BySP = 21,
    LoadResultBySP = 22,
    LoadParam1BySP = 23,
    LoadParam2BySP = 24,
    IncrementRefResult = 25,
    DecrementRefResult = 26,
    IncrementRefParam1 = 27,
    DecrementRefParam1 = 28,
    IncrementRefParam2 = 29,
    DecrementRefParam2 = 30,
    SwapRefResultParam1 = 31,
    SwapRefParam1Param2 = 32,
    SwapRefResultParam2 = 33,
    StackAllocObject = 34,
    AllocObjectToResult = 35,
    AllocObjectToParam1 = 36,
    AllocObjectToParam2 = 37,
    DerefObjectResult = 38,
    DerefObjectParam1 = 39,
    DerefObjectParam2 = 40,
    AssignRefResultParam1 = 41,
    AssignRefResultParam2 = 42,
    AssignRefParam1Result = 43,
    AssignRefParam1Param2 = 44,
    AssignRefParam2Result = 45,
    AssignRefParam2Param1 = 46,
    EqualByte = 47,
    LessByte = 48,
    EqualSByte = 49,
    LessSByte = 50,
    EqualUInt16 = 51,
    LessUInt16 = 52,
    EqualInt16 = 53,
    LessInt16 = 54,
    EqualUInt32 = 55,
    LessUInt32 = 56,
    EqualInt32 = 57,
    LessInt32 = 58,
    EqualUInt64 = 59,
    LessUInt64 = 60,
    EqualInt64 = 61,
    LessInt64 = 62,
    EqualHalf = 63,
    LessHalf = 64,
    EqualFloat = 65,
    LessFloat = 66,
    EqualDouble = 67,
    LessDouble = 68,
    AddByte = 69,
    SubByte = 70,
    MulByte = 71,
    DivByte = 72,
    AddSByte = 73,
    SubSByte = 74,
    MulSByte = 75,
    DivSByte = 76,
    AddUInt16 = 77,
    SubUInt16 = 78,
    MulUInt16 = 79,
    DivUInt16 = 80,
    AddInt16 = 81,
    SubInt16 = 82,
    MulInt16 = 83,
    DivInt16 = 84,
    AddUInt32 = 85,
    SubUInt32 = 86,
    MulUInt32 = 87,
    DivUInt32 = 88,
    AddInt32 = 89,
    SubInt32 = 90,
    MulInt32 = 91,
    DivInt32 = 92,
    AddUInt64 = 93,
    SubUInt64 = 94,
    MulUInt64 = 95,
    DivUInt64 = 96,
    AddInt64 = 97,
    SubInt64 = 98,
    MulInt64 = 99,
    DivInt64 = 100,
    AddHalf = 101,
    SubHalf = 102,
    MulHalf = 103,
    DivHalf = 104,
    AddFloat = 105,
    SubFloat = 106,
    MulFloat = 107,
    DivFloat = 108,
    AddDouble = 109,
    SubDouble = 110,
    MulDouble = 111,
    DivDouble = 112,
    BinaryAndByte = 113,
    BinaryOrByte = 114,
    BinaryXorByte = 115,
    BinaryNotByte = 116,
    BinaryAndUInt16 = 117,
    BinaryOrUInt16 = 118,
    BinaryXorUInt16 = 119,
    BinaryNotUInt16 = 120,
    BinaryAndUInt32 = 121,
    BinaryOrUInt32 = 122,
    BinaryXorUInt32 = 123,
    BinaryNotUInt32 = 124,
    BinaryAndUInt64 = 125,
    BinaryOrUInt64 = 126,
    BinaryXorUInt64 = 127,
    BinaryNotUInt64 = 128,
    NegByte = 129,
    NegSByte = 130,
    NegUInt16 = 131,
    NegInt16 = 132,
    NegUInt32 = 133,
    NegInt32 = 134,
    NegUInt64 = 135,
    NegInt64 = 136,
    NegHalf = 137,
    NegFloat = 138,
    NegDouble = 139,
    CodeOps = 140,
    ThrowException = 141,
    Rethrow = 142,
    EndOfFinally = 143,
    CatchedException = 144,
    JmpFinally = 145,
    StackStructAlloc = 146,
    CallGetter = 147,
    CallSetter = 148,
    ConvertParam1ToResult = 149,
    StackBytesAlloc = 150
}

public enum CodeOp : byte
{
    Invalid = 0,
    Skip1Byte = 1,
    Skip2Bytes = 2,
    Skip4Bytes = 3,
    Skip8Bytes = 4,
    SkipVInt16 = 5,
    SkipVUInt16 = 6,
    SkipVInt32 = 7,
    SkipVUInt32 = 8,
    SkipVInt64 = 9,
    SkipVUInt64 = 10,
    SkipDateTimeOffset = 11,
    SkipString = 12,
    SkipStringOrdered = 13,
    SkipStringInUtf8 = 14,
    SkipByteArray = 15,
    SkipGuid = 16,
    SkipDecimal = 17,
    SkipIPAddress = 18,
    SkipVersion = 19,
    SkipStringValues = 20,
    SkipBlock = 21,
    ReadUInt8 = 22,
    ReadInt8 = 23,
    ReadInt8Ordered = 24,
    ReadBool = 25,
    ReadUInt16LE = 26,
    ReadUInt16BE = 27,
    ReadInt16LE = 28,
    ReadInt16BE = 29,
    ReadHalf = 30,
    ReadUInt32LE = 31,
    ReadUInt32BE = 32,
    ReadInt32LE = 33,
    ReadInt32BE = 34,
    ReadSingle = 35,
    ReadUInt64LE = 36,
    ReadUInt64BE = 37,
    ReadInt64LE = 38,
    ReadInt64BE = 39,
    ReadDouble = 40,
    ReadDoubleOrdered = 41,
    ReadVInt16 = 42,
    ReadVUInt16 = 43,
    ReadVInt32 = 44,
    ReadVUInt32 = 45,
    ReadVInt64 = 46,
    ReadVUInt64 = 47,
    ReadDateTime = 48,
    ReadDateTimeOffset = 49,
    ReadTimeSpan = 50,
    ReadString = 51,
    ReadStringOrdered = 52,
    ReadStringInUtf8 = 53,
    ReadGuid = 54,
    ReadDecimal = 55,
    ReadByteArray = 56,
    ReadByteArrayAsMemory = 57,
    ReadIPAddress = 58,
    ReadVersion = 59,
    ReadStringValues = 60,
    ReadPointer = 61,
    ReadBlock = 62,
    ReadByteArrayRaw = 63,
    ReadByteArrayRawTillEof = 64,
    ReadByteArrayRawTillEofAsMemory = 65,
    WriteByteZero = 66,
    WriteBool = 67,
    WriteUInt8 = 68,
    WriteInt8 = 69,
    WriteInt8Ordered = 70,
    WriteVInt16 = 71,
    WriteVUInt16 = 72,
    WriteVInt32 = 73,
    WriteVUInt32 = 74,
    WriteVInt64 = 75,
    WriteVUInt64 = 76,
    WriteInt64BE = 77,
    WriteInt64LE = 78,
    WriteUInt16LE = 79,
    WriteInt32BE = 80,
    WriteInt16BE = 81,
    WriteInt32LE = 82,
    WriteUInt32LE = 83,
    WriteUInt64LE = 84,
    WriteUInt64BE = 85,
    WriteDateTime = 86,
    WriteDateTimeForbidUnspecifiedKind = 87,
    WriteDateTimeOffset = 88,
    WriteTimeSpan = 89,
    WriteString = 90,
    WriteStringOrdered = 91,
    WriteStringOrderedPrefix = 92,
    WriteStringInUtf8 = 93,
    WriteGuid = 94,
    WriteSingle = 95,
    WriteDouble = 96,
    WriteDoubleOrdered = 97,
    WriteHalf = 98,
    WriteDecimal = 99,
    WriteByteArray = 100,
    WriteByteArrayFromByteBuffer = 101,
    WriteByteArrayFromMemory = 102,
    WriteByteArrayLength = 103,
    WriteByteArrayRaw = 104,
    WriteBlockFromByteArray = 105,
    WriteBlockFromByteBuffer = 106,
    WriteBlockFromMemory = 107,
    WriteIPAddress = 108,
    WriteVersion = 109,
    WriteStringValues = 110,
    WritePointer = 111
}

public ref struct InterpreterCtx
{
    public ref byte Result;
    public ref byte Param1;
    public ref byte Param2;
    public Span<byte> Stack;
    public ReadOnlySpan<object?> ObjectConstants;
    public ReadOnlySpan<uint> TryCatchFinallyBlocks;
    public Exception? CurrentException;
    public uint SP;
    public uint BP;
    public bool BoolResult;
    public ref byte BeginProgram;
    public ref byte BeginConstants;

    public InterpreterCtx(ref byte result, ref byte param1, ref byte param2, Span<byte> stack,
        ReadOnlySpan<object?> objectConstants, ref byte beginProgram, ref byte beginConstants)
    {
        Result = ref result;
        Param1 = ref param1;
        Param2 = ref param2;
        Stack = stack;
        ObjectConstants = objectConstants;
        TryCatchFinallyBlocks = default;
        CurrentException = null;
        SP = 0;
        BP = 0;
        BoolResult = false;
        BeginProgram = ref beginProgram;
        BeginConstants = ref beginConstants;
    }

    internal InterpreterCtx(ref byte result, ref byte param1, ref byte param2, Span<byte> stack,
        InterpreterProgram program)
        : this(ref result, ref param1, ref param2, stack, program.ObjectConstants, ref program.BeginProgram,
            ref program.BeginConstants)
    {
        TryCatchFinallyBlocks = program.TryCatchFinallyBlocks;
    }
}

class InterpreterBuilder
{
    readonly ArrayBufferWriter<byte> _program = new();
    readonly ArrayBufferWriter<byte> _constants = new();
    readonly List<object?> _objectConstants = [];
    readonly List<(uint Offset, string? Name)> _labels = [];
    readonly List<(uint Offset, Type Type)> _constantTypes = [];
    readonly List<(int Index, Type Type)> _typeParameters = [];
    readonly List<(ulong Pointer, Type Type)> _stackAllocatorTypes = [];
    readonly List<(uint Offset, Type Type, string FieldName)> _fieldAccessors = [];
    readonly List<(uint ParameterOffset, Type From, Type To, ITypeConverterFactory Factory)> _converters = [];
    readonly List<TryBlockBuilder> _tryBlocks = [];
    readonly List<int> _openTryBlocks = [];
    int _openStackAllocBlocks;
    bool _pendingStackAllocParameter;
    bool _needsOuterStopAfterStackAlloc;
    OpCode? _lastOpCode;

    public int AddObjectConstant(object? value)
    {
        for (var i = 0; i < _objectConstants.Count; i++)
        {
            if (Equals(_objectConstants[i], value)) return i;
        }

        _objectConstants.Add(value);
        return _objectConstants.Count - 1;
    }

    public uint AllocAlignedConst(uint size, uint alignment = 0)
    {
        if (size == 0) throw new ArgumentOutOfRangeException(nameof(size));
        if (alignment == 0) alignment = Math.Min(size, 8);
        var written = (uint)_constants.WrittenCount;
        var offset = AlignUp(written, alignment);
        var advance = checked((int)(offset + size - written));
        _constants.GetSpan(advance)[..advance].Clear();
        _constants.Advance(advance);
        return offset;
    }

    public ref T ConstSpan<T>(uint offset) where T : unmanaged
    {
        var end = checked(offset + (uint)Unsafe.SizeOf<T>());
        if (end > _constants.WrittenCount)
            throw new ArgumentOutOfRangeException(nameof(offset));
        RememberConstantType<T>(offset);
        return ref Unsafe.As<byte, T>(ref Unsafe.Add(ref MemoryMarshal.GetReference(_constants.WrittenSpan),
            (nint)offset));
    }

    public uint DeclareLabel(string? name = null)
    {
        var offset = AllocAlignedConst(sizeof(uint));
        _labels.Add((offset, name));
        return offset;
    }

    public void AddLabelParameter(uint label)
    {
        AddVUInt64(label / sizeof(uint));
    }

    public void AddTypeParameter(Type type)
    {
        var metadata = ReflectionMetadata.FindByType(type) ??
                       throw new ArgumentException($"Type {type.FullName} is not registered in metadata.",
                           nameof(type));
        var index = AddObjectConstant(metadata);
        RememberTypeParameter(index, type);
        AddVUInt64((ulong)index);
    }

    public unsafe void AddStackStructTypeParameter(Type type)
    {
        if (!type.IsValueType)
            throw new ArgumentException($"Type {type.FullName} is not a value type.", nameof(type));
        var allocator = ReflectionMetadata.FindStackAllocatorByType(type);
        var pointer = (ulong)(nint)allocator;
        _stackAllocatorTypes.Add((pointer, type));
        AddVUInt64(pointer);
    }

    public unsafe void AddGetterParameter(Type type, string fieldName)
    {
        var field = FindField(type, fieldName);
        if (field.PropRefGetter == null)
            throw new ArgumentException($"Field {type.FullName}.{fieldName} does not have getter metadata.",
                nameof(fieldName));
        AddFieldAccessorParameter(type, fieldName, (nint)field.PropRefGetter);
    }

    public unsafe void AddSetterParameter(Type type, string fieldName)
    {
        var field = FindField(type, fieldName);
        if (field.PropRefSetter == null)
            throw new ArgumentException($"Field {type.FullName}.{fieldName} does not have setter metadata.",
                nameof(fieldName));
        AddFieldAccessorParameter(type, fieldName, (nint)field.PropRefSetter);
    }

    public void AddConverterParameter(ITypeConverterFactory typeConverterFactory, Type from, Type to)
    {
        var converter = typeConverterFactory.GetConverter(from, to) ??
                        throw new ArgumentException($"Converter from {from.FullName} to {to.FullName} was not found.",
                            nameof(typeConverterFactory));
        var index = AddObjectConstant(converter);
        _converters.Add(((uint)_program.WrittenCount, from, to, typeConverterFactory));
        AddVUInt64((ulong)index);
    }

    public void AddConverterParameter(Type from, Type to)
    {
        AddConverterParameter(DefaultTypeConverterFactory.Instance, from, to);
    }

    public void MarkLabel(uint label)
    {
        ConstSpan<uint>(label) = (uint)_program.WrittenCount;
    }

    public void AddTry()
    {
        _tryBlocks.Add(new((uint)_program.WrittenCount));
        _openTryBlocks.Add(_tryBlocks.Count - 1);
    }

    public void AddCatch()
    {
        if (_openTryBlocks.Count == 0) throw new InvalidOperationException("No open try block.");
        var tryBlockIndex = _openTryBlocks[^1];
        AddInstruction(OpCode.JmpFinally);
        AddVUInt64((ulong)tryBlockIndex);
        ref var block = ref CurrentTryBlock();
        block.Catch = (uint)_program.WrittenCount;
    }

    public void AddFinally()
    {
        ref var block = ref CurrentTryBlock();
        block.Finally = (uint)_program.WrittenCount;
    }

    public void AddTryEnd()
    {
        ref var block = ref CurrentTryBlock();
        block.TryEnd = (uint)_program.WrittenCount;
        if (block.Finally == uint.MaxValue) block.Finally = block.TryEnd;
        _openTryBlocks.RemoveAt(_openTryBlocks.Count - 1);
    }

    public void AddInstruction(OpCode opCode)
    {
        _program.Write([(byte)opCode]);
        _lastOpCode = opCode;
        if (IsStackAllocOp(opCode))
        {
            _pendingStackAllocParameter = true;
        }
        else if (opCode == OpCode.Stop && _openStackAllocBlocks > 0)
        {
            AddFinally();
            _program.Write([(byte)OpCode.Stop]);
            AddTryEnd();
            _openStackAllocBlocks--;
            _needsOuterStopAfterStackAlloc = true;
        }
        else if (opCode == OpCode.Stop)
        {
            _needsOuterStopAfterStackAlloc = false;
        }
    }

    public void AddCodeOp(CodeOp codeOp)
    {
        _program.Write([(byte)codeOp]);
    }

    public void AddVUInt64(ulong value)
    {
        var len = PackUnpack.LengthVUInt(value);
        var span = _program.GetSpan((int)len);
        PackUnpack.UnsafePackVUInt(ref MemoryMarshal.GetReference(span), value, len);
        _program.Advance((int)len);
        FinishPendingStackAllocParameter();
    }

    public InterpreterProgram Materialize()
    {
        if (_openTryBlocks.Count != 0) throw new InvalidOperationException("Try block was not closed.");
        if (_program.WrittenCount != 0 && _lastOpCode != OpCode.Stop)
            throw new InvalidOperationException("Interpreter program must end with Stop opcode.");
        if (_needsOuterStopAfterStackAlloc)
            throw new InvalidOperationException("Stack allocation nested program must be followed by outer Stop opcode.");
        var constantsLength = _constants.WrittenCount;
        var image = new byte[constantsLength + _program.WrittenCount];
        _constants.WrittenSpan.CopyTo(image);
        _program.WrittenSpan.CopyTo(image.AsSpan(constantsLength));
        return new(image, (uint)constantsLength, _objectConstants.ToArray(), MaterializeTryBlocks());
    }

    public void DumpAsm(StringBuilder sb)
    {
        DumpConstants(sb);
        var program = _program.WrittenSpan;
        if (_constantTypes.Count != 0) sb.Append("program:\n");
        var offset = 0;
        var indent = 0;
        while (offset < program.Length)
        {
            DumpLabelsAt(sb, (uint)offset);
            DumpVirtualInstructionsAt(sb, (uint)offset, ref indent);
            var instructionOffset = offset;
            var opCode = (OpCode)program[offset++];
            AppendInstructionIndent(sb, indent);
            sb.Append(instructionOffset.ToString("D4"));
            sb.Append(": ");
            sb.Append(opCode);
            switch (opCode)
            {
                case OpCode.SetParam1ObjectConst:
                case OpCode.SetParam2ObjectConst:
                    var parameter = ReadVUInt64(program, ref offset);
                    sb.Append(' ');
                    sb.Append(parameter);
                    sb.Append(" ; ");
                    sb.Append(_objectConstants[(int)parameter]?.ToString());
                    break;
                case OpCode.AllocObjectToResult:
                case OpCode.AllocObjectToParam1:
                case OpCode.AllocObjectToParam2:
                    parameter = ReadVUInt64(program, ref offset);
                    sb.Append(' ');
                    AppendTypeParameter(sb, parameter);
                    break;
                case OpCode.SetParam1Const:
                case OpCode.SetParam2Const:
                    parameter = ReadVUInt64(program, ref offset);
                    sb.Append(' ');
                    sb.Append(parameter);
                    AppendInlineConstantValue(sb, (uint)parameter);
                    break;
                case OpCode.PushStack:
                case OpCode.PopStack:
                case OpCode.SetResultBySP:
                case OpCode.SetParam1BySP:
                case OpCode.SetParam2BySP:
                case OpCode.StoreResultBySP:
                case OpCode.StoreParam1BySP:
                case OpCode.StoreParam2BySP:
                case OpCode.LoadResultBySP:
                case OpCode.LoadParam1BySP:
                case OpCode.LoadParam2BySP:
                case OpCode.IncrementRefResult:
                case OpCode.DecrementRefResult:
                case OpCode.IncrementRefParam1:
                case OpCode.DecrementRefParam1:
                case OpCode.IncrementRefParam2:
                case OpCode.DecrementRefParam2:
                case OpCode.StackAllocObject:
                case OpCode.StackBytesAlloc:
                case OpCode.JmpFinally:
                    sb.Append(' ');
                    sb.Append(ReadVUInt64(program, ref offset));
                    break;
                case OpCode.StackStructAlloc:
                    parameter = ReadVUInt64(program, ref offset);
                    sb.Append(' ');
                    AppendStackAllocatorParameter(sb, parameter);
                    break;
                case OpCode.CallGetter:
                case OpCode.CallSetter:
                    parameter = ReadVUInt64(program, ref offset);
                    sb.Append(' ');
                    AppendFieldAccessorParameter(sb, (uint)parameter);
                    break;
                case OpCode.ConvertParam1ToResult:
                    var parameterOffset = (uint)offset;
                    parameter = ReadVUInt64(program, ref offset);
                    sb.Append(' ');
                    AppendConverterParameter(sb, parameterOffset, parameter);
                    break;
                case OpCode.Jmp:
                case OpCode.JmpIfTrue:
                case OpCode.JmpIfFalse:
                    sb.Append(' ');
                    sb.Append(GetLabelName(checked((uint)(ReadVUInt64(program, ref offset) * sizeof(uint)))));
                    break;
                case OpCode.CodeOps:
                    var codeOp = (CodeOp)program[offset++];
                    sb.Append(' ');
                    sb.Append(codeOp);
                    break;
            }

            sb.Append('\n');
        }

        DumpLabelsAt(sb, (uint)offset);
        DumpVirtualInstructionsAt(sb, (uint)offset, ref indent);
    }

    static uint AlignUp(uint value, uint alignment)
    {
        return checked(((value + alignment - 1) / alignment) * alignment);
    }

    void FinishPendingStackAllocParameter()
    {
        if (!_pendingStackAllocParameter) return;
        AddTry();
        _openStackAllocBlocks++;
        _pendingStackAllocParameter = false;
    }

    static bool IsStackAllocOp(OpCode opCode)
    {
        return opCode is OpCode.StackAllocObject or OpCode.StackStructAlloc or OpCode.StackBytesAlloc;
    }

    ref TryBlockBuilder CurrentTryBlock()
    {
        if (_openTryBlocks.Count == 0) throw new InvalidOperationException("No open try block.");
        return ref CollectionsMarshal.AsSpan(_tryBlocks)[_openTryBlocks[^1]];
    }

    uint[] MaterializeTryBlocks()
    {
        if (_tryBlocks.Count == 0) return [];
        var result = new uint[_tryBlocks.Count * 4];
        for (var i = 0; i < _tryBlocks.Count; i++)
        {
            var block = _tryBlocks[i];
            if (block.TryEnd == uint.MaxValue) throw new InvalidOperationException("Try block was not closed.");
            result[i * 4] = block.Try;
            result[i * 4 + 1] = block.Catch;
            result[i * 4 + 2] = block.Finally;
            result[i * 4 + 3] = block.TryEnd;
        }

        return result;
    }

    void DumpVirtualInstructionsAt(StringBuilder sb, uint programOffset, ref int indent)
    {
        for (var i = _tryBlocks.Count - 1; i >= 0; i--)
        {
            if (_tryBlocks[i].TryEnd != programOffset) continue;
            indent--;
            AppendVirtualIndent(sb, indent);
            sb.Append("TryEnd\n");
        }

        for (var i = _tryBlocks.Count - 1; i >= 0; i--)
        {
            var block = _tryBlocks[i];
            if (block.Catch != programOffset) continue;
            indent--;
            AppendVirtualIndent(sb, indent);
            sb.Append("Catch\n");
            indent++;
        }

        for (var i = _tryBlocks.Count - 1; i >= 0; i--)
        {
            var block = _tryBlocks[i];
            if (block.Finally != programOffset || block.Finally == block.TryEnd) continue;
            indent--;
            AppendVirtualIndent(sb, indent);
            sb.Append("Finally\n");
            indent++;
        }

        for (var i = 0; i < _tryBlocks.Count; i++)
        {
            if (_tryBlocks[i].Try != programOffset) continue;
            AppendVirtualIndent(sb, indent);
            sb.Append("Try\n");
            indent++;
        }
    }

    static void AppendInstructionIndent(StringBuilder sb, int indent)
    {
        sb.Append("  ");
        sb.Append(' ', indent * 2);
    }

    static void AppendVirtualIndent(StringBuilder sb, int indent)
    {
        sb.Append(' ', indent * 2);
    }

    void RememberConstantType<T>(uint offset) where T : unmanaged
    {
        var type = typeof(T);
        for (var i = 0; i < _constantTypes.Count; i++)
        {
            if (_constantTypes[i].Offset != offset) continue;
            _constantTypes[i] = (offset, type);
            return;
        }

        _constantTypes.Add((offset, type));
    }

    void RememberTypeParameter(int index, Type type)
    {
        for (var i = 0; i < _typeParameters.Count; i++)
        {
            if (_typeParameters[i].Index != index) continue;
            _typeParameters[i] = (index, type);
            return;
        }

        _typeParameters.Add((index, type));
    }

    void DumpConstants(StringBuilder sb)
    {
        if (_constantTypes.Count == 0) return;
        sb.Append("constants:\n");
        foreach (var (offset, type) in _constantTypes)
        {
            sb.Append("  ");
            sb.Append(offset.ToString("D4"));
            sb.Append(": ");
            sb.Append(type.Name);
            sb.Append(' ');
            DumpConstantValue(sb, offset, type);
            sb.Append('\n');
        }
    }

    void DumpConstantValue(StringBuilder sb, uint offset, Type type)
    {
        var span = _constants.WrittenSpan[(int)offset..];
        if (type == typeof(bool)) sb.Append(MemoryMarshal.Read<bool>(span));
        else if (type == typeof(byte)) sb.Append(MemoryMarshal.Read<byte>(span));
        else if (type == typeof(sbyte)) sb.Append(MemoryMarshal.Read<sbyte>(span));
        else if (type == typeof(short)) sb.Append(MemoryMarshal.Read<short>(span));
        else if (type == typeof(ushort)) sb.Append(MemoryMarshal.Read<ushort>(span));
        else if (type == typeof(int)) sb.Append(MemoryMarshal.Read<int>(span));
        else if (type == typeof(uint)) sb.Append(MemoryMarshal.Read<uint>(span));
        else if (type == typeof(long)) sb.Append(MemoryMarshal.Read<long>(span));
        else if (type == typeof(ulong)) sb.Append(MemoryMarshal.Read<ulong>(span));
        else if (type == typeof(float)) sb.Append(MemoryMarshal.Read<float>(span));
        else if (type == typeof(double)) sb.Append(MemoryMarshal.Read<double>(span));
        else if (type == typeof(char)) sb.Append(MemoryMarshal.Read<char>(span));
        else if (type == typeof(Guid)) sb.Append(MemoryMarshal.Read<Guid>(span));
        else DumpConstantBytes(sb, offset, Unsafe.SizeOf<nint>());
    }

    void AppendInlineConstantValue(StringBuilder sb, uint offset)
    {
        foreach (var constantType in _constantTypes)
        {
            if (constantType.Offset != offset) continue;
            sb.Append(" ; ");
            DumpConstantValue(sb, offset, constantType.Type);
            return;
        }
    }

    void AppendTypeParameter(StringBuilder sb, ulong index)
    {
        foreach (var typeParameter in _typeParameters)
        {
            if (typeParameter.Index != (int)index) continue;
            sb.Append(typeParameter.Type.FullName);
            return;
        }

        sb.Append(index);
    }

    void AppendStackAllocatorParameter(StringBuilder sb, ulong pointer)
    {
        foreach (var (allocatorPointer, type) in _stackAllocatorTypes)
        {
            if (allocatorPointer != pointer) continue;
            sb.Append(type.FullName);
            return;
        }

        sb.Append(pointer);
    }

    void AppendFieldAccessorParameter(StringBuilder sb, uint offset)
    {
        foreach (var accessor in _fieldAccessors)
        {
            if (accessor.Offset != offset) continue;
            sb.Append(accessor.Type.FullName);
            sb.Append('.');
            sb.Append(accessor.FieldName);
            return;
        }

        sb.Append(offset);
    }

    void AppendConverterParameter(StringBuilder sb, uint parameterOffset, ulong index)
    {
        foreach (var converter in _converters)
        {
            if (converter.ParameterOffset != parameterOffset) continue;
            sb.Append(converter.From.FullName);
            sb.Append(" -> ");
            sb.Append(converter.To.FullName);
            if (!IsDefaultTypeConverterFactory(converter.Factory))
            {
                sb.Append(" ; ");
                sb.Append(converter.Factory.GetType().FullName);
            }

            return;
        }

        sb.Append(index);
    }

    static bool IsDefaultTypeConverterFactory(ITypeConverterFactory factory)
    {
        return ReferenceEquals(factory, DefaultTypeConverterFactory.Instance) || factory.GetType() == typeof(DefaultTypeConverterFactory);
    }

    unsafe void AddFieldAccessorParameter(Type type, string fieldName, nint accessor)
    {
        var offset = AllocAlignedConst((uint)Unsafe.SizeOf<nint>());
        ConstSpan<nint>(offset) = accessor;
        _fieldAccessors.Add((offset, type, fieldName));
        AddVUInt64(offset);
    }

    static FieldMetadata FindField(Type type, string fieldName)
    {
        var metadata = ReflectionMetadata.FindByType(type) ??
                       throw new ArgumentException($"Type {type.FullName} is not registered in metadata.",
                           nameof(type));
        foreach (var field in metadata.Fields)
        {
            if (field.Name == fieldName) return field;
        }

        throw new ArgumentException($"Field {type.FullName}.{fieldName} was not found.", nameof(fieldName));
    }

    void DumpConstantBytes(StringBuilder sb, uint offset, int maxSize)
    {
        var span = _constants.WrittenSpan[(int)offset..Math.Min(_constants.WrittenCount, (int)offset + maxSize)];
        sb.Append("0x");
        foreach (var b in span)
            sb.Append(b.ToString("x2"));
    }

    static ulong ReadVUInt64(ReadOnlySpan<byte> span, ref int offset)
    {
        var len = PackUnpack.LengthVUIntByFirstByte(span[offset]);
        var value = PackUnpack.UnsafeUnpackVUInt(ref MemoryMarshal.GetReference(span.Slice(offset)), len);
        offset += (int)len;
        return value;
    }

    void DumpLabelsAt(StringBuilder sb, uint programOffset)
    {
        foreach (var label in _labels)
        {
            if (MemoryMarshal.Read<uint>(_constants.WrittenSpan.Slice((int)label.Offset)) != programOffset)
                continue;
            sb.Append(GetLabelName(label.Offset));
            sb.Append(":\n");
        }
    }

    string GetLabelName(uint label)
    {
        foreach (var (offset, name) in _labels)
        {
            if (offset == label) return name ?? $"L{label / sizeof(uint)}";
        }

        return $"L{label / sizeof(uint)}";
    }
}

struct TryBlockBuilder
{
    public TryBlockBuilder(uint @try)
    {
        Try = @try;
        Catch = uint.MaxValue;
        Finally = uint.MaxValue;
        TryEnd = uint.MaxValue;
    }

    public uint Try;
    public uint Catch;
    public uint Finally;
    public uint TryEnd;
}

class InterpreterProgram
{
    public InterpreterProgram(byte[] image, uint programOffset, object?[] objectConstants,
        uint[] tryCatchFinallyBlocks)
    {
        Image = image;
        ProgramOffset = programOffset;
        ObjectConstants = objectConstants;
        TryCatchFinallyBlocks = tryCatchFinallyBlocks;
    }

    public byte[] Image { get; }
    public uint ProgramOffset { get; }
    public object?[] ObjectConstants { get; }
    public uint[] TryCatchFinallyBlocks { get; }
    public ReadOnlySpan<byte> Program => Image.AsSpan((int)ProgramOffset);
    public ReadOnlySpan<byte> Constants => Image.AsSpan(0, (int)ProgramOffset);
    public ref byte BeginProgram => ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(Image),
        (nint)ProgramOffset);
    public ref byte BeginConstants => ref MemoryMarshal.GetArrayDataReference(Image);
}

static class Interpreter
{
    public static void Run(ref InterpreterCtx ctx)
    {
        var pc = 0u;
        Run(ref ctx, ref pc);
    }

    static void Run(ref InterpreterCtx ctx, ref uint pc)
    {
        var pendingStop = false;
        var pendingStopPc = 0u;
        var pendingFinallyBlockIndex = int.MaxValue;
        while (true)
        {
            var instructionPc = pc;
            try
            {
                var opCode = Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
                pc++;
                switch ((OpCode)opCode)
                {
                case OpCode.Invalid:
                    throw new InvalidOperationException("Invalid interpreter opcode.");
                case OpCode.Stop:
                    if (EnterNextFinallyForStop(ref ctx, ref pc, instructionPc, ref pendingFinallyBlockIndex))
                    {
                        pendingStop = true;
                        pendingStopPc = instructionPc;
                        continue;
                    }

                    return;
                case OpCode.SetBoolResultFalse:
                    ctx.BoolResult = false;
                    break;
                case OpCode.SetBoolResultTrue:
                    ctx.BoolResult = true;
                    break;
                case OpCode.SetParam1ObjectConst:
                    var index = ReadVUInt64(ref ctx, ref pc);
                    Unsafe.As<byte, object?>(ref ctx.Param1) = ctx.ObjectConstants[(int)index];
                    break;
                case OpCode.SetParam2ObjectConst:
                    index = ReadVUInt64(ref ctx, ref pc);
                    Unsafe.As<byte, object?>(ref ctx.Param2) = ctx.ObjectConstants[(int)index];
                    break;
                case OpCode.SetParam1Const:
                    var offset = ReadVUInt64(ref ctx, ref pc);
                    ctx.Param1 = ref Unsafe.AddByteOffset(ref ctx.BeginConstants, (nint)offset);
                    break;
                case OpCode.SetParam2Const:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    ctx.Param2 = ref Unsafe.AddByteOffset(ref ctx.BeginConstants, (nint)offset);
                    break;
                case OpCode.EqualString:
                    ctx.BoolResult = Unsafe.As<byte, string?>(ref ctx.Param1) ==
                                     Unsafe.As<byte, string?>(ref ctx.Param2);
                    break;
                case OpCode.LessString:
                    ctx.BoolResult = string.CompareOrdinal(Unsafe.As<byte, string?>(ref ctx.Param1),
                        Unsafe.As<byte, string?>(ref ctx.Param2)) < 0;
                    break;
                case OpCode.EqualByte:
                    ctx.BoolResult = Unsafe.As<byte, byte>(ref ctx.Param1) == Unsafe.As<byte, byte>(ref ctx.Param2);
                    break;
                case OpCode.LessByte:
                    ctx.BoolResult = Unsafe.As<byte, byte>(ref ctx.Param1) < Unsafe.As<byte, byte>(ref ctx.Param2);
                    break;
                case OpCode.EqualSByte:
                    ctx.BoolResult = Unsafe.As<byte, sbyte>(ref ctx.Param1) == Unsafe.As<byte, sbyte>(ref ctx.Param2);
                    break;
                case OpCode.LessSByte:
                    ctx.BoolResult = Unsafe.As<byte, sbyte>(ref ctx.Param1) < Unsafe.As<byte, sbyte>(ref ctx.Param2);
                    break;
                case OpCode.EqualUInt16:
                    ctx.BoolResult = Unsafe.As<byte, ushort>(ref ctx.Param1) == Unsafe.As<byte, ushort>(ref ctx.Param2);
                    break;
                case OpCode.LessUInt16:
                    ctx.BoolResult = Unsafe.As<byte, ushort>(ref ctx.Param1) < Unsafe.As<byte, ushort>(ref ctx.Param2);
                    break;
                case OpCode.EqualInt16:
                    ctx.BoolResult = Unsafe.As<byte, short>(ref ctx.Param1) == Unsafe.As<byte, short>(ref ctx.Param2);
                    break;
                case OpCode.LessInt16:
                    ctx.BoolResult = Unsafe.As<byte, short>(ref ctx.Param1) < Unsafe.As<byte, short>(ref ctx.Param2);
                    break;
                case OpCode.EqualUInt32:
                    ctx.BoolResult = Unsafe.As<byte, uint>(ref ctx.Param1) == Unsafe.As<byte, uint>(ref ctx.Param2);
                    break;
                case OpCode.LessUInt32:
                    ctx.BoolResult = Unsafe.As<byte, uint>(ref ctx.Param1) < Unsafe.As<byte, uint>(ref ctx.Param2);
                    break;
                case OpCode.EqualInt32:
                    ctx.BoolResult = Unsafe.As<byte, int>(ref ctx.Param1) == Unsafe.As<byte, int>(ref ctx.Param2);
                    break;
                case OpCode.LessInt32:
                    ctx.BoolResult = Unsafe.As<byte, int>(ref ctx.Param1) < Unsafe.As<byte, int>(ref ctx.Param2);
                    break;
                case OpCode.EqualUInt64:
                    ctx.BoolResult = Unsafe.As<byte, ulong>(ref ctx.Param1) == Unsafe.As<byte, ulong>(ref ctx.Param2);
                    break;
                case OpCode.LessUInt64:
                    ctx.BoolResult = Unsafe.As<byte, ulong>(ref ctx.Param1) < Unsafe.As<byte, ulong>(ref ctx.Param2);
                    break;
                case OpCode.EqualInt64:
                    ctx.BoolResult = Unsafe.As<byte, long>(ref ctx.Param1) == Unsafe.As<byte, long>(ref ctx.Param2);
                    break;
                case OpCode.LessInt64:
                    ctx.BoolResult = Unsafe.As<byte, long>(ref ctx.Param1) < Unsafe.As<byte, long>(ref ctx.Param2);
                    break;
                case OpCode.EqualHalf:
                    ctx.BoolResult = Unsafe.As<byte, Half>(ref ctx.Param1) == Unsafe.As<byte, Half>(ref ctx.Param2);
                    break;
                case OpCode.LessHalf:
                    ctx.BoolResult = Unsafe.As<byte, Half>(ref ctx.Param1) < Unsafe.As<byte, Half>(ref ctx.Param2);
                    break;
                case OpCode.EqualFloat:
                    ctx.BoolResult = Unsafe.As<byte, float>(ref ctx.Param1) == Unsafe.As<byte, float>(ref ctx.Param2);
                    break;
                case OpCode.LessFloat:
                    ctx.BoolResult = Unsafe.As<byte, float>(ref ctx.Param1) < Unsafe.As<byte, float>(ref ctx.Param2);
                    break;
                case OpCode.EqualDouble:
                    ctx.BoolResult = Unsafe.As<byte, double>(ref ctx.Param1) == Unsafe.As<byte, double>(ref ctx.Param2);
                    break;
                case OpCode.LessDouble:
                    ctx.BoolResult = Unsafe.As<byte, double>(ref ctx.Param1) < Unsafe.As<byte, double>(ref ctx.Param2);
                    break;
                case OpCode.AddByte:
                    Add<byte>(ref ctx);
                    break;
                case OpCode.SubByte:
                    Sub<byte>(ref ctx);
                    break;
                case OpCode.MulByte:
                    Mul<byte>(ref ctx);
                    break;
                case OpCode.DivByte:
                    Div<byte>(ref ctx);
                    break;
                case OpCode.AddSByte:
                    Add<sbyte>(ref ctx);
                    break;
                case OpCode.SubSByte:
                    Sub<sbyte>(ref ctx);
                    break;
                case OpCode.MulSByte:
                    Mul<sbyte>(ref ctx);
                    break;
                case OpCode.DivSByte:
                    Div<sbyte>(ref ctx);
                    break;
                case OpCode.AddUInt16:
                    Add<ushort>(ref ctx);
                    break;
                case OpCode.SubUInt16:
                    Sub<ushort>(ref ctx);
                    break;
                case OpCode.MulUInt16:
                    Mul<ushort>(ref ctx);
                    break;
                case OpCode.DivUInt16:
                    Div<ushort>(ref ctx);
                    break;
                case OpCode.AddInt16:
                    Add<short>(ref ctx);
                    break;
                case OpCode.SubInt16:
                    Sub<short>(ref ctx);
                    break;
                case OpCode.MulInt16:
                    Mul<short>(ref ctx);
                    break;
                case OpCode.DivInt16:
                    Div<short>(ref ctx);
                    break;
                case OpCode.AddUInt32:
                    Add<uint>(ref ctx);
                    break;
                case OpCode.SubUInt32:
                    Sub<uint>(ref ctx);
                    break;
                case OpCode.MulUInt32:
                    Mul<uint>(ref ctx);
                    break;
                case OpCode.DivUInt32:
                    Div<uint>(ref ctx);
                    break;
                case OpCode.AddInt32:
                    Add<int>(ref ctx);
                    break;
                case OpCode.SubInt32:
                    Sub<int>(ref ctx);
                    break;
                case OpCode.MulInt32:
                    Mul<int>(ref ctx);
                    break;
                case OpCode.DivInt32:
                    Div<int>(ref ctx);
                    break;
                case OpCode.AddUInt64:
                    Add<ulong>(ref ctx);
                    break;
                case OpCode.SubUInt64:
                    Sub<ulong>(ref ctx);
                    break;
                case OpCode.MulUInt64:
                    Mul<ulong>(ref ctx);
                    break;
                case OpCode.DivUInt64:
                    Div<ulong>(ref ctx);
                    break;
                case OpCode.AddInt64:
                    Add<long>(ref ctx);
                    break;
                case OpCode.SubInt64:
                    Sub<long>(ref ctx);
                    break;
                case OpCode.MulInt64:
                    Mul<long>(ref ctx);
                    break;
                case OpCode.DivInt64:
                    Div<long>(ref ctx);
                    break;
                case OpCode.AddHalf:
                    Add<Half>(ref ctx);
                    break;
                case OpCode.SubHalf:
                    Sub<Half>(ref ctx);
                    break;
                case OpCode.MulHalf:
                    Mul<Half>(ref ctx);
                    break;
                case OpCode.DivHalf:
                    Div<Half>(ref ctx);
                    break;
                case OpCode.AddFloat:
                    Add<float>(ref ctx);
                    break;
                case OpCode.SubFloat:
                    Sub<float>(ref ctx);
                    break;
                case OpCode.MulFloat:
                    Mul<float>(ref ctx);
                    break;
                case OpCode.DivFloat:
                    Div<float>(ref ctx);
                    break;
                case OpCode.AddDouble:
                    Add<double>(ref ctx);
                    break;
                case OpCode.SubDouble:
                    Sub<double>(ref ctx);
                    break;
                case OpCode.MulDouble:
                    Mul<double>(ref ctx);
                    break;
                case OpCode.DivDouble:
                    Div<double>(ref ctx);
                    break;
                case OpCode.BinaryAndByte:
                    BinaryAnd<byte>(ref ctx);
                    break;
                case OpCode.BinaryOrByte:
                    BinaryOr<byte>(ref ctx);
                    break;
                case OpCode.BinaryXorByte:
                    BinaryXor<byte>(ref ctx);
                    break;
                case OpCode.BinaryNotByte:
                    BinaryNot<byte>(ref ctx);
                    break;
                case OpCode.BinaryAndUInt16:
                    BinaryAnd<ushort>(ref ctx);
                    break;
                case OpCode.BinaryOrUInt16:
                    BinaryOr<ushort>(ref ctx);
                    break;
                case OpCode.BinaryXorUInt16:
                    BinaryXor<ushort>(ref ctx);
                    break;
                case OpCode.BinaryNotUInt16:
                    BinaryNot<ushort>(ref ctx);
                    break;
                case OpCode.BinaryAndUInt32:
                    BinaryAnd<uint>(ref ctx);
                    break;
                case OpCode.BinaryOrUInt32:
                    BinaryOr<uint>(ref ctx);
                    break;
                case OpCode.BinaryXorUInt32:
                    BinaryXor<uint>(ref ctx);
                    break;
                case OpCode.BinaryNotUInt32:
                    BinaryNot<uint>(ref ctx);
                    break;
                case OpCode.BinaryAndUInt64:
                    BinaryAnd<ulong>(ref ctx);
                    break;
                case OpCode.BinaryOrUInt64:
                    BinaryOr<ulong>(ref ctx);
                    break;
                case OpCode.BinaryXorUInt64:
                    BinaryXor<ulong>(ref ctx);
                    break;
                case OpCode.BinaryNotUInt64:
                    BinaryNot<ulong>(ref ctx);
                    break;
                case OpCode.NegByte:
                    Neg<byte>(ref ctx);
                    break;
                case OpCode.NegSByte:
                    Neg<sbyte>(ref ctx);
                    break;
                case OpCode.NegUInt16:
                    Neg<ushort>(ref ctx);
                    break;
                case OpCode.NegInt16:
                    Neg<short>(ref ctx);
                    break;
                case OpCode.NegUInt32:
                    Neg<uint>(ref ctx);
                    break;
                case OpCode.NegInt32:
                    Neg<int>(ref ctx);
                    break;
                case OpCode.NegUInt64:
                    Neg<ulong>(ref ctx);
                    break;
                case OpCode.NegInt64:
                    Neg<long>(ref ctx);
                    break;
                case OpCode.NegHalf:
                    Neg<Half>(ref ctx);
                    break;
                case OpCode.NegFloat:
                    Neg<float>(ref ctx);
                    break;
                case OpCode.NegDouble:
                    Neg<double>(ref ctx);
                    break;
                case OpCode.NegateBoolResult:
                    ctx.BoolResult = !ctx.BoolResult;
                    break;
                case OpCode.Jmp:
                    pc = ReadJumpTarget(ref ctx, ref pc);
                    break;
                case OpCode.JmpIfTrue:
                    var target = ReadJumpTarget(ref ctx, ref pc);
                    if (ctx.BoolResult)
                        pc = target;
                    break;
                case OpCode.JmpIfFalse:
                    target = ReadJumpTarget(ref ctx, ref pc);
                    if (!ctx.BoolResult)
                        pc = target;
                    break;
                case OpCode.PushStack:
                    var size = (uint)ReadVUInt64(ref ctx, ref pc);
                    EnsureStackSize(ref ctx, ctx.SP + size);
                    ctx.SP += size;
                    break;
                case OpCode.PopStack:
                    ctx.SP -= (uint)ReadVUInt64(ref ctx, ref pc);
                    break;
                case OpCode.SetResultBySP:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    ctx.Result = ref Unsafe.Add(ref MemoryMarshal.GetReference(ctx.Stack), (nint)(ctx.SP - offset));
                    break;
                case OpCode.SetParam1BySP:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    ctx.Param1 = ref Unsafe.Add(ref MemoryMarshal.GetReference(ctx.Stack), (nint)(ctx.SP - offset));
                    break;
                case OpCode.SetParam2BySP:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    ctx.Param2 = ref Unsafe.Add(ref MemoryMarshal.GetReference(ctx.Stack), (nint)(ctx.SP - offset));
                    break;
                case OpCode.StoreResultBySP:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    unsafe
                    {
                        Unsafe.As<byte, nint>(ref Unsafe.Add(ref MemoryMarshal.GetReference(ctx.Stack),
                            (nint)(ctx.SP - offset))) = (nint)Unsafe.AsPointer(ref ctx.Result);
                    }
                    break;
                case OpCode.StoreParam1BySP:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    unsafe
                    {
                        Unsafe.As<byte, nint>(ref Unsafe.Add(ref MemoryMarshal.GetReference(ctx.Stack),
                            (nint)(ctx.SP - offset))) = (nint)Unsafe.AsPointer(ref ctx.Param1);
                    }
                    break;
                case OpCode.StoreParam2BySP:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    unsafe
                    {
                        Unsafe.As<byte, nint>(ref Unsafe.Add(ref MemoryMarshal.GetReference(ctx.Stack),
                            (nint)(ctx.SP - offset))) = (nint)Unsafe.AsPointer(ref ctx.Param2);
                    }
                    break;
                case OpCode.LoadResultBySP:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    unsafe
                    {
                        ctx.Result = ref Unsafe.AsRef<byte>((void*)Unsafe.As<byte, nint>(
                            ref Unsafe.Add(ref MemoryMarshal.GetReference(ctx.Stack), (nint)(ctx.SP - offset))));
                    }
                    break;
                case OpCode.LoadParam1BySP:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    unsafe
                    {
                        ctx.Param1 = ref Unsafe.AsRef<byte>((void*)Unsafe.As<byte, nint>(
                            ref Unsafe.Add(ref MemoryMarshal.GetReference(ctx.Stack), (nint)(ctx.SP - offset))));
                    }
                    break;
                case OpCode.LoadParam2BySP:
                    offset = ReadVUInt64(ref ctx, ref pc);
                    unsafe
                    {
                        ctx.Param2 = ref Unsafe.AsRef<byte>((void*)Unsafe.As<byte, nint>(
                            ref Unsafe.Add(ref MemoryMarshal.GetReference(ctx.Stack), (nint)(ctx.SP - offset))));
                    }
                    break;
                case OpCode.IncrementRefResult:
                    ctx.Result = ref Unsafe.AddByteOffset(ref ctx.Result, (nint)ReadVUInt64(ref ctx, ref pc));
                    break;
                case OpCode.DecrementRefResult:
                    ctx.Result = ref Unsafe.AddByteOffset(ref ctx.Result, -(nint)ReadVUInt64(ref ctx, ref pc));
                    break;
                case OpCode.IncrementRefParam1:
                    ctx.Param1 = ref Unsafe.AddByteOffset(ref ctx.Param1, (nint)ReadVUInt64(ref ctx, ref pc));
                    break;
                case OpCode.DecrementRefParam1:
                    ctx.Param1 = ref Unsafe.AddByteOffset(ref ctx.Param1, -(nint)ReadVUInt64(ref ctx, ref pc));
                    break;
                case OpCode.IncrementRefParam2:
                    ctx.Param2 = ref Unsafe.AddByteOffset(ref ctx.Param2, (nint)ReadVUInt64(ref ctx, ref pc));
                    break;
                case OpCode.DecrementRefParam2:
                    ctx.Param2 = ref Unsafe.AddByteOffset(ref ctx.Param2, -(nint)ReadVUInt64(ref ctx, ref pc));
                    break;
                case OpCode.SwapRefResultParam1:
                    unsafe
                    {
                        var result = Unsafe.AsPointer(ref ctx.Result);
                        ctx.Result = ref ctx.Param1;
                        ctx.Param1 = ref Unsafe.AsRef<byte>(result);
                    }
                    break;
                case OpCode.SwapRefParam1Param2:
                    unsafe
                    {
                        var param1 = Unsafe.AsPointer(ref ctx.Param1);
                        ctx.Param1 = ref ctx.Param2;
                        ctx.Param2 = ref Unsafe.AsRef<byte>(param1);
                    }
                    break;
                case OpCode.SwapRefResultParam2:
                    unsafe
                    {
                        var result = Unsafe.AsPointer(ref ctx.Result);
                        ctx.Result = ref ctx.Param2;
                        ctx.Param2 = ref Unsafe.AsRef<byte>(result);
                    }
                    break;
                case OpCode.AllocObjectToResult:
                    index = ReadVUInt64(ref ctx, ref pc);
                    unsafe
                    {
                        Unsafe.As<byte, object?>(ref ctx.Result) = ((ClassMetadata)ctx.ObjectConstants[(int)index]!)
                            .Creator();
                    }
                    break;
                case OpCode.AllocObjectToParam1:
                    index = ReadVUInt64(ref ctx, ref pc);
                    unsafe
                    {
                        Unsafe.As<byte, object?>(ref ctx.Param1) = ((ClassMetadata)ctx.ObjectConstants[(int)index]!)
                            .Creator();
                    }
                    break;
                case OpCode.AllocObjectToParam2:
                    index = ReadVUInt64(ref ctx, ref pc);
                    unsafe
                    {
                        Unsafe.As<byte, object?>(ref ctx.Param2) = ((ClassMetadata)ctx.ObjectConstants[(int)index]!)
                            .Creator();
                    }
                    break;
                case OpCode.DerefObjectResult:
                    ctx.Result = ref RawData.Ref(Unsafe.As<byte, object?>(ref ctx.Result));
                    break;
                case OpCode.DerefObjectParam1:
                    ctx.Param1 = ref RawData.Ref(Unsafe.As<byte, object?>(ref ctx.Param1));
                    break;
                case OpCode.DerefObjectParam2:
                    ctx.Param2 = ref RawData.Ref(Unsafe.As<byte, object?>(ref ctx.Param2));
                    break;
                case OpCode.AssignRefResultParam1:
                    ctx.Result = ref ctx.Param1;
                    break;
                case OpCode.AssignRefResultParam2:
                    ctx.Result = ref ctx.Param2;
                    break;
                case OpCode.AssignRefParam1Result:
                    ctx.Param1 = ref ctx.Result;
                    break;
                case OpCode.AssignRefParam1Param2:
                    ctx.Param1 = ref ctx.Param2;
                    break;
                case OpCode.AssignRefParam2Result:
                    ctx.Param2 = ref ctx.Result;
                    break;
                case OpCode.AssignRefParam2Param1:
                    ctx.Param2 = ref ctx.Param1;
                    break;
                case OpCode.StackAllocObject:
                    var count = ReadVUInt64(ref ctx, ref pc);
                    StackAllocObject(ref ctx, ref pc, count);
                    break;
                case OpCode.StackBytesAlloc:
                    var bytesSize = ReadVUInt64(ref ctx, ref pc);
                    StackBytesAlloc(ref ctx, ref pc, bytesSize);
                    break;
                case OpCode.StackStructAlloc:
                    unsafe
                    {
                        var allocator =
                            (delegate*<ref byte, ref nint, delegate*<ref byte, void>, void>)(nint)ReadVUInt64(
                                ref ctx, ref pc);
                        StackStructAlloc(ref ctx, ref pc, allocator);
                    }

                    break;
                case OpCode.CallGetter:
                    unsafe
                    {
                        var accessor = (delegate*<object, ref byte, void>)Unsafe.As<byte, nint>(
                            ref Unsafe.AddByteOffset(ref ctx.BeginConstants, (nint)ReadVUInt64(ref ctx, ref pc)));
                        accessor(Unsafe.As<byte, object>(ref ctx.Param1), ref ctx.Result);
                    }

                    break;
                case OpCode.CallSetter:
                    unsafe
                    {
                        var accessor = (delegate*<object, ref byte, void>)Unsafe.As<byte, nint>(
                            ref Unsafe.AddByteOffset(ref ctx.BeginConstants, (nint)ReadVUInt64(ref ctx, ref pc)));
                        accessor(Unsafe.As<byte, object>(ref ctx.Param1), ref ctx.Param2);
                    }

                    break;
                case OpCode.ConvertParam1ToResult:
                    index = ReadVUInt64(ref ctx, ref pc);
                    ((Converter)ctx.ObjectConstants[(int)index]!)(ref ctx.Param1, ref ctx.Result);
                    break;
                case OpCode.CodeOps:
                    RunCodeOp(ref ctx, ref pc);
                    break;
                case OpCode.ThrowException:
                    ThrowExceptionFromParam1(ref ctx);
                    break;
                case OpCode.Rethrow:
                    Rethrow(ref ctx, ref pc, instructionPc);
                    break;
                case OpCode.EndOfFinally:
                    if (ctx.CurrentException != null)
                    {
                        PropagateCurrentException(ref ctx, ref pc, instructionPc);
                    }
                    else if (pendingStop)
                    {
                        if (EnterNextFinallyForStop(ref ctx, ref pc, pendingStopPc, ref pendingFinallyBlockIndex))
                            continue;
                        return;
                    }

                    break;
                case OpCode.CatchedException:
                    CatchedException(ref ctx, ref pc, instructionPc);
                    break;
                case OpCode.JmpFinally:
                    pc = ReadFinallyTarget(ref ctx, ref pc);
                    break;
                default:
                    throw new InvalidOperationException("Unknown interpreter opcode.");
                }
            }
            catch (Exception exception)
            {
                ctx.CurrentException = exception;
                if (HandleException(ref ctx, ref pc, instructionPc, -1)) continue;
                ExceptionDispatchInfo.Capture(exception).Throw();
            }
        }
    }

    static uint ReadJumpTarget(ref InterpreterCtx ctx, ref uint pc)
    {
        var labelIndex = ReadVUInt64(ref ctx, ref pc);
        var labelOffset = checked((nint)(labelIndex * sizeof(uint)));
        return Unsafe.As<byte, uint>(ref Unsafe.AddByteOffset(ref ctx.BeginConstants, labelOffset));
    }

    static uint ReadFinallyTarget(ref InterpreterCtx ctx, ref uint pc)
    {
        var tryBlockIndex = (int)ReadVUInt64(ref ctx, ref pc);
        var tableIndex = tryBlockIndex * 4 + 2;
        if ((uint)tableIndex >= (uint)ctx.TryCatchFinallyBlocks.Length)
            throw new InvalidOperationException("Invalid try block index.");
        return ctx.TryCatchFinallyBlocks[tableIndex];
    }

    static ulong ReadVUInt64(ref InterpreterCtx ctx, ref uint pc)
    {
        ref var pcRef = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        var len = PackUnpack.LengthVUIntByFirstByte(pcRef);
        ref var data = ref pcRef;
        var result = PackUnpack.UnsafeUnpackVUInt(ref data, len);
        pc += len;
        return result;
    }

    static void ThrowExceptionFromParam1(ref InterpreterCtx ctx)
    {
        throw Unsafe.As<byte, Exception?>(ref ctx.Param1) ?? new NullReferenceException();
    }

    static void Rethrow(ref InterpreterCtx ctx, ref uint pc, uint instructionPc)
    {
        if (ctx.CurrentException == null)
            throw new InvalidOperationException("Cannot rethrow without current exception.");
        var blockIndex = FindCatchBlockIndex(ctx.TryCatchFinallyBlocks, instructionPc);
        if (blockIndex >= 0)
        {
            var finallyPc = ctx.TryCatchFinallyBlocks[blockIndex + 2];
            var tryEndPc = ctx.TryCatchFinallyBlocks[blockIndex + 3];
            if (finallyPc != tryEndPc)
            {
                pc = finallyPc;
                return;
            }

            PropagateCurrentException(ref ctx, ref pc, instructionPc, blockIndex);
            return;
        }

        PropagateCurrentException(ref ctx, ref pc, instructionPc);
    }

    static void CatchedException(ref InterpreterCtx ctx, ref uint pc, uint instructionPc)
    {
        var blockIndex = FindCatchBlockIndex(ctx.TryCatchFinallyBlocks, instructionPc);
        if (blockIndex < 0) throw new InvalidOperationException("CatchedException must be used inside catch block.");
        ctx.CurrentException = null;
        var finallyPc = ctx.TryCatchFinallyBlocks[blockIndex + 2];
        var tryEndPc = ctx.TryCatchFinallyBlocks[blockIndex + 3];
        pc = finallyPc == tryEndPc ? tryEndPc : finallyPc;
    }

    static void PropagateCurrentException(ref InterpreterCtx ctx, ref uint pc, uint instructionPc,
        int skipBlockIndex = -1)
    {
        if (ctx.CurrentException == null) return;
        if (HandleException(ref ctx, ref pc, instructionPc, skipBlockIndex)) return;
        ExceptionDispatchInfo.Capture(ctx.CurrentException).Throw();
    }

    static bool HandleException(ref InterpreterCtx ctx, ref uint pc, uint instructionPc, int skipBlockIndex)
    {
        var blocks = ctx.TryCatchFinallyBlocks;
        var startIndex = skipBlockIndex >= 0 ? skipBlockIndex - 4 : blocks.Length - 4;
        for (var i = startIndex; i >= 0; i -= 4)
        {
            var tryPc = blocks[i];
            var catchPc = blocks[i + 1];
            var finallyPc = blocks[i + 2];
            var tryEndPc = blocks[i + 3];
            if (!Contains(instructionPc, tryPc, tryEndPc)) continue;
            if (finallyPc != tryEndPc && Contains(instructionPc, finallyPc, tryEndPc)) continue;
            if (catchPc != uint.MaxValue && Contains(instructionPc, catchPc, finallyPc))
            {
                if (finallyPc != tryEndPc)
                {
                    pc = finallyPc;
                    return true;
                }

                continue;
            }

            if (catchPc != uint.MaxValue)
            {
                pc = catchPc;
                unsafe
                {
                    ctx.Param1 = ref Unsafe.AsRef<byte>(Unsafe.AsPointer(ref ctx.CurrentException));
                }

                return true;
            }

            if (finallyPc != tryEndPc)
            {
                pc = finallyPc;
                return true;
            }
        }

        return false;
    }

    static bool EnterNextFinallyForStop(ref InterpreterCtx ctx, ref uint pc, uint stopPc, ref int maxBlockIndex)
    {
        var blocks = ctx.TryCatchFinallyBlocks;
        var startIndex = maxBlockIndex == int.MaxValue ? blocks.Length - 4 : maxBlockIndex - 4;
        for (var i = startIndex; i >= 0; i -= 4)
        {
            var tryPc = blocks[i];
            var finallyPc = blocks[i + 2];
            var tryEndPc = blocks[i + 3];
            if (finallyPc == tryEndPc) continue;
            if (!Contains(stopPc, tryPc, tryEndPc)) continue;
            if (Contains(stopPc, finallyPc, tryEndPc)) continue;
            maxBlockIndex = i;
            pc = finallyPc;
            return true;
        }

        return false;
    }

    static int FindCatchBlockIndex(ReadOnlySpan<uint> blocks, uint instructionPc)
    {
        for (var i = blocks.Length - 4; i >= 0; i -= 4)
        {
            var catchPc = blocks[i + 1];
            if (catchPc == uint.MaxValue) continue;
            if (Contains(instructionPc, catchPc, blocks[i + 2])) return i;
        }

        return -1;
    }

    static bool Contains(uint pc, uint start, uint end)
    {
        return pc >= start && pc < end;
    }

    static void Add<T>(ref InterpreterCtx ctx) where T : unmanaged, INumber<T>
    {
        Unsafe.As<byte, T>(ref ctx.Result) = Unsafe.As<byte, T>(ref ctx.Param1) + Unsafe.As<byte, T>(ref ctx.Param2);
    }

    static void Sub<T>(ref InterpreterCtx ctx) where T : unmanaged, INumber<T>
    {
        Unsafe.As<byte, T>(ref ctx.Result) = Unsafe.As<byte, T>(ref ctx.Param1) - Unsafe.As<byte, T>(ref ctx.Param2);
    }

    static void Mul<T>(ref InterpreterCtx ctx) where T : unmanaged, INumber<T>
    {
        Unsafe.As<byte, T>(ref ctx.Result) = Unsafe.As<byte, T>(ref ctx.Param1) * Unsafe.As<byte, T>(ref ctx.Param2);
    }

    static void Div<T>(ref InterpreterCtx ctx) where T : unmanaged, INumber<T>
    {
        Unsafe.As<byte, T>(ref ctx.Result) = Unsafe.As<byte, T>(ref ctx.Param1) / Unsafe.As<byte, T>(ref ctx.Param2);
    }

    static void Neg<T>(ref InterpreterCtx ctx) where T : unmanaged, INumber<T>
    {
        Unsafe.As<byte, T>(ref ctx.Result) = -Unsafe.As<byte, T>(ref ctx.Result);
    }

    static void BinaryAnd<T>(ref InterpreterCtx ctx) where T : unmanaged, IBinaryInteger<T>
    {
        Unsafe.As<byte, T>(ref ctx.Result) = Unsafe.As<byte, T>(ref ctx.Param1) & Unsafe.As<byte, T>(ref ctx.Param2);
    }

    static void BinaryOr<T>(ref InterpreterCtx ctx) where T : unmanaged, IBinaryInteger<T>
    {
        Unsafe.As<byte, T>(ref ctx.Result) = Unsafe.As<byte, T>(ref ctx.Param1) | Unsafe.As<byte, T>(ref ctx.Param2);
    }

    static void BinaryXor<T>(ref InterpreterCtx ctx) where T : unmanaged, IBinaryInteger<T>
    {
        Unsafe.As<byte, T>(ref ctx.Result) = Unsafe.As<byte, T>(ref ctx.Param1) ^ Unsafe.As<byte, T>(ref ctx.Param2);
    }

    static void BinaryNot<T>(ref InterpreterCtx ctx) where T : unmanaged, IBinaryInteger<T>
    {
        Unsafe.As<byte, T>(ref ctx.Result) = ~Unsafe.As<byte, T>(ref ctx.Param1);
    }

    static void RunCodeOp(ref InterpreterCtx ctx, ref uint pc)
    {
        var codeOp = (CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        pc++;
        ref var reader = ref Unsafe.As<byte, MemReader>(ref ctx.Param1);
        switch (codeOp)
        {
            case CodeOp.Invalid:
                throw new InvalidOperationException("Invalid interpreter code opcode.");
            case CodeOp.Skip1Byte:
                reader.Skip1Byte();
                break;
            case CodeOp.Skip2Bytes:
                reader.Skip2Bytes();
                break;
            case CodeOp.Skip4Bytes:
                reader.Skip4Bytes();
                break;
            case CodeOp.Skip8Bytes:
                reader.Skip8Bytes();
                break;
            case CodeOp.SkipVInt16:
                reader.SkipVInt16();
                break;
            case CodeOp.SkipVUInt16:
                reader.SkipVUInt16();
                break;
            case CodeOp.SkipVInt32:
                reader.SkipVInt32();
                break;
            case CodeOp.SkipVUInt32:
                reader.SkipVUInt32();
                break;
            case CodeOp.SkipVInt64:
                reader.SkipVInt64();
                break;
            case CodeOp.SkipVUInt64:
                reader.SkipVUInt64();
                break;
            case CodeOp.SkipDateTimeOffset:
                reader.SkipDateTimeOffset();
                break;
            case CodeOp.SkipString:
                reader.SkipString();
                break;
            case CodeOp.SkipStringOrdered:
                reader.SkipStringOrdered();
                break;
            case CodeOp.SkipStringInUtf8:
                reader.SkipStringInUtf8();
                break;
            case CodeOp.SkipByteArray:
                reader.SkipByteArray();
                break;
            case CodeOp.SkipGuid:
                reader.SkipGuid();
                break;
            case CodeOp.SkipDecimal:
                reader.SkipDecimal();
                break;
            case CodeOp.SkipIPAddress:
                reader.SkipIPAddress();
                break;
            case CodeOp.SkipVersion:
                reader.SkipVersion();
                break;
            case CodeOp.SkipStringValues:
                reader.SkipStringValues();
                break;
            case CodeOp.SkipBlock:
                reader.SkipBlock(Unsafe.As<byte, uint>(ref ctx.Param2));
                break;
            case CodeOp.ReadUInt8:
                Unsafe.As<byte, byte>(ref ctx.Result) = reader.ReadUInt8();
                break;
            case CodeOp.ReadInt8:
                Unsafe.As<byte, sbyte>(ref ctx.Result) = reader.ReadInt8();
                break;
            case CodeOp.ReadInt8Ordered:
                Unsafe.As<byte, sbyte>(ref ctx.Result) = reader.ReadInt8Ordered();
                break;
            case CodeOp.ReadBool:
                Unsafe.As<byte, bool>(ref ctx.Result) = reader.ReadBool();
                break;
            case CodeOp.ReadUInt16LE:
                Unsafe.As<byte, ushort>(ref ctx.Result) = reader.ReadUInt16LE();
                break;
            case CodeOp.ReadUInt16BE:
                Unsafe.As<byte, ushort>(ref ctx.Result) = reader.ReadUInt16BE();
                break;
            case CodeOp.ReadInt16LE:
                Unsafe.As<byte, short>(ref ctx.Result) = reader.ReadInt16LE();
                break;
            case CodeOp.ReadInt16BE:
                Unsafe.As<byte, short>(ref ctx.Result) = reader.ReadInt16BE();
                break;
            case CodeOp.ReadHalf:
                Unsafe.As<byte, Half>(ref ctx.Result) = reader.ReadHalf();
                break;
            case CodeOp.ReadUInt32LE:
                Unsafe.As<byte, uint>(ref ctx.Result) = reader.ReadUInt32LE();
                break;
            case CodeOp.ReadUInt32BE:
                Unsafe.As<byte, uint>(ref ctx.Result) = reader.ReadUInt32BE();
                break;
            case CodeOp.ReadInt32LE:
                Unsafe.As<byte, int>(ref ctx.Result) = reader.ReadInt32LE();
                break;
            case CodeOp.ReadInt32BE:
                Unsafe.As<byte, int>(ref ctx.Result) = reader.ReadInt32BE();
                break;
            case CodeOp.ReadSingle:
                Unsafe.As<byte, float>(ref ctx.Result) = reader.ReadSingle();
                break;
            case CodeOp.ReadUInt64LE:
                Unsafe.As<byte, ulong>(ref ctx.Result) = reader.ReadUInt64LE();
                break;
            case CodeOp.ReadUInt64BE:
                Unsafe.As<byte, ulong>(ref ctx.Result) = reader.ReadUInt64BE();
                break;
            case CodeOp.ReadInt64LE:
                Unsafe.As<byte, long>(ref ctx.Result) = reader.ReadInt64LE();
                break;
            case CodeOp.ReadInt64BE:
                Unsafe.As<byte, long>(ref ctx.Result) = reader.ReadInt64BE();
                break;
            case CodeOp.ReadDouble:
                Unsafe.As<byte, double>(ref ctx.Result) = reader.ReadDouble();
                break;
            case CodeOp.ReadDoubleOrdered:
                Unsafe.As<byte, double>(ref ctx.Result) = reader.ReadDoubleOrdered();
                break;
            case CodeOp.ReadVInt16:
                Unsafe.As<byte, short>(ref ctx.Result) = reader.ReadVInt16();
                break;
            case CodeOp.ReadVUInt16:
                Unsafe.As<byte, ushort>(ref ctx.Result) = reader.ReadVUInt16();
                break;
            case CodeOp.ReadVInt32:
                Unsafe.As<byte, int>(ref ctx.Result) = reader.ReadVInt32();
                break;
            case CodeOp.ReadVUInt32:
                Unsafe.As<byte, uint>(ref ctx.Result) = reader.ReadVUInt32();
                break;
            case CodeOp.ReadVInt64:
                Unsafe.As<byte, long>(ref ctx.Result) = reader.ReadVInt64();
                break;
            case CodeOp.ReadVUInt64:
                Unsafe.As<byte, ulong>(ref ctx.Result) = reader.ReadVUInt64();
                break;
            case CodeOp.ReadDateTime:
                Unsafe.As<byte, DateTime>(ref ctx.Result) = reader.ReadDateTime();
                break;
            case CodeOp.ReadDateTimeOffset:
                Unsafe.As<byte, DateTimeOffset>(ref ctx.Result) = reader.ReadDateTimeOffset();
                break;
            case CodeOp.ReadTimeSpan:
                Unsafe.As<byte, TimeSpan>(ref ctx.Result) = reader.ReadTimeSpan();
                break;
            case CodeOp.ReadString:
                Unsafe.As<byte, string?>(ref ctx.Result) = reader.ReadString();
                break;
            case CodeOp.ReadStringOrdered:
                Unsafe.As<byte, string?>(ref ctx.Result) = reader.ReadStringOrdered();
                break;
            case CodeOp.ReadStringInUtf8:
                Unsafe.As<byte, string>(ref ctx.Result) = reader.ReadStringInUtf8();
                break;
            case CodeOp.ReadGuid:
                Unsafe.As<byte, Guid>(ref ctx.Result) = reader.ReadGuid();
                break;
            case CodeOp.ReadDecimal:
                Unsafe.As<byte, decimal>(ref ctx.Result) = reader.ReadDecimal();
                break;
            case CodeOp.ReadByteArray:
                Unsafe.As<byte, byte[]?>(ref ctx.Result) = reader.ReadByteArray();
                break;
            case CodeOp.ReadByteArrayAsMemory:
                Unsafe.As<byte, ReadOnlyMemory<byte>>(ref ctx.Result) = reader.ReadByteArrayAsMemory();
                break;
            case CodeOp.ReadIPAddress:
                Unsafe.As<byte, IPAddress?>(ref ctx.Result) = reader.ReadIPAddress();
                break;
            case CodeOp.ReadVersion:
                Unsafe.As<byte, Version?>(ref ctx.Result) = reader.ReadVersion();
                break;
            case CodeOp.ReadStringValues:
                Unsafe.As<byte, StringValues>(ref ctx.Result) = reader.ReadStringValues();
                break;
            case CodeOp.ReadPointer:
                Unsafe.As<byte, nint>(ref ctx.Result) = reader.ReadPointer();
                break;
            case CodeOp.ReadBlock:
                reader.ReadBlock(ref ctx.Result, Unsafe.As<byte, uint>(ref ctx.Param2));
                break;
            case CodeOp.ReadByteArrayRaw:
                Unsafe.As<byte, byte[]>(ref ctx.Result) = reader.ReadByteArrayRaw((int)Unsafe.As<byte, uint>(ref ctx.Param2));
                break;
            case CodeOp.ReadByteArrayRawTillEof:
                Unsafe.As<byte, byte[]>(ref ctx.Result) = reader.ReadByteArrayRawTillEof();
                break;
            case CodeOp.ReadByteArrayRawTillEofAsMemory:
                Unsafe.As<byte, ReadOnlyMemory<byte>>(ref ctx.Result) = reader.ReadByteArrayRawTillEofAsMemory();
                break;
            case CodeOp.WriteByteZero:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteByteZero();
                break;
            case CodeOp.WriteBool:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteBool(Unsafe.As<byte, bool>(ref ctx.Param2));
                break;
            case CodeOp.WriteUInt8:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteUInt8(Unsafe.As<byte, byte>(ref ctx.Param2));
                break;
            case CodeOp.WriteInt8:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteInt8(Unsafe.As<byte, sbyte>(ref ctx.Param2));
                break;
            case CodeOp.WriteInt8Ordered:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteInt8Ordered(Unsafe.As<byte, sbyte>(ref ctx.Param2));
                break;
            case CodeOp.WriteVInt16:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteVInt16(Unsafe.As<byte, short>(ref ctx.Param2));
                break;
            case CodeOp.WriteVUInt16:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteVUInt16(Unsafe.As<byte, ushort>(ref ctx.Param2));
                break;
            case CodeOp.WriteVInt32:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteVInt32(Unsafe.As<byte, int>(ref ctx.Param2));
                break;
            case CodeOp.WriteVUInt32:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteVUInt32(Unsafe.As<byte, uint>(ref ctx.Param2));
                break;
            case CodeOp.WriteVInt64:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteVInt64(Unsafe.As<byte, long>(ref ctx.Param2));
                break;
            case CodeOp.WriteVUInt64:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteVUInt64(Unsafe.As<byte, ulong>(ref ctx.Param2));
                break;
            case CodeOp.WriteInt64BE:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteInt64BE(Unsafe.As<byte, long>(ref ctx.Param2));
                break;
            case CodeOp.WriteInt64LE:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteInt64LE(Unsafe.As<byte, long>(ref ctx.Param2));
                break;
            case CodeOp.WriteUInt16LE:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteUInt16LE(Unsafe.As<byte, ushort>(ref ctx.Param2));
                break;
            case CodeOp.WriteInt32BE:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteInt32BE(Unsafe.As<byte, int>(ref ctx.Param2));
                break;
            case CodeOp.WriteInt16BE:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteInt16BE(Unsafe.As<byte, short>(ref ctx.Param2));
                break;
            case CodeOp.WriteInt32LE:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteInt32LE(Unsafe.As<byte, int>(ref ctx.Param2));
                break;
            case CodeOp.WriteUInt32LE:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteUInt32LE(Unsafe.As<byte, uint>(ref ctx.Param2));
                break;
            case CodeOp.WriteUInt64LE:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteUInt64LE(Unsafe.As<byte, ulong>(ref ctx.Param2));
                break;
            case CodeOp.WriteUInt64BE:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteUInt64BE(Unsafe.As<byte, ulong>(ref ctx.Param2));
                break;
            case CodeOp.WriteDateTime:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteDateTime(Unsafe.As<byte, DateTime>(ref ctx.Param2));
                break;
            case CodeOp.WriteDateTimeForbidUnspecifiedKind:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1)
                    .WriteDateTimeForbidUnspecifiedKind(Unsafe.As<byte, DateTime>(ref ctx.Param2));
                break;
            case CodeOp.WriteDateTimeOffset:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1)
                    .WriteDateTimeOffset(Unsafe.As<byte, DateTimeOffset>(ref ctx.Param2));
                break;
            case CodeOp.WriteTimeSpan:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteTimeSpan(Unsafe.As<byte, TimeSpan>(ref ctx.Param2));
                break;
            case CodeOp.WriteString:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteString(Unsafe.As<byte, string?>(ref ctx.Param2));
                break;
            case CodeOp.WriteStringOrdered:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteStringOrdered(Unsafe.As<byte, string?>(ref ctx.Param2));
                break;
            case CodeOp.WriteStringOrderedPrefix:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteStringOrderedPrefix(Unsafe.As<byte, string>(ref ctx.Param2));
                break;
            case CodeOp.WriteStringInUtf8:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteStringInUtf8(Unsafe.As<byte, string>(ref ctx.Param2));
                break;
            case CodeOp.WriteGuid:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteGuid(Unsafe.As<byte, Guid>(ref ctx.Param2));
                break;
            case CodeOp.WriteSingle:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteSingle(Unsafe.As<byte, float>(ref ctx.Param2));
                break;
            case CodeOp.WriteDouble:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteDouble(Unsafe.As<byte, double>(ref ctx.Param2));
                break;
            case CodeOp.WriteDoubleOrdered:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteDoubleOrdered(Unsafe.As<byte, double>(ref ctx.Param2));
                break;
            case CodeOp.WriteHalf:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteHalf(Unsafe.As<byte, Half>(ref ctx.Param2));
                break;
            case CodeOp.WriteDecimal:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteDecimal(Unsafe.As<byte, decimal>(ref ctx.Param2));
                break;
            case CodeOp.WriteByteArray:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteByteArray(Unsafe.As<byte, byte[]?>(ref ctx.Param2));
                break;
            case CodeOp.WriteByteArrayFromByteBuffer:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteByteArray(Unsafe.As<byte, ByteBuffer>(ref ctx.Param2));
                break;
            case CodeOp.WriteByteArrayFromMemory:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1)
                    .WriteByteArray(Unsafe.As<byte, ReadOnlyMemory<byte>>(ref ctx.Param2));
                break;
            case CodeOp.WriteByteArrayLength:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1)
                    .WriteByteArrayLength(Unsafe.As<byte, ReadOnlyMemory<byte>>(ref ctx.Param2));
                break;
            case CodeOp.WriteByteArrayRaw:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteByteArrayRaw(Unsafe.As<byte, byte[]?>(ref ctx.Param2));
                break;
            case CodeOp.WriteBlockFromByteArray:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteBlock(Unsafe.As<byte, byte[]>(ref ctx.Param2));
                break;
            case CodeOp.WriteBlockFromByteBuffer:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteBlock(Unsafe.As<byte, ByteBuffer>(ref ctx.Param2));
                break;
            case CodeOp.WriteBlockFromMemory:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1)
                    .WriteBlock(Unsafe.As<byte, ReadOnlyMemory<byte>>(ref ctx.Param2));
                break;
            case CodeOp.WriteIPAddress:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteIPAddress(Unsafe.As<byte, IPAddress?>(ref ctx.Param2));
                break;
            case CodeOp.WriteVersion:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteVersion(Unsafe.As<byte, Version?>(ref ctx.Param2));
                break;
            case CodeOp.WriteStringValues:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WriteStringValues(Unsafe.As<byte, StringValues>(ref ctx.Param2));
                break;
            case CodeOp.WritePointer:
                Unsafe.As<byte, MemWriter>(ref ctx.Param1).WritePointer(Unsafe.As<byte, nint>(ref ctx.Param2));
                break;
            default:
                throw new InvalidOperationException("Unknown interpreter code opcode.");
        }
    }

    static void StackAllocObject(ref InterpreterCtx ctx, ref uint pc, ulong count)
    {
        switch (count)
        {
            case 1:
            {
                SmallArgs1 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 2:
            {
                SmallArgs2 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 3:
            {
                SmallArgs3 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 4:
            {
                SmallArgs4 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 5:
            {
                SmallArgs5 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 6:
            {
                SmallArgs6 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 7:
            {
                SmallArgs7 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 8:
            {
                SmallArgs8 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 9:
            {
                SmallArgs9 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 10:
            {
                SmallArgs10 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 11:
            {
                SmallArgs11 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 12:
            {
                SmallArgs12 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 13:
            {
                SmallArgs13 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 14:
            {
                SmallArgs14 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 15:
            {
                SmallArgs15 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            case 16:
            {
                SmallArgs16 args = default;
                SetParam2ToObjectRef(ref ctx, ref args.A);
                Run(ref ctx, ref pc);
                break;
            }
            default:
                throw new NotSupportedException("StackAllocObject supports only 1 to 16 object references.");
        }
    }

    static unsafe void StackStructAlloc(ref InterpreterCtx ctx, ref uint pc,
        delegate*<ref byte, ref nint, delegate*<ref byte, void>, void> allocator)
    {
        StackStructAllocCtx context = default;
        context.CtxPtr = (nint)Unsafe.AsPointer(ref ctx);
        context.Pc = pc;
        allocator(ref Unsafe.As<StackStructAllocCtx, byte>(ref context), ref context.StoragePtr,
            &NestedStackStructAlloc);
        pc = context.Pc;
    }

    static unsafe void NestedStackStructAlloc(ref byte value)
    {
        ref var context = ref Unsafe.As<byte, StackStructAllocCtx>(ref value);
        ref var ctx = ref Unsafe.AsRef<InterpreterCtx>((void*)context.CtxPtr);
        ctx.Param2 = ref Unsafe.AsRef<byte>((void*)context.StoragePtr);
        Run(ref ctx, ref context.Pc);
    }

    static unsafe void StackBytesAlloc(ref InterpreterCtx ctx, ref uint pc, ulong size)
    {
        if (size > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(size));
        var bytes = stackalloc byte[(int)size];
        ctx.Param2 = ref Unsafe.AsRef<byte>(bytes);
        Run(ref ctx, ref pc);
    }

    static unsafe void SetParam2ToObjectRef(ref InterpreterCtx ctx, ref object? value)
    {
        ctx.Param2 = ref Unsafe.AsRef<byte>(Unsafe.AsPointer(ref value));
    }

    static void EnsureStackSize(ref InterpreterCtx ctx, uint size)
    {
        if (size <= ctx.Stack.Length) return;
        var newSize = Math.Max(ctx.Stack.Length, 1);
        while (newSize < size)
            newSize *= 2;
        var newStack = new byte[newSize];
        ctx.Stack.CopyTo(newStack);
        ctx.Stack = newStack;
    }

#pragma warning disable CS0649
    struct StackStructAllocCtx
    {
        public nint CtxPtr;
        public uint Pc;
        public nint StoragePtr;
    }

    ref struct SmallArgs1
    {
        public object? A;
    }

    ref struct SmallArgs2
    {
        public object? A;
        public object? B;
    }

    ref struct SmallArgs3
    {
        public object? A;
        public object? B;
        public object? C;
    }

    ref struct SmallArgs4
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
    }

    ref struct SmallArgs5
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
    }

    ref struct SmallArgs6
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
    }

    ref struct SmallArgs7
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
    }

    ref struct SmallArgs8
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
        public object? H;
    }

    ref struct SmallArgs9
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
        public object? H;
        public object? I;
    }

    ref struct SmallArgs10
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
        public object? H;
        public object? I;
        public object? J;
    }

    ref struct SmallArgs11
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
        public object? H;
        public object? I;
        public object? J;
        public object? K;
    }

    ref struct SmallArgs12
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
        public object? H;
        public object? I;
        public object? J;
        public object? K;
        public object? L;
    }

    ref struct SmallArgs13
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
        public object? H;
        public object? I;
        public object? J;
        public object? K;
        public object? L;
        public object? M;
    }

    ref struct SmallArgs14
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
        public object? H;
        public object? I;
        public object? J;
        public object? K;
        public object? L;
        public object? M;
        public object? N;
    }

    ref struct SmallArgs15
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
        public object? H;
        public object? I;
        public object? J;
        public object? K;
        public object? L;
        public object? M;
        public object? N;
        public object? O;
    }

    ref struct SmallArgs16
    {
        public object? A;
        public object? B;
        public object? C;
        public object? D;
        public object? E;
        public object? F;
        public object? G;
        public object? H;
        public object? I;
        public object? J;
        public object? K;
        public object? L;
        public object? M;
        public object? N;
        public object? O;
        public object? P;
    }
#pragma warning restore CS0649
}
