using System;
using System.Buffers;
using System.IO;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Text;
using BTDB.Buffer;
using BTDB.FieldHandler;
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
    StackBytesAlloc = 150,
    EqualDateTime = 151,

    // Frame ops below use fixed-width little-endian operands and address InterpreterCtx.Frame (8-byte aligned bytes),
    // InterpreterCtx.Objects (object slots), InterpreterCtx.KeyReader and InterpreterCtx.ValueReader directly.

    /// Operands: CodeOp u8, frame byte offset u16. Runs a reading CodeOp on KeyReader into the frame.
    ReadKeyToFrame = 152,

    /// Operands: CodeOp u8, object slot index u16. Runs a reading CodeOp on KeyReader into an object slot.
    ReadKeyToObject = 153,

    /// Operands: CodeOp u8, frame byte offset u16. Runs a reading CodeOp on ValueReader into the frame.
    ReadValueToFrame = 154,

    /// Operands: CodeOp u8, object slot index u16. Runs a reading CodeOp on ValueReader into an object slot.
    ReadValueToObject = 155,

    /// Operands: skipping CodeOp u8, count u16. Runs the CodeOp count times on KeyReader.
    SkipKey = 156,

    /// Operands: skipping CodeOp u8, count u16. Runs the CodeOp count times on ValueReader.
    SkipValue = 157,

    /// Operands: QueryValueType u8, QueryCompare u8, frame byte offset u16, constant offset u32, target pc u32.
    /// Jumps when the comparison of the frame value with the constant is true.
    JmpIfFrameConst = 158,

    /// Same operands as JmpIfFrameConst; jumps when the comparison is false.
    JmpIfNotFrameConst = 159,

    /// Operands: QueryCompare u8, object slot index u16, object constant index u32, target pc u32.
    /// Jumps when the ordinal comparison of the string in the slot with the string constant is true.
    JmpIfStringConst = 160,

    /// Same operands as JmpIfStringConst; jumps when the comparison is false.
    JmpIfNotStringConst = 161,

    /// Operands: from QuerySlotKind u8, from slot u16, to QuerySlotKind u8, to slot u16, converter object constant
    /// index u32.
    ConvertSlot = 162,

    /// Operands: QueryValueType u8, QueryCompare u8, left frame offset u16, right frame offset u16, target pc u32.
    /// Jumps when the comparison of the two frame values is true.
    JmpIfFrameFrame = 163,

    /// Same operands as JmpIfFrameFrame; jumps when the comparison is false.
    JmpIfNotFrameFrame = 164,

    /// Operands: QueryValueType u8, QueryArithmetic u8, destination frame offset u16, left QueryOperandKind u8,
    /// left offset u32, right QueryOperandKind u8, right offset u32. Negations ignore the right operand.
    Arithmetic = 165,

    /// Operands: from QueryValueType u8, to QueryValueType u8, checked u8, destination frame offset u16, source
    /// QueryOperandKind u8, source offset u32. Converts like C# numeric casts.
    ConvertNumber = 166,

    /// Operands: skipping CodeOp u8, frame offset u16. Stores the pointer (nint) and length (int) of the next field's
    /// encoding in KeyReader into the 16 byte frame slot, then skips the field.
    CaptureKey = 167,

    /// Same as CaptureKey for ValueReader.
    CaptureValue = 168,

    /// Operands: captured frame offset u16, constant offset u32, constant length u32, target pc u32.
    /// Jumps when the captured encoding equals the constant bytes.
    JmpIfEncodedEqual = 169,

    /// Same operands as JmpIfEncodedEqual; jumps when the bytes differ.
    JmpIfNotEncodedEqual = 170,

    /// Operands: QueryStringEncoding u8, captured frame offset u16, constant offset u32, constant length u32, target
    /// pc u32. Jumps when the captured string starts with the encoded characters (ordinal); throws
    /// NullReferenceException for a null string.
    JmpIfEncodedStartsWith = 171,

    /// Same operands as JmpIfEncodedStartsWith; jumps when the string does not start with the prefix.
    JmpIfNotEncodedStartsWith = 172,

    /// Operands: QueryStringEncoding u8, captured frame offset u16, char[] object slot u16, length frame offset u16.
    /// Decodes the captured string into the reused char buffer and stores its length as int, -1 for null.
    DecodeString = 173,

    /// Operands: QueryStringOp u8, StringComparison or QueryCompare u8, char[] object slot u16, length frame offset
    /// u16, object constant index u32, target pc u32. Jumps when the string operation on decoded chars is true.
    JmpIfStringOp = 174,

    /// Same operands as JmpIfStringOp; jumps when the operation is false.
    JmpIfNotStringOp = 175,

    /// Operands: captured frame offset u16 of a QueryStringEncoding.String field, destination frame offset u16.
    /// Stores the string length as int, -1 for null, without decoding.
    StringLength = 176,

    /// Operands: QueryValueType u8, frame offset u16, object constant index u32 of IReadOnlySet, target pc u32.
    /// Jumps when the set contains the frame value.
    JmpIfFrameInSet = 177,

    /// Same operands as JmpIfFrameInSet; jumps when the set does not contain the value.
    JmpIfNotFrameInSet = 178,

    /// Operands: frame offset u16, value u8.
    SetFrameByte = 179,

    /// Operands: QueryException u8.
    ThrowQueryException = 180,

    /// Operands: IFieldHandler object constant index u32. Skips one field in KeyReader with the handler, which must
    /// not need a reader context.
    SkipKeyByHandler = 181,

    /// Same as SkipKeyByHandler for ValueReader.
    SkipValueByHandler = 182,

    /// Operands: size u8, destination frame offset u16, source QueryOperandKind u8, source offset u32.
    CopyToFrame = 183,

    /// Operands: QueryStringEncoding u8, captured frame offset u16, object slot index u16. Stores the captured string
    /// as a new string, or null.
    StringFromCaptured = 184,

    /// Operands: object slot index u16. Stores null.
    SetObjectNull = 185
}

public enum QueryValueType : byte
{
    UInt8 = 0,
    Int8 = 1,
    UInt16 = 2,
    Int16 = 3,
    UInt32 = 4,
    Int32 = 5,
    UInt64 = 6,
    Int64 = 7,
    Half = 8,
    Single = 9,
    Double = 10,
    DateTime = 11,
    Guid = 12,
    Decimal = 13,
    TimeSpan = 14,
    DateTimeOffset = 15
}

public enum QueryArithmetic : byte
{
    Add = 0,
    Subtract = 1,
    Multiply = 2,
    Divide = 3,
    Modulo = 4,
    AddChecked = 5,
    SubtractChecked = 6,
    MultiplyChecked = 7,
    Negate = 8,
    NegateChecked = 9
}

public enum QueryOperandKind : byte
{
    Frame = 0,
    Constant = 1
}

public enum QueryStringEncoding : byte
{
    String = 0,
    StringOrdered = 1
}

public enum QueryStringOp : byte
{
    /// Static string.Equals; null equals null. The operand byte is StringComparison.
    Equals = 0,

    /// Instance string.Equals; throws NullReferenceException for a null string. The operand byte is StringComparison.
    InstanceEquals = 1,

    /// The constant is a string or a char; throws for a null string. The operand byte is StringComparison.
    StartsWith = 2,
    EndsWith = 3,
    Contains = 4,

    /// string.CompareOrdinal(value, constant) compared to zero; the operand byte is QueryCompare.
    CompareOrdinal = 5,

    /// The constant is a QueryStringSet.
    InSet = 6
}

public enum QueryException : byte
{
    NullReference = 0,
    NullableHasNoValue = 1
}

/// Constant of OpCode.JmpIfStringOp with QueryStringOp.InSet.
public sealed class QueryStringSet
{
    public QueryStringSet(IEnumerable<string?> values)
    {
        var set = new HashSet<string>(StringComparer.Ordinal);
        foreach (var value in values)
        {
            if (value == null) ContainsNull = true;
            else set.Add(value);
        }

        Lookup = set.ToFrozenSet(StringComparer.Ordinal).GetAlternateLookup<ReadOnlySpan<char>>();
    }

    public FrozenSet<string>.AlternateLookup<ReadOnlySpan<char>> Lookup { get; }
    public bool ContainsNull { get; }

    public override string ToString() => $"[{string.Join(", ", Lookup.Set)}{(ContainsNull ? ", null" : "")}]";
}

public enum QueryCompare : byte
{
    Equal = 0,
    NotEqual = 1,
    Less = 2,
    LessOrEqual = 3,
    Greater = 4,
    GreaterOrEqual = 5
}

public enum QuerySlotKind : byte
{
    Frame = 0,
    Object = 1
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
    public ref byte Frame;
    public ref object? Objects;
    public ref MemReader KeyReader;
    public ref MemReader ValueReader;

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

    internal InterpreterCtx(InterpreterProgram program, ref byte frame, ref object? objects, ref MemReader keyReader,
        ref MemReader valueReader)
        : this(ref Unsafe.NullRef<byte>(), ref Unsafe.NullRef<byte>(), ref Unsafe.NullRef<byte>(), default, program)
    {
        Frame = ref frame;
        Objects = ref objects;
        KeyReader = ref keyReader;
        ValueReader = ref valueReader;
    }
}

class InterpreterBuilder
{
    readonly ArrayBufferWriter<byte> _program = new();
    readonly ArrayBufferWriter<byte> _constants = new();
    readonly List<object?> _objectConstants = [];
    readonly List<(uint Offset, string? Name)> _labels = [];
    readonly HashSet<uint> _markedLabels = [];
    readonly List<(uint ProgramOffset, uint Label)> _labelTargets = [];
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

    /// Copies bytes into the constants and returns their offset; empty bytes return 0 without allocating.
    public uint AddConstantBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty) return 0;
        var offset = AllocAlignedConst((uint)bytes.Length, 1);
        bytes.CopyTo(_constants.WrittenSpan.Length >= offset + bytes.Length
            ? MemoryMarshal.CreateSpan(ref Unsafe.Add(ref MemoryMarshal.GetReference(_constants.WrittenSpan), (nint)offset),
                bytes.Length)
            : throw new InvalidOperationException());
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
        _markedLabels.Add(label);
    }

    /// Writes a fixed-width u32 jump target operand which Materialize resolves to the label's program offset.
    public void AddLabelTarget(uint label)
    {
        _labelTargets.Add(((uint)_program.WrittenCount, label));
        AddUInt32(0);
    }

    public void AddUInt8(byte value)
    {
        _program.Write([value]);
    }

    public void AddUInt16(ushort value)
    {
        var span = _program.GetSpan(sizeof(ushort));
        Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(span), value);
        _program.Advance(sizeof(ushort));
    }

    public void AddUInt32(uint value)
    {
        var span = _program.GetSpan(sizeof(uint));
        Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(span), value);
        _program.Advance(sizeof(uint));
    }

    /// Writes a fixed-width u32 object constant index of the resolved converter, used by OpCode.ConvertSlot.
    public void AddConverterIndex(ITypeConverterFactory typeConverterFactory, Type from, Type to)
    {
        var converter = typeConverterFactory.GetConverter(from, to) ??
                        throw new ArgumentException($"Converter from {from.FullName} to {to.FullName} was not found.",
                            nameof(typeConverterFactory));
        var index = AddObjectConstant(converter);
        _converters.Add(((uint)_program.WrittenCount, from, to, typeConverterFactory));
        AddUInt32((uint)index);
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
        foreach (var (programOffset, label) in _labelTargets)
        {
            if (!_markedLabels.Contains(label))
                throw new InvalidOperationException($"Label {GetLabelName(label)} was not marked.");
            var target = MemoryMarshal.Read<uint>(_constants.WrittenSpan[(int)label..]);
            Unsafe.WriteUnaligned(ref image[constantsLength + (int)programOffset], target);
        }
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
                case OpCode.ReadKeyToFrame:
                case OpCode.ReadValueToFrame:
                    sb.Append(' ').Append((CodeOp)program[offset]).Append(" frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append(']');
                    offset += 3;
                    break;
                case OpCode.ReadKeyToObject:
                case OpCode.ReadValueToObject:
                    sb.Append(' ').Append((CodeOp)program[offset]).Append(" objects[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append(']');
                    offset += 3;
                    break;
                case OpCode.SkipKey:
                case OpCode.SkipValue:
                    sb.Append(' ').Append((CodeOp)program[offset]).Append(" x")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..]));
                    offset += 3;
                    break;
                case OpCode.JmpIfFrameConst:
                case OpCode.JmpIfNotFrameConst:
                    var constantOffset = MemoryMarshal.Read<uint>(program[(offset + 4)..]);
                    sb.Append(' ').Append((QueryValueType)program[offset]).Append(' ')
                        .Append((QueryCompare)program[offset + 1]).Append(" frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 2)..])).Append("] const[")
                        .Append(constantOffset).Append(']');
                    AppendInlineConstantValue(sb, constantOffset);
                    sb.Append(" -> ");
                    AppendLabelTarget(sb, (uint)offset + 8);
                    offset += 12;
                    break;
                case OpCode.JmpIfStringConst:
                case OpCode.JmpIfNotStringConst:
                    var objectConstant = MemoryMarshal.Read<uint>(program[(offset + 3)..]);
                    sb.Append(' ').Append((QueryCompare)program[offset]).Append(" objects[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append("] ").Append(objectConstant)
                        .Append(" ; ").Append(_objectConstants[(int)objectConstant]?.ToString() ?? "null").Append(" -> ");
                    AppendLabelTarget(sb, (uint)offset + 7);
                    offset += 11;
                    break;
                case OpCode.JmpIfFrameFrame:
                case OpCode.JmpIfNotFrameFrame:
                    sb.Append(' ').Append((QueryValueType)program[offset]).Append(' ')
                        .Append((QueryCompare)program[offset + 1]).Append(" frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 2)..])).Append("] frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 4)..])).Append("] -> ");
                    AppendLabelTarget(sb, (uint)offset + 6);
                    offset += 10;
                    break;
                case OpCode.Arithmetic:
                    sb.Append(' ').Append((QueryValueType)program[offset]).Append(' ')
                        .Append((QueryArithmetic)program[offset + 1]).Append(" frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 2)..])).Append("] = ");
                    AppendOperand(sb, (QueryOperandKind)program[offset + 4], MemoryMarshal.Read<uint>(program[(offset + 5)..]));
                    sb.Append(", ");
                    AppendOperand(sb, (QueryOperandKind)program[offset + 9],
                        MemoryMarshal.Read<uint>(program[(offset + 10)..]));
                    offset += 14;
                    break;
                case OpCode.ConvertNumber:
                    sb.Append(' ').Append((QueryValueType)program[offset]).Append(" -> ")
                        .Append((QueryValueType)program[offset + 1]).Append(program[offset + 2] != 0 ? " checked" : "")
                        .Append(" frame[").Append(MemoryMarshal.Read<ushort>(program[(offset + 3)..])).Append("] = ");
                    AppendOperand(sb, (QueryOperandKind)program[offset + 5], MemoryMarshal.Read<uint>(program[(offset + 6)..]));
                    offset += 10;
                    break;
                case OpCode.CaptureKey:
                case OpCode.CaptureValue:
                    sb.Append(' ').Append((CodeOp)program[offset]).Append(" frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append(']');
                    offset += 3;
                    break;
                case OpCode.JmpIfEncodedEqual:
                case OpCode.JmpIfNotEncodedEqual:
                    sb.Append(" frame[").Append(MemoryMarshal.Read<ushort>(program[offset..])).Append("] ");
                    AppendConstantBytes(sb, MemoryMarshal.Read<uint>(program[(offset + 2)..]),
                        MemoryMarshal.Read<uint>(program[(offset + 6)..]));
                    sb.Append(" -> ");
                    AppendLabelTarget(sb, (uint)offset + 10);
                    offset += 14;
                    break;
                case OpCode.JmpIfEncodedStartsWith:
                case OpCode.JmpIfNotEncodedStartsWith:
                    sb.Append(' ').Append((QueryStringEncoding)program[offset]).Append(" frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append("] ");
                    AppendConstantBytes(sb, MemoryMarshal.Read<uint>(program[(offset + 3)..]),
                        MemoryMarshal.Read<uint>(program[(offset + 7)..]));
                    sb.Append(" -> ");
                    AppendLabelTarget(sb, (uint)offset + 11);
                    offset += 15;
                    break;
                case OpCode.DecodeString:
                    sb.Append(' ').Append((QueryStringEncoding)program[offset]).Append(" frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append("] -> objects[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 3)..])).Append("] length frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 5)..])).Append(']');
                    offset += 7;
                    break;
                case OpCode.JmpIfStringOp:
                case OpCode.JmpIfNotStringOp:
                    var stringOp = (QueryStringOp)program[offset];
                    sb.Append(' ').Append(stringOp).Append(' ');
                    if (stringOp == QueryStringOp.CompareOrdinal) sb.Append((QueryCompare)program[offset + 1]);
                    else if (stringOp != QueryStringOp.InSet) sb.Append((StringComparison)program[offset + 1]);
                    var stringConstant = MemoryMarshal.Read<uint>(program[(offset + 6)..]);
                    sb.Append(" objects[").Append(MemoryMarshal.Read<ushort>(program[(offset + 2)..]))
                        .Append("] length frame[").Append(MemoryMarshal.Read<ushort>(program[(offset + 4)..]))
                        .Append("] ").Append(stringConstant).Append(" ; ")
                        .Append(_objectConstants[(int)stringConstant]?.ToString() ?? "null").Append(" -> ");
                    AppendLabelTarget(sb, (uint)offset + 10);
                    offset += 14;
                    break;
                case OpCode.StringLength:
                    sb.Append(" frame[").Append(MemoryMarshal.Read<ushort>(program[offset..])).Append("] -> frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 2)..])).Append(']');
                    offset += 4;
                    break;
                case OpCode.JmpIfFrameInSet:
                case OpCode.JmpIfNotFrameInSet:
                    var setConstant = MemoryMarshal.Read<uint>(program[(offset + 3)..]);
                    sb.Append(' ').Append((QueryValueType)program[offset]).Append(" frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append("] ").Append(setConstant)
                        .Append(" -> ");
                    AppendLabelTarget(sb, (uint)offset + 7);
                    offset += 11;
                    break;
                case OpCode.SetFrameByte:
                    sb.Append(" frame[").Append(MemoryMarshal.Read<ushort>(program[offset..])).Append("] = ")
                        .Append(program[offset + 2]);
                    offset += 3;
                    break;
                case OpCode.ThrowQueryException:
                    sb.Append(' ').Append((QueryException)program[offset]);
                    offset += 1;
                    break;
                case OpCode.StringFromCaptured:
                    sb.Append(' ').Append((QueryStringEncoding)program[offset]).Append(" frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append("] -> objects[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 3)..])).Append(']');
                    offset += 5;
                    break;
                case OpCode.SetObjectNull:
                    sb.Append(" objects[").Append(MemoryMarshal.Read<ushort>(program[offset..])).Append(']');
                    offset += 2;
                    break;
                case OpCode.CopyToFrame:
                    sb.Append(' ').Append(program[offset]).Append(" bytes frame[")
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append("] = ");
                    AppendOperand(sb, (QueryOperandKind)program[offset + 3], MemoryMarshal.Read<uint>(program[(offset + 4)..]));
                    offset += 8;
                    break;
                case OpCode.SkipKeyByHandler:
                case OpCode.SkipValueByHandler:
                    var handlerConstant = MemoryMarshal.Read<uint>(program[offset..]);
                    sb.Append(' ').Append(handlerConstant).Append(" ; ")
                        .Append((_objectConstants[(int)handlerConstant] as IFieldHandler)?.Name);
                    offset += 4;
                    break;
                case OpCode.ConvertSlot:
                    sb.Append(' ').Append((QuerySlotKind)program[offset]).Append('[')
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 1)..])).Append("] -> ")
                        .Append((QuerySlotKind)program[offset + 3]).Append('[')
                        .Append(MemoryMarshal.Read<ushort>(program[(offset + 4)..])).Append("] ");
                    AppendConverterParameter(sb, (uint)offset + 6, MemoryMarshal.Read<uint>(program[(offset + 6)..]));
                    offset += 10;
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

    void AppendOperand(StringBuilder sb, QueryOperandKind kind, uint offset)
    {
        if (kind == QueryOperandKind.Frame)
        {
            sb.Append("frame[").Append(offset).Append(']');
            return;
        }

        sb.Append("const[").Append(offset).Append(']');
        AppendInlineConstantValue(sb, offset);
    }

    void AppendConstantBytes(StringBuilder sb, uint offset, uint length)
    {
        sb.Append("const[").Append(offset).Append("] 0x");
        foreach (var b in _constants.WrittenSpan.Slice((int)offset, (int)length))
            sb.Append(b.ToString("x2"));
    }

    void AppendLabelTarget(StringBuilder sb, uint programOffset)
    {
        foreach (var (offset, label) in _labelTargets)
        {
            if (offset != programOffset) continue;
            sb.Append(GetLabelName(label));
            return;
        }

        sb.Append('?');
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
        if (ctx.TryCatchFinallyBlocks.IsEmpty)
            RunWithoutTryBlocks(ref ctx, ref pc);
        else
            RunWithTryBlocks(ref ctx, ref pc);
    }

    // Programs without try blocks cannot handle exceptions, so the instruction loop runs without any exception
    // bookkeeping and exceptions simply leave this Run.
    static void RunWithoutTryBlocks(ref InterpreterCtx ctx, ref uint pc)
    {
        var state = new RunState { PendingFinallyBlockIndex = int.MaxValue };
        try
        {
            Loop<WithoutTryBlocks>(ref ctx, ref pc, ref state);
        }
        catch (Exception exception)
        {
            ctx.CurrentException = exception;
            throw;
        }
    }

    static void RunWithTryBlocks(ref InterpreterCtx ctx, ref uint pc)
    {
        var state = new RunState { PendingFinallyBlockIndex = int.MaxValue };
        while (true)
        {
            try
            {
                Loop<WithTryBlocks>(ref ctx, ref pc, ref state);
                break;
            }
            catch (Exception exception)
            {
                ctx.CurrentException = exception;
                if (HandleException(ref ctx, ref pc, state.InstructionPc, -1)) continue;
                ExceptionDispatchInfo.Capture(exception).Throw();
            }
        }

        // A finally block entered because of an exception ended, for example the one closing a stack allocation. The
        // exception leaves this Run, so a nested Run never continues in outer code.
        if (state.EscapingException != null) ExceptionDispatchInfo.Capture(state.EscapingException).Throw();
    }

    interface IRunMode
    {
        static abstract bool HasTryBlocks { get; }
    }

    struct WithoutTryBlocks : IRunMode
    {
        public static bool HasTryBlocks => false;
    }

    struct WithTryBlocks : IRunMode
    {
        public static bool HasTryBlocks => true;
    }

    struct RunState
    {
        public uint InstructionPc;
        public bool PendingStop;
        public uint PendingStopPc;
        public int PendingFinallyBlockIndex;
        public Exception? EscapingException;
    }

    // Keeps pc in a local so it can live in a register; it is written back to pcRef when the program stops. Cold
    // instructions that need pc by reference get a temporary copy, so the local itself is never address exposed.
    static void Loop<TMode>(ref InterpreterCtx ctx, ref uint pcRef, ref RunState state) where TMode : struct, IRunMode
    {
        var pc = pcRef;
        while (true)
        {
            var instructionPc = pc;
            if (TMode.HasTryBlocks) state.InstructionPc = instructionPc;
            var opCode = Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
            pc++;
            switch ((OpCode)opCode)
            {
            case OpCode.Invalid:
                throw new InvalidOperationException($"Invalid interpreter opcode at {instructionPc}.");
            case OpCode.Stop:
                if (TMode.HasTryBlocks)
                {
                    // A finally block entered because of an exception ends here, for example the one closing a stack
                    // allocation.
                    if (ctx.CurrentException != null && IsInsideFinally(ctx.TryCatchFinallyBlocks, instructionPc))
                    {
                        state.EscapingException = ctx.CurrentException;
                        pcRef = pc;
                        return;
                    }

                    if (!state.PendingStop)
                    {
                        var finallyPc = pc;
                        if (EnterNextFinallyForStop(ref ctx, ref finallyPc, instructionPc,
                                ref state.PendingFinallyBlockIndex))
                        {
                            state.PendingStop = true;
                            state.PendingStopPc = instructionPc;
                            pc = finallyPc;
                            continue;
                        }
                    }
                }

                pcRef = pc;
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
            case OpCode.EqualDateTime:
                ctx.BoolResult = Unsafe.As<byte, DateTime>(ref ctx.Param1) ==
                                 Unsafe.As<byte, DateTime>(ref ctx.Param2);
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
                var nestedPc = pc;
                StackAllocObject(ref ctx, ref nestedPc, count);
                pc = nestedPc;
                break;
            case OpCode.StackBytesAlloc:
                var bytesSize = ReadVUInt64(ref ctx, ref pc);
                nestedPc = pc;
                StackBytesAlloc(ref ctx, ref nestedPc, bytesSize);
                pc = nestedPc;
                break;
            case OpCode.StackStructAlloc:
                unsafe
                {
                    var allocator =
                        (delegate*<ref byte, ref nint, delegate*<ref byte, void>, void>)(nint)ReadVUInt64(
                            ref ctx, ref pc);
                    nestedPc = pc;
                    StackStructAlloc(ref ctx, ref nestedPc, allocator);
                    pc = nestedPc;
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
                var codeOp = (CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
                pc++;
                RunCodeOp(codeOp, ref ctx.Param1, ref ctx.Param2, ref ctx.Result);
                break;
            case OpCode.ThrowException:
                ThrowExceptionFromParam1(ref ctx);
                break;
            case OpCode.Rethrow:
                nestedPc = pc;
                Rethrow(ref ctx, ref nestedPc, instructionPc);
                pc = nestedPc;
                break;
            case OpCode.EndOfFinally:
                if (ctx.CurrentException != null)
                {
                    nestedPc = pc;
                    PropagateCurrentException(ref ctx, ref nestedPc, instructionPc);
                    pc = nestedPc;
                }
                else if (TMode.HasTryBlocks && state.PendingStop)
                {
                    nestedPc = pc;
                    if (EnterNextFinallyForStop(ref ctx, ref nestedPc, state.PendingStopPc,
                            ref state.PendingFinallyBlockIndex))
                    {
                        pc = nestedPc;
                        continue;
                    }

                    pcRef = pc;
                    return;
                }

                break;
            case OpCode.CatchedException:
                nestedPc = pc;
                CatchedException(ref ctx, ref nestedPc, instructionPc);
                pc = nestedPc;
                break;
            case OpCode.JmpFinally:
                nestedPc = pc;
                pc = ReadFinallyTarget(ref ctx, ref nestedPc);
                break;
            case OpCode.ReadKeyToFrame:
                ReadField(ref ctx.KeyReader, (CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc),
                    ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)ReadUInt16(ref ctx, pc + 1)));
                pc += 3;
                break;
            case OpCode.ReadKeyToObject:
                RunCodeOp((CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc),
                    ref Unsafe.As<MemReader, byte>(ref ctx.KeyReader), ref Unsafe.NullRef<byte>(),
                    ref ObjectSlot(ref ctx, ReadUInt16(ref ctx, pc + 1)));
                pc += 3;
                break;
            case OpCode.ReadValueToFrame:
                ReadField(ref ctx.ValueReader, (CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc),
                    ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)ReadUInt16(ref ctx, pc + 1)));
                pc += 3;
                break;
            case OpCode.ReadValueToObject:
                RunCodeOp((CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc),
                    ref Unsafe.As<MemReader, byte>(ref ctx.ValueReader), ref Unsafe.NullRef<byte>(),
                    ref ObjectSlot(ref ctx, ReadUInt16(ref ctx, pc + 1)));
                pc += 3;
                break;
            case OpCode.SkipKey:
                SkipFields(ref ctx.KeyReader, (CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc),
                    ReadUInt16(ref ctx, pc + 1));
                pc += 3;
                break;
            case OpCode.SkipValue:
                SkipFields(ref ctx.ValueReader, (CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc),
                    ReadUInt16(ref ctx, pc + 1));
                pc += 3;
                break;
            case OpCode.JmpIfFrameConst:
                pc = CompareFrameConst(ref ctx, pc) ? ReadUInt32(ref ctx, pc + 8) : pc + 12;
                break;
            case OpCode.JmpIfNotFrameConst:
                pc = CompareFrameConst(ref ctx, pc) ? pc + 12 : ReadUInt32(ref ctx, pc + 8);
                break;
            case OpCode.JmpIfStringConst:
                pc = CompareStringConst(ref ctx, pc) ? ReadUInt32(ref ctx, pc + 7) : pc + 11;
                break;
            case OpCode.JmpIfNotStringConst:
                pc = CompareStringConst(ref ctx, pc) ? pc + 11 : ReadUInt32(ref ctx, pc + 7);
                break;
            case OpCode.ConvertSlot:
                ConvertSlot(ref ctx, pc);
                pc += 10;
                break;
            case OpCode.JmpIfFrameFrame:
                pc = CompareFrameFrame(ref ctx, pc) ? ReadUInt32(ref ctx, pc + 6) : pc + 10;
                break;
            case OpCode.JmpIfNotFrameFrame:
                pc = CompareFrameFrame(ref ctx, pc) ? pc + 10 : ReadUInt32(ref ctx, pc + 6);
                break;
            case OpCode.Arithmetic:
                Arithmetic(ref ctx, pc);
                pc += 14;
                break;
            case OpCode.ConvertNumber:
                ConvertNumber(ref ctx, pc);
                pc += 10;
                break;
            case OpCode.CaptureKey:
                Capture(ref ctx.KeyReader, (CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc),
                    ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)ReadUInt16(ref ctx, pc + 1)));
                pc += 3;
                break;
            case OpCode.CaptureValue:
                Capture(ref ctx.ValueReader, (CodeOp)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc),
                    ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)ReadUInt16(ref ctx, pc + 1)));
                pc += 3;
                break;
            case OpCode.JmpIfEncodedEqual:
                pc = EncodedEqual(ref ctx, pc) ? ReadUInt32(ref ctx, pc + 10) : pc + 14;
                break;
            case OpCode.JmpIfNotEncodedEqual:
                pc = EncodedEqual(ref ctx, pc) ? pc + 14 : ReadUInt32(ref ctx, pc + 10);
                break;
            case OpCode.JmpIfEncodedStartsWith:
                pc = EncodedStartsWith(ref ctx, pc) ? ReadUInt32(ref ctx, pc + 11) : pc + 15;
                break;
            case OpCode.JmpIfNotEncodedStartsWith:
                pc = EncodedStartsWith(ref ctx, pc) ? pc + 15 : ReadUInt32(ref ctx, pc + 11);
                break;
            case OpCode.DecodeString:
                DecodeString(ref ctx, pc);
                pc += 7;
                break;
            case OpCode.JmpIfStringOp:
                pc = StringOp(ref ctx, pc) ? ReadUInt32(ref ctx, pc + 10) : pc + 14;
                break;
            case OpCode.JmpIfNotStringOp:
                pc = StringOp(ref ctx, pc) ? pc + 14 : ReadUInt32(ref ctx, pc + 10);
                break;
            case OpCode.StringLength:
                StringLength(ref ctx, pc);
                pc += 4;
                break;
            case OpCode.JmpIfFrameInSet:
                pc = FrameInSet(ref ctx, pc) ? ReadUInt32(ref ctx, pc + 7) : pc + 11;
                break;
            case OpCode.JmpIfNotFrameInSet:
                pc = FrameInSet(ref ctx, pc) ? pc + 11 : ReadUInt32(ref ctx, pc + 7);
                break;
            case OpCode.SetFrameByte:
                Unsafe.AddByteOffset(ref ctx.Frame, (nint)ReadUInt16(ref ctx, pc)) =
                    Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc + 2);
                pc += 3;
                break;
            case OpCode.ThrowQueryException:
                ThrowQueryException((QueryException)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc));
                break;
            case OpCode.SkipKeyByHandler:
                ((IFieldHandler)ctx.ObjectConstants[(int)ReadUInt32(ref ctx, pc)]!).Skip(ref ctx.KeyReader, null);
                pc += 4;
                break;
            case OpCode.SkipValueByHandler:
                ((IFieldHandler)ctx.ObjectConstants[(int)ReadUInt32(ref ctx, pc)]!).Skip(ref ctx.ValueReader, null);
                pc += 4;
                break;
            case OpCode.StringFromCaptured:
                StringFromCaptured(ref ctx, pc);
                pc += 5;
                break;
            case OpCode.SetObjectNull:
                Unsafe.As<byte, object?>(ref ObjectSlot(ref ctx, ReadUInt16(ref ctx, pc))) = null;
                pc += 2;
                break;
            case OpCode.CopyToFrame:
                Unsafe.CopyBlockUnaligned(ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)ReadUInt16(ref ctx, pc + 1)),
                    ref Operand(ref ctx, (QueryOperandKind)Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc + 3),
                        ReadUInt32(ref ctx, pc + 4)), Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc));
                pc += 8;
                break;
            default:
                throw new InvalidOperationException("Unknown interpreter opcode.");
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ushort ReadUInt16(ref InterpreterCtx ctx, uint offset)
    {
        return Unsafe.ReadUnaligned<ushort>(ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)offset));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static uint ReadUInt32(ref InterpreterCtx ctx, uint offset)
    {
        return Unsafe.ReadUnaligned<uint>(ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)offset));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static ref byte ObjectSlot(ref InterpreterCtx ctx, ushort index)
    {
        return ref Unsafe.As<object?, byte>(ref Unsafe.Add(ref ctx.Objects, index));
    }

    static ref byte Slot(ref InterpreterCtx ctx, QuerySlotKind kind, ushort index)
    {
        if (kind == QuerySlotKind.Object) return ref ObjectSlot(ref ctx, index);
        return ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)index);
    }

    static void SkipFields(ref MemReader reader, CodeOp codeOp, ushort count)
    {
        for (var i = 0; i < count; i++) SkipField(ref reader, codeOp);
    }

    // The most common field encodings are decoded without the RunCodeOp switch.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void ReadField(ref MemReader reader, CodeOp codeOp, ref byte target)
    {
        if (codeOp == CodeOp.ReadVInt64) Unsafe.As<byte, long>(ref target) = reader.ReadVInt64();
        else if (codeOp == CodeOp.ReadVUInt64) Unsafe.As<byte, ulong>(ref target) = reader.ReadVUInt64();
        else RunCodeOp(codeOp, ref Unsafe.As<MemReader, byte>(ref reader), ref Unsafe.NullRef<byte>(), ref target);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void SkipField(ref MemReader reader, CodeOp codeOp)
    {
        if (codeOp == CodeOp.SkipString) reader.SkipString();
        else if (codeOp == CodeOp.SkipVUInt64) reader.SkipVUInt64();
        else if (codeOp == CodeOp.SkipVInt64) reader.SkipVInt64();
        else RunCodeOp(codeOp, ref Unsafe.As<MemReader, byte>(ref reader), ref Unsafe.NullRef<byte>(),
            ref Unsafe.NullRef<byte>());
    }

    static void ConvertSlot(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        ref var from = ref Slot(ref ctx, (QuerySlotKind)operands,
            Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 1)));
        ref var to = ref Slot(ref ctx, (QuerySlotKind)Unsafe.Add(ref operands, 3),
            Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 4)));
        ((Converter)ctx.ObjectConstants[(int)Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref operands, 6))]!)(
            ref from, ref to);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static bool CompareFrameConst(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        ref var value = ref Unsafe.AddByteOffset(ref ctx.Frame,
            (nint)Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 2)));
        ref var constant = ref Unsafe.AddByteOffset(ref ctx.BeginConstants,
            (nint)Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref operands, 4)));
        return CompareValues((QueryValueType)operands, (QueryCompare)Unsafe.Add(ref operands, 1), ref value,
            ref constant);
    }

    static bool CompareValues(QueryValueType type, QueryCompare compare, ref byte left, ref byte right)
    {
        return type switch
        {
            QueryValueType.UInt8 => Compare<byte>(compare, ref left, ref right),
            QueryValueType.Int8 => Compare<sbyte>(compare, ref left, ref right),
            QueryValueType.UInt16 => Compare<ushort>(compare, ref left, ref right),
            QueryValueType.Int16 => Compare<short>(compare, ref left, ref right),
            QueryValueType.UInt32 => Compare<uint>(compare, ref left, ref right),
            QueryValueType.Int32 => Compare<int>(compare, ref left, ref right),
            QueryValueType.UInt64 => Compare<ulong>(compare, ref left, ref right),
            QueryValueType.Int64 => Compare<long>(compare, ref left, ref right),
            QueryValueType.Half => Compare<Half>(compare, ref left, ref right),
            QueryValueType.Single => Compare<float>(compare, ref left, ref right),
            QueryValueType.Double => Compare<double>(compare, ref left, ref right),
            QueryValueType.DateTime => CompareDateTime(compare, Unsafe.ReadUnaligned<DateTime>(ref left),
                Unsafe.ReadUnaligned<DateTime>(ref right)),
            QueryValueType.Guid => CompareComparable<Guid>(compare, ref left, ref right),
            QueryValueType.Decimal => Compare<decimal>(compare, ref left, ref right),
            QueryValueType.TimeSpan => CompareComparable<TimeSpan>(compare, ref left, ref right),
            QueryValueType.DateTimeOffset => CompareDateTimeOffset(compare,
                Unsafe.ReadUnaligned<DateTimeOffset>(ref left), Unsafe.ReadUnaligned<DateTimeOffset>(ref right)),
            _ => throw new InvalidOperationException($"Unknown query value type {type}.")
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static bool Compare<T>(QueryCompare compare, ref byte left, ref byte right)
        where T : unmanaged, IComparisonOperators<T, T, bool>
    {
        var l = Unsafe.ReadUnaligned<T>(ref left);
        var r = Unsafe.ReadUnaligned<T>(ref right);
        return compare switch
        {
            QueryCompare.Equal => l == r,
            QueryCompare.NotEqual => l != r,
            QueryCompare.Less => l < r,
            QueryCompare.LessOrEqual => l <= r,
            QueryCompare.Greater => l > r,
            QueryCompare.GreaterOrEqual => l >= r,
            _ => throw new InvalidOperationException($"Unknown query comparison {compare}.")
        };
    }

    // DateTime operators compare ticks and ignore Kind, like C#.
    static bool CompareDateTime(QueryCompare compare, DateTime l, DateTime r)
    {
        return compare switch
        {
            QueryCompare.Equal => l == r,
            QueryCompare.NotEqual => l != r,
            QueryCompare.Less => l < r,
            QueryCompare.LessOrEqual => l <= r,
            QueryCompare.Greater => l > r,
            QueryCompare.GreaterOrEqual => l >= r,
            _ => throw new InvalidOperationException($"Unknown query comparison {compare}.")
        };
    }

    static bool CompareComparable<T>(QueryCompare compare, ref byte left, ref byte right)
        where T : unmanaged, IComparable<T>
    {
        return CompareOrder(compare, Unsafe.ReadUnaligned<T>(ref left).CompareTo(Unsafe.ReadUnaligned<T>(ref right)));
    }

    // DateTimeOffset operators compare UTC instants and ignore the offset, like C#.
    static bool CompareDateTimeOffset(QueryCompare compare, DateTimeOffset l, DateTimeOffset r)
    {
        return compare switch
        {
            QueryCompare.Equal => l == r,
            QueryCompare.NotEqual => l != r,
            QueryCompare.Less => l < r,
            QueryCompare.LessOrEqual => l <= r,
            QueryCompare.Greater => l > r,
            QueryCompare.GreaterOrEqual => l >= r,
            _ => throw new InvalidOperationException($"Unknown query comparison {compare}.")
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static bool CompareFrameFrame(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        return CompareValues((QueryValueType)operands, (QueryCompare)Unsafe.Add(ref operands, 1),
            ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 2))),
            ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 4))));
    }

    static ref byte Operand(ref InterpreterCtx ctx, QueryOperandKind kind, uint offset)
    {
        if (kind == QueryOperandKind.Constant) return ref Unsafe.AddByteOffset(ref ctx.BeginConstants, (nint)offset);
        return ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)offset);
    }

    static void Arithmetic(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        var type = (QueryValueType)operands;
        var operation = (QueryArithmetic)Unsafe.Add(ref operands, 1);
        ref var destination = ref Unsafe.AddByteOffset(ref ctx.Frame,
            (nint)Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 2)));
        ref var left = ref Operand(ref ctx, (QueryOperandKind)Unsafe.Add(ref operands, 4),
            Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref operands, 5)));
        ref var right = ref Operand(ref ctx, (QueryOperandKind)Unsafe.Add(ref operands, 9),
            Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref operands, 10)));
        switch (type)
        {
            case QueryValueType.UInt8: Arithmetic<byte>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.Int8: Arithmetic<sbyte>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.UInt16: Arithmetic<ushort>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.Int16: Arithmetic<short>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.UInt32: Arithmetic<uint>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.Int32: Arithmetic<int>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.UInt64: Arithmetic<ulong>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.Int64: Arithmetic<long>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.Half: Arithmetic<Half>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.Single: Arithmetic<float>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.Double: Arithmetic<double>(operation, ref destination, ref left, ref right); break;
            case QueryValueType.Decimal: Arithmetic<decimal>(operation, ref destination, ref left, ref right); break;
            default: throw new InvalidOperationException($"Query arithmetic does not support {type}.");
        }
    }

    static void Arithmetic<T>(QueryArithmetic operation, ref byte destination, ref byte left, ref byte right)
        where T : unmanaged, INumber<T>
    {
        var l = Unsafe.ReadUnaligned<T>(ref left);
        if (operation is QueryArithmetic.Negate or QueryArithmetic.NegateChecked)
        {
            Unsafe.WriteUnaligned(ref destination, operation == QueryArithmetic.Negate ? -l : checked(-l));
            return;
        }

        var r = Unsafe.ReadUnaligned<T>(ref right);
        Unsafe.WriteUnaligned(ref destination, operation switch
        {
            QueryArithmetic.Add => l + r,
            QueryArithmetic.Subtract => l - r,
            QueryArithmetic.Multiply => l * r,
            QueryArithmetic.Divide => l / r,
            QueryArithmetic.Modulo => l % r,
            QueryArithmetic.AddChecked => checked(l + r),
            QueryArithmetic.SubtractChecked => checked(l - r),
            QueryArithmetic.MultiplyChecked => checked(l * r),
            _ => throw new InvalidOperationException($"Unknown query arithmetic {operation}.")
        });
    }

    static void ConvertNumber(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        var from = (QueryValueType)operands;
        var to = (QueryValueType)Unsafe.Add(ref operands, 1);
        // Like C#, conversions from and to decimal are always checked.
        var isChecked = Unsafe.Add(ref operands, 2) != 0 || from == QueryValueType.Decimal ||
                        to == QueryValueType.Decimal;
        ref var destination = ref Unsafe.AddByteOffset(ref ctx.Frame,
            (nint)Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 3)));
        ref var source = ref Operand(ref ctx, (QueryOperandKind)Unsafe.Add(ref operands, 5),
            Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref operands, 6)));
        switch (from)
        {
            case QueryValueType.UInt8: ConvertNumberFrom<byte>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.Int8: ConvertNumberFrom<sbyte>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.UInt16: ConvertNumberFrom<ushort>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.Int16: ConvertNumberFrom<short>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.UInt32: ConvertNumberFrom<uint>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.Int32: ConvertNumberFrom<int>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.UInt64: ConvertNumberFrom<ulong>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.Int64: ConvertNumberFrom<long>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.Half: ConvertNumberFrom<Half>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.Single: ConvertNumberFrom<float>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.Double: ConvertNumberFrom<double>(to, isChecked, ref source, ref destination); break;
            case QueryValueType.Decimal: ConvertNumberFrom<decimal>(to, isChecked, ref source, ref destination); break;
            default: throw new InvalidOperationException($"Query conversion does not support {from}.");
        }
    }

    static void ConvertNumberFrom<TFrom>(QueryValueType to, bool isChecked, ref byte source, ref byte destination)
        where TFrom : unmanaged, INumberBase<TFrom>
    {
        var value = Unsafe.ReadUnaligned<TFrom>(ref source);
        switch (to)
        {
            case QueryValueType.UInt8: Store<byte>(value, isChecked, ref destination); break;
            case QueryValueType.Int8: Store<sbyte>(value, isChecked, ref destination); break;
            case QueryValueType.UInt16: Store<ushort>(value, isChecked, ref destination); break;
            case QueryValueType.Int16: Store<short>(value, isChecked, ref destination); break;
            case QueryValueType.UInt32: Store<uint>(value, isChecked, ref destination); break;
            case QueryValueType.Int32: Store<int>(value, isChecked, ref destination); break;
            case QueryValueType.UInt64: Store<ulong>(value, isChecked, ref destination); break;
            case QueryValueType.Int64: Store<long>(value, isChecked, ref destination); break;
            case QueryValueType.Half: Store<Half>(value, isChecked, ref destination); break;
            case QueryValueType.Single: Store<float>(value, isChecked, ref destination); break;
            case QueryValueType.Double: Store<double>(value, isChecked, ref destination); break;
            case QueryValueType.Decimal: Store<decimal>(value, isChecked, ref destination); break;
            default: throw new InvalidOperationException($"Query conversion does not support {to}.");
        }

        static void Store<TTo>(TFrom value, bool isChecked, ref byte destination) where TTo : unmanaged, INumberBase<TTo>
        {
            Unsafe.WriteUnaligned(ref destination,
                isChecked ? TTo.CreateChecked(value) : TTo.CreateTruncating(value));
        }
    }

    static void Capture(ref MemReader reader, CodeOp skipCodeOp, ref byte slot)
    {
        var start = reader.Current;
        SkipField(ref reader, skipCodeOp);
        Unsafe.WriteUnaligned(ref slot, start);
        Unsafe.WriteUnaligned(ref Unsafe.Add(ref slot, 8), (int)(reader.Current - start));
    }

    static unsafe ReadOnlySpan<byte> Captured(ref InterpreterCtx ctx, ushort offset)
    {
        ref var slot = ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)offset);
        return new((void*)Unsafe.ReadUnaligned<nint>(ref slot), Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref slot, 8)));
    }

    static ReadOnlySpan<byte> ConstantBytes(ref InterpreterCtx ctx, ref byte operands)
    {
        return MemoryMarshal.CreateReadOnlySpan(
            ref Unsafe.AddByteOffset(ref ctx.BeginConstants, (nint)Unsafe.ReadUnaligned<uint>(ref operands)),
            (int)Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref operands, 4)));
    }

    static bool EncodedEqual(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        return Captured(ref ctx, Unsafe.ReadUnaligned<ushort>(ref operands))
            .SequenceEqual(ConstantBytes(ref ctx, ref Unsafe.Add(ref operands, 2)));
    }

    static unsafe bool EncodedStartsWith(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        var captured = Captured(ref ctx, Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 1)));
        fixed (byte* pointer = captured)
        {
            var reader = new MemReader(pointer, captured.Length);
            if ((QueryStringEncoding)operands == QueryStringEncoding.String)
            {
                if (reader.ReadVUInt64() == 0) throw new NullReferenceException();
                captured = captured[(int)reader.GetCurrentPositionWithoutController()..];
            }
            else if (reader.ReadVUInt32() == OrderedNullMarker)
            {
                throw new NullReferenceException();
            }
        }

        return captured.StartsWith(ConstantBytes(ref ctx, ref Unsafe.Add(ref operands, 3)));
    }

    // MemWriter.WriteStringOrdered writes null as this VUInt32.
    const uint OrderedNullMarker = 0x110001;

    static unsafe void DecodeString(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        var captured = Captured(ref ctx, Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 1)));
        ref var buffer = ref Unsafe.As<byte, char[]?>(ref ObjectSlot(ref ctx,
            Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 3))));
        int length;
        fixed (byte* pointer = captured)
        {
            var reader = new MemReader(pointer, captured.Length);
            length = (QueryStringEncoding)operands == QueryStringEncoding.String
                ? DecodeStringChars(ref reader, ref buffer)
                : DecodeOrderedStringChars(ref reader, ref buffer);
        }

        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref ctx.Frame,
            (nint)Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 5))), length);
    }

    // Mirrors MemReader.ReadString without allocating a string; the buffer is reused across rows.
    static int DecodeStringChars(ref MemReader reader, ref char[]? buffer)
    {
        var header = reader.ReadVUInt64();
        if (header == 0) return -1;
        if (header - 1 > int.MaxValue) throw new InvalidDataException($"Reading String length overflowed with {header}");
        var length = (int)(header - 1);
        if (buffer == null || buffer.Length < length) buffer = new char[Math.Max(length, (buffer?.Length ?? 8) * 2)];
        var i = 0;
        while (i < length)
        {
            var c = reader.ReadVUInt64();
            if (c > 0xffff)
            {
                if (c > 0x10ffff) throw new InvalidDataException($"Reading String unicode value overflowed with {c}");
                c -= 0x10000;
                buffer[i++] = (char)((c >> 10) + 0xD800);
                buffer[i++] = (char)((c & 0x3FF) + 0xDC00);
            }
            else
            {
                buffer[i++] = (char)c;
            }
        }

        return length;
    }

    // Mirrors MemReader.ReadStringOrdered without allocating: each char is a VUInt32 of its code point + 1, a zero
    // ends the string and null is a single OrderedNullMarker.
    static int DecodeOrderedStringChars(ref MemReader reader, ref char[]? buffer)
    {
        buffer ??= new char[16];
        var length = 0;
        while (true)
        {
            var c = reader.ReadVUInt32();
            if (c == 0) return length;
            if (c == OrderedNullMarker && length == 0) return -1;
            c--;
            if (buffer.Length < length + 2) Array.Resize(ref buffer, buffer.Length * 2);
            if (c > 0xffff)
            {
                if (c > 0x10ffff) throw new InvalidDataException($"Reading String unicode value overflowed with {c}");
                c -= 0x10000;
                buffer[length++] = (char)((c >> 10) + 0xD800);
                buffer[length++] = (char)((c & 0x3FF) + 0xDC00);
            }
            else
            {
                buffer[length++] = (char)c;
            }
        }
    }

    static unsafe void StringFromCaptured(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        var captured = Captured(ref ctx, Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 1)));
        fixed (byte* pointer = captured)
        {
            var reader = new MemReader(pointer, captured.Length);
            Unsafe.As<byte, object?>(ref ObjectSlot(ref ctx, Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 3)))) =
                (QueryStringEncoding)operands == QueryStringEncoding.String
                    ? reader.ReadString()
                    : reader.ReadStringOrdered();
        }
    }

    static bool StringOp(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        var operation = (QueryStringOp)operands;
        var argument = Unsafe.Add(ref operands, 1);
        var buffer = Unsafe.As<byte, char[]?>(ref ObjectSlot(ref ctx,
            Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 2))));
        var length = Unsafe.ReadUnaligned<int>(ref Unsafe.AddByteOffset(ref ctx.Frame,
            (nint)Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 4))));
        var constant = ctx.ObjectConstants[(int)Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref operands, 6))];
        var isNull = length < 0;
        var value = isNull ? default : new ReadOnlySpan<char>(buffer, 0, length);
        var comparison = (StringComparison)argument;
        switch (operation)
        {
            case QueryStringOp.Equals:
                if (isNull || constant == null) return isNull && constant == null;
                return value.Equals((string)constant, comparison);
            case QueryStringOp.InstanceEquals:
                if (isNull) throw new NullReferenceException();
                return constant != null && value.Equals((string)constant, comparison);
            case QueryStringOp.StartsWith:
                if (isNull) throw new NullReferenceException();
                if (constant is char startChar) return value.Length != 0 && value[0] == startChar;
                return value.StartsWith((string?)constant ?? throw new ArgumentNullException("value"), comparison);
            case QueryStringOp.EndsWith:
                if (isNull) throw new NullReferenceException();
                if (constant is char endChar) return value.Length != 0 && value[^1] == endChar;
                return value.EndsWith((string?)constant ?? throw new ArgumentNullException("value"), comparison);
            case QueryStringOp.Contains:
                if (isNull) throw new NullReferenceException();
                // Span Contains(char) is always ordinal, so the char is passed as a span to honor the comparison.
                if (constant is char containedChar)
                    return value.Contains(new ReadOnlySpan<char>(in containedChar), comparison);
                return value.Contains((string?)constant ?? throw new ArgumentNullException("value"), comparison);
            case QueryStringOp.CompareOrdinal:
                var order = isNull
                    ? constant == null ? 0 : -1
                    : constant == null ? 1 : value.SequenceCompareTo((string)constant);
                return CompareOrder((QueryCompare)argument, order);
            case QueryStringOp.InSet:
                var set = (QueryStringSet)constant!;
                return isNull ? set.ContainsNull : set.Lookup.Contains(value);
            default:
                throw new InvalidOperationException($"Unknown query string operation {operation}.");
        }
    }

    static bool CompareOrder(QueryCompare compare, int order)
    {
        return compare switch
        {
            QueryCompare.Equal => order == 0,
            QueryCompare.NotEqual => order != 0,
            QueryCompare.Less => order < 0,
            QueryCompare.LessOrEqual => order <= 0,
            QueryCompare.Greater => order > 0,
            QueryCompare.GreaterOrEqual => order >= 0,
            _ => throw new InvalidOperationException($"Unknown query comparison {compare}.")
        };
    }

    static unsafe void StringLength(ref InterpreterCtx ctx, uint pc)
    {
        var captured = Captured(ref ctx, ReadUInt16(ref ctx, pc));
        int length;
        fixed (byte* pointer = captured)
        {
            var reader = new MemReader(pointer, captured.Length);
            length = (int)reader.ReadVUInt64() - 1;
        }

        Unsafe.WriteUnaligned(ref Unsafe.AddByteOffset(ref ctx.Frame, (nint)ReadUInt16(ref ctx, pc + 2)), length);
    }

    static bool FrameInSet(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        ref var value = ref Unsafe.AddByteOffset(ref ctx.Frame,
            (nint)Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 1)));
        var set = ctx.ObjectConstants[(int)Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref operands, 3))]!;
        return (QueryValueType)operands switch
        {
            // bool values are stored in frames as UInt8, but their sets stay sets of bool
            QueryValueType.UInt8 => set is IReadOnlySet<bool> boolSet
                ? boolSet.Contains(value != 0)
                : InSet<byte>(set, ref value),
            QueryValueType.Int8 => InSet<sbyte>(set, ref value),
            QueryValueType.UInt16 => InSet<ushort>(set, ref value),
            QueryValueType.Int16 => InSet<short>(set, ref value),
            QueryValueType.UInt32 => InSet<uint>(set, ref value),
            QueryValueType.Int32 => InSet<int>(set, ref value),
            QueryValueType.UInt64 => InSet<ulong>(set, ref value),
            QueryValueType.Int64 => InSet<long>(set, ref value),
            QueryValueType.Half => InSet<Half>(set, ref value),
            QueryValueType.Single => InSet<float>(set, ref value),
            QueryValueType.Double => InSet<double>(set, ref value),
            QueryValueType.DateTime => InSet<DateTime>(set, ref value),
            QueryValueType.Guid => InSet<Guid>(set, ref value),
            QueryValueType.Decimal => InSet<decimal>(set, ref value),
            QueryValueType.TimeSpan => InSet<TimeSpan>(set, ref value),
            QueryValueType.DateTimeOffset => InSet<DateTimeOffset>(set, ref value),
            _ => throw new InvalidOperationException($"Unknown query value type {(QueryValueType)operands}.")
        };

        static bool InSet<T>(object set, ref byte value) where T : unmanaged
        {
            return ((IReadOnlySet<T>)set).Contains(Unsafe.ReadUnaligned<T>(ref value));
        }
    }

    static void ThrowQueryException(QueryException exception)
    {
        throw exception switch
        {
            QueryException.NullReference => new NullReferenceException(),
            QueryException.NullableHasNoValue => new InvalidOperationException("Nullable object must have a value."),
            _ => new InvalidOperationException($"Unknown query exception {exception}.")
        };
    }

    static bool CompareStringConst(ref InterpreterCtx ctx, uint pc)
    {
        ref var operands = ref Unsafe.AddByteOffset(ref ctx.BeginProgram, (nint)pc);
        var value = Unsafe.As<byte, string?>(ref ObjectSlot(ref ctx,
            Unsafe.ReadUnaligned<ushort>(ref Unsafe.Add(ref operands, 1))));
        var constant = (string?)ctx.ObjectConstants[(int)Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref operands, 3))];
        return (QueryCompare)operands switch
        {
            QueryCompare.Equal => string.Equals(value, constant),
            QueryCompare.NotEqual => !string.Equals(value, constant),
            QueryCompare.Less => string.CompareOrdinal(value, constant) < 0,
            QueryCompare.LessOrEqual => string.CompareOrdinal(value, constant) <= 0,
            QueryCompare.Greater => string.CompareOrdinal(value, constant) > 0,
            QueryCompare.GreaterOrEqual => string.CompareOrdinal(value, constant) >= 0,
            _ => throw new InvalidOperationException($"Unknown query comparison {(QueryCompare)operands}.")
        };
    }

    static bool IsInsideFinally(ReadOnlySpan<uint> blocks, uint instructionPc)
    {
        for (var i = blocks.Length - 4; i >= 0; i -= 4)
        {
            var finallyPc = blocks[i + 2];
            var tryEndPc = blocks[i + 3];
            if (finallyPc != tryEndPc && Contains(instructionPc, finallyPc, tryEndPc)) return true;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    // Param1 is MemReader for reading code ops and MemWriter for writing ones.
    static void RunCodeOp(CodeOp codeOp, ref byte param1, ref byte param2, ref byte result)
    {
        ref var reader = ref Unsafe.As<byte, MemReader>(ref param1);
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
                reader.SkipBlock(Unsafe.As<byte, uint>(ref param2));
                break;
            case CodeOp.ReadUInt8:
                Unsafe.As<byte, byte>(ref result) = reader.ReadUInt8();
                break;
            case CodeOp.ReadInt8:
                Unsafe.As<byte, sbyte>(ref result) = reader.ReadInt8();
                break;
            case CodeOp.ReadInt8Ordered:
                Unsafe.As<byte, sbyte>(ref result) = reader.ReadInt8Ordered();
                break;
            case CodeOp.ReadBool:
                Unsafe.As<byte, bool>(ref result) = reader.ReadBool();
                break;
            case CodeOp.ReadUInt16LE:
                Unsafe.As<byte, ushort>(ref result) = reader.ReadUInt16LE();
                break;
            case CodeOp.ReadUInt16BE:
                Unsafe.As<byte, ushort>(ref result) = reader.ReadUInt16BE();
                break;
            case CodeOp.ReadInt16LE:
                Unsafe.As<byte, short>(ref result) = reader.ReadInt16LE();
                break;
            case CodeOp.ReadInt16BE:
                Unsafe.As<byte, short>(ref result) = reader.ReadInt16BE();
                break;
            case CodeOp.ReadHalf:
                Unsafe.As<byte, Half>(ref result) = reader.ReadHalf();
                break;
            case CodeOp.ReadUInt32LE:
                Unsafe.As<byte, uint>(ref result) = reader.ReadUInt32LE();
                break;
            case CodeOp.ReadUInt32BE:
                Unsafe.As<byte, uint>(ref result) = reader.ReadUInt32BE();
                break;
            case CodeOp.ReadInt32LE:
                Unsafe.As<byte, int>(ref result) = reader.ReadInt32LE();
                break;
            case CodeOp.ReadInt32BE:
                Unsafe.As<byte, int>(ref result) = reader.ReadInt32BE();
                break;
            case CodeOp.ReadSingle:
                Unsafe.As<byte, float>(ref result) = reader.ReadSingle();
                break;
            case CodeOp.ReadUInt64LE:
                Unsafe.As<byte, ulong>(ref result) = reader.ReadUInt64LE();
                break;
            case CodeOp.ReadUInt64BE:
                Unsafe.As<byte, ulong>(ref result) = reader.ReadUInt64BE();
                break;
            case CodeOp.ReadInt64LE:
                Unsafe.As<byte, long>(ref result) = reader.ReadInt64LE();
                break;
            case CodeOp.ReadInt64BE:
                Unsafe.As<byte, long>(ref result) = reader.ReadInt64BE();
                break;
            case CodeOp.ReadDouble:
                Unsafe.As<byte, double>(ref result) = reader.ReadDouble();
                break;
            case CodeOp.ReadDoubleOrdered:
                Unsafe.As<byte, double>(ref result) = reader.ReadDoubleOrdered();
                break;
            case CodeOp.ReadVInt16:
                Unsafe.As<byte, short>(ref result) = reader.ReadVInt16();
                break;
            case CodeOp.ReadVUInt16:
                Unsafe.As<byte, ushort>(ref result) = reader.ReadVUInt16();
                break;
            case CodeOp.ReadVInt32:
                Unsafe.As<byte, int>(ref result) = reader.ReadVInt32();
                break;
            case CodeOp.ReadVUInt32:
                Unsafe.As<byte, uint>(ref result) = reader.ReadVUInt32();
                break;
            case CodeOp.ReadVInt64:
                Unsafe.As<byte, long>(ref result) = reader.ReadVInt64();
                break;
            case CodeOp.ReadVUInt64:
                Unsafe.As<byte, ulong>(ref result) = reader.ReadVUInt64();
                break;
            case CodeOp.ReadDateTime:
                Unsafe.As<byte, DateTime>(ref result) = reader.ReadDateTime();
                break;
            case CodeOp.ReadDateTimeOffset:
                Unsafe.As<byte, DateTimeOffset>(ref result) = reader.ReadDateTimeOffset();
                break;
            case CodeOp.ReadTimeSpan:
                Unsafe.As<byte, TimeSpan>(ref result) = reader.ReadTimeSpan();
                break;
            case CodeOp.ReadString:
                Unsafe.As<byte, string?>(ref result) = reader.ReadString();
                break;
            case CodeOp.ReadStringOrdered:
                Unsafe.As<byte, string?>(ref result) = reader.ReadStringOrdered();
                break;
            case CodeOp.ReadStringInUtf8:
                Unsafe.As<byte, string>(ref result) = reader.ReadStringInUtf8();
                break;
            case CodeOp.ReadGuid:
                Unsafe.As<byte, Guid>(ref result) = reader.ReadGuid();
                break;
            case CodeOp.ReadDecimal:
                Unsafe.As<byte, decimal>(ref result) = reader.ReadDecimal();
                break;
            case CodeOp.ReadByteArray:
                Unsafe.As<byte, byte[]?>(ref result) = reader.ReadByteArray();
                break;
            case CodeOp.ReadByteArrayAsMemory:
                Unsafe.As<byte, ReadOnlyMemory<byte>>(ref result) = reader.ReadByteArrayAsMemory();
                break;
            case CodeOp.ReadIPAddress:
                Unsafe.As<byte, IPAddress?>(ref result) = reader.ReadIPAddress();
                break;
            case CodeOp.ReadVersion:
                Unsafe.As<byte, Version?>(ref result) = reader.ReadVersion();
                break;
            case CodeOp.ReadStringValues:
                Unsafe.As<byte, StringValues>(ref result) = reader.ReadStringValues();
                break;
            case CodeOp.ReadPointer:
                Unsafe.As<byte, nint>(ref result) = reader.ReadPointer();
                break;
            case CodeOp.ReadBlock:
                reader.ReadBlock(ref result, Unsafe.As<byte, uint>(ref param2));
                break;
            case CodeOp.ReadByteArrayRaw:
                Unsafe.As<byte, byte[]>(ref result) = reader.ReadByteArrayRaw((int)Unsafe.As<byte, uint>(ref param2));
                break;
            case CodeOp.ReadByteArrayRawTillEof:
                Unsafe.As<byte, byte[]>(ref result) = reader.ReadByteArrayRawTillEof();
                break;
            case CodeOp.ReadByteArrayRawTillEofAsMemory:
                Unsafe.As<byte, ReadOnlyMemory<byte>>(ref result) = reader.ReadByteArrayRawTillEofAsMemory();
                break;
            case CodeOp.WriteByteZero:
                Unsafe.As<byte, MemWriter>(ref param1).WriteByteZero();
                break;
            case CodeOp.WriteBool:
                Unsafe.As<byte, MemWriter>(ref param1).WriteBool(Unsafe.As<byte, bool>(ref param2));
                break;
            case CodeOp.WriteUInt8:
                Unsafe.As<byte, MemWriter>(ref param1).WriteUInt8(Unsafe.As<byte, byte>(ref param2));
                break;
            case CodeOp.WriteInt8:
                Unsafe.As<byte, MemWriter>(ref param1).WriteInt8(Unsafe.As<byte, sbyte>(ref param2));
                break;
            case CodeOp.WriteInt8Ordered:
                Unsafe.As<byte, MemWriter>(ref param1).WriteInt8Ordered(Unsafe.As<byte, sbyte>(ref param2));
                break;
            case CodeOp.WriteVInt16:
                Unsafe.As<byte, MemWriter>(ref param1).WriteVInt16(Unsafe.As<byte, short>(ref param2));
                break;
            case CodeOp.WriteVUInt16:
                Unsafe.As<byte, MemWriter>(ref param1).WriteVUInt16(Unsafe.As<byte, ushort>(ref param2));
                break;
            case CodeOp.WriteVInt32:
                Unsafe.As<byte, MemWriter>(ref param1).WriteVInt32(Unsafe.As<byte, int>(ref param2));
                break;
            case CodeOp.WriteVUInt32:
                Unsafe.As<byte, MemWriter>(ref param1).WriteVUInt32(Unsafe.As<byte, uint>(ref param2));
                break;
            case CodeOp.WriteVInt64:
                Unsafe.As<byte, MemWriter>(ref param1).WriteVInt64(Unsafe.As<byte, long>(ref param2));
                break;
            case CodeOp.WriteVUInt64:
                Unsafe.As<byte, MemWriter>(ref param1).WriteVUInt64(Unsafe.As<byte, ulong>(ref param2));
                break;
            case CodeOp.WriteInt64BE:
                Unsafe.As<byte, MemWriter>(ref param1).WriteInt64BE(Unsafe.As<byte, long>(ref param2));
                break;
            case CodeOp.WriteInt64LE:
                Unsafe.As<byte, MemWriter>(ref param1).WriteInt64LE(Unsafe.As<byte, long>(ref param2));
                break;
            case CodeOp.WriteUInt16LE:
                Unsafe.As<byte, MemWriter>(ref param1).WriteUInt16LE(Unsafe.As<byte, ushort>(ref param2));
                break;
            case CodeOp.WriteInt32BE:
                Unsafe.As<byte, MemWriter>(ref param1).WriteInt32BE(Unsafe.As<byte, int>(ref param2));
                break;
            case CodeOp.WriteInt16BE:
                Unsafe.As<byte, MemWriter>(ref param1).WriteInt16BE(Unsafe.As<byte, short>(ref param2));
                break;
            case CodeOp.WriteInt32LE:
                Unsafe.As<byte, MemWriter>(ref param1).WriteInt32LE(Unsafe.As<byte, int>(ref param2));
                break;
            case CodeOp.WriteUInt32LE:
                Unsafe.As<byte, MemWriter>(ref param1).WriteUInt32LE(Unsafe.As<byte, uint>(ref param2));
                break;
            case CodeOp.WriteUInt64LE:
                Unsafe.As<byte, MemWriter>(ref param1).WriteUInt64LE(Unsafe.As<byte, ulong>(ref param2));
                break;
            case CodeOp.WriteUInt64BE:
                Unsafe.As<byte, MemWriter>(ref param1).WriteUInt64BE(Unsafe.As<byte, ulong>(ref param2));
                break;
            case CodeOp.WriteDateTime:
                Unsafe.As<byte, MemWriter>(ref param1).WriteDateTime(Unsafe.As<byte, DateTime>(ref param2));
                break;
            case CodeOp.WriteDateTimeForbidUnspecifiedKind:
                Unsafe.As<byte, MemWriter>(ref param1)
                    .WriteDateTimeForbidUnspecifiedKind(Unsafe.As<byte, DateTime>(ref param2));
                break;
            case CodeOp.WriteDateTimeOffset:
                Unsafe.As<byte, MemWriter>(ref param1)
                    .WriteDateTimeOffset(Unsafe.As<byte, DateTimeOffset>(ref param2));
                break;
            case CodeOp.WriteTimeSpan:
                Unsafe.As<byte, MemWriter>(ref param1).WriteTimeSpan(Unsafe.As<byte, TimeSpan>(ref param2));
                break;
            case CodeOp.WriteString:
                Unsafe.As<byte, MemWriter>(ref param1).WriteString(Unsafe.As<byte, string?>(ref param2));
                break;
            case CodeOp.WriteStringOrdered:
                Unsafe.As<byte, MemWriter>(ref param1).WriteStringOrdered(Unsafe.As<byte, string?>(ref param2));
                break;
            case CodeOp.WriteStringOrderedPrefix:
                Unsafe.As<byte, MemWriter>(ref param1).WriteStringOrderedPrefix(Unsafe.As<byte, string>(ref param2));
                break;
            case CodeOp.WriteStringInUtf8:
                Unsafe.As<byte, MemWriter>(ref param1).WriteStringInUtf8(Unsafe.As<byte, string>(ref param2));
                break;
            case CodeOp.WriteGuid:
                Unsafe.As<byte, MemWriter>(ref param1).WriteGuid(Unsafe.As<byte, Guid>(ref param2));
                break;
            case CodeOp.WriteSingle:
                Unsafe.As<byte, MemWriter>(ref param1).WriteSingle(Unsafe.As<byte, float>(ref param2));
                break;
            case CodeOp.WriteDouble:
                Unsafe.As<byte, MemWriter>(ref param1).WriteDouble(Unsafe.As<byte, double>(ref param2));
                break;
            case CodeOp.WriteDoubleOrdered:
                Unsafe.As<byte, MemWriter>(ref param1).WriteDoubleOrdered(Unsafe.As<byte, double>(ref param2));
                break;
            case CodeOp.WriteHalf:
                Unsafe.As<byte, MemWriter>(ref param1).WriteHalf(Unsafe.As<byte, Half>(ref param2));
                break;
            case CodeOp.WriteDecimal:
                Unsafe.As<byte, MemWriter>(ref param1).WriteDecimal(Unsafe.As<byte, decimal>(ref param2));
                break;
            case CodeOp.WriteByteArray:
                Unsafe.As<byte, MemWriter>(ref param1).WriteByteArray(Unsafe.As<byte, byte[]?>(ref param2));
                break;
            case CodeOp.WriteByteArrayFromByteBuffer:
                Unsafe.As<byte, MemWriter>(ref param1).WriteByteArray(Unsafe.As<byte, ByteBuffer>(ref param2));
                break;
            case CodeOp.WriteByteArrayFromMemory:
                Unsafe.As<byte, MemWriter>(ref param1)
                    .WriteByteArray(Unsafe.As<byte, ReadOnlyMemory<byte>>(ref param2));
                break;
            case CodeOp.WriteByteArrayLength:
                Unsafe.As<byte, MemWriter>(ref param1)
                    .WriteByteArrayLength(Unsafe.As<byte, ReadOnlyMemory<byte>>(ref param2));
                break;
            case CodeOp.WriteByteArrayRaw:
                Unsafe.As<byte, MemWriter>(ref param1).WriteByteArrayRaw(Unsafe.As<byte, byte[]?>(ref param2));
                break;
            case CodeOp.WriteBlockFromByteArray:
                Unsafe.As<byte, MemWriter>(ref param1).WriteBlock(Unsafe.As<byte, byte[]>(ref param2));
                break;
            case CodeOp.WriteBlockFromByteBuffer:
                Unsafe.As<byte, MemWriter>(ref param1).WriteBlock(Unsafe.As<byte, ByteBuffer>(ref param2));
                break;
            case CodeOp.WriteBlockFromMemory:
                Unsafe.As<byte, MemWriter>(ref param1)
                    .WriteBlock(Unsafe.As<byte, ReadOnlyMemory<byte>>(ref param2));
                break;
            case CodeOp.WriteIPAddress:
                Unsafe.As<byte, MemWriter>(ref param1).WriteIPAddress(Unsafe.As<byte, IPAddress?>(ref param2));
                break;
            case CodeOp.WriteVersion:
                Unsafe.As<byte, MemWriter>(ref param1).WriteVersion(Unsafe.As<byte, Version?>(ref param2));
                break;
            case CodeOp.WriteStringValues:
                Unsafe.As<byte, MemWriter>(ref param1).WriteStringValues(Unsafe.As<byte, StringValues>(ref param2));
                break;
            case CodeOp.WritePointer:
                Unsafe.As<byte, MemWriter>(ref param1).WritePointer(Unsafe.As<byte, nint>(ref param2));
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
        new Span<byte>(bytes, (int)size).Clear();
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
