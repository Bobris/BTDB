using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using BTDB.Buffer;
using BTDB.Collections;
using BTDB.StreamLayer;

namespace BTDB.Bon;

public enum BonType
{
    Error, // 0
    Null, // 1
    Undefined, // 2
    Bool, // 3 - false, 4 - true
    Integer, // 5 - 0, ... 15 - 10, 16 - -1, ... 25 - -10, 128 - VUlong, 129 - VUlong (*-1)
    Float, // 64 - half, 65 - float, 66 - double
    String, // 26 - empty, 130 - VUint offset to VUint len of UTF-8 bytes and UTF-8 bytes themself
    DateTime, // 67 - (DateTime.Ticks in 8 bytes(long))
    Guid, // 68 - 16 bytes
    Array, // 27 - empty, 131 - VUint offset to VUint len + Bons*len, 136 - VUint offset to VUint len + UInt64LE offsets*((len+31)/32)
    Object, // 28 - empty, 132 - VUint offset to VUint offset to VUint len + VUint offsets to strings, Bons*len
    Class, // 133 - VUint offset to VUint offset to VUint len + VUint offset to type name string + VUint offsets to strings, Bons*len
    Dictionary, // 29 - empty, 134 - VUint offset to VUint len + (Key Bon + Value Bon)*len
    ByteArray, // 30 - empty, 135 - VUint offset to VUint len + bytes
    Tuple, // 137 - VUint offset to VUint len + Bons*len (same as Array)
}

public static class Helpers
{
    // ReSharper disable InconsistentNaming
    public const byte CodeError = 0;
    public const byte CodeNull = 1;
    public const byte CodeUndefined = 2;
    public const byte CodeFalse = 3;
    public const byte CodeTrue = 4;
    public const byte Code0 = 5;
    public const byte CodeM1 = 16;
    public const byte CodeM10 = 25;
    public const byte CodeInteger = 128;
    public const byte CodeMInteger = 129;
    public const byte CodeHalf = 64;
    public const byte CodeFloat = 65;
    public const byte CodeDouble = 66;
    public const byte CodeStringEmpty = 26;
    public const byte CodeStringPtr = 130;
    public const byte CodeDateTime = 67;
    public const byte CodeGuid = 68;
    public const byte CodeArrayEmpty = 27;
    public const byte CodeArrayPtr = 131;
    public const byte CodeObjectEmpty = 28;
    public const byte CodeObjectPtr = 132;
    public const byte CodeClassPtr = 133;
    public const byte CodeDictionaryEmpty = 29;
    public const byte CodeDictionaryPtr = 134;
    public const byte CodeByteArrayEmpty = 30;
    public const byte CodeByteArrayPtr = 135;
    public const byte CodeArraySplitBy32Ptr = 136;
    public const byte CodeTuplePtr = 137;
    // ReSharper restore InconsistentNaming

    public static BonType BonTypeFromByte(byte b)
    {
        return b switch
        {
            CodeNull => BonType.Null,
            CodeUndefined => BonType.Undefined,
            CodeFalse or CodeTrue => BonType.Bool,
            >= Code0 and <= CodeM10 or CodeInteger or CodeMInteger => BonType.Integer,
            CodeHalf or CodeFloat or CodeDouble => BonType.Float,
            CodeStringEmpty or CodeStringPtr => BonType.String,
            CodeDateTime => BonType.DateTime,
            CodeGuid => BonType.Guid,
            CodeArrayEmpty or CodeArrayPtr or CodeArraySplitBy32Ptr => BonType.Array,
            CodeObjectEmpty or CodeObjectPtr => BonType.Object,
            CodeClassPtr => BonType.Class,
            CodeDictionaryEmpty or CodeDictionaryPtr => BonType.Dictionary,
            CodeByteArrayEmpty or CodeByteArrayPtr => BonType.ByteArray,
            CodeTuplePtr => BonType.Tuple,
            _ => BonType.Error
        };
    }

    public static void Write(scoped ref MemWriter data, double value)
    {
        var f = (float)value;
        if (f == value)
        {
            var h = (Half)value;
            if ((double)h == value)
            {
                data.WriteUInt8(CodeHalf);
                data.WriteUInt16LE(Unsafe.BitCast<Half, ushort>(h));
            }
            else
            {
                data.WriteUInt8(CodeFloat);
                data.WriteUInt32LE(Unsafe.BitCast<float, uint>(f));
            }
        }
        else
        {
            data.WriteUInt8(CodeDouble);
            data.WriteUInt64LE(Unsafe.BitCast<double, ulong>(value));
        }
    }

    public static ulong CalcStartOffsetOfBon(scoped ref MemReader reader)
    {
        var len = reader.GetLength();
        reader.SetCurrentPositionWithoutController((ulong)(len - 1));
        var b = reader.ReadUInt8();
        return (ulong)len - b - 1;
    }

    public static void SkipItemOfBon(scoped ref MemReader reader)
    {
        var b = reader.ReadUInt8();
        switch (b)
        {
            case < 64:
                break;
            case >= 128:
                reader.SkipVUInt64();
                break;
            case 64:
                reader.Skip2Bytes();
                break;
            case 65:
                reader.Skip4Bytes();
                break;
            case 66 or 67:
                reader.Skip8Bytes();
                break;
            case 68:
                reader.SkipBlock(16);
                break;
        }
    }

    public static bool TryGetDouble(scoped ref MemReader reader, out double value)
    {
        var b = reader.PeekUInt8();
        switch (b)
        {
            case >= Code0 and <= CodeM10:
            {
                reader.Skip1Byte();
                value = b < CodeM1 ? b - Code0 : CodeM1 - b - 1;
                return true;
            }
            case CodeInteger:
            {
                reader.Skip1Byte();
                value = reader.ReadVUInt64();
                return true;
            }
            case CodeMInteger:
            {
                reader.Skip1Byte();
                value = -(double)reader.ReadVUInt64();
                return true;
            }
            case CodeHalf:
            {
                reader.Skip1Byte();
                value = (double)Unsafe.BitCast<ushort, Half>(reader.ReadUInt16LE());
                return true;
            }
            case CodeFloat:
            {
                reader.Skip1Byte();
                value = Unsafe.BitCast<uint, float>(reader.ReadUInt32LE());
                return true;
            }
            case CodeDouble:
            {
                reader.Skip1Byte();
                value = Unsafe.BitCast<ulong, double>(reader.ReadUInt64LE());
                return true;
            }
        }

        value = 0;
        return false;
    }

    public static bool TryGetDateTime(scoped ref MemReader reader, out DateTime value)
    {
        var b = reader.PeekUInt8();
        switch (b)
        {
            case CodeDateTime:
            {
                reader.Skip1Byte();
                value = DateTime.FromBinary(reader.ReadInt64LE());
                return true;
            }
        }

        value = new();
        return false;
    }

    public static bool TryGetGuid(scoped ref MemReader reader, out Guid value)
    {
        var b = reader.PeekUInt8();
        switch (b)
        {
            case CodeGuid:
            {
                reader.Skip1Byte();
                value = reader.ReadGuid();
                return true;
            }
        }

        value = new();
        return false;
    }

    public static bool TryGetString(scoped ref MemReader reader, out string value)
    {
        var b = reader.PeekUInt8();
        switch (b)
        {
            case CodeStringEmpty:
            {
                reader.Skip1Byte();
                value = "";
                return true;
            }
            case CodeStringPtr:
            {
                reader.Skip1Byte();
                var strOfs = reader.ReadVUInt64();
                value = ReadUtf8WithVUintLen(reader, strOfs);
                return true;
            }
        }

        value = "";
        return false;
    }

    public static bool TryGetStringBytes(scoped ref MemReader reader, out ReadOnlySpan<byte> value)
    {
        var b = reader.PeekUInt8();
        switch (b)
        {
            case CodeStringEmpty:
            {
                reader.Skip1Byte();
                value = new();
                return true;
            }
            case CodeStringPtr:
            {
                reader.Skip1Byte();
                var strOfs = reader.ReadVUInt64();
                var reader2 = reader;
                reader2.SetCurrentPositionWithoutController(strOfs);
                var len = reader2.ReadVUInt32();
                value = reader2.ReadBlockAsSpan(len);
                return true;
            }
        }

        value = new();
        return false;
    }

    public static bool TryGetLong(ref MemReader reader, out long value)
    {
        var b = reader.PeekUInt8();
        switch (b)
        {
            case >= Code0 and <= CodeM10:
            {
                reader.Skip1Byte();
                value = b < CodeM1 ? b - Code0 : CodeM1 - b - 1;
                return true;
            }
            case CodeInteger:
            {
                reader.Skip1Byte();
                value = (long)reader.ReadVUInt64();
                return true;
            }
            case CodeMInteger:
            {
                reader.Skip1Byte();
                value = -(long)reader.ReadVUInt64();
                return true;
            }
        }

        value = 0;
        return false;
    }

    public static bool TryGetULong(ref MemReader reader, out ulong value)
    {
        var b = reader.PeekUInt8();
        switch (b)
        {
            case >= Code0 and < CodeM1:
            {
                reader.Skip1Byte();
                value = (ulong)(b - Code0);
                return true;
            }
            case CodeInteger:
            {
                reader.Skip1Byte();
                value = reader.ReadVUInt64();
                return true;
            }
        }

        value = 0;
        return false;
    }

    public static string ReadUtf8WithVUintLen(in MemReader reader, ulong ofs)
    {
        var reader2 = reader;
        reader2.SetCurrentPositionWithoutController(ofs);
        var res = reader2.ReadStringInUtf8();
        return res;
    }
}

public struct BonBuilder
{
    enum State
    {
        Empty,
        Full,
        Finished,
        Array,
        ObjectKey,
        ObjectValue,
        ClassKey,
        ClassValue,
        DictionaryKey,
        DictionaryValue,
        Tuple
    };

    State _state = State.Empty;
    ulong _lastBonPos = 0;

    StructList<(StructList<ulong> ObjKeys, MemWriter Data, uint Items, State State, HashSet<string>? ObjKeysSet)>
        _stack = new();

    StructList<ulong> _objKeys = new();
    MemWriter _topData;
    uint _items = 0;
    LruCache<string, ulong>? _strCache = null;
    SpanByteLruCache<ulong>? _u8Cache = null;
    LruCache<(bool IsClass, StructList<ulong> ObjKeys), ulong>? _objKeysCache = null;
    HashSet<string>? _objKeysSet;

    public BonBuilder(in MemWriter memWriter)
    {
        _topData = memWriter;
    }

    public BonBuilder()
    {
        _topData = new();
    }

    public BonBuilder(IMemWriter memWriter)
    {
        _topData = new(memWriter);
    }

    public ulong EstimateLowerBoundSize()
    {
        var res = _topData.GetCurrentPosition();
        foreach (var item in _stack)
        {
            res += item.Data.GetCurrentPosition();
        }

        return (ulong)res;
    }

    public void Write(string? value)
    {
        BeforeBon();
        BasicWriteString(ref _topData, out _lastBonPos, value);
        AfterBon();
    }

    public void WriteUtf8(ReadOnlySpan<byte> value)
    {
        BeforeBon();
        BasicWriteString(ref _topData, out _lastBonPos, value);
        AfterBon();
    }

    public void Write(bool value)
    {
        BeforeBon();
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        _topData.WriteUInt8(value ? Helpers.CodeTrue : Helpers.CodeFalse);
        AfterBon();
    }

    public void Write(double value)
    {
        BeforeBon();
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        Helpers.Write(ref _topData, value);
        AfterBon();
    }

    public void Write(long value)
    {
        BeforeBon();
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        switch (value)
        {
            case > 10:
                _topData.WriteUInt8(Helpers.CodeInteger);
                _topData.WriteVUInt64((ulong)value);
                break;
            case >= 0:
                _topData.WriteUInt8((byte)(Helpers.Code0 + (int)value));
                break;
            case >= -10:
                _topData.WriteUInt8((byte)(Helpers.CodeM1 - 1 - (int)value));
                break;
            default:
                _topData.WriteUInt8(Helpers.CodeMInteger);
                _topData.WriteVUInt64((ulong)-value);
                break;
        }

        AfterBon();
    }

    public void Write(ulong value)
    {
        BeforeBon();
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        switch (value)
        {
            case <= 10:
                _topData.WriteUInt8((byte)(Helpers.Code0 + (int)value));
                break;
            default:
                _topData.WriteUInt8(Helpers.CodeInteger);
                _topData.WriteVUInt64(value);
                break;
        }

        AfterBon();
    }

    public void WriteNull()
    {
        BeforeBon();
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        _topData.WriteUInt8(Helpers.CodeNull);
        AfterBon();
    }

    public void WriteUndefined()
    {
        BeforeBon();
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        _topData.WriteUInt8(Helpers.CodeUndefined);
        AfterBon();
    }

    public void Write(DateTime value)
    {
        BeforeBon();
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        _topData.WriteUInt8(Helpers.CodeDateTime);
        var v = value.ToBinary();
        _topData.WriteInt64LE(v);
        AfterBon();
    }

    public void Write(Guid value)
    {
        BeforeBon();
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        _topData.WriteUInt8(Helpers.CodeGuid);
        _topData.WriteGuid(value);
        AfterBon();
    }

    public void Write(ReadOnlySpan<byte> value)
    {
        BeforeBon();

        if (value.IsEmpty)
        {
            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeByteArrayEmpty);
        }
        else
        {
            ref var rootData = ref _topData;
            if (_stack.Count > 0)
            {
                rootData = ref _stack[0].Item2;
            }

            var pos = rootData.GetCurrentPosition();
            rootData.WriteVUInt32((uint)value.Length);
            rootData.WriteBlock(value);
            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeByteArrayPtr);
            _topData.WriteVUInt64((ulong)pos);
        }

        AfterBon();
    }

    public void StartArray()
    {
        BeforeBon();
        StackPush();
        _state = State.Array;
    }

    public void FinishArray()
    {
        if (_state != State.Array) ThrowWrongState();
        var items = _items;
        var bytes = _topData;
        var objKeys = _objKeys;
        StackPop();
        if (items == 0)
        {
            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeArrayEmpty);
        }
        else if (items >= 32)
        {
            ref var rootData = ref _topData;
            if (_stack.Count > 0)
            {
                rootData = ref _stack[0].Item2;
            }

            if (!bytes.GetSpan().IsEmpty)
            {
                var pos2 = rootData.GetCurrentPosition();
                rootData.WriteBlock(bytes.GetSpan());
                objKeys.Add((ulong)pos2);
            }

            var pos = rootData.GetCurrentPosition();
            rootData.WriteVUInt32(items);
            for (var i = 0u; i < objKeys.Count; i++)
            {
                rootData.WriteUInt64LE(objKeys[i]);
            }

            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeArraySplitBy32Ptr);
            _topData.WriteVUInt64((ulong)pos);
        }
        else
        {
            Debug.Assert(objKeys.Count == 0);
            ref var rootData = ref _topData;
            if (_stack.Count > 0)
            {
                rootData = ref _stack[0].Item2;
            }

            var pos = rootData.GetCurrentPosition();
            rootData.WriteVUInt32(items);
            rootData.WriteBlock(bytes.GetSpan());
            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeArrayPtr);
            _topData.WriteVUInt64((ulong)pos);
        }

        AfterBon();
    }

    public void StartTuple()
    {
        BeforeBon();
        StackPush();
        _state = State.Tuple;
    }

    public void FinishTuple()
    {
        if (_state != State.Tuple) ThrowWrongState();
        var items = _items;
        var bytes = _topData;
        var objKeys = _objKeys;
        StackPop();
        Debug.Assert(objKeys.Count == 0);
        ref var rootData = ref _topData;
        if (_stack.Count > 0)
        {
            rootData = ref _stack[0].Item2;
        }

        var pos = rootData.GetCurrentPosition();
        rootData.WriteVUInt32(items);
        rootData.WriteBlock(bytes.GetSpan());
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        _topData.WriteUInt8(Helpers.CodeTuplePtr);
        _topData.WriteVUInt64((ulong)pos);

        AfterBon();
    }

    public void StartObject()
    {
        BeforeBon();
        StackPush();
        _state = State.ObjectKey;
        _objKeysSet = null;
    }

    public void WriteKey(string name)
    {
        if (!TryWriteKey(name)) throw new ArgumentException("Key " + name + " already written");
    }

    public bool TryWriteKey(string name)
    {
        if (_state is not State.ObjectKey and not State.ClassKey) ThrowWrongState();
        _objKeysSet ??= [];
        if (!_objKeysSet!.Add(name)) return false;
        _objKeys.Add(WriteDedupString(name));
        _state = _state == State.ObjectKey ? State.ObjectValue : State.ClassValue;
        return true;
    }

    public void WriteKey(ReadOnlySpan<byte> name)
    {
        if (_state is not State.ObjectKey and not State.ClassKey) ThrowWrongState();
        _objKeys.Add(WriteDedupString(name));
        _state = _state == State.ObjectKey ? State.ObjectValue : State.ClassValue;
    }

    public void FinishObject()
    {
        if (_state != State.ObjectKey) ThrowWrongState();

        var items = _items;
        var objKeys = _objKeys;
        var bytes = _topData;
        StackPop();
        if (items == 0)
        {
            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeObjectEmpty);
        }
        else
        {
            ref var rootData = ref _topData;
            if (_stack.Count > 0)
            {
                rootData = ref _stack[0].Item2;
            }

            _objKeysCache ??= new();
            ref var posKeys = ref _objKeysCache.GetOrAddValueRef((false, objKeys), out var added);
            if (added)
            {
                posKeys = (ulong)rootData.GetCurrentPosition();
                rootData.WriteVUInt32(items);
                foreach (var keyOfs in objKeys)
                {
                    rootData.WriteVUInt64(keyOfs);
                }
            }

            var pos = rootData.GetCurrentPosition();
            rootData.WriteVUInt64(posKeys);
            rootData.WriteBlock(bytes.GetSpan());
            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeObjectPtr);
            _topData.WriteVUInt64((ulong)pos);
        }

        AfterBon();
    }

    public void StartClass(string name)
    {
        BeforeBon();
        StackPush();
        _state = State.ClassKey;
        _objKeys.Add(WriteDedupString(name));
        _objKeysSet = null;
    }

    public void StartClass(ReadOnlySpan<byte> name)
    {
        BeforeBon();
        StackPush();
        _state = State.ClassKey;
        _objKeys.Add(WriteDedupString(name));
        _objKeysSet = null;
    }

    public void FinishClass()
    {
        if (_state != State.ClassKey) ThrowWrongState();

        var items = _items;
        var objKeys = _objKeys;
        var bytes = _topData;
        StackPop();
        ref var rootData = ref _topData;
        if (_stack.Count > 0)
        {
            rootData = ref _stack[0].Item2;
        }

        _objKeysCache ??= new();
        ref var posKeys = ref _objKeysCache.GetOrAddValueRef((true, objKeys), out var added);
        if (added)
        {
            posKeys = (ulong)rootData.GetCurrentPosition();
            rootData.WriteVUInt32(items);
            foreach (var keyOfs in objKeys)
            {
                rootData.WriteVUInt64(keyOfs);
            }
        }

        var pos = rootData.GetCurrentPosition();
        rootData.WriteVUInt64(posKeys);
        rootData.WriteBlock(bytes.GetSpan());
        _lastBonPos = (ulong)_topData.GetCurrentPosition();
        _topData.WriteUInt8(Helpers.CodeClassPtr);
        _topData.WriteVUInt64((ulong)pos);

        AfterBon();
    }

    public void StartDictionary()
    {
        BeforeBon();
        StackPush();
        _state = State.DictionaryKey;
    }

    public void FinishDictionary()
    {
        if (_state != State.DictionaryKey) ThrowWrongState();

        var items = _items;
        var bytes = _topData;
        StackPop();
        if (items == 0)
        {
            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeDictionaryEmpty);
        }
        else
        {
            ref var rootData = ref _topData;
            if (_stack.Count > 0)
            {
                rootData = ref _stack[0].Item2;
            }

            var pos = rootData.GetCurrentPosition();
            rootData.WriteVUInt32(items / 2);
            rootData.WriteBlock(bytes.GetSpan());
            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeDictionaryPtr);
            _topData.WriteVUInt64((ulong)pos);
        }

        AfterBon();
    }

    void StackPush()
    {
        _stack.Add((_objKeys, _topData, _items, _state, _objKeysSet));
        _objKeys = new();
        _objKeysSet = null;
        _topData = new();
        _items = 0;
    }

    void StackPop()
    {
        (_objKeys, _topData, _items, _state, _objKeysSet) = _stack.Last;
        _stack.Pop();
    }

    void AfterBon()
    {
        _items++;
        _state = _state switch
        {
            State.Empty => State.Full,
            State.ObjectValue => State.ObjectKey,
            State.ClassValue => State.ClassKey,
            State.DictionaryKey => State.DictionaryValue,
            State.DictionaryValue => State.DictionaryKey,
            _ => _state
        };
        if (_state == State.Array && _items % 32 == 0)
        {
            ref var writer = ref _stack[0].Data;
            Debug.Assert(_stack.Count > 0);
            var pos = writer.GetCurrentPosition();
            writer.WriteBlock(_topData.GetSpan());
            _topData.Reset();
            _objKeys.Add((ulong)pos);
        }
    }

    void BeforeBon()
    {
        if (_state is not (State.Empty or State.Array or State.Tuple or State.ObjectValue or State.ClassValue
            or State.DictionaryKey
            or State.DictionaryValue))
        {
            ThrowWrongState();
        }
    }

    void BasicWriteString(ref MemWriter data, out ulong bonPos, string? value)
    {
        if (value == null)
        {
            bonPos = (ulong)data.GetCurrentPosition();
            data.WriteUInt8(Helpers.CodeNull);
        }
        else if (value.Length == 0)
        {
            bonPos = (ulong)data.GetCurrentPosition();
            data.WriteUInt8(Helpers.CodeStringEmpty);
        }
        else
        {
            var ofs = WriteDedupString(value);
            bonPos = (ulong)data.GetCurrentPosition();
            data.WriteUInt8(Helpers.CodeStringPtr);
            data.WriteVUInt64(ofs);
        }
    }

    void BasicWriteString(ref MemWriter data, out ulong bonPos, ReadOnlySpan<byte> value)
    {
        if (value.Length == 0)
        {
            bonPos = (ulong)data.GetCurrentPosition();
            data.WriteUInt8(Helpers.CodeStringEmpty);
        }
        else
        {
            var ofs = WriteDedupString(value);
            bonPos = (ulong)data.GetCurrentPosition();
            data.WriteUInt8(Helpers.CodeStringPtr);
            data.WriteVUInt64(ofs);
        }
    }

    void ThrowWrongState()
    {
        throw new InvalidOperationException("State " + _state + " is not valid for this operation");
    }

    ulong WriteDedupString(string value)
    {
        _strCache ??= new(512);
        ref var pos = ref _strCache.GetOrAddValueRef(value, out var added);
        if (!added)
        {
            return pos;
        }

        ref var writer = ref _topData;
        if (_stack.Count > 0)
        {
            writer = ref _stack[0].Data;
        }

        pos = (ulong)writer.GetCurrentPosition();
        writer.WriteStringInUtf8(value);
        return pos;
    }

    ulong WriteDedupString(ReadOnlySpan<byte> value)
    {
        _u8Cache ??= new(512);
        ref var pos = ref _u8Cache.GetOrAddValueRef(value, out var added);
        if (!added)
        {
            return pos;
        }

        ref var writer = ref _topData;
        if (_stack.Count > 0)
        {
            writer = ref _stack[0].Data;
        }

        pos = (ulong)writer.GetCurrentPosition();
        writer.WriteVUInt32((uint)value.Length);
        writer.WriteBlock(value);
        _u8Cache[value] = pos;
        return pos;
    }

    public MemWriter Finish()
    {
        MoveToFinished();
        return _topData;
    }

    void MoveToFinished()
    {
        if (_state == State.Full)
        {
            _topData.WriteUInt8((byte)((ulong)_topData.GetCurrentPosition() - _lastBonPos));
            _state = State.Finished;
        }

        if (_state != State.Finished) ThrowWrongState();
    }

    public ReadOnlyMemory<byte> FinishAsMemory()
    {
        return Finish().GetPersistentMemoryAndReset();
    }
}

public struct Bon
{
    MemReader _reader;
    uint _items;

    public Bon(ReadOnlyMemory<byte> buf) : this(new ReadOnlyMemoryMemReader(buf))
    {
    }

    public Bon(ByteBuffer buf) : this(new ReadOnlyMemoryMemReader(buf.AsSyncReadOnlyMemory()))
    {
    }

    public Bon(IMemReader reader)
    {
        _reader = new(reader);
        _reader.SetCurrentPositionWithoutController(Helpers.CalcStartOffsetOfBon(ref _reader));
        _items = 1;
    }

    public Bon(in MemReader reader)
    {
        _reader = reader;
        _reader.SetCurrentPositionWithoutController(Helpers.CalcStartOffsetOfBon(ref _reader));
        _items = 1;
    }

    public uint Items => _items;

    public Bon(in MemReader reader, ulong ofs, uint items)
    {
        _reader = reader;
        _reader.SetCurrentPositionWithoutController(ofs);
        _items = items;
    }

    public BonType BonType
    {
        get
        {
            Debug.Assert(_items > 0);
            return Helpers.BonTypeFromByte(_reader.PeekUInt8());
        }
    }

    public bool TryGetDouble(out double value)
    {
        return ItemConsumed(Helpers.TryGetDouble(ref _reader, out value));
    }

    bool ItemConsumed(bool result)
    {
        if (result) _items--;
        return result;
    }

    public bool Eof => _items == 0;

    public void Skip()
    {
        Debug.Assert(_items > 0);
        Helpers.SkipItemOfBon(ref _reader);
        _items--;
    }

    public bool TryGetLong(out long value)
    {
        return ItemConsumed(Helpers.TryGetLong(ref _reader, out value));
    }

    public bool TryGetULong(out ulong value)
    {
        return ItemConsumed(Helpers.TryGetULong(ref _reader, out value));
    }

    public bool TryGetDateTime(out DateTime value)
    {
        return ItemConsumed(Helpers.TryGetDateTime(ref _reader, out value));
    }

    public bool TryGetGuid(out Guid value)
    {
        return ItemConsumed(Helpers.TryGetGuid(ref _reader, out value));
    }

    public bool TryGetString(out string value)
    {
        return ItemConsumed(Helpers.TryGetString(ref _reader, out value));
    }

    public bool TryGetStringBytes(out ReadOnlySpan<byte> value)
    {
        return ItemConsumed(Helpers.TryGetStringBytes(ref _reader, out value));
    }

    public bool TryGetBool(out bool value)
    {
        value = false;
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeTrue:
                value = true;
                goto case Helpers.CodeFalse;
            case Helpers.CodeFalse:
                _reader.Skip1Byte();
                _items--;
                return true;
            default:
                return false;
        }
    }

    public bool TryGetUndefined()
    {
        if (_reader.PeekUInt8() == Helpers.CodeUndefined)
        {
            _reader.Skip1Byte();
            _items--;
            return true;
        }

        return false;
    }

    public bool TryGetNull()
    {
        if (_reader.PeekUInt8() == Helpers.CodeNull)
        {
            _reader.Skip1Byte();
            _items--;
            return true;
        }

        return false;
    }

    public bool TryGetByteArray(out ReadOnlySpan<byte> value)
    {
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeByteArrayEmpty:
                _reader.Skip1Byte();
                _items--;
                value = new();
                return true;
            case Helpers.CodeByteArrayPtr:
            {
                _reader.Skip1Byte();
                _items--;
                var ofs = _reader.ReadVUInt64();
                var reader2 = _reader;
                reader2.SetCurrentPositionWithoutController(ofs);
                var len = reader2.ReadVUInt32();
                value = reader2.ReadBlockAsSpan(len);
                return true;
            }
            default:
                value = new();
                return false;
        }
    }

    public bool TryGetArray(out ArrayBon bon)
    {
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeArrayEmpty:
                _reader.Skip1Byte();
                _items--;
                bon = new(new(), 0, 0, false);
                return true;
            case Helpers.CodeArrayPtr:
            {
                _reader.Skip1Byte();
                _items--;
                var ofs = _reader.ReadVUInt64();
                var reader2 = _reader;
                reader2.SetCurrentPositionWithoutController(ofs);
                var items = reader2.ReadVUInt32();
                bon = new(_reader, reader2.GetCurrentPositionWithoutController(), items, false);
                return true;
            }
            case Helpers.CodeArraySplitBy32Ptr:
            {
                _reader.Skip1Byte();
                _items--;
                var ofs = _reader.ReadVUInt64();
                var reader2 = _reader;
                reader2.SetCurrentPositionWithoutController(ofs);
                var items = reader2.ReadVUInt32();
                bon = new(_reader, reader2.GetCurrentPositionWithoutController(), items, true);
                return true;
            }
            default:
                bon = new(new(), 0, 0, false);
                return false;
        }
    }

    public bool TryGetTuple(out ArrayBon bon)
    {
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeTuplePtr:
            {
                _reader.Skip1Byte();
                _items--;
                var ofs = _reader.ReadVUInt64();
                var reader2 = _reader;
                reader2.SetCurrentPositionWithoutController(ofs);
                var items = reader2.ReadVUInt32();
                bon = new(_reader, reader2.GetCurrentPositionWithoutController(), items, false);
                return true;
            }
            default:
                bon = new(new(), 0, 0, false);
                return false;
        }
    }

    public bool TryGetObject(out KeyedBon bon)
    {
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeObjectEmpty:
                _reader.Skip1Byte();
                _items--;
                bon = new(new(), 0, 0, 0);
                return true;
            case Helpers.CodeObjectPtr:
            {
                _reader.Skip1Byte();
                _items--;
                var ofs = _reader.ReadVUInt64();
                var reader2 = _reader;
                reader2.SetCurrentPositionWithoutController(ofs);
                var ofsKeys = reader2.ReadVUInt64();
                ofs = reader2.GetCurrentPositionWithoutController();
                reader2.SetCurrentPositionWithoutController(ofsKeys);
                var items = reader2.ReadVUInt32();
                bon = new(_reader, ofs, reader2.GetCurrentPositionWithoutController(), items);
                return true;
            }
            default:
                bon = new(new(), 0, 0, 0);
                return false;
        }
    }

    public bool TryGetClass(out KeyedBon bon, out ReadOnlySpan<byte> name)
    {
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeClassPtr:
            {
                _reader.Skip1Byte();
                _items--;
                var ofs = _reader.ReadVUInt64();
                var reader2 = _reader;
                reader2.SetCurrentPositionWithoutController(ofs);
                var ofsKeys = reader2.ReadVUInt64();
                ofs = reader2.GetCurrentPositionWithoutController();
                reader2.SetCurrentPositionWithoutController(ofsKeys);
                var items = reader2.ReadVUInt32();
                var nameOfs = reader2.ReadVUInt64();
                var reader3 = reader2;
                reader3.SetCurrentPositionWithoutController(nameOfs);
                var len = reader3.ReadVUInt32();
                name = reader3.ReadBlockAsSpan(len);
                bon = new(_reader, ofs, reader2.GetCurrentPositionWithoutController(), items);
                return true;
            }
            default:
                bon = new(new(), 0, 0, 0);
                name = new();
                return false;
        }
    }

    public bool PeekClass(out ReadOnlySpan<byte> name)
    {
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeClassPtr:
            {
                var reader = _reader;
                reader.Skip1Byte();
                var ofs = reader.ReadVUInt64();
                var reader2 = reader;
                reader2.SetCurrentPositionWithoutController(ofs);
                var ofsKeys = reader2.ReadVUInt64();
                reader2.SetCurrentPositionWithoutController(ofsKeys);
                reader2.SkipVUInt32();
                var nameOfs = reader2.ReadVUInt64();
                reader2.SetCurrentPositionWithoutController(nameOfs);
                var len = reader2.ReadVUInt32();
                name = reader2.ReadBlockAsSpan(len);
                return true;
            }
            default:
                name = new();
                return false;
        }
    }

    public bool TryGetDictionary(out Bon bon)
    {
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeDictionaryEmpty:
                _reader.Skip1Byte();
                _items--;
                bon = new(new(), 0, 0);
                return true;
            case Helpers.CodeDictionaryPtr:
            {
                _reader.Skip1Byte();
                _items--;
                var ofs = _reader.ReadVUInt64();
                var reader2 = _reader;
                reader2.SetCurrentPositionWithoutController(ofs);
                var items = reader2.ReadVUInt32();
                bon = new(_reader, reader2.GetCurrentPositionWithoutController(), items * 2);
                return true;
            }
            default:
                bon = new(new(), 0, 0);
                return false;
        }
    }

    public void DumpToJson(Utf8JsonWriter writer)
    {
        switch (BonType)
        {
            case BonType.Error:
                writer.WriteCommentValue("Error");
                Skip();
                break;
            case BonType.Null:
                writer.WriteNullValue();
                Skip();
                break;
            case BonType.Undefined:
                writer.WriteNullValue();
                writer.WriteCommentValue("undefined");
                Skip();
                break;
            case BonType.Bool:
                TryGetBool(out var b);
                writer.WriteBooleanValue(b);
                break;
            case BonType.Integer:
                if (TryGetULong(out var ul))
                {
                    writer.WriteNumberValue(ul);
                }
                else if (TryGetLong(out var l))
                {
                    writer.WriteNumberValue(l);
                }
                else
                {
                    writer.WriteCommentValue("Not integer");
                    Skip();
                }

                break;
            case BonType.Float:
                TryGetDouble(out var d);
                switch (d)
                {
                    case double.PositiveInfinity:
                        writer.WriteStringValue("+∞");
                        break;
                    case double.NegativeInfinity:
                        writer.WriteStringValue("-∞");
                        break;
                    default:
                        writer.WriteNumberValue(d);
                        break;
                }

                break;
            case BonType.String:
                TryGetString(out var s);
                writer.WriteStringValue(s);
                break;
            case BonType.DateTime:
                TryGetDateTime(out var dt);
                writer.WriteStringValue(dt.ToString("O"));
                break;
            case BonType.Guid:
                TryGetGuid(out var g);
                writer.WriteStringValue(g.ToString("D"));
                break;
            case BonType.Array:
            {
                writer.WriteStartArray();
                TryGetArray(out var ab);
                var pos = 0u;
                while (ab.TryGet(pos, out var bon))
                {
                    pos += bon.Items;
                    while (!bon.Eof)
                    {
                        bon.DumpToJson(writer);
                    }
                }

                writer.WriteEndArray();
                break;
            }
            case BonType.Tuple:
            {
                writer.WriteStartArray();
                TryGetTuple(out var ab);
                if (ab.TryGet(0, out var bon))
                {
                    while (!bon.Eof)
                    {
                        bon.DumpToJson(writer);
                    }
                }

                writer.WriteEndArray();
                break;
            }
            case BonType.Object:
                writer.WriteStartObject();
                TryGetObject(out var o);
                var ov = o.Values();
                while (o.NextKey() is { } k)
                {
                    writer.WritePropertyName(k);
                    ov.DumpToJson(writer);
                }

                writer.WriteEndObject();
                break;
            case BonType.Class:
                writer.WriteStartObject();
                TryGetClass(out var c, out var cn);
                writer.WritePropertyName("__type__");
                writer.WriteStringValue(cn);
                var cv = c.Values();
                while (c.NextKey() is { } k)
                {
                    writer.WritePropertyName(k);
                    cv.DumpToJson(writer);
                }

                writer.WriteEndObject();
                break;
            case BonType.Dictionary:
                writer.WriteStartArray();
                TryGetDictionary(out var db);
                while (!db.Eof)
                {
                    writer.WriteStartArray();
                    db.DumpToJson(writer);
                    db.DumpToJson(writer);
                    writer.WriteEndArray();
                }

                writer.WriteEndArray();
                break;
            case BonType.ByteArray:
                TryGetByteArray(out var ba);
                writer.WriteBase64StringValue(ba);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public string DumpToJson()
    {
        var options = new JsonWriterOptions
        {
            Indented = true,
            SkipValidation = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream, options);
        DumpToJson(writer);
        writer.Flush();
        return Encoding.UTF8.GetString(stream.ToArray()).ReplaceLineEndings("\n");
    }
}

public struct ArrayBon
{
    MemReader _reader;
    readonly ulong _ofs;
    readonly uint _items;
    readonly bool _splitBy32;

    public ArrayBon(in MemReader reader, ulong ofs, uint items, bool splitBy32)
    {
        _reader = reader;
        _ofs = ofs;
        _items = items;
        _splitBy32 = splitBy32;
    }

    public uint Items => _items;

    public bool TryGet(uint index, out Bon bon)
    {
        if (index >= _items)
        {
            bon = new(new(), 0, 0);
            return false;
        }

        if (_splitBy32)
        {
            _reader.SetCurrentPositionWithoutController(_ofs + index / 32 * 8);
            var ofs = _reader.ReadUInt64LE();
            bon = new(_reader, ofs, Math.Min(_items - index / 32 * 32, 32));
            var skipCount = index % 32;
            for (var i = 0u; i < skipCount; i++)
            {
                bon.Skip();
            }
        }
        else
        {
            bon = new(_reader, _ofs, _items);
            for (var i = 0u; i < index; i++)
            {
                bon.Skip();
            }
        }

        return true;
    }
}

public struct KeyedBon
{
    MemReader _reader;
    readonly ulong _ofs;
    readonly ulong _ofsKeys;
    readonly uint _items;
    uint _keysItemsIter;

    public KeyedBon(in MemReader reader, ulong ofs, ulong ofsKeys, uint items)
    {
        _reader = reader;
        _ofs = ofs;
        _ofsKeys = ofsKeys;
        _items = items;
        _keysItemsIter = items;
        _reader.SetCurrentPositionWithoutController(ofsKeys);
    }

    public uint Items => _items;

    public Bon Values()
    {
        return new(_reader, _ofs, _items);
    }

    public void Reset()
    {
        _keysItemsIter = _items;
        _reader.SetCurrentPositionWithoutController(_ofsKeys);
    }

    public string? NextKey()
    {
        if (_keysItemsIter == 0) return null;
        var ofs = _reader.ReadVUInt64();
        _keysItemsIter--;
        var reader = _reader;
        reader.SetCurrentPositionWithoutController(ofs);
        return reader.ReadStringInUtf8();
    }

    public ReadOnlySpan<byte> NextKeyUtf8()
    {
        if (_keysItemsIter == 0) return new();
        var ofs = _reader.ReadVUInt64();
        _keysItemsIter--;
        var reader = _reader;
        reader.SetCurrentPositionWithoutController(ofs);
        var len = reader.ReadVUInt32();
        return reader.ReadBlockAsSpan(len);
    }

    public bool TryGet(string key, out Bon bon)
    {
        var l = Encoding.UTF8.GetByteCount(key);
        _reader.SetCurrentPositionWithoutController(_ofsKeys);
        var keyBuf = l > 256 ? GC.AllocateUninitializedArray<byte>(l) : stackalloc byte[l];
        var first = true;
        for (var i = 0; i < _items; i++)
        {
            var kOfs = _reader.ReadVUInt64();
            var reader = _reader;
            reader.SetCurrentPositionWithoutController(kOfs);
            var kLen = reader.ReadVUInt32();
            if (kLen != l) continue;
            if (first)
            {
                Encoding.UTF8.GetBytes(key, keyBuf);
                first = false;
            }

            if (!reader.ReadBlockAsSpan((uint)l).SequenceEqual(keyBuf)) continue;
            bon = new(_reader, _ofs, (uint)i + 1);
            while (i-- > 0)
            {
                bon.Skip();
            }

            return true;
        }

        bon = new(new(), 0, 0);
        return false;
    }
}
