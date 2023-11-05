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
    Array, // 27 - empty, 131 - VUint offset to VUint len + Bons*len
    Object, // 28 - empty, 132 - VUint offset to VUint offset to VUint len + VUint offsets to strings, Bons*len
    Class, // 133 - VUint offset to VUint offset to VUint len + VUint offset to type name string + VUint offsets to strings, Bons*len
    Dictionary, // 29 - empty, 134 - VUint offset to VUint len + (Key Bon + Value Bon)*len
    ByteArray, // 30 - empty, 135 - VUint offset to VUint len + bytes
}

public static class Helpers
{
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

    public static BonType BonTypeFromByte(byte b)
    {
        return b switch
        {
            1 => BonType.Null,
            2 => BonType.Undefined,
            3 or 4 => BonType.Bool,
            >= 5 and <= 25 or 128 or 129 => BonType.Integer,
            64 or 65 or 66 => BonType.Float,
            26 or 130 => BonType.String,
            67 => BonType.DateTime,
            68 => BonType.Guid,
            27 or 131 => BonType.Array,
            28 or 132 => BonType.Object,
            133 => BonType.Class,
            29 or 134 => BonType.Dictionary,
            30 or 135 => BonType.ByteArray,
            _ => BonType.Error
        };
    }

    public static void Write(ref MemWriter data, double value)
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

    public static nint CalcStartOffsetOfBon(ref MemReader reader)
    {
        var len = reader.GetLength();
        reader.SetCurrentPosition(len - 1);
        var b = reader.ReadUInt8();
        return len - b - 1;
    }

    public static void SkipItemOfBon(ref MemReader reader)
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

    public static bool TryGetDouble(ref MemReader reader, out double value)
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

    public static bool TryGetDateTime(ref MemReader reader, out DateTime value)
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

    public static bool TryGetGuid(ref MemReader reader, out Guid value)
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

    public static bool TryGetString(ref MemReader reader, out string value)
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
                value = ReadUtf8WithVUintLen(ref reader, strOfs);
                return true;
            }
        }

        value = "";
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

    public static string ReadUtf8WithVUintLen(ref MemReader reader, ulong ofs)
    {
        var pos = reader.GetCurrentPosition();
        reader.SetCurrentPosition((long)ofs);
        var res = reader.ReadStringInUtf8();
        reader.SetCurrentPosition(pos);
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
    };

    State _state = State.Empty;
    ulong _lastBonPos = 0;

    StructList<(StructList<ulong> ObjKeys, MemWriter Data, uint Items, State State, HashSet<string>? ObjKeysSet)>
        _stack = new();

    StructList<ulong> _objKeys = new();
    MemWriter _topData;
    uint _items = 0;
    readonly Dictionary<string, ulong> _strCache = new();
    readonly Dictionary<(bool IsClass, StructList<ulong> ObjKeys), ulong> _objKeysCache = new();
    HashSet<string>? _objKeysSet;

    public BonBuilder(MemWriter memWriter)
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
        StackPop();
        if (items == 0)
        {
            _lastBonPos = (ulong)_topData.GetCurrentPosition();
            _topData.WriteUInt8(Helpers.CodeArrayEmpty);
        }
        else
        {
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

    public void StartObject()
    {
        BeforeBon();
        StackPush();
        _state = State.ObjectKey;
        _objKeysSet = new();
    }

    public void WriteKey(string name)
    {
        if (!TryWriteKey(name)) throw new ArgumentException("Key " + name + " already written");
    }

    public bool TryWriteKey(string name)
    {
        if (_state is not State.ObjectKey and not State.ClassKey) ThrowWrongState();
        if (!_objKeysSet!.Add(name)) return false;
        _objKeys.Add(WriteDedupString(name));
        _state = _state == State.ObjectKey ? State.ObjectValue : State.ClassValue;
        return true;
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

            if (!_objKeysCache.TryGetValue((false, objKeys), out var posKeys))
            {
                posKeys = (ulong)rootData.GetCurrentPosition();
                rootData.WriteVUInt32(items);
                foreach (var keyOfs in objKeys)
                {
                    rootData.WriteVUInt64(keyOfs);
                }

                _objKeysCache.Add((false, objKeys), posKeys);
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
        _objKeysSet = new();
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

        if (!_objKeysCache.TryGetValue((true, objKeys), out var posKeys))
        {
            posKeys = (ulong)rootData.GetCurrentPosition();
            rootData.WriteVUInt32(items);
            foreach (var keyOfs in objKeys)
            {
                rootData.WriteVUInt64(keyOfs);
            }

            _objKeysCache.Add((true, objKeys), posKeys);
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
    }

    void BeforeBon()
    {
        if (_state is not (State.Empty or State.Array or State.ObjectValue or State.ClassValue or State.DictionaryKey
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

    void ThrowWrongState()
    {
        throw new InvalidOperationException("State " + _state + " is not valid for this operation");
    }

    ulong WriteDedupString(string value)
    {
        if (_strCache.TryGetValue(value, out var pos))
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
        _strCache[value] = pos;
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
        _reader.SetCurrentPosition(Helpers.CalcStartOffsetOfBon(ref _reader));
        _items = 1;
    }

    public Bon(in MemReader reader)
    {
        _reader = reader;
        _reader.SetCurrentPosition(Helpers.CalcStartOffsetOfBon(ref _reader));
        _items = 1;
    }

    public uint Items => _items;

    public Bon(in MemReader reader, long ofs, uint items)
    {
        _reader = reader;
        _reader.SetCurrentPosition(ofs);
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
                var pos = _reader.GetCurrentPosition();
                _reader.SetCurrentPosition((long)ofs);
                var len = _reader.ReadVUInt32();
                value = _reader.ReadBlockAsSpan(len);
                _reader.SetCurrentPosition(pos);
                return true;
            }
            default:
                value = new();
                return false;
        }
    }

    public bool TryGetArray(out Bon bon)
    {
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeArrayEmpty:
                _reader.Skip1Byte();
                _items--;
                bon = new(new(), 0, 0);
                return true;
            case Helpers.CodeArrayPtr:
            {
                _reader.Skip1Byte();
                _items--;
                var ofs = _reader.ReadVUInt64();
                var pos = _reader.GetCurrentPosition();
                _reader.SetCurrentPosition((long)ofs);
                var items = _reader.ReadVUInt32();
                bon = new(_reader, _reader.GetCurrentPosition(), items);
                _reader.SetCurrentPosition(pos);
                return true;
            }
            default:
                bon = new(new(), 0, 0);
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
                var pos = _reader.GetCurrentPosition();
                _reader.SetCurrentPosition((long)ofs);
                var ofsKeys = _reader.ReadVUInt64();
                ofs = (ulong)_reader.GetCurrentPosition();
                _reader.SetCurrentPosition((long)ofsKeys);
                var items = _reader.ReadVUInt32();
                bon = new(_reader, (long)ofs, _reader.GetCurrentPosition(), items);
                _reader.SetCurrentPosition(pos);
                return true;
            }
            default:
                bon = new(new(), 0, 0, 0);
                return false;
        }
    }

    public bool TryGetClass(out KeyedBon bon, out string name)
    {
        var b = _reader.PeekUInt8();
        switch (b)
        {
            case Helpers.CodeClassPtr:
            {
                _reader.Skip1Byte();
                _items--;
                var ofs = _reader.ReadVUInt64();
                var pos = _reader.GetCurrentPosition();
                _reader.SetCurrentPosition((long)ofs);
                var ofsKeys = _reader.ReadVUInt64();
                ofs = (ulong)_reader.GetCurrentPosition();
                _reader.SetCurrentPosition((long)ofsKeys);
                var items = _reader.ReadVUInt32();
                var nameOfs = _reader.ReadVUInt64();
                name = Helpers.ReadUtf8WithVUintLen(ref _reader, nameOfs);
                bon = new(_reader, (long)ofs, _reader.GetCurrentPosition(), items);
                _reader.SetCurrentPosition(pos);
                return true;
            }
            default:
                bon = new(new(), 0, 0, 0);
                name = "";
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
                var pos = _reader.GetCurrentPosition();
                _reader.SetCurrentPosition((long)ofs);
                var items = _reader.ReadVUInt32();
                bon = new(_reader, _reader.GetCurrentPosition(), items * 2);
                _reader.SetCurrentPosition(pos);
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
                writer.WriteStartArray();
                TryGetArray(out var ab);
                while (!ab.Eof)
                {
                    ab.DumpToJson(writer);
                }

                writer.WriteEndArray();
                break;
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

public struct KeyedBon
{
    MemReader _reader;
    readonly long _ofs;
    readonly long _ofsKeys;
    long _keysOfsIter;
    readonly uint _items;
    uint _keysItemsIter;

    public KeyedBon(in MemReader reader, long ofs, long ofsKeys, uint items)
    {
        _reader = reader;
        _ofs = ofs;
        _ofsKeys = ofsKeys;
        _items = items;
        _keysOfsIter = ofsKeys;
        _keysItemsIter = items;
    }

    public uint Items => _items;

    public Bon Values()
    {
        return new(_reader, _ofs, _items);
    }

    public void Reset()
    {
        _keysOfsIter = _ofsKeys;
        _keysItemsIter = _items;
    }

    public string? NextKey()
    {
        if (_keysItemsIter == 0) return null;
        _reader.SetCurrentPosition(_keysOfsIter);
        var ofs = _reader.ReadVUInt64();
        _keysItemsIter--;
        _keysOfsIter = _reader.GetCurrentPosition();
        return Helpers.ReadUtf8WithVUintLen(ref _reader, ofs);
    }

    public bool TryGet(string key, out Bon bon)
    {
        var l = Encoding.UTF8.GetByteCount(key);
        var ofs = _ofsKeys;
        var keyBuf = l > 256 ? GC.AllocateUninitializedArray<byte>(l) : stackalloc byte[l];
        var first = true;
        for (var i = 0; i < _items; i++)
        {
            _reader.SetCurrentPosition(ofs);
            var kOfs = _reader.ReadVUInt64();
            ofs = _reader.GetCurrentPosition();
            _reader.SetCurrentPosition((long)kOfs);
            var kLen = _reader.ReadVUInt32();
            if (kLen != l) continue;
            if (first)
            {
                Encoding.UTF8.GetBytes(key, keyBuf);
                first = false;
            }

            if (!_reader.ReadBlockAsSpan((uint)l).SequenceEqual(keyBuf)) continue;
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
