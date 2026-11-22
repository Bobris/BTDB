using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using BTDB.Buffer;

namespace BTDB.EventStore2Layer;

/// <summary>Appends strings and types to a <see cref="Layer2Metadata"/> snapshot and writes the next file. Reads
/// see the snapshot and everything appended so far.</summary>
sealed class Layer2MetadataBuilder
{
    readonly Layer2Metadata? _basis;
    readonly string _bindingName;
    readonly List<byte[]> _newStrings = new();
    readonly List<(byte[]? Record, uint Key)> _newTypes = new();
    Dictionary<string, int>? _stringIds;

    public Layer2MetadataBuilder(Layer2Metadata basis)
    {
        _basis = basis;
        _bindingName = basis.BindingName;
    }

    public Layer2MetadataBuilder(string bindingName)
    {
        _bindingName = bindingName;
        _newStrings.Add(Encoding.UTF8.GetBytes(bindingName));
    }

    int BasisStrings => _basis?.StringCount ?? 0;
    int BasisTypes => _basis?.TypeCount ?? 0;

    public int StringCount => BasisStrings + _newStrings.Count;
    public int TypeCount => BasisTypes + _newTypes.Count;
    public bool HasNewTypes => _newTypes.Count > 0;

    public bool ContainsType(int typeId) =>
        typeId >= Layer2Metadata.FirstTypeId && typeId - Layer2Metadata.FirstTypeId < TypeCount;

    public ReadOnlySpan<byte> StringUtf8(int id) =>
        id < BasisStrings ? _basis!.StringUtf8(id) : _newStrings[id - BasisStrings];

    public ReadOnlySpan<byte> Record(int typeId)
    {
        var index = typeId - Layer2Metadata.FirstTypeId;
        return index < BasisTypes ? _basis!.Record(typeId)
            : _newTypes[index - BasisTypes].Record ?? throw new InvalidOperationException("Type record not set yet.");
    }

    /// <summary>Type IDs stored or appended with this shape key.</summary>
    public IEnumerable<int> TypesWithKey(uint key)
    {
        if (_basis != null)
            for (var position = _basis.LowerBound(key); position < _basis.TypeCount; position++)
            {
                var (entryKey, index) = _basis.KeyEntry(position);
                if (entryKey != key) break;
                yield return Layer2Metadata.FirstTypeId + (int)index;
            }
        for (var i = 0; i < _newTypes.Count; i++)
            if (_newTypes[i].Key == key && _newTypes[i].Record != null)
                yield return Layer2Metadata.FirstTypeId + BasisTypes + i;
    }

    /// <summary>The ID of a string, appended when new. The name to ID map is built from the snapshot on first use,
    /// only by a writer.</summary>
    public int GetOrAddString(string value)
    {
        if (_stringIds == null)
        {
            _stringIds = new(StringComparer.Ordinal);
            for (var i = 0; i < StringCount; i++)
                _stringIds.TryAdd(Encoding.UTF8.GetString(StringUtf8(i)), i);
        }
        if (_stringIds.TryGetValue(value, out var id)) return id;
        id = StringCount;
        _newStrings.Add(Encoding.UTF8.GetBytes(value));
        _stringIds.Add(value, id);
        return id;
    }

    /// <summary>Reserves the next type ID; its record is set by <see cref="SetType"/> before <see cref="Build"/>.
    /// Members of a cyclic group reserve consecutive IDs first, because their records refer to each other.</summary>
    public int ReserveType()
    {
        _newTypes.Add((null, 0));
        return Layer2Metadata.FirstTypeId + TypeCount - 1;
    }

    public void SetType(int typeId, byte[] record, uint key)
    {
        var index = typeId - Layer2Metadata.FirstTypeId - BasisTypes;
        if (record.Length == 0) throw new ArgumentException("A record has at least its category.", nameof(record));
        _newTypes[index] = (record, key);
    }

    public Layer2Metadata Build()
    {
        if (_basis != null && _newStrings.Count == 0 && _newTypes.Count == 0) return _basis;
        var stringCount = StringCount;
        var typeCount = TypeCount;
        long stringLength = _basis?.StringDataBytes.Length ?? 0;
        foreach (var value in _newStrings) stringLength += value.Length;
        long recordLength = _basis?.RecordDataBytes.Length ?? 0;
        foreach (var (record, _) in _newTypes)
            recordLength += (record ?? throw new InvalidOperationException("Type record not set yet.")).Length;
        var total = Layer2Metadata.HeaderSize + 4L * stringCount + 12L * typeCount + stringLength + recordLength;
        if (total > Array.MaxLength) throw new InvalidOperationException("Layer2 metadata would be too large.");
        var bytes = new byte[total];
        var span = bytes.AsSpan();
        BinaryPrimitives.WriteUInt32LittleEndian(span, Layer2Metadata.Magic);
        BinaryPrimitives.WriteUInt32LittleEndian(span[4..], Layer2Metadata.FormatVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(span[16..], (uint)stringCount);
        BinaryPrimitives.WriteUInt32LittleEndian(span[20..], (uint)typeCount);
        var table = Layer2Metadata.HeaderSize;
        var data = table + 4 * stringCount + 12 * typeCount;
        uint end = 0;
        if (_basis != null)
        {
            _basis.StringEndsBytes.CopyTo(span[table..]);
            table += _basis.StringEndsBytes.Length;
            _basis.StringDataBytes.CopyTo(span[data..]);
            end = (uint)_basis.StringDataBytes.Length;
        }
        foreach (var value in _newStrings)
        {
            value.CopyTo(span[(data + (int)end)..]);
            end += (uint)value.Length;
            BinaryPrimitives.WriteUInt32LittleEndian(span[table..], end);
            table += 4;
        }
        data += (int)end;
        end = 0;
        if (_basis != null)
        {
            _basis.RecordEndsBytes.CopyTo(span[table..]);
            table += _basis.RecordEndsBytes.Length;
            _basis.RecordDataBytes.CopyTo(span[data..]);
            end = (uint)_basis.RecordDataBytes.Length;
        }
        foreach (var (record, _) in _newTypes)
        {
            record!.CopyTo(span[(data + (int)end)..]);
            end += (uint)record.Length;
            BinaryPrimitives.WriteUInt32LittleEndian(span[table..], end);
            table += 4;
        }
        var keys = new (uint Key, uint Index)[typeCount];
        for (var i = 0; i < BasisTypes; i++) keys[i] = _basis!.KeyEntry(i);
        for (var i = 0; i < _newTypes.Count; i++) keys[BasisTypes + i] = (_newTypes[i].Key, (uint)(BasisTypes + i));
        // The snapshot's entries are sorted already; sorting all of them keeps the writer simple and is cheap.
        Array.Sort(keys);
        foreach (var (key, index) in keys)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(span[table..], key);
            BinaryPrimitives.WriteUInt32LittleEndian(span[(table + 4)..], index);
            table += 8;
        }
        BinaryPrimitives.WriteUInt64LittleEndian(span[8..], XxHash64.Hash(span[16..]));
        return Layer2Metadata.FromBuilder(bytes, stringCount, typeCount, (int)stringLength, _bindingName);
    }
}

/// <summary>Varint writing in the encoding of <see cref="PackUnpack"/>.</summary>
static class Layer2Varint
{
    public static void WriteVUInt(this ArrayBufferWriter<byte> writer, ulong value)
    {
        var length = PackUnpack.LengthVUInt(value);
        PackUnpack.UnsafePackVUInt(ref MemoryMarshal.GetReference(writer.GetSpan((int)length)), value, length);
        writer.Advance((int)length);
    }

    public static void WriteVInt(this ArrayBufferWriter<byte> writer, long value)
    {
        var length = PackUnpack.LengthVInt(value);
        PackUnpack.UnsafePackVInt(ref MemoryMarshal.GetReference(writer.GetSpan((int)length)), value, length);
        writer.Advance((int)length);
    }

    public static void WriteByte(this ArrayBufferWriter<byte> writer, byte value)
    {
        writer.GetSpan(1)[0] = value;
        writer.Advance(1);
    }

    /// <summary>A name in canonical encodings: its UTF-8 bytes, length-prefixed.</summary>
    public static void WriteName(this ArrayBufferWriter<byte> writer, ReadOnlySpan<byte> utf8)
    {
        writer.WriteVUInt((ulong)utf8.Length);
        writer.Write(utf8);
    }

    public static void WriteName(this ArrayBufferWriter<byte> writer, string name)
    {
        var length = Encoding.UTF8.GetByteCount(name);
        writer.WriteVUInt((ulong)length);
        Encoding.UTF8.GetBytes(name, writer.GetSpan(length));
        writer.Advance(length);
    }
}

/// <summary>Bounds-checked varint reading of one record; anything malformed is <see cref="InvalidDataException"/>.</summary>
ref struct Layer2Reader(ReadOnlySpan<byte> data)
{
    readonly ReadOnlySpan<byte> _data = data;
    int _position;

    public int Position => _position;
    public bool AtEnd => _position == _data.Length;
    public int Remaining => _data.Length - _position;
    public ReadOnlySpan<byte> Consumed(int from) => _data[from.._position];

    public byte ReadByte()
    {
        if (_position >= _data.Length) throw Truncated();
        return _data[_position++];
    }

    public ulong ReadVUInt64()
    {
        if (_position >= _data.Length) throw Truncated();
        var length = PackUnpack.LengthVUIntByFirstByte(_data[_position]);
        if (_position + length > _data.Length) throw Truncated();
        var value = PackUnpack.UnsafeUnpackVUInt(ref MemoryMarshal.GetReference(_data[_position..]), length);
        _position += (int)length;
        return value;
    }

    public long ReadVInt64()
    {
        if (_position >= _data.Length) throw Truncated();
        var length = PackUnpack.LengthVIntByFirstByte(_data[_position]);
        if (_position + length > _data.Length) throw Truncated();
        var value = PackUnpack.UnsafeUnpackVInt(ref MemoryMarshal.GetReference(_data[_position..]), length);
        _position += (int)length;
        return value;
    }

    public int ReadCount()
    {
        var value = ReadVUInt64();
        // Every counted item takes at least one byte.
        if (value > (ulong)Remaining) throw new InvalidDataException("Layer2 metadata record has a wrong count.");
        return (int)value;
    }

    public int ReadId()
    {
        var value = ReadVUInt64();
        if (value > int.MaxValue) throw new InvalidDataException("Layer2 metadata record has a wrong ID.");
        return (int)value;
    }

    static InvalidDataException Truncated() => new("Layer2 metadata record is truncated.");
}
