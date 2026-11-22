using System;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Unicode;
using BTDB.Buffer;

namespace BTDB.EventStore2Layer;

/// <summary>
/// An immutable snapshot of a Layer2 metadata file: every type ever stored for one event stream, kept in its
/// serialized form and read in place. Writers only append, so type IDs (50 + type index) and string IDs never change.
/// </summary>
/// <remarks>
/// Layout, all fixed-width fields little-endian:
/// header (magic <c>BL2M</c>, format version, XxHash64 of bytes 16..end, string count S, type count T);
/// u32[S] string end offsets; u32[T] record end offsets; (u32 shape key, u32 type index)[T] sorted by key and index;
/// UTF-8 string bytes; record bytes. String 0 is the binding name, the name of the event stream the file belongs to.
/// </remarks>
public sealed class Layer2Metadata
{
    /// <summary>The ID of the first type stored in the file; lower IDs are null, back reference and built-in types.</summary>
    public const int FirstTypeId = 50;

    internal const uint Magic = 0x4D324C42; // "BL2M"
    internal const uint FormatVersion = 1;
    internal const int HeaderSize = 24;
    const int ChecksumStart = 16;

    readonly byte[] _bytes;
    readonly int _stringEnds, _recordEnds, _keyIndex, _stringBytes, _recordBytes;

    Layer2Metadata(byte[] bytes)
    {
        _bytes = bytes;
        var span = bytes.AsSpan();
        if (span.Length < HeaderSize) throw Corrupted("is shorter than its header");
        if (BinaryPrimitives.ReadUInt32LittleEndian(span) != Magic) throw Corrupted("is not Layer2 metadata");
        if (BinaryPrimitives.ReadUInt32LittleEndian(span[4..]) != FormatVersion)
            throw Corrupted("has an unsupported format version");
        if (BinaryPrimitives.ReadUInt64LittleEndian(span[8..]) != XxHash64.Hash(span[ChecksumStart..]))
            throw Corrupted("has a wrong checksum");
        var stringCount = BinaryPrimitives.ReadUInt32LittleEndian(span[16..]);
        var typeCount = BinaryPrimitives.ReadUInt32LittleEndian(span[20..]);
        var tables = HeaderSize + 4L * stringCount + 12L * typeCount;
        if (stringCount == 0 || tables > span.Length) throw Corrupted("has tables larger than the file");
        StringCount = (int)stringCount;
        TypeCount = (int)typeCount;
        _stringEnds = HeaderSize;
        _recordEnds = _stringEnds + 4 * StringCount;
        _keyIndex = _recordEnds + 4 * TypeCount;
        _stringBytes = _keyIndex + 8 * TypeCount;
        var stringLength = CheckEnds(span, _stringEnds, StringCount, false, "string");
        _recordBytes = _stringBytes + stringLength;
        var recordLength = CheckEnds(span, _recordEnds, TypeCount, true, "record");
        if ((long)_recordBytes + recordLength != span.Length) throw Corrupted("has a wrong length");
        if (!Utf8.IsValid(span.Slice(_stringBytes, stringLength))) throw Corrupted("has a string that is not UTF-8");
        for (var i = 0; i < TypeCount; i++)
        {
            var (key, index) = KeyEntry(i);
            if (index >= TypeCount) throw Corrupted("has a key index entry of a missing type");
            if (i > 0 && KeyEntry(i - 1).CompareTo((key, index)) >= 0) throw Corrupted("has an unsorted key index");
        }
        BindingName = String(0);
    }

    static int CheckEnds(ReadOnlySpan<byte> span, int offset, int count, bool nonEmpty, string what)
    {
        uint previous = 0;
        for (var i = 0; i < count; i++)
        {
            var end = BinaryPrimitives.ReadUInt32LittleEndian(span[(offset + 4 * i)..]);
            if (end < previous || nonEmpty && end == previous || end > span.Length)
                throw Corrupted($"has a wrong {what} end offset");
            previous = end;
        }
        return (int)previous;
    }

    static InvalidDataException Corrupted(string problem) => new("Layer2 metadata " + problem + ".");

    /// <summary>Metadata of a stream nothing was stored to yet.</summary>
    public static Layer2Metadata Create(string bindingName)
    {
        ArgumentException.ThrowIfNullOrEmpty(bindingName);
        return new Layer2MetadataBuilder(bindingName).Build();
    }

    /// <summary>Validates a file and wraps it without copying when it is a whole array; the memory must not change
    /// afterwards. Throws <see cref="InvalidDataException"/> for anything but a valid file.</summary>
    public static Layer2Metadata Load(ReadOnlyMemory<byte> file) =>
        new(MemoryMarshal.TryGetArray(file, out var segment) && segment.Offset == 0 &&
            segment.Count == segment.Array!.Length
                ? segment.Array
                : file.ToArray());

    internal static Layer2Metadata FromBuilder(byte[] bytes) => new(bytes);

    /// <summary>The whole file, as read or as it is to be written.</summary>
    public ReadOnlyMemory<byte> Bytes => _bytes;

    /// <summary>The name of the event stream this metadata belongs to.</summary>
    public string BindingName { get; }

    public int StringCount { get; }

    /// <summary>Number of stored types; their IDs are <see cref="FirstTypeId"/> up to <see cref="FirstTypeId"/> +
    /// TypeCount - 1.</summary>
    public int TypeCount { get; }

    /// <summary>True when this snapshot holds everything <paramref name="previous"/> holds, unchanged, as every
    /// write by an appending writer does.</summary>
    public bool Extends(Layer2Metadata previous)
    {
        ArgumentNullException.ThrowIfNull(previous);
        if (StringCount < previous.StringCount || TypeCount < previous.TypeCount) return false;
        var mine = _bytes.AsSpan();
        var theirs = previous._bytes.AsSpan();
        return mine.Slice(_stringEnds, 4 * previous.StringCount)
                   .SequenceEqual(theirs.Slice(previous._stringEnds, 4 * previous.StringCount)) &&
               mine.Slice(_recordEnds, 4 * previous.TypeCount)
                   .SequenceEqual(theirs.Slice(previous._recordEnds, 4 * previous.TypeCount)) &&
               mine.Slice(_stringBytes, previous._recordBytes - previous._stringBytes)
                   .SequenceEqual(theirs[previous._stringBytes..previous._recordBytes]) &&
               mine.Slice(_recordBytes, previous._bytes.Length - previous._recordBytes)
                   .SequenceEqual(theirs[previous._recordBytes..]);
    }

    public bool ContainsType(int typeId) => typeId >= FirstTypeId && typeId - FirstTypeId < TypeCount;

    uint End(int table, int index) => BinaryPrimitives.ReadUInt32LittleEndian(_bytes.AsSpan(table + 4 * index));

    internal ReadOnlySpan<byte> StringUtf8(int id)
    {
        var start = id == 0 ? 0 : (int)End(_stringEnds, id - 1);
        return _bytes.AsSpan(_stringBytes + start, (int)End(_stringEnds, id) - start);
    }

    internal string String(int id) => Encoding.UTF8.GetString(StringUtf8(id));

    internal ReadOnlySpan<byte> Record(int typeId)
    {
        var index = typeId - FirstTypeId;
        var start = index == 0 ? 0 : (int)End(_recordEnds, index - 1);
        return _bytes.AsSpan(_recordBytes + start, (int)End(_recordEnds, index) - start);
    }

    internal (uint Key, uint Index) KeyEntry(int position)
    {
        var entry = _bytes.AsSpan(_keyIndex + 8 * position, 8);
        return (BinaryPrimitives.ReadUInt32LittleEndian(entry), BinaryPrimitives.ReadUInt32LittleEndian(entry[4..]));
    }

    /// <summary>The first key index position whose key is not less than <paramref name="key"/>.</summary>
    internal int LowerBound(uint key)
    {
        int low = 0, high = TypeCount;
        while (low < high)
        {
            var middle = (low + high) >>> 1;
            if (KeyEntry(middle).Key < key) low = middle + 1;
            else high = middle;
        }
        return low;
    }
}
