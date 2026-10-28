using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using BTDB.StreamLayer;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace BTDBTest;

/// Property style tests: every MemWriter.WriteX must round-trip through every MemReader implementation,
/// including Skip methods, current positions and buffer boundaries splitting values.
public class MemReaderWriterRoundTripTest
{
    public delegate void WriteAction(ref MemWriter writer);

    public delegate void ReadAction(ref MemReader reader);

    public delegate void WriteValue<in T>(ref MemWriter writer, T value);

    public delegate T ReadValue<out T>(ref MemReader reader);

    public sealed record Item(string Name, WriteAction Write, ReadAction Read, ReadAction Skip)
    {
        public override string ToString() => Name;
    }

    static Item I<T>(string name, T value, WriteValue<T> write, ReadValue<T> read, ReadAction skip,
        Action<T, T>? assertEqual = null)
    {
        return new(name + ":" + Describe(value), (ref MemWriter w) => write(ref w, value), (ref MemReader r) =>
        {
            var actual = read(ref r);
            if (assertEqual != null) assertEqual(value, actual);
            else Assert.Equal(value, actual);
        }, skip);
    }

    static string Describe<T>(T value)
    {
        var s = value?.ToString() ?? "null";
        return s.Length > 20 ? s[..20] + "..." + s.Length : s;
    }

    internal static readonly ulong[] VUInt64Boundaries = CreateVUInt64Boundaries();
    internal static readonly long[] VInt64Boundaries = CreateVInt64Boundaries();

    static ulong[] CreateVUInt64Boundaries()
    {
        var res = new List<ulong> { 0, 1, ulong.MaxValue, ulong.MaxValue - 1 };
        for (var bit = 1; bit < 64; bit++)
        {
            res.Add((1ul << bit) - 1);
            res.Add(1ul << bit);
            res.Add((1ul << bit) + 1);
        }

        return res.Distinct().ToArray();
    }

    static long[] CreateVInt64Boundaries()
    {
        var res = new List<long> { 0, 1, -1, long.MaxValue, long.MinValue, long.MinValue + 1, long.MaxValue - 1 };
        for (var bit = 1; bit < 63; bit++)
        {
            res.Add((1L << bit) - 1);
            res.Add(1L << bit);
            res.Add(-(1L << bit));
            res.Add(-(1L << bit) - 1);
            res.Add(-(1L << bit) + 1);
        }

        return res.Distinct().ToArray();
    }

    internal static readonly string?[] Strings =
    [
        null, "", "a", "ab", "abc", "abcd", "abcdefgh", "\0", "\u007f", "\u0080", "߿", "ࠀ", "￿",
        "Žluťoučký kůň úpěl ďábelské ódy", "😀", "a😀b", "😀😀😀😀", "\ud800", "\udc00", "\ud800x", "x\udbff",
        "\udc00\ud800", new string('x', 31), new string('y', 32) + "€", new string('z', 33),
        new string('q', 100) + "\u007f" + new string('r', 50),
        "0123456789abcdef0123456789ABCDEF0123456789abcdefĀtail",
        string.Concat(Enumerable.Range(0, 300).Select(i => (char)(i * 7 % 0x2000))),
        new string('L', 9000) + "end€"
    ];

    static bool IsWellFormedUtf16(string s)
    {
        for (var i = 0; i < s.Length; i++)
        {
            if (char.IsHighSurrogate(s[i]) && i + 1 < s.Length && char.IsLowSurrogate(s[i + 1])) i++;
            else if (char.IsSurrogate(s[i])) return false;
        }

        return true;
    }

    static IEnumerable<string> Utf8Strings => Strings.Where(s => s != null && IsWellFormedUtf16(s))!;

    public static List<Item> CreateItems()
    {
        var items = new List<Item>();
        foreach (var v in new byte[] { 0, 1, 127, 128, 255 })
        {
            items.Add(I("UInt8", v, (ref MemWriter w, byte x) => w.WriteUInt8(x), (ref MemReader r) => r.ReadUInt8(),
                (ref MemReader r) => r.Skip1Byte()));
        }

        foreach (var v in new sbyte[] { 0, 1, -1, sbyte.MinValue, sbyte.MaxValue })
        {
            items.Add(I("Int8", v, (ref MemWriter w, sbyte x) => w.WriteInt8(x), (ref MemReader r) => r.ReadInt8(),
                (ref MemReader r) => r.Skip1Byte()));
            items.Add(I("Int8Ordered", v, (ref MemWriter w, sbyte x) => w.WriteInt8Ordered(x),
                (ref MemReader r) => r.ReadInt8Ordered(), (ref MemReader r) => r.Skip1Byte()));
        }

        items.Add(I("Bool", true, (ref MemWriter w, bool x) => w.WriteBool(x), (ref MemReader r) => r.ReadBool(),
            (ref MemReader r) => r.Skip1Byte()));
        items.Add(I("Bool", false, (ref MemWriter w, bool x) => w.WriteBool(x), (ref MemReader r) => r.ReadBool(),
            (ref MemReader r) => r.Skip1Byte()));
        items.Add(I("ByteZero", (byte)0, (ref MemWriter w, byte _) => w.WriteByteZero(),
            (ref MemReader r) => r.PeekUInt8() == 0 ? r.ReadUInt8() : throw new InvalidDataException(),
            (ref MemReader r) => r.Skip1Byte()));

        foreach (var v in new ushort[] { 0, 1, 0x1234, 0x8000, ushort.MaxValue })
        {
            items.Add(I("UInt16LE", v, (ref MemWriter w, ushort x) => w.WriteUInt16LE(x),
                (ref MemReader r) => r.ReadUInt16LE(), (ref MemReader r) => r.Skip2Bytes()));
            items.Add(I("UInt16LE-as-Int16LE", (short)v, (ref MemWriter w, short x) => w.WriteUInt16LE((ushort)x),
                (ref MemReader r) => r.ReadInt16LE(), (ref MemReader r) => r.Skip2Bytes()));
        }

        foreach (var v in new short[] { 0, 1, -1, 0x1234, short.MinValue, short.MaxValue })
        {
            items.Add(I("Int16BE", v, (ref MemWriter w, short x) => w.WriteInt16BE(x),
                (ref MemReader r) => r.ReadInt16BE(), (ref MemReader r) => r.Skip2Bytes()));
            items.Add(I("Int16BE-as-UInt16BE", (ushort)v, (ref MemWriter w, ushort x) => w.WriteInt16BE((short)x),
                (ref MemReader r) => r.ReadUInt16BE(), (ref MemReader r) => r.Skip2Bytes()));
        }

        foreach (var v in new[] { Half.Zero, Half.One, Half.NegativeOne, Half.MaxValue, Half.MinValue, Half.Epsilon,
                     Half.NaN, Half.PositiveInfinity, (Half)(-0.0) })
        {
            items.Add(I("Half", v, (ref MemWriter w, Half x) => w.WriteHalf(x), (ref MemReader r) => r.ReadHalf(),
                (ref MemReader r) => r.Skip2Bytes(),
                (e, a) => Assert.Equal(BitConverter.HalfToUInt16Bits(e), BitConverter.HalfToUInt16Bits(a))));
        }

        foreach (var v in new[] { 0, 1, -1, 0x12345678, int.MinValue, int.MaxValue })
        {
            items.Add(I("Int32LE", v, (ref MemWriter w, int x) => w.WriteInt32LE(x),
                (ref MemReader r) => r.ReadInt32LE(), (ref MemReader r) => r.Skip4Bytes()));
            items.Add(I("Int32BE", v, (ref MemWriter w, int x) => w.WriteInt32BE(x),
                (ref MemReader r) => r.ReadInt32BE(), (ref MemReader r) => r.Skip4Bytes()));
            items.Add(I("UInt32LE", (uint)v, (ref MemWriter w, uint x) => w.WriteUInt32LE(x),
                (ref MemReader r) => r.ReadUInt32LE(), (ref MemReader r) => r.Skip4Bytes()));
            items.Add(I("Int32BE-as-UInt32BE", (uint)v, (ref MemWriter w, uint x) => w.WriteInt32BE((int)x),
                (ref MemReader r) => r.ReadUInt32BE(), (ref MemReader r) => r.Skip4Bytes()));
        }

        foreach (var v in new[] { 0f, -0f, 1.5f, float.NaN, float.Epsilon, float.MaxValue, float.NegativeInfinity })
        {
            items.Add(I("Single", v, (ref MemWriter w, float x) => w.WriteSingle(x),
                (ref MemReader r) => r.ReadSingle(), (ref MemReader r) => r.Skip4Bytes(),
                (e, a) => Assert.Equal(BitConverter.SingleToUInt32Bits(e), BitConverter.SingleToUInt32Bits(a))));
        }

        foreach (var v in new[] { 0L, 1, -1, 0x123456789abcdef0, long.MinValue, long.MaxValue })
        {
            items.Add(I("Int64LE", v, (ref MemWriter w, long x) => w.WriteInt64LE(x),
                (ref MemReader r) => r.ReadInt64LE(), (ref MemReader r) => r.Skip8Bytes()));
            items.Add(I("Int64BE", v, (ref MemWriter w, long x) => w.WriteInt64BE(x),
                (ref MemReader r) => r.ReadInt64BE(), (ref MemReader r) => r.Skip8Bytes()));
            items.Add(I("UInt64LE", (ulong)v, (ref MemWriter w, ulong x) => w.WriteUInt64LE(x),
                (ref MemReader r) => r.ReadUInt64LE(), (ref MemReader r) => r.Skip8Bytes()));
            items.Add(I("UInt64BE", (ulong)v, (ref MemWriter w, ulong x) => w.WriteUInt64BE(x),
                (ref MemReader r) => r.ReadUInt64BE(), (ref MemReader r) => r.Skip8Bytes()));
            items.Add(I("Pointer", (nint)v, (ref MemWriter w, nint x) => w.WritePointer(x),
                (ref MemReader r) => r.ReadPointer(), (ref MemReader r) => r.Skip8Bytes()));
        }

        foreach (var v in new[] { 0d, -0d, 1.5, double.NaN, double.Epsilon, double.MaxValue, double.MinValue,
                     double.PositiveInfinity, -1e-300 })
        {
            items.Add(I("Double", v, (ref MemWriter w, double x) => w.WriteDouble(x),
                (ref MemReader r) => r.ReadDouble(), (ref MemReader r) => r.Skip8Bytes(),
                (e, a) => Assert.Equal(BitConverter.DoubleToUInt64Bits(e), BitConverter.DoubleToUInt64Bits(a))));
            items.Add(I("DoubleOrdered", v, (ref MemWriter w, double x) => w.WriteDoubleOrdered(x),
                (ref MemReader r) => r.ReadDoubleOrdered(), (ref MemReader r) => r.Skip8Bytes(),
                (e, a) => Assert.Equal(BitConverter.DoubleToUInt64Bits(e), BitConverter.DoubleToUInt64Bits(a))));
        }

        foreach (var v in VUInt64Boundaries)
        {
            items.Add(I("VUInt64", v, (ref MemWriter w, ulong x) => w.WriteVUInt64(x),
                (ref MemReader r) => r.ReadVUInt64(), (ref MemReader r) => r.SkipVUInt64()));
            items.Add(I("VUInt64-copied", v, (ref MemWriter w, ulong x) => w.WriteVUInt64(x),
                (ref MemReader r) =>
                {
                    var copy = new MemWriter();
                    r.CopyVUInt64ToWriter(ref copy);
                    var copyReader = MemReader.CreateFromPinnedSpan(copy.GetSpan());
                    var res = copyReader.ReadVUInt64();
                    Assert.True(copyReader.Eof);
                    return res;
                }, (ref MemReader r) => r.SkipVUInt64()));
            if (v <= uint.MaxValue)
            {
                items.Add(I("VUInt32", (uint)v, (ref MemWriter w, uint x) => w.WriteVUInt32(x),
                    (ref MemReader r) => r.ReadVUInt32(), (ref MemReader r) => r.SkipVUInt32()));
            }

            if (v <= ushort.MaxValue)
            {
                items.Add(I("VUInt16", (ushort)v, (ref MemWriter w, ushort x) => w.WriteVUInt16(x),
                    (ref MemReader r) => r.ReadVUInt16(), (ref MemReader r) => r.SkipVUInt16()));
            }
        }

        foreach (var v in VInt64Boundaries)
        {
            items.Add(I("VInt64", v, (ref MemWriter w, long x) => w.WriteVInt64(x),
                (ref MemReader r) => r.ReadVInt64(), (ref MemReader r) => r.SkipVInt64()));
            items.Add(I("VInt64-copied", v, (ref MemWriter w, long x) => w.WriteVInt64(x),
                (ref MemReader r) =>
                {
                    var copy = new MemWriter();
                    r.CopyVInt64ToWriter(ref copy);
                    var copyReader = MemReader.CreateFromPinnedSpan(copy.GetSpan());
                    var res = copyReader.ReadVInt64();
                    Assert.True(copyReader.Eof);
                    return res;
                }, (ref MemReader r) => r.SkipVInt64()));
            if (v is >= int.MinValue and <= int.MaxValue)
            {
                items.Add(I("VInt32", (int)v, (ref MemWriter w, int x) => w.WriteVInt32(x),
                    (ref MemReader r) => r.ReadVInt32(), (ref MemReader r) => r.SkipVInt32()));
                items.Add(I("TimeSpan", TimeSpan.FromTicks(v), (ref MemWriter w, TimeSpan x) => w.WriteTimeSpan(x),
                    (ref MemReader r) => r.ReadTimeSpan(), (ref MemReader r) => r.SkipVInt64()));
            }

            if (v is >= short.MinValue and <= short.MaxValue)
            {
                items.Add(I("VInt16", (short)v, (ref MemWriter w, short x) => w.WriteVInt16(x),
                    (ref MemReader r) => r.ReadVInt16(), (ref MemReader r) => r.SkipVInt16()));
            }
        }

        foreach (var v in new[] { DateTime.MinValue, DateTime.MaxValue, new DateTime(2024, 2, 29, 1, 2, 3, DateTimeKind.Utc),
                     new DateTime(1999, 12, 31, 23, 59, 59, DateTimeKind.Unspecified) })
        {
            items.Add(I("DateTime", v, (ref MemWriter w, DateTime x) => w.WriteDateTime(x),
                (ref MemReader r) => r.ReadDateTime(), (ref MemReader r) => r.Skip8Bytes(),
                (e, a) =>
                {
                    Assert.Equal(e, a);
                    Assert.Equal(e.Kind, a.Kind);
                }));
        }

        foreach (var v in new[] { DateTimeOffset.MinValue, DateTimeOffset.MaxValue,
                     new DateTimeOffset(2020, 1, 2, 3, 4, 5, TimeSpan.FromHours(2)),
                     new DateTimeOffset(2020, 1, 2, 3, 4, 5, TimeSpan.FromMinutes(-570)) })
        {
            items.Add(I("DateTimeOffset", v, (ref MemWriter w, DateTimeOffset x) => w.WriteDateTimeOffset(x),
                (ref MemReader r) => r.ReadDateTimeOffset(), (ref MemReader r) => r.SkipDateTimeOffset(),
                (e, a) =>
                {
                    Assert.Equal(e, a);
                    Assert.Equal(e.Offset, a.Offset);
                }));
        }

        foreach (var v in Strings)
        {
            items.Add(I("String", v, (ref MemWriter w, string? x) => w.WriteString(x),
                (ref MemReader r) => r.ReadString(), (ref MemReader r) => r.SkipString()));
            items.Add(I("StringOrdered", v, (ref MemWriter w, string? x) => w.WriteStringOrdered(x),
                (ref MemReader r) => r.ReadStringOrdered(), (ref MemReader r) => r.SkipStringOrdered()));
            items.Add(I("StringOrderedAsSpan", v ?? "", (ref MemWriter w, string x) => w.WriteStringOrdered(x),
                (ref MemReader r) =>
                {
                    Span<char> buf = stackalloc char[4];
                    return r.ReadStringOrderedAsSpan(ref MemoryMarshal.GetReference(buf), buf.Length).ToString();
                }, (ref MemReader r) => r.SkipStringOrdered()));
            if (v != null)
            {
                items.Add(I("String-copied-to-ordered", v, (ref MemWriter w, string x) => w.WriteString(x),
                    (ref MemReader r) =>
                    {
                        var copy = new MemWriter();
                        r.CopyStringToOrdered(ref copy);
                        var expected = new MemWriter();
                        expected.WriteStringOrdered(v);
                        Assert.Equal(expected.GetSpan().ToArray(), copy.GetSpan().ToArray());
                        var copyReader = MemReader.CreateFromPinnedSpan(copy.GetSpan());
                        return copyReader.ReadStringOrdered()!;
                    }, (ref MemReader r) => r.SkipString()));
            }
        }

        foreach (var v in Utf8Strings)
        {
            items.Add(I("StringInUtf8", v, (ref MemWriter w, string x) => w.WriteStringInUtf8(x),
                (ref MemReader r) => r.ReadStringInUtf8(), (ref MemReader r) => r.SkipStringInUtf8()));
        }

        items.Add(I("StringValues", StringValues.Empty, (ref MemWriter w, StringValues x) => w.WriteStringValues(x),
            (ref MemReader r) => r.ReadStringValues(), (ref MemReader r) => r.SkipStringValues(),
            (e, a) => Assert.Equal(e.ToArray(), a.ToArray())));
        items.Add(I("StringValues", new StringValues("single"),
            (ref MemWriter w, StringValues x) => w.WriteStringValues(x), (ref MemReader r) => r.ReadStringValues(),
            (ref MemReader r) => r.SkipStringValues(), (e, a) => Assert.Equal(e.ToArray(), a.ToArray())));
        items.Add(I("StringValues", new StringValues(["a", null, "", "Žluť😀"]),
            (ref MemWriter w, StringValues x) => w.WriteStringValues(x), (ref MemReader r) => r.ReadStringValues(),
            (ref MemReader r) => r.SkipStringValues(), (e, a) => Assert.Equal(e.ToArray(), a.ToArray())));

        foreach (var v in new[] { null, Array.Empty<byte>(), [42], Enumerable.Range(0, 300).Select(i => (byte)i).ToArray() })
        {
            items.Add(I("ByteArray", v, (ref MemWriter w, byte[]? x) => w.WriteByteArray(x),
                (ref MemReader r) => r.ReadByteArray(), (ref MemReader r) => r.SkipByteArray()));
            var nonNull = v ?? [];
            items.Add(I("ByteArrayAsSpan", nonNull, (ref MemWriter w, byte[] x) => w.WriteByteArray(x.AsSpan()),
                (ref MemReader r) => r.ReadByteArrayAsSpan().ToArray(), (ref MemReader r) => r.SkipByteArray()));
            items.Add(I("ByteArrayAsMemory", nonNull, (ref MemWriter w, byte[] x) => w.WriteByteArray((ReadOnlyMemory<byte>)x),
                (ref MemReader r) => r.ReadByteArrayAsMemory().ToArray(), (ref MemReader r) => r.SkipByteArray()));
            if (nonNull.Length > 0)
            {
                items.Add(I("Block", nonNull, (ref MemWriter w, byte[] x) => w.WriteBlock(x),
                    (ref MemReader r) => r.ReadByteArrayRaw(nonNull.Length),
                    (ref MemReader r) => r.SkipBlock((uint)nonNull.Length)));
                items.Add(I("BlockAsSpan", nonNull, (ref MemWriter w, byte[] x) => w.WriteByteArrayRaw(x),
                    (ref MemReader r) => r.ReadBlockAsSpan((uint)nonNull.Length).ToArray(),
                    (ref MemReader r) => r.SkipBlock((uint)nonNull.Length)));
                items.Add(I("ByteArrayLengthAndBlock", nonNull, (ref MemWriter w, byte[] x) =>
                    {
                        w.WriteByteArrayLength(x);
                        w.WriteBlock((ReadOnlyMemory<byte>)x);
                    }, (ref MemReader r) => r.ReadByteArray()!,
                    (ref MemReader r) => r.SkipByteArray()));
            }
        }

        foreach (var v in new[] { Guid.Empty, new Guid("0123456789abcdef0123456789abcdef"), new Guid(Enumerable.Repeat((byte)0xff, 16).ToArray()) })
        {
            items.Add(I("Guid", v, (ref MemWriter w, Guid x) => w.WriteGuid(x), (ref MemReader r) => r.ReadGuid(),
                (ref MemReader r) => r.SkipGuid()));
        }

        foreach (var v in new[] { 0m, 1m, -1m, 1.5m, -0.001m, decimal.MaxValue, decimal.MinValue,
                     0.0000000000000000000000000001m, new decimal(0, 0, 1, false, 0), new decimal(0, 0, 5, true, 3),
                     new decimal(-1, -1, 0x0fffffff, false, 10), new decimal(1, 2, 0x10000000, false, 28),
                     new decimal(-1, -1, 0, false, 5), new decimal(0, 0, 0, true, 7) })
        {
            items.Add(I("Decimal", v, (ref MemWriter w, decimal x) => w.WriteDecimal(x),
                (ref MemReader r) => r.ReadDecimal(), (ref MemReader r) => r.SkipDecimal(),
                (e, a) => Assert.Equal(decimal.GetBits(e), decimal.GetBits(a))));
        }

        foreach (var v in new[] { null, IPAddress.Loopback, IPAddress.Parse("192.168.1.254"), IPAddress.IPv6Loopback,
                     IPAddress.Parse("fe80::1234:5678%5"), IPAddress.Parse("2001:db8::ff00:42:8329") })
        {
            items.Add(I("IPAddress", v, (ref MemWriter w, IPAddress? x) => w.WriteIPAddress(x),
                (ref MemReader r) => r.ReadIPAddress(), (ref MemReader r) => r.SkipIPAddress(),
                (e, a) =>
                {
                    Assert.Equal(e, a);
                    if (e != null && e.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
                        Assert.Equal(e.ScopeId, a!.ScopeId);
                }));
        }

        foreach (var v in new[] { null, new Version(0, 0), new Version(1, 2), new Version(1, 2, 3), new Version(1, 2, 3, 4),
                     new Version(int.MaxValue - 1, 1000, 0, 70000) })
        {
            items.Add(I("Version", v, (ref MemWriter w, Version? x) => w.WriteVersion(x),
                (ref MemReader r) => r.ReadVersion(), (ref MemReader r) => r.SkipVersion()));
        }

        return items;
    }

    static readonly List<Item> AllItems = CreateItems();

    static (byte[] Bytes, long[] Offsets) WriteAll(IReadOnlyList<Item> items)
    {
        var writer = new MemWriter();
        var offsets = new long[items.Count + 1];
        for (var i = 0; i < items.Count; i++)
        {
            offsets[i] = writer.GetCurrentPosition();
            items[i].Write(ref writer);
        }

        offsets[items.Count] = writer.GetCurrentPosition();
        return (writer.GetSpan().ToArray(), offsets);
    }

    static readonly (byte[] Bytes, long[] Offsets) Reference = WriteAll(AllItems);

    public sealed class ChunkedMemWriter : IMemWriter
    {
        readonly byte[] _buf;
        readonly List<byte> _out = new();

        public ChunkedMemWriter(int bufferSize)
        {
            _buf = GC.AllocateArray<byte>(bufferSize, pinned: true);
        }

        public int FlushCount { get; private set; }

        public unsafe void Init(ref MemWriter memWriter)
        {
            memWriter.Start = (nint)Unsafe.AsPointer(ref _buf[0]);
            memWriter.Current = memWriter.Start;
            memWriter.End = memWriter.Start + _buf.Length;
        }

        public unsafe void Flush(ref MemWriter memWriter, uint spaceNeeded)
        {
            FlushCount++;
            var len = (int)(memWriter.Current - memWriter.Start);
            _out.AddRange(new ReadOnlySpan<byte>((void*)memWriter.Start, len));
            Init(ref memWriter);
        }

        public long GetCurrentPosition(in MemWriter memWriter)
        {
            return _out.Count + (memWriter.Current - memWriter.Start);
        }

        public void WriteBlock(ref MemWriter memWriter, ref byte buffer, nuint length)
        {
            Flush(ref memWriter, 0);
            _out.AddRange(MemoryMarshal.CreateReadOnlySpan(ref buffer, (int)length));
        }

        public void SetCurrentPosition(ref MemWriter memWriter, long position)
        {
            throw new NotSupportedException();
        }

        public byte[] GetBytes(ref MemWriter memWriter)
        {
            Flush(ref memWriter, 0);
            return _out.ToArray();
        }
    }

    sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var res = new Segment(memory, RunningIndex + Memory.Length);
            Next = res;
            return res;
        }
    }

    public static ReadOnlySequence<byte> CreateSequence(byte[] data, Func<int, int> chunkSize, bool insertEmpty = false)
    {
        if (data.Length == 0) return ReadOnlySequence<byte>.Empty;
        var pos = 0;
        var index = 0;
        Segment? first = null;
        Segment? last = null;
        while (pos < data.Length)
        {
            var len = Math.Min(Math.Max(1, chunkSize(index++)), data.Length - pos);
            // each chunk in its own array to make sure nothing reads past chunk boundaries
            var chunk = data.AsMemory(pos, len).ToArray();
            if (first == null)
            {
                first = last = new(chunk, 0);
            }
            else
            {
                last = last!.Append(chunk);
            }

            if (insertEmpty && index % 3 == 0) last = last.Append(ReadOnlyMemory<byte>.Empty);
            pos += len;
        }

        return new(first!, 0, last!, last!.Memory.Length);
    }

    public delegate void ReaderUser(ref MemReader reader);

    public static IEnumerable<string> ReaderKinds =>
    [
        "PinnedSpan", "ReadOnlyMemory", "PinnedArray", "Sequence1", "Sequence3", "Sequence7Empty", "SequenceRandom",
        "PositionLess1", "PositionLess5", "PositionLess64", "PositionLessDefault", "MemoryMapped", "Brotli",
        "FileCollection1", "FileCollection7", "FileCollectionDefault"
    ];

    sealed class ArrayFile(byte[] data) : BTDB.KVDBLayer.IFileCollectionFile
    {
        public uint Index => 1;
        public ulong GetSize() => (ulong)data.Length;
        public void RandomRead(Span<byte> target, ulong position, bool doNotCache) =>
            data.AsSpan((int)position, target.Length).CopyTo(target);
        public IMemReader GetExclusiveReader() => throw new NotSupportedException();
        public void AdvisePrefetch() => throw new NotSupportedException();
        public IMemWriter GetAppenderWriter() => throw new NotSupportedException();
        public IMemWriter GetExclusiveAppenderWriter() => throw new NotSupportedException();
        public void HardFlush() => throw new NotSupportedException();
        public void HardFlushTruncateSwitchToReadOnlyMode() => throw new NotSupportedException();
        public void HardFlushTruncateSwitchToDisposedMode() => throw new NotSupportedException();
        public void Remove() => throw new NotSupportedException();
    }

    public static TheoryData<string> ReaderKindsData() => new(ReaderKinds);

    internal static unsafe void WithReader(string kind, byte[] data, ReaderUser use)
    {
        switch (kind)
        {
            case "PinnedSpan":
                fixed (byte* ptr = data)
                {
                    var reader = MemReader.CreateFromPinnedSpan(new ReadOnlySpan<byte>(ptr, data.Length));
                    use(ref reader);
                }

                break;
            case "ReadOnlyMemory":
            {
                var reader = MemReader.CreateFromReadOnlyMemory(data);
                try
                {
                    use(ref reader);
                }
                finally
                {
                    reader.Dispose();
                }

                break;
            }
            case "PinnedArray":
            {
                var pinned = GC.AllocateArray<byte>(data.Length + 3, pinned: true);
                data.CopyTo(pinned, 3);
                var reader = MemReader.CreateFromPinnedArray(pinned, 3, data.Length);
                use(ref reader);
                break;
            }
            case "Sequence1":
            case "Sequence3":
            case "Sequence7Empty":
            case "SequenceRandom":
            {
                var rnd = new Random(42);
                var sequence = kind switch
                {
                    "Sequence1" => CreateSequence(data, _ => 1),
                    "Sequence3" => CreateSequence(data, _ => 3),
                    "Sequence7Empty" => CreateSequence(data, _ => 7, true),
                    _ => CreateSequence(data, _ => rnd.Next(1, 40))
                };
                using var controller = new ReadOnlySequenceMemReader(sequence);
                var reader = new MemReader(controller);
                use(ref reader);
                break;
            }
            case "PositionLess1":
            case "PositionLess5":
            case "PositionLess64":
            case "PositionLessDefault":
            {
                var stream = new MemoryPositionLessStream();
                stream.Write(data, 0);
                var controller = kind switch
                {
                    "PositionLess1" => new PositionLessStreamReader(stream, 1),
                    "PositionLess5" => new PositionLessStreamReader(stream, 5),
                    "PositionLess64" => new PositionLessStreamReader(stream, 64),
                    _ => new PositionLessStreamReader(stream)
                };
                var reader = new MemReader(controller);
                use(ref reader);
                break;
            }
            case "FileCollection1":
            case "FileCollection7":
            case "FileCollectionDefault":
            {
                var file = new ArrayFile(data);
                var controller = kind switch
                {
                    "FileCollection1" => new FileCollectionFileReader(file, bufferSize: 1),
                    "FileCollection7" => new FileCollectionFileReader(file, bufferSize: 7),
                    _ => new FileCollectionFileReader(file)
                };
                var reader = new MemReader(controller);
                use(ref reader);
                break;
            }
            case "MemoryMapped":
            {
                var fileName = Path.GetTempFileName();
                try
                {
                    File.WriteAllBytes(fileName, data);
                    using var controller = new MemoryMappedMemReader(fileName);
                    var reader = new MemReader(controller);
                    use(ref reader);
                }
                finally
                {
                    File.Delete(fileName);
                }

                break;
            }
            case "Brotli":
            {
                var compressed = BrotliCompress(data);
                // trailing marker checks that decompression does not consume more than it should
                var source = MemReader.CreateFromReadOnlyMemory(compressed.Concat(new byte[] { 0xAB, 0xCD }).ToArray());
                var controller = new BrotliDecompressMemReader(source);
                var reader = new MemReader(controller);
                use(ref reader);
                source = controller.Finish(reader);
                Assert.Equal(0xAB, source.ReadUInt8());
                Assert.Equal(0xCD, source.ReadUInt8());
                Assert.True(source.Eof);
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    public static byte[] BrotliCompress(ReadOnlySpan<byte> data, int quality = 5, int windowBits = 22)
    {
        var outer = new MemWriter();
        var compressor = new BrotliCompressMemWriter(outer, quality, windowBits);
        var writer = new MemWriter(compressor);
        writer.WriteBlock(data);
        outer = compressor.Finish(writer);
        return outer.GetSpan().ToArray();
    }

    static void AssertReadAll(ref MemReader reader, IReadOnlyList<Item> items, long[] offsets, Func<int, bool> skip)
    {
        for (var i = 0; i < items.Count; i++)
        {
            Assert.False(reader.Eof, items[i].Name);
            Assert.True(offsets[i] == reader.GetCurrentPosition(),
                $"Position before {i}:{items[i].Name} expected {offsets[i]} actual {reader.GetCurrentPosition()}");
            if (skip(i)) items[i].Skip(ref reader);
            else items[i].Read(ref reader);
        }

        Assert.Equal(offsets[items.Count], reader.GetCurrentPosition());
        Assert.True(reader.Eof);
    }

    [Theory]
    [MemberData(nameof(ReaderKindsData))]
    public void AllItemsCanBeReadByAllReaders(string readerKind)
    {
        WithReader(readerKind, Reference.Bytes,
            (ref MemReader reader) => AssertReadAll(ref reader, AllItems, Reference.Offsets, _ => false));
    }

    public delegate void ReadDeclared(ref MemReader reader);

    /// A string, byte array or string list declaring far more than the input holds fails at the end of the input,
    /// allocating for what was there rather than for the declared length.
    [Theory]
    [MemberData(nameof(ReaderKindsData))]
    public void LengthsBeyondTheInputFailWithoutAllocatingThem(string readerKind)
    {
        foreach (var (declared, read) in new (ulong, ReadDeclared)[]
                 {
                     (1ul << 28, (ref MemReader r) => r.ReadString()),
                     (1ul << 28, (ref MemReader r) => r.ReadByteArray()),
                     (uint.MaxValue, (ref MemReader r) => r.ReadStringValues())
                 })
        {
            var writer = new MemWriter();
            writer.WriteVUInt64(declared);
            writer.WriteBlock(Enumerable.Repeat((byte)'a', 100).ToArray());
            var bytes = writer.GetSpan().ToArray();
            WithReader(readerKind, bytes, (ref MemReader reader) =>
            {
                var before = GC.GetAllocatedBytesForCurrentThread();
                try
                {
                    read(ref reader);
                    Assert.Fail("Read past the end of the input.");
                }
                catch (EndOfStreamException)
                {
                }

                Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 1 << 20);
            });
        }
    }

    [Theory]
    [MemberData(nameof(ReaderKindsData))]
    public void AllItemsCanBeSkippedByAllReaders(string readerKind)
    {
        WithReader(readerKind, Reference.Bytes,
            (ref MemReader reader) => AssertReadAll(ref reader, AllItems, Reference.Offsets, _ => true));
    }

    [Theory]
    [MemberData(nameof(ReaderKindsData))]
    public void AlternateSkipAndReadByAllReaders(string readerKind)
    {
        WithReader(readerKind, Reference.Bytes,
            (ref MemReader reader) => AssertReadAll(ref reader, AllItems, Reference.Offsets, i => i % 2 == 0));
        WithReader(readerKind, Reference.Bytes,
            (ref MemReader reader) => AssertReadAll(ref reader, AllItems, Reference.Offsets, i => i % 3 != 1));
    }

    [Theory]
    [MemberData(nameof(ReaderKindsData))]
    public void ReadingPastEndThrowsEndOfStream(string readerKind)
    {
        var data = new byte[] { 1, 2, 3 };
        WithReader(readerKind, data, (ref MemReader reader) =>
        {
            Assert.Equal(0x030201u, reader.ReadUInt16LE() + ((uint)reader.ReadUInt8() << 16));
            Assert.True(reader.Eof);
        });
        WithReader(readerKind, data, (ref MemReader reader) =>
        {
            var r = reader;
            Assert.Throws<EndOfStreamException>(() => ReadUInt32(r));
        });
        WithReader(readerKind, data, (ref MemReader reader) =>
        {
            reader.Skip2Bytes();
            var r = reader;
            Assert.Throws<EndOfStreamException>(() => SkipBlock(r, 2));
        });
        WithReader(readerKind, data, (ref MemReader reader) =>
        {
            reader.SkipBlock(3);
            var r = reader;
            Assert.Throws<EndOfStreamException>(() => ReadUInt8(r));
        });
        WithReader(readerKind, data, (ref MemReader reader) =>
        {
            var r = reader;
            Assert.Throws<EndOfStreamException>(() => ReadBlock(r, 10));
        });
        WithReader(readerKind, data, (ref MemReader reader) =>
        {
            var r = reader;
            Assert.Throws<EndOfStreamException>(() => ReadGuid(r));
        });
    }

    static uint ReadUInt32(MemReader reader) => reader.ReadUInt32LE();
    static byte ReadUInt8(MemReader reader) => reader.ReadUInt8();
    static Guid ReadGuid(MemReader reader) => reader.ReadGuid();
    static void SkipBlock(MemReader reader, uint len) => reader.SkipBlock(len);
    static byte[] ReadBlock(MemReader reader, int len) => reader.ReadByteArrayRaw(len);

    public delegate (MemWriter Writer, Func<MemWriter, byte[]> Finish) WriterFactory();

    public static TheoryData<string> WriterKindsData() =>
        new("Default", "Stack1", "Chunked5", "Chunked7", "Chunked64", "PositionLess", "PositionLessAtEnd", "File",
            "Brotli");

    static (MemWriter, Func<MemWriter, byte[]>) CreateWriter(string kind)
    {
        switch (kind)
        {
            case "Default":
                return (new(), w => w.GetSpan().ToArray());
            case "Stack1":
            {
                var buf = GC.AllocateArray<byte>(1, pinned: true);
                return (MemWriter.CreateFromPinnedSpan(buf), w => w.GetSpan().ToArray());
            }
            case "Chunked5":
            case "Chunked7":
            case "Chunked64":
            {
                var controller = new ChunkedMemWriter(int.Parse(kind["Chunked".Length..]));
                return (new(controller), w => controller.GetBytes(ref w));
            }
            case "PositionLess":
            {
                var stream = new MemoryPositionLessStream();
                stream.Write(new byte[] { 1, 2, 3 }, 0);
                var controller = new PositionLessStreamWriter(stream);
                return (new(controller), w =>
                {
                    w.Flush();
                    controller.Dispose();
                    var res = new byte[stream.GetSize()];
                    stream.Read(res, 0);
                    return res;
                });
            }
            case "PositionLessAtEnd":
            {
                var stream = new MemoryPositionLessStream();
                stream.Write(new byte[] { 1, 2, 3 }, 0);
                var disposed = false;
                var controller = new PositionLessStreamWriter(stream, () => disposed = true, true);
                return (new(controller), w =>
                {
                    w.Flush();
                    controller.Dispose();
                    Assert.True(disposed);
                    var res = new byte[stream.GetSize()];
                    stream.Read(res, 0);
                    Assert.Equal(new byte[] { 1, 2, 3 }, res[..3]);
                    return res[3..];
                });
            }
            case "File":
            {
                var fileName = Path.GetTempFileName();
                var controller = new FileMemWriter(fileName);
                return (new(controller), w =>
                {
                    w.Flush();
                    controller.Dispose();
                    var res = File.ReadAllBytes(fileName);
                    File.Delete(fileName);
                    return res;
                });
            }
            case "Brotli":
            {
                var outer = new MemWriter();
                var controller = new BrotliCompressMemWriter(outer);
                return (new(controller), w =>
                {
                    var compressed = controller.Finish(w).GetSpan().ToArray();
                    var reader = new MemReader(new BrotliDecompressMemReader(MemReader.CreateFromReadOnlyMemory(compressed)));
                    var res = new List<byte>();
                    while (!reader.Eof) res.Add(reader.ReadUInt8());
                    return res.ToArray();
                });
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    [Theory]
    [MemberData(nameof(WriterKindsData))]
    public void AllWritersProduceSameBytesAndPositions(string writerKind)
    {
        var (writer, finish) = CreateWriter(writerKind);
        // writer appending at end of stream reports absolute position in the stream
        var basePosition = writerKind == "PositionLessAtEnd" ? 3 : 0;
        for (var i = 0; i < AllItems.Count; i++)
        {
            Assert.True(basePosition + Reference.Offsets[i] == writer.GetCurrentPosition(),
                $"Position before {i}:{AllItems[i].Name} expected {Reference.Offsets[i]} actual {writer.GetCurrentPosition()}");
            AllItems[i].Write(ref writer);
        }

        Assert.Equal(basePosition + Reference.Offsets[AllItems.Count], writer.GetCurrentPosition());
        var bytes = finish(writer);
        Assert.Equal(Reference.Bytes.Length, bytes.Length);
        Assert.True(Reference.Bytes.AsSpan().SequenceEqual(bytes));
    }

    [Fact]
    public void BrotliWriterReportsCorrectPositionAfterBufferFlushes()
    {
        var outer = new MemWriter();
        var controller = new BrotliCompressMemWriter(outer);
        var writer = new MemWriter(controller);
        for (var i = 0; i < 200_000; i++)
        {
            writer.WriteUInt8((byte)i);
            if (i % 9973 == 0) Assert.Equal(i + 1, writer.GetCurrentPosition());
        }

        Assert.Equal(200_000, writer.GetCurrentPosition());
        writer.Flush();
        Assert.Equal(200_000, writer.GetCurrentPosition());
        writer.WriteInt32LE(1);
        Assert.Equal(200_004, writer.GetCurrentPosition());
        outer = controller.Finish(writer);
        var reader = new MemReader(new BrotliDecompressMemReader(MemReader.CreateFromReadOnlyMemory(outer.GetSpan().ToArray())));
        for (var i = 0; i < 200_000; i++)
        {
            Assert.Equal((byte)i, reader.ReadUInt8());
        }

        Assert.Equal(1, reader.ReadInt32LE());
        Assert.Equal(200_004, reader.GetCurrentPosition());
    }

    [Theory]
    [InlineData(-1, 5)]
    [InlineData(12, 30)]
    [InlineData(0, 10)]
    [InlineData(11, 24)]
    public void BrotliParametersAreClampedAndStillRoundTrip(int quality, int windowBits)
    {
        var data = Enumerable.Range(0, 100_000).Select(i => (byte)(i * 31 % 251)).ToArray();
        var compressed = BrotliCompress(data, quality, windowBits);
        Assert.True(compressed.Length < data.Length);
        var reader = new MemReader(new BrotliDecompressMemReader(MemReader.CreateFromReadOnlyMemory(compressed)));
        Assert.Equal(data, reader.ReadByteArrayRaw(data.Length));
        Assert.True(reader.Eof);
    }

    [Fact]
    public void BrotliDecompressionOfCorruptedDataThrows()
    {
        var compressed = BrotliCompress(Encoding.UTF8.GetBytes(new string('a', 1000) + "hello"));
        compressed[0] ^= 0xff;
        compressed[1] ^= 0xff;
        Assert.Throws<InvalidDataException>(() =>
        {
            var reader = new MemReader(new BrotliDecompressMemReader(MemReader.CreateFromReadOnlyMemory(compressed)));
            reader.ReadByteArrayRaw(1005);
        });
    }

    [Fact]
    public void BrotliDecompressionOfTruncatedDataThrows()
    {
        var data = Enumerable.Range(0, 10000).Select(i => (byte)(i * 7919 >> 3)).ToArray();
        var compressed = BrotliCompress(data);
        var truncated = compressed[..(compressed.Length / 2)];
        Assert.Throws<InvalidDataException>(() =>
        {
            var reader = new MemReader(new BrotliDecompressMemReader(MemReader.CreateFromReadOnlyMemory(truncated)));
            reader.ReadByteArrayRaw(data.Length);
        });
    }

    [Fact]
    public void BrotliFinishSkipsUnreadDataAndReturnsSourceAfterCompressedBlock()
    {
        var data = Enumerable.Range(0, 300_000).Select(i => (byte)(i % 7)).ToArray();
        var outer = new MemWriter();
        outer.WriteUInt8(0x11);
        var controller = new BrotliCompressMemWriter(outer);
        var writer = new MemWriter(controller);
        writer.WriteBlock(data);
        outer = controller.Finish(writer);
        outer.WriteInt32BE(0x12345678);
        var source = MemReader.CreateFromReadOnlyMemory(outer.GetSpan().ToArray());
        Assert.Equal(0x11, source.ReadUInt8());
        var decompressor = new BrotliDecompressMemReader(source);
        var reader = new MemReader(decompressor);
        Assert.Equal(data[..10], reader.ReadByteArrayRaw(10));
        reader.SkipBlock(100_000);
        Assert.Equal(data[100_010..100_020], reader.ReadByteArrayRaw(10));
        source = decompressor.Finish(reader);
        Assert.Equal(0x12345678, source.ReadInt32BE());
        Assert.True(source.Eof);
    }

    [Fact]
    public void BrotliReaderAndWriterDoNotSupportSeeking()
    {
        var outer = new MemWriter();
        var controller = new BrotliCompressMemWriter(outer);
        var writer = new MemWriter(controller);
        writer.WriteUInt8(1);
        Assert.Throws<NotSupportedException>(() => SetPosition(writer, 0));
        outer = controller.Finish(writer);
        var reader = new MemReader(new BrotliDecompressMemReader(MemReader.CreateFromReadOnlyMemory(outer.GetSpan().ToArray())));
        Assert.Throws<NotSupportedException>(() => SetPosition(reader, 0));
    }

    static void SetPosition(MemWriter writer, long pos) => writer.SetCurrentPosition(pos);
    static void SetPosition(MemReader reader, long pos) => reader.SetCurrentPosition(pos);

    [Fact]
    public void WriteStringInUtf8WorksWhenMultiByteCharacterCrossesControllerBufferBoundary()
    {
        foreach (var bufferSize in new[] { 5, 6, 7, 16, 17, 64 })
        {
            foreach (var value in new[] { new string('€', 1000), "a" + new string('€', 1000), "ab" + string.Concat(Enumerable.Repeat("😀", 500)),
                         new string('ř', 777) })
            {
                var controller = new ChunkedMemWriter(bufferSize);
                var writer = new MemWriter(controller);
                writer.WriteUInt8(42);
                writer.WriteStringInUtf8(value);
                writer.WriteUInt8(43);
                var bytes = controller.GetBytes(ref writer);
                var reader = MemReader.CreateFromReadOnlyMemory(bytes);
                Assert.Equal(42, reader.ReadUInt8());
                Assert.Equal(value, reader.ReadStringInUtf8());
                Assert.Equal(43, reader.ReadUInt8());
                Assert.True(reader.Eof);
            }
        }
    }

    [Fact]
    public void WriteStringInUtf8WorksWithPositionLessStreamWriter()
    {
        var stream = new MemoryPositionLessStream();
        var controller = new PositionLessStreamWriter(stream);
        var writer = new MemWriter(controller);
        var values = new List<string>();
        for (var i = 0; i < 50; i++)
        {
            var value = new string('a', i * 37 % 11) + new string('€', 300 + i * 13);
            values.Add(value);
            writer.WriteStringInUtf8(value);
        }

        writer.Flush();
        controller.Dispose();
        var bytes = new byte[stream.GetSize()];
        stream.Read(bytes, 0);
        var reader = MemReader.CreateFromReadOnlyMemory(bytes);
        foreach (var value in values)
        {
            Assert.Equal(value, reader.ReadStringInUtf8());
        }

        Assert.True(reader.Eof);
    }

    [Theory]
    [MemberData(nameof(ReaderKindsData))]
    public void BigValuesCrossingBuffersRoundTrip(string readerKind)
    {
        var rnd = new Random(1234);
        var writer = new MemWriter();
        var big = new byte[100_000];
        rnd.NextBytes(big);
        var bigString = string.Concat(Enumerable.Range(0, 20_000).Select(i => (char)rnd.Next(1, 0x3000)));
        var asciiString = new string('A', 70_000);
        writer.WriteByteArray(big);
        writer.WriteString(bigString);
        writer.WriteStringOrdered(bigString);
        writer.WriteString(asciiString);
        writer.WriteStringOrdered(asciiString);
        writer.WriteStringInUtf8(bigString);
        writer.WriteBlock(big);
        var data = writer.GetSpan().ToArray();
        WithReader(readerKind, data, (ref MemReader reader) =>
        {
            Assert.Equal(big, reader.ReadByteArray());
            Assert.Equal(bigString, reader.ReadString());
            Assert.Equal(bigString, reader.ReadStringOrdered());
            Assert.Equal(asciiString, reader.ReadString());
            Assert.Equal(asciiString, reader.ReadStringOrdered());
            Assert.Equal(bigString, reader.ReadStringInUtf8());
            Assert.Equal(big, reader.ReadByteArrayRaw(big.Length));
            Assert.True(reader.Eof);
        });
        WithReader(readerKind, data, (ref MemReader reader) =>
        {
            reader.SkipByteArray();
            reader.SkipString();
            reader.SkipStringOrdered();
            reader.SkipString();
            reader.SkipStringOrdered();
            reader.SkipStringInUtf8();
            reader.SkipBlock((uint)big.Length);
            Assert.True(reader.Eof);
        });
    }

    [Fact]
    public void ReadOnlySequenceReaderReadsValuesSplitAcrossSegments()
    {
        var writer = new MemWriter();
        writer.WriteInt64BE(0x0102030405060708);
        writer.WriteGuid(new Guid("00112233445566778899aabbccddeeff"));
        writer.WriteByteArray(Enumerable.Range(0, 50).Select(i => (byte)i).ToArray());
        var data = writer.GetSpan().ToArray();
        using var controller = new ReadOnlySequenceMemReader(CreateSequence(data, _ => 3));
        var reader = new MemReader(controller);
        Assert.Equal(0x0102030405060708, reader.ReadInt64BE());
        Assert.Equal(new Guid("00112233445566778899aabbccddeeff"), reader.ReadGuid());
        Assert.Equal(Enumerable.Range(0, 50).Select(i => (byte)i).ToArray(), reader.ReadByteArray());
        Assert.True(reader.Eof);
    }

    [Fact]
    public void ReadOnlyMemoryReaderReadByteArrayAsMemoryAdvancesPosition()
    {
        var writer = new MemWriter();
        writer.WriteByteArray(new byte[] { 1, 2, 3 });
        writer.WriteVUInt32(42);
        writer.WriteBlock(new byte[] { 4, 5 });
        var reader = MemReader.CreateFromReadOnlyMemory(writer.GetSpan().ToArray());
        Assert.Equal(new byte[] { 1, 2, 3 }, reader.ReadByteArrayAsMemory().ToArray());
        Assert.Equal(4, reader.GetCurrentPosition());
        Assert.Equal(42u, reader.ReadVUInt32());
        Assert.Equal(new byte[] { 4, 5 }, reader.ReadByteArrayRawTillEofAsMemory().ToArray());
        Assert.True(reader.Eof);
    }

    [Fact]
    public void BrotliReaderReportsCorrectPosition()
    {
        var data = Enumerable.Range(0, 200_000).Select(i => (byte)(i % 13)).ToArray();
        var reader = new MemReader(new BrotliDecompressMemReader(
            MemReader.CreateFromReadOnlyMemory(BrotliCompress(data).Concat(new byte[] { 0 }).ToArray())));
        Assert.Equal(0, reader.GetCurrentPosition());
        Assert.Equal(0, reader.ReadUInt8());
        Assert.Equal(1, reader.GetCurrentPosition());
        reader.SkipBlock(99_999);
        Assert.Equal(100_000, reader.GetCurrentPosition());
        Assert.Equal(data[100_000..100_010], reader.ReadByteArrayRaw(10));
        Assert.Equal(100_010, reader.GetCurrentPosition());
    }

    [Fact]
    public void BrotliReaderCanReadToEndOfSourceWithoutTrailingData()
    {
        var data = Enumerable.Range(0, 100_000).Select(i => (byte)(i * 31 % 251)).ToArray();
        var reader = new MemReader(new BrotliDecompressMemReader(MemReader.CreateFromReadOnlyMemory(BrotliCompress(data))));
        Assert.Equal(data, reader.ReadByteArrayRaw(data.Length));
        Assert.True(reader.Eof);
        var r = reader;
        Assert.Throws<EndOfStreamException>(() => ReadUInt8(r));
    }

    [Fact]
    public void BrotliReaderReadsLargeBlockCrossingInternalBuffers()
    {
        var data = new byte[300_000];
        new Random(7).NextBytes(data);
        var reader = new MemReader(new BrotliDecompressMemReader(MemReader.CreateFromReadOnlyMemory(BrotliCompress(data))));
        Assert.Equal(data[0], reader.ReadUInt8());
        Assert.Equal(data[1..200_001], reader.ReadByteArrayRaw(200_000));
        Assert.Equal(data[200_001..], reader.ReadByteArrayRaw(data.Length - 200_001));
        Assert.True(reader.Eof);
    }
}
