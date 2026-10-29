using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BTDB.Bon;
using BTDB.Buffer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class BonCoverageTest
{
    [Theory]
    [InlineData(0, BonType.Error)]
    [InlineData(1, BonType.Null)]
    [InlineData(2, BonType.Undefined)]
    [InlineData(3, BonType.Bool)]
    [InlineData(4, BonType.Bool)]
    [InlineData(5, BonType.Integer)]
    [InlineData(25, BonType.Integer)]
    [InlineData(128, BonType.Integer)]
    [InlineData(129, BonType.Integer)]
    [InlineData(64, BonType.Float)]
    [InlineData(65, BonType.Float)]
    [InlineData(66, BonType.Float)]
    [InlineData(26, BonType.String)]
    [InlineData(130, BonType.String)]
    [InlineData(67, BonType.DateTime)]
    [InlineData(68, BonType.Guid)]
    [InlineData(27, BonType.Array)]
    [InlineData(131, BonType.Array)]
    [InlineData(136, BonType.Array)]
    [InlineData(28, BonType.Object)]
    [InlineData(132, BonType.Object)]
    [InlineData(133, BonType.Class)]
    [InlineData(29, BonType.Dictionary)]
    [InlineData(134, BonType.Dictionary)]
    [InlineData(30, BonType.ByteArray)]
    [InlineData(135, BonType.ByteArray)]
    [InlineData(137, BonType.Tuple)]
    [InlineData(31, BonType.Error)]
    [InlineData(69, BonType.Error)]
    [InlineData(138, BonType.Error)]
    [InlineData(255, BonType.Error)]
    public void BonTypeFromByteMapsAllCodes(byte code, BonType expected)
    {
        Assert.Equal(expected, Helpers.BonTypeFromByte(code));
    }

    delegate void BuilderAction(ref BonBuilder builder);

    static ReadOnlyMemory<byte> Build(BuilderAction write)
    {
        var builder = new BonBuilder();
        write(ref builder);
        return builder.FinishAsMemory();
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(10L)]
    [InlineData(11L)]
    [InlineData(-1L)]
    [InlineData(-10L)]
    [InlineData(-11L)]
    [InlineData(long.MaxValue)]
    [InlineData(long.MinValue)]
    public void IntegersCanBeReadAsLongDoubleAndUlong(long value)
    {
        var buf = Build((ref BonBuilder b) => b.Write(value));
        var bon = new Bon(buf);
        Assert.Equal(BonType.Integer, bon.BonType);
        Assert.False(bon.TryGetString(out _));
        Assert.False(bon.TryGetBool(out _));
        Assert.False(bon.TryGetDateTime(out _));
        Assert.True(bon.TryGetLong(out var l));
        Assert.Equal(value, l);
        Assert.True(bon.Eof);
        bon = new(buf);
        Assert.True(bon.TryGetDouble(out var d));
        Assert.Equal((double)value, d);
        bon = new(buf);
        Assert.Equal(value >= 0, bon.TryGetULong(out var ul));
        if (value >= 0) Assert.Equal((ulong)value, ul);
        Assert.Equal(value.ToString(), new Bon(buf).DumpToJson());
    }

    [Theory]
    [InlineData(0ul)]
    [InlineData(10ul)]
    [InlineData(11ul)]
    [InlineData(ulong.MaxValue)]
    public void UnsignedIntegers(ulong value)
    {
        var buf = Build((ref BonBuilder b) => b.Write(value));
        var bon = new Bon(buf);
        Assert.True(bon.TryGetULong(out var ul));
        Assert.Equal(value, ul);
        Assert.Equal(value.ToString(), new Bon(buf).DumpToJson());
    }

    [Theory]
    [InlineData(1.5, 64)]
    [InlineData(1e-10, 66)]
    [InlineData(3.4028234663852886E+38, 65)]
    [InlineData(0.1, 66)]
    [InlineData(double.PositiveInfinity, 64)]
    [InlineData(double.NegativeInfinity, 64)]
    // Before the fix dumping NaN to JSON failed with ArgumentException from Utf8JsonWriter.
    [InlineData(double.NaN, 66)]
    public void FloatsUseSmallestExactEncoding(double value, byte expectedCode)
    {
        var buf = Build((ref BonBuilder b) => b.Write(value));
        Assert.Equal(expectedCode, buf.Span[0]);
        var bon = new Bon(buf);
        Assert.Equal(BonType.Float, bon.BonType);
        Assert.False(bon.TryGetLong(out _));
        Assert.False(bon.TryGetULong(out _));
        Assert.True(bon.TryGetDouble(out var d));
        Assert.Equal(value, d);
        var json = new Bon(buf).DumpToJson();
        if (double.IsPositiveInfinity(value)) Assert.Equal("\"+∞\"", json);
        else if (double.IsNegativeInfinity(value)) Assert.Equal("\"-∞\"", json);
        else if (double.IsNaN(value)) Assert.Equal("\"NaN\"", json);
        else Assert.Equal(value, double.Parse(json, System.Globalization.CultureInfo.InvariantCulture));
    }

    // UTF-8 cannot hold a lone surrogate, so it is written as U+FFFD; keys compare so too, before the fix two keys
    // differing only in a lone surrogate were both written and came out the same.
    [Fact]
    public void LoneSurrogatesAreWrittenAsReplacementCharacter()
    {
        var buf = Build((ref BonBuilder b) =>
        {
            b.StartObject();
            b.WriteKey("a\ud800");
            b.Write("x\udc00y");
            Assert.False(b.TryWriteKey("a\ud801"));
            b.WriteKey("pair\ud83d\ude00");
            b.Write("ok\ud83d\ude00\ud800");
            b.FinishObject();
        });
        var bon = new Bon(buf);
        Assert.True(bon.TryGetObject(out var obj));
        var read = new List<string>();
        var values = obj.Values();
        while (obj.NextKey() is { } key)
        {
            read.Add(key);
            values.TryGetString(out var value);
            read.Add(value);
        }

        Assert.Equal(["a\ufffd", "x\ufffdy", "pair\ud83d\ude00", "ok\ud83d\ude00\ufffd"], read);
    }

    // Before the fix ticks out of range, which only damage leaves, failed with ArgumentException.
    [Fact]
    public void DamagedDateTimeIsInvalidData()
    {
        var buf = Build((ref BonBuilder b) => b.Write(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc))).ToArray();
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(1), 0x3FFF_FFFF_FFFF_FFFF);
        var bon = new Bon(buf);
        Assert.Equal(BonType.DateTime, bon.BonType);
        Assert.Throws<System.IO.InvalidDataException>(() => new Bon(buf).TryGetDateTime(out _));
        Assert.Throws<System.IO.InvalidDataException>(() => new Bon(buf).DumpToJson());
    }

    [Fact]
    public void OtherScalarsRejectWrongTypes()
    {
        var str = new Bon(Build((ref BonBuilder b) => b.Write("abc")));
        Assert.False(str.TryGetDouble(out _));
        Assert.False(str.TryGetGuid(out _));
        Assert.False(str.TryGetNull());
        Assert.False(str.TryGetUndefined());
        Assert.False(str.TryGetByteArray(out _));
        Assert.False(str.TryGetArray(out _));
        Assert.False(str.TryGetTuple(out _));
        Assert.False(str.TryGetObject(out _));
        Assert.False(str.TryGetClass(out _, out _));
        Assert.False(str.PeekClass(out _));
        Assert.False(str.TryGetDictionary(out _));
        Assert.True(str.TryGetStringBytes(out var bytes));
        Assert.Equal("abc"u8.ToArray(), bytes.ToArray());
        var num = new Bon(Build((ref BonBuilder b) => b.Write(1L)));
        Assert.False(num.TryGetStringBytes(out _));
        Assert.False(num.TryGetGuid(out _));
        var empty = new Bon(Build((ref BonBuilder b) => b.WriteUtf8(ReadOnlySpan<byte>.Empty)));
        Assert.True(empty.TryGetStringBytes(out bytes));
        Assert.Equal(0, bytes.Length);
        empty = new(Build((ref BonBuilder b) => b.WriteUtf8(ReadOnlySpan<byte>.Empty)));
        Assert.True(empty.TryGetString(out var s));
        Assert.Equal("", s);
    }

    [Fact]
    public void ConstructorsFromVariousSources()
    {
        var buf = Build((ref BonBuilder b) => b.Write(true)).ToArray();
        Assert.True(new Bon(ByteBuffer.NewAsync(buf)).TryGetBool(out var v1) && v1);
        var reader = MemReader.CreateFromReadOnlyMemory(buf);
        Assert.True(new Bon(reader).TryGetBool(out var v2) && v2);
        Assert.True(new Bon(new ReadOnlyMemoryMemReader(buf)).TryGetBool(out var v3) && v3);
        var memWriter = new MemWriter();
        var builder = new BonBuilder(memWriter);
        builder.Write(false);
        Assert.Equal(1ul, builder.EstimateLowerBoundSize());
        var fromWriter = builder.Finish();
        Assert.True(new Bon(fromWriter.GetSpan().ToArray()).TryGetBool(out var v4));
        Assert.False(v4);
    }

    [Fact]
    public void BuilderOverControllerWriter()
    {
        var controller = new MemReaderWriterRoundTripTest.ChunkedMemWriter(16);
        var builder = new BonBuilder(controller);
        builder.StartArray();
        for (var i = 0; i < 40; i++) builder.Write("item" + i);
        builder.FinishArray();
        var writer = builder.Finish();
        var bytes = controller.GetBytes(ref writer);
        var bon = new Bon(bytes);
        Assert.True(bon.TryGetArray(out var arr));
        Assert.Equal(40u, arr.Items);
        Assert.True(arr.TryGet(35, out var item));
        Assert.True(item.TryGetString(out var s));
        Assert.Equal("item35", s);
    }

    [Fact]
    public void LargeArraysCanBeIndexedRandomly()
    {
        foreach (var count in new[] { 1, 31, 32, 33, 64, 65, 100 })
        {
            var buf = Build((ref BonBuilder b) =>
            {
                b.StartObject();
                b.WriteKey("arr");
                b.StartArray();
                for (var i = 0; i < count; i++)
                {
                    if (i % 5 == 0) b.Write(new byte[] { (byte)i, 1 });
                    else b.Write(i);
                }

                b.FinishArray();
                b.FinishObject();
            });
            var bon = new Bon(buf);
            Assert.True(bon.TryGetObject(out var obj));
            Assert.True(obj.TryGet("arr", out var arrBon));
            Assert.True(arrBon.TryGetArray(out var arr));
            Assert.Equal((uint)count, arr.Items);
            for (var i = count - 1; i >= 0; i--)
            {
                Assert.True(arr.TryGet((uint)i, out var item));
                if (i % 5 == 0)
                {
                    Assert.True(item.TryGetByteArray(out var bytes));
                    Assert.Equal(new byte[] { (byte)i, 1 }, bytes.ToArray());
                }
                else
                {
                    Assert.True(item.TryGetLong(out var l));
                    Assert.Equal(i, l);
                }
            }

            Assert.False(arr.TryGet((uint)count, out _));
            var json = new Bon(buf).DumpToJson();
            Assert.Contains("\"arr\"", json);
            Assert.StartsWith("{", json);
        }
    }

    [Fact]
    public void ObjectsAndClassesAccessByKey()
    {
        var longKey = new string('k', 300);
        var buf = Build((ref BonBuilder b) =>
        {
            b.StartArray();
            b.StartObject();
            b.WriteKey("a");
            b.Write(1L);
            Assert.False(b.TryWriteKey("a"));
            Assert.True(b.TryWriteKey("b"));
            b.Write("x");
            b.WriteKey(longKey);
            b.Write(2L);
            b.FinishObject();
            b.StartClass("MyClass");
            b.WriteKey("f1"u8);
            b.Write(10L);
            b.WriteKey("f2");
            b.WriteNull();
            b.FinishClass();
            b.StartClass("MyClass"u8);
            b.WriteKey("f1"u8);
            b.Write(20L);
            b.WriteKey("f2");
            b.Write(Guid.Empty);
            b.FinishClass();
            b.StartObject();
            b.FinishObject();
            b.FinishArray();
        });
        var bon = new Bon(buf);
        Assert.True(bon.TryGetArray(out var arr));
        Assert.True(arr.TryGet(0, out var item));
        Assert.True(item.TryGetObject(out var obj));
        Assert.Equal(3u, obj.Items);
        Assert.True(obj.TryGet(longKey, out var v));
        Assert.True(v.TryGetLong(out var l));
        Assert.Equal(2, l);
        Assert.True(obj.TryGet("b", out v));
        Assert.True(v.TryGetString(out var s));
        Assert.Equal("x", s);
        Assert.False(obj.TryGet("c", out _));
        Assert.False(obj.TryGet("zz", out _));
        obj.Reset();
        Assert.Equal("a", obj.NextKey());
        Assert.Equal("b"u8.ToArray(), obj.NextKeyUtf8().ToArray());
        Assert.Equal(longKey, obj.NextKey());
        Assert.Null(obj.NextKey());
        Assert.Equal(0, obj.NextKeyUtf8().Length);
        obj.Reset();
        Assert.Equal("a", obj.NextKey());

        Assert.True(arr.TryGet(1, out item));
        Assert.True(item.PeekClass(out var peekName));
        Assert.Equal("MyClass"u8.ToArray(), peekName.ToArray());
        Assert.True(item.TryGetClass(out var cls, out var name));
        Assert.Equal("MyClass", Encoding.UTF8.GetString(name));
        Assert.True(cls.TryGet("f2", out v));
        Assert.True(v.TryGetNull());
        Assert.True(arr.TryGet(2, out item));
        Assert.True(item.TryGetClass(out cls, out _));
        Assert.True(cls.TryGet("f2", out v));
        Assert.True(v.TryGetGuid(out var g));
        Assert.Equal(Guid.Empty, g);
        Assert.True(arr.TryGet(3, out item));
        Assert.True(item.TryGetObject(out obj));
        Assert.Equal(0u, obj.Items);
        var json = new Bon(buf).DumpToJson();
        Assert.Contains("\"__type__\": \"MyClass\"", json);
        Assert.Contains("\"f2\": \"00000000-0000-0000-0000-000000000000\"", json);
    }

    [Fact]
    public void DictionariesTuplesAndEmptyContainers()
    {
        var date = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var buf = Build((ref BonBuilder b) =>
        {
            b.StartTuple();
            b.StartDictionary();
            b.Write(1L);
            b.Write("one");
            b.Write(2L);
            b.Write(date);
            b.FinishDictionary();
            b.StartDictionary();
            b.FinishDictionary();
            b.StartArray();
            b.FinishArray();
            b.Write(ReadOnlySpan<byte>.Empty);
            b.WriteUndefined();
            b.Write((string?)null);
            b.Write("");
            b.FinishTuple();
        });
        var bon = new Bon(buf);
        Assert.Equal(BonType.Tuple, bon.BonType);
        Assert.True(bon.TryGetTuple(out var tuple));
        Assert.Equal(7u, tuple.Items);
        Assert.True(tuple.TryGet(0, out var item));
        Assert.True(item.TryGetDictionary(out var dict));
        Assert.Equal(4u, dict.Items);
        Assert.True(dict.TryGetLong(out var k));
        Assert.Equal(1, k);
        Assert.True(dict.TryGetString(out var sv));
        Assert.Equal("one", sv);
        dict.Skip();
        Assert.True(dict.TryGetDateTime(out var dt));
        Assert.Equal(date, dt);
        Assert.True(dict.Eof);
        Assert.True(tuple.TryGet(1, out item));
        Assert.True(item.TryGetDictionary(out dict));
        Assert.True(dict.Eof);
        Assert.True(tuple.TryGet(2, out item));
        Assert.True(item.TryGetArray(out var arr));
        Assert.Equal(0u, arr.Items);
        Assert.True(tuple.TryGet(3, out item));
        Assert.True(item.TryGetByteArray(out var ba));
        Assert.Equal(0, ba.Length);
        Assert.True(tuple.TryGet(4, out item));
        Assert.True(item.TryGetUndefined());
        Assert.True(tuple.TryGet(5, out item));
        Assert.True(item.TryGetNull());
        var json = new Bon(buf).DumpToJson();
        Assert.Contains("\"2020-01-02T03:04:05.0000000Z\"", json);
        Assert.Contains("\"one\"", json);
        Assert.Contains("/*undefined*/", json);
    }

    [Fact]
    public void SkipHandlesAllFixedSizeEncodings()
    {
        var buf = Build((ref BonBuilder b) =>
        {
            b.StartArray();
            b.Write(1.5); // half
            b.Write(3.4028234663852886E+38); // float
            b.Write(0.1); // double
            b.Write(DateTime.UnixEpoch);
            b.Write(Guid.NewGuid());
            b.Write(1000L);
            b.Write(true);
            b.Write("last");
            b.FinishArray();
        });
        var bon = new Bon(buf);
        Assert.True(bon.TryGetArray(out var arr));
        Assert.True(arr.TryGet(7, out var last));
        Assert.True(last.TryGetString(out var s));
        Assert.Equal("last", s);
    }

    [Fact]
    public void ErrorCodeIsDumpedAsComment()
    {
        // single byte 0 (error) followed by offset byte 1
        Assert.Equal("/*Error*/", new Bon(new byte[] { 0, 1 }).DumpToJson());
    }

    [Fact]
    public void BuilderValidatesState()
    {
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) => b.FinishArray()));
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) => b.FinishTuple()));
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) => b.FinishObject()));
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) => b.FinishClass()));
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) => b.FinishDictionary()));
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) => b.WriteKey("a")));
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) => b.WriteKey("a"u8)));
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) =>
        {
            b.Write(1L);
            b.Write(2L);
        }));
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) =>
        {
            b.StartObject();
            b.Write(1L);
        }));
        Assert.Throws<ArgumentException>(() => Build((ref BonBuilder b) =>
        {
            b.StartObject();
            b.WriteKey("a");
            b.Write(1L);
            b.WriteKey("a");
        }));
        Assert.Throws<InvalidOperationException>(() => Build((ref BonBuilder b) => { }));
        var builder = new BonBuilder();
        builder.Write(1L);
        builder.Finish();
        Assert.Throws<InvalidOperationException>(() => WriteAfterFinish(builder));
        // finishing already finished builder is allowed
        builder.Finish();
    }

    static void WriteAfterFinish(BonBuilder builder) => builder.Write(2L);

    [Fact]
    public void StringsAreDeduplicated()
    {
        var once = Build((ref BonBuilder b) =>
        {
            b.StartArray();
            b.Write("duplicated string value");
            b.FinishArray();
        });
        var twice = Build((ref BonBuilder b) =>
        {
            b.StartArray();
            b.Write("duplicated string value");
            b.Write("duplicated string value");
            b.WriteUtf8("utf8 duplicated"u8);
            b.WriteUtf8("utf8 duplicated"u8);
            b.FinishArray();
        });
        Assert.True(twice.Length < once.Length * 2);
        var bon = new Bon(twice);
        Assert.True(bon.TryGetArray(out var arr));
        Assert.True(arr.TryGet(3, out var item));
        Assert.True(item.TryGetString(out var s));
        Assert.Equal("utf8 duplicated", s);
    }
}
