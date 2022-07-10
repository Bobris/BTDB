using System;
using System.Globalization;
using Assent;
using BTDB.Bon;
using BTDB.Buffer;
using Xunit;

namespace BTDBTest;

public class BonTests
{
    [Fact]
    public void CanStoreNull()
    {
        var builder = new BonBuilder();
        builder.WriteNull();
        var buffer = builder.Finish();
        Assert.Equal(2, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Null, bon.BonType);
        Assert.True(bon.TryGetNull());
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("null", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreUndefined()
    {
        var builder = new BonBuilder();
        builder.WriteUndefined();
        var buffer = builder.Finish();
        Assert.Equal(2, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Undefined, bon.BonType);
        Assert.True(bon.TryGetUndefined());
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("null\r\n/*undefined*/", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreEmptyString()
    {
        var builder = new BonBuilder();
        builder.Write("");
        var buffer = builder.Finish();
        Assert.Equal(2, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.String, bon.BonType);
        Assert.True(bon.TryGetString(out var result));
        Assert.Equal("", result);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("\"\"", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreSomeString()
    {
        var builder = new BonBuilder();
        builder.Write("Hello");
        var buffer = builder.Finish();
        // Len(5) Hello CodeStringPtr(130) Ofs(0) LastBonLen(2)
        Assert.Equal(1 + 5 + 1 + 1 + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.String, bon.BonType);
        Assert.True(bon.TryGetString(out var result));
        Assert.Equal("Hello", result);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("\"Hello\"", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreSmallNumbers()
    {
        for (var i = -10; i <= 10; i++)
        {
            var builder = new BonBuilder();
            builder.Write(i);
            var buffer = builder.Finish();
            Assert.Equal(1 + 1, buffer.Length);
            var bon = new Bon(buffer);
            Assert.False(bon.Eof);
            Assert.Equal(1u, bon.Items);
            Assert.Equal(BonType.Integer, bon.BonType);
            Assert.True(bon.TryGetLong(out var result));
            Assert.Equal(i, result);
            Assert.True(bon.Eof);
            Assert.Equal(0u, bon.Items);
            Assert.Equal(i.ToString(), new Bon(buffer).DumpToJson());
        }
    }

    [Theory]
    [InlineData(-11)]
    [InlineData(11)]
    [InlineData(-123456)]
    [InlineData(123456)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void CanStoreBiggerNumbers(long i)
    {
        var builder = new BonBuilder();
        builder.Write(i);
        var buffer = builder.Finish();
        Assert.Equal(1 + (int)PackUnpack.LengthVInt(i) + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Integer, bon.BonType);
        Assert.True(bon.TryGetLong(out var result));
        Assert.Equal(i, result);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal(i.ToString(), new Bon(buffer).DumpToJson());
    }

    [Theory]
    [InlineData(11)]
    [InlineData(123456)]
    [InlineData(ulong.MaxValue)]
    public void CanStoreBiggerPositiveNumbers(ulong i)
    {
        var builder = new BonBuilder();
        builder.Write(i);
        var buffer = builder.Finish();
        Assert.Equal(1 + (int)PackUnpack.LengthVUInt(i) + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Integer, bon.BonType);
        Assert.True(bon.TryGetULong(out var result));
        Assert.Equal(i, result);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal(i.ToString(), new Bon(buffer).DumpToJson());
    }

    [Theory]
    [InlineData(0, 2)]
    [InlineData(0.5, 2)]
    [InlineData(0.25, 2)]
    [InlineData(-0.125, 2)]
    [InlineData(float.PositiveInfinity, 2)]
    [InlineData(float.NegativeInfinity, 2)]
    [InlineData(float.Epsilon, 4)]
    [InlineData(float.MinValue, 4)]
    [InlineData(float.MaxValue, 4)]
    [InlineData(1.5e20, 8)]
    [InlineData(double.Epsilon, 8)]
    [InlineData(double.MinValue, 8)]
    [InlineData(double.MaxValue, 8)]
    public void CanStoreDouble(double i, int byteLen)
    {
        var builder = new BonBuilder();
        builder.Write(i);
        var buffer = builder.Finish();
        Assert.Equal(1 + byteLen + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Float, bon.BonType);
        Assert.True(bon.TryGetDouble(out var result));
        Assert.Equal(i, result);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        if (i == double.PositiveInfinity)
        {
            Assert.Equal("\"+\u221E\"", new Bon(buffer).DumpToJson());
        }
        else if (i == double.NegativeInfinity)
        {
            Assert.Equal("\"-\u221E\"", new Bon(buffer).DumpToJson());
        }
        else
        {
            Assert.Equal(i.ToString(CultureInfo.InvariantCulture), new Bon(buffer).DumpToJson());
        }
    }

    [Fact]
    public void CanStoreDateTime()
    {
        var dt = new DateTime(2022, 7, 5, 21, 15, 42, 123);
        var builder = new BonBuilder();
        builder.Write(dt);
        var buffer = builder.Finish();
        Assert.Equal(1 + 8 + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.DateTime, bon.BonType);
        Assert.True(bon.TryGetDateTime(out var result));
        Assert.Equal(dt, result);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("\"2022-07-05T21:15:42.1230000\"", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreGuid()
    {
        var g = Guid.NewGuid();
        var builder = new BonBuilder();
        builder.Write(g);
        var buffer = builder.Finish();
        Assert.Equal(1 + 16 + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Guid, bon.BonType);
        Assert.True(bon.TryGetGuid(out var result));
        Assert.Equal(g, result);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("\"" + g.ToString("D") + "\"", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreEmptyByteArray()
    {
        var g = Array.Empty<byte>();
        var builder = new BonBuilder();
        builder.Write(g);
        var buffer = builder.Finish();
        Assert.Equal(1 + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.ByteArray, bon.BonType);
        Assert.True(bon.TryGetByteArray(out var result));
        Assert.True(g.AsSpan().SequenceEqual(result));
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("\"\"", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreSomeByteArray()
    {
        var g = new byte[] { 1, 0, 42, 255 };
        var builder = new BonBuilder();
        builder.Write(g);
        var buffer = builder.Finish();
        Assert.Equal(1 + 4 + 1 + 1 + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.ByteArray, bon.BonType);
        Assert.True(bon.TryGetByteArray(out var result));
        Assert.True(g.AsSpan().SequenceEqual(result));
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("\"AQAq/w==\"", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreEmptyArray()
    {
        var builder = new BonBuilder();
        builder.StartArray();
        builder.FinishArray();
        var buffer = builder.Finish();
        Assert.Equal(1 + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Array, bon.BonType);
        Assert.True(bon.TryGetArray(out var bonArrayItems));
        Assert.Equal(0u, bonArrayItems.Items);
        Assert.True(bonArrayItems.Eof);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("[]", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreSomeArray()
    {
        var builder = new BonBuilder();
        builder.StartArray();
        builder.Write(42);
        builder.FinishArray();
        var buffer = builder.Finish();
        Assert.Equal(1 + 2 + 1 + 1 + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Array, bon.BonType);
        Assert.True(bon.TryGetArray(out var bonArrayItems));
        Assert.Equal(1u, bonArrayItems.Items);
        Assert.True(bonArrayItems.TryGetLong(out var result));
        Assert.Equal(42, result);
        Assert.True(bonArrayItems.Eof);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("[\r\n  42\r\n]", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreEmptyArrayInArray()
    {
        var builder = new BonBuilder();
        builder.StartArray();
        builder.StartArray();
        builder.FinishArray();
        builder.FinishArray();
        var buffer = builder.Finish();
        Assert.Equal(1 + 1 + 1 + 1 + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Array, bon.BonType);
        Assert.True(bon.TryGetArray(out var bonArrayItems));
        Assert.Equal(1u, bonArrayItems.Items);
        Assert.True(bonArrayItems.TryGetArray(out var result));
        Assert.Equal(0u, result.Items);
        Assert.True(bonArrayItems.Eof);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("[\r\n  []\r\n]", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreEmptyObject()
    {
        var builder = new BonBuilder();
        builder.StartObject();
        builder.FinishObject();
        var buffer = builder.Finish();
        Assert.Equal(1 + 1, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Object, bon.BonType);
        Assert.True(bon.TryGetObject(out var keyedBon));
        Assert.Equal(0u, keyedBon.Items);
        Assert.True(keyedBon.Values().Eof);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("{}", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreSomeObject()
    {
        var builder = new BonBuilder();
        builder.StartObject();
        builder.WriteKey("a");
        builder.Write(1);
        builder.FinishObject();
        var buffer = builder.Finish();
        Assert.Equal(9, buffer.Length);
        var bon = new Bon(buffer);
        Assert.False(bon.Eof);
        Assert.Equal(1u, bon.Items);
        Assert.Equal(BonType.Object, bon.BonType);
        Assert.True(bon.TryGetObject(out var keyedBon));
        Assert.Equal(1u, keyedBon.Items);
        Assert.Equal("a", keyedBon.NextKey());
        Assert.Null(keyedBon.NextKey());
        Assert.True(keyedBon.Values().TryGetLong(out var value));
        Assert.Equal(1, value);
        Assert.True(bon.Eof);
        Assert.Equal(0u, bon.Items);
        Assert.Equal("{\r\n  \"a\": 1\r\n}", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreComplexArrayObjectCombination()
    {
        var builder = new BonBuilder();
        builder.StartObject();
        builder.WriteKey("a");
        builder.StartArray();
        builder.WriteNull();
        builder.StartObject();
        builder.WriteKey("b");
        builder.Write(12.34);
        builder.FinishObject();
        builder.FinishArray();
        builder.WriteKey("b");
        builder.Write("last");
        builder.FinishObject();
        var buffer = builder.Finish();
        this.Assent(new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreClass()
    {
        var builder = new BonBuilder();
        builder.StartClass("MyKlass");
        builder.WriteKey("a");
        builder.WriteNull();
        builder.WriteKey("b");
        builder.Write("last");
        builder.FinishClass();
        var buffer = builder.Finish();
        this.Assent(new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreEmptyDictionary()
    {
        var builder = new BonBuilder();
        builder.StartDictionary();
        builder.FinishDictionary();
        var buffer = builder.Finish();
        Assert.Equal(2, buffer.Length);
        Assert.Equal("[]", new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanStoreSomeDictionary()
    {
        var builder = new BonBuilder();
        builder.StartDictionary();
        builder.Write("a");
        builder.Write("A");
        builder.Write(2);
        builder.Write(4);
        builder.FinishDictionary();
        var buffer = builder.Finish();
        this.Assent(new Bon(buffer).DumpToJson());
    }

    [Fact]
    public void CanQuicklyDrillDownIntoBon()
    {
        var builder = new BonBuilder();
        builder.StartObject();
        builder.WriteKey("first");
        builder.StartArray();
        for (var i = 0; i < 10; i++)
        {
            builder.Write(i);
        }

        builder.FinishArray();
        builder.WriteKey("last");
        builder.StartArray();
        for (var i = 0; i < 10; i++)
        {
            builder.Write(i + 10);
        }

        builder.FinishArray();
        builder.FinishObject();
        var buffer = builder.Finish();
        var bon = new Bon(buffer);
        Assert.True(bon.TryGetObject(out var keyedBon));
        Assert.True(keyedBon.TryGet("last", out var array));
        Assert.True(array.TryGetArray(out var items));
        items.Skip();
        Assert.True(items.TryGetLong(out var result));
        Assert.Equal(11, result);
    }
}
