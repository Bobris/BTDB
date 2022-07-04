using BTDB.Bon;
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
        Assert.Equal(2,buffer.Length);
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
        Assert.Equal(2,buffer.Length);
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
        Assert.Equal(2,buffer.Length);
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
        Assert.Equal(1+5+1+1+1,buffer.Length);
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
}
