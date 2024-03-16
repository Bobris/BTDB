using System.Text;
using BTDB.Collections;
using Xunit;

namespace BTDBTest;

public class SpanByteLruCacheTest
{
    [Fact]
    public void SimpleTest()
    {
        var cache = new SpanByteLruCache<int>(2);
        Assert.Empty(cache);
        Assert.False(cache.TryGetValue("1"u8, out var value));
        Assert.Equal(0, value);
        Assert.Empty(cache);
        cache["1"u8] = 11;
        Assert.Single(cache);
        Assert.True(cache.TryGetValue("1"u8, out value));
        Assert.Equal(11, value);
        Assert.Single(cache);
        Assert.False(cache.TryGetValue("2"u8, out value));
        Assert.Equal(0, value);
        Assert.Single(cache);
        cache["2"u8] = 22;
        Assert.Equal(2, cache.Count);
        Assert.True(cache.TryGetValue("1"u8, out value));
        Assert.Equal(11, value);
        Assert.Equal(2, cache.Count);
        Assert.True(cache.TryGetValue("2"u8, out value));
        Assert.Equal(22, value);
        Assert.Equal(2, cache.Count);
        Assert.False(cache.TryGetValue("3"u8, out value));
        Assert.Equal(0, value);
        Assert.Equal(2, cache.Count);
        cache["3"u8] = 33;
        Assert.Equal(2, cache.Count);
        Assert.False(cache.TryGetValue("1"u8, out value));
        Assert.Equal(0, value);
        Assert.True(cache.TryGetValue("2"u8, out value));
        Assert.Equal(22, value);
        Assert.True(cache.TryGetValue("3"u8, out value));
        Assert.Equal(33, value);
        cache.Clear();
        Assert.Empty(cache);
        cache["1"u8] = 11;
        Assert.Single(cache);
    }

    [Fact]
    void CapacityIncreasingTest()
    {
        var cache = new SpanByteLruCache<int>(100);
        for (var i = 0; i < 128; i++)
        {
            cache[Encoding.UTF8.GetBytes(i.ToString())] = i;
        }

        // overflow capacity
        cache["128"u8] = 128;

        Assert.Equal(128, cache.Count);
        for (var i = 1; i < 129; i++)
        {
            Assert.True(cache.TryGetValue(Encoding.UTF8.GetBytes(i.ToString()), out var value));
            Assert.Equal(i, value);
        }

        Assert.False(cache.TryGetValue("0"u8, out _));
    }

    [Fact]
    void RemoveTest()
    {
        var cache = new SpanByteLruCache<int>(100);
        for (var i = 0; i < 128; i++)
        {
            cache[Encoding.UTF8.GetBytes(i.ToString())] = i;
        }

        for (var i = 0; i < 128; i += 2)
        {
            cache.Remove(Encoding.UTF8.GetBytes(i.ToString()));
        }

        Assert.Equal(64, cache.Count);
        for (var i = 1; i < 129; i += 2)
        {
            Assert.True(cache.TryGetValue(Encoding.UTF8.GetBytes(i.ToString()), out var value));
            Assert.Equal(i, value);
        }

        for (var i = 0; i < 128; i += 2)
        {
            Assert.False(cache.TryGetValue(Encoding.UTF8.GetBytes(i.ToString()), out var value));
            Assert.Equal(0, value);
        }
    }

    [Fact]
    void ComplexTest()
    {
        var cache = new SpanByteLruCache<int>(4);
        cache["-1"u8] = -1;
        cache["-2"u8] = -2;
        for (var i = 0; i < 100; i++)
        {
            Assert.True(cache.TryGetValue("-1"u8, out var value));
            Assert.Equal(-1, value);
            Assert.True(cache.TryGetValue("-2"u8, out value));
            Assert.Equal(-2, value);
            cache[Encoding.UTF8.GetBytes(i.ToString())] = i;
        }
    }
}
