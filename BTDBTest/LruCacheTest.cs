using BTDB.Collections;
using Xunit;

namespace BTDBTest;

public class LruCacheTest
{
    [Fact]
    public void SimpleTest()
    {
        var cache = new LruCache<int, int>(2);
        Assert.Empty(cache);
        Assert.False(cache.TryGetValue(1, out var value));
        Assert.Equal(0, value);
        Assert.Empty(cache);
        cache[1] = 11;
        Assert.Single(cache);
        Assert.True(cache.TryGetValue(1, out value));
        Assert.Equal(11, value);
        Assert.Single(cache);
        Assert.False(cache.TryGetValue(2, out value));
        Assert.Equal(0, value);
        Assert.Single(cache);
        cache[2] = 22;
        Assert.Equal(2, cache.Count);
        Assert.True(cache.TryGetValue(1, out value));
        Assert.Equal(11, value);
        Assert.Equal(2, cache.Count);
        Assert.True(cache.TryGetValue(2, out value));
        Assert.Equal(22, value);
        Assert.Equal(2, cache.Count);
        Assert.False(cache.TryGetValue(3, out value));
        Assert.Equal(0, value);
        Assert.Equal(2, cache.Count);
        cache[3] = 33;
        Assert.Equal(2, cache.Count);
        Assert.False(cache.TryGetValue(1, out value));
        Assert.Equal(0, value);
        Assert.True(cache.TryGetValue(2, out value));
        Assert.Equal(22, value);
        Assert.True(cache.TryGetValue(3, out value));
        Assert.Equal(33, value);
    }

    [Fact]
    void CapacityIncreasingTest()
    {
        var cache = new LruCache<int, int>(100);
        for (var i = 0; i < 128; i++)
        {
            cache[i] = i;
        }

        // overflow capacity
        cache[128] = 128;

        Assert.Equal(128, cache.Count);
        for (var i = 1; i < 129; i++)
        {
            Assert.True(cache.TryGetValue(i, out var value));
            Assert.Equal(i, value);
        }

        Assert.False(cache.TryGetValue(0, out _));
    }

    [Fact]
    void RemoveTest()
    {
        var cache = new LruCache<int, int>(100);
        for (var i = 0; i < 128; i++)
        {
            cache[i] = i;
        }

        for (var i = 0; i < 128; i += 2)
        {
            cache.Remove(i);
        }

        Assert.Equal(64, cache.Count);
        for (var i = 1; i < 129; i += 2)
        {
            Assert.True(cache.TryGetValue(i, out var value));
            Assert.Equal(i, value);
        }

        for (var i = 0; i < 128; i += 2)
        {
            Assert.False(cache.TryGetValue(i, out var value));
            Assert.Equal(0, value);
        }
    }

    [Fact]
    void ComplexTest()
    {
        var cache = new LruCache<int, int>(4);
        cache[-1] = -1;
        cache[-2] = -2;
        for (var i = 0; i < 100; i++)
        {
            Assert.True(cache.TryGetValue(-1, out var value));
            Assert.Equal(-1, value);
            Assert.True(cache.TryGetValue(-2, out value));
            Assert.Equal(-2, value);
            cache[i] = i;
        }
    }
}
