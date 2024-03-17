using System;
using System.Linq;
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

    [Fact]
    void RandomWalkTest()
    {
        var cache = new SpanByteLruCache<int>(32);
        var cache2 = new LruCache<string, int>(32);
        var random = new Random(1);
        for (var i = 0; i < 10000; i++)
        {
            if (random.Next(100000) == 0)
            {
                cache.Clear();
                cache2.Clear();
            }
            else
            {
                var key = RandomString(random);
                var key8 = Encoding.UTF8.GetBytes(key);
                if (random.Next(3) == 0)
                {
                    var removed = cache.Remove(key8);
                    var removed2 = cache2.Remove(key);
                    Assert.Equal(removed, removed2);
                }
                else
                {
                    var value = random.Next();
                    cache[key8] = value;
                    cache2[key] = value;
                }
            }

            Assert.Equal(cache2.Count, cache.Count);
            foreach (var pair in cache2.Zip(cache))
            {
                Assert.Equal(pair.First.Key, Encoding.UTF8.GetString(pair.Second.Key));
                Assert.Equal(pair.First.Value, pair.Second.Value);
            }
        }
    }

    static string RandomString(Random random)
    {
        var length = random.Next(1, 500);
        var result = new StringBuilder(length);
        for (var i = 0; i < length; i++)
        {
            result.Append((char)random.Next('a', 'z' + 1));
        }

        return result.ToString();
    }
}
