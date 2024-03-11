using BTDB.Collections;
using Xunit;

namespace BTDBTest;

public class SpanByteNoRemoveDictionaryTest
{
    [Fact]
    public void SimpleTest()
    {
        var dict = new SpanByteNoRemoveDictionary<int>();
        Assert.Empty(dict);
        Assert.False(dict.TryGetValue(new byte[] { 1, 2, 3 }, out var value));
        Assert.Equal(0, value);
        Assert.Empty(dict);
        dict[new byte[] { 1, 2, 3 }] = 123;
        Assert.Single(dict);
        Assert.True(dict.TryGetValue(new byte[] { 1, 2, 3 }, out value));
        Assert.Equal(123, value);
        Assert.Single(dict);
        Assert.False(dict.TryGetValue(new byte[] { 3, 2, 1 }, out value));
        Assert.Equal(0, value);
        Assert.Single(dict);
        dict[new byte[] { 3, 2, 1 }] = 321;
        Assert.Equal(2, dict.Count);
        Assert.True(dict.TryGetValue(new byte[] { 1, 2, 3 }, out value));
        Assert.Equal(123, value);
        Assert.Equal(2, dict.Count);
        Assert.True(dict.TryGetValue(new byte[] { 3, 2, 1 }, out value));
        Assert.Equal(321, value);
        Assert.Equal(2, dict.Count);
        Assert.False(dict.TryGetValue(new byte[] { 1, 2, 3, 4 }, out value));
        Assert.Equal(0, value);
        Assert.Equal(2, dict.Count);
        dict[new byte[] { 1, 2, 3, 4 }] = 1234;
        Assert.Equal(3, dict.Count);
        Assert.True(dict.TryGetValue(new byte[] { 1, 2, 3 }, out value));
        Assert.Equal(123, value);
        Assert.True(dict.TryGetValue(new byte[] { 3, 2, 1 }, out value));
        Assert.Equal(321, value);
        Assert.True(dict.TryGetValue(new byte[] { 1, 2, 3, 4 }, out value));
        Assert.Equal(1234, value);
    }
}
