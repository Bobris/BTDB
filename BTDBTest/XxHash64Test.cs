using System;
using BTDB.Buffer;
using Xunit;

namespace BTDBTest;

public class XxHash64Test
{
    [Fact]
    void EmptyHash()
    {
        var hash = new XxHash64();
        Assert.Equal(0xef46_db37_51d8_e999u, hash.Digest());
        Assert.Equal(0xef46_db37_51d8_e999u, XxHash64.Hash(Array.Empty<byte>()));
    }

    [Fact]
    void HashOfOneByte()
    {
        var hash = new XxHash64();
        hash.Update(new byte[] { 1 });
        Assert.Equal(0x8a41_2781_1b21_e730u, hash.Digest());
        Assert.Equal(0x8a41_2781_1b21_e730u, XxHash64.Hash(new byte[] { 1 }));
    }

    [Fact]
    void HashOf32Bytes()
    {
        var data = new byte[32];
        for (var i = 0; i < 32; i++)
        {
            data[i] = (byte)i;
        }
        Assert.Equal(0xcbf5_9c51_16ff_32b4u, XxHash64.Hash(data));
        var hash = new XxHash64();
        hash.Update(data);
        Assert.Equal(0xcbf5_9c51_16ff_32b4u, hash.Digest());
    }

    [Fact]
    void VariousCombinations()
    {
        var random = new Random(42);
        var data = new byte[254];
        random.NextBytes(data);
        var fullHash = XxHash64.Hash(data);
        for (var i = 0; i < 128; i++)
        {
            for (var j = 0; j < 128; j++)
            {
                var hash = new XxHash64();
                hash.Update(data.AsSpan(0, i));
                hash.Update(data.AsSpan(i,j));
                hash.Update(data.AsSpan(i+j));
                Assert.Equal(fullHash, hash.Digest());
            }
        }
    }
}
