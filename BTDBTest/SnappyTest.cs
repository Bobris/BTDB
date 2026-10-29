using System;
using BTDB.Buffer;
using Xunit;
using BTDB.SnappyCompression;

namespace BTDBTest;

public class SnappyTest
{
    [Fact]
    public void Basic()
    {
        var s = new byte[] { 1, 2, 3, 1, 2, 3, 1, 2, 3 };
        var t = new byte[10];
        var r = SnappyCompress.Compress(t, s);
        Assert.Equal(7, r);
        var d = SnappyDecompress.Decompress(ByteBuffer.NewSync(t, 0, r));
        Assert.Equal(s, d.AsSyncReadOnlySpan());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(100000)]
    [InlineData(1000000)]
    [InlineData(10000000)]
    public void RandomData(int len)
    {
        var s = new byte[len];
        new Random(1234567).NextBytes(s);
        RoundTrip(s);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(100000)]
    [InlineData(1000000)]
    [InlineData(10000000)]
    public void ConstantData(int len)
    {
        var s = new byte[len];
        RoundTrip(s);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(15)]
    [InlineData(30)]
    [InlineData(60)]
    [InlineData(120)]
    [InlineData(240)]
    [InlineData(255)]
    public void RepeatingData(int len)
    {
        var s = new byte[len * 10];
        for (int i = 0; i < s.Length; i++) s[i] = (byte)(i % len);
        RoundTrip(s);
        s = new byte[len * 2];
        for (int i = 0; i < s.Length; i++) s[i] = (byte)(i % len);
        RoundTrip(s);
        s = new byte[len * 2 + len / 2];
        for (int i = 0; i < s.Length; i++) s[i] = (byte)(i % len);
        RoundTrip(s);
    }

    [Theory]
    [InlineData(1000)]
    [InlineData(1000000)]
    public void HighlyCompressibleDataRoundTrips(int len)
    {
        var compressed = new byte[len + 32];
        var compressedLength = SnappyCompress.Compress(compressed, new byte[len]);
        var decompressed = SnappyDecompress.Decompress(ByteBuffer.NewSync(compressed, 0, compressedLength));
        Assert.Equal(new byte[len], decompressed.AsSyncReadOnlySpan());
    }

    // Before the fix a copy with offset 0 copied a byte onto itself and the damaged data decompressed.
    [Fact]
    public void CopyWithOffsetZeroIsRefused()
    {
        Assert.Throws<System.IO.InvalidDataException>(() =>
            SnappyDecompress.Decompress(new byte[] { 5, 0x00, (byte)'A', 0x01, 0x00 }));
    }

    // Before the fix a declared size of nearly 2 GB in 6 bytes was allocated, failing with OutOfMemoryException.
    [Fact]
    public void ImpossibleDecompressedSizeIsRefusedBeforeAllocating()
    {
        Assert.Throws<System.IO.InvalidDataException>(() =>
            SnappyDecompress.Decompress(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0x00 }));
        Assert.Throws<System.IO.InvalidDataException>(() =>
            SnappyDecompress.Decompress(ByteBuffer.NewSync(new byte[] { 0x80, 0x80, 0x80, 0x01, 0x00 })));
    }

    void RoundTrip(byte[] source)
    {
        var compressed = new byte[(long)source.Length * 6 / 5 + 32];
        var compressedLength = SnappyCompress.Compress(compressed, source);
        var decompressed = SnappyDecompress.Decompress(ByteBuffer.NewSync(compressed, 0, compressedLength));
        Assert.Equal(source, decompressed.AsSyncReadOnlySpan());
        compressed = new byte[compressedLength / 2];
        Assert.Equal(-1, SnappyCompress.Compress(compressed, source));
    }
}
