using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

public class DBReaderCtxDamageTest
{
    static byte[] Reference(long id)
    {
        var writer = new MemWriter();
        writer.WriteVInt64(id);
        writer.WriteBlock(new byte[16]);
        return writer.GetSpan().ToArray();
    }

    // Before the fix a damaged inline object number grew the list of read objects to it, a null at a time: up to
    // two billion entries for one value.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public unsafe void InlineObjectNumberBeyondRecordIsDamage(bool skip)
    {
        var data = Reference(-(1L << 24));
        fixed (byte* ptr = data)
        {
            var reader = new MemReader(ptr, data.Length);
            var ctx = new DBReaderCtx(null);
            if (skip)
                Assert.Throws<BTDBException>(() => ctx.SkipObject(ref reader));
            else
                Assert.Throws<BTDBException>(() => ctx.ReadObject(ref reader, out _));
        }
    }

    [Fact]
    public unsafe void InlineObjectNumbersWithinRecordAreRead()
    {
        var data = Reference(-3);
        fixed (byte* ptr = data)
        {
            var reader = new MemReader(ptr, data.Length);
            var ctx = new DBReaderCtx(null);
            Assert.True(ctx.ReadObject(ref reader, out var obj));
            Assert.Null(obj);
        }
    }
}
