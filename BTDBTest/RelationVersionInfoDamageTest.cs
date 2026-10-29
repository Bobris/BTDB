using System;
using System.IO;
using BTDB.KVDBLayer;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;

namespace BTDBTest;

/// Stored relation metadata names how many key fields, secondary keys and fields follow. Before the fix a damaged count
/// sized arrays and collections before they were read, so opening failed with ArgumentOutOfRangeException or
/// OverflowException, or allocated gigabytes, instead of reporting the damage.
public class RelationVersionInfoDamageTest
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void DamagedCountIsReported(int countsBefore)
    {
        var writer = new MemWriter();
        for (var i = 0; i < countsBefore; i++) writer.WriteVUInt32(0);
        // The damaged count is the last thing stored: what follows it is missing, as in a truncated value.
        writer.WriteVUInt32((1u << 31) + 1);
        var bytes = writer.GetSpan().ToArray();
        var before = GC.GetAllocatedBytesForCurrentThread();
        Exception? error = null;
        try
        {
            var reader = MemReader.CreateFromPinnedSpan(bytes);
            RelationVersionInfo.LoadUnresolved(ref reader, "Damaged");
        }
        catch (Exception e)
        {
            error = e;
        }

        Assert.True(error is BTDBException or EndOfStreamException, error?.ToString());
        Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 1 << 20);
    }

    // The same for an object table's stored fields and for an enum's stored configuration, whose names and values were
    // allocated for the count before any was read.
    [Fact]
    public void DamagedTableFieldCountIsReported()
    {
        var writer = new MemWriter();
        writer.WriteVUInt32((1u << 31) + 1);
        var bytes = writer.GetSpan().ToArray();
        var before = GC.GetAllocatedBytesForCurrentThread();
        var reader = MemReader.CreateFromPinnedSpan(bytes);
        try
        {
            TableVersionInfo.Load(ref reader, null!, "Damaged");
            Assert.Fail("Loaded damaged table metadata.");
        }
        catch (BTDBException)
        {
        }

        Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 1 << 20);
    }

    [Fact]
    public void DamagedEnumConfigurationCountIsReported()
    {
        var writer = new MemWriter();
        writer.WriteVUInt32((1u << 28) << 2);
        var bytes = writer.GetSpan().ToArray();
        var before = GC.GetAllocatedBytesForCurrentThread();
        Assert.Throws<BTDBException>(() => new BTDB.FieldHandler.EnumFieldHandler.EnumConfiguration(bytes));
        Assert.InRange(GC.GetAllocatedBytesForCurrentThread() - before, 0, 1 << 20);
    }
}
