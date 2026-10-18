using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using BTDB;
using BTDB.ODBLayer;
using BTDB.Serialization;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// RawData.GetSizeAndAlign drives the offsets at which serializers read and write values inside Nullable, ValueTuple
/// and KeyValuePair, so it must match the runtime layout exactly.
public class RawDataLayoutTest : ObjectDbTestBase
{
    public RawDataLayoutTest(ITestOutputHelper output) : base(output)
    {
    }

    struct AlignProbe<T>
    {
        public byte Before;
        public T Value;
    }

    static (uint Size, uint Align) Actual<T>()
    {
        var probe = new AlignProbe<T>();
        var align = (uint)Unsafe.ByteOffset(ref Unsafe.As<AlignProbe<T>, byte>(ref probe),
            ref Unsafe.As<T, byte>(ref probe.Value));
        return ((uint)Unsafe.SizeOf<T>(), align);
    }

    public struct ThreeShorts
    {
        public short A, B, C;
    }

    public struct LongAndByte
    {
        public long A;
        public byte B;
    }

    public struct ThreeReferences
    {
        public object A, B, C;
    }

    public struct IntAndTwoBytes
    {
        public int A;
        public byte B, C;
    }

    static readonly Dictionary<string, (Type Type, Func<(uint, uint)> Actual)> Types = new()
    {
        ["DateTimeOffset"] = (typeof(DateTimeOffset), Actual<DateTimeOffset>),
        ["TimeSpan"] = (typeof(TimeSpan), Actual<TimeSpan>),
        ["DateTime"] = (typeof(DateTime), Actual<DateTime>),
        ["Guid"] = (typeof(Guid), Actual<Guid>),
        ["decimal"] = (typeof(decimal), Actual<decimal>),
        ["ThreeShorts"] = (typeof(ThreeShorts), Actual<ThreeShorts>),
        ["LongAndByte"] = (typeof(LongAndByte), Actual<LongAndByte>),
        ["ThreeReferences"] = (typeof(ThreeReferences), Actual<ThreeReferences>),
        ["IntAndTwoBytes"] = (typeof(IntAndTwoBytes), Actual<IntAndTwoBytes>),
        ["DateTimeOffset?"] = (typeof(DateTimeOffset?), Actual<DateTimeOffset?>),
        ["ThreeShorts?"] = (typeof(ThreeShorts?), Actual<ThreeShorts?>),
        ["(byte, DateTimeOffset)"] = (typeof((byte, DateTimeOffset)), Actual<(byte, DateTimeOffset)>),
        ["(DateTimeOffset, int, byte)"] = (typeof((DateTimeOffset, int, byte)), Actual<(DateTimeOffset, int, byte)>),
        ["KeyValuePair<int, DateTimeOffset>"] = (typeof(KeyValuePair<int, DateTimeOffset>), Actual<KeyValuePair<int, DateTimeOffset>>)
    };

    public static IEnumerable<object[]> TypeNames => Types.Keys.Select(k => new object[] { k });

    [Theory]
    [MemberData(nameof(TypeNames))]
    public void SizeAndAlignMatchRuntimeLayout(string name)
    {
        var (type, actual) = Types[name];
        Assert.Equal(actual(), RawData.GetSizeAndAlign(type));
    }

    [Generate]
    public class RowWithNullableStructs
    {
        [PrimaryKey(1)] public uint Id { get; set; }
        public DateTimeOffset? Seen { get; set; }
        public (byte, DateTimeOffset) Pair { get; set; }
        public DateTimeOffset Plain { get; set; }
    }

    public interface IRowWithNullableStructsTable : IRelation<RowWithNullableStructs>
    {
        void Insert(RowWithNullableStructs row);
    }

    [Generate]
    public class EventWithNullableStructs
    {
        public DateTimeOffset? Seen { get; set; }
        public DateTimeOffset? Missing { get; set; }
        public int After { get; set; }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void NullableDateTimeOffsetRoundTripsInEvents(EventLayer layer)
    {
        var value = new DateTimeOffset(new DateTime(2025, 3, 4, 5, 6, 7), TimeSpan.FromHours(3));
        var result = (EventWithNullableStructs)EventLayerRoundTripHelper.RoundTrip(layer,
            new EventWithNullableStructs { Seen = value, After = 42 })!;
        Assert.Equal(value, result.Seen);
        Assert.Equal(value.Offset, result.Seen!.Value.Offset);
        Assert.Null(result.Missing);
        Assert.Equal(42, result.After);
    }

    [Fact]
    public void NullableDateTimeOffsetAndTupleWithDateTimeOffsetRoundTripInRelation()
    {
        var value = new DateTimeOffset(new DateTime(2025, 3, 4, 5, 6, 7), TimeSpan.FromHours(-2));
        using (var tr = _db.StartTransaction())
        {
            var table = tr.GetRelation<IRowWithNullableStructsTable>();
            table.Insert(new() { Id = 1, Seen = value, Pair = (7, value), Plain = value });
            table.Insert(new() { Id = 2, Seen = null, Pair = (8, DateTimeOffset.MinValue), Plain = DateTimeOffset.MaxValue });
            tr.Commit();
        }

        using var readTr = _db.StartReadOnlyTransaction();
        var rows = readTr.GetRelation<IRowWithNullableStructsTable>().ToList();
        Assert.Equal(value, rows[0].Seen);
        Assert.Equal(value.Offset, rows[0].Seen!.Value.Offset);
        Assert.Equal((byte)7, rows[0].Pair.Item1);
        Assert.Equal(value, rows[0].Pair.Item2);
        Assert.Equal(value.Offset, rows[0].Pair.Item2.Offset);
        Assert.Equal(value, rows[0].Plain);
        Assert.Null(rows[1].Seen);
        Assert.Equal(((byte)8, DateTimeOffset.MinValue), rows[1].Pair);
        Assert.Equal(DateTimeOffset.MaxValue, rows[1].Plain);
    }
}
