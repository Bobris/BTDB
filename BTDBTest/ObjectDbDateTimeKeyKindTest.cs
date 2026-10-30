using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.FieldHandler;
using BTDB.ODBLayer;
using BTDB.StreamLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// A key keeps a DateTime's kind in its highest bits, so before the fix a local DateTime key sorted after every UTC one,
// missed UTC ranges and could not be found by its UTC equivalent. With MemWriter.ForbidLocalDateTimeKind keys refuse
// local DateTimes like unspecified ones.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class ObjectDbDateTimeKeyKindTest : ObjectDbTestBase
{
    public ObjectDbDateTimeKeyKindTest(ITestOutputHelper output) : base(output)
    {
    }

    static void WithForbidLocal(bool forbid, Action action)
    {
        var old = MemWriter.ForbidLocalDateTimeKind;
        try
        {
            MemWriter.ForbidLocalDateTimeKind = forbid;
            action();
        }
        finally
        {
            MemWriter.ForbidLocalDateTimeKind = old;
        }
    }

    public class Reading
    {
        [PrimaryKey]
        public DateTime At { get; set; }

        [SecondaryKey("Taken")]
        public DateTime Taken { get; set; }

        public DateTime Noted { get; set; }
    }

    public interface IReadingTable : IRelation<Reading>
    {
        Reading? FindByIdOrDefault(DateTime at);
        IEnumerable<Reading> ListByTaken(DateTime taken);
    }

    [Fact]
    public void LocalDateTimeKeysAreAcceptedByDefault()
    {
        Assert.False(MemWriter.ForbidLocalDateTimeKind);
        var utc = new DateTime(2020, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var local = utc.ToLocalTime();
        Assert.Equal(local, Constraint.DateTime.ForbidUnspecifiedKind(local));

        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IReadingTable>();
        table.Upsert(new Reading { At = local, Taken = local });
        Assert.Equal(local, table.FindByIdOrDefault(local)!.At);
        Assert.Single(table.ListByTaken(local));
        var nullableTable = tr.GetRelation<INullableReadingTable>();
        nullableTable.Upsert(new NullableReading { At = local, Taken = local });
        Assert.Equal(local, nullableTable.FindByIdOrDefault(local)!.At);
        Assert.Single(nullableTable.ListByTaken(local));
        // Unspecified kind stays refused.
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            table.Upsert(new Reading { At = DateTime.SpecifyKind(utc, DateTimeKind.Unspecified), Taken = utc }));
    }

    [Fact]
    public void LocalDateTimeKeysAreRefusedWhenForbidden() => WithForbidLocal(true, RefuseLocalDateTimeKeys);

    void RefuseLocalDateTimeKeys()
    {
        var utc = new DateTime(2020, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var local = utc.ToLocalTime();
        // A refused write may leave the transaction partly written, so each one runs in a transaction not committed.
        foreach (var refused in new Action<IReadingTable>[]
                 {
                     t => t.Upsert(new Reading { At = local, Taken = utc }),
                     t => t.Upsert(new Reading { At = utc, Taken = local }),
                     t => t.FindByIdOrDefault(local)
                 })
        {
            using var tr = _db.StartTransaction();
            Assert.Throws<ArgumentOutOfRangeException>(() => refused(tr.GetRelation<IReadingTable>()));
        }

        Assert.Throws<ArgumentOutOfRangeException>(() => Constraint.DateTime.Exact(local));

        // A value that is no key may stay local.
        using var write = _db.StartTransaction();
        var table = write.GetRelation<IReadingTable>();
        Assert.Equal(0, table.Count);
        table.Upsert(new Reading { At = utc, Taken = utc, Noted = local });
        Assert.Equal(DateTimeKind.Local, table.FindByIdOrDefault(utc)!.Noted.Kind);
        Assert.Single(table.ListByTaken(utc));
    }

    public class NullableReading
    {
        [PrimaryKey]
        public DateTime? At { get; set; }

        [SecondaryKey("Taken")]
        public DateTime? Taken { get; set; }
    }

    public interface INullableReadingTable : IRelation<NullableReading>
    {
        NullableReading? FindByIdOrDefault(DateTime? at);
        IEnumerable<NullableReading> ListByTaken(DateTime? taken);
    }

    // A nullable DateTime key stores its value with the handler of values, which did not check the kind.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LocalAndUnspecifiedNullableDateTimeKeysAreRefusedWhenForbidden(bool noEmit)
    {
        var old = IFieldHandler.UseNoEmitForRelations;
        try
        {
            IFieldHandler.UseNoEmitForRelations = noEmit;
            ObjectDB.ResetAllMetadataCaches();
            WithForbidLocal(true, RefuseLocalAndUnspecifiedNullableDateTimeKeys);
        }
        finally
        {
            IFieldHandler.UseNoEmitForRelations = old;
            ObjectDB.ResetAllMetadataCaches();
        }
    }

    void RefuseLocalAndUnspecifiedNullableDateTimeKeys()
    {
        var utc = new DateTime(2020, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var local = utc.ToLocalTime();
        var unspecified = DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);
        foreach (var refused in new Action<INullableReadingTable>[]
                 {
                     t => t.Upsert(new NullableReading { At = local, Taken = utc }),
                     t => t.Upsert(new NullableReading { At = unspecified, Taken = utc }),
                     t => t.Upsert(new NullableReading { At = utc, Taken = local }),
                     t => t.FindByIdOrDefault(local)
                 })
        {
            using var tr = _db.StartTransaction();
            Assert.Throws<ArgumentOutOfRangeException>(() => refused(tr.GetRelation<INullableReadingTable>()));
        }

        using var write = _db.StartTransaction();
        var table = write.GetRelation<INullableReadingTable>();
        Assert.Equal(0, table.Count);
        table.Upsert(new NullableReading { At = utc, Taken = null });
        table.Upsert(new NullableReading { At = null, Taken = utc });
        table.Upsert(new NullableReading { At = DateTime.MinValue, Taken = DateTime.MaxValue });
        Assert.Equal([null, DateTime.MinValue, utc], table.Select(r => r.At));
        Assert.Single(table.ListByTaken(utc));
        Assert.Equal(utc, table.FindByIdOrDefault(utc)!.At);
    }
}
