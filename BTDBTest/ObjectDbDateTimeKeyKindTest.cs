using System;
using System.Collections.Generic;
using System.Linq;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

// A key keeps a DateTime's kind in its highest bits, so before the fix a local DateTime key sorted after every UTC one,
// missed UTC ranges and could not be found by its UTC equivalent. Keys now refuse local DateTimes like unspecified ones.
public class ObjectDbDateTimeKeyKindTest : ObjectDbTestBase
{
    public ObjectDbDateTimeKeyKindTest(ITestOutputHelper output) : base(output)
    {
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
    public void LocalDateTimeKeysAreRefused()
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
}
