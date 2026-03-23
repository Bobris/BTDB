using System;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class SchedulerRelationMetadataRegressionTest : ObjectDbTestBase
{
    public SchedulerRelationMetadataRegressionTest(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void GeneratedSchedulerRelationShouldMatchReflectionMetadata()
    {
        using var tr = _db.StartTransaction();

        var exception = Record.Exception(() => tr.GetRelation<ISchedulerTable>());

        Assert.Null(exception);
    }

    [Generate]
    [GenerateFor(typeof(ISchedulerTable))]
    public class SchedulerItem
    {
        [PrimaryKey(2)]
        public ulong SchedulerId { get; set; }

        [PrimaryKey(1)]
        public ulong CompanyId { get; set; }

        [SecondaryKey("ScheduledForDateTime")]
        public SchedulerStatus Status { get; set; }

        [SecondaryKey("ScheduledForDateTime", Order = 2)]
        public DateTime? ScheduledForDateTime { get; set; }
    }

    [Generate]
    public interface ISchedulerTable : IRelation<SchedulerItem>;

    public enum SchedulerStatus
    {
        Idle,
        Processing
    }
}
