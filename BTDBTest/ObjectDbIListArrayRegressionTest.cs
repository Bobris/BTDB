using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbIListArrayRegressionTest : ObjectDbTestBase
{
    public ObjectDbIListArrayRegressionTest(ITestOutputHelper output) : base(output)
    {
    }

    public class JourneyMapLikeRow
    {
        [PrimaryKey]
        public ulong Id { get; set; }

        public IList<ulong> Phases { get; set; } = null!;
    }

    public interface IJourneyMapLikeTable : IRelation<JourneyMapLikeRow>
    {
    }

    [Fact]
    public void UpsertOfIListPropertyBackedByArrayShouldWork()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IJourneyMapLikeTable>();

        table.Upsert(new JourneyMapLikeRow
        {
            Id = 123,
            Phases = new ulong[] { 4 }
        });

        Assert.Equal(new ulong[] { 4 }, table.Single().Phases);
    }
}