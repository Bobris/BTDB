using System.Collections.Generic;
using System.Collections.ObjectModel;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbIListCollectionRegressionTest : ObjectDbTestBase
{
    public ObjectDbIListCollectionRegressionTest(ITestOutputHelper output) : base(output)
    {
    }

    [Generate]
    public class SenStructureLike
    {
        [PrimaryKey]
        public ulong CompanyId { get; set; }

        public IList<SenTypeLike> Types { get; set; } = null!;
    }

    [Generate]
    public class SenTypeLike
    {
        public string Name { get; set; } = string.Empty;
        public IList<SenRecordParameterLike> RecordParameters { get; set; } = null!;
    }

    [Generate]
    public class SenRecordParameterLike
    {
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
    }

    public interface ISenStructureLikeTable : IRelation<SenStructureLike>
    {
    }

    [Fact]
    public void UpsertOfIListPropertyBackedByCollectionShouldWork()
    {
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<ISenStructureLikeTable>();

        table.Upsert(new SenStructureLike
        {
            CompanyId = 1,
            Types =
            [
                new SenTypeLike
                {
                    Name = "TypeA",
                    RecordParameters = new Collection<SenRecordParameterLike>
                    {
                        new()
                        {
                            Name = "ParamA",
                            Description = "DescriptionA"
                        }
                    }
                }
            ]
        });

        var loaded = Assert.Single(table);
        var loadedType = Assert.Single(loaded.Types);
        var loadedParameter = Assert.Single(loadedType.RecordParameters);
        Assert.Equal("ParamA", loadedParameter.Name);
        Assert.Equal("DescriptionA", loadedParameter.Description);
    }
}