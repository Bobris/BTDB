using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class RelationTests : GeneratorTestsBase
{
    [Fact]
    public Task VerifyBasicRelation()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public int ParentId { get; set; }
                [PrimaryKey(2)] public int PersonId { get; set; }
                public string Name { get; set; } = null!;
            }

            public interface IPersonTable : ICovariantRelation<Person>
            {
            }
            """);
    }
}
