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

    [Fact]
    public Task VerifyRelationWithSecondaryKey()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)] public int ParentId { get; set; }
                [PrimaryKey(2)] [SecondaryKey("PersonId", Order = 1) public int PersonId { get; set; }
                [PrimaryKey(3, true)] public string Name { get; set; } = null!;
                [SecondaryKey("LowerCaseName", IncludePrimaryKeyOrder = 1, Order = 2)] public string LowerCaseName => Name.ToLower();
                [InKeyValue(4)] public string Description { get; set; } = null!;
            }

            public interface IPersonTable : IRelation<Person>
            {
            }
            """);
    }

    [Fact]
    public Task VerifyCannotUsePrimaryKeyTogetherWithInKeyValue()
    {
        // language=cs
        return VerifySourceGenerator(@"
            using BTDB.ODBLayer;

            namespace TestNamespace;

            public class Person
            {
                [PrimaryKey(1)]
                [InKeyValue(2)] public int Id { get; set; }
            }

            public interface IPersonTable : IRelation<Person>
            {
            }
            ");
    }
}
