using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class RelationFindByIdOrDefaultCompatibilityTests : GeneratorTestsBase
{
    [Fact]
    public Task FindByIdOrDefaultAllowsCompatibleUnsignedParameter()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)] public uint Id { get; set; }
            }

            public interface IPersonTable : IRelation<Person>
            {
                void Insert(Person person);
                Person FindByIdOrDefault(ulong id);
            }

            """);
    }
}
