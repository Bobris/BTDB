using System.Threading.Tasks;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class RelationPersistedNameParameterTests : GeneratorTestsBase
{
    [Fact]
    public Task ParameterNameMayUsePersistedName()
    {
        // language=cs
        return VerifySourceGenerator("""
            using BTDB.FieldHandler;
            using BTDB.ODBLayer;

            public class Person
            {
                [PrimaryKey(1)]
                [PersistedName("Tenant")]
                public ulong TenantId { get; set; }

                [PrimaryKey(2)]
                public ulong Id { get; set; }
            }

            public interface IPersonTable : IRelation<Person>
            {
                void Insert(Person person);
                Person FindById(ulong tenant, ulong id);
            }

            """);
    }
}
