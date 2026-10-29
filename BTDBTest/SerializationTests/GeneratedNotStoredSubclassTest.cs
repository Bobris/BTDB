using System.Linq;
using BTDB;
using BTDB.Bon;
using BTDB.ODBLayer;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests;

public class GeneratedNotStoredSubclassTest
{
    public class TransientAttribute : NotStoredAttribute
    {
    }

    [Generate]
    public class WithTransientMembers
    {
        public int Stored { get; set; }
        [Transient] public int TransientProperty { get; set; }
        [Transient] public int TransientField;
    }

    [Fact]
    public void MembersMarkedWithNotStoredSubclassAreNotInMetadata()
    {
        var metadata = ReflectionMetadata.FindByType(typeof(WithTransientMembers));
        Assert.NotNull(metadata);
        Assert.Equal(["Stored"], metadata.Fields.Select(f => f.Name));
    }

    [Fact]
    public void BonDoesNotStoreMembersMarkedWithNotStoredSubclass()
    {
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder,
            new WithTransientMembers { Stored = 1, TransientProperty = 2, TransientField = 3 });
        var bon = new Bon(builder.FinishAsMemory());
        var res = Assert.IsType<WithTransientMembers>(BonSerializerFactory.Deserialize(ref bon));
        Assert.Equal(1, res.Stored);
        Assert.Equal(0, res.TransientProperty);
        Assert.Equal(0, res.TransientField);
    }
}
