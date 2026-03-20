using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class EmptyClassGenerateTest : ObjectDbTestBase
{
    public EmptyClassGenerateTest(ITestOutputHelper output) : base(output)
    {
    }

    [Generate]
    public interface IMarkerInterface
    {
    }

    public class EmptyData : IMarkerInterface
    {
    }

    public class Container
    {
        [PrimaryKey(1)] public ulong Id { get; set; }
        public object Data { get; set; }
    }

    public interface IContainerTable : IRelation<Container>
    {
        void Insert(Container item);
        Container FindById(ulong id);
    }

    [Fact]
    public void EmptyClassWithGenerateCanBeStoredAsPolymorphicField()
    {
        _db.RegisterType(typeof(EmptyData));
        using var tr = _db.StartTransaction();
        var table = tr.GetRelation<IContainerTable>();
        table.Insert(new Container { Id = 1, Data = new EmptyData() });
        var loaded = table.FindById(1);
        Assert.IsType<EmptyData>(loaded.Data);
        tr.Commit();
    }
}
