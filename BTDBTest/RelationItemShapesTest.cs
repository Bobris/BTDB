using System.Collections.Generic;
using System.Linq;
using BTDB;
using BTDB.ODBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

/// Relation items whose stored properties come from a generic base class and from an explicit interface
/// implementation keep them across reopening, and the generated metadata agrees with reflection on them.
public class RelationItemShapesTest : ObjectDbTestBase
{
    public RelationItemShapesTest(ITestOutputHelper output) : base(output)
    {
    }

    public interface IHasNote
    {
        string Note { get; set; }
    }

    public class ShapeRowBase<TKey>
    {
        [PrimaryKey(1)] public TKey Id { get; set; } = default!;
        public List<TKey>? Others { get; set; }
    }

    [Generate]
    public class ShapeRow : ShapeRowBase<ulong>, IHasNote
    {
        [SecondaryKey("Name")] public string Name { get; set; } = "";
        string IHasNote.Note { get; set; } = "";
        public int Count { get; set; }
    }

    public interface IShapeRowTable : IRelation<ShapeRow>
    {
        void Insert(ShapeRow row);
        ShapeRow FindById(ulong id);
        IEnumerable<ShapeRow> FindByName(string name);
    }

    // Before the fix the explicitly implemented property was stored also under the interface property's name, through
    // a backing field that does not exist, which failed loading the assembly and disagreed with reflection.
    [Fact]
    public void GenericBaseAndExplicitInterfacePropertiesRoundTrip()
    {
        using (var tr = _db.StartTransaction())
        {
            var row = new ShapeRow { Id = 5, Others = [1, 2], Name = "n", Count = 3 };
            ((IHasNote)row).Note = "note";
            tr.GetRelation<IShapeRowTable>().Insert(row);
            tr.Commit();
        }

        ReopenDb();
        using (var tr = _db.StartReadOnlyTransaction())
        {
            var table = tr.GetRelation<IShapeRowTable>();
            var read = table.FindById(5);
            Assert.Equal((5ul, "n", 3, "note"), (read.Id, read.Name, read.Count, ((IHasNote)read).Note));
            Assert.Equal([1ul, 2ul], read.Others!);
            Assert.Single(table.FindByName("n"));
            Assert.Equal(5ul, table.Query<ShapeRow>().Where(r => r.Count == 3).Single().Id);
        }
    }
}
