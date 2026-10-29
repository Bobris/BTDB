using System;
using BTDB;
using BTDB.ODBLayer;
using BTDB.KVDBLayer;
using Xunit;
using Xunit.Abstractions;

namespace BTDBTest;

public class ObjectDbTypeNameClashTest : ObjectDbTestBase
{
    public ObjectDbTypeNameClashTest(ITestOutputHelper output) : base(output)
    {
    }

    public static class First
    {
        [Generate]
        public class Person
        {
            public string? Name { get; set; }
        }
    }

    public static class Second
    {
        [Generate]
        public class Person
        {
            public int Age { get; set; }
        }
    }

    // Before the fix both types took the name Person, as auto-registration names a type without its namespace or
    // enclosing type, and a stored Second.Person was loaded as a First.Person after reopening.
    [Fact]
    public void RegisteringOtherTypeUnderTakenNameThrows()
    {
        Assert.Equal("Person", _db.RegisterType(typeof(First.Person)));
        var ex = Assert.Throws<BTDBException>(() => _db.RegisterType(typeof(Second.Person)));
        Assert.Contains("Name Person is already registered", ex.Message);
        Assert.DoesNotContain(typeof(Second.Person), _db.GetPolymorphicTypes(typeof(object)));
        Assert.Equal("Person2", _db.RegisterType(typeof(Second.Person), "Person2"));
        Assert.Equal("Person", _db.RegisterType(typeof(First.Person)));
    }

    public interface IShape;

    [Generate]
    public class Shape : IShape
    {
        public int Sides { get; set; }
    }

    public class Drawing
    {
        [PrimaryKey] public ulong Id { get; set; }
        public IShape? Shape { get; set; }
    }

    public interface IDrawings : IRelation<Drawing>
    {
        void Insert(Drawing drawing);
        Drawing FindById(ulong id);
    }

    // Before the fix the interface, registered as Shape by the relation's field, kept the name, and loading the stored
    // Shape tried to create an instance of the interface.
    [Fact]
    public void ImplementationRegisteredAfterItsInterfaceLoads()
    {
        using (var tr = _db.StartTransaction())
        {
            var drawings = tr.InitRelation<IDrawings>("Drawings")(tr);
            _db.RegisterType(typeof(Shape));
            drawings.Insert(new Drawing { Id = 1, Shape = new Shape { Sides = 3 } });
            tr.Commit();
        }

        ReopenDb();
        using var read = _db.StartTransaction();
        var loaded = read.InitRelation<IDrawings>("Drawings")(read);
        _db.RegisterType(typeof(Shape));
        Assert.Equal(3, Assert.IsType<Shape>(loaded.FindById(1).Shape).Sides);
    }

    // Before the fix a read before the implementation was registered bound the table to the interface, failing with
    // MemberAccessException then and for the rest of the session.
    [Fact]
    public void ReadingBeforeImplementationIsRegisteredDoesNotBindTableToInterface()
    {
        using (var tr = _db.StartTransaction())
        {
            var drawings = tr.InitRelation<IDrawings>("Drawings")(tr);
            _db.RegisterType(typeof(Shape));
            drawings.Insert(new Drawing { Id = 1, Shape = new Shape { Sides = 3 } });
            tr.Commit();
        }

        ReopenDb();
        Func<IObjectDBTransaction, IDrawings> creator;
        using (var tr = _db.StartTransaction())
        {
            creator = tr.InitRelation<IDrawings>("Drawings");
            var ex = Assert.Throws<BTDBException>(() => creator(tr).FindById(1));
            Assert.Contains("not registered", ex.Message);
        }

        _db.RegisterType(typeof(Shape));
        using var read = _db.StartTransaction();
        Assert.Equal(3, Assert.IsType<Shape>(creator(read).FindById(1).Shape).Sides);
    }
}
