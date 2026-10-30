using System;
using System.Collections.Generic;
using System.Linq;
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

    [GenerateFor(typeof(Wrapper<IShape>))]
    [GenerateFor(typeof(Wrapper<Shape>))]
    public class Wrapper<T>
    {
        public T? Value { get; set; }
    }

    // Before the fix a generic argument lost the I of its interface too, so Wrapper<IShape> and Wrapper<Shape>, both
    // stored, took the name Wrapper<Shape>, and one loaded as the other.
    [Fact]
    public void GenericArgumentKeepsInterfacePrefix()
    {
        Assert.Equal("Wrapper<Shape>", _db.RegisterType(typeof(Wrapper<Shape>)));
        Assert.Equal("Wrapper<IShape>", _db.RegisterType(typeof(Wrapper<IShape>)));
        Assert.Equal("Wrapper<IList<IShape>>", _db.RegisterType(typeof(Wrapper<IList<IShape>>)));
        Assert.Equal("Shape", _db.RegisterType(typeof(IShape)));
    }

    // Objects an earlier version stored under the name without the I load by it, as the type registered under it when
    // there is one, as before, and as the type the name was made from otherwise.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ObjectsStoredUnderNameWithoutInterfacePrefixLoad(bool otherTypeOfNameRegistered)
    {
        _db.RegisterType(typeof(Shape));
        _db.RegisterType(typeof(Wrapper<IShape>), "Wrapper<Shape>");
        ulong oldOid;
        using (var tr = _db.StartTransaction())
        {
            oldOid = tr.Store(new Wrapper<IShape> { Value = new Shape { Sides = 4 } });
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(Shape));
        if (otherTypeOfNameRegistered) _db.RegisterType(typeof(Wrapper<Shape>));
        _db.RegisterType(typeof(Wrapper<IShape>));
        ulong newOid;
        using (var tr = _db.StartTransaction())
        {
            if (otherTypeOfNameRegistered)
                Assert.Equal(4, Assert.IsType<Wrapper<Shape>>(tr.Get(oldOid)).Value!.Sides);
            else
                Assert.Equal(4, Assert.IsType<Shape>(Assert.IsType<Wrapper<IShape>>(tr.Get(oldOid)).Value).Sides);
            newOid = tr.Store(new Wrapper<IShape> { Value = new Shape { Sides = 5 } });
            tr.Commit();
        }

        ReopenDb();
        _db.RegisterType(typeof(Shape));
        _db.RegisterType(typeof(Wrapper<IShape>));
        using var read = _db.StartTransaction();
        Assert.Equal(5, Assert.IsType<Shape>(Assert.IsType<Wrapper<IShape>>(read.Get(newOid)).Value).Sides);
        Assert.IsType<Wrapper<IShape>>(read.Get(oldOid));
        Assert.Equal(2, read.Enumerate<Wrapper<IShape>>().Count());
    }

    [Fact]
    public void AliasLoadsObjectsStoredUnderItUnlessItsNameIsRegistered()
    {
        _db.RegisterType(typeof(First.Person), "Person");
        ulong oid;
        using (var tr = _db.StartTransaction())
        {
            oid = tr.Store(new First.Person { Name = "A" });
            tr.Commit();
        }

        ReopenDb();
        Assert.Equal("RenamedPerson", _db.RegisterType(typeof(First.Person), "RenamedPerson"));
        Assert.True(_db.RegisterTypeAlias(typeof(First.Person), "Person"));
        Assert.True(_db.RegisterTypeAlias(typeof(First.Person), "Person"));
        Assert.False(_db.RegisterTypeAlias(typeof(Second.Person), "Person"));
        Assert.Throws<ArgumentException>(() => _db.RegisterTypeAlias(typeof(IShape), "Person"));
        using (var tr = _db.StartTransaction())
        {
            Assert.Equal("A", Assert.IsType<First.Person>(tr.Get(oid)).Name);
        }

        ReopenDb();
        _db.RegisterTypeAlias(typeof(First.Person), "Person");
        _db.RegisterType(typeof(Second.Person), "Person");
        using var read = _db.StartTransaction();
        Assert.IsType<Second.Person>(read.Get(oid));
    }
}
