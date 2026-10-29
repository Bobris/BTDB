using BTDB;
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
}
