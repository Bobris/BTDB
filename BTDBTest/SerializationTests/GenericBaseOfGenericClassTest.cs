using BTDB;
using BTDB.Bon;
using BTDB.Serialization;
using Xunit;

namespace BTDBTest.SerializationTests.GenericBaseOfGenericClass;

public class Box<T>
{
    T _computed = default!;

    public T Value { get; set; } = default!;

    public T Computed
    {
        get => _computed;
        set => _computed = value;
    }
}

[GenerateFor(typeof(Derived<int>))]
[GenerateFor(typeof(Derived<string>))]
public class Derived<T> : Box<T>
{
    public T Extra { get; set; } = default!;
}

// Before the fix accessors of members a generic class inherits from a generic base were nested in its generic
// Activator, so UnsafeAccessor did not find them and the module initializer of the assembly threw.
public class GenericBaseOfGenericClassTest
{
    static object? RoundTrip(object value)
    {
        var builder = new BonBuilder();
        BonSerializerFactory.Serialize(ref builder, value);
        var bon = new Bon(builder.FinishAsMemory());
        return BonSerializerFactory.Deserialize(ref bon);
    }

    [Fact]
    public void MembersOfGenericBaseRoundTrip()
    {
        var ints = Assert.IsType<Derived<int>>(RoundTrip(new Derived<int> { Value = 1, Computed = 2, Extra = 3 }));
        Assert.Equal((1, 2, 3), (ints.Value, ints.Computed, ints.Extra));
        var strings = Assert.IsType<Derived<string>>(RoundTrip(new Derived<string> { Value = "a", Computed = "b", Extra = "c" }));
        Assert.Equal(("a", "b", "c"), (strings.Value, strings.Computed, strings.Extra));
    }
}
