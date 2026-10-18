using System.Collections;
using System.Linq;
using Xunit;

namespace BTDB.SourceGenerator.Tests;

public class EquatableArrayTests
{
    [Fact]
    public void EqualsObjectComparesItems()
    {
        // Equals(object) delegated to static object.Equals which called Equals(object) again => stack overflow.
        object boxed = new EquatableArray<int>([1, 2]);
        Assert.True(boxed.Equals(new EquatableArray<int>([1, 2])));
        Assert.False(boxed.Equals(new EquatableArray<int>([2, 1])));
        Assert.False(boxed.Equals("not array"));
        Assert.False(boxed.Equals(null));
    }

    [Fact]
    public void HashCodeAndOperatorsAreValueBased()
    {
        var a = new EquatableArray<string>(["a", "b"]);
        var b = new EquatableArray<string>(["a", "b"]);
        var c = new EquatableArray<string>(["c"]);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.True(a == b);
        Assert.False(a != b);
        Assert.True(a != c);
        Assert.Equal(0, default(EquatableArray<string>).GetHashCode());
    }

    [Fact]
    public void DefaultArrayBehavesAsEmpty()
    {
        var empty = default(EquatableArray<int>);
        Assert.True(empty.IsEmpty);
        Assert.Equal(0, empty.Count);
        Assert.Null(empty.GetArray());
        Assert.Empty(empty);
        Assert.Empty(((IEnumerable)empty).Cast<int>());
        EquatableArray<int>? fromNull = (int[]?)null;
        Assert.Null(fromNull);
        EquatableArray<int> created = [1, 2, 3];
        Assert.Equal(new[] { 1, 2, 3 }, ((IEnumerable)created).Cast<int>());
        Assert.Equal(2, created[1]);
    }
}
