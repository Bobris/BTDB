using System.Collections.Generic;
using BTDB;
using Xunit;
using static BTDBTest.EventLayerRoundTripHelper;

namespace BTDBTest;

public class TypeSerializersMappingCoverageTest
{
    [Generate]
    public class CyclicNode
    {
        public string? Name { get; set; }
        public CyclicNode? Next { get; set; }
        public List<CyclicNode>? Children { get; set; }
    }

    static CyclicNode CreateCycle()
    {
        var a = new CyclicNode { Name = "a" };
        var b = new CyclicNode { Name = "b", Next = a };
        a.Next = b;
        a.Children = [a, b];
        return a;
    }

    static void AssertCycle(object? result)
    {
        var a = Assert.IsType<CyclicNode>(result);
        Assert.Equal("a", a.Name);
        Assert.Equal("b", a.Next!.Name);
        Assert.Same(a, a.Next.Next);
        Assert.Same(a, a.Children![0]);
        Assert.Same(a.Next, a.Children[1]);
    }

    [Fact]
    public void TypeSerializersMappingCanStoreCyclicObjectGraph()
    {
        AssertCycle(RoundTripMapping(CreateCycle()));
    }

    [Fact]
    public void EventStoreCanStoreCyclicObjectGraph()
    {
        AssertCycle(RoundTrip1(CreateCycle()));
    }

    [Fact]
    public void EventStore2CanStoreCyclicObjectGraph()
    {
        AssertCycle(RoundTrip2(CreateCycle()));
    }
}
