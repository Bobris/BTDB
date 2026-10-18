using System;
using System.Collections.Generic;
using BTDB;
using BTDB.EventStoreLayer;
using Xunit;
using static BTDBTest.EventLayerRoundTripHelper;

namespace BTDBTest;

public class TupleTypeDescriptorCoverageTest
{
    public enum Color
    {
        Red,
        Green,
        Blue
    }

    [Generate]
    public class Payload
    {
        public string? Name { get; set; }
        public int Value { get; set; }
    }

    [Generate]
    public class TupleWithEnum
    {
        public (Color, int) Pair { get; set; }
        public List<(Color, string)>? List { get; set; }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void TupleWithEnumItemRoundTrips(EventLayer layer)
    {
        var value = new TupleWithEnum { Pair = (Color.Blue, 42), List = [(Color.Green, "g")] };
        var result = Assert.IsType<TupleWithEnum>(RoundTrip(layer, value));
        Assert.Equal((Color.Blue, 42), result.Pair);
        Assert.Equal([(Color.Green, "g")], result.List);
    }

    [Generate]
    public class ReferenceTuple
    {
        public Tuple<int, string>? Pair { get; set; }
        public Tuple<long, Payload>? WithObject { get; set; }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void ReferenceTupleRoundTrips(EventLayer layer)
    {
        var value = new ReferenceTuple
        {
            Pair = Tuple.Create(1, "one"), WithObject = Tuple.Create(2L, new Payload { Name = "p", Value = 3 })
        };
        var result = Assert.IsType<ReferenceTuple>(RoundTrip(layer, value));
        Assert.Equal(Tuple.Create(1, "one"), result.Pair);
        Assert.Equal(2L, result.WithObject!.Item1);
        Assert.Equal("p", result.WithObject.Item2.Name);
        Assert.Equal(3, result.WithObject.Item2.Value);
    }

    [Generate]
    public class TupleWithObject
    {
        public (int, Payload) Pair { get; set; }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void TupleWithUnknownTypeCanBeReadDynamically(EventLayer layer)
    {
        var value = new TupleWithObject { Pair = (7, new Payload { Name = "x", Value = 8 }) };
        var readMapper = new RenamingTypeMapper()
            .Hide(new FullNameTypeMapper().ToName(typeof(TupleWithObject)))
            .Hide(new FullNameTypeMapper().ToName(typeof(Payload)));
        var result = RoundTrip(layer, value, readMapper: readMapper);
        var fields = EventLayerScenarios.Fields(result);
        var pair = Assert.IsType<ValueTuple<int, object>>(fields["Pair"]);
        Assert.Equal(7, pair.Item1);
        var payload = EventLayerScenarios.Fields(pair.Item2);
        Assert.Equal("x", payload["Name"]);
        Assert.Equal(8, payload["Value"]);
    }

    [Generate]
    public class TupleWithEnumAndNullable
    {
        public (Color, Guid?) Pair { get; set; }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void TupleWithUnregisteredPreferredTypeCanBeReadDynamically(EventLayer layer)
    {
        var g = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        var value = new TupleWithEnumAndNullable { Pair = (Color.Green, g) };
        var readMapper = new RenamingTypeMapper()
            .Hide(new FullNameTypeMapper().ToName(typeof(TupleWithEnumAndNullable)))
            .Hide(new FullNameTypeMapper().ToName(typeof(Color)));
        var fields = EventLayerScenarios.Fields(RoundTrip(layer, value, readMapper: readMapper));
        var pair = Assert.IsType<TupleTypeDescriptor.DynamicTuple>(fields["Pair"]);
        var items = new List<object>(pair);
        Assert.Equal(2, items.Count);
        Assert.Equal("Green", items[0].ToString());
        Assert.Equal(g, items[1]);
        Assert.Equal("(\"Green\", \"00112233-4455-6677-8899-aabbccddeeff\")", pair.ToString());
        Assert.Same(pair.GetDescriptor(), ((BTDB.EventStoreLayer.IKnowDescriptor)pair).GetDescriptor());
        dynamic d = pair;
        Assert.Equal(g, (Guid)d.Item2);
        Assert.Throws<MemberAccessException>(() => (object)d.Item3);
        d.Item2 = Guid.Empty;
        Assert.Equal(Guid.Empty, (Guid)d.Item2);
        Assert.Throws<MemberAccessException>(() => d.Item3 = 1);
        Assert.Throws<MemberAccessException>(() => d.Unknown = 1);
    }
}
