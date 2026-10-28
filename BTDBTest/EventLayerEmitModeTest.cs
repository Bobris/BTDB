using System.Collections.Generic;
using System;
using BTDB;
using BTDB.EventStoreLayer;
using Xunit;
using static BTDBTest.EventLayerRoundTripHelper;

namespace BTDBTest;

/// Tests of IL emitting (non default) code paths of EventStoreLayer and EventStore2Layer.
[Collection("IFieldHandler.UseNoEmitForRelations")]
public class EventLayerEmitModeTest
{
    [Generate]
    public class TupleHolder
    {
        public string? Name { get; set; }
    }

    [Generate]
    public class ListOfTuplesWithObject
    {
        public List<(int, TupleHolder)>? Items { get; set; }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void ListOfValueTuplesWithNonSealedItemCanBeSerialized(EventLayer layer)
    {
        RunWithEmitMode(false, () =>
        {
            var value = new ListOfTuplesWithObject { Items = [(1, new() { Name = "a" }), (2, new() { Name = "b" })] };
            var result = Assert.IsType<ListOfTuplesWithObject>(RoundTrip(layer, value));
            Assert.Equal(2, result.Items!.Count);
            Assert.Equal(1, result.Items[0].Item1);
            Assert.Equal("a", result.Items[0].Item2.Name);
            Assert.Equal(2, result.Items[1].Item1);
            Assert.Equal("b", result.Items[1].Item2.Name);
        });
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void ListOfTuplesWithUnknownItemTypeCanBeReadDynamically(EventLayer layer)
    {
        RunWithEmitMode(false, () =>
        {
            var value = new ListOfTuplesWithObject { Items = [(1, new() { Name = "a" })] };
            var readMapper = new RenamingTypeMapper()
                .Hide(new FullNameTypeMapper().ToName(typeof(ListOfTuplesWithObject)))
                .Hide(new FullNameTypeMapper().ToName(typeof(TupleHolder)));
            var fields = EventLayerScenarios.Fields(RoundTrip(layer, value, readMapper: readMapper));
            var items = Assert.IsAssignableFrom<System.Collections.IList>(fields["Items"]);
            var item = Assert.IsType<ValueTuple<int, object>>(Assert.Single(items));
            Assert.Equal(1, item.Item1);
            Assert.Equal("a", EventLayerScenarios.Fields(item.Item2)["Name"]);
        });
    }

    [Generate]
    public class UIntValue
    {
        public uint Value { get; set; }
    }

    [Generate]
    public class LongValue
    {
        public long Value { get; set; }
    }

    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store2)]
    public void UIntUpgradedToLongKeepsValue(EventLayer layer)
    {
        RunWithEmitMode(false, () =>
        {
            var result = RoundTrip(layer, new UIntValue { Value = uint.MaxValue },
                new RenamingTypeMapper().Map(typeof(UIntValue), "V"),
                new RenamingTypeMapper().Map(typeof(LongValue), "V"));
            Assert.Equal(uint.MaxValue, Assert.IsType<LongValue>(result).Value);
        });
    }
}
