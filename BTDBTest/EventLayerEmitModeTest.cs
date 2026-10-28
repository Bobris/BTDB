using System.Collections.Generic;
using System;
using BTDB;
using BTDB.EventStoreLayer;
using BTDB.KVDBLayer;
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

    public enum TupleKind
    {
        A,
        B
    }

    [Generate]
    public class StructItemTupleEvent
    {
        public (DateTime, byte) DateAndByte { get; set; }
        public ((int, string), TupleKind) NestedWithEnum { get; set; }
        public (Guid, byte)? NullableGuidAndByte { get; set; }
    }

    // Without emitting code the layers access tuple items at the offsets of the generated metadata, which before the
    // fix were measured on stand-in structs with sequential layout instead of the tuple's automatic layout.
    [Theory]
    [InlineData(EventLayer.Mapping, false)]
    [InlineData(EventLayer.Mapping, true)]
    [InlineData(EventLayer.Store1, false)]
    [InlineData(EventLayer.Store1, true)]
    [InlineData(EventLayer.Store2, false)]
    [InlineData(EventLayer.Store2, true)]
    public void TuplesWithStructItemsRoundTrip(EventLayer layer, bool useNoEmit)
    {
        RunWithEmitMode(useNoEmit, () =>
        {
            var when = new DateTime(2025, 6, 7, 8, 9, 10, DateTimeKind.Utc);
            var guid = Guid.NewGuid();
            var result = Assert.IsType<StructItemTupleEvent>(RoundTrip(layer, new StructItemTupleEvent
            {
                DateAndByte = (when, 7), NestedWithEnum = ((1, "s"), TupleKind.B), NullableGuidAndByte = (guid, 3)
            }));
            Assert.Equal(((when, (byte)7), ((1, "s"), TupleKind.B), ((Guid, byte)?)(guid, 3)),
                (result.DateAndByte, result.NestedWithEnum, result.NullableGuidAndByte));
        });
    }

    [Generate]
    public class RenamedItem
    {
        public string? Text { get; set; }
    }

    [Generate]
    public class HoldsRenamedItems
    {
        public List<RenamedItem>? Items { get; set; }
    }

    // An event stored with a type name that maps to no type, as after the type was renamed or removed, loads as a dynamic
    // object. Before the fix loading it into a typed member failed with NullReferenceException.
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void ItemsOfAnUnmappedTypeAreReported(EventLayer layer)
    {
        RunWithEmitMode(true, () =>
        {
            var writeMapper = new RenamingTypeMapper().Map(typeof(RenamedItem), "Gone.RenamedItem");
            var error = Assert.Throws<BTDBException>(() =>
                RoundTrip(layer, new HoldsRenamedItems { Items = [new() { Text = "a" }] }, writeMapper));
            Assert.Contains(nameof(RenamedItem), error.Message);
        });
    }

    // The 2026-10-28 fixes of collections other than List, HashSet and Dictionary, of lists declared only as read-only
    // interfaces, of read-only IPAddress instances and of wide nullable conversions, with IL emitted. Before the fix of
    // IntelligentCast an IReadOnlySet<T> member failed here with InvalidCastException.
    [Theory]
    [InlineData(EventLayer.Mapping)]
    [InlineData(EventLayer.Store1)]
    [InlineData(EventLayer.Store2)]
    public void CollectionAndConversionFixesHoldWithEmit(EventLayer layer)
    {
        RunWithEmitMode(false, () =>
        {
            var coverage = new EventStore2CoverageTest();
            coverage.OtherDictionariesStore(layer);
            coverage.ReadOnlyAndImmutableListsStore(layer);
            coverage.ListDeclaredOnlyAsReadOnlyLoads(layer);
            coverage.ReadOnlyIPAddressStoresAsObject(layer);
            coverage.WideNullableMemberLoadsAsPlain(layer);
            coverage.DictionaryInObjectMemberLoads(layer);
        });
    }
}
