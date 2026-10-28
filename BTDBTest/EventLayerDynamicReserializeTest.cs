using Xunit;

namespace BTDBTest;

public class EventLayerDynamicReserializeTest
{
    public static TheoryData<string, EventLayer> Cases()
    {
        var data = new TheoryData<string, EventLayer>();
        foreach (var name in new[]
                 {
                     "Primitives", "Enums", "Nullables", "Lists", "Dictionaries", "Tuples", "Bytes", "Secrets",
                     "Everything"
                 })
        foreach (var layer in new[] { EventLayer.Mapping, EventLayer.Store1, EventLayer.Store2 })
            data.Add(name, layer);
        return data;
    }

    /// Deserialized objects remember descriptors of their collections (PreserveDescriptors),
    /// storing them again (into different store) must use these remembered descriptors correctly.
    [Theory]
    [MemberData(nameof(Cases))]
    public void DeserializedObjectsCanBeSerializedAgain(string scenarioName, EventLayer layer)
    {
        var scenario = EventLayerScenarios.All[scenarioName];
        var cipher = EventLayerScenarios.CipherFor(scenario);
        var original = scenario.Create();
        var firstResult = EventLayerRoundTripHelper.RoundTrip(layer, original, cipher: cipher);
        foreach (var otherLayer in new[] { EventLayer.Mapping, EventLayer.Store1, EventLayer.Store2 })
        {
            var result = EventLayerRoundTripHelper.RoundTrip(otherLayer, firstResult, cipher: cipher);
            Assert.Equal(ObjectGraphDumper.Dump(original), ObjectGraphDumper.Dump(result));
        }
    }

    [BTDB.Generate]
    public class CollectionsOfSimpleValues
    {
        public System.Collections.Generic.List<string>? Strings { get; set; }
        public System.Collections.Generic.Dictionary<int, string>? Map { get; set; }
        public System.Collections.Generic.List<System.Collections.Generic.List<int>>? Nested { get; set; }
        public System.Collections.Generic.List<EventLayerScenarios.IntEnum>? Enums { get; set; }
        public EventLayerScenarios.FlagsEnum Flags { get; set; }
    }

    /// Dynamically loaded collections know their descriptors, so they can be stored again
    /// (their descriptors are merged into serializer's descriptors).
    /// Re-serialization of DynamicEnum and DynamicObject is not supported yet; nested dynamic lists are since the
    /// 2026-10-29 fix, which registers the descriptors nested in merged ones.
    [Theory]
    [InlineData(EventLayer.Mapping, EventLayer.Mapping)]
    [InlineData(EventLayer.Mapping, EventLayer.Store1)]
    [InlineData(EventLayer.Mapping, EventLayer.Store2)]
    [InlineData(EventLayer.Store2, EventLayer.Mapping)]
    [InlineData(EventLayer.Store2, EventLayer.Store1)]
    [InlineData(EventLayer.Store2, EventLayer.Store2)]
    public void DynamicCollectionsCanBeSerializedAgain(EventLayer readLayer, EventLayer writeLayer)
    {
        var original = new CollectionsOfSimpleValues
        {
            Strings = ["a", "b"], Map = new() { [1] = "one" }, Nested = [[1, 2], []],
            Enums = [EventLayerScenarios.IntEnum.Two],
            Flags = EventLayerScenarios.FlagsEnum.A | EventLayerScenarios.FlagsEnum.B
        };
        var mapper = new BTDB.EventStoreLayer.FullNameTypeMapper();
        var hide = new RenamingTypeMapper()
            .Hide(mapper.ToName(typeof(CollectionsOfSimpleValues)))
            .Hide(mapper.ToName(typeof(EventLayerScenarios.IntEnum)))
            .Hide(mapper.ToName(typeof(EventLayerScenarios.FlagsEnum)));
        var fields = EventLayerScenarios.Fields(
            EventLayerRoundTripHelper.RoundTrip(readLayer, original, readMapper: hide));
        foreach (var (name, value) in fields)
        {
            if (name is not ("Strings" or "Map" or "Nested")) continue; // see limitation above
            var expected = typeof(CollectionsOfSimpleValues).GetProperty(name)!.GetValue(original);
            var result = EventLayerRoundTripHelper.RoundTrip(writeLayer, value);
            Assert.Equal(ObjectGraphDumper.Dump(expected), ObjectGraphDumper.Dump(result));
        }
    }

    [BTDB.Generate]
    public class RecursiveHiddenNode
    {
        public int Value { get; set; }
        public RecursiveHiddenNode? Next { get; set; }
    }

    [BTDB.Generate]
    public class HoldsRecursiveHiddenNodes
    {
        public System.Collections.Generic.List<RecursiveHiddenNode>? Nodes { get; set; }
    }

    /// Re-serializing dynamic objects is not supported, but before the fix a type holding itself below the root made
    /// merging its descriptor recurse until the stack overflowed and ended the process; now it fails with an exception.
    [Fact]
    public void DynamicRecursiveObjectsFailWithExceptionInsteadOfCrash()
    {
        var mapper = new BTDB.EventStoreLayer.FullNameTypeMapper();
        var hide = new RenamingTypeMapper().Hide(mapper.ToName(typeof(RecursiveHiddenNode)))
            .Hide(mapper.ToName(typeof(HoldsRecursiveHiddenNodes)));
        var loaded = EventLayerRoundTripHelper.RoundTrip2(new HoldsRecursiveHiddenNodes
        {
            Nodes = [new() { Value = 1, Next = new() { Value = 2 } }]
        }, readMapper: hide);
        Assert.NotNull(Record.Exception(() => EventLayerRoundTripHelper.RoundTrip2(loaded, writeMapper: hide)));
    }

    [BTDB.Generate]
    public class HoldsBlobs
    {
        public System.Collections.Generic.List<byte[]>? Blobs { get; set; }
    }

    /// Before the fix storing a dynamically loaded list of byte arrays again overflowed the stack and ended the
    /// process, as the byte array descriptor converted its object items with the types of the conversion swapped.
    [Theory]
    [InlineData(EventLayer.Store2, EventLayer.Store2)]
    [InlineData(EventLayer.Store2, EventLayer.Store1)]
    [InlineData(EventLayer.Mapping, EventLayer.Mapping)]
    public void DynamicListOfByteArraysCanBeSerializedAgain(EventLayer readLayer, EventLayer writeLayer)
    {
        var original = new HoldsBlobs { Blobs = [[1, 2], []] };
        var hide = new RenamingTypeMapper().Hide(new BTDB.EventStoreLayer.FullNameTypeMapper().ToName(typeof(HoldsBlobs)));
        var blobs = EventLayerScenarios.Fields(EventLayerRoundTripHelper.RoundTrip(readLayer, original, readMapper: hide))["Blobs"];
        var result = EventLayerRoundTripHelper.RoundTrip(writeLayer, blobs);
        Assert.Equal(ObjectGraphDumper.Dump(original.Blobs), ObjectGraphDumper.Dump(result));
    }

    [BTDB.Generate]
    public class HoldsNestedCollections
    {
        public System.Collections.Generic.List<int?>? NullableInts { get; set; }
        public System.Collections.Generic.List<System.Collections.Generic.List<int>>? Nested { get; set; }
        public System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<int>>? MapOfLists { get; set; }
        public System.Collections.Generic.List<System.Collections.Generic.Dictionary<int, string>>? ListOfMaps { get; set; }
        public System.Collections.Generic.List<int?[]>? Deep { get; set; }
    }

    /// Before the fix storing again dynamically loaded collections of nullables or of collections failed: the store did
    /// not register the descriptors nested in merged ones, list and dictionary descriptors could not save as object, and
    /// the nested collections could not load in an object member.
    [Theory]
    [InlineData(EventLayer.Store2, EventLayer.Store2)]
    [InlineData(EventLayer.Store2, EventLayer.Store1)]
    [InlineData(EventLayer.Store1, EventLayer.Store2)]
    [InlineData(EventLayer.Mapping, EventLayer.Store2)]
    [InlineData(EventLayer.Mapping, EventLayer.Mapping)]
    public void DynamicNestedCollectionsCanBeSerializedAgain(EventLayer readLayer, EventLayer writeLayer)
    {
        var original = new HoldsNestedCollections
        {
            NullableInts = [1, null], Nested = [[1, 2], []], MapOfLists = new() { ["x"] = [3] },
            ListOfMaps = [new() { [1] = "a" }], Deep = [[1, null]]
        };
        var hide = new RenamingTypeMapper()
            .Hide(new BTDB.EventStoreLayer.FullNameTypeMapper().ToName(typeof(HoldsNestedCollections)));
        foreach (var (name, value) in EventLayerScenarios.Fields(
                     EventLayerRoundTripHelper.RoundTrip(readLayer, original, readMapper: hide)))
        {
            var expected = typeof(HoldsNestedCollections).GetProperty(name)!.GetValue(original);
            var result = EventLayerRoundTripHelper.RoundTrip(writeLayer, value);
            Assert.Equal(ObjectGraphDumper.Dump(expected), ObjectGraphDumper.Dump(result));
        }
    }
}
