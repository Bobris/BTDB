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
    /// Re-serialization of DynamicEnum, DynamicObject and nested dynamic lists is not supported yet.
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
            if (name is not ("Strings" or "Map")) continue; // see limitation above
            var expected = typeof(CollectionsOfSimpleValues).GetProperty(name)!.GetValue(original);
            var result = EventLayerRoundTripHelper.RoundTrip(writeLayer, value);
            Assert.Equal(ObjectGraphDumper.Dump(expected), ObjectGraphDumper.Dump(result));
        }
    }
}
